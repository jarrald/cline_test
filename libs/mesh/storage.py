"""Sharded, replicated document store client.

Architecture
------------
Storage tier: N independent ``storage-node`` processes, each with its own SQLite
database file (one PVC in Kubernetes). Each shard has one primary that accepts
writes and zero or more read replicas. The order service and inventory worker
use this client to place documents via consistent hashing.

Guarantees are deliberately honest:
* reads from replicas are eventually consistent (async replication, bounded lag)
* writes go to the shard primary; if the primary is unreachable the client can
  promote a replica (client-side failover, no consensus - see ADR 0004)
* ``mutate`` is exactly-once per ``idempotency_key`` because the storage node
  commits the value change and the idempotency record in one transaction
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable

from . import logging as mesh_log
from .config import env, env_int
from .hashing import ConsistentHashRing
from .httpkit import HttpClient, HttpError, HttpResult, HttpUnavailable
from .metrics import Metrics

log = mesh_log.get_logger("storage")


class StorageUnavailable(Exception):
    """No node in the shard could serve the request."""


class StorageConflict(Exception):
    """Optimistic concurrency check failed (HTTP 409)."""


class DocumentNotFound(Exception):
    """The shard is reachable but the document does not exist (HTTP 404)."""


@dataclass
class Shard:
    name: str
    primary: str
    replicas: list[str] = field(default_factory=list)


class StorageNodeClient:
    def __init__(self, http: HttpClient | None = None) -> None:
        self.http = http or HttpClient(timeout=2.0, name="storage")

    def health(self, url: str) -> dict[str, Any]:
        result = self.http.request("GET", f"{url}/readyz", timeout=1.0, retry=False)
        return result.json() or {}

    def get(self, url: str, key: str) -> dict[str, Any]:
        result = self.http.request("GET", f"{url}/kv/{key}", retry=False)
        self._raise(result, url)
        return result.json()

    def put(
        self,
        url: str,
        key: str,
        value: Any,
        expected_version: int | None = None,
        version: int | None = None,
        monotonic: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"value": value}
        if expected_version is not None:
            body["expected_version"] = expected_version
        if version is not None:
            body["version"] = version
        if monotonic:
            body["monotonic"] = True
        result = self.http.request("PUT", f"{url}/kv/{key}", body=body, retry=False)
        self._raise(result, url)
        return result.json()

    def mutate(self, url: str, key: str, op: str, amount: int, idempotency_key: str) -> dict[str, Any]:
        body = {"op": op, "amount": amount, "idempotency_key": idempotency_key}
        result = self.http.request("POST", f"{url}/kv/{key}/mutate", body=body, retry=False)
        self._raise(result, url)
        return result.json()

    def stats(self, url: str) -> dict[str, Any]:
        result = self.http.request("GET", f"{url}/stats", timeout=1.0, retry=False)
        self._raise(result, url)
        return result.json()

    @staticmethod
    def _raise(result: HttpResult, url: str) -> None:
        if result.status == 409:
            raise StorageConflict(result.body.decode() or "conflict")
        if result.status == 404:
            raise DocumentNotFound(result.body.decode() or "not found")
        if result.status >= 500:
            raise HttpUnavailable(f"{url} returned {result.status}")
        if result.status >= 400:
            raise HttpError(result.status, result.body, url)


class ShardedStore:
    """Consistent-hash sharded store with read replicas and primary failover."""

    def __init__(
        self,
        shards: list[Shard],
        metrics: Metrics | None = None,
        client: StorageNodeClient | None = None,
        failure_threshold: int = 2,
        enable_failover: bool = True,
        replication_workers: int = 2,
    ) -> None:
        if not shards:
            raise ValueError("at least one shard is required")
        self.shards = {shard.name: shard for shard in shards}
        self.ring = ConsistentHashRing(self.shards.keys())
        self.metrics = metrics
        self.client = client or StorageNodeClient()
        self.failure_threshold = failure_threshold
        self.enable_failover = enable_failover
        self._primaries = {name: shard.primary for name, shard in self.shards.items()}
        self._failures: dict[str, int] = {}
        self._lock = threading.Lock()
        self._replication = ThreadPoolExecutor(max_workers=replication_workers, thread_name_prefix="repl")
        self._replication_dropped = 0
        self._events: list[dict[str, Any]] = []

    # ------------------------------------------------------------------ helpers
    def shard_for(self, key: str) -> Shard:
        return self.shards[self.ring.get(key)]

    def primary_for(self, shard: Shard) -> str:
        with self._lock:
            return self._primaries[shard.name]

    def placement(self, keys: list[str]) -> dict[str, int]:
        counts: dict[str, int] = {name: 0 for name in self.shards}
        for key in keys:
            counts[self.shard_for(key).name] += 1
        return counts

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._events)

    def _record(self, kind: str, **details: Any) -> None:
        event = {"ts": round(time.time(), 3), "event": kind, **details}
        with self._lock:
            self._events.append(event)
        log.warning(kind, extra=mesh_log.fields(**details))

    def _observe(self, op: str, shard: str, node: str, status: str, started: float) -> None:
        if not self.metrics:
            return
        self.metrics.inc("storage_requests_total", op=op, shard=shard, node=node, status=status)
        self.metrics.observe("storage_request_duration_seconds", time.monotonic() - started, op=op, shard=shard)

    def _note_failure(self, shard: Shard, url: str) -> None:
        with self._lock:
            self._failures[url] = self._failures.get(url, 0) + 1
            count = self._failures[url]
        if self.metrics:
            self.metrics.inc("storage_node_failures_total", shard=shard.name)
        if (
            self.enable_failover
            and count >= self.failure_threshold
            and url == self.primary_for(shard)
            and shard.replicas
        ):
            self._promote(shard, failed=url)

    def _note_success(self, url: str) -> None:
        with self._lock:
            self._failures.pop(url, None)

    def _promote(self, shard: Shard, failed: str) -> None:
        candidates = [r for r in shard.replicas if r != failed]
        if not candidates:
            return
        promoted = candidates[0]
        with self._lock:
            if self._primaries[shard.name] != failed:
                return  # another thread already failed over
            self._primaries[shard.name] = promoted
            self.shards[shard.name].primary = promoted
            self.shards[shard.name].replicas = [r for r in shard.replicas if r != promoted]
        if self.metrics:
            self.metrics.inc("storage_primary_failovers_total", shard=shard.name)
        self._record("primary_failover", shard=shard.name, failed=failed, promoted=promoted)

    def _write_primary(
        self, shard: Shard, key: str, operation: Callable[[str], dict[str, Any]], op_name: str
    ) -> dict[str, Any]:
        attempts = 2 if self.enable_failover else 1
        last_error: Exception | None = None
        for _ in range(attempts):
            primary = self.primary_for(shard)
            started = time.monotonic()
            try:
                result = operation(primary)
                self._note_success(primary)
                self._observe(op_name, shard.name, primary, "ok", started)
                return result
            except DocumentNotFound:
                self._observe(op_name, shard.name, primary, "missing", started)
                raise
            except (HttpUnavailable, HttpError) as exc:
                last_error = exc
                self._observe(op_name, shard.name, primary, "error", started)
                self._note_failure(shard, primary)
        raise StorageUnavailable(f"shard {shard.name} unavailable: {last_error}")

    # -------------------------------------------------------------------- public
    def put(self, key: str, value: Any, expected_version: int | None = None) -> dict[str, Any]:
        shard = self.shard_for(key)
        result = self._write_primary(
            shard,
            key,
            lambda primary: self.client.put(primary, key, value, expected_version=expected_version),
            "put",
        )
        self._replicate(shard, key, value, result.get("version"), exclude=self.primary_for(shard))
        return result

    def mutate(self, key: str, op: str, amount: int, idempotency_key: str) -> dict[str, Any]:
        shard = self.shard_for(key)
        result = self._write_primary(
            shard,
            key,
            lambda primary: self.client.mutate(primary, key, op, amount, idempotency_key),
            "mutate",
        )
        if not result.get("cached") and result.get("value") is not None:
            self._replicate(shard, key, result["value"], result.get("version"), exclude=self.primary_for(shard))
        return result

    def get(self, key: str, consistent: bool = False) -> dict[str, Any] | None:
        shard = self.shard_for(key)
        nodes = [self.primary_for(shard)] if consistent else list(shard.replicas) + [self.primary_for(shard)]
        last_error: Exception | None = None
        for index, node in enumerate(nodes):
            started = time.monotonic()
            try:
                result = self.client.get(node, key)
                if index > 0 and not consistent and self.metrics:
                    self.metrics.inc("storage_replica_fallback_total", shard=shard.name)
                self._note_success(node)
                self._observe("get", shard.name, node, "ok", started)
                return result
            except DocumentNotFound:
                self._observe("get", shard.name, node, "miss", started)
                return None
            except HttpError as exc:
                last_error = exc
                self._observe("get", shard.name, node, "error", started)
            except HttpUnavailable as exc:
                last_error = exc
                self._observe("get", shard.name, node, "error", started)
        raise StorageUnavailable(f"shard {shard.name} unavailable: {last_error}")

    def _replicate(self, shard: Shard, key: str, value: Any, version: int | None, exclude: str) -> None:
        replicas = [r for r in shard.replicas if r != exclude]
        if not replicas:
            return

        def push(url: str) -> None:
            try:
                self.client.put(url, key, value, version=version, monotonic=True)
                self._note_success(url)
                if self.metrics:
                    self.metrics.inc("storage_replication_total", shard=shard.name, node=url, status="ok")
            except Exception as exc:  # replication is best effort
                if self.metrics:
                    self.metrics.inc("storage_replication_total", shard=shard.name, node=url, status="error")
                    self.metrics.inc("storage_replication_failures_total", shard=shard.name)
                log.warning("replication failed", extra=mesh_log.fields(shard=shard.name, node=url, error=str(exc)))

        try:
            for url in replicas:
                self._replication.submit(push, url)
        except RuntimeError:  # pool shut down
            with self._lock:
                self._replication_dropped += 1

    def close(self) -> None:
        self._replication.shutdown(wait=True)

    # ------------------------------------------------------------------ factory
    @classmethod
    def from_env(cls, metrics: Metrics | None = None) -> "ShardedStore":
        raw = json.loads(env("SHARDS"))
        shards = [
            Shard(name=item["name"], primary=item["primary"], replicas=list(item.get("replicas", [])))
            for item in raw
        ]
        return cls(
            shards,
            metrics=metrics,
            failure_threshold=env_int("STORAGE_FAILURE_THRESHOLD", 2),
            enable_failover=env("STORAGE_FAILOVER", "true").lower() in {"1", "true", "yes"},
        )

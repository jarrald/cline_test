"""Full-cluster test harness.

Boots the real topology with kpilot on a free block of ports and guarantees
teardown. Tests interact only through public interfaces: the edge LB, kpilot's
admin API, storage nodes and broker stats.
"""

from __future__ import annotations

import json
import os
import random
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

from libs.mesh.httpkit import HttpClient, HttpUnavailable
from libs.mesh.storage import Shard, ShardedStore
from services.kpilot.spec import build_cluster, shard_topology

REPO_ROOT = Path(__file__).resolve().parents[2]


class WaitTimeout(AssertionError):
    pass


def port_free(port: int) -> bool:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        sock.close()


def find_free_block(size: int = 100, rng: random.Random | None = None) -> int:
    rng = rng or random.Random()
    for _ in range(300):
        base = rng.randrange(24000, 54000)
        if all(port_free(base + offset) for offset in range(size)):
            return base
    raise RuntimeError("could not find a free block of ports")


def wait_until(predicate, timeout: float, interval: float = 0.2, description: str = "condition"):
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as exc:  # noqa: BLE001 - the last error is reported
            last_error = exc
        time.sleep(interval)
    raise WaitTimeout(f"timed out after {timeout}s waiting for {description} (last error: {last_error!r})")


class Stack:
    def __init__(
        self,
        profile: str = "test",
        name: str = "stack",
        order_io_ms: int | None = None,
        order_replicas: int = 2,
        order_concurrency: int = 8,
        max_backlog: int | None = None,
        hpa_interval_s: float | None = None,
    ) -> None:
        self.name = name
        self.base_port = find_free_block()
        self.runtime_dir = str(REPO_ROOT / "runtime" / "tests" / f"{name}-{uuid.uuid4().hex[:8]}")
        self.spec = build_cluster(
            base_port=self.base_port,
            runtime_dir=self.runtime_dir,
            profile=profile,
            order_io_ms=order_io_ms,
            order_concurrency=order_concurrency,
            order_replicas=order_replicas,
            max_backlog=max_backlog,
            hpa_interval_s=hpa_interval_s,
        )
        self.process: subprocess.Popen | None = None
        self.http = HttpClient(timeout=10.0, name=f"harness-{name}")
        self.log_path = os.path.join(self.runtime_dir, "kpilot.log")

    # ------------------------------------------------------------------ setup
    def start(self, timeout: float = 120.0) -> "Stack":
        os.makedirs(self.runtime_dir, exist_ok=True)
        handle = open(self.log_path, "ab", buffering=0)
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "services.kpilot",
                "--base-port",
                str(self.base_port),
                "--runtime-dir",
                self.runtime_dir,
                "--profile",
                "test",
            ],
            cwd=str(REPO_ROOT),
            stdout=handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        wait_until(self._admin_ready, timeout, interval=0.5, description="kpilot cluster ready")
        return self

    def __enter__(self) -> "Stack":
        return self.start()

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    def stop(self) -> None:
        pod_pids: list[int] = []
        try:
            status = self.admin_get("/status")
            for deployment in status.get("deployments", {}).values():
                for pod in deployment.get("pods", []):
                    if pod.get("pid"):
                        pod_pids.append(int(pod["pid"]))
        except Exception:
            pass
        try:
            self.http.request("POST", self.admin_url("/shutdown"), body={}, timeout=2.0, retry=False)
        except Exception:
            pass
        if self.process is not None:
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                except (ProcessLookupError, PermissionError):
                    pass
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
        for pid in pod_pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if self.process is not None and self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    # ------------------------------------------------------------------- admin
    def admin_url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.spec.admin_port}{path}"

    def admin_get(self, path: str):
        result = self.http.request("GET", self.admin_url(path), retry=False)
        if result.status >= 400:
            raise AssertionError(f"admin GET {path} -> {result.status}: {result.body[:400]!r}")
        return result.json()

    def admin_post(self, path: str, body: dict):
        result = self.http.request("POST", self.admin_url(path), body=body, retry=False)
        if result.status >= 400:
            raise AssertionError(f"admin POST {path} -> {result.status}: {result.body[:400]!r}")
        return result.json()

    def _admin_ready(self) -> bool:
        try:
            return self.http.request("GET", self.admin_url("/readyz"), timeout=1.0, retry=False).status == 200
        except HttpUnavailable:
            return False

    def status(self) -> dict:
        return self.admin_get("/status")

    def deployment(self, name: str) -> dict:
        return self.admin_get(f"/deployments/{name}")

    def events(self, limit: int = 500) -> list[dict]:
        return self.admin_get(f"/events?limit={limit}")["events"]

    def scale(self, deployment: str, replicas: int, disable_autoscale: bool = True) -> dict:
        return self.admin_post("/scale", {"deployment": deployment, "replicas": replicas, "disable_autoscale": disable_autoscale})

    def autoscale(self, deployment: str, enabled: bool) -> dict:
        return self.admin_post("/autoscale", {"deployment": deployment, "enabled": enabled})

    def kill_pod(self, deployment: str, pod: int | None = None) -> dict:
        return self.admin_post("/kill", {"deployment": deployment, "pod": pod})

    def wait_cluster_ready(self, timeout: float = 90.0) -> None:
        wait_until(self._admin_ready, timeout, interval=0.5, description="cluster ready again")

    # --------------------------------------------------------------------- api
    @property
    def edge_port(self) -> int:
        return self.base_port + 60

    def api(self, method: str, path: str, body: dict | None = None, headers: dict | None = None):
        merged = {"X-API-Key": "demo-key", **(headers or {})}
        return self.http.request(method, f"http://127.0.0.1:{self.edge_port}{path}", body=body, headers=merged, timeout=15.0)

    def place_order(
        self,
        customer_id: str | None = None,
        sku: str = "sku-1",
        quantity: int = 1,
        idempotency_key: str | None = None,
        expected: int | None = 202,
        use_idempotency: bool = True,
    ) -> dict:
        customer_id = customer_id or f"cust_{uuid.uuid4().hex[:10]}"
        key = idempotency_key or uuid.uuid4().hex
        headers = {"Idempotency-Key": key} if use_idempotency else {}
        result = self.api(
            "POST",
            "/api/v1/orders",
            body={"customer_id": customer_id, "sku": sku, "quantity": quantity},
            headers=headers,
        )
        if expected is not None and result.status != expected:
            raise AssertionError(f"place_order expected {expected}, got {result.status}: {result.body[:400]!r}")
        order = result.json() or {}
        order["_status"] = result.status
        order["_key"] = key
        return order

    def get_order(self, order_id: str, consistent: bool = False) -> dict:
        suffix = "?consistent=true" if consistent else ""
        result = self.api("GET", f"/api/v1/orders/{order_id}{suffix}")
        return {"status": result.status, "body": result.json() or {}}

    def wait_for_order(self, order_id: str, expected: set[str] | None = None, timeout: float = 30.0) -> dict:
        expected = expected or {"CONFIRMED", "REJECTED"}
        def check():
            response = self.get_order(order_id, consistent=True)
            order = response["body"].get("order") or {}
            return order if order.get("status") in expected else None
        order = wait_until(check, timeout, interval=0.25, description=f"order {order_id} in {expected}")
        return order

    # ------------------------------------------------------------------ storage
    def seed_inventory(self, skus: list[str], stock: int = 500) -> None:
        shards = [
            Shard(name=item["name"], primary=item["primary"], replicas=item["replicas"])
            for item in shard_topology(self.base_port)
        ]
        store = ShardedStore(shards, enable_failover=False)
        try:
            for sku in skus:
                store.put(f"inventory:{sku}", {"sku": sku, "available": stock, "reserved": 0}, expected_version=0)
        finally:
            store.close()

    def inventory(self, sku: str, consistent: bool = True) -> dict:
        shards = [
            Shard(name=item["name"], primary=item["primary"], replicas=item["replicas"])
            for item in shard_topology(self.base_port)
        ]
        store = ShardedStore(shards, enable_failover=False)
        try:
            doc = store.get(f"inventory:{sku}", consistent=consistent)
            return (doc or {}).get("value") or {}
        finally:
            store.close()

    def shard_stats(self) -> dict[str, dict]:
        stats = {}
        for shard in shard_topology(self.base_port):
            for role in ("primary", "replica"):
                url = f"http://127.0.0.1:{shard['ports'][role]}"
                try:
                    stats[f"{shard['name']}-{role}"] = self.http.get_json(f"{url}/stats")
                except Exception:
                    stats[f"{shard['name']}-{role}"] = {}
        return stats

    def broker_stats(self, topic: str) -> dict:
        return self.http.get_json(f"http://127.0.0.1:{self.base_port + 10}/topics/{topic}/stats")

    def broker_total_lag(self) -> int:
        total = 0
        for topic in ("orders.created", "inventory.events"):
            stats = self.broker_stats(topic)
            total += sum(group["lag"] for group in stats.get("groups", {}).values())
        return total

    def wait_until_lag_zero(self, timeout: float = 30.0) -> bool:
        try:
            wait_until(lambda: self.broker_total_lag() == 0, timeout, interval=0.5, description="consumer lag zero")
            return True
        except WaitTimeout:
            return False

    def pod_metrics(self, deployment: str, ordinal: int = 0) -> dict:
        pod = self.deployment(deployment)["pods"][ordinal]
        return self.http.get_json(f"{pod['url']}/metrics.json")

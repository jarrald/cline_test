"""Client side service discovery.

In Kubernetes, kubelet publishes pod IPs to the Endpoints/EndpointSlice API and
kube-proxy load balances across them. This file mirrors that contract for the
local runtime: the ``kpilot`` control plane writes a registry file atomically and
clients poll it (like a watch with a TTL). The same interface accepts static
endpoints from environment variables, which is what unit tests use.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable, Iterable, Protocol

from . import logging as mesh_log
from .metrics import Metrics

log = mesh_log.get_logger("discovery")


@dataclass(frozen=True)
class Endpoint:
    service: str
    pod: str
    url: str
    ready: bool = True


class ServiceRegistry(Protocol):
    def endpoints(self, service: str) -> list[Endpoint]: ...


class StaticRegistry:
    """Registry backed by an in-memory map (used by unit/integration tests)."""

    def __init__(self, services: dict[str, list[str]]) -> None:
        self._services = {
            name: [Endpoint(name, f"{name}-{i}", url) for i, url in enumerate(urls)]
            for name, urls in services.items()
        }
        self._lock = threading.Lock()

    def endpoints(self, service: str) -> list[Endpoint]:
        with self._lock:
            return [e for e in self._services.get(service, []) if e.ready]

    def set(self, service: str, urls: list[str]) -> None:
        with self._lock:
            self._services[service] = [
                Endpoint(service, f"{service}-{i}", url) for i, url in enumerate(urls)
            ]


class FileRegistry:
    """Polls an atomically written JSON registry produced by ``kpilot``."""

    def __init__(self, path: str, ttl: float = 0.5, clock: Callable[[], float] = time.monotonic) -> None:
        self.path = path
        self.ttl = ttl
        self._clock = clock
        self._lock = threading.Lock()
        self._cached: dict[str, list[Endpoint]] = {}
        self._loaded_at = 0.0
        self._mtime = 0.0

    def _load(self) -> None:
        now = self._clock()
        with self._lock:
            if now - self._loaded_at < self.ttl:
                return
            self._loaded_at = now
        try:
            mtime = os.path.getmtime(self.path)
        except OSError:
            return
        if mtime <= self._mtime:
            return
        try:
            with open(self.path, encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            log.warning("registry unreadable, keeping previous snapshot", extra=mesh_log.fields(path=self.path))
            return
        services: dict[str, list[Endpoint]] = {}
        for name, pods in payload.get("services", {}).items():
            services[name] = [
                Endpoint(name, pod.get("pod", "?"), pod["url"], bool(pod.get("ready", True)))
                for pod in pods
            ]
        with self._lock:
            self._cached = services
            self._mtime = mtime

    def endpoints(self, service: str) -> list[Endpoint]:
        self._load()
        with self._lock:
            return [e for e in self._cached.get(service, []) if e.ready]


class EndpointPool:
    """Round-robin load balancer with passive health checking.

    A replica that fails is ejected for ``failure_cooldown`` seconds, which is
    the client-side equivalent of a readiness probe removing a pod from the
    Kubernetes Service endpoints. Round robin spreads load evenly; failures are
    tracked per endpoint for observability.
    """

    def __init__(
        self,
        registry: ServiceRegistry,
        service: str,
        metrics: Metrics | None = None,
        failure_cooldown: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.registry = registry
        self.service = service
        self.metrics = metrics
        self.failure_cooldown = failure_cooldown
        self._clock = clock
        self._lock = threading.Lock()
        self._cursor = 0
        self._ejected: dict[str, float] = {}
        self._failures: dict[str, int] = {}

    def endpoints(self) -> list[Endpoint]:
        now = self._clock()
        with self._lock:
            self._ejected = {url: until for url, until in self._ejected.items() if until > now}
            ejected = set(self._ejected)
        return [e for e in self.registry.endpoints(self.service) if e.url not in ejected]

    def pick(self) -> Endpoint | None:
        candidates = self.endpoints()
        if not candidates:
            if self.metrics:
                self.metrics.inc("discovery_no_endpoints_total", service=self.service)
            return None
        with self._lock:
            endpoint = candidates[self._cursor % len(candidates)]
            self._cursor += 1
        if self.metrics:
            self.metrics.inc("discovery_picks_total", service=self.service, endpoint=endpoint.pod)
        return endpoint

    def mark_failure(self, url: str) -> None:
        with self._lock:
            self._failures[url] = self._failures.get(url, 0) + 1
            self._ejected[url] = self._clock() + self.failure_cooldown
        if self.metrics:
            self.metrics.inc("discovery_ejected_total", service=self.service)

    def mark_success(self, url: str) -> None:
        with self._lock:
            self._failures.pop(url, None)
            self._ejected.pop(url, None)

    def failure_counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self._failures)


def static_from_env(service: str, env_name: str | None = None) -> StaticRegistry:
    var = env_name or f"{service.upper().replace('-', '_')}_ENDPOINTS"
    raw = os.environ.get(var, "")
    urls = [u.strip() for u in raw.split(",") if u.strip()]
    return StaticRegistry({service: urls})


def iter_ready(registry: ServiceRegistry, service: str) -> Iterable[Endpoint]:
    for endpoint in registry.endpoints(service):
        if endpoint.ready:
            yield endpoint

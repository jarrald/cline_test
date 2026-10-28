"""Metrics collection in the Prometheus data model.

A single :class:`Metrics` instance lives per process. Services expose it in two
formats:

* ``/metrics``      Prometheus text exposition (scraped by Prometheus/kubelet)
* ``/metrics.json`` machine readable snapshot consumed by the local autoscaler
  and by the reliability tests.

The ``inflight`` and ``cpu_percent`` signals are the ones the autoscaler uses;
``inflight`` is concurrency (Little's law: inflight = rps x latency) which is a
far better saturation signal than raw CPU for I/O bound Python services.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Iterable

LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)


def _labels(labels: Iterable[tuple[str, str]]) -> str:
    parts = [f'{k}="{v}"' for k, v in sorted(labels)]
    return "{" + ",".join(parts) + "}" if parts else ""


@dataclass
class _Histogram:
    buckets: list[float] = field(default_factory=lambda: [0.0] * len(LATENCY_BUCKETS))
    count: float = 0.0
    total: float = 0.0

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        for i, bound in enumerate(LATENCY_BUCKETS):
            if value <= bound:
                self.buckets[i] += 1
                break
        else:
            self.buckets[-1] += 1


class Metrics:
    def __init__(self, service: str, pod: str = "local-0") -> None:
        self.service = service
        self.pod = pod
        self._lock = threading.Lock()
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, _Histogram] = {}
        self._inflight = 0
        self._inflight_max = 0
        self._latencies: list[float] = []
        self._recent_requests: list[float] = []
        self._cpu_sample = (time.monotonic(), time.process_time())
        self._cpu_percent = 0.0
        self._started = time.time()
        self._sampler = threading.Thread(target=self._sample_cpu, daemon=True)
        self._sampler.start()

    # ------------------------------------------------------------------ update
    def inc(self, name: str, value: float = 1.0, **labels: str) -> None:
        key = name + _labels(list(labels.items()))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + value

    def gauge(self, name: str, value: float, **labels: str) -> None:
        key = name + _labels(list(labels.items()))
        with self._lock:
            self._gauges[key] = value

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = name + _labels(list(labels.items()))
        with self._lock:
            self._histograms.setdefault(key, _Histogram()).observe(value)
            if name == "http_request_duration_seconds":
                self._latencies.append(value)
                self._recent_requests.append(time.monotonic())
                del self._latencies[:-2048]
                del self._recent_requests[:-8192]

    def enter(self) -> int:
        with self._lock:
            self._inflight += 1
            self._inflight_max = max(self._inflight_max, self._inflight)
            return self._inflight

    def leave(self) -> None:
        with self._lock:
            self._inflight = max(0, self._inflight - 1)

    # -------------------------------------------------------------------- read
    def _sample_cpu(self) -> None:
        while True:
            time.sleep(0.5)
            now, cpu = time.monotonic(), time.process_time()
            prev_now, prev_cpu = self._cpu_sample
            wall = now - prev_now
            if wall > 0:
                pct = max(0.0, (cpu - prev_cpu) / wall * 100.0)
                with self._lock:
                    self._cpu_percent = 0.7 * self._cpu_percent + 0.3 * pct
            self._cpu_sample = (now, cpu)

    def _rps(self, window: float = 5.0) -> float:
        cutoff = time.monotonic() - window
        recent = [t for t in self._recent_requests if t >= cutoff]
        self._recent_requests = recent
        if not recent:
            return 0.0
        span = min(window, time.monotonic() - recent[0]) or window
        return len(recent) / span

    @staticmethod
    def _percentile(values: list[float], pct: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        idx = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
        return ordered[idx]

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "service": self.service,
                "pod": self.pod,
                "uptime_s": round(time.time() - self._started, 3),
                "inflight": self._inflight,
                "inflight_max": self._inflight_max,
                "cpu_percent": round(self._cpu_percent, 2),
                "rps": round(self._rps(), 2),
                "latency_ms": {
                    "p50": round(self._percentile(self._latencies, 50) * 1000, 2),
                    "p95": round(self._percentile(self._latencies, 95) * 1000, 2),
                    "p99": round(self._percentile(self._latencies, 99) * 1000, 2),
                },
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }

    def render_prometheus(self) -> str:
        from .metrics_prometheus import render_prometheus

        return render_prometheus(self)


_metrics: Metrics | None = None


def configure(service: str, pod: str = "local-0") -> Metrics:
    global _metrics
    _metrics = Metrics(service, pod)
    return _metrics


def get() -> Metrics:
    if _metrics is None:  # pragma: no cover - programming error
        raise RuntimeError("metrics.configure() must be called at startup")
    return _metrics

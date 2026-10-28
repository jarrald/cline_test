"""Closed/open-loop load generator used by demos and reliability tests.

Each worker thread keeps a persistent HTTP connection (like k6/wrk) and issues
requests at a target rate. Reports throughput, latency percentiles, status code
histogram and per-endpoint distribution. Exits non-zero if the error budget is
exceeded, so CI can gate on it.
"""

from __future__ import annotations

import argparse
import http.client
import json
import random
import threading
import time
import uuid
from dataclasses import dataclass, field


@dataclass
class Stats:
    latencies: list[float] = field(default_factory=list)
    statuses: dict[int, int] = field(default_factory=dict)
    errors: dict[str, int] = field(default_factory=dict)
    pods: dict[str, int] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, status: int, latency: float, pod: str | None, error: str | None = None) -> None:
        with self.lock:
            self.latencies.append(latency)
            self.statuses[status] = self.statuses.get(status, 0) + 1
            if pod:
                self.pods[pod] = self.pods.get(pod, 0) + 1
            if error:
                self.errors[error] = self.errors.get(error, 0) + 1


class Worker(threading.Thread):
    def __init__(
        self,
        worker_id: int,
        host: str,
        port: int,
        rate: float,
        duration: float,
        stats: Stats,
        api_key: str,
        sku: str,
        stop: threading.Event,
    ) -> None:
        super().__init__(daemon=True, name=f"loadgen-{worker_id}")
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.rate = rate
        self.duration = duration
        self.stats = stats
        self.api_key = api_key
        self.sku = sku
        self.stop = stop

    def _connection(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(self.host, self.port, timeout=5.0)

    def run(self) -> None:
        conn = self._connection()
        interval = 1.0 / self.rate if self.rate > 0 else 0.0
        next_at = time.monotonic()
        deadline = next_at + self.duration
        rng = random.Random(self.worker_id)
        while not self.stop.is_set() and time.monotonic() < deadline:
            payload = json.dumps(
                {
                    "customer_id": f"cust_{rng.randint(1, 20000)}",
                    "sku": self.sku,
                    "quantity": rng.randint(1, 2),
                }
            )
            headers = {
                "Content-Type": "application/json",
                "X-API-Key": self.api_key,
                "Idempotency-Key": uuid.uuid4().hex,
            }
            started = time.monotonic()
            status = 0
            error = None
            pod = None
            try:
                conn.request("POST", "/api/v1/orders", body=payload, headers=headers)
                response = conn.getresponse()
                response.read()
                status = response.status
                pod = response.getheader("X-Upstream-Pod")
            except Exception as exc:  # transport error
                error = type(exc).__name__
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self._connection()
            self.stats.record(status, time.monotonic() - started, pod, error)
            next_at += interval
            sleep_for = next_at - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            elif sleep_for < -1.0:
                next_at = time.monotonic()
        conn.close()


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[index]


def run(args: argparse.Namespace) -> dict:
    stats = Stats()
    stop = threading.Event()
    workers = [
        Worker(i, args.host, args.port, args.rate / args.workers, args.duration, stats, args.api_key, args.sku, stop)
        for i in range(args.workers)
    ]
    started = time.monotonic()
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=args.duration + 30)
    elapsed = time.monotonic() - started
    total = len(stats.latencies)
    ok = sum(count for status, count in stats.statuses.items() if 200 <= status < 300)
    summary = {
        "target_rps": args.rate,
        "workers": args.workers,
        "duration_s": round(elapsed, 2),
        "requests": total,
        "achieved_rps": round(total / elapsed, 2) if elapsed else 0.0,
        "success_rate": round(ok / total, 4) if total else 0.0,
        "statuses": {str(k): v for k, v in sorted(stats.statuses.items())},
        "transport_errors": stats.errors,
        "latency_ms": {
            "p50": round(percentile(stats.latencies, 50) * 1000, 2),
            "p95": round(percentile(stats.latencies, 95) * 1000, 2),
            "p99": round(percentile(stats.latencies, 99) * 1000, 2),
            "max": round(max(stats.latencies, default=0) * 1000, 2),
        },
        "upstream_pods": stats.pods,
    }
    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="HTTP load generator")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=14000)
    parser.add_argument("--rate", type=float, default=50.0, help="target requests per second")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--api-key", default="load-test-key")
    parser.add_argument("--sku", default="sku-1")
    parser.add_argument("--output", default=None)
    parser.add_argument("--max-error-rate", type=float, default=0.5)
    args = parser.parse_args()
    summary = run(args)
    print(json.dumps(summary, indent=2))
    raise SystemExit(0 if summary["success_rate"] >= 1 - args.max_error_rate else 1)


if __name__ == "__main__":
    main()

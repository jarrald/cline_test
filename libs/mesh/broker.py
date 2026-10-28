"""Message broker client: producer + partition consumer.

Semantics: at-least-once delivery with consumer-group offsets, per-partition
ordering, retry with attempt counting and a dead-letter topic after
``MAX_ATTEMPTS``. Partitioning is by message key, so all events for one
customer land on the same partition and stay ordered.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from . import logging as mesh_log
from .httpkit import HttpClient, HttpError, HttpUnavailable
from .metrics import Metrics

log = mesh_log.get_logger("broker")


class BrokerUnavailable(Exception):
    pass


class BrokerRejected(Exception):
    """Broker refused the publish (e.g. backpressure 503)."""

    def __init__(self, status: int, detail: Any) -> None:
        super().__init__(f"broker rejected publish: {status} {detail}")
        self.status = status
        self.detail = detail


@dataclass
class Message:
    topic: str
    partition: int
    offset: int
    key: str
    value: Any
    headers: dict[str, str] = field(default_factory=dict)
    attempts: int = 0

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Message":
        return cls(
            topic=payload["topic"],
            partition=int(payload["partition"]),
            offset=int(payload["offset"]),
            key=payload.get("key", ""),
            value=payload.get("value"),
            headers=payload.get("headers") or {},
            attempts=int(payload.get("attempts", 0)),
        )


class BrokerClient:
    def __init__(self, base_url: str, http: HttpClient | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http or HttpClient(timeout=2.0, name="broker")

    def publish(
        self, topic: str, key: str, value: Any, headers: dict[str, str] | None = None
    ) -> dict[str, Any]:
        result = self.http.request(
            "POST",
            f"{self.base_url}/topics/{topic}/messages",
            body={"key": key, "value": value, "headers": headers or {}},
            retry=False,
        )
        if result.status in (429, 503):
            raise BrokerRejected(result.status, result.body.decode())
        if result.status >= 400:
            raise BrokerUnavailable(f"publish failed: {result.status} {result.body!r}")
        return result.json() or {}

    def fetch(
        self, topic: str, group: str, partition: int, max_messages: int = 10
    ) -> list[Message]:
        result = self.http.request(
            "POST",
            f"{self.base_url}/topics/{topic}/fetch",
            body={"group": group, "partition": partition, "max_messages": max_messages},
            retry=False,
            timeout=10.0,
        )
        if result.status >= 400:
            raise BrokerUnavailable(f"fetch failed: {result.status} {result.body!r}")
        payload = result.json() or {}
        return [Message.from_dict(item) for item in payload.get("messages", [])]

    def ack(self, topic: str, group: str, partition: int, offset: int) -> None:
        result = self.http.request(
            "POST",
            f"{self.base_url}/topics/{topic}/ack",
            body={"group": group, "partition": partition, "offset": offset},
            retry=False,
        )
        if result.status >= 400:
            raise BrokerUnavailable(f"ack failed: {result.status}")

    def nack(self, topic: str, group: str, partition: int, offset: int, error: str) -> dict[str, Any]:
        result = self.http.request(
            "POST",
            f"{self.base_url}/topics/{topic}/nack",
            body={"group": group, "partition": partition, "offset": offset, "error": error},
            retry=False,
        )
        if result.status >= 400:
            raise BrokerUnavailable(f"nack failed: {result.status}")
        return result.json() or {}

    def partitions(self, topic: str) -> int:
        result = self.http.request("GET", f"{self.base_url}/topics/{topic}/partitions", timeout=1.5, retry=False)
        if result.status >= 400:
            raise BrokerUnavailable(f"partitions failed: {result.status}")
        payload = result.json() or {}
        return int(payload.get("partitions", 1))

    def stats(self, topic: str) -> dict[str, Any]:
        result = self.http.request("GET", f"{self.base_url}/topics/{topic}/stats", timeout=1.5, retry=False)
        if result.status >= 400:
            raise BrokerUnavailable(f"stats failed: {result.status}")
        return result.json() or {}

    def health(self) -> bool:
        try:
            return self.http.request("GET", f"{self.base_url}/readyz", timeout=0.7, retry=False).ok
        except HttpUnavailable:
            return False


class PartitionConsumer:
    """One thread per assigned partition, at-least-once processing."""

    def __init__(
        self,
        broker: BrokerClient,
        topic: str,
        group: str,
        partitions: list[int],
        handler: Callable[[Message], None],
        metrics: Metrics,
        max_messages: int = 10,
        poll_interval: float = 0.05,
    ) -> None:
        self.broker = broker
        self.topic = topic
        self.group = group
        self.partitions = partitions
        self.handler = handler
        self.metrics = metrics
        self.max_messages = max_messages
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.processed = 0
        self.failed = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        for partition in self.partitions:
            thread = threading.Thread(target=self._loop, args=(partition,), daemon=True, name=f"consumer-{self.topic}-{partition}")
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=3.0)

    def _loop(self, partition: int) -> None:
        consecutive_errors = 0
        while not self._stop.is_set():
            try:
                messages = self.broker.fetch(self.topic, self.group, partition, self.max_messages)
                consecutive_errors = 0
            except (BrokerUnavailable, HttpUnavailable) as exc:
                consecutive_errors += 1
                self.metrics.inc("consumer_fetch_errors_total", topic=self.topic, group=self.group)
                log.warning("fetch failed", extra=mesh_log.fields(topic=self.topic, partition=partition, error=str(exc)))
                self._stop.wait(min(2.0, 0.1 * (2 ** min(consecutive_errors, 5))))
                continue
            if not messages:
                self._stop.wait(self.poll_interval)
                continue
            for message in messages:
                if self._stop.is_set():
                    return
                started = time.monotonic()
                try:
                    self.handler(message)
                except Exception as exc:  # noqa: BLE001 - any handler failure is a nack
                    self.failed += 1
                    self.metrics.inc("consumer_failures_total", topic=self.topic, group=self.group)
                    try:
                        outcome = self.broker.nack(
                            self.topic, self.group, partition, message.offset, f"{type(exc).__name__}: {exc}"
                        )
                        log.warning(
                            "message nacked",
                            extra=mesh_log.fields(
                                topic=self.topic, offset=message.offset, dead_lettered=outcome.get("dead_lettered")
                            ),
                        )
                    except (BrokerUnavailable, HttpUnavailable):
                        pass  # redelivery happens automatically after the visibility timeout
                else:
                    self.processed += 1
                    self.metrics.inc("consumer_processed_total", topic=self.topic, group=self.group)
                    try:
                        self.broker.ack(self.topic, self.group, partition, message.offset)
                    except (BrokerUnavailable, HttpUnavailable) as exc:
                        log.warning(
                            "ack failed; message will be redelivered",
                            extra=mesh_log.fields(topic=self.topic, offset=message.offset, error=str(exc)),
                        )
                finally:
                    self.metrics.observe(
                        "consumer_processing_seconds",
                        time.monotonic() - started,
                        topic=self.topic,
                        group=self.group,
                    )


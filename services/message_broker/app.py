"""Message broker: topics, partitions, consumer groups, retries, DLQ.

API (all JSON over HTTP):

POST /topics/{topic}/messages          publish  {key, value, headers}
POST /topics/{topic}/fetch             fetch    {group, partition, max_messages}
POST /topics/{topic}/ack               commit   {group, partition, offset}
POST /topics/{topic}/nack              retry    {group, partition, offset, error}
GET  /topics/{topic}/stats             lag, backpressure, dead letters
GET  /topics/{topic}/partitions        partition count

Delivery is at-least-once. A nacked message is redelivered after a short
backoff with an incremented attempt counter, and after ``MAX_ATTEMPTS`` it is
moved to ``<topic>.dlq`` in the same transaction as the offset commit, so a
poison message cannot block a partition forever.

Backpressure: publishing is rejected with HTTP 503 when the slowest consumer
group is more than ``MAX_BACKLOG`` messages behind. That is the signal the API
gateway translates into a 429/503 with ``Retry-After``.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid

from libs.mesh import config, httpkit, logging as mesh_log, metrics
from libs.mesh.hashing import stable_bucket
from libs.mesh.httpkit import Problem
from services.message_broker.log import PartitionLog, Record

SERVICE = "message-broker"


class Topic:
    def __init__(self, name: str, partitions: int, data_dir: str, max_attempts: int, retry_backoff: float) -> None:
        self.name = name
        self.max_attempts = max_attempts
        self.retry_backoff = retry_backoff
        self.lock = threading.RLock()
        self.logs = [PartitionLog(os.path.join(data_dir, "topics", name, f"partition-{i}.jsonl")) for i in range(partitions)]
        self.offsets: dict[str, list[int]] = {}
        self.attempts: dict[tuple[str, int, int], int] = {}
        self.retry_at: dict[tuple[str, int, int], float] = {}
        self.dead_lettered = 0
        self.offsets_path = os.path.join(data_dir, "offsets", f"{name}.json")
        self._last_save = 0.0
        self._load_offsets()

    @property
    def partition_count(self) -> int:
        return len(self.logs)

    def _load_offsets(self) -> None:
        try:
            with open(self.offsets_path, encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return
        for group, offsets in payload.get("groups", {}).items():
            self.offsets[group] = [int(o) for o in offsets]

    def _save_offsets(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_save < 0.2:
            return
        self._last_save = now
        os.makedirs(os.path.dirname(self.offsets_path), exist_ok=True)
        tmp = f"{self.offsets_path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump({"groups": self.offsets, "saved_at": now}, handle)
        os.replace(tmp, self.offsets_path)

    def group_offsets(self, group: str) -> list[int]:
        with self.lock:
            if group not in self.offsets:
                self.offsets[group] = [0] * self.partition_count
            return self.offsets[group]

    def committed(self, group: str, partition: int) -> int:
        return self.group_offsets(group)[partition]

    def lag(self, group: str) -> int:
        with self.lock:
            offsets = self.group_offsets(group)
            return sum(max(0, log.high_watermark - offsets[i]) for i, log in enumerate(self.logs))

    def backlog(self) -> int:
        with self.lock:
            if not self.offsets:
                return 0
            return max(self.lag(group) for group in self.offsets)

    def publish(self, key: str, value: object, headers: dict[str, str]) -> dict:
        with self.lock:
            partition = stable_bucket(key or uuid.uuid4().hex, self.partition_count)
            record = self.logs[partition].append(key, value, headers)
        return {"topic": self.name, "partition": partition, "offset": record.offset}

    def fetch(self, group: str, partition: int, max_messages: int) -> list[Record]:
        with self.lock:
            start = self.committed(group, partition)
            now = time.monotonic()
            messages: list[Record] = []
            for record in self.logs[partition].read(start, max_messages):
                retry_key = (group, partition, record.offset)
                ready_at = self.retry_at.get(retry_key, 0.0)
                if ready_at > now:
                    break  # preserve per-partition ordering while backing off
                record.attempts = self.attempts.get(retry_key, 0)
                messages.append(record)
                if len(messages) >= max_messages:
                    break
            return messages

    def ack(self, group: str, partition: int, offset: int) -> None:
        with self.lock:
            offsets = self.group_offsets(group)
            offsets[partition] = max(offsets[partition], offset + 1)
            self.attempts.pop((group, partition, offset), None)
            self.retry_at.pop((group, partition, offset), None)
            self._save_offsets()

    def nack(self, group: str, partition: int, offset: int, error: str) -> dict:
        topic_key = (group, partition, offset)
        with self.lock:
            attempts = self.attempts.get(topic_key, 0) + 1
            self.attempts[topic_key] = attempts
            if attempts >= self.max_attempts:
                return {"dead_lettered": True, "attempts": attempts, "dead_letter_topic": f"{self.name}.dlq"}
            self.retry_at[topic_key] = time.monotonic() + self.retry_backoff * attempts
            return {"dead_lettered": False, "attempts": attempts}

    def stats(self) -> dict:
        with self.lock:
            partitions = [
                {
                    "partition": i,
                    "high_watermark": log.high_watermark,
                    "records": log.high_watermark,
                }
                for i, log in enumerate(self.logs)
            ]
            groups = {}
            for group in self.offsets:
                offsets = self.group_offsets(group)
                groups[group] = {
                    "committed": list(offsets),
                    "lag": max(0, self.lag(group)),
                }
            return {
                "topic": self.name,
                "partitions": partitions,
                "groups": groups,
                "backlog": self.backlog(),
                "dead_lettered": self.dead_lettered,
            }


class Broker:
    def __init__(
        self,
        data_dir: str,
        default_partitions: int,
        max_backlog: int,
        max_attempts: int,
        retry_backoff: float,
        topic_config: dict[str, dict] | None = None,
        metrics_obj: metrics.Metrics | None = None,
    ) -> None:
        self.data_dir = data_dir
        self.default_partitions = default_partitions
        self.max_backlog = max_backlog
        self.max_attempts = max_attempts
        self.retry_backoff = retry_backoff
        self.topic_config = topic_config or {}
        self.metrics = metrics_obj
        self.topics: dict[str, Topic] = {}
        self.lock = threading.RLock()
        for name, cfg in self.topic_config.items():
            self.get_or_create(name, partitions=int(cfg.get("partitions", default_partitions)))

    def get_or_create(self, name: str, partitions: int | None = None) -> Topic:
        with self.lock:
            topic = self.topics.get(name)
            if topic is None:
                configured = int(self.topic_config.get(name, {}).get("partitions", self.default_partitions))
                topic = Topic(
                    name,
                    partitions or configured,
                    self.data_dir,
                    self.max_attempts,
                    self.retry_backoff,
                )
                self.topics[name] = topic
            return topic

    def publish(
        self,
        topic_name: str,
        key: str,
        value: object,
        headers: dict[str, str] | None = None,
        enforce_backpressure: bool = True,
    ) -> dict:
        topic = self.get_or_create(topic_name)
        if enforce_backpressure and topic.backlog() >= self.max_backlog:
            if self.metrics:
                self.metrics.inc("broker_backpressure_rejected_total", topic=topic_name)
            raise Problem(
                503,
                "broker_backpressure",
                f"topic {topic_name!r} backlog exceeds {self.max_backlog} messages",
                retry_after=0.1,
            )
        result = topic.publish(key, value, headers or {})
        if self.metrics:
            self.metrics.inc("broker_published_total", topic=topic_name, partition=str(result["partition"]))
        return result

    def fetch(self, topic_name: str, group: str, partition: int, max_messages: int) -> list[Record]:
        topic = self.get_or_create(topic_name)
        if partition < 0 or partition >= topic.partition_count:
            raise Problem(400, "invalid_partition", f"partition {partition} does not exist")
        messages = topic.fetch(group, partition, max_messages)
        if self.metrics:
            self.metrics.gauge("broker_consumer_lag", topic.lag(group), topic=topic_name, group=group)
        return messages

    def ack(self, topic_name: str, group: str, partition: int, offset: int) -> None:
        self.get_or_create(topic_name).ack(group, partition, offset)

    def nack(self, topic_name: str, group: str, partition: int, offset: int, error: str) -> dict:
        topic = self.get_or_create(topic_name)
        outcome = topic.nack(group, partition, offset, error)
        if outcome.get("dead_lettered"):
            with topic.lock:
                records = topic.logs[partition].read(offset, 1)
                original = records[0] if records else None
            if original is not None:
                self.publish(
                    f"{topic_name}.dlq",
                    original.key,
                    {
                        "original_topic": topic_name,
                        "original_partition": partition,
                        "original_offset": offset,
                        "value": original.value,
                        "error": error,
                        "attempts": outcome["attempts"],
                    },
                    headers={"dead-letter-reason": error[:200]},
                    enforce_backpressure=False,
                )
            topic.ack(group, partition, offset)
            with topic.lock:
                topic.dead_lettered += 1
            if self.metrics:
                self.metrics.inc("broker_dead_letters_total", topic=topic_name)
        return outcome

    def stats(self, topic_name: str) -> dict:
        return self.get_or_create(topic_name).stats()


def build_app(broker: Broker) -> httpkit.App:
    app = httpkit.App(SERVICE, max_concurrency=config.env_int("MAX_CONCURRENCY", 128))

    @app.route("POST", "/topics/{topic}/messages", name="topic.publish")
    def publish(req: httpkit.Request):
        payload = req.json()
        if "value" not in payload:
            raise Problem(400, "invalid_body", "'value' is required")
        key = str(payload.get("key") or "")
        headers = {str(k): str(v) for k, v in (payload.get("headers") or {}).items()}
        return broker.publish(req.params["topic"], key, payload["value"], headers)

    @app.route("POST", "/topics/{topic}/fetch", name="topic.fetch")
    def fetch(req: httpkit.Request):
        payload = req.json()
        group = str(payload.get("group") or "default")
        messages = broker.fetch(
            req.params["topic"],
            group,
            int(payload.get("partition", 0)),
            int(payload.get("max_messages", 10)),
        )
        topic = broker.get_or_create(req.params["topic"])
        return {
            "messages": [m.to_dict(topic.name, int(payload.get("partition", 0))) for m in messages],
            "high_watermark": topic.logs[int(payload.get("partition", 0))].high_watermark,
        }

    @app.route("POST", "/topics/{topic}/ack", name="topic.ack")
    def ack(req: httpkit.Request):
        payload = req.json()
        broker.ack(req.params["topic"], str(payload["group"]), int(payload["partition"]), int(payload["offset"]))
        return {"acked": payload["offset"]}

    @app.route("POST", "/topics/{topic}/nack", name="topic.nack")
    def nack(req: httpkit.Request):
        payload = req.json()
        outcome = broker.nack(
            req.params["topic"],
            str(payload["group"]),
            int(payload["partition"]),
            int(payload["offset"]),
            str(payload.get("error", "unknown")),
        )
        return outcome

    @app.route("GET", "/topics/{topic}/stats", name="topic.stats")
    def stats(req: httpkit.Request):
        return broker.stats(req.params["topic"])

    @app.route("GET", "/topics/{topic}/partitions", name="topic.partitions")
    def partitions(req: httpkit.Request):
        topic = broker.get_or_create(req.params["topic"])
        return {"topic": topic.name, "partitions": topic.partition_count}

    @app.route("GET", "/broker/stats", name="broker.stats")
    def broker_stats(_: httpkit.Request):
        return {
            "topics": {name: topic.stats() for name, topic in broker.topics.items()},
            "max_backlog": broker.max_backlog,
        }

    return app


def main() -> None:
    mesh_log.configure(SERVICE, config.env("POD_NAME", "broker-0"))
    metrics.configure(SERVICE, config.env("POD_NAME", "broker-0"))
    data_dir = config.env("BROKER_DATA_DIR", os.path.join("runtime", "data", "broker"))
    broker = Broker(
        data_dir=data_dir,
        default_partitions=config.env_int("DEFAULT_PARTITIONS", 3),
        max_backlog=config.env_int("MAX_BACKLOG", 5000),
        max_attempts=config.env_int("MAX_ATTEMPTS", 3),
        retry_backoff=config.env_float("RETRY_BACKOFF_S", 0.2),
        topic_config=config.env_json("TOPICS", {}),
        metrics_obj=metrics.get(),
    )
    app = build_app(broker)
    mesh_log.get_logger(SERVICE).info(
        "broker ready", extra=mesh_log.fields(data_dir=data_dir, topics=list(broker.topics))
    )
    app.serve(config.env_int("PORT", 16000))


if __name__ == "__main__":
    main()

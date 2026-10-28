"""Append-only partition log with offsets persisted to disk.

Layout: ``DATA_DIR/topics/<topic>/partition-<n>.jsonl`` plus
``DATA_DIR/offsets/<topic>.json``. Records are appended and flushed; offsets are
saved on acknowledgement (atomically via rename) so a broker restart resumes
where consumers left off. This is a simplified Kafka log: no segments, no
compaction, no replication - enough to demonstrate partition semantics.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass


@dataclass
class Record:
    offset: int
    key: str
    value: object
    headers: dict[str, str]
    ts: float
    attempts: int = 0

    def to_dict(self, topic: str, partition: int) -> dict:
        return {
            "topic": topic,
            "partition": partition,
            "offset": self.offset,
            "key": self.key,
            "value": self.value,
            "headers": self.headers,
            "ts": self.ts,
            "attempts": self.attempts,
        }


class PartitionLog:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._records: list[Record] = []
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self._records.append(
                    Record(
                        offset=int(payload["offset"]),
                        key=payload.get("key", ""),
                        value=payload.get("value"),
                        headers=payload.get("headers") or {},
                        ts=float(payload.get("ts", time.time())),
                    )
                )

    @property
    def high_watermark(self) -> int:
        with self._lock:
            return len(self._records)

    def append(self, key: str, value: object, headers: dict[str, str]) -> Record:
        with self._lock:
            record = Record(len(self._records), key, value, headers, time.time())
            self._records.append(record)
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record.to_dict("", 0), separators=(",", ":")) + "\n")
                handle.flush()
            return record

    def read(self, start: int, max_messages: int) -> list[Record]:
        with self._lock:
            return list(self._records[start : start + max_messages])

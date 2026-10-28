import json
import os
import shutil
import tempfile
import unittest

from services.message_broker.log import PartitionLog


class PartitionLogTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="broker-log-")
        self.path = os.path.join(self.tmp, "topics", "orders.created", "partition-0.jsonl")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_append_assigns_sequential_offsets(self) -> None:
        log = PartitionLog(self.path)
        records = [log.append(f"key-{i}", {"n": i}, {}) for i in range(3)]
        self.assertEqual([r.offset for r in records], [0, 1, 2])
        self.assertEqual(log.high_watermark, 3)
        self.assertEqual(log.read(1, 2)[0].value, {"n": 1})

    def test_reload_replays_from_disk(self) -> None:
        log = PartitionLog(self.path)
        log.append("a", {"n": 1}, {"h": "v"})
        log.append("b", {"n": 2}, {})
        reopened = PartitionLog(self.path)
        self.assertEqual(reopened.high_watermark, 2)
        first = reopened.read(0, 1)[0]
        self.assertEqual((first.key, first.value, first.headers), ("a", {"n": 1}, {"h": "v"}))

    def test_read_beyond_high_watermark_is_empty(self) -> None:
        log = PartitionLog(self.path)
        log.append("a", {}, {})
        self.assertEqual(log.read(5, 10), [])

    def test_records_are_json_lines_on_disk(self) -> None:
        log = PartitionLog(self.path)
        log.append("key", {"hello": "world"}, {})
        with open(self.path, encoding="utf-8") as handle:
            line = json.loads(handle.readline())
        self.assertEqual(line["key"], "key")
        self.assertEqual(line["value"], {"hello": "world"})
        self.assertEqual(line["offset"], 0)


if __name__ == "__main__":
    unittest.main()

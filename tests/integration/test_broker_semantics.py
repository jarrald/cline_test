import shutil
import tempfile
import time
import unittest

from libs.mesh import metrics
from libs.mesh.broker import BrokerClient, BrokerRejected
from libs.mesh.httpkit import HttpError
from services.message_broker.app import Broker, build_app


class BrokerSemanticsTest(unittest.TestCase):
    """Exercises the broker's API contract, not its internals."""

    @classmethod
    def setUpClass(cls) -> None:
        metrics.configure("broker-it", "pod-0")

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="broker-it-")
        self.broker = Broker(
            data_dir=self.tmp,
            default_partitions=3,
            max_backlog=5,
            max_attempts=3,
            retry_backoff=0.01,
            topic_config={"orders.created": {"partitions": 3}},
            metrics_obj=metrics.get(),
        )
        self.app = build_app(self.broker)
        self.port = self.app.start_in_thread()
        self.client = BrokerClient(f"http://127.0.0.1:{self.port}")

    def tearDown(self) -> None:
        self.app.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_same_key_maps_to_same_partition(self) -> None:
        first = self.client.publish("orders.created", "cust-7", {"n": 1})
        second = self.client.publish("orders.created", "cust-7", {"n": 2})
        other = self.client.publish("orders.created", "cust-8", {"n": 3})
        self.assertEqual(first["partition"], second["partition"])
        self.assertIn(other["partition"], {0, 1, 2})
        self.assertLess(first["offset"], second["offset"])

    def test_fetch_is_ordered_and_ack_advances_offset(self) -> None:
        for i in range(3):
            self.client.publish("orders.created", "cust-1", {"n": i})
        partition = self.client.publish("orders.created", "cust-1", {"n": 3})["partition"]
        messages = self.client.fetch("orders.created", "g1", partition, max_messages=10)
        values = [m.value["n"] for m in messages]
        self.assertEqual(values, sorted(values))
        self.client.ack("orders.created", "g1", partition, messages[-1].offset)
        self.assertEqual(self.client.fetch("orders.created", "g1", partition, 10), [])

    def test_nack_redelivers_with_attempts(self) -> None:
        record = self.client.publish("orders.created", "cust-2", {"boom": True})
        partition = record["partition"]
        [message] = self.client.fetch("orders.created", "g1", partition, 1)
        self.client.nack("orders.created", "g1", partition, message.offset, "handler failed")
        time.sleep(0.05)
        [redelivered] = self.client.fetch("orders.created", "g1", partition, 1)
        self.assertEqual(redelivered.offset, message.offset)
        self.assertEqual(redelivered.attempts, 1)

    def test_dead_letter_after_max_attempts(self) -> None:
        record = self.client.publish("orders.created", "cust-3", {"poison": True})
        partition = record["partition"]
        for _ in range(3):
            messages = self.client.fetch("orders.created", "g1", partition, 1)
            self.assertEqual(len(messages), 1)
            outcome = self.client.nack("orders.created", "g1", partition, messages[0].offset, "poison")
            time.sleep(0.03)
        self.assertTrue(outcome["dead_lettered"])
        stats = self.client.stats("orders.created")
        self.assertEqual(stats["dead_lettered"], 1)
        dlq = self.broker.get_or_create("orders.created.dlq")
        self.assertEqual(sum(log.high_watermark for log in dlq.logs), 1)
        dead = next(log.read(0, 1)[0] for log in dlq.logs if log.high_watermark)
        self.assertEqual(dead.value["original_topic"], "orders.created")
        self.assertEqual(dead.value["attempts"], 3)
        # partition is unblocked: the group offset advanced past the poison message
        self.assertEqual(stats["groups"]["g1"]["committed"][partition], record["offset"] + 1)

    def test_publish_backpressure_when_consumers_lag(self) -> None:
        # create the consumer group offset by fetching once
        self.client.fetch("orders.created", "slow-group", 0, 1)
        accepted = 0
        rejected = None
        for i in range(8):
            try:
                self.client.publish("orders.created", "cust-9", {"n": i})
                accepted += 1
            except BrokerRejected as exc:
                rejected = exc
                break
        self.assertIsNotNone(rejected, "expected the broker to apply backpressure")
        self.assertEqual(rejected.status, 503)
        self.assertEqual(accepted, 5)
        self.assertGreaterEqual(self.broker.stats("orders.created")["backlog"], 5)

    def test_stats_report_partitions_and_lag(self) -> None:
        self.client.publish("orders.created", "cust-4", {})
        self.client.fetch("orders.created", "g1", 0, 10)
        stats = self.client.stats("orders.created")
        self.assertEqual(len(stats["partitions"]), 3)
        self.assertIn("g1", stats["groups"])
        self.assertEqual(self.client.partitions("orders.created"), 3)

    def test_unknown_partition_is_rejected(self) -> None:
        with self.assertRaises(Exception) as ctx:
            self.client.fetch("orders.created", "g1", 9, 1)
        self.assertIn("400", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

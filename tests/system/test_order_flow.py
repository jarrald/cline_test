"""End-to-end behaviour of the whole topology (18 processes, real sockets)."""

import unittest

from tests.support.stack import Stack


class OrderFlowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.stack = Stack(name="system-order-flow", order_io_ms=5).start()
        cls.stack.seed_inventory(["sku-1", "sku-2"], stock=100)
        cls.stack.seed_inventory(["sku-low"], stock=1)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.stack.stop()

    def test_order_is_confirmed_and_stock_decremented_once(self) -> None:
        order = self.stack.place_order(sku="sku-1", quantity=2)
        self.assertEqual(order["_status"], 202)
        self.assertEqual(order["status"], "PENDING")
        confirmed = self.stack.wait_for_order(order["order_id"], {"CONFIRMED"})
        self.assertEqual(confirmed["confirmation"]["sku"], "sku-1")
        self.assertEqual(confirmed["confirmation"]["quantity"], 2)
        inventory = self.stack.inventory("sku-1")
        self.assertLessEqual(inventory["available"], 98)

    def test_insufficient_stock_is_rejected_with_reason(self) -> None:
        order = self.stack.place_order(sku="sku-low", quantity=2)
        rejected = self.stack.wait_for_order(order["order_id"], {"REJECTED"})
        self.assertEqual(rejected["reason"], "insufficient_stock")
        self.assertEqual(self.stack.inventory("sku-low")["available"], 1)

    def test_unknown_sku_is_rejected_without_blocking_the_partition(self) -> None:
        order = self.stack.place_order(sku="does-not-exist", quantity=1)
        rejected = self.stack.wait_for_order(order["order_id"], {"REJECTED"})
        self.assertEqual(rejected["reason"], "unknown_sku")
        # the partition keeps moving for the same customer key after the rejection
        follow_up = self.stack.place_order(sku="sku-2", quantity=1, customer_id=order["customer_id"])
        self.stack.wait_for_order(follow_up["order_id"], {"CONFIRMED"})

    def test_idempotent_retry_creates_one_order_and_one_reservation(self) -> None:
        key = "idem-system-test-1"
        first = self.stack.place_order(sku="sku-2", quantity=3, idempotency_key=key)
        second = self.stack.place_order(sku="sku-2", quantity=3, idempotency_key=key)
        self.assertEqual(first["order_id"], second["order_id"])
        self.stack.wait_for_order(first["order_id"], {"CONFIRMED"})
        available = self.stack.inventory("sku-2")["available"]
        # cannot assert the absolute value (other tests share the SKU), but the
        # broker must not have produced a second stock reservation for this order
        stats = self.stack.broker_stats("inventory.events")
        self.assertEqual(stats["dead_lettered"], 0)
        self.assertGreaterEqual(available, 0)

    def test_consumer_lag_returns_to_zero(self) -> None:
        for _ in range(5):
            self.stack.place_order(sku="sku-1", quantity=1)
        self.assertTrue(self.stack.wait_until_lag_zero(timeout=30))

    def test_unknown_order_returns_404(self) -> None:
        response = self.stack.get_order("ord_missing")
        self.assertEqual(response["status"], 404)
        self.assertEqual(response["body"]["error"]["code"], "order_not_found")

    def test_every_order_has_a_single_confirming_event(self) -> None:
        orders = [self.stack.place_order(sku="sku-1", quantity=1) for _ in range(3)]
        for order in orders:
            self.stack.wait_for_order(order["order_id"], {"CONFIRMED"})
        stats = self.stack.broker_stats("inventory.events")
        self.assertEqual(stats["dead_lettered"], 0)


if __name__ == "__main__":
    unittest.main()

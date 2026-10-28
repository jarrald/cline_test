import unittest

from libs.mesh import broker, metrics
from libs.mesh.storage import DocumentNotFound, StorageUnavailable
from services.inventory_worker.handler import InventoryHandler


class FakeStore:
    def __init__(self) -> None:
        self.result = {"value": {"available": 3}, "outcome": "applied", "cached": False}
        self.error: Exception | None = None
        self.calls: list[dict] = []

    def mutate(self, key: str, op: str, amount: int, idempotency_key: str):
        self.calls.append({"key": key, "op": op, "amount": amount, "idempotency_key": idempotency_key})
        if self.error is not None:
            raise self.error
        return self.result


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[dict] = []

    def publish(self, topic: str, key: str, value: dict, headers: dict | None = None) -> dict:
        self.published.append({"topic": topic, "key": key, "value": value, "headers": headers or {}})
        return {"topic": topic, "partition": 0, "offset": len(self.published)}


def message(order_id: str = "ord_1", sku: str = "sku-1", quantity: int = 2, customer_id: str = "cust-1"):
    return broker.Message(
        topic="orders.created",
        partition=0,
        offset=0,
        key=customer_id,
        value={"order_id": order_id, "customer_id": customer_id, "sku": sku, "quantity": quantity},
    )


class InventoryHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        metrics.configure("worker-it", "worker-0")
        self.store = FakeStore()
        self.broker = FakeBroker()
        self.handler = InventoryHandler(
            store=self.store, broker=self.broker, events_topic="inventory.events", work_ms=0, metrics_obj=metrics.get()
        )

    def test_confirmed_order_decrements_stock_once(self) -> None:
        self.handler.handle(message())
        self.assertEqual(self.store.calls[0]["idempotency_key"], "order:ord_1")
        self.assertEqual(self.store.calls[0]["key"], "inventory:sku-1")
        event = self.broker.published[0]["value"]
        self.assertEqual(event["status"], "CONFIRMED")
        self.assertIsNone(event["reason"])
        self.assertEqual(event["available"], 3)
        self.assertEqual(self.broker.published[0]["topic"], "inventory.events")

    def test_insufficient_stock_rejects_with_reason(self) -> None:
        self.store.result = {"value": {"available": 1}, "outcome": "insufficient", "cached": False}
        self.handler.handle(message(quantity=5))
        event = self.broker.published[0]["value"]
        self.assertEqual(event["status"], "REJECTED")
        self.assertEqual(event["reason"], "insufficient_stock")
        self.assertEqual(event["available"], 1)

    def test_unknown_sku_rejects_without_retry(self) -> None:
        self.store.error = DocumentNotFound("missing")
        self.handler.handle(message(sku="ghost"))
        event = self.broker.published[0]["value"]
        self.assertEqual(event["status"], "REJECTED")
        self.assertEqual(event["reason"], "unknown_sku")

    def test_storage_outage_propagates_so_the_broker_retries(self) -> None:
        self.store.error = StorageUnavailable("shard down")
        with self.assertRaises(StorageUnavailable):
            self.handler.handle(message())
        self.assertEqual(self.broker.published, [])

    def test_duplicate_event_is_acknowledged_and_counted(self) -> None:
        self.store.result = {"value": {"available": 3}, "outcome": "applied", "cached": True}
        self.handler.handle(message())
        counters = metrics.get().snapshot()["counters"]
        self.assertEqual(counters.get("inventory_duplicate_events_total"), 1)
        self.assertEqual(self.broker.published[0]["value"]["status"], "CONFIRMED")

    def test_invalid_payload_raises_for_dead_lettering(self) -> None:
        bad = broker.Message(topic="orders.created", partition=0, offset=1, key="k", value={"order_id": ""})
        with self.assertRaises(ValueError):
            self.handler.handle(bad)


if __name__ == "__main__":
    unittest.main()

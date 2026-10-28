import json
import unittest

from libs.mesh.broker import BrokerRejected
from libs.mesh.httpkit import Problem
from libs.mesh.storage import StorageConflict
from services.order_service.repository import OrderRepository


class FakeStore:
    """In-memory stand-in with the same optimistic-concurrency contract."""

    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}
        self.versions: dict[str, int] = {}

    def put(self, key: str, value, expected_version: int | None = None) -> dict:
        current = self.versions.get(key, 0)
        if expected_version is not None and expected_version != current:
            raise StorageConflict(f"expected {expected_version}, found {current}")
        self.versions[key] = current + 1
        self.docs[key] = json.loads(json.dumps(value))
        return {"key": key, "version": current + 1}

    def get(self, key: str, consistent: bool = False):
        if key not in self.docs:
            return None
        return {"key": key, "value": json.loads(json.dumps(self.docs[key])), "version": self.versions[key]}


class OrderRepositoryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = FakeStore()
        self.published: list[dict] = []
        self.fail_publish = False
        self.repo = OrderRepository(self.store, self.publisher)
        self.payload = {"customer_id": "cust-1", "sku": "sku-1", "quantity": 2}

    def publisher(self, order: dict) -> None:
        if self.fail_publish:
            raise BrokerRejected(503, "backpressure")
        self.published.append(order)

    def test_create_persists_and_publishes_once(self) -> None:
        order = self.repo.create(dict(self.payload), "idem-1")
        self.assertEqual(order["status"], "PENDING")
        self.assertTrue(order["event_published"])
        self.assertEqual(len(self.published), 1)
        stored = self.repo.get(order["order_id"], consistent=True)
        self.assertEqual(stored["customer_id"], "cust-1")

    def test_idempotency_key_returns_same_order_without_republishing(self) -> None:
        first = self.repo.create(dict(self.payload), "idem-1")
        second = self.repo.create(dict(self.payload), "idem-1")
        self.assertEqual(first["order_id"], second["order_id"])
        self.assertEqual(len(self.published), 1)

    def test_different_keys_create_different_orders(self) -> None:
        first = self.repo.create(dict(self.payload), "idem-1")
        second = self.repo.create(dict(self.payload), "idem-2")
        self.assertNotEqual(first["order_id"], second["order_id"])
        self.assertEqual(len(self.published), 2)

    def test_publish_failure_marks_order_and_returns_503(self) -> None:
        self.fail_publish = True
        with self.assertRaises(Problem) as ctx:
            self.repo.create(dict(self.payload), "idem-1")
        self.assertEqual(ctx.exception.status, 503)
        self.assertEqual(ctx.exception.retry_after, 0.25)
        pointer = self.store.get("idem:order:idem-1", consistent=True)
        order = self.repo.get(pointer["value"]["order_id"], consistent=True)
        self.assertEqual(order["status"], "PUBLISH_FAILED")
        self.assertFalse(order["event_published"])

        # the retry with the same idempotency key republishes and restores PENDING
        self.fail_publish = False
        retried = self.repo.create(dict(self.payload), "idem-1")
        self.assertEqual(retried["order_id"], order["order_id"])
        self.assertEqual(retried["status"], "PENDING")
        self.assertEqual(len(self.published), 1)

    def test_validation_rejects_bad_payloads(self) -> None:
        cases = [
            ({"sku": "s", "quantity": 1}, "invalid_customer"),
            ({"customer_id": "c", "quantity": 1}, "invalid_sku"),
            ({"customer_id": "c", "sku": "s", "quantity": 0}, "invalid_quantity"),
            ({"customer_id": "c", "sku": "s", "quantity": "two"}, "invalid_quantity"),
        ]
        for payload, code in cases:
            with self.subTest(code=code):
                with self.assertRaises(Problem) as ctx:
                    self.repo.create(payload, None)
                self.assertEqual(ctx.exception.code, code)
        self.assertEqual(self.published, [])

    def test_update_status_is_idempotent_and_first_terminal_wins(self) -> None:
        order = self.repo.create(dict(self.payload), "idem-1")
        first = self.repo.update_status(order["order_id"], "CONFIRMED", None, {"available": 5})
        self.assertEqual(first["status"], "CONFIRMED")
        second = self.repo.update_status(order["order_id"], "REJECTED", "late", {})
        self.assertEqual(second["status"], "CONFIRMED")

    def test_update_status_unknown_order_returns_none(self) -> None:
        self.assertIsNone(self.repo.update_status("ord_missing", "CONFIRMED", None, {}))


if __name__ == "__main__":
    unittest.main()

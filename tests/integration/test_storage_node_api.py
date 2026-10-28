import shutil
import tempfile
import unittest

from libs.mesh import httpkit, metrics
from services.storage_node.app import SqliteStore, build_app


class StorageNodeApiTest(unittest.TestCase):
    def setUp(self) -> None:
        metrics.configure("storage-node-it", "node-a")
        self.tmp = tempfile.mkdtemp(prefix="storage-node-it-")
        self.app = build_app(SqliteStore(self.tmp, "node-a"), max_concurrency=16)
        self.port = self.app.start_in_thread()
        self.client = httpkit.HttpClient(timeout=2.0)

    def tearDown(self) -> None:
        self.app.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def call(self, method: str, path: str, body=None):
        return self.client.request(method, f"http://127.0.0.1:{self.port}{path}", body=body)

    def test_put_and_get(self) -> None:
        self.assertEqual(self.call("PUT", "/kv/order:1", {"value": {"a": 1}}).status, 200)
        document = self.call("GET", "/kv/order:1").json()
        self.assertEqual(document["value"], {"a": 1})
        self.assertEqual(document["version"], 1)
        self.assertTrue(document["consistent"])

    def test_missing_document_is_404_problem(self) -> None:
        response = self.call("GET", "/kv/nope")
        self.assertEqual(response.status, 404)
        self.assertEqual(response.json()["error"]["code"], "not_found")

    def test_optimistic_concurrency_conflict(self) -> None:
        self.call("PUT", "/kv/order:1", {"value": {"a": 1}, "expected_version": 0})
        conflict = self.call("PUT", "/kv/order:1", {"value": {"a": 2}, "expected_version": 0})
        self.assertEqual(conflict.status, 409)
        self.assertEqual(conflict.json()["error"]["code"], "version_conflict")

    def test_monotonic_write_does_not_regress_replica(self) -> None:
        self.call("PUT", "/kv/k", {"value": {"a": 2}, "version": 2})
        skipped = self.call("PUT", "/kv/k", {"value": {"a": 1}, "version": 1, "monotonic": True}).json()
        self.assertTrue(skipped["skipped"])
        self.assertEqual(self.call("GET", "/kv/k").json()["value"], {"a": 2})

    def test_mutate_is_idempotent_and_returns_cached_result(self) -> None:
        self.call("PUT", "/kv/inventory:sku", {"value": {"available": 5}})
        first = self.call(
            "POST", "/kv/inventory:sku/mutate", {"op": "decrement", "amount": 2, "idempotency_key": "order:1"}
        ).json()
        self.assertEqual(first["value"]["available"], 3)
        self.assertEqual(first["outcome"], "applied")
        replay = self.call(
            "POST", "/kv/inventory:sku/mutate", {"op": "decrement", "amount": 2, "idempotency_key": "order:1"}
        ).json()
        self.assertTrue(replay["cached"])
        self.assertEqual(replay["value"]["available"], 3)

    def test_mutate_rejects_invalid_operation(self) -> None:
        self.call("PUT", "/kv/inventory:sku", {"value": {"available": 5}})
        response = self.call(
            "POST", "/kv/inventory:sku/mutate", {"op": "explode", "amount": 1, "idempotency_key": "x"}
        )
        self.assertEqual(response.status, 400)
        self.assertEqual(response.json()["error"]["code"], "invalid_op")

    def test_delete_removes_document(self) -> None:
        self.call("PUT", "/kv/temp", {"value": 1})
        self.assertEqual(self.call("DELETE", "/kv/temp").status, 200)
        self.assertEqual(self.call("GET", "/kv/temp").status, 404)
        self.assertEqual(self.call("DELETE", "/kv/temp").status, 404)

    def test_stats_and_probes(self) -> None:
        self.call("PUT", "/kv/a", {"value": 1})
        stats = self.call("GET", "/stats").json()
        self.assertEqual(stats["keys"], 1)
        self.assertEqual(stats["node"], "node-a")
        self.assertEqual(self.call("GET", "/readyz").status, 200)
        self.assertIn("mesh_inflight", self.call("GET", "/metrics").body.decode())


if __name__ == "__main__":
    unittest.main()

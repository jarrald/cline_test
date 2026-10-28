import shutil
import tempfile
import unittest

from libs.mesh import metrics
from libs.mesh.storage import (
    DocumentNotFound,
    Shard,
    ShardedStore,
    StorageConflict,
    StorageUnavailable,
)
from services.storage_node.app import SqliteStore, build_app
from tests.support.stack import wait_until


class StorageNode:
    """A real storage-node app in this process on an ephemeral port."""

    def __init__(self, tmpdir: str, name: str) -> None:
        self.name = name
        self.app = build_app(SqliteStore(tmpdir, name), max_concurrency=32)
        self.port = self.app.start_in_thread()
        self.url = f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self.app.stop()


class StorageClientTest(unittest.TestCase):
    def setUp(self) -> None:
        metrics.configure("storage-unit", "pod-0")
        self.tmp = tempfile.mkdtemp(prefix="storage-unit-")
        self.nodes: list[StorageNode] = []

    def tearDown(self) -> None:
        for node in self.nodes:
            node.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def node(self, name: str) -> StorageNode:
        node = StorageNode(self.tmp, name)
        self.nodes.append(node)
        return node

    def test_put_and_get_roundtrip(self) -> None:
        primary = self.node("primary")
        store = ShardedStore([Shard("shard-0", primary.url)], enable_failover=False)
        store.put("order:1", {"id": 1})
        self.assertEqual(store.get("order:1")["value"], {"id": 1})
        self.assertIsNone(store.get("order:404"))

    def test_optimistic_version_conflict(self) -> None:
        primary = self.node("primary")
        store = ShardedStore([Shard("shard-0", primary.url)], enable_failover=False)
        store.put("order:1", {"v": 1}, expected_version=0)
        with self.assertRaises(StorageConflict):
            store.put("order:1", {"v": 2}, expected_version=0)

    def test_mutate_is_exactly_once_per_idempotency_key(self) -> None:
        primary = self.node("primary")
        store = ShardedStore([Shard("shard-0", primary.url)], enable_failover=False)
        store.put("inventory:sku-1", {"available": 10}, expected_version=0)
        first = store.mutate("inventory:sku-1", "decrement", 3, "order:a")
        self.assertEqual((first["outcome"], first["value"]["available"]), ("applied", 7))
        replay = store.mutate("inventory:sku-1", "decrement", 3, "order:a")
        self.assertTrue(replay["cached"])
        self.assertEqual(replay["value"]["available"], 7)
        third = store.mutate("inventory:sku-1", "decrement", 3, "order:b")
        self.assertEqual(third["value"]["available"], 4)

    def test_insufficient_stock_does_not_go_negative(self) -> None:
        primary = self.node("primary")
        store = ShardedStore([Shard("shard-0", primary.url)], enable_failover=False)
        store.put("inventory:sku-1", {"available": 2}, expected_version=0)
        result = store.mutate("inventory:sku-1", "decrement", 5, "order:c")
        self.assertEqual(result["outcome"], "insufficient")
        self.assertEqual(result["value"]["available"], 2)

    def test_missing_document_raises_not_found(self) -> None:
        primary = self.node("primary")
        store = ShardedStore([Shard("shard-0", primary.url)], enable_failover=False)
        with self.assertRaises(DocumentNotFound):
            store.mutate("inventory:ghost", "decrement", 1, "order:d")

    def test_replication_converges_on_replica(self) -> None:
        primary = self.node("primary")
        replica = self.node("replica")
        store = ShardedStore([Shard("shard-0", primary.url, [replica.url])], enable_failover=False)
        try:
            store.put("order:9", {"value": 9})
            document = wait_until(
                lambda: (store.client.get(replica.url, "order:9") if self._exists(replica.url, "order:9") else None),
                timeout=5,
                description="replica convergence",
            )
            self.assertEqual(document["value"], {"value": 9})
        finally:
            store.close()

    def _exists(self, url: str, key: str) -> bool:
        try:
            store_client = ShardedStore([Shard("probe", url)], enable_failover=False)
            try:
                return store_client.get(key) is not None
            finally:
                store_client.close()
        except StorageUnavailable:
            return False

    def test_reads_fall_back_to_primary_when_replica_is_down(self) -> None:
        primary = self.node("primary")
        replica = self.node("replica")
        store = ShardedStore([Shard("shard-0", primary.url, [replica.url])], enable_failover=False)
        store.put("order:11", {"value": 11})
        replica.stop()
        self.nodes.remove(replica)
        self.assertEqual(store.get("order:11")["value"], {"value": 11})
        store.close()

    def test_primary_failover_promotes_replica(self) -> None:
        primary = self.node("primary")
        replica = self.node("replica")
        store = ShardedStore(
            [Shard("shard-0", primary.url, [replica.url])],
            failure_threshold=1,
            enable_failover=True,
        )
        store.put("order:21", {"value": 21})
        wait_until(
            lambda: self._exists(replica.url, "order:21"),
            timeout=5,
            description="replication before failover",
        )
        primary.stop()
        self.nodes.remove(primary)
        result = store.put("order:22", {"value": 22}, expected_version=0)
        self.assertEqual(result["node"], "replica")
        self.assertTrue(any(event["event"] == "primary_failover" for event in store.events()))
        self.assertEqual(store.get("order:22", consistent=True)["value"], {"value": 22})
        store.close()

    def test_all_nodes_down_raises_unavailable(self) -> None:
        primary = self.node("primary")
        store = ShardedStore([Shard("shard-0", primary.url)], enable_failover=False)
        primary.stop()
        self.nodes.remove(primary)
        with self.assertRaises(StorageUnavailable):
            store.put("order:1", {"value": 1})
        store.close()


if __name__ == "__main__":
    unittest.main()

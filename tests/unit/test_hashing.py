import unittest

from libs.mesh.hashing import ConsistentHashRing, ShardRouter, stable_bucket


class ConsistentHashRingTest(unittest.TestCase):
    def test_ring_is_deterministic(self) -> None:
        first = ConsistentHashRing(["shard-0", "shard-1", "shard-2"])
        second = ConsistentHashRing(["shard-0", "shard-1", "shard-2"])
        for key in [f"customer-{i}" for i in range(200)]:
            self.assertEqual(first.get(key), second.get(key))

    def test_distribution_is_balanced_across_shards(self) -> None:
        ring = ConsistentHashRing(["shard-0", "shard-1", "shard-2"])
        distribution = ring.distribution([f"customer-{i}" for i in range(3000)])
        self.assertEqual(set(distribution), {"shard-0", "shard-1", "shard-2"})
        for shard, count in distribution.items():
            self.assertGreater(count, 700, f"{shard} got only {count}")
            self.assertLess(count, 1300, f"{shard} got {count}")

    def test_removing_a_shard_only_moves_its_keys(self) -> None:
        nodes = ["shard-0", "shard-1", "shard-2"]
        ring = ConsistentHashRing(nodes)
        keys = [f"customer-{i}" for i in range(500)]
        before = {key: ring.get(key) for key in keys}
        ring.remove("shard-2")
        for key, shard in before.items():
            if shard != "shard-2":
                self.assertEqual(ring.get(key), shard)
            else:
                self.assertIn(ring.get(key), {"shard-0", "shard-1"})

    def test_empty_ring_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ConsistentHashRing([])


class ShardRouterTest(unittest.TestCase):
    def test_router_maps_to_known_shard(self) -> None:
        router = ShardRouter(["a", "b", "c"])
        self.assertIn(router.shard_for("inventory:sku-1"), {"a", "b", "c"})


class PartitionTest(unittest.TestCase):
    def test_stable_bucket_is_stable_and_bounded(self) -> None:
        self.assertEqual(stable_bucket("cust-1", 3), stable_bucket("cust-1", 3))
        self.assertTrue(all(0 <= stable_bucket(f"k{i}", 3) < 3 for i in range(100)))
        with self.assertRaises(ValueError):
            stable_bucket("k", 0)


if __name__ == "__main__":
    unittest.main()

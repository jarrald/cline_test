"""Consistent hashing used to place documents on database shards.

The ring is what lets the demo shard by ``customer_id`` (orders) and ``sku``
(inventory) without a central lookup table. Virtual nodes smooth the
distribution so three shards get roughly a third of the keys each.
"""

from __future__ import annotations

import hashlib
from typing import Iterable


def _hash(key: str) -> int:
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


class ConsistentHashRing:
    def __init__(self, nodes: Iterable[str], virtual_nodes: int = 160) -> None:
        if not nodes:
            raise ValueError("ring requires at least one node")
        self.virtual_nodes = virtual_nodes
        self._ring: list[tuple[int, str]] = []
        self._nodes: set[str] = set()
        for node in nodes:
            self.add(node)

    def add(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.virtual_nodes):
            self._ring.append((_hash(f"{node}#{i}"), node))
        self._ring.sort(key=lambda item: item[0])

    def remove(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        self._ring = [(h, n) for h, n in self._ring if n != node]

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def get(self, key: str) -> str:
        if not self._ring:
            raise ValueError("empty ring")
        point = _hash(key)
        lo, hi = 0, len(self._ring)
        while lo < hi:
            mid = (lo + hi) // 2
            if self._ring[mid][0] < point:
                lo = mid + 1
            else:
                hi = mid
        return self._ring[lo % len(self._ring)][1]

    def distribution(self, keys: Iterable[str]) -> dict[str, int]:
        counts = {node: 0 for node in self._nodes}
        for key in keys:
            counts[self.get(key)] += 1
        return counts


class ShardRouter:
    """Maps a business key to a shard name using the hash ring."""

    def __init__(self, shard_names: Iterable[str], virtual_nodes: int = 160) -> None:
        self.ring = ConsistentHashRing(shard_names, virtual_nodes)

    def shard_for(self, key: str) -> str:
        return self.ring.get(key)


def stable_bucket(key: str, buckets: int) -> int:
    """Deterministic partition assignment for broker messages."""
    if buckets <= 0:
        raise ValueError("buckets must be > 0")
    return _hash(key) % buckets

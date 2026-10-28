"""Seed inventory documents into the correct shards (consistent hashing).

Usage:
    python -m scripts.seed_inventory --base-port 21000 --skus 5 --stock 1000
    python -m scripts.seed_inventory --shards-json "[...]"
"""

from __future__ import annotations

import argparse
import json

from libs.mesh.storage import Shard, ShardedStore
from services.kpilot.spec import shard_topology


def main() -> None:
    parser = argparse.ArgumentParser(description="seed inventory across shards")
    parser.add_argument("--base-port", type=int, default=21000)
    parser.add_argument("--shards-json", default=None)
    parser.add_argument("--skus", type=int, default=5)
    parser.add_argument("--sku-prefix", default="sku-")
    parser.add_argument("--stock", type=int, default=1000)
    args = parser.parse_args()

    if args.shards_json:
        shards = [Shard(name=s["name"], primary=s["primary"], replicas=s.get("replicas", [])) for s in json.loads(args.shards_json)]
    else:
        shards = [
            Shard(name=s["name"], primary=s["primary"], replicas=s["replicas"])
            for s in shard_topology(args.base_port)
        ]
    store = ShardedStore(shards, enable_failover=False)
    placement: dict[str, str] = {}
    for i in range(1, args.skus + 1):
        sku = f"{args.sku_prefix}{i}"
        key = f"inventory:{sku}"
        shard = store.shard_for(key)
        store.put(key, {"sku": sku, "available": args.stock, "reserved": 0}, expected_version=0)
        placement[sku] = shard.name
    print(json.dumps({"seeded": placement, "stock": args.stock}, indent=2))
    store.close()


if __name__ == "__main__":
    main()

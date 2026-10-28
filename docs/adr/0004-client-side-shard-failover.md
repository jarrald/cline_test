# ADR 0004 - Client-side sharding and replica promotion

**Status:** accepted (with known limits)

## Context

The demo needs visible sharding and read replicas without operating a consensus
system. Two independent SQLite databases per shard (primary + replica) are easy
to run and reason about.

## Decision

* Placement uses a consistent hash ring (160 virtual nodes per shard) over
  business keys: orders by `customer_id`, inventory by `sku`.
* Writes go to the shard primary; successful writes are replicated asynchronously
  to replicas with monotonic version guards (an older write can never overwrite
  a newer one).
* Reads prefer replicas (round robin) and fall through to the primary; a caller
  can request `consistent=true` for read-your-write semantics.
* After `STORAGE_FAILURE_THRESHOLD` consecutive primary failures the client
  promotes the first healthy replica and records a `primary_failover` event.

## Consequences

* Read scaling and shard isolation are demonstrated with real processes/files.
* Replicas can serve stale data - made explicit via the `consistent` flag and
  measured through replication metrics.
* **No consensus:** two clients could promote different replicas under a network
  partition (split-brain). Production would use a controller (Patroni,
  CloudNativePG) or quorum writes; SQLite's single-writer model is the demo's
  declared ceiling.
* Rebalancing the ring (adding a shard) requires a migration/backfill job; not
  implemented here.

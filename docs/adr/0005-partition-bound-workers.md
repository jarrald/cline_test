# ADR 0005 - Worker tiers are partition-bound

**Status:** accepted

## Context

Autoscaling queue workers naively (more replicas = more throughput) breaks
ordering and correctness: two consumers processing the same partition race on
stock and offsets.

## Decision

* `orders.created` and `inventory.events` have 3 partitions each.
* Workers use static assignment `partition % POD_COUNT == POD_ORDINAL` and run
exactly 3 replicas (one per partition).
* The autoscaler (kpilot HPA) only targets stateless HTTP tiers
  (`order-service`, `api-gateway`). Kubernetes manifests keep worker replicas
  equal to the partition count; scaling them requires increasing partitions and
  a KEDA-style scaler, which is out of scope and documented.

## Consequences

* Per-customer ordering is preserved (messages are keyed by `customer_id`).
* Worst-case worker parallelism is the partition count; the demo shows the
  correct trade-off instead of a misleading "scale to 100 workers" story.
* A crashed worker's partitions are resumed from committed offsets on restart
  (at-least-once). Live rebalancing (Kafka's group protocol) is not implemented;
  duplicates during reassignment are safe due to idempotent handlers.

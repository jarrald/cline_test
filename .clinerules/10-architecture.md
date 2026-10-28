# Architecture Rules

## Request path (synchronous)

```
client -> edge-lb (L4) -> api-gateway (auth, quota, retry, breaker)
       -> order-service (stateless replicas) -> sharded storage (primary/replicas)
```

## Event path (asynchronous)

```
order-service --publish--> orders.created (3 partitions, key=customer_id)
  --> inventory-worker (consumer group, partition-locked replicas)
  --publish--> inventory.events --> status-projector (CQRS) --> orders read model
```

## Hard rules

1. **Services never share a database.** Each shard is owned by exactly one
   storage-node. Access is through `mesh.storage.ShardedStore`.
2. **The order service is stateless.** No sticky sessions, no local caches that
   correctness depends on. Any replica must serve any request.
3. **Writes are idempotent.** HTTP writes require `Idempotency-Key`; consumer
   handlers pass a deterministic key (`order:<id>`) to `mutate`.
4. **Workers are partition-bound.** Never autoscale a consumer group beyond its
   partition count; document the assignment (`POD_ORDINAL % POD_COUNT`).
5. **Timeout every hop.** HTTP clients: 2s default, probes: <1s. Never block a
   request thread without a deadline.
6. **Backpressure, not unbounded queues.** Bounded concurrency in `httpkit`
   (503 + `Retry-After`), broker backlog limit, replica ejection in discovery.
7. **Fail fast and loudly.** Circuit breakers per upstream endpoint; errors use
   problem documents `{error: {code, message, request_id}}`.
8. **Measure before claiming.** New behaviour needs a metric, a log field, or a
   test that can observe it.

## Patterns in use (and where)

| Pattern | Implementation |
|---|---|
| API Gateway | `services/api_gateway` |
| Client-side discovery + round robin | `mesh.discovery.EndpointPool` |
| Circuit breaker | `mesh.resilience.CircuitBreaker` |
| Token bucket quota | `mesh.resilience.TokenBucket` |
| Consistent hashing | `mesh.hashing.ConsistentHashRing` |
| Sharding + read replicas | `mesh.storage.ShardedStore` |
| Transactional idempotency | `storage_node` `mutate` (SQLite transaction) |
| Competing consumers / consumer groups | `message_broker` + `mesh.broker.PartitionConsumer` |
| Dead-letter queue | `*.dlq` topics after `MAX_ATTEMPTS` |
| CQRS projection | `services/status_projector` |
| Reconciliation loop / HPA | `services/kpilot/cluster.py` |
| Health/readiness probes | `httpkit` `/healthz`, `/readyz` |

See `docs/adr/` for the trade-offs (SQLite shards, client-side failover,
at-least-once + idempotency instead of distributed transactions).

# Test Strategy

## Principles

1. **Standard tooling, zero hidden magic.** The suite uses Python's `unittest`
   (stdlib) and `unittest discover`. Results are also written as **JUnit XML**
   (the format every CI system understands) by `scripts/run_tests.py`, which is a
   thin `unittest.TextTestRunner` subclass - no third-party runner required.
2. **Test behaviour, not implementation.** Unit tests target pure functions and
   policy objects; integration tests speak HTTP to real service apps; system
   tests treat the cluster as a black box.
3. **Determinism.** Fixed hash seeds, ephemeral port allocation, injectable
   clocks for timing logic, bounded polling instead of arbitrary `sleep`.
4. **Every reliability claim has an executable test.** If the README says "the
   HPA scales out under load", a test asserts the scale-out event.

## The pyramid

| Layer | Location | Speed | What is real |
|---|---|---|---|
| Unit | `tests/unit` | ms | pure logic, temp SQLite files |
| Integration | `tests/integration` | ~1s/test | real HTTP servers on ephemeral ports, real broker/gateway code |
| System | `tests/system` | ~15s/class | full topology as OS processes (kpilot), edge LB included |
| Reliability | `tests/reliability` | 20-60s/test | full topology + load + chaos injection |
| Static | `make lint` | ms | `compileall`, import graph, manifest validation |

## Planned inventory

### Unit (`tests/unit`)

| File | Covers |
|---|---|
| `test_hashing.py` | ring stability, distribution within tolerance, shard routing |
| `test_resilience.py` | token bucket refill/throttle, breaker state machine, retry backoff |
| `test_httpkit.py` | routing/templating, 404, problem documents, admission control (503), `Content-Length` correctness, keep-alive client reuse |
| `test_config.py` | required env errors, parsing, pod identity |
| `test_broker_log.py` | append/read offsets, restart replay |
| `test_storage_client.py` | sharding, replication convergence, exactly-once mutate, primary failover |
| `test_order_repository.py` | validation, idempotency, publish failure path, status projection idempotence |

### Integration (`tests/integration`)

| File | Covers |
|---|---|
| `test_broker_semantics.py` | partition by key, consumer group offsets, nack/retry, DLQ, publish backpressure |
| `test_gateway_policies.py` | 401 auth, 429 rate limit, retry on 5xx, breaker opening, round-robin spread, header propagation |
| `test_storage_node_api.py` | optimistic version conflicts, conditional write, idempotent mutate, stats |
| `test_consumer_worker.py` | worker handler: confirm/reject/unknown sku, duplicate event safety, nack on storage outage |

### System (`tests/system`)

| File | Covers |
|---|---|
| `test_order_flow.py` | order through edge LB -> gateway -> order service -> broker -> worker -> projector -> CONFIRMED; stock decremented once |
| `test_rejection_flow.py` | insufficient stock -> REJECTED with reason; unknown SKU -> REJECTED |
| `test_sharding.py` | orders and inventory land on all 3 shards; replicas converge |
| `test_gateway_contract.py` | 401, 404, 429, idempotent retry, `X-Upstream-Pod`/`X-Gateway-Pod` attribution, edge LB spreads across gateway replicas |
| `test_observability.py` | Prometheus exposition parses, business counters present, `/metrics.json` shape used by the HPA |

### Reliability (`tests/reliability`)

| File | Experiment | Assertion |
|---|---|---|
| `test_autoscaling.py` | 100 rps, 120 ms service time, target inflight 2.8/pod | `hpa_scale_up` event, replicas > 2, p95 bounded, success >= 99%; later scale-down |
| `test_pod_failure.py` | SIGKILL an order-service replica during load | error budget respected, replacement pod becomes ready, throughput recovers |
| `test_consumer_crash.py` | SIGKILL inventory-worker pod mid-stream | lag returns to 0, every accepted order eventually CONFIRMED, no message loss |
| `test_database_failover.py` | SIGKILL a shard primary | replica promoted, writes resume, no committed data lost |
| `test_backpressure.py` | stop workers, exceed `MAX_BACKLOG` | publisher gets 503 + `Retry-After`, no crash; recovery once workers return |

## Harness design

`tests/support/stack.py` starts a complete cluster:

* picks a **free block of 100 ports** (probes each port with a bind check)
* writes a per-test runtime dir under `runtime/tests/<name>-<uuid>`
* starts `kpilot`, polls `/readyz` until all pods are ready (bounded timeout)
* exposes helpers: `place_order`, `wait_for_status`, `seed_inventory`,
  `kill_pod`, `scale`, `events`, `metrics`
* **always** tears down: POST `/shutdown`, SIGTERM, then SIGKILL + a pid sweep so
  a failing assertion cannot leak processes into the next test

## Coverage matrix (requirements -> tests)

| Requirement | Test |
|---|---|
| Services communicate via a broker | `test_order_flow`, `test_broker_semantics` |
| API gateway with auth, rate limits | `test_gateway_policies`, `test_gateway_contract` |
| Replicated services behind a load balancer | `test_gateway_policies` (RR), `test_pod_failure`, `test_gateway_contract` (pods) |
| Sharded databases + replicas | `test_hashing`, `test_storage_client`, `test_sharding`, `test_database_failover` |
| Autoscaling on overload | `test_autoscaling` |
| Reliability under failure | all `tests/reliability` |

## Running

```bash
make test              # unit + integration (fast, no cluster)
make system-test       # full cluster, functional behaviour
make reliability       # chaos + autoscaling + load, writes docs/results/
make test-all          # everything, CI-style, JUnit XML in runtime/test-reports/
```

Exit codes are meaningful: `scripts/run_tests.py` returns non-zero on any failure
and writes `junit.xml` plus `summary.md` for the CI artefact upload.

## Non-goals

* No mocking of the database with in-memory dicts in system tests - sharding is
  only meaningful against real files.
* Reliability tests are not benchmarks; they assert *behaviour* (scaled, healed,
  no loss) with generous latency budgets to stay stable on shared CI runners.

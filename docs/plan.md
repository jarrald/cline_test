# Delivery Plan (agile, iteration by iteration)

This repository is built the way the system it demonstrates should be: in small,
reviewable increments, each with an explicit goal, acceptance criteria and
verifiable evidence. Every iteration ends on a green test run and a commit - no
iteration is "done" because the code compiles.

Legend: `[x]` shipped, `[~]` in progress, `[ ]` planned.

---

## Iteration 0 - Foundation  `[x]`

**Goal:** a dependency-free platform library the services share, so business code
never reinvents transport, config, logging or metrics.

* `libs/mesh/config.py` - 12-factor configuration (env only)
* `libs/mesh/logging.py` - structured JSON logs with request/trace correlation
* `libs/mesh/metrics.py` + `metrics_prometheus.py` - Prometheus text + JSON snapshot
* `libs/mesh/httpkit.py` - routing, bounded concurrency, graceful shutdown, keep-alive client

**Acceptance:** `python -m compileall` clean; health/readiness/metrics endpoints on
every service; no third-party imports (PyPI is unavailable in the target sandbox).

**Evidence:** unit tests (`tests/unit/test_httpkit.py`, `test_resilience.py`).

---

## Iteration 1 - Storage tier (sharding + replicas)  `[x]`

**Goal:** show real database sharding and read replicas without hiding the
trade-offs.

* `services/storage_node` - one SQLite database per shard, WAL mode, optimistic
  concurrency, transactional idempotent `mutate`
* `libs/mesh/hashing.py` - consistent hash ring (160 vnodes) for key placement
* `libs/mesh/storage.py` - sharded client, async replication, primary failover

**Acceptance:** keys spread within +/-10% across 3 shards; a duplicated mutate is
applied exactly once; reads survive a dead replica; writes survive a dead primary
after promotion.

**Evidence:** `tests/unit/test_hashing.py`, `tests/unit/test_storage_client.py`,
`tests/reliability/test_database_failover.py`.

---

## Iteration 2 - Event backbone and business services  `[x]`

**Goal:** asynchronous, at-least-once communication between independently
deployable services.

* `services/message_broker` - topics, 3 partitions, consumer groups, retries,
dead-letter topic, publish backpressure (503)
* `services/order_service` - stateless write API, idempotent create, event publish
* `services/inventory_worker` - consumer-group member, exactly-once stock decrement
* `services/status_projector` - CQRS projection (PENDING -> CONFIRMED/REJECTED)
* `services/api_gateway` - auth, rate limit, routing, retries, circuit breaker
* `services/edge_lb` - L4 load balancer in front of gateway replicas

**Acceptance:** an order placed through the edge LB reaches a terminal state and
decrements stock once; consumer lag returns to zero; a poison message lands in the
DLQ instead of blocking a partition permanently.

**Evidence:** `tests/integration/test_broker_semantics.py`,
`tests/integration/test_gateway_policies.py`, `tests/system/test_order_flow.py`.

---

## Iteration 3 - Control plane and autoscaling  `[x]`

**Goal:** Kubernetes-style reconciliation and HPA behaviour that can be observed
locally, plus real manifests for a real cluster.

* `services/kpilot` - Deployments, readiness probes, Service registry (Endpoints),
  HPA loop (inflight metric), CrashLoopBackOff, chaos kill API
* replica sets for every tier; gateway HA; worker tiers pinned to partitions

**Acceptance:** cluster converges to all-ready from cold start in <30s; killing a
pod produces a replacement automatically; load above target inflight triggers
`hpa_scale_up` with replicas > initial.

**Evidence:** `tests/reliability/test_autoscaling.py`,
`tests/reliability/test_pod_failure.py`, `docs/results/`.

---

## Iteration 4 - Test suites  `[~]`

**Goal:** a standard, layered test suite with machine-readable output.

* `[x]` unit layer (pure logic, milliseconds)
* `[x]` integration layer (real HTTP servers in-process)
* `[x]` system layer (whole cluster as OS processes via kpilot)
* `[x]` reliability layer (autoscaling, chaos, backpressure, failover)
* `[x]` JUnit XML + markdown evidence via `scripts/run_tests.py`

**Acceptance:** `make test` green; `make system-test` green; `make reliability`
green and writes evidence tables under `docs/results/`.

See [testing.md](testing.md) for the strategy and the coverage matrix.

---

## Iteration 5 - Deployment assets  `[ ]`

**Goal:** the same topology expressed for real infrastructure.

* Dockerfiles per service (non-root, healthcheck, slim base)
* `deploy/compose/docker-compose.yml` for local development
* `deploy/k8s`: Namespace, ConfigMap, Secret, Deployments with probes and
  resource requests/limits, Services, Ingress, HPA v2, PodDisruptionBudget,
  NetworkPolicy, storage StatefulSet with PVCs
* `deploy/observability`: Prometheus scrape config + Grafana dashboard JSON
* `scripts/validate_manifests.py` (no PyYAML in the sandbox)

**Acceptance:** manifests validate; `kubectl apply -k deploy/k8s` is documented;
all probes/metrics names match what the services expose.

---

## Iteration 6 - Load and stress validation  `[ ]`

**Goal:** quantify behaviour, not anecdotes.

* k6 script (`deploy/load/k6.js`) for standard tooling + built-in generator
* scenarios: baseline, ramp, spike, soak, dependency failure, DB failover
* results recorded in `docs/results/` with SLO pass/fail

See [stress-testing.md](stress-testing.md).

**Acceptance:** baseline meets p95 < 500 ms and >= 99% success; spike proves
scale-out; soak shows no memory/fd leak over the run window.

---

## Iteration 7 - Hardening and CI  `[ ]`

* GitHub Actions: unit + integration on every push, system + reliability nightly
* runbooks, ADRs, dashboards screenshots/ASCII, final README polish
* known limitations honestly documented (SQLite single-writer, client-side
  failover without consensus, static consumer partition assignment)

---

## Working agreement

1. One concern per commit; message format `type(scope): summary`
   (`feat`, `fix`, `test`, `docs`, `chore`, `perf`).
2. Tests precede or accompany behaviour changes; a bug fix names the failing case.
3. Never commit `runtime/` artefacts; evidence goes to `docs/results/`.
4. Update this plan and the README when scope changes - the docs are the contract.

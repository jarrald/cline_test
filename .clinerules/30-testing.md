# Testing Rules

Full strategy: `docs/testing.md`. Stress plan: `docs/stress-testing.md`.

## Layer selection

| You changed... | Add/extend |
|---|---|
| pure logic (hashing, breaker, bucket, repository) | `tests/unit` |
| HTTP semantics of a service app | `tests/integration` (real servers, ephemeral ports) |
| cross-service flow or topology | `tests/system` (kpilot cluster) |
| failure/overload behaviour | `tests/reliability` (+ evidence in `docs/results/`) |
| manifests/Dockerfile | `scripts/validate_manifests.py` + docs |

## Rules

1. **Standard tooling only:** `unittest`, `unittest discover`, JUnit XML via
   `scripts/run_tests.py`. Do not add pytest.
2. **No fixed ports.** Bind port 0 or use `tests/support/stack.py` free-block
   allocation; tests must run in parallel with a dev cluster.
3. **No unbounded sleeps.** Poll with a deadline (`wait_until(predicate, timeout)`);
   fail with the last observed state in the message.
4. **Always tear down.** Use the `Stack` context manager; never leave kpilot or
   pods running after a failure.
5. **Assert observable behaviour**, not private fields: HTTP responses, kpilot
   events, broker stats, storage documents, Prometheus counters.
6. **Reliability tests must be honest about budgets.** Assert scale-out happened
   and error budget respected; do not assert exact replica counts at exact times.
7. **Test data is unique per test** (`cust_<uuid>`, fresh SKUs) so tests do not
   interfere through shared shards.
8. **A bug fix starts with a failing test** named after the scenario, e.g.
   `test_duplicate_event_does_not_double_decrement`.

## Evidence

Reliability/system tests append a markdown table to `docs/results/<suite>.md`:
scenario, observed replicas/events, latency, pass/fail. Generated files are
committed so reviewers can see actual runs, not claims.

# ADR 0002 - The API gateway owns edge concerns

**Status:** accepted

## Context

Every service needs request ids and metrics, but authentication, quotas, retries
and circuit breaking are cross-cutting. Duplicating them in each service creates
inconsistent policy and couples business code to transport concerns.

## Decision

`api-gateway` owns authentication (`X-API-Key`), per-key token-bucket rate
limiting, routing/path rewriting, retry policy, circuit breaking per upstream
replica, request-id propagation and edge metrics. Services expose plain internal
HTTP APIs and trust the network boundary; they still implement their own
idempotency, validation and timeouts because those are correctness concerns.

## Consequences

* Policy changes ship independently of business services.
* The gateway becomes a critical hop: it is stateless, replicated (2+ pods),
  autoscaled, and protected by its own admission control; `edge-lb`/Ingress
  spreads traffic across gateway replicas.
* Retrying POSTs is only safe with an idempotency key; the gateway enforces that
  rule (no retry without `Idempotency-Key`).

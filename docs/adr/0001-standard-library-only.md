# ADR 0001 - Standard library only

**Status:** accepted (constraint-driven)

## Context

The demonstration must run in environments without Docker, Kubernetes, a package
proxy, or reliable internet. The sandbox this repo was developed in has Python
3.11 only; PyPI TLS is unreachable. Reviewers should be able to clone and run
without setup.

## Decision

Implement the platform layer (`libs/mesh`) with the Python standard library:
`http.server` for services, `http.client` for clients, `sqlite3` for storage,
`threading` for concurrency, `json`/`logging` for observability. No third-party
runtime dependencies; `unittest` for tests.

## Consequences

* Good: zero install, fast start, reviewable primitives, deterministic CI.
* Good: the interesting architecture (gateway, broker, HPA, sharding) is explicit
  rather than hidden behind a framework.
* Bad: no HTTP/2, TLS termination, ASGI, or production-grade connection pooling.
  These are delegated to the Ingress/load balancer in the Kubernetes deployment,
  which is where they belong in a real system.
* Bad: thread-per-connection limits throughput versus an event loop. The demo
  deliberately uses that limit to make overload behaviour (503 + `Retry-After`)
  observable.

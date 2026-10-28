# Project Context

`shop-mesh` demonstrates large-system engineering: microservices, a message
broker, an API gateway, replicated services, a sharded + replicated database
tier, load balancers, and Kubernetes-style autoscaling - all runnable and
testable locally.

## Non-negotiable constraints

1. **Standard library only.** PyPI is unreachable in the target environment.
   Never add `fastapi`, `flask`, `kafka-python`, `sqlalchemy`, `pytest`, etc.
   Use `libs/mesh` for HTTP, metrics, logging, discovery, storage and broker
   clients.
2. **Everything must run with `python3.11` alone.** If a feature cannot be
   demonstrated without Docker/Kubernetes, it must still have a local execution
   path (`services/kpilot`), plus real manifests under `deploy/k8s`.
3. **No hidden state.** Configuration comes from environment variables only.
   Runtime data belongs under `runtime/` (git-ignored).
4. **Two ways to run every tier:** local cluster (kpilot) and Kubernetes
   manifests. Keep them in sync when ports, env vars or probes change.

## Vocabulary

* **tier** - storage, broker, service (HTTP), worker (queue), edge (LB/gateway)
* **shard** - one SQLite database owned by one storage-node process
* **replica** - a storage node fed asynchronously by its shard primary
* **pod** - one OS process started by kpilot (or a container in Kubernetes)
* **kpilot** - the local control plane (Deployments/Service/HPA/chaos)

## Where things live

| Path | Purpose |
|---|---|
| `libs/mesh/` | shared platform library (no business logic) |
| `services/<name>/` | one deployable per directory, entrypoint `app.py` |
| `tests/{unit,integration,system,reliability}` | test pyramid, see `docs/testing.md` |
| `deploy/` | Docker, Compose, Kubernetes, observability, k6 |
| `docs/` | plan, strategy, ADRs, generated evidence |
| `scripts/` | developer and test entrypoints |

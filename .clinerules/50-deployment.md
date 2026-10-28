# Deployment Rules (Docker + Kubernetes)

## Images

* One Dockerfile per service under `deploy/docker/`, multi-stage, `python:3.12-slim`,
  non-root `app` user, `PYTHONUNBUFFERED=1`, `PYTHONDONTWRITEBYTECODE=1`.
* `HEALTHCHECK` hits `/readyz`; `CMD ["python","-m","services.<name>.app"]`.
* No build-time pip installs (stdlib only) - image builds are offline-safe.

## Kubernetes invariants

1. **Probes** - `readinessProbe` `/readyz` (removes pod from endpoints),
   `livenessProbe` `/healthz` (restarts hung pods), `startupProbe` for slow tiers.
2. **Resources** - every container has requests **and** limits; HPA needs requests.
3. **HPA** - `autoscaling/v2`, CPU 70% + Pods custom metric `inflight` when a
   metrics adapter is present; behaviour blocks prevent flapping:
   scaleUp stabilization 30s / 4 pods per 15s, scaleDown 60s.
4. **Workers are partition-bound** - never autoscale past partition count. Use
   KEDA/manual replicas = partitions (documented in the deployment).
5. **Disruption** - PodDisruptionBudget `minAvailable: 1` for gateway and order
   service; `maxUnavailable: 0` rollouts for stateful storage.
6. **Storage** - one StatefulSet per shard with a PVC; primary/replica wiring is
   expressed in the ConfigMap `SHARDS` JSON, exactly like local kpilot.
7. **Secrets** - API keys in a `Secret`, never in ConfigMaps or images.
8. **Network** - default-deny NetworkPolicy; allow edge -> gateway, gateway ->
   order-service, services -> storage/broker only.
9. **Ports and env names must match the local spec** (`services/kpilot/spec.py`).
   If you change one, change both and update the README table.
10. **Ingress** - nginx class, `/api` -> api-gateway Service; edge-lb is only for
    local runs (documented substitution).

## Validation (no cluster required)

```bash
python -m scripts.validate_manifests   # YAML subset parser + invariants
```

Run it in CI. It checks: probes present, resource requests/limits, HPA targets,
image tags, env names, PDB coverage. It is intentionally a real check, not a
placeholder.

# Coding Standards

## Shape of a service

```
services/<name>/
  __init__.py
  app.py        # composition root: config -> wiring -> routes -> serve
  handler.py    # (workers) pure message handling, unit testable
  repository.py # (stateful services) persistence logic
```

`app.py` must stay a thin composition root: read env, build dependencies, register
routes, call `app.serve(port)`. Business rules live in testable functions/classes.

## Python style

* Python 3.11, `from __future__ import annotations`, full type hints on public
  functions, `dataclass` for value objects.
* Line length ~120, 4 spaces, no wildcard imports, no global mutable state
  outside `mesh.metrics` (one registry per process).
* Raise `httpkit.Problem(status, code, message)` for expected failures; let
  unexpected exceptions become 500 with a request id (never leak traces).
* Log with `mesh_log.fields(...)`, never `print()` in services (CLI tools may
  print JSON for humans).
* Do not swallow exceptions silently; either handle with a metric + log or let
  them propagate to the retry layer.

## Endpoint conventions

| Kind | Path | Auth |
|---|---|---|
| Business API (public) | `/api/v1/...` via gateway | `X-API-Key` |
| Service API (internal) | `/orders`, `/topics/...`, `/kv/...` | network boundary |
| Probes | `/healthz`, `/readyz` | none, never rate limited |
| Metrics | `/metrics`, `/metrics.json` | none, bypasses admission control |
| Debug (non-prod) | `/stats`, `/debug/state` | none |

Error body: `{"error": {"code": "...", "message": "...", "request_id": "..."}}`.

## Metrics naming

`<domain>_<noun>_<unit>_total` for counters, `_seconds` for histograms,
`mesh_` prefix for platform metrics. Labels must be low cardinality
(`route` template, `status`, `pod`, `topic`) - never raw paths or order ids.

Add a metric whenever you add a failure mode: rejections
(`mesh_admission_rejected_total`), retries (`gateway_upstream_errors_total`),
dead letters (`broker_dead_letters_total`), failovers
(`storage_primary_failovers_total`).

## Configuration

* Every knob is an env var read via `mesh.config`, with a safe default.
* Document new env vars in the README table and in `deploy/k8s` ConfigMaps.
* Never hardcode ports inside services; use `PORT` / `LISTEN_PORT` / `ADMIN_PORT`.
* Test-only knobs (`ORDER_IO_MS`, `MAX_BACKLOG`, `WORK_MS`) are allowed but must
  be documented as simulation controls.

## Definition of done for a change

1. Reads cleanly without comments explaining *what*; comments explain *why*.
2. Unit or integration test added/updated (see `30-testing.md`).
3. Metrics/logs updated if a new failure mode exists.
4. Docs touched if behaviour, config, ports or topology changed.
5. Committed in a focused commit (`40-workflow.md`).

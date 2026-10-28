"""kpilot: a tiny Kubernetes-compatible control plane for local runs.

It implements the subset of Kubernetes this system depends on:

* Deployments with replica reconciliation and readiness probes
* a Service registry (Endpoints) consumed by client-side load balancers
* Horizontal Pod Autoscaling driven by metrics-server-style snapshots
* CrashLoopBackOff-ish restarts, manual scale, and chaos kills

The real cluster configuration lives in ``deploy/k8s``. kpilot exists so the
same architecture is executable (and testable) on a laptop or CI runner without
a container runtime: every pod is a real OS process with its own port, log file
and /readyz endpoint.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading

from libs.mesh import config, httpkit, logging as mesh_log, metrics
from libs.mesh.httpkit import Problem
from services.kpilot.cluster import Cluster
from services.kpilot.spec import build_cluster

SERVICE = "kpilot"
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build_app(cluster: Cluster) -> httpkit.App:
    app = httpkit.App(SERVICE, max_concurrency=32, readiness=lambda: (cluster.ready(), "reconciling" if not cluster.ready() else "all pods ready"))

    @app.route("GET", "/status", name="cluster.status")
    def status(_: httpkit.Request):
        return cluster.status()

    @app.route("GET", "/deployments/{name}", name="cluster.deployment")
    def deployment(req: httpkit.Request):
        try:
            controller = cluster.deployment(req.params["name"])
        except KeyError as exc:
            raise Problem(404, "unknown_deployment", f"deployment {req.params['name']!r} not found") from exc
        return controller.status()

    @app.route("GET", "/events", name="cluster.events")
    def events(req: httpkit.Request):
        limit = req.query_int("limit", 100)
        with cluster.lock:
            return {"events": list(cluster.events[-limit:])}

    @app.route("POST", "/scale", name="cluster.scale")
    def scale(req: httpkit.Request):
        payload = req.json()
        try:
            return cluster.scale(
                payload["deployment"], int(payload["replicas"]), bool(payload.get("disable_autoscale", True))
            )
        except KeyError as exc:
            raise Problem(404, "unknown_deployment", str(exc)) from exc

    @app.route("POST", "/autoscale", name="cluster.autoscale")
    def autoscale(req: httpkit.Request):
        payload = req.json()
        return cluster.set_autoscale(payload["deployment"], bool(payload.get("enabled", True)))

    @app.route("POST", "/kill", name="cluster.kill")
    def kill(req: httpkit.Request):
        payload = req.json()
        ordinal = payload.get("pod")
        return cluster.kill(payload["deployment"], None if ordinal is None else int(ordinal))

    @app.route("GET", "/spec", name="cluster.spec")
    def spec(_: httpkit.Request):
        return cluster.spec.to_dict()

    @app.route("POST", "/shutdown", name="cluster.shutdown")
    def shutdown(_: httpkit.Request):
        threading.Thread(target=cluster.shutdown, daemon=True).start()
        return {"stopping": True}

    return app


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="kpilot local control plane")
    parser.add_argument("--base-port", type=int, default=int(os.environ.get("BASE_PORT", "21000")))
    parser.add_argument("--runtime-dir", default=os.environ.get("RUNTIME_DIR", "runtime/demo"))
    parser.add_argument("--profile", default=os.environ.get("PROFILE", "demo"))
    parser.add_argument("--order-io-ms", type=int, default=None)
    parser.add_argument("--print-spec", action="store_true")
    args = parser.parse_args(argv)

    spec = build_cluster(
        base_port=args.base_port,
        runtime_dir=os.path.abspath(args.runtime_dir),
        profile=args.profile,
        order_io_ms=args.order_io_ms,
    )
    if args.print_spec:
        print(spec.to_json())
        return

    os.makedirs(spec.runtime_dir, exist_ok=True)
    mesh_log.configure(SERVICE, config.env("POD_NAME", "kpilot-0"))
    metrics.configure(SERVICE, config.env("POD_NAME", "kpilot-0"))
    cluster = Cluster(spec, REPO_ROOT)
    thread = threading.Thread(target=cluster.run_forever, name="reconcile", daemon=True)
    thread.start()
    app = build_app(cluster)

    def _stop(*_: object) -> None:
        cluster.shutdown()
        app.shutdown()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _stop)
    mesh_log.get_logger(SERVICE).info(
        "kpilot starting",
        extra=mesh_log.fields(
            cluster=spec.name,
            base_port=spec.base_port,
            deployments=len(spec.deployments),
            runtime_dir=spec.runtime_dir,
        ),
    )
    try:
        app.serve(spec.admin_port)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main(sys.argv[1:])

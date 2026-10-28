"""Edge load balancer: L4 TCP proxy in front of the API gateway replicas.

In Kubernetes this role is played by an Ingress controller / cloud load
balancer. Keeping a small explicit implementation in the repo makes the
load-balancing layer visible and testable locally:

* round robin across discovered gateway endpoints (kube-proxy semantics)
* passive ejection of endpoints that refuse connections
* bounded concurrent connections: beyond the limit it answers HTTP 503 with
  ``Retry-After`` instead of queueing forever (load shedding at the edge)
* Prometheus metrics for connections and bytes
"""

from __future__ import annotations

import select
import signal
import socket
import socketserver
import threading
import time
from urllib.parse import urlparse

from libs.mesh import config, discovery, httpkit, logging as mesh_log, metrics

SERVICE = "edge-lb"


class EdgeProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        pool: discovery.EndpointPool,
        metrics_obj: metrics.Metrics,
        max_connections: int,
        idle_timeout: float = 30.0,
    ) -> None:
        super().__init__(address, EdgeHandler)
        self.pool = pool
        self.metrics = metrics_obj
        self.max_connections = max_connections
        self.idle_timeout = idle_timeout
        self._active = 0
        self._lock = threading.Lock()
        self._shutdown = threading.Event()

    def _acquire_slot(self) -> bool:
        with self._lock:
            if self._active >= self.max_connections:
                return False
            self._active += 1
        self.metrics.gauge("edge_active_connections", self._active)
        return True

    def _release_slot(self) -> None:
        with self._lock:
            self._active = max(0, self._active - 1)
        self.metrics.gauge("edge_active_connections", self._active)

    def shutdown(self) -> None:  # type: ignore[override]
        self._shutdown.set()
        super().shutdown()


class EdgeHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server: EdgeProxy = self.server  # type: ignore[assignment]
        metrics_obj = server.metrics
        if not server._acquire_slot():
            metrics_obj.inc("edge_overload_rejections_total")
            body = b'{"error":{"code":"overloaded","message":"edge connection limit reached"}}'
            self.request.sendall(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n"
                b"Retry-After: 0.2\r\nConnection: close\r\nContent-Length: "
                + str(len(body)).encode()
                + b"\r\n\r\n"
                + body
            )
            return
        upstream = None
        try:
            for _ in range(3):
                endpoint = server.pool.pick()
                if endpoint is None:
                    break
                parsed = urlparse(endpoint.url)
                try:
                    upstream = socket.create_connection((parsed.hostname, parsed.port), timeout=1.0)
                    upstream.settimeout(None)
                    metrics_obj.inc("edge_connections_total", endpoint=endpoint.pod)
                    break
                except OSError:
                    server.pool.mark_failure(endpoint.url)
                    metrics_obj.inc("edge_upstream_connect_errors_total", endpoint=endpoint.pod)
                    upstream = None
            if upstream is None:
                body = b'{"error":{"code":"no_gateway","message":"no healthy API gateway replica"}}'
                self.request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\n"
                    b"Retry-After: 0.2\r\nConnection: close\r\nContent-Length: "
                    + str(len(body)).encode()
                    + b"\r\n\r\n"
                    + body
                )
                return
            self._tunnel(self.request, upstream, metrics_obj, server.idle_timeout)
        finally:
            if upstream is not None:
                upstream.close()
            server._release_slot()

    @staticmethod
    def _tunnel(client: socket.socket, upstream: socket.socket, metrics_obj: metrics.Metrics, idle_timeout: float) -> None:
        sockets = [client, upstream]
        while True:
            try:
                readable, _, exceptional = select.select(sockets, [], sockets, idle_timeout)
            except (OSError, ValueError):
                return
            if exceptional or not readable:
                return
            for source in readable:
                try:
                    data = source.recv(65536)
                except OSError:
                    return
                if not data:
                    return
                target = upstream if source is client else client
                try:
                    target.sendall(data)
                except OSError:
                    return
                direction = "up" if source is client else "down"
                metrics_obj.inc("edge_bytes_total", direction=direction, value=len(data))


def build_admin_app(pool: discovery.EndpointPool) -> httpkit.App:
    def readiness() -> tuple[bool, str]:
        endpoints = pool.endpoints()
        return (bool(endpoints), f"{len(endpoints)} gateway replica(s) available")

    app = httpkit.App(SERVICE, max_concurrency=16, readiness=readiness)

    @app.route("GET", "/stats", name="edge.stats")
    def stats(_: httpkit.Request):
        return {
            "pod": config.env("POD_NAME", "edge-lb-0"),
            "upstream_service": pool.service,
            "endpoints": [
                {"pod": e.pod, "url": e.url, "failures": pool.failure_counts().get(e.url, 0)}
                for e in pool.endpoints()
            ],
        }

    return app


def main() -> None:
    pod = config.env("POD_NAME", "edge-lb-0")
    mesh_log.configure(SERVICE, pod)
    metrics_obj = metrics.configure(SERVICE, pod)
    registry = discovery.FileRegistry(config.env("DISCOVERY_FILE", "runtime/registry.json"), ttl=0.3)
    pool = discovery.EndpointPool(registry, config.env("UPSTREAM_SERVICE", "api-gateway"), metrics=metrics_obj)
    listen_port = config.env_int("LISTEN_PORT", 14000)
    admin_port = config.env_int("ADMIN_PORT", 14001)
    proxy = EdgeProxy(
        ("0.0.0.0", listen_port),
        pool,
        metrics_obj,
        max_connections=config.env_int("MAX_CONNECTIONS", 512),
    )
    admin = build_admin_app(pool)
    threading.Thread(target=admin.serve, args=(admin_port, False), daemon=True, name="edge-admin").start()
    stop = threading.Event()

    def _stop(*_: object) -> None:
        stop.set()
        proxy.shutdown()
        admin.shutdown()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _stop)
    mesh_log.get_logger(SERVICE).info(
        "edge load balancer ready",
        extra=mesh_log.fields(listen_port=listen_port, admin_port=admin_port, upstream=pool.service),
    )
    try:
        proxy.serve_forever(poll_interval=0.2)
    finally:
        proxy.server_close()


if __name__ == "__main__":
    main()

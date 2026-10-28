"""API gateway: the single ingress for clients.

Responsibilities (the "edge" concerns, so services do not reimplement them):

1. authentication        X-API-Key -> 401
2. rate limiting         token bucket per API key -> 429 + Retry-After
3. routing               /api/v1/orders -> order-service (path rewriting)
4. load balancing        round robin over discovered replicas, passive ejection
5. resilience            per-replica circuit breaker + bounded retries with
                         idempotency-key awareness (POST is only retried when the
                         client supplied an Idempotency-Key)
6. observability         request ids, Prometheus metrics, upstream attribution
7. admission control     bounded concurrency -> 503 when saturated

It is stateless and horizontally scalable; the edge load balancer (edge-lb) or
the Kubernetes Ingress spreads traffic across gateway replicas.
"""

from __future__ import annotations

import hmac
import time
from dataclasses import dataclass, field

from libs.mesh import config, discovery, httpkit, logging as mesh_log, metrics
from libs.mesh.httpkit import HttpClient, HttpUnavailable, Problem
from libs.mesh.resilience import CircuitBreaker, TokenBucket

SERVICE = "api-gateway"


@dataclass
class GatewayConfig:
    upstream_service: str = "order-service"
    api_keys: list[str] = field(default_factory=lambda: ["demo-key"])
    rate_limit_rps: float = 200.0
    rate_limit_burst: float = 400.0
    timeout_s: float = 2.0
    retry_attempts: int = 1
    breaker_threshold: int = 5
    breaker_reset_s: float = 5.0
    max_concurrency: int = 64

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            upstream_service=config.env("UPSTREAM_SERVICE", "order-service"),
            api_keys=config.env_list("API_KEYS", ["demo-key"]),
            rate_limit_rps=config.env_float("RATE_LIMIT_RPS", 200.0),
            rate_limit_burst=config.env_float("RATE_LIMIT_BURST", 400.0),
            timeout_s=config.env_float("UPSTREAM_TIMEOUT_S", 2.0),
            retry_attempts=config.env_int("UPSTREAM_RETRIES", 1),
            breaker_threshold=config.env_int("BREAKER_THRESHOLD", 5),
            breaker_reset_s=config.env_float("BREAKER_RESET_S", 5.0),
            max_concurrency=config.env_int("MAX_CONCURRENCY", 64),
        )


class UpstreamProxy:
    """Load balancing + resilience for one upstream service."""

    def __init__(self, cfg: GatewayConfig, registry: discovery.ServiceRegistry, metrics_obj: metrics.Metrics) -> None:
        self.cfg = cfg
        self.metrics = metrics_obj
        self.pool = discovery.EndpointPool(registry, cfg.upstream_service, metrics=metrics_obj)
        self.http = HttpClient(timeout=cfg.timeout_s, name="gateway")
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = __import__("threading").Lock()

    def breaker(self, url: str) -> CircuitBreaker:
        with self._lock:
            breaker = self._breakers.get(url)
            if breaker is None:
                breaker = CircuitBreaker(self.cfg.breaker_threshold, self.cfg.breaker_reset_s)
                self._breakers[url] = breaker
            return breaker

    def forward_headers(self, req: httpkit.Request) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "X-Request-Id": req.request_id,
            "X-Trace-Id": req.trace_id,
            "X-Forwarded-For": req.client,
            "X-Gateway": SERVICE,
        }
        for name in ("Idempotency-Key", "User-Agent", "Accept"):
            value = req.header(name)
            if value:
                headers[name] = value
        return headers

    def call(self, req: httpkit.Request, method: str, path: str) -> httpkit.Response:
        idempotency_key = req.header("Idempotency-Key")
        safe_to_retry = method == "GET" or bool(idempotency_key)
        attempts = 1 + (self.cfg.retry_attempts if safe_to_retry else 0)
        started = time.monotonic()
        last_error = "no upstream attempt made"
        for attempt in range(1, attempts + 1):
            endpoint = self.pool.pick()
            if endpoint is None:
                last_error = "no healthy replicas"
                break
            breaker = self.breaker(endpoint.url)
            if not breaker.allow():
                last_error = f"circuit open for {endpoint.pod}"
                self.metrics.inc("gateway_breaker_rejections_total", endpoint=endpoint.pod)
                continue
            try:
                result = self.http.request(
                    method,
                    f"{endpoint.url}{path}",
                    body=req.body or None,
                    headers=self.forward_headers(req),
                    timeout=self.cfg.timeout_s,
                    retry=False,
                )
            except HttpUnavailable as exc:
                breaker.record_failure()
                self.pool.mark_failure(endpoint.url)
                last_error = str(exc)
                self.metrics.inc("gateway_upstream_errors_total", endpoint=endpoint.pod, reason="transport")
                continue
            if result.status >= 500:
                breaker.record_failure()
                self.pool.mark_failure(endpoint.url)
                last_error = f"upstream {result.status} from {endpoint.pod}"
                self.metrics.inc("gateway_upstream_errors_total", endpoint=endpoint.pod, reason="5xx")
                continue
            breaker.record_success()
            self.pool.mark_success(endpoint.url)
            self.metrics.inc("gateway_proxied_total", status=str(result.status), endpoint=endpoint.pod)
            return self._translate(result, endpoint, attempt, time.monotonic() - started)
        self.metrics.inc("gateway_failed_total")
        raise Problem(503, "upstream_unavailable", f"{self.cfg.upstream_service} unavailable: {last_error}", retry_after=0.25)

    def _translate(self, result, endpoint: discovery.Endpoint, attempt: int, elapsed: float) -> httpkit.Response:
        try:
            body = result.json() if result.body else None
        except ValueError:
            body = {"upstream_body": result.body.decode(errors="replace")}
        headers = {
            "X-Gateway-Pod": config.env("POD_NAME", "gateway-0"),
            "X-Upstream-Pod": endpoint.pod,
            "X-Upstream-Url": endpoint.url,
            "X-Gateway-Attempts": str(attempt),
            "X-Gateway-Duration-Ms": f"{elapsed * 1000:.1f}",
        }
        for name in ("Location", "Retry-After", "X-Order-Status"):
            if name.lower() in result.headers:
                headers[name] = result.headers[name.lower()]
        content_type = result.headers.get("content-type", "application/json")
        return httpkit.Response(status=result.status, body=body, headers={**headers, "Content-Type": content_type})


class RateLimiter:
    """Per-client token buckets; the gateway equivalent of an API quota."""

    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = __import__("threading").Lock()

    def check(self, client_key: str) -> tuple[bool, float]:
        with self._lock:
            bucket = self._buckets.get(client_key)
            if bucket is None:
                bucket = TokenBucket(self.rate, self.capacity)
                self._buckets[client_key] = bucket
        allowed = bucket.allow()
        return allowed, bucket.retry_after()


def authenticate(req: httpkit.Request, cfg: GatewayConfig) -> str:
    provided = req.header("X-API-Key")
    if not provided or not any(hmac.compare_digest(provided, key) for key in cfg.api_keys):
        raise Problem(401, "unauthorized", "a valid X-API-Key header is required")
    return provided


def build_app(cfg: GatewayConfig, registry: discovery.ServiceRegistry) -> httpkit.App:
    metrics_obj = metrics.get()
    proxy = UpstreamProxy(cfg, registry, metrics_obj)
    limiter = RateLimiter(cfg.rate_limit_rps, cfg.rate_limit_burst)

    def readiness() -> tuple[bool, str]:
        endpoints = proxy.pool.endpoints()
        return (bool(endpoints), f"{len(endpoints)} upstream replica(s) available")

    app = httpkit.App(SERVICE, max_concurrency=cfg.max_concurrency, readiness=readiness)

    def guard(req: httpkit.Request) -> str:
        api_key = authenticate(req, cfg)
        allowed, retry_after = limiter.check(api_key[:12])
        metrics_obj.inc("gateway_requests_total", route=req.path.split("/")[2] if "/" in req.path else req.path)
        if not allowed:
            raise Problem(429, "rate_limited", "rate limit exceeded", retry_after=retry_after)
        return api_key

    @app.route("POST", "/api/v1/orders", name="orders.create")
    def create_order(req: httpkit.Request):
        guard(req)
        response = proxy.call(req, "POST", "/orders")
        return response

    @app.route("GET", "/api/v1/orders/{order_id}", name="orders.get")
    def get_order(req: httpkit.Request):
        guard(req)
        path = f"/orders/{req.params['order_id']}"
        if req.query_one("consistent"):
            path += "?consistent=" + str(req.query_one("consistent"))
        return proxy.call(req, "GET", path)

    @app.route("GET", "/api/v1/system/status", name="system.status")
    def system_status(req: httpkit.Request):
        guard(req)
        endpoints = proxy.pool.endpoints()
        health = []
        for endpoint in endpoints:
            healthy = False
            try:
                result = proxy.http.request("GET", f"{endpoint.url}/readyz", timeout=0.4, retry=False)
                healthy = result.ok
            except HttpUnavailable:
                healthy = False
            stats = proxy.breaker(endpoint.url).stats()
            health.append(
                {
                    "pod": endpoint.pod,
                    "url": endpoint.url,
                    "healthy": healthy,
                    "circuit": stats.state,
                    "breaker_rejections": stats.rejected,
                    "failures": proxy.pool.failure_counts().get(endpoint.url, 0),
                }
            )
        return {
            "gateway": {
                "pod": config.env("POD_NAME", "gateway-0"),
                "upstream_service": cfg.upstream_service,
                "max_concurrency": cfg.max_concurrency,
                "rate_limit_rps": cfg.rate_limit_rps,
                "retries": cfg.retry_attempts,
                "breaker_threshold": cfg.breaker_threshold,
            },
            "upstreams": health,
            "available_replicas": len(endpoints),
            "healthy_replicas": sum(1 for h in health if h["healthy"]),
        }

    return app


def main() -> None:
    pod = config.env("POD_NAME", "gateway-0")
    mesh_log.configure(SERVICE, pod)
    metrics.configure(SERVICE, pod)
    cfg = GatewayConfig.from_env()
    registry_path = config.env("DISCOVERY_FILE", "")
    if registry_path:
        registry: discovery.ServiceRegistry = discovery.FileRegistry(registry_path)
    else:
        registry = discovery.static_from_env(cfg.upstream_service)
    app = build_app(cfg, registry)
    mesh_log.get_logger(SERVICE).info(
        "api gateway ready",
        extra=mesh_log.fields(
            upstream=cfg.upstream_service,
            rate_limit_rps=cfg.rate_limit_rps,
            registry=registry_path or "static",
            auth_enabled=True,
        ),
    )
    app.serve(config.env_int("PORT", 13000))


if __name__ == "__main__":
    main()

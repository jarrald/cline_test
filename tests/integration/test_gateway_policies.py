import threading
import time
import unittest

from libs.mesh import discovery, httpkit, metrics
from services.api_gateway.app import GatewayConfig, build_app


class UpstreamStub:
    """A controllable order-service stand-in."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.requests: list[dict] = []
        self.failures_remaining = 0
        self.lock = threading.Lock()
        app = httpkit.App(name, max_concurrency=16)

        @app.route("POST", "/orders", name="stub.create")
        def create(req: httpkit.Request):
            self._record("POST", req)
            if self._should_fail():
                return httpkit.Response.json({"error": {"code": "boom"}}, status=500)
            return httpkit.Response.json({"order_id": "ord_stub", "pod": self.name}, status=202, Location="/orders/ord_stub")

        @app.route("GET", "/orders/{order_id}", name="stub.get")
        def get(req: httpkit.Request):
            self._record("GET", req)
            if self._should_fail():
                return httpkit.Response.json({"error": {"code": "boom"}}, status=500)
            if req.params["order_id"] == "missing":
                return httpkit.Response.json({"error": {"code": "order_not_found"}}, status=404)
            return {"order": {"order_id": req.params["order_id"], "pod": self.name, "consistent": req.query_one("consistent")}}

        self.app = app
        self.port = app.start_in_thread()
        self.url = f"http://127.0.0.1:{self.port}"

    def _record(self, method: str, req: httpkit.Request) -> None:
        with self.lock:
            self.requests.append({"method": method, "path": req.path, "headers": req.headers, "query": req.query})

    def _should_fail(self) -> bool:
        with self.lock:
            if self.failures_remaining > 0:
                self.failures_remaining -= 1
                return True
        return False

    def calls(self, method: str | None = None) -> int:
        with self.lock:
            return sum(1 for r in self.requests if method is None or r["method"] == method)

    def stop(self) -> None:
        self.app.stop()


class GatewayPolicyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        metrics.configure("gateway-it", "gateway-0")

    def setUp(self) -> None:
        self.upstreams = [UpstreamStub("order-service-0"), UpstreamStub("order-service-1")]
        self.registry = discovery.StaticRegistry({"order-service": [u.url for u in self.upstreams]})
        self.apps: list[httpkit.App] = []

    def tearDown(self) -> None:
        for app in self.apps:
            app.stop()
        for upstream in self.upstreams:
            upstream.stop()

    def gateway(self, **overrides) -> tuple[httpkit.App, int]:
        config = GatewayConfig(
            api_keys=["test-key"],
            rate_limit_rps=overrides.pop("rate_limit_rps", 100.0),
            rate_limit_burst=overrides.pop("rate_limit_burst", 100.0),
            retry_attempts=overrides.pop("retry_attempts", 1),
            breaker_threshold=overrides.pop("breaker_threshold", 5),
            breaker_reset_s=overrides.pop("breaker_reset_s", 0.3),
            failure_cooldown_s=overrides.pop("failure_cooldown_s", 2.0),
            timeout_s=overrides.pop("timeout_s", 1.0),
            max_concurrency=32,
            **overrides,
        )
        app = build_app(config, self.registry)
        port = app.start_in_thread()
        self.apps.append(app)
        return app, port

    @staticmethod
    def call(port: int, method: str, path: str, body=None, key: str | None = "test-key", headers=None):
        merged = dict(headers or {})
        if key:
            merged["X-API-Key"] = key
        return httpkit.HttpClient(timeout=3.0).request(method, f"http://127.0.0.1:{port}{path}", body=body, headers=merged)

    def test_missing_or_wrong_api_key_is_rejected(self) -> None:
        _, port = self.gateway()
        self.assertEqual(self.call(port, "GET", "/api/v1/orders/1", key=None).status, 401)
        self.assertEqual(self.call(port, "GET", "/api/v1/orders/1", key="wrong").status, 401)
        self.assertEqual(self.call(port, "GET", "/api/v1/orders/1").status, 200)

    def test_rate_limit_uses_token_bucket_and_retry_after(self) -> None:
        _, port = self.gateway(rate_limit_rps=0.5, rate_limit_burst=2)
        statuses = [self.call(port, "GET", "/api/v1/orders/1").status for _ in range(3)]
        self.assertEqual(statuses[:2], [200, 200])
        limited = self.call(port, "GET", "/api/v1/orders/1")
        self.assertEqual(limited.status, 429)
        self.assertEqual(limited.json()["error"]["code"], "rate_limited")
        self.assertIn("retry-after", limited.headers)

    def test_post_is_retried_only_with_idempotency_key(self) -> None:
        upstream = self.upstreams[0]
        self.registry.set("order-service", [upstream.url])
        _, port = self.gateway(retry_attempts=1, breaker_threshold=10, failure_cooldown_s=0.0)
        payload = {"customer_id": "c", "sku": "s", "quantity": 1}

        upstream.failures_remaining = 1
        no_key = self.call(port, "POST", "/api/v1/orders", body=payload)
        self.assertEqual(no_key.status, 503)
        self.assertIn("upstream 500", no_key.json()["error"]["message"])
        self.assertEqual(upstream.calls("POST"), 1, "a non-idempotent POST must not be retried")

        upstream.failures_remaining = 1
        with_key = self.call(
            port,
            "POST",
            "/api/v1/orders",
            body=payload,
            headers={"Idempotency-Key": "abc"},
        )
        self.assertEqual(with_key.status, 202)
        self.assertEqual(with_key.headers["x-gateway-attempts"], "2")
        self.assertEqual(upstream.calls("POST"), 3, "one non-retried call + two attempts with a key")

    def test_circuit_breaker_opens_and_recovers(self) -> None:
        upstream = self.upstreams[0]
        self.registry.set("order-service", [upstream.url])
        _, port = self.gateway(breaker_threshold=1, breaker_reset_s=0.3, failure_cooldown_s=0.05, retry_attempts=0)
        upstream.failures_remaining = 10
        first = self.call(port, "GET", "/api/v1/orders/1")
        self.assertEqual(first.status, 503)
        second = self.call(port, "GET", "/api/v1/orders/1")
        self.assertEqual(second.status, 503)
        time.sleep(0.08)  # endpoint cooldown elapsed, breaker still open
        third = self.call(port, "GET", "/api/v1/orders/1")
        self.assertEqual(third.status, 503)
        self.assertIn("circuit open", third.json()["error"]["message"])
        time.sleep(0.3)  # reset timeout -> half-open probe
        upstream.failures_remaining = 0
        recovered = self.call(port, "GET", "/api/v1/orders/1")
        self.assertEqual(recovered.status, 200)

    def test_round_robin_spreads_requests(self) -> None:
        _, port = self.gateway()
        for i in range(6):
            self.assertEqual(self.call(port, "GET", f"/api/v1/orders/{i}").status, 200)
        self.assertEqual(self.upstreams[0].calls("GET"), 3)
        self.assertEqual(self.upstreams[1].calls("GET"), 3)

    def test_path_rewrite_query_and_header_propagation(self) -> None:
        _, port = self.gateway()
        response = self.call(port, "GET", "/api/v1/orders/ord_1?consistent=true")
        self.assertEqual(response.status, 200)
        body = response.json()["order"]
        self.assertEqual(body["order_id"], "ord_1")
        self.assertEqual(body["consistent"], "true")
        self.assertIn(response.headers["x-upstream-pod"], {"order-service-0", "order-service-1"})
        self.assertEqual(response.headers["x-gateway-pod"], "gateway-0")
        upstream_request = next(r for u in self.upstreams for r in u.requests)
        self.assertEqual(upstream_request["path"], "/orders/ord_1")
        self.assertEqual(upstream_request["headers"]["X-Request-Id"], response.headers["x-request-id"])

    def test_upstream_404_is_passed_through(self) -> None:
        _, port = self.gateway()
        response = self.call(port, "GET", "/api/v1/orders/missing")
        self.assertEqual(response.status, 404)
        self.assertEqual(response.json()["error"]["code"], "order_not_found")

    def test_all_upstreams_dead_returns_503(self) -> None:
        self.registry.set("order-service", [])
        _, port = self.gateway()
        response = self.call(port, "GET", "/api/v1/orders/1")
        self.assertEqual(response.status, 503)
        self.assertEqual(response.json()["error"]["code"], "upstream_unavailable")

    def test_system_status_reports_replicas(self) -> None:
        _, port = self.gateway()
        status = self.call(port, "GET", "/api/v1/system/status").json()
        self.assertEqual(status["available_replicas"], 2)
        self.assertEqual(status["healthy_replicas"], 2)
        self.assertTrue(all(item["circuit"] == "closed" for item in status["upstreams"]))


if __name__ == "__main__":
    unittest.main()

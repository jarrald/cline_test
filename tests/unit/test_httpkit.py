import unittest

from libs.mesh import httpkit, metrics
from libs.mesh.httpkit import HttpClient, Problem


class HttpKitTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        metrics.configure("httpkit-test", "pod-0")
        app = httpkit.App("httpkit-test", max_concurrency=2)

        @app.route("GET", "/items/{item_id}", name="items.get")
        def get_item(req: httpkit.Request):
            return {"item": req.params["item_id"], "limit": req.query_int("limit", 10), "flag": req.query_bool("flag")}

        @app.route("POST", "/items", name="items.create")
        def create_item(req: httpkit.Request):
            payload = req.json()
            if "name" not in payload:
                raise Problem(422, "invalid_body", "name is required")
            return httpkit.Response.json({"created": payload["name"]}, status=201, Location="/items/1")

        @app.route("GET", "/slow", name="slow")
        def slow(_: httpkit.Request):
            import time

            time.sleep(0.5)
            return {"slow": True}

        @app.route("GET", "/boom", name="boom")
        def boom(_: httpkit.Request):
            raise RuntimeError("unexpected")

        cls.app = app
        cls.port = app.start_in_thread()
        cls.client = HttpClient(timeout=2.0, name="test")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.app.stop()

    def url(self, path: str) -> str:
        return f"http://127.0.0.1:{self.port}{path}"

    def test_path_params_and_query_parsing(self) -> None:
        result = self.client.request("GET", self.url("/items/abc?limit=3&flag=true"))
        self.assertEqual(result.status, 200)
        self.assertEqual(result.json(), {"item": "abc", "limit": 3, "flag": True})

    def test_problem_document_is_returned(self) -> None:
        result = self.client.request("POST", self.url("/items"), body={"nope": 1})
        self.assertEqual(result.status, 422)
        self.assertEqual(result.json()["error"]["code"], "invalid_body")
        self.assertTrue(result.json()["error"]["request_id"])

    def test_created_response_keeps_headers_and_body(self) -> None:
        result = self.client.request("POST", self.url("/items"), body={"name": "widget"})
        self.assertEqual(result.status, 201)
        self.assertEqual(result.json(), {"created": "widget"})
        self.assertEqual(result.headers["location"], "/items/1")

    def test_unknown_route_is_404(self) -> None:
        self.assertEqual(self.client.request("GET", self.url("/missing")).status, 404)

    def test_unexpected_error_is_500_without_trace(self) -> None:
        result = self.client.request("GET", self.url("/boom"))
        self.assertEqual(result.status, 500)
        self.assertEqual(result.json()["error"]["code"], "internal_error")
        self.assertNotIn("RuntimeError", result.body.decode())

    def test_keep_alive_reuses_connection(self) -> None:
        first = self.client.request("GET", self.url("/items/1"))
        second = self.client.request("GET", self.url("/items/2"))
        self.assertEqual((first.status, second.status), (200, 200))
        self.assertTrue(self.client._local.pool)  # connection is pooled

    def test_admission_control_rejects_when_saturated(self) -> None:
        import threading

        statuses: list[int] = []
        lock = threading.Lock()

        def hammer() -> None:
            try:
                result = HttpClient(timeout=2.0).request("GET", self.url("/slow"))
                with lock:
                    statuses.append(result.status)
            except Exception as exc:  # pragma: no cover - diagnostic
                with lock:
                    statuses.append(-1)
                    print(f"transport error: {exc}")

        threads = [threading.Thread(target=hammer) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # capacity is 2: two requests are served, two are load-shed with 503
        self.assertEqual(statuses.count(200), 2, statuses)
        self.assertEqual(statuses.count(503), 2, statuses)

    def test_probes_and_metrics_are_available(self) -> None:
        self.assertEqual(self.client.request("GET", self.url("/healthz")).status, 200)
        self.assertEqual(self.client.request("GET", self.url("/readyz")).status, 200)
        metrics_text = self.client.request("GET", self.url("/metrics")).body.decode()
        self.assertIn("http_requests_total", metrics_text)
        snapshot = self.client.request("GET", self.url("/metrics.json")).json()
        self.assertIn("inflight", snapshot)
        self.assertIn("cpu_percent", snapshot)


if __name__ == "__main__":
    unittest.main()

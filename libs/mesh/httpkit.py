"""A deliberately small HTTP framework + keep-alive client.

Why hand-rolled? The repo must run with nothing but CPython, and the exercise
is about architecture, not web frameworks. The server implements the pieces the
architecture needs:

* bounded concurrency (admission control) -> HTTP 503 + ``Retry-After`` when
  saturated. This is what makes the autoscaling demo observable.
* request ids / trace propagation and structured access logs
* Prometheus metrics per route template (bounded cardinality)
* liveness/readiness probes, exactly like a Kubernetes deployment needs
* graceful shutdown on SIGTERM

The client pools one persistent connection per thread and netloc, which keeps
TIME_WAIT sockets low under load.
"""

from __future__ import annotations

import http.client
import json
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from . import logging as mesh_log
from .metrics import Metrics, get as get_metrics

log = mesh_log.get_logger("httpkit")


class Problem(Exception):
    """Domain error rendered as a JSON problem document."""

    def __init__(self, status: int, code: str, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.retry_after = retry_after


@dataclass
class Request:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: bytes
    client: str
    request_id: str
    trace_id: str
    params: dict[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        if not self.body:
            return {}
        try:
            return json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise Problem(400, "invalid_json", "request body must be valid JSON") from exc

    def header(self, name: str, default: str = "") -> str:
        for key, value in self.headers.items():
            if key.lower() == name.lower():
                return value
        return default

    def query_one(self, name: str, default: str | None = None) -> str | None:
        values = self.query.get(name)
        return values[0] if values else default

    def query_int(self, name: str, default: int) -> int:
        raw = self.query_one(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise Problem(400, "invalid_query", f"query parameter {name!r} must be an integer") from exc

    def query_bool(self, name: str, default: bool = False) -> bool:
        raw = self.query_one(name)
        if raw is None:
            return default
        return raw.lower() in {"1", "true", "yes", "on"}


@dataclass
class Response:
    status: int = 200
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def json(cls, body: Any, status: int = 200, **headers: str) -> "Response":
        return cls(status=status, body=body, headers=headers)

    @classmethod
    def text(cls, body: str, status: int = 200, **headers: str) -> "Response":
        return cls(status=status, body=body, headers={"Content-Type": "text/plain; charset=utf-8", **headers})


Handler = Callable[[Request], Response | tuple[int, Any] | dict[str, Any] | None]


class App:
    """Routing, admission control, probes and metrics for one service."""

    def __init__(
        self,
        name: str,
        max_concurrency: int = 32,
        max_body_bytes: int = 1 << 20,
        readiness: Callable[[], tuple[bool, str]] | None = None,
    ) -> None:
        self.name = name
        self.max_concurrency = max_concurrency
        self.max_body_bytes = max_body_bytes
        self.metrics: Metrics = get_metrics()
        self._routes: list[tuple[str, tuple[str, ...], Handler, str]] = []
        self._semaphore = threading.BoundedSemaphore(max_concurrency)
        self._readiness = readiness or (lambda: (True, "ok"))
        self._started_at = time.time()
        self._shutdown = threading.Event()
        self._server: ThreadingHTTPServer | None = None
        self.overloaded = 0
        self._overload_lock = threading.Lock()
        self.metrics.gauge("mesh_concurrency_limit", max_concurrency)

    # ----------------------------------------------------------------- routes
    def route(self, method: str, pattern: str, name: str | None = None) -> Callable[[Handler], Handler]:
        def decorator(func: Handler) -> Handler:
            self.add_route(method, pattern, func, name)
            return func

        return decorator

    def add_route(self, method: str, pattern: str, handler: Handler, name: str | None = None) -> None:
        self._routes.append((method.upper(), tuple(pattern.strip("/").split("/")), handler, name or pattern))

    # --------------------------------------------------------------- dispatch
    def _match(self, method: str, path: str) -> tuple[Handler, dict[str, str], str] | None:
        parts = tuple(path.strip("/").split("/")) if path.strip("/") else ()
        for route_method, pattern, handler, name in self._routes:
            if route_method != method or len(pattern) != len(parts):
                continue
            params: dict[str, str] = {}
            for expected, actual in zip(pattern, parts):
                if expected.startswith("{") and expected.endswith("}"):
                    params[expected[1:-1]] = actual
                elif expected != actual:
                    break
            else:
                return handler, params, name
        return None

    def handle_request(
        self, method: str, raw_path: str, headers: dict[str, str], body: bytes, client: str
    ) -> Response:
        parsed = urlparse(raw_path)
        path = parsed.path
        query = parse_qs(parsed.query)
        request_id = headers.get("X-Request-Id") or uuid.uuid4().hex[:16]
        trace_id = headers.get("X-Trace-Id") or request_id
        mesh_log.bind(request_id, trace_id)

        if path == "/healthz":
            return self._probe({"status": "ok", "service": self.name})
        if path == "/readyz":
            ready, reason = self._readiness()
            return self._probe(
                {"status": "ready" if ready else "not_ready", "reason": reason}, 200 if ready else 503
            )
        if path == "/metrics":
            return Response.text(self.metrics.render_prometheus(), headers={"X-Request-Id": request_id})
        if path == "/metrics.json":
            payload = self.metrics.snapshot()
            payload["pid"] = _pid()
            return Response.json(payload, headers={"X-Request-Id": request_id})

        if len(body) > self.max_body_bytes:
            return self._error(413, "payload_too_large", "request body too large", request_id)

        acquired = self._semaphore.acquire(blocking=False)
        if not acquired:
            with self._overload_lock:
                self.overloaded += 1
            self.metrics.inc("mesh_admission_rejected_total")
            log.warning("rejected request: concurrency limit reached", extra=mesh_log.fields(limit=self.max_concurrency))
            return self._error(
                503,
                "overloaded",
                "server concurrency limit reached, retry with backoff",
                request_id,
                retry_after=0.05,
                extra_headers={"Connection": "close"},
            )

        started = time.monotonic()
        self.metrics.enter()
        route_name = "unmatched"
        status = 500
        try:
            match = self._match(method.upper(), path)
            if match is None:
                status = 404
                return self._error(404, "not_found", f"no route for {method} {path}", request_id)
            handler, params, route_name = match
            request = Request(
                method=method.upper(),
                path=path,
                query=query,
                headers=headers,
                body=body,
                client=client,
                request_id=request_id,
                trace_id=trace_id,
                params=params,
            )
            response = self._normalise(handler(request))
            status = response.status
            return self._finalise(response, request_id)
        except Problem as exc:
            status = exc.status
            if exc.status == 429:
                self.metrics.inc("mesh_rate_limited_total", route=route_name)
            return self._error(exc.status, exc.code, exc.message, request_id, exc.retry_after)
        except Exception:  # pragma: no cover - defensive, exercised by chaos tests
            log.exception("unhandled error", extra=mesh_log.fields(path=path, method=method))
            return self._error(500, "internal_error", "unexpected server error", request_id)
        finally:
            elapsed = time.monotonic() - started
            self.metrics.leave()
            self._semaphore.release()
            self.metrics.inc("http_requests_total", route=route_name, method=method.upper(), status=str(status))
            self.metrics.observe("http_request_duration_seconds", elapsed, route=route_name, method=method.upper())
            log.info(
                "request",
                extra=mesh_log.fields(
                    method=method,
                    path=path,
                    status=status,
                    duration_ms=round(elapsed * 1000, 2),
                    client=client,
                ),
            )

    def _normalise(self, result: Response | tuple[int, Any] | dict[str, Any] | None) -> Response:
        if isinstance(result, Response):
            return result
        if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], int):
            return Response.json(result[1], status=result[0])
        if isinstance(result, dict):
            return Response.json(result)
        if result is None:
            return Response(status=204)
        raise TypeError(f"unsupported handler return type: {type(result)!r}")

    def _finalise(self, response: Response, request_id: str) -> Response:
        response.headers.setdefault("X-Request-Id", request_id)
        response.headers.setdefault("Content-Type", "application/json")
        return response

    def _error(
        self,
        status: int,
        code: str,
        message: str,
        request_id: str,
        retry_after: float | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Response:
        headers = {"Content-Type": "application/json", "X-Request-Id": request_id}
        if retry_after is not None:
            headers["Retry-After"] = f"{retry_after:g}"
        if extra_headers:
            headers.update(extra_headers)
        return Response(
            status=status,
            body={"error": {"code": code, "message": message, "request_id": request_id}},
            headers=headers,
        )

    def _probe(self, payload: dict[str, Any], status: int = 200) -> Response:
        return Response.json({**payload, "uptime_s": round(time.time() - self._started_at, 1)}, status=status)

    # ----------------------------------------------------------------- serving
    def serve(self, port: int, install_signal_handlers: bool = True) -> None:
        server = ThreadingHTTPServer(("0.0.0.0", port), _make_handler(self))
        server.daemon_threads = True
        server.app = self  # type: ignore[attr-defined]
        self._server = server
        if install_signal_handlers:
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: self.shutdown())
        log.info("listening", extra=mesh_log.fields(port=port, max_concurrency=self.max_concurrency))
        server.serve_forever(poll_interval=0.2)

    def shutdown(self) -> None:
        if self._shutdown.is_set():
            return
        self._shutdown.set()
        log.info("graceful shutdown requested")
        server = self._server
        if server is not None:
            threading.Thread(target=server.shutdown, daemon=True).start()


def _pid() -> int:
    import os

    return os.getpid()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mesh/1.0"

    def _handle(self) -> None:
        app: App = self.server.app  # type: ignore[attr-defined]
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length > 0 else b""
        try:
            response = app.handle_request(
                self.command, self.path, dict(self.headers.items()), body, self.client_address[0]
            )
        except Exception:  # pragma: no cover - never leak a stack trace to clients
            response = Response.json({"error": {"code": "internal_error"}}, status=500)
        payload = response.body
        if payload is None:
            data = b""
        elif isinstance(payload, (dict, list)):
            data = json.dumps(payload, separators=(",", ":")).encode()
        elif isinstance(payload, bytes):
            data = payload
        else:
            data = str(payload).encode()
        headers = dict(response.headers)
        if headers.get("Connection", "").lower() == "close":
            self.close_connection = True
        self.send_response(response.status)
        self.send_header("Content-Length", str(len(data)))
        for key, value in headers.items():
            if key.lower() in {"content-length", "connection"}:
                continue
            self.send_header(key, value)
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        if data and self.command != "HEAD":
            self.wfile.write(data)

    do_GET = _handle
    do_POST = _handle
    do_PUT = _handle
    do_PATCH = _handle
    do_DELETE = _handle
    do_HEAD = _handle

    def log_message(self, fmt: str, *args: Any) -> str:
        return ""  # structured logs are emitted by the app


def _make_handler(app: App) -> type[_Handler]:
    return type("BoundHandler", (_Handler,), {"app": app})


# --------------------------------------------------------------------- client
class HttpUnavailable(Exception):
    """Transport level failure (connection refused, timeout, reset)."""


class HttpError(Exception):
    """Non-2xx response."""

    def __init__(self, status: int, body: Any, url: str) -> None:
        super().__init__(f"HTTP {status} from {url}: {body}")
        self.status = status
        self.body = body
        self.url = url


@dataclass
class HttpResult:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        if not self.body:
            return None
        return json.loads(self.body)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


class HttpClient:
    """Client with per-thread keep-alive connection reuse."""

    def __init__(self, timeout: float = 2.0, name: str = "http", retry_transport: bool = True) -> None:
        self.timeout = timeout
        self.name = name
        self.retry_transport = retry_transport
        self._local = threading.local()

    def _connection(self, scheme: str, host: str, port: int, timeout: float) -> http.client.HTTPConnection:
        pool: dict[tuple[str, str, int], http.client.HTTPConnection] = getattr(self._local, "pool", {})
        self._local.pool = pool
        key = (scheme, host, port)
        conn = pool.get(key)
        if conn is None:
            cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
            conn = cls(host, port, timeout=timeout)
            pool[key] = conn
        conn.timeout = timeout
        return conn

    def _drop(self, scheme: str, host: str, port: int) -> None:
        pool = getattr(self._local, "pool", {})
        conn = pool.pop((scheme, host, port), None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def request(
        self,
        method: str,
        url: str,
        body: Any = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        retry: bool | None = None,
    ) -> HttpResult:
        parsed = urlparse(url)
        scheme = parsed.scheme or "http"
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        if body is None:
            payload: bytes | None = None
        elif isinstance(body, (bytes, bytearray)):
            payload = bytes(body)
        else:
            payload = json.dumps(body).encode()
            headers = {"Content-Type": "application/json", **(headers or {})}
        attempt = 0
        attempts = 2 if (retry if retry is not None else self.retry_transport) else 1
        while True:
            attempt += 1
            conn = self._connection(scheme, host, port, timeout or self.timeout)
            try:
                conn.request(method.upper(), path, body=payload, headers=headers or {})
                raw = conn.getresponse()
                data = raw.read()
                result = HttpResult(raw.status, {k.lower(): v for k, v in raw.getheaders()}, data)
                if result.headers.get("connection", "").lower() == "close":
                    self._drop(scheme, host, port)
                return result
            except (http.client.HTTPException, OSError) as exc:
                self._drop(scheme, host, port)
                if attempt >= attempts:
                    raise HttpUnavailable(f"{method} {url} failed: {exc}") from exc
                time.sleep(0.005)

    def get_json(self, url: str, timeout: float | None = None) -> Any:
        result = self.request("GET", url, timeout=timeout)
        if not result.ok:
            raise HttpError(result.status, result.body, url)
        return result.json()


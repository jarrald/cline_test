"""Structured JSON logging with request correlation.

Kubernetes collects container stdout, so logs are emitted as one JSON object
per line. Every log record carries the service and pod name; request scoped
records also carry the propagated ``request_id`` and ``trace_id``.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextvars import ContextVar
from typing import Any

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)

_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, pod: str) -> None:
        super().__init__()
        self.service = service
        self.pod = pod

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(time.time(), 6),
            "level": record.levelname.lower(),
            "service": self.service,
            "pod": self.pod,
            "msg": record.getMessage(),
        }
        request_id = _request_id.get()
        trace_id = _trace_id.get()
        if request_id:
            payload["request_id"] = request_id
        if trace_id:
            payload["trace_id"] = trace_id
        extra = getattr(record, "fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure(service: str, pod: str | None = None) -> None:
    """Configure the root logger once per process."""
    pod = pod or os.environ.get("POD_NAME", "local-0")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service, pod))
    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(_LEVELS.get(os.environ.get("LOG_LEVEL", "info").lower(), logging.INFO))


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def bind(request_id: str | None, trace_id: str | None = None) -> None:
    _request_id.set(request_id)
    if trace_id:
        _trace_id.set(trace_id)


def current_request_id() -> str | None:
    return _request_id.get()


def fields(**values: Any) -> dict[str, Any]:
    """Use as ``logger.info("msg", extra=fields(key=value))``."""
    return {"fields": values}

"""12-factor configuration helpers.

Every service is configured exclusively through environment variables so that
the same container image can run locally, in Docker Compose and in Kubernetes
(where the manifests project ConfigMaps/Secrets into the environment).
"""

from __future__ import annotations

import json
import os
from typing import Any, Mapping


class ConfigError(RuntimeError):
    """Raised when a required setting is missing or malformed."""


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise ConfigError(f"missing required environment variable {name!r}")
    return value


def env_int(name: str, default: int | None = None) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        if default is None:
            raise ConfigError(f"missing required environment variable {name!r}")
        return default
    try:
        return int(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"{name!r} must be an integer, got {raw!r}") from exc


def env_float(name: str, default: float | None = None) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        if default is None:
            raise ConfigError(f"missing required environment variable {name!r}")
        return default
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - defensive
        raise ConfigError(f"{name!r} must be a float, got {raw!r}") from exc


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_list(name: str, default: list[str] | None = None) -> list[str]:
    raw = os.environ.get(name)
    if not raw:
        return list(default or [])
    return [item.strip() for item in raw.split(",") if item.strip()]


def env_json(name: str, default: Any = None) -> Any:
    raw = os.environ.get(name)
    if not raw:
        if default is None:
            raise ConfigError(f"missing required environment variable {name!r}")
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{name!r} must contain valid JSON") from exc


def pod_identity() -> tuple[str, int, int]:
    """Return (pod_name, ordinal, replica_count) for partition assignment."""
    name = env("POD_NAME", "local-0")
    ordinal = env_int("POD_ORDINAL", 0)
    count = env_int("POD_COUNT", 1)
    return name, ordinal, count


def as_dict(values: Mapping[str, Any]) -> dict[str, Any]:
    """Small helper used by tests to build env dictionaries."""
    return {str(k): str(v) for k, v in values.items()}

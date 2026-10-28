"""Resilience patterns: admission control, rate limiting, circuit breaking, retry.

These are the building blocks the API gateway composes when talking to
upstreams. All classes are thread safe and accept an injectable clock so the
unit tests can run without sleeping.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, TypeVar

T = TypeVar("T")


class TokenBucket:
    """Token bucket used for per-client rate limiting at the gateway.

    ``rate`` tokens per second are added up to ``capacity``. ``allow`` consumes
    one token and returns False when the caller must be throttled (HTTP 429).
    """

    def __init__(self, rate: float, capacity: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.rate = max(rate, 0.001)
        self.capacity = max(capacity, 1.0)
        self._clock = clock
        self._tokens = self.capacity
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._updated = now

    def allow(self, tokens: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def retry_after(self) -> float:
        with self._lock:
            self._refill()
            missing = max(0.0, 1.0 - self._tokens)
            return round(missing / self.rate, 3)


@dataclass
class BreakerStats:
    state: str
    opened: int = 0
    rejected: int = 0
    failures: int = 0


class CircuitBreaker:
    """Classic closed -> open -> half-open breaker.

    One shared instance is kept per upstream endpoint. When the failure
    threshold is reached the breaker opens for ``reset_timeout`` seconds and
    rejects calls immediately (fail fast), returning a 503 downstream.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._clock = clock
        self._lock = threading.Lock()
        self._state = self.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._opened = 0
        self._rejected = 0
        self._total_failures = 0
        self._half_open_probe = False

    @property
    def state(self) -> str:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if self._state == self.OPEN and self._clock() - self._opened_at >= self.reset_timeout:
            self._state = self.HALF_OPEN
            self._half_open_probe = False

    def allow(self) -> bool:
        with self._lock:
            self._maybe_half_open()
            if self._state == self.CLOSED:
                return True
            if self._state == self.OPEN:
                self._rejected += 1
                return False
            # half-open: allow exactly one probe at a time
            if self._half_open_probe:
                self._rejected += 1
                return False
            self._half_open_probe = True
            return True

    def record_success(self) -> None:
        with self._lock:
            self._state = self.CLOSED
            self._failures = 0
            self._half_open_probe = False

    def record_failure(self) -> None:
        with self._lock:
            self._total_failures += 1
            self._half_open_probe = False
            if self._state == self.HALF_OPEN:
                self._trip()
                return
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._trip()

    def _trip(self) -> None:
        self._state = self.OPEN
        self._opened_at = self._clock()
        self._opened += 1
        self._failures = 0

    def stats(self) -> BreakerStats:
        with self._lock:
            return BreakerStats(self._state, self._opened, self._rejected, self._total_failures)


def retry_call(
    fn: Callable[[], T],
    attempts: int = 3,
    base_delay: float = 0.05,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Exponential-backoff retry for idempotent operations."""
    last: BaseException | None = None
    for attempt in range(attempts):
        try:
            return fn()
        except retry_on as exc:  # noqa: PERF203 - explicit retry loop
            last = exc
            if attempt == attempts - 1:
                break
            sleep(base_delay * (2**attempt))
    assert last is not None
    raise last


@dataclass
class SlidingWindow:
    """Counts events in a rolling window; used for failure-rate breakers."""

    window_s: float = 10.0
    events: list[tuple[float, bool]] = field(default_factory=list)

    def add(self, success: bool, now: float | None = None) -> None:
        now = now if now is not None else time.monotonic()
        self.events.append((now, success))
        cutoff = now - self.window_s
        self.events = [e for e in self.events if e[0] >= cutoff]

    def failure_rate(self) -> float:
        if not self.events:
            return 0.0
        return sum(1 for _, ok in self.events if not ok) / len(self.events)

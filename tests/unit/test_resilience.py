import unittest

from libs.mesh.resilience import CircuitBreaker, TokenBucket, retry_call


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TokenBucketTest(unittest.TestCase):
    def test_allows_burst_then_throttles(self) -> None:
        clock = FakeClock()
        bucket = TokenBucket(rate=2.0, capacity=3.0, clock=clock)
        self.assertTrue(all(bucket.allow() for _ in range(3)))
        self.assertFalse(bucket.allow())
        clock.advance(0.5)
        self.assertTrue(bucket.allow())  # one token refilled
        self.assertFalse(bucket.allow())

    def test_retry_after_reports_wait(self) -> None:
        clock = FakeClock()
        bucket = TokenBucket(rate=1.0, capacity=1.0, clock=clock)
        bucket.allow()
        self.assertAlmostEqual(bucket.retry_after(), 1.0, places=3)

    def test_capacity_is_never_exceeded(self) -> None:
        clock = FakeClock()
        bucket = TokenBucket(rate=1.0, capacity=2.0, clock=clock)
        clock.advance(100)
        self.assertTrue(bucket.allow())
        self.assertTrue(bucket.allow())
        self.assertFalse(bucket.allow())


class CircuitBreakerTest(unittest.TestCase):
    def test_opens_after_threshold_and_recovers(self) -> None:
        clock = FakeClock()
        breaker = CircuitBreaker(failure_threshold=3, reset_timeout=5.0, clock=clock)
        for _ in range(2):
            breaker.record_failure()
        self.assertEqual(breaker.state, CircuitBreaker.CLOSED)
        breaker.record_failure()
        self.assertEqual(breaker.state, CircuitBreaker.OPEN)
        self.assertFalse(breaker.allow())
        self.assertEqual(breaker.stats().rejected, 1)
        clock.advance(5.0)
        self.assertTrue(breaker.allow())  # half-open probe
        self.assertFalse(breaker.allow())  # only one probe at a time
        breaker.record_success()
        self.assertEqual(breaker.state, CircuitBreaker.CLOSED)
        self.assertTrue(breaker.allow())

    def test_failed_probe_reopens_immediately(self) -> None:
        clock = FakeClock()
        breaker = CircuitBreaker(failure_threshold=1, reset_timeout=1.0, clock=clock)
        breaker.record_failure()
        clock.advance(1.0)
        self.assertTrue(breaker.allow())
        breaker.record_failure()
        self.assertEqual(breaker.state, CircuitBreaker.OPEN)
        self.assertEqual(breaker.stats().opened, 2)


class RetryTest(unittest.TestCase):
    def test_retries_until_success(self) -> None:
        attempts = {"n": 0}

        def flaky() -> str:
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("boom")
            return "ok"

        self.assertEqual(retry_call(flaky, attempts=3, base_delay=0, sleep=lambda _: None), "ok")
        self.assertEqual(attempts["n"], 3)

    def test_raises_last_error_after_budget(self) -> None:
        def always_fails() -> None:
            raise ValueError("nope")

        with self.assertRaises(ValueError):
            retry_call(always_fails, attempts=2, base_delay=0, sleep=lambda _: None)


if __name__ == "__main__":
    unittest.main()

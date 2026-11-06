import asyncio
import time


class RateLimiter:
    """Token bucket rate limiter with circuit breaker."""

    def __init__(
        self,
        rate_per_second: float = 10.0,
        burst: int = 10,
        circuit_threshold: int = 3,
        circuit_cooldown: float = 30.0,
    ):
        self.rate = rate_per_second
        self.burst = burst
        self.tokens = float(burst)
        self.last_refill = time.monotonic()
        self.circuit_threshold = circuit_threshold
        self.circuit_cooldown = circuit_cooldown

        self.circuit_failures = 0
        self.circuit_open_until = 0.0
        self._lock = asyncio.Lock()

        self.total_calls = 0
        self.total_waits = 0
        self.total_429s = 0

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

    async def acquire(self):
        async with self._lock:
            if self.circuit_open_until > time.monotonic():
                wait = self.circuit_open_until - time.monotonic()
                print(f"RateLimiter circuit OPEN — waiting {wait:.1f}s")
                await asyncio.sleep(wait)

            self._refill()

            if self.tokens < 1.0:
                wait = (1.0 - self.tokens) / self.rate
                self.total_waits += 1
                await asyncio.sleep(wait)
                self._refill()

            self.tokens -= 1.0
            self.total_calls += 1

    def record_success(self):
        self.circuit_failures = 0
        self.circuit_open_until = 0.0

    def record_429(self):
        self.total_429s += 1
        self.circuit_failures += 1
        if self.circuit_failures >= self.circuit_threshold:
            self.circuit_open_until = time.monotonic() + self.circuit_cooldown
            print(
                f"RateLimiter circuit OPEN after {self.circuit_failures} failures. "
                f"Cooling down {self.circuit_cooldown}s"
            )

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def stats(self):
        return {
            "calls": self.total_calls,
            "waits": self.total_waits,
            "429s": self.total_429s,
            "circuit_open": self.circuit_open_until > time.monotonic(),
        }


class DailyQuota:
    """Hard daily quota with per-minute sub-limit."""

    def __init__(self, max_per_day: int = 200, rate_per_minute: int = 10):
        self.max_per_day = max_per_day
        self.rate_per_minute = rate_per_minute
        self.used_today = 0
        self.minute_start = time.monotonic()
        self.minute_count = 0
        self.day_start = time.monotonic()
        self._lock = asyncio.Lock()

    def _reset_if_needed(self):
        now = time.monotonic()
        if now - self.minute_start >= 60:
            self.minute_start = now
            self.minute_count = 0
        if now - self.day_start >= 86400:
            self.day_start = now
            self.used_today = 0
            print("DailyQuota reset — full quota available again")

    def can_spend(self) -> bool:
        self._reset_if_needed()
        return (
            self.used_today < self.max_per_day
            and self.minute_count < self.rate_per_minute
        )

    async def acquire(self):
        async with self._lock:
            self._reset_if_needed()
            if self.used_today >= self.max_per_day:
                raise QuotaExhausted(
                    f"Daily quota exhausted: {self.used_today}/{self.max_per_day}"
                )
            if self.minute_count >= self.rate_per_minute:
                wait = 60 - (time.monotonic() - self.minute_start)
                if wait > 0:
                    await asyncio.sleep(wait)
                self.minute_start = time.monotonic()
                self.minute_count = 0
            self.used_today += 1
            self.minute_count += 1

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def remaining(self):
        self._reset_if_needed()
        return self.max_per_day - self.used_today

    def stats(self):
        self._reset_if_needed()
        return {
            "used_today": self.used_today,
            "remaining": self.max_per_day - self.used_today,
            "minute_count": self.minute_count,
        }


class QuotaExhausted(Exception):
    pass
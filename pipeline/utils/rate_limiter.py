"""Rate limiter for HTTP requests."""

from __future__ import annotations

import asyncio
import time
from collections import deque


class RateLimiter:
    """Token-bucket rate limiter.

    Args:
        requests_per_second: Maximum sustained request rate.
        burst: Maximum burst size (defaults to requests_per_second).
    """

    def __init__(self, requests_per_second: float = 2.0, burst: int | None = None):
        self.rate = requests_per_second
        self.burst = burst or max(1, int(requests_per_second))
        self._timestamps: deque[float] = deque()

    def wait(self) -> None:
        """Block until a request is allowed (synchronous)."""
        now = time.monotonic()
        # Remove timestamps outside the window
        window = 1.0 / self.rate * self.burst
        while self._timestamps and now - self._timestamps[0] > window:
            self._timestamps.popleft()

        if len(self._timestamps) >= self.burst:
            sleep_until = self._timestamps[0] + window
            sleep_time = sleep_until - now
            if sleep_time > 0:
                time.sleep(sleep_time)

        self._timestamps.append(time.monotonic())

    async def async_wait(self) -> None:
        """Yield until a request is allowed (async)."""
        now = time.monotonic()
        window = 1.0 / self.rate * self.burst
        while self._timestamps and now - self._timestamps[0] > window:
            self._timestamps.popleft()

        if len(self._timestamps) >= self.burst:
            sleep_until = self._timestamps[0] + window
            sleep_time = sleep_until - now
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

        self._timestamps.append(time.monotonic())

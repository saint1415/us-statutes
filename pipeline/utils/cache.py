"""HTTP response caching for pipeline downloads."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path

import httpx

from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

DEFAULT_TTL = 7 * 24 * 3600  # 7 days


class HttpCache:
    """Disk-backed HTTP response cache with rate limiting.

    Args:
        cache_dir: Directory for cached responses.
        ttl: Time-to-live in seconds for cached responses.
        rate_limiter: Optional rate limiter for requests.
    """

    def __init__(
        self,
        cache_dir: Path,
        ttl: int = DEFAULT_TTL,
        rate_limiter: RateLimiter | None = None,
    ):
        self.cache_dir = cache_dir
        self.ttl = ttl
        self.rate_limiter = rate_limiter or RateLimiter()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()

    def _cache_path(self, url: str) -> Path:
        key = self._cache_key(url)
        return self.cache_dir / f"{key}.json"

    def _meta_path(self, url: str) -> Path:
        key = self._cache_key(url)
        return self.cache_dir / f"{key}.meta.json"

    def get_cached(self, url: str) -> str | None:
        """Return cached response body if valid, else None."""
        meta_path = self._meta_path(url)
        cache_path = self._cache_path(url)

        if not meta_path.exists() or not cache_path.exists():
            return None

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if time.time() - meta.get("timestamp", 0) > self.ttl:
            logger.debug("Cache expired for %s", url)
            return None

        logger.debug("Cache hit for %s", url)
        return cache_path.read_text(encoding="utf-8")

    def put(self, url: str, body: str, status_code: int = 200) -> None:
        """Store a response in the cache."""
        cache_path = self._cache_path(url)
        meta_path = self._meta_path(url)

        cache_path.write_text(body, encoding="utf-8")
        meta = {
            "url": url,
            "timestamp": time.time(),
            "status_code": status_code,
        }
        meta_path.write_text(json.dumps(meta), encoding="utf-8")

    def fetch(self, url: str, **kwargs) -> str:
        """Fetch URL with caching and rate limiting.

        Args:
            url: URL to fetch.
            **kwargs: Additional arguments passed to httpx.get().

        Returns:
            Response body as string.

        Raises:
            httpx.HTTPStatusError: On non-2xx response.
        """
        cached = self.get_cached(url)
        if cached is not None:
            return cached

        self.rate_limiter.wait()
        logger.info("Fetching %s", url)

        kwargs.setdefault("timeout", 60)
        kwargs.setdefault("follow_redirects", True)
        kwargs.setdefault("headers", {
            "User-Agent": "us-statutes-bot/1.0 (+https://github.com/saint1415/us-statutes)"
        })

        response = httpx.get(url, **kwargs)
        response.raise_for_status()

        body = response.text
        self.put(url, body, response.status_code)
        return body

    def fetch_bytes(self, url: str, **kwargs) -> bytes:
        """Fetch URL and return raw bytes (for binary downloads).

        Results are cached as files with .bin extension.
        """
        key = self._cache_key(url)
        bin_path = self.cache_dir / f"{key}.bin"
        meta_path = self._meta_path(url)

        if meta_path.exists() and bin_path.exists():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if time.time() - meta.get("timestamp", 0) <= self.ttl:
                logger.debug("Cache hit (binary) for %s", url)
                return bin_path.read_bytes()

        self.rate_limiter.wait()
        logger.info("Fetching (binary) %s", url)

        kwargs.setdefault("timeout", 120)
        kwargs.setdefault("follow_redirects", True)
        kwargs.setdefault("headers", {
            "User-Agent": "us-statutes-bot/1.0 (+https://github.com/saint1415/us-statutes)"
        })

        response = httpx.get(url, **kwargs)
        response.raise_for_status()

        bin_path.write_bytes(response.content)
        meta = {"url": url, "timestamp": time.time(), "status_code": response.status_code}
        meta_path.write_text(json.dumps(meta), encoding="utf-8")

        return response.content

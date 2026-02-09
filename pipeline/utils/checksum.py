"""Checksum utilities for detecting changes between ingestion runs."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def file_hash(path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def content_hash(text: str) -> str:
    """Compute SHA-256 hash of a string."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class ChangeDetector:
    """Tracks content checksums to detect changes between runs.

    Stores a JSON file mapping keys to their last-known hashes.
    """

    def __init__(self, checksum_file: Path):
        self.checksum_file = checksum_file
        self._checksums: dict[str, str] = {}
        if checksum_file.exists():
            self._checksums = json.loads(checksum_file.read_text(encoding="utf-8"))

    def has_changed(self, key: str, current_hash: str) -> bool:
        """Check if content has changed since last recorded hash."""
        previous = self._checksums.get(key)
        return previous != current_hash

    def update(self, key: str, current_hash: str) -> None:
        """Record the current hash for a key."""
        self._checksums[key] = current_hash

    def save(self) -> None:
        """Persist checksums to disk."""
        self.checksum_file.parent.mkdir(parents=True, exist_ok=True)
        self.checksum_file.write_text(
            json.dumps(self._checksums, indent=2),
            encoding="utf-8",
        )
        logger.debug("Saved checksums to %s", self.checksum_file)

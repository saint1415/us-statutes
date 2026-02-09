"""Base ingestor and canonical data structures for statute ingestion."""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Section:
    """A single statute section (the atomic unit of law)."""

    id: str
    number: str
    heading: str
    text: str
    history: str = ""
    source_url: str = ""


@dataclass
class Chapter:
    """A chapter containing sections."""

    id: str
    number: str
    heading: str
    sections: list[Section] = field(default_factory=list)


@dataclass
class Title:
    """A title (or equivalent top-level division) containing chapters."""

    id: str
    number: str
    heading: str
    chapters: list[Chapter] = field(default_factory=list)


@dataclass
class StructureLevel:
    """Describes one level of a state code's hierarchy."""

    level: str
    label: str


@dataclass
class StateCode:
    """The complete parsed statute code for one state."""

    state: str
    state_abbr: str
    code_name: str
    source: str
    source_url: str
    year: int
    structure: list[StructureLevel]
    titles: list[Title] = field(default_factory=list)
    last_updated: Optional[datetime] = None

    def __post_init__(self):
        if self.last_updated is None:
            self.last_updated = datetime.now(timezone.utc)


class BaseIngestor(abc.ABC):
    """Abstract base class for all statute ingestors.

    Subclasses must implement fetch() and parse().
    """

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        self.state = state
        self.config = config
        self.cache_dir = cache_dir or Path("cache")
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abc.abstractmethod
    def fetch(self) -> Path:
        """Download raw data and return path to cached files.

        Returns:
            Path to directory containing downloaded raw data.
        """

    @abc.abstractmethod
    def parse(self, raw_path: Path) -> StateCode:
        """Parse raw downloaded data into canonical StateCode.

        Args:
            raw_path: Path returned by fetch().

        Returns:
            Fully populated StateCode instance.
        """

    def ingest(self) -> StateCode:
        """Run the full ingestion pipeline: fetch then parse."""
        self.logger.info("Starting ingestion for %s", self.state)
        raw_path = self.fetch()
        self.logger.info("Fetched raw data to %s", raw_path)
        state_code = self.parse(raw_path)
        self.logger.info(
            "Parsed %d titles, %d chapters, %d sections for %s",
            len(state_code.titles),
            sum(len(t.chapters) for t in state_code.titles),
            sum(len(c.sections) for t in state_code.titles for c in t.chapters),
            self.state,
        )
        return state_code

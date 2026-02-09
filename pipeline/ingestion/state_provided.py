"""Ingestor for states that provide their own bulk statute data.

Supports: CT, FL, MD, NE
Each state has a unique format requiring custom handling.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from bs4 import BeautifulSoup

from .base import BaseIngestor, Chapter, Section, StateCode, StructureLevel, Title
from ..normalization.text_cleaner import clean_text, clean_section_number
from ..utils.cache import HttpCache
from ..utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class StateProvidedIngestor(BaseIngestor):
    """Ingest statutes from state-provided bulk data sources.

    Dispatches to state-specific parsing logic based on the state slug.
    """

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        super().__init__(state, config, cache_dir)
        self.http_cache = HttpCache(
            cache_dir=(cache_dir or Path("cache")) / "http",
            rate_limiter=RateLimiter(requests_per_second=1.5),
        )

    def fetch(self) -> Path:
        """Download statute data from the state's official source."""
        raw_dir = self.cache_dir / "raw" / self.state
        raw_dir.mkdir(parents=True, exist_ok=True)

        handlers = {
            "connecticut": self._fetch_connecticut,
            "florida": self._fetch_florida,
            "maryland": self._fetch_maryland,
            "nebraska": self._fetch_nebraska,
        }

        handler = handlers.get(self.state)
        if handler is None:
            raise ValueError(f"No state_provided handler for {self.state}")

        return handler(raw_dir)

    def parse(self, raw_path: Path) -> StateCode:
        """Parse downloaded data into a StateCode."""
        handlers = {
            "connecticut": self._parse_connecticut,
            "florida": self._parse_florida,
            "maryland": self._parse_maryland,
            "nebraska": self._parse_nebraska,
        }

        handler = handlers.get(self.state)
        if handler is None:
            raise ValueError(f"No state_provided parser for {self.state}")

        titles = handler(raw_path)

        return StateCode(
            state=self.state,
            state_abbr=self.config.get("state_abbr", ""),
            code_name=self.config.get("code_name", ""),
            source="state_provided",
            source_url=self.config["url"],
            year=self.config.get("year", 2024),
            structure=self.config.get("structure", [
                StructureLevel("title", "Title"),
                StructureLevel("chapter", "Chapter"),
                StructureLevel("section", "Section"),
            ]),
            titles=titles,
        )

    # === Connecticut ===

    def _fetch_connecticut(self, raw_dir: Path) -> Path:
        """Fetch CT statutes from cga.ct.gov."""
        base_url = "https://www.cga.ct.gov/current/pub"
        out_dir = raw_dir / "html"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Get titles index
        index_html = self.http_cache.fetch(f"{base_url}/titles.htm")
        (out_dir / "titles.htm").write_text(index_html, encoding="utf-8")

        # Parse title links
        soup = BeautifulSoup(index_html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.startswith("title") and href.endswith(".htm"):
                try:
                    title_html = self.http_cache.fetch(f"{base_url}/{href}")
                    (out_dir / href).write_text(title_html, encoding="utf-8")
                except Exception as e:
                    logger.warning("Failed to fetch CT title %s: %s", href, e)

        return out_dir

    def _parse_connecticut(self, raw_path: Path) -> list[Title]:
        """Parse CT HTML statute files."""
        titles = []
        for html_file in sorted(raw_path.glob("title*.htm")):
            try:
                content = html_file.read_text(encoding="utf-8", errors="replace")
                title = self._parse_ct_title(content, html_file.stem)
                if title and title.chapters:
                    titles.append(title)
            except Exception as e:
                logger.warning("Failed to parse CT file %s: %s", html_file, e)
        return titles

    def _parse_ct_title(self, html: str, filename: str) -> Title | None:
        """Parse a single CT title HTML page."""
        soup = BeautifulSoup(html, "html.parser")

        title_heading = ""
        h1 = soup.find("h1") or soup.find("h2")
        if h1:
            title_heading = h1.get_text(strip=True)

        # Extract title number from filename or heading
        num_match = re.search(r"(\d+)", filename)
        title_num = num_match.group(1) if num_match else filename

        sections = _extract_sections_from_html(soup)
        if not sections:
            return None

        title_id = f"title-{title_num}"
        return Title(
            id=title_id,
            number=title_num,
            heading=title_heading or f"Title {title_num}",
            chapters=[Chapter(
                id=f"chapter-{title_num}",
                number=title_num,
                heading=title_heading or f"Title {title_num}",
                sections=sections,
            )],
        )

    # === Florida ===

    def _fetch_florida(self, raw_dir: Path) -> Path:
        """Fetch FL statutes from leg.state.fl.us."""
        base_url = "http://www.leg.state.fl.us/statutes"
        out_dir = raw_dir / "html"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Get main index
        index_html = self.http_cache.fetch(f"{base_url}/index.cfm?App_mode=Display_Index&Title_Request=I&Sort=T")
        (out_dir / "index.html").write_text(index_html, encoding="utf-8")

        # Parse title links
        soup = BeautifulSoup(index_html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "Title_Request" in href or "StatutesBrowser" in href:
                url = href if href.startswith("http") else f"{base_url}/{href}"
                try:
                    page = self.http_cache.fetch(url)
                    safe_name = re.sub(r"[^\w.-]", "_", href.split("?")[-1][:80]) + ".html"
                    (out_dir / safe_name).write_text(page, encoding="utf-8")
                except Exception as e:
                    logger.debug("Failed to fetch FL page %s: %s", href, e)

        return out_dir

    def _parse_florida(self, raw_path: Path) -> list[Title]:
        """Parse FL HTML statute files."""
        titles = []
        for html_file in sorted(raw_path.glob("*.html")):
            try:
                content = html_file.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")
                sections = _extract_sections_from_html(soup)
                if sections:
                    title_num = html_file.stem[:20]
                    h1 = soup.find("h1") or soup.find("h2")
                    heading = h1.get_text(strip=True) if h1 else title_num

                    titles.append(Title(
                        id=f"title-{_slugify(title_num)}",
                        number=title_num,
                        heading=heading,
                        chapters=[Chapter(
                            id=f"chapter-{_slugify(title_num)}",
                            number=title_num,
                            heading=heading,
                            sections=sections,
                        )],
                    ))
            except Exception as e:
                logger.warning("Failed to parse FL file %s: %s", html_file, e)
        return titles

    # === Maryland ===

    def _fetch_maryland(self, raw_dir: Path) -> Path:
        """Fetch MD statutes from mgaleg.maryland.gov."""
        base_url = "https://mgaleg.maryland.gov/mgawebsite/Laws/StatuteText"
        out_dir = raw_dir / "html"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Maryland uses article-based organization
        articles = [
            "gag", "gal", "gbr", "gcj", "gcl", "gcm", "gcr", "ged",
            "gel", "gen", "get", "gfi", "gfr", "ghg", "gho", "ghs",
            "ghu", "gin", "gis", "glg", "gnr", "gpp", "gps", "grp",
            "gsg", "gsf", "gtg", "gtp", "gtl", "gtr",
        ]

        for article in articles:
            try:
                url = f"{base_url}/{article}/0"
                page = self.http_cache.fetch(url)
                (out_dir / f"{article}.html").write_text(page, encoding="utf-8")
            except Exception as e:
                logger.debug("Failed to fetch MD article %s: %s", article, e)

        return out_dir

    def _parse_maryland(self, raw_path: Path) -> list[Title]:
        """Parse MD HTML statute files."""
        titles = []
        for html_file in sorted(raw_path.glob("*.html")):
            try:
                content = html_file.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")
                sections = _extract_sections_from_html(soup)
                if sections:
                    article = html_file.stem
                    h1 = soup.find("h1") or soup.find("h2")
                    heading = h1.get_text(strip=True) if h1 else article

                    titles.append(Title(
                        id=f"title-{article}",
                        number=article,
                        heading=heading,
                        chapters=[Chapter(
                            id=f"chapter-{article}",
                            number=article,
                            heading=heading,
                            sections=sections,
                        )],
                    ))
            except Exception as e:
                logger.warning("Failed to parse MD file %s: %s", html_file, e)
        return titles

    # === Nebraska ===

    def _fetch_nebraska(self, raw_dir: Path) -> Path:
        """Fetch NE statutes from nebraskalegislature.gov."""
        base_url = "https://nebraskalegislature.gov/laws/browse-statutes.php"
        out_dir = raw_dir / "html"
        out_dir.mkdir(parents=True, exist_ok=True)

        index_html = self.http_cache.fetch(base_url)
        (out_dir / "index.html").write_text(index_html, encoding="utf-8")

        # Parse chapter links
        soup = BeautifulSoup(index_html, "html.parser")
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "chapter" in href.lower() or "statutes.php" in href.lower():
                url = href if href.startswith("http") else f"https://nebraskalegislature.gov{href}"
                try:
                    page = self.http_cache.fetch(url)
                    safe_name = re.sub(r"[^\w.-]", "_", href.split("/")[-1].split("?")[0])[:80] + ".html"
                    (out_dir / safe_name).write_text(page, encoding="utf-8")
                except Exception as e:
                    logger.debug("Failed to fetch NE page %s: %s", href, e)

        return out_dir

    def _parse_nebraska(self, raw_path: Path) -> list[Title]:
        """Parse NE HTML statute files."""
        titles = []
        for html_file in sorted(raw_path.glob("*.html")):
            if html_file.name == "index.html":
                continue
            try:
                content = html_file.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")
                sections = _extract_sections_from_html(soup)
                if sections:
                    chapter = html_file.stem
                    h1 = soup.find("h1") or soup.find("h2")
                    heading = h1.get_text(strip=True) if h1 else chapter

                    titles.append(Title(
                        id=f"title-{_slugify(chapter)}",
                        number=chapter,
                        heading=heading,
                        chapters=[Chapter(
                            id=f"chapter-{_slugify(chapter)}",
                            number=chapter,
                            heading=heading,
                            sections=sections,
                        )],
                    ))
            except Exception as e:
                logger.warning("Failed to parse NE file %s: %s", html_file, e)
        return titles


def _extract_sections_from_html(soup: BeautifulSoup) -> list[Section]:
    """Generic HTML section extractor - looks for common patterns."""
    sections = []
    seen_numbers = set()

    # Strategy 1: look for elements with section numbers in text
    for elem in soup.find_all(["p", "div", "li", "span", "td"]):
        text = elem.get_text(strip=True)
        if not text:
            continue

        # Match section number patterns
        match = re.match(
            r"(?:§|Sec(?:tion)?\.?\s*)([\d\-\.a-zA-Z]+)\s*[.\s]+(.*)",
            text,
        )
        if match:
            num = clean_section_number(match.group(1))
            if not num or num in seen_numbers:
                continue
            seen_numbers.add(num)

            rest = match.group(2)
            lines = rest.split("\n", 1)
            heading = lines[0].strip()[:300]
            body = lines[1].strip() if len(lines) > 1 else ""

            sections.append(Section(
                id=f"section-{_slugify(num)}",
                number=num,
                heading=heading,
                text=clean_text(body or rest),
            ))

    return sections


def _slugify(text: str) -> str:
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-") or "unknown"

"""Justia web scraper for ~31 states.

Scrapes statute hierarchies from law.justia.com/codes/{state}/.
Handles per-state structural variations with adaptive parsing.
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseIngestor, Chapter, Section, StateCode, StructureLevel, Title
from ..normalization.text_cleaner import clean_text, clean_section_number
from ..utils.cache import HttpCache
from ..utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

JUSTIA_BASE = "https://law.justia.com/codes/"


class JustiaIngestor(BaseIngestor):
    """Scrape statutes from Justia's free law resources."""

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        super().__init__(state, config, cache_dir)
        self.http_cache = HttpCache(
            cache_dir=(cache_dir or Path("cache")) / "http",
            ttl=7 * 24 * 3600,  # 7 day cache
            rate_limiter=RateLimiter(requests_per_second=1.0, burst=2),
        )
        self.base_url = self.config["url"].rstrip("/") + "/"
        self.max_retries = 3
        self.retry_delay = 5

    def fetch(self) -> Path:
        """Scrape the state's statute pages from Justia."""
        raw_dir = self.cache_dir / "raw" / self.state / "justia"
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Fetch the state index page
        index_html = self._fetch_with_retry(self.base_url)
        index_path = raw_dir / "index.html"
        index_path.write_text(index_html, encoding="utf-8")

        # Step 2: Parse the index to find title/chapter links
        soup = BeautifulSoup(index_html, "html.parser")
        title_links = self._extract_title_links(soup)

        logger.info("Found %d top-level links for %s", len(title_links), self.state)

        # Step 3: Fetch each title page
        for i, (title_url, title_name) in enumerate(title_links):
            safe_name = _slugify(title_name or f"title-{i}")
            title_dir = raw_dir / safe_name
            title_dir.mkdir(parents=True, exist_ok=True)

            try:
                title_html = self._fetch_with_retry(title_url)
                (title_dir / "index.html").write_text(title_html, encoding="utf-8")

                # Parse title page for chapter links
                title_soup = BeautifulSoup(title_html, "html.parser")
                chapter_links = self._extract_chapter_links(title_soup, title_url)

                # Fetch each chapter/section page
                for j, (ch_url, ch_name) in enumerate(chapter_links):
                    safe_ch = _slugify(ch_name or f"chapter-{j}")
                    try:
                        ch_html = self._fetch_with_retry(ch_url)
                        (title_dir / f"{safe_ch}.html").write_text(ch_html, encoding="utf-8")
                    except Exception as e:
                        logger.warning("Failed to fetch chapter %s: %s", ch_url, e)

            except Exception as e:
                logger.warning("Failed to fetch title %s: %s", title_url, e)

        return raw_dir

    def parse(self, raw_path: Path) -> StateCode:
        """Parse scraped Justia HTML into a StateCode."""
        titles = []

        # Each subdirectory is a title
        title_dirs = sorted(
            [d for d in raw_path.iterdir() if d.is_dir()],
            key=lambda d: _sort_key(d.name),
        )

        for title_dir in title_dirs:
            title = self._parse_title_dir(title_dir)
            if title and title.chapters:
                titles.append(title)

        return StateCode(
            state=self.state,
            state_abbr=self.config.get("state_abbr", ""),
            code_name=self.config.get("code_name", ""),
            source="justia",
            source_url=self.base_url,
            year=self.config.get("year", 2024),
            structure=self.config.get("structure", [
                StructureLevel("title", "Title"),
                StructureLevel("chapter", "Chapter"),
                StructureLevel("section", "Section"),
            ]),
            titles=titles,
        )

    def _fetch_with_retry(self, url: str) -> str:
        """Fetch URL with exponential backoff retry."""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return self.http_cache.fetch(url)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.info("Retry %d for %s in %ds: %s", attempt + 1, url, delay, e)
                    time.sleep(delay)
        raise last_error

    def _extract_title_links(self, soup: BeautifulSoup) -> list[tuple[str, str]]:
        """Extract title/top-level division links from the state index page."""
        links = []

        # Justia uses various layouts. Look for the main content area.
        content = soup.find("div", class_="codes-listing") or soup.find("div", id="codes") or soup

        for a in content.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)

            # Filter for actual code links (not navigation, not external)
            if not text or len(text) < 2:
                continue
            if href.startswith("#") or href.startswith("javascript"):
                continue

            full_url = urljoin(self.base_url, href)
            if "law.justia.com/codes/" in full_url and full_url != self.base_url:
                # Must be deeper than the state index
                if full_url.rstrip("/").count("/") > self.base_url.rstrip("/").count("/"):
                    links.append((full_url, text))

        # Deduplicate
        seen = set()
        unique = []
        for url, name in links:
            normalized = url.rstrip("/")
            if normalized not in seen:
                seen.add(normalized)
                unique.append((url, name))

        return unique

    def _extract_chapter_links(self, soup: BeautifulSoup, parent_url: str) -> list[tuple[str, str]]:
        """Extract chapter/sub-division links from a title page."""
        links = []
        content = soup.find("div", class_="codes-listing") or soup.find("div", id="codes") or soup

        for a in content.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if not text or href.startswith("#") or href.startswith("javascript"):
                continue

            full_url = urljoin(parent_url, href)
            if "law.justia.com/codes/" in full_url and full_url != parent_url:
                if full_url.rstrip("/").count("/") > parent_url.rstrip("/").count("/"):
                    links.append((full_url, text))

        seen = set()
        unique = []
        for url, name in links:
            normalized = url.rstrip("/")
            if normalized not in seen:
                seen.add(normalized)
                unique.append((url, name))

        return unique

    def _parse_title_dir(self, title_dir: Path) -> Title | None:
        """Parse a title directory containing scraped HTML files."""
        title_num = title_dir.name
        title_heading = ""

        # Get heading from index
        index_file = title_dir / "index.html"
        if index_file.exists():
            try:
                soup = BeautifulSoup(index_file.read_text(encoding="utf-8", errors="replace"), "html.parser")
                h1 = soup.find("h1") or soup.find("h2")
                if h1:
                    title_heading = h1.get_text(strip=True)
            except Exception:
                pass

        # Parse chapter files
        chapters = []
        chapter_files = sorted(
            [f for f in title_dir.glob("*.html") if f.name != "index.html"],
            key=lambda f: _sort_key(f.stem),
        )

        for ch_file in chapter_files:
            chapter = self._parse_chapter_file(ch_file)
            if chapter:
                chapters.append(chapter)

        # If no chapter files, try to extract sections from the index page itself
        if not chapters and index_file.exists():
            try:
                soup = BeautifulSoup(index_file.read_text(encoding="utf-8", errors="replace"), "html.parser")
                sections = self._extract_sections_from_page(soup)
                if sections:
                    chapters.append(Chapter(
                        id=f"chapter-{title_num}",
                        number=title_num,
                        heading=title_heading or f"Chapter {title_num}",
                        sections=sections,
                    ))
            except Exception:
                pass

        if not chapters:
            return None

        # Clean up title number from slug
        display_num = title_num.replace("-", " ").strip()
        num_match = re.match(r"(?:title-?)?(\d+[a-zA-Z]*)", title_num)
        if num_match:
            display_num = num_match.group(1)

        title_id = f"title-{title_num}"
        return Title(
            id=title_id,
            number=display_num,
            heading=title_heading or f"Title {display_num}",
            chapters=chapters,
        )

    def _parse_chapter_file(self, ch_file: Path) -> Chapter | None:
        """Parse a chapter HTML file for sections."""
        try:
            content = ch_file.read_text(encoding="utf-8", errors="replace")
            soup = BeautifulSoup(content, "html.parser")

            # Get chapter heading
            heading = ""
            h1 = soup.find("h1") or soup.find("h2")
            if h1:
                heading = h1.get_text(strip=True)

            sections = self._extract_sections_from_page(soup)
            if not sections:
                return None

            ch_num = ch_file.stem
            num_match = re.match(r"(?:chapter-?)?(\d+[a-zA-Z]*)", ch_num)
            if num_match:
                ch_num = num_match.group(1)

            return Chapter(
                id=f"chapter-{_slugify(ch_file.stem)}",
                number=ch_num,
                heading=heading or f"Chapter {ch_num}",
                sections=sections,
            )

        except Exception as e:
            logger.warning("Failed to parse chapter file %s: %s", ch_file, e)
            return None

    def _extract_sections_from_page(self, soup: BeautifulSoup) -> list[Section]:
        """Extract statute sections from a Justia page.

        Justia section pages typically have the section text in the main content
        div with specific CSS classes.
        """
        sections = []

        # Strategy 1: Look for Justia's section divs
        section_divs = soup.find_all("div", class_="codes-section")
        if not section_divs:
            section_divs = soup.find_all("div", class_="statute-section")

        for div in section_divs:
            section = self._parse_justia_section_div(div)
            if section:
                sections.append(section)

        if sections:
            return sections

        # Strategy 2: Look for section content in the main text area
        main_content = (
            soup.find("div", class_="codes-primary-content") or
            soup.find("div", id="content") or
            soup.find("div", class_="content") or
            soup.find("main")
        )

        if main_content:
            # Look for section patterns in paragraphs
            current_section = None
            text_parts = []

            for elem in main_content.children:
                if not hasattr(elem, "get_text"):
                    continue
                text = elem.get_text(strip=True)
                if not text:
                    continue

                # Check for section number header
                match = re.match(
                    r"(?:§|Section)\s*([\d\-\.a-zA-Z]+)\s*[-.\s]+(.*)",
                    text,
                )
                if match:
                    # Save previous section
                    if current_section:
                        current_section.text = clean_text("\n\n".join(text_parts))
                        if current_section.text:
                            sections.append(current_section)

                    num = clean_section_number(match.group(1))
                    heading = match.group(2).strip()
                    # Truncate heading at first period if very long
                    if len(heading) > 200:
                        dot = heading.find(".", 50)
                        if dot > 0:
                            heading = heading[:dot]

                    current_section = Section(
                        id=f"section-{_slugify(num)}",
                        number=num,
                        heading=heading,
                        text="",
                    )
                    text_parts = []
                elif current_section:
                    text_parts.append(text)

            # Save last section
            if current_section:
                current_section.text = clean_text("\n\n".join(text_parts))
                if current_section.text:
                    sections.append(current_section)

        # Strategy 3: The whole page might be one section
        if not sections:
            h1 = soup.find("h1")
            if h1:
                heading_text = h1.get_text(strip=True)
                match = re.match(
                    r"(?:§|Section)\s*([\d\-\.a-zA-Z]+)\s*[-.\s]+(.*)",
                    heading_text,
                )
                if match:
                    num = clean_section_number(match.group(1))
                    heading = match.group(2).strip()

                    # Get body text
                    body = soup.find("div", class_="codes-body") or soup.find("div", class_="content")
                    text = clean_text(body.get_text() if body else "")

                    if text:
                        sections.append(Section(
                            id=f"section-{_slugify(num)}",
                            number=num,
                            heading=heading,
                            text=text,
                            source_url=self.base_url,
                        ))

        return sections

    def _parse_justia_section_div(self, div) -> Section | None:
        """Parse a Justia section div element."""
        # Get section number and heading
        heading_elem = div.find(["h2", "h3", "h4"])
        if not heading_elem:
            return None

        heading_text = heading_elem.get_text(strip=True)
        match = re.match(
            r"(?:§|Section)\s*([\d\-\.a-zA-Z]+)\s*[-.\s]*(.*)",
            heading_text,
        )

        if match:
            num = clean_section_number(match.group(1))
            heading = match.group(2).strip()
        else:
            # Try splitting on first dash/period
            parts = re.split(r"\s*[-–—.]\s*", heading_text, 1)
            num = clean_section_number(parts[0])
            heading = parts[1] if len(parts) > 1 else ""

        if not num:
            return None

        # Get text content (everything except the heading)
        heading_elem.decompose()
        text = clean_text(div.get_text())

        # Extract history if present
        history = ""
        history_elem = div.find(class_="history") or div.find(class_="source-note")
        if history_elem:
            history = clean_text(history_elem.get_text())

        # Get source URL from any link
        source_url = ""
        link = div.find("a", href=True)
        if link and "justia.com" in link["href"]:
            source_url = link["href"]

        return Section(
            id=f"section-{_slugify(num)}",
            number=num,
            heading=heading,
            text=text,
            history=history,
            source_url=source_url,
        )


def _slugify(text: str) -> str:
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-") or "unknown"


def _sort_key(name: str):
    """Sort key that handles mixed numeric/alpha names."""
    parts = re.split(r"(\d+)", name)
    return [(0, int(p)) if p.isdigit() else (1, p) for p in parts]

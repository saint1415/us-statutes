"""DC Council ingestor - parses DC law XML from GitHub."""

from __future__ import annotations

import logging
import os
import re
import zipfile
from io import BytesIO
from pathlib import Path

from lxml import etree

from .base import BaseIngestor, Chapter, Section, StateCode, StructureLevel, Title
from ..normalization.text_cleaner import clean_text
from ..utils.cache import HttpCache
from ..utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

NS = {"dc": "https://code.dccouncil.us/schemas/dc-library"}

# The law-xml-codified repo has the complete codified DC Code
REPO_ZIP_URL = "https://github.com/DCCouncil/law-xml-codified/archive/refs/heads/master.zip"


class DCCouncilIngestor(BaseIngestor):
    """Ingest DC statutes from the DCCouncil/law-xml-codified GitHub repository."""

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        super().__init__(state, config, cache_dir)
        self.http_cache = HttpCache(
            cache_dir=(cache_dir or Path("cache")) / "http",
            rate_limiter=RateLimiter(requests_per_second=5.0),
        )

    def fetch(self) -> Path:
        """Download the law-xml-codified repo as a zip archive."""
        raw_dir = self.cache_dir / "raw" / "dc"
        raw_dir.mkdir(parents=True, exist_ok=True)

        zip_path = raw_dir / "law-xml-codified.zip"
        extract_dir = raw_dir / "law-xml-codified"

        if extract_dir.exists() and any(extract_dir.iterdir()):
            logger.info("Using cached DC law-xml-codified at %s", extract_dir)
            return extract_dir

        logger.info("Downloading DC law-xml-codified from GitHub...")
        data = self.http_cache.fetch_bytes(REPO_ZIP_URL)

        zip_path.write_bytes(data)
        logger.info("Downloaded %d bytes", len(data))

        # Extract
        with zipfile.ZipFile(BytesIO(data)) as zf:
            zf.extractall(raw_dir)

        # GitHub zips have a top-level directory like "law-xml-codified-master"
        extracted = [d for d in raw_dir.iterdir() if d.is_dir() and "law-xml-codified" in d.name]
        if extracted and extracted[0] != extract_dir:
            extracted[0].rename(extract_dir)

        logger.info("Extracted to %s", extract_dir)
        return extract_dir

    def parse(self, raw_path: Path) -> StateCode:
        """Parse the extracted DC law XML into a StateCode."""
        # Find the code directory
        code_dir = self._find_code_dir(raw_path)
        if code_dir is None:
            raise FileNotFoundError(f"Could not find DC Code directory in {raw_path}")

        logger.info("Parsing DC Code from %s", code_dir)

        # Parse the code index to find titles
        titles = self._parse_titles(code_dir)

        return StateCode(
            state="district-of-columbia",
            state_abbr="DC",
            code_name=self.config.get("code_name", "District of Columbia Official Code"),
            source="dc_council",
            source_url="https://github.com/DCCouncil/law-xml-codified",
            year=self.config.get("year", 2024),
            structure=self.config.get("structure", [
                StructureLevel("title", "Title"),
                StructureLevel("chapter", "Chapter"),
                StructureLevel("section", "Section"),
            ]),
            titles=titles,
        )

    def _find_code_dir(self, raw_path: Path) -> Path | None:
        """Locate the DC Code titles directory."""
        # Try standard paths
        candidates = [
            raw_path / "us" / "dc" / "council" / "code" / "titles",
            raw_path / "dc" / "council" / "code" / "titles",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate

        # Search for titles directory
        for p in raw_path.rglob("titles"):
            if p.is_dir() and (p / "1").exists():
                return p

        return None

    def _parse_titles(self, titles_dir: Path) -> list[Title]:
        """Parse all titles from the titles directory."""
        titles = []

        # Title directories are numbered
        title_dirs = sorted(
            [d for d in titles_dir.iterdir() if d.is_dir()],
            key=lambda d: self._sort_key(d.name),
        )

        for title_dir in title_dirs:
            title = self._parse_title(title_dir)
            if title and title.chapters:
                titles.append(title)
                logger.debug(
                    "Parsed title %s: %s (%d chapters)",
                    title.number, title.heading, len(title.chapters),
                )

        return titles

    def _parse_title(self, title_dir: Path) -> Title | None:
        """Parse a single title directory."""
        title_num = title_dir.name
        title_id = f"title-{title_num}"

        # Try to get title heading from index.xml
        heading = self._get_title_heading(title_dir)

        # Find all section XML files
        sections_dir = title_dir / "sections"
        if not sections_dir.exists():
            logger.debug("No sections directory in %s", title_dir)
            return None

        section_files = sorted(
            sections_dir.glob("*.xml"),
            key=lambda f: self._sort_key(f.stem),
        )

        if not section_files:
            return None

        # Group sections into chapters based on section numbering
        # DC sections are numbered like "1-101", "1-102" where first number is title
        # We group by the chapter portion
        chapters_dict: dict[str, list[Section]] = {}

        for section_file in section_files:
            section = self._parse_section_file(section_file, title_num)
            if section:
                # Determine chapter from section number
                chapter_key = self._extract_chapter(section.number, title_num)
                if chapter_key not in chapters_dict:
                    chapters_dict[chapter_key] = []
                chapters_dict[chapter_key].append(section)

        # Build chapter objects
        chapters = []
        for chapter_key in sorted(chapters_dict.keys(), key=self._sort_key):
            sections = chapters_dict[chapter_key]
            chapter_id = f"chapter-{chapter_key}"
            chapters.append(Chapter(
                id=chapter_id,
                number=chapter_key,
                heading=f"Chapter {chapter_key}",
                sections=sections,
            ))

        return Title(
            id=title_id,
            number=title_num,
            heading=heading or f"Title {title_num}",
            chapters=chapters,
        )

    def _get_title_heading(self, title_dir: Path) -> str | None:
        """Extract title heading from index.xml."""
        index_file = title_dir / "index.xml"
        if not index_file.exists():
            return None

        try:
            tree = etree.parse(str(index_file))
            root = tree.getroot()

            # Try to find heading element
            heading = root.find(".//dc:heading", NS)
            if heading is None:
                heading = root.find(".//{*}heading")
            if heading is not None and heading.text:
                return heading.text.strip()
        except Exception as e:
            logger.debug("Could not parse title heading from %s: %s", index_file, e)

        return None

    def _parse_section_file(self, section_file: Path, title_num: str) -> Section | None:
        """Parse a single section XML file."""
        try:
            tree = etree.parse(str(section_file))
            root = tree.getroot()

            # Get section number
            num_elem = root.find("dc:num", NS)
            if num_elem is None:
                num_elem = root.find("{*}num")
            if num_elem is None or not num_elem.text:
                return None
            number = num_elem.text.strip()

            # Get heading
            heading_elem = root.find("dc:heading", NS)
            if heading_elem is None:
                heading_elem = root.find("{*}heading")
            heading = ""
            if heading_elem is not None and heading_elem.text:
                heading = heading_elem.text.strip()

            # Get text content - combine all <text> and <para> elements
            text_parts = []
            self._extract_text(root, text_parts, depth=0)
            text = "\n\n".join(text_parts)
            text = clean_text(text)

            # Get legislative history from annotations
            history = self._extract_history(root)

            section_id = f"section-{number}"
            source_url = f"https://code.dccouncil.us/us/dc/council/code/sections/{number}.html"

            return Section(
                id=section_id,
                number=number,
                heading=heading,
                text=text,
                history=history,
                source_url=source_url,
            )

        except Exception as e:
            logger.warning("Failed to parse %s: %s", section_file, e)
            return None

    def _extract_text(self, element, parts: list[str], depth: int) -> None:
        """Recursively extract text from <text> and <para> elements."""
        tag = etree.QName(element.tag).localname if isinstance(element.tag, str) else ""

        if tag == "text":
            text = self._get_element_text(element)
            if text:
                indent = "  " * max(0, depth - 1)
                parts.append(f"{indent}{text}")

        elif tag == "para":
            # Get paragraph number
            num_elem = element.find("{*}num")
            num_text = ""
            if num_elem is not None and num_elem.text:
                num_text = num_elem.text.strip() + " "

            # Get paragraph text
            text_elem = element.find("{*}text")
            if text_elem is not None:
                text = self._get_element_text(text_elem)
                if text:
                    indent = "  " * max(0, depth - 1)
                    parts.append(f"{indent}{num_text}{text}")

            # Recurse into nested para elements
            for child in element:
                child_tag = etree.QName(child.tag).localname if isinstance(child.tag, str) else ""
                if child_tag == "para":
                    self._extract_text(child, parts, depth + 1)

        elif tag in ("section", "container"):
            for child in element:
                self._extract_text(child, parts, depth)

    def _get_element_text(self, element) -> str:
        """Get all text content from an element, including tail text of children."""
        parts = []
        if element.text:
            parts.append(element.text)
        for child in element:
            # Include text of inline elements like <cite>
            if child.text:
                parts.append(child.text)
            if child.tail:
                parts.append(child.tail)
        return "".join(parts).strip()

    def _extract_history(self, root) -> str:
        """Extract legislative history from annotations."""
        history_parts = []
        annotations = root.find("{*}annotations")
        if annotations is None:
            return ""

        for annotation in annotations.findall("{*}annotation"):
            ann_type = annotation.get("type", "")
            if ann_type == "History":
                text = self._get_element_text(annotation)
                if text:
                    history_parts.append(text)

        return "; ".join(history_parts)

    def _extract_chapter(self, section_number: str, title_num: str) -> str:
        """Extract chapter identifier from a DC section number.

        DC sections are numbered like "1-101" where:
        - "1" is the title number
        - "1" (first digit of 101) is roughly the chapter
        - "01" is the section within the chapter

        For more complex numbers like "1-206.01", chapter would be "2".
        """
        # Remove title prefix (e.g., "1-" from "1-101")
        parts = section_number.split("-", 1)
        if len(parts) < 2:
            return "1"

        remainder = parts[1]
        # Extract chapter number (first digit(s) before the section digits)
        # For "101" -> chapter "1", for "206" -> chapter "2", for "1501" -> chapter "15"
        match = re.match(r"(\d+)", remainder)
        if not match:
            return "1"

        num = match.group(1)
        if len(num) <= 2:
            return num[0]  # "01" -> "0" shouldn't happen, but "10" -> "1"
        elif len(num) == 3:
            return num[0]  # "101" -> "1", "206" -> "2"
        else:
            return num[:-2]  # "1501" -> "15"

    @staticmethod
    def _sort_key(name: str):
        """Sort key that handles numeric components."""
        parts = re.split(r"(\d+)", name)
        result = []
        for part in parts:
            if part.isdigit():
                result.append((0, int(part)))
            else:
                result.append((1, part))
        return result

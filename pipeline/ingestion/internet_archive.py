"""Ingestor for states whose statutes are available via Internet Archive.

Supports: KY, NC, VT, VA, WY
"""

from __future__ import annotations

import json
import logging
import re
import zipfile
from io import BytesIO
from pathlib import Path

from lxml import etree

from .base import BaseIngestor, Chapter, Section, StateCode, StructureLevel, Title
from ..normalization.text_cleaner import clean_text, clean_section_number
from ..utils.cache import HttpCache
from ..utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class InternetArchiveIngestor(BaseIngestor):
    """Ingest statutes from Internet Archive bulk downloads."""

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        super().__init__(state, config, cache_dir)
        self.http_cache = HttpCache(
            cache_dir=(cache_dir or Path("cache")) / "http",
            ttl=30 * 24 * 3600,  # 30 day cache for archive.org
            rate_limiter=RateLimiter(requests_per_second=1.0),
        )

    def fetch(self) -> Path:
        """Download statute data from Internet Archive."""
        raw_dir = self.cache_dir / "raw" / self.state
        raw_dir.mkdir(parents=True, exist_ok=True)

        extract_dir = raw_dir / "content"
        if extract_dir.exists() and any(extract_dir.rglob("*")):
            logger.info("Using cached content for %s", self.state)
            return extract_dir

        extract_dir.mkdir(parents=True, exist_ok=True)

        # Get metadata for the Internet Archive item
        item_url = self.config["url"]
        # Extract item identifier from URL
        # e.g., https://archive.org/details/kentucky-revised-statutes -> kentucky-revised-statutes
        item_id = item_url.rstrip("/").split("/")[-1]

        metadata_url = f"https://archive.org/metadata/{item_id}"
        try:
            metadata_text = self.http_cache.fetch(metadata_url)
            metadata = json.loads(metadata_text)
        except Exception as e:
            logger.warning("Could not fetch IA metadata for %s: %s", item_id, e)
            # Fallback: try to download known file patterns
            return self._fallback_fetch(item_id, extract_dir)

        # Download relevant files (XML, HTML, TXT, PDF)
        files = metadata.get("files", [])
        download_base = f"https://archive.org/download/{item_id}"

        for file_info in files:
            name = file_info.get("name", "")
            ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
            if ext in ("xml", "html", "htm", "txt", "json"):
                file_url = f"{download_base}/{name}"
                try:
                    content = self.http_cache.fetch(file_url)
                    dest = extract_dir / name
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_text(content, encoding="utf-8")
                except Exception as e:
                    logger.warning("Failed to download %s: %s", file_url, e)
            elif ext == "zip":
                file_url = f"{download_base}/{name}"
                try:
                    data = self.http_cache.fetch_bytes(file_url)
                    with zipfile.ZipFile(BytesIO(data)) as zf:
                        zf.extractall(extract_dir)
                except Exception as e:
                    logger.warning("Failed to download zip %s: %s", file_url, e)

        return extract_dir

    def _fallback_fetch(self, item_id: str, extract_dir: Path) -> Path:
        """Fallback: try common file patterns if metadata fetch fails."""
        download_base = f"https://archive.org/download/{item_id}"
        for ext in ["xml", "zip", "html"]:
            url = f"{download_base}/{item_id}.{ext}"
            try:
                if ext == "zip":
                    data = self.http_cache.fetch_bytes(url)
                    with zipfile.ZipFile(BytesIO(data)) as zf:
                        zf.extractall(extract_dir)
                else:
                    content = self.http_cache.fetch(url)
                    (extract_dir / f"{item_id}.{ext}").write_text(content, encoding="utf-8")
                return extract_dir
            except Exception:
                continue

        logger.warning("No downloadable files found for %s", item_id)
        return extract_dir

    def parse(self, raw_path: Path) -> StateCode:
        """Parse downloaded files into a StateCode."""
        titles = []

        # Try XML files first
        xml_files = list(raw_path.rglob("*.xml"))
        if xml_files:
            titles = self._parse_xml_files(xml_files)

        # Fall back to HTML files
        if not titles:
            html_files = list(raw_path.rglob("*.html")) + list(raw_path.rglob("*.htm"))
            if html_files:
                titles = self._parse_html_files(html_files)

        # Fall back to text files
        if not titles:
            txt_files = list(raw_path.rglob("*.txt"))
            if txt_files:
                titles = self._parse_text_files(txt_files)

        return StateCode(
            state=self.state,
            state_abbr=self.config.get("state_abbr", ""),
            code_name=self.config.get("code_name", ""),
            source="internet_archive",
            source_url=self.config["url"],
            year=self.config.get("year", 2024),
            structure=self.config.get("structure", [
                StructureLevel("title", "Title"),
                StructureLevel("chapter", "Chapter"),
                StructureLevel("section", "Section"),
            ]),
            titles=titles,
        )

    def _parse_xml_files(self, xml_files: list[Path]) -> list[Title]:
        """Parse XML statute files."""
        titles = []
        for xml_file in sorted(xml_files):
            try:
                tree = etree.parse(str(xml_file))
                root = tree.getroot()
                ns = ""
                if root.tag.startswith("{"):
                    ns = root.tag.split("}")[0] + "}"

                for title_elem in root.findall(f".//{ns}title") or [root]:
                    title = self._parse_title_xml(title_elem, ns)
                    if title and title.chapters:
                        titles.append(title)
            except Exception as e:
                logger.warning("Failed to parse XML %s: %s", xml_file, e)

        return titles

    def _parse_title_xml(self, elem, ns: str) -> Title | None:
        """Parse a title from XML."""
        num_elem = elem.find(f"{ns}num") or elem.find("num")
        heading_elem = elem.find(f"{ns}heading") or elem.find("heading")

        num = (num_elem.text.strip() if num_elem is not None and num_elem.text else
               elem.get("number", ""))
        heading = (heading_elem.text.strip() if heading_elem is not None and heading_elem.text else
                   elem.get("heading", ""))

        title_id = f"title-{_slugify(num or heading or 'unknown')}"

        chapters = []
        for ch_elem in (elem.findall(f".//{ns}chapter") or elem.findall(".//chapter") or
                        elem.findall(f".//{ns}article") or elem.findall(".//article")):
            chapter = self._parse_chapter_xml(ch_elem, ns)
            if chapter:
                chapters.append(chapter)

        if not chapters:
            # Try sections directly
            sections = self._find_sections_xml(elem, ns)
            if sections:
                chapters.append(Chapter(
                    id="chapter-1", number="1",
                    heading="General Provisions", sections=sections,
                ))

        if not chapters:
            return None

        return Title(id=title_id, number=num, heading=heading, chapters=chapters)

    def _parse_chapter_xml(self, elem, ns: str) -> Chapter | None:
        """Parse a chapter from XML."""
        num_elem = elem.find(f"{ns}num") or elem.find("num")
        heading_elem = elem.find(f"{ns}heading") or elem.find("heading")

        num = (num_elem.text.strip() if num_elem is not None and num_elem.text else
               elem.get("number", ""))
        heading = (heading_elem.text.strip() if heading_elem is not None and heading_elem.text else
                   elem.get("heading", ""))

        chapter_id = f"chapter-{_slugify(num or heading or 'unknown')}"
        sections = self._find_sections_xml(elem, ns)

        if not sections:
            return None

        return Chapter(id=chapter_id, number=num, heading=heading, sections=sections)

    def _find_sections_xml(self, parent, ns: str) -> list[Section]:
        """Find sections within an XML element."""
        sections = []
        for sec_elem in parent.findall(f".//{ns}section") or parent.findall(".//section"):
            num_elem = sec_elem.find(f"{ns}num") or sec_elem.find("num")
            heading_elem = sec_elem.find(f"{ns}heading") or sec_elem.find("heading")

            num = clean_section_number(
                num_elem.text.strip() if num_elem is not None and num_elem.text else
                sec_elem.get("number", "")
            )
            if not num:
                continue

            heading = (heading_elem.text.strip() if heading_elem is not None and heading_elem.text else "")
            text = clean_text("".join(sec_elem.itertext()))

            sections.append(Section(
                id=f"section-{_slugify(num)}",
                number=num,
                heading=heading,
                text=text,
            ))

        return sections

    def _parse_html_files(self, html_files: list[Path]) -> list[Title]:
        """Parse HTML statute files."""
        from bs4 import BeautifulSoup

        titles = []
        for html_file in sorted(html_files):
            try:
                content = html_file.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")

                # Try to find title/chapter structure in HTML
                title = self._parse_html_doc(soup, html_file.stem)
                if title and title.chapters:
                    titles.append(title)
            except Exception as e:
                logger.warning("Failed to parse HTML %s: %s", html_file, e)

        return titles

    def _parse_html_doc(self, soup, filename: str) -> Title | None:
        """Parse a single HTML document as a title."""
        title_heading = ""
        h1 = soup.find("h1")
        if h1:
            title_heading = h1.get_text(strip=True)

        # Look for section patterns
        sections = []
        # Common pattern: sections in divs or paragraphs with section numbers
        for elem in soup.find_all(["div", "p", "section"]):
            text = elem.get_text(strip=True)
            # Look for section number patterns like "§ 1-101" or "Section 1.01"
            match = re.match(r"(?:§|Section)\s*([\d\-\.]+)\s*[.\s]*(.*)", text)
            if match:
                num = clean_section_number(match.group(1))
                rest = match.group(2)
                # Split heading from text
                lines = rest.split("\n", 1)
                heading = lines[0].strip()[:200]
                body = lines[1].strip() if len(lines) > 1 else ""

                sections.append(Section(
                    id=f"section-{_slugify(num)}",
                    number=num,
                    heading=heading,
                    text=clean_text(body or rest),
                ))

        if not sections:
            return None

        title_id = f"title-{_slugify(filename)}"
        return Title(
            id=title_id,
            number=filename,
            heading=title_heading or filename,
            chapters=[Chapter(
                id="chapter-1", number="1",
                heading=title_heading or "Content",
                sections=sections,
            )],
        )

    def _parse_text_files(self, txt_files: list[Path]) -> list[Title]:
        """Parse plain text statute files."""
        titles = []
        for txt_file in sorted(txt_files):
            try:
                content = txt_file.read_text(encoding="utf-8", errors="replace")
                sections = self._parse_text_content(content)
                if sections:
                    title_id = f"title-{_slugify(txt_file.stem)}"
                    titles.append(Title(
                        id=title_id,
                        number=txt_file.stem,
                        heading=txt_file.stem.replace("-", " ").title(),
                        chapters=[Chapter(
                            id="chapter-1", number="1",
                            heading="Content", sections=sections,
                        )],
                    ))
            except Exception as e:
                logger.warning("Failed to parse text %s: %s", txt_file, e)

        return titles

    def _parse_text_content(self, content: str) -> list[Section]:
        """Parse section-numbered text content."""
        sections = []
        # Split on section markers
        parts = re.split(r"\n(?=(?:§|Section)\s*[\d\-\.]+)", content)

        for part in parts:
            match = re.match(r"(?:§|Section)\s*([\d\-\.]+)\s*[.\s]*(.*)", part, re.DOTALL)
            if match:
                num = clean_section_number(match.group(1))
                rest = match.group(2).strip()
                lines = rest.split("\n", 1)
                heading = lines[0].strip()[:200]
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

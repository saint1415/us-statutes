"""Ingestor for states whose statutes are available via Law.Resource.Org XML bulk downloads.

Supports: AR, CA, CO, DE, GA, ID, MS, OR, TN
"""

from __future__ import annotations

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


class LawResourceOrgIngestor(BaseIngestor):
    """Ingest statutes from Law.Resource.Org XML bulk downloads."""

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        super().__init__(state, config, cache_dir)
        self.http_cache = HttpCache(
            cache_dir=(cache_dir or Path("cache")) / "http",
            rate_limiter=RateLimiter(requests_per_second=2.0),
        )

    def fetch(self) -> Path:
        """Download the XML archive from Law.Resource.Org."""
        raw_dir = self.cache_dir / "raw" / self.state
        raw_dir.mkdir(parents=True, exist_ok=True)

        base_url = self.config["url"].rstrip("/")
        extract_dir = raw_dir / "xml"

        if extract_dir.exists() and any(extract_dir.rglob("*.xml")):
            logger.info("Using cached XML for %s at %s", self.state, extract_dir)
            return extract_dir

        # Law.Resource.Org has XML in subdirectories like state.xml.2012/
        from bs4 import BeautifulSoup

        extract_dir.mkdir(parents=True, exist_ok=True)
        downloaded = 0

        # Fetch the top-level index
        index_html = self.http_cache.fetch(base_url + "/")
        soup = BeautifulSoup(index_html, "html.parser")

        # Collect all URLs to check (top-level + subdirectories)
        urls_to_scan = [(base_url, soup)]

        # Find subdirectories that might contain XML
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith("/index.html") and "xml" in href.lower():
                subdir_url = base_url + "/" + href.replace("/index.html", "")
                try:
                    sub_html = self.http_cache.fetch(subdir_url + "/index.html")
                    sub_soup = BeautifulSoup(sub_html, "html.parser")
                    urls_to_scan.append((subdir_url, sub_soup))
                except Exception as e:
                    logger.debug("Skip subdir %s: %s", subdir_url, e)

        # Download all XML/zip files from all directories
        for dir_url, dir_soup in urls_to_scan:
            for link in dir_soup.find_all("a", href=True):
                href = link["href"]
                if href.endswith(".xml") or href.endswith(".zip"):
                    file_url = dir_url + "/" + href if not href.startswith("http") else href
                    try:
                        if href.endswith(".zip"):
                            data = self.http_cache.fetch_bytes(file_url)
                            with zipfile.ZipFile(BytesIO(data)) as zf:
                                zf.extractall(extract_dir)
                        else:
                            content = self.http_cache.fetch(file_url)
                            (extract_dir / href).write_text(content, encoding="utf-8")
                        downloaded += 1
                    except Exception as e:
                        logger.warning("Failed to download %s: %s", file_url, e)

        logger.info("Downloaded %d files for %s", downloaded, self.state)
        return extract_dir

    def parse(self, raw_path: Path) -> StateCode:
        """Parse XML files into a StateCode."""
        xml_files = sorted(raw_path.rglob("*.xml"))
        if not xml_files:
            raise FileNotFoundError(f"No XML files found in {raw_path}")

        titles = []

        for xml_file in xml_files:
            try:
                parsed_titles = self._parse_xml_file(xml_file)
                titles.extend(parsed_titles)
            except Exception as e:
                logger.warning("Failed to parse %s: %s", xml_file, e)

        # Merge titles with same ID
        titles = self._merge_titles(titles)

        return StateCode(
            state=self.state,
            state_abbr=self.config.get("state_abbr", ""),
            code_name=self.config.get("code_name", ""),
            source="law_resource_org",
            source_url=self.config["url"],
            year=self.config.get("year", 2024),
            structure=self.config.get("structure", [
                StructureLevel("title", "Title"),
                StructureLevel("chapter", "Chapter"),
                StructureLevel("section", "Section"),
            ]),
            titles=titles,
        )

    def _parse_xml_file(self, xml_file: Path) -> list[Title]:
        """Parse a single XML file, which may contain one or more titles."""
        tree = etree.parse(str(xml_file))
        root = tree.getroot()
        ns = self._detect_namespace(root)

        titles = []

        # Look for title-level elements
        title_elements = root.findall(f".//{ns}title") or root.findall(".//title")
        if not title_elements:
            # The whole file might be a single title
            title_elements = [root]

        for title_elem in title_elements:
            title = self._parse_title_element(title_elem, ns)
            if title and (title.chapters or title.heading):
                titles.append(title)

        return titles

    def _parse_title_element(self, elem, ns: str) -> Title | None:
        """Parse a title XML element."""
        num = self._get_text(elem, f"{ns}num") or self._get_text(elem, "num") or ""
        heading = self._get_text(elem, f"{ns}heading") or self._get_text(elem, "heading") or ""

        if not num and not heading:
            # Try attributes
            num = elem.get("number", elem.get("num", ""))
            heading = elem.get("heading", elem.get("title", ""))

        title_id = f"title-{self._slugify(num or heading)}"

        # Find chapters
        chapters = []
        chapter_elems = (
            elem.findall(f".//{ns}chapter") or
            elem.findall(".//chapter") or
            elem.findall(f".//{ns}article") or
            elem.findall(".//article")
        )

        if chapter_elems:
            for ch_elem in chapter_elems:
                chapter = self._parse_chapter_element(ch_elem, ns)
                if chapter:
                    chapters.append(chapter)
        else:
            # No chapter subdivision - sections directly under title
            sections = self._find_sections(elem, ns)
            if sections:
                chapters.append(Chapter(
                    id="chapter-1",
                    number="1",
                    heading=heading or "General Provisions",
                    sections=sections,
                ))

        if not chapters:
            return None

        return Title(id=title_id, number=num, heading=heading, chapters=chapters)

    def _parse_chapter_element(self, elem, ns: str) -> Chapter | None:
        """Parse a chapter XML element."""
        num = self._get_text(elem, f"{ns}num") or self._get_text(elem, "num") or ""
        heading = self._get_text(elem, f"{ns}heading") or self._get_text(elem, "heading") or ""

        if not num:
            num = elem.get("number", elem.get("num", ""))
        if not heading:
            heading = elem.get("heading", elem.get("title", ""))

        chapter_id = f"chapter-{self._slugify(num or heading)}"

        sections = self._find_sections(elem, ns)
        if not sections:
            return None

        return Chapter(id=chapter_id, number=num, heading=heading, sections=sections)

    def _find_sections(self, parent, ns: str) -> list[Section]:
        """Find and parse all section elements within a parent."""
        sections = []
        section_elems = (
            parent.findall(f".//{ns}section") or
            parent.findall(".//section")
        )

        for sec_elem in section_elems:
            section = self._parse_section_element(sec_elem, ns)
            if section:
                sections.append(section)

        return sections

    def _parse_section_element(self, elem, ns: str) -> Section | None:
        """Parse a section XML element."""
        num = self._get_text(elem, f"{ns}num") or self._get_text(elem, "num") or ""
        heading = self._get_text(elem, f"{ns}heading") or self._get_text(elem, "heading") or ""

        if not num:
            num = elem.get("number", elem.get("num", ""))
        if not heading:
            heading = elem.get("heading", elem.get("title", ""))

        num = clean_section_number(num)
        if not num:
            return None

        # Get text content
        text_parts = []
        for text_elem in elem.findall(f".//{ns}text") or elem.findall(".//text"):
            t = self._element_text_content(text_elem)
            if t:
                text_parts.append(t)

        # Also try <p> elements
        for p_elem in elem.findall(f".//{ns}p") or elem.findall(".//p"):
            t = self._element_text_content(p_elem)
            if t:
                text_parts.append(t)

        text = clean_text("\n\n".join(text_parts))

        if not text:
            # Try getting all text content from the element
            text = clean_text(self._element_text_content(elem))

        # Get history/source notes
        history = ""
        for hist_elem in (elem.findall(f".//{ns}history") or elem.findall(".//history") or
                          elem.findall(f".//{ns}source") or elem.findall(".//source")):
            h = self._element_text_content(hist_elem)
            if h:
                history = clean_text(h)
                break

        section_id = f"section-{self._slugify(num)}"

        return Section(
            id=section_id,
            number=num,
            heading=heading,
            text=text,
            history=history,
        )

    def _detect_namespace(self, root) -> str:
        """Detect XML namespace from root element."""
        tag = root.tag
        if tag.startswith("{"):
            ns = tag.split("}")[0] + "}"
            return ns
        return ""

    def _get_text(self, elem, tag: str) -> str | None:
        """Get text content of a child element."""
        child = elem.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return None

    def _element_text_content(self, elem) -> str:
        """Get all text content from an element and its children."""
        return "".join(elem.itertext()).strip()

    def _merge_titles(self, titles: list[Title]) -> list[Title]:
        """Merge titles with the same ID."""
        merged: dict[str, Title] = {}
        for title in titles:
            if title.id in merged:
                merged[title.id].chapters.extend(title.chapters)
            else:
                merged[title.id] = title
        return list(merged.values())

    @staticmethod
    def _slugify(text: str) -> str:
        slug = text.lower().strip()
        slug = re.sub(r"[^\w\s-]", "", slug)
        slug = re.sub(r"[\s_]+", "-", slug)
        slug = re.sub(r"-+", "-", slug)
        return slug.strip("-") or "unknown"

"""Ingestor for states scraped directly from their official legislature websites.

Handles ~40 states with per-state URL patterns and parsing logic.
Replaces the Justia scraper since Justia is Cloudflare-blocked.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import urljoin, quote

from bs4 import BeautifulSoup, Tag

from .base import BaseIngestor, Chapter, Section, StateCode, StructureLevel, Title
from ..normalization.text_cleaner import clean_text, clean_section_number
from ..utils.cache import HttpCache
from ..utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class OfficialWebsiteIngestor(BaseIngestor):
    """Scrape statutes from official state legislature websites."""

    def __init__(self, state: str, config: dict, cache_dir: Path | None = None):
        super().__init__(state, config, cache_dir)
        verify_ssl = config.get("verify_ssl", True)
        self.http_cache = HttpCache(
            cache_dir=(cache_dir or Path("cache")) / "http",
            ttl=7 * 24 * 3600,
            rate_limiter=RateLimiter(requests_per_second=1.0, burst=2),
            verify_ssl=verify_ssl,
        )
        self.base_url = config["url"].rstrip("/")
        self.max_retries = 3

    def _fetch_page(self, url: str) -> str:
        """Fetch a URL with retries."""
        for attempt in range(self.max_retries):
            try:
                return self.http_cache.fetch(url)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    delay = 3 * (2 ** attempt)
                    logger.info("Retry %d for %s: %s", attempt + 1, url, e)
                    time.sleep(delay)
                else:
                    raise

    def fetch(self) -> Path:
        """Fetch all pages for this state, save to cache dir."""
        raw_dir = self.cache_dir / "raw" / self.state / "official"
        raw_dir.mkdir(parents=True, exist_ok=True)

        handler = _FETCH_HANDLERS.get(self.state)
        if handler:
            handler(self, raw_dir)
        else:
            self._generic_fetch(raw_dir)

        return raw_dir

    def parse(self, raw_path: Path) -> StateCode:
        """Parse cached HTML into a StateCode."""
        handler = _PARSE_HANDLERS.get(self.state)
        if handler:
            titles = handler(self, raw_path)
        else:
            titles = self._generic_parse(raw_path)

        return StateCode(
            state=self.state,
            state_abbr=self.config.get("state_abbr", ""),
            code_name=self.config.get("code_name", ""),
            source="official_website",
            source_url=self.base_url,
            year=self.config.get("year", 2024),
            structure=self.config.get("structure", [
                StructureLevel("title", "Title"),
                StructureLevel("chapter", "Chapter"),
                StructureLevel("section", "Section"),
            ]),
            titles=titles,
        )

    # ================================================================
    # Generic fetcher: download index page, follow title/chapter links
    # ================================================================

    def _generic_fetch(self, raw_dir: Path) -> None:
        """Generic: fetch index, then follow links 2 levels deep."""
        index_html = self._fetch_page(self.base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")

        soup = BeautifulSoup(index_html, "html.parser")
        links = self._find_code_links(soup, self.base_url)
        logger.info("Found %d top-level links for %s", len(links), self.state)

        for i, (url, text) in enumerate(links[:200]):
            safe = _slugify(text or f"item-{i}")[:60]
            tdir = raw_dir / safe
            tdir.mkdir(exist_ok=True)
            try:
                html = self._fetch_page(url)
                (tdir / "index.html").write_text(html, encoding="utf-8")
                sub_soup = BeautifulSoup(html, "html.parser")
                sub_links = self._find_code_links(sub_soup, url)
                for j, (sub_url, sub_text) in enumerate(sub_links[:300]):
                    safe_sub = _slugify(sub_text or f"sub-{j}")[:60]
                    try:
                        sub_html = self._fetch_page(sub_url)
                        (tdir / f"{safe_sub}.html").write_text(sub_html, encoding="utf-8")
                    except Exception as e:
                        logger.debug("Skip %s: %s", sub_url, e)
            except Exception as e:
                logger.warning("Skip title %s: %s", url, e)

    def _generic_parse(self, raw_path: Path) -> list[Title]:
        """Generic: parse directories as titles, files as chapters."""
        titles = []
        for tdir in sorted(d for d in raw_path.iterdir() if d.is_dir()):
            chapters = []
            for html_file in sorted(tdir.glob("*.html")):
                if html_file.name == "index.html":
                    continue
                sections = self._extract_sections_from_file(html_file)
                if sections:
                    chapters.append(Chapter(
                        id=f"chapter-{html_file.stem}",
                        number=html_file.stem,
                        heading=html_file.stem.replace("-", " ").title(),
                        sections=sections,
                    ))
            idx = tdir / "index.html"
            if idx.exists() and not chapters:
                sections = self._extract_sections_from_file(idx)
                if sections:
                    chapters.append(Chapter(
                        id=f"chapter-{tdir.name}",
                        number=tdir.name,
                        heading=tdir.name.replace("-", " ").title(),
                        sections=sections,
                    ))
            if chapters:
                titles.append(Title(
                    id=f"title-{tdir.name}",
                    number=tdir.name,
                    heading=tdir.name.replace("-", " ").title(),
                    chapters=chapters,
                ))

        # Also parse flat HTML files in raw_path itself
        for html_file in sorted(raw_path.glob("*.html")):
            if html_file.name == "index.html":
                continue
            sections = self._extract_sections_from_file(html_file)
            if sections:
                name = html_file.stem
                titles.append(Title(
                    id=f"title-{name}",
                    number=name,
                    heading=name.replace("-", " ").title(),
                    chapters=[Chapter(
                        id=f"chapter-{name}",
                        number=name,
                        heading=name.replace("-", " ").title(),
                        sections=sections,
                    )],
                ))

        return titles

    def _extract_sections_from_file(self, path: Path) -> list[Section]:
        """Extract sections from an HTML file."""
        content = path.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(content, "html.parser")
        return extract_sections_from_soup(soup)

    def _find_code_links(self, soup: BeautifulSoup, parent_url: str) -> list[tuple[str, str]]:
        """Find statute navigation links in a page."""
        links = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if not text or len(text) < 2 or len(text) > 300:
                continue
            if href.startswith("#") or href.startswith("javascript") or href.startswith("mailto"):
                continue
            full = urljoin(parent_url, href)
            normalized = full.rstrip("/")
            if normalized not in seen and normalized != parent_url.rstrip("/"):
                seen.add(normalized)
                links.append((full, text))
        return links

    # ================================================================
    # State-specific fetch handlers
    # ================================================================

    def _fetch_arizona(self, raw_dir: Path) -> None:
        """AZ: Static HTML at azleg.gov/arsTable/"""
        index_url = "https://www.azleg.gov/arsTable/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/ars/" in href.lower() or "/arstitle" in href.lower():
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        if ".htm" in sa["href"]:
                            surl = urljoin(url, sa["href"])
                            stext = sa.get_text(strip=True)
                            sname = _slugify(stext or sa["href"].split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug("Skip AZ title: %s", e)

    def _fetch_california(self, raw_dir: Path) -> None:
        """CA: leginfo.legislature.ca.gov - structured code browser."""
        base = "https://leginfo.legislature.ca.gov/faces"
        codes_url = f"{base}/codes.xhtml"
        index_html = self._fetch_page(codes_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        # Find links to code TOCs like codesTOCSelected.xhtml?tocCode=BPC
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "tocCode=" in href or "codesTOC" in href:
                url = urljoin(codes_url, href)
                safe = _slugify(text or href.split("=")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    # Follow section/text links
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        if "codes_displaySection" in sh or "codes_displayText" in sh:
                            surl = urljoin(url, sh)
                            stext = sa.get_text(strip=True)
                            sname = _slugify(stext or sh.split("=")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug("Skip CA code %s: %s", text, e)

    def _fetch_colorado(self, raw_dir: Path) -> None:
        """CO: leg.colorado.gov/colorado-revised-statutes."""
        index_url = "https://leg.colorado.gov/colorado-revised-statutes"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/node/" in href or "title" in href.lower():
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_delaware(self, raw_dir: Path) -> None:
        """DE: delcode.delaware.gov - clean static HTML."""
        base = "https://delcode.delaware.gov"
        for title_num in range(1, 32):
            tdir = raw_dir / f"title-{title_num}"
            tdir.mkdir(exist_ok=True)
            # Title index page
            url = f"{base}/title{title_num}/index.html"
            try:
                html = self._fetch_page(url)
                (tdir / "index.html").write_text(html, encoding="utf-8")
                tsoup = BeautifulSoup(html, "html.parser")
                for a in tsoup.find_all("a", href=True):
                    href = a["href"]
                    if href.endswith(".html") and "c0" in href.lower():
                        surl = urljoin(url, href)
                        sname = _slugify(href.replace(".html", ""))[:60]
                        try:
                            shtml = self._fetch_page(surl)
                            (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                        except Exception:
                            pass
            except Exception:
                pass

    def _fetch_idaho(self, raw_dir: Path) -> None:
        """ID: legislature.idaho.gov/statutesrules/idstat/."""
        base_url = "https://legislature.idaho.gov/statutesrules/idstat/"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/idstat/Title" in href or "/idstat/title" in href:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        # Chapter links like /statutesrules/idstat/Title1/T1CH1
                        if re.search(r"T\d+CH\d+", sh):
                            surl = urljoin(url, sh)
                            sname = _slugify(sa.get_text(strip=True) or sh.split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug("Skip ID title: %s", e)

    def _fetch_oregon(self, raw_dir: Path) -> None:
        """OR: oregonlegislature.gov - numbered ORS chapter HTML files."""
        base = "https://www.oregonlegislature.gov/bills_laws"
        index_url = f"{base}/Pages/ORS.aspx"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "ors" in href.lower() and href.endswith(".html"):
                url = urljoin(index_url, href)
                safe = _slugify(href.replace(".html", "").split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_alaska(self, raw_dir: Path) -> None:
        """AK: akleg.gov - title/chapter structure."""
        base_url = "https://www.akleg.gov/basis/statutes.asp"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "statutes.asp" in href and "#" not in href and href != base_url:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("=")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_hawaii(self, raw_dir: Path) -> None:
        """HI: capitol.hawaii.gov/hrscurrent/."""
        base_url = "https://www.capitol.hawaii.gov/hrscurrent/"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if href.endswith(".htm") or href.endswith(".html"):
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        if sh.endswith(".htm") or sh.endswith(".html"):
                            surl = urljoin(url, sh)
                            sname = _slugify(sa.get_text(strip=True) or sh.split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_indiana(self, raw_dir: Path) -> None:
        """IN: iga.in.gov - clean structure."""
        base_url = "https://iga.in.gov/laws/current/ic"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/ic/" in href or "/laws/" in href:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_iowa(self, raw_dir: Path) -> None:
        """IA: legis.iowa.gov - section-based."""
        base_url = "https://www.legis.iowa.gov/law/iowaCode/sections"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "iowaCode" in href and href != base_url:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_kentucky(self, raw_dir: Path) -> None:
        """KY: apps.legislature.ky.gov."""
        base_url = "https://apps.legislature.ky.gov/law/statutes/"
        index_html = self._fetch_page(base_url + "index.aspx")
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "statute.aspx" in href or "chapter.aspx" in href:
                url = urljoin(base_url + "index.aspx", href)
                safe = _slugify(text or href.split("=")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_minnesota(self, raw_dir: Path) -> None:
        """MN: revisor.mn.gov/statutes/cite/{chapter}."""
        index_url = "https://www.revisor.mn.gov/statutes/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/statutes/cite/" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_nevada(self, raw_dir: Path) -> None:
        """NV: Static HTML at leg.state.nv.us/nrs/NRS-{chapter}.html."""
        index_url = "https://www.leg.state.nv.us/nrs/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.upper().startswith("NRS-") and href.endswith(".html"):
                url = urljoin(index_url, href)
                safe = _slugify(href.replace(".html", ""))[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_new_hampshire(self, raw_dir: Path) -> None:
        """NH: gc.nh.gov/rsa/html/."""
        index_url = "https://gc.nh.gov/rsa/html/NHTOC.htm"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".htm"):
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href)[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        if sa["href"].endswith(".htm"):
                            surl = urljoin(url, sa["href"])
                            sname = _slugify(sa.get_text(strip=True) or sa["href"])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_new_jersey(self, raw_dir: Path) -> None:
        """NJ: pub.njleg.state.nj.us/statutes/."""
        base_url = "https://pub.njleg.state.nj.us/statutes/"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "title" in href.lower() or ".htm" in href:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_north_carolina(self, raw_dir: Path) -> None:
        """NC: ncleg.gov/Laws/GeneralStatutesTOC."""
        index_url = "https://www.ncleg.gov/Laws/GeneralStatutesTOC"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/EnactedLegislation/Statutes" in href or "GeneralStatutes" in href:
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_north_dakota(self, raw_dir: Path) -> None:
        """ND: ndlegis.gov century code."""
        base_url = "https://ndlegis.gov/general-information/north-dakota-century-code"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "cencode" in href.lower() or "century-code" in href:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_texas(self, raw_dir: Path) -> None:
        """TX: statutes.capitol.texas.gov - code-based structure."""
        base = "https://statutes.capitol.texas.gov"
        index_url = f"{base}/Docs/SDTocs/SDTOC.htm"
        try:
            index_html = self._fetch_page(index_url)
        except Exception:
            index_url = f"{base}/"
            index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if ".htm" in href and "SDTocs" not in href:
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        if ".htm" in sa["href"]:
                            surl = urljoin(url, sa["href"])
                            sname = _slugify(sa.get_text(strip=True) or sa["href"])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_vermont(self, raw_dir: Path) -> None:
        """VT: legislature.vermont.gov/statutes/."""
        base_url = "https://legislature.vermont.gov/statutes/"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/statutes/title" in href or "/statutes/chapter" in href:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_virginia(self, raw_dir: Path) -> None:
        """VA: law.lis.virginia.gov/vacode/ - well structured."""
        base_url = "https://law.lis.virginia.gov/vacode/"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/vacode/title" in href or "/vacode/" in href:
                url = urljoin(base_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_wyoming(self, raw_dir: Path) -> None:
        """WY: wyoleg.gov statutes."""
        base_url = "https://wyoleg.gov/statutes/compress/title01.pdf"
        # Wyoming has PDFs - try the HTML version instead
        index_url = "https://wyoleg.gov/NXT/gateway.dll?f=templates&fn=default.htm"
        try:
            index_html = self._fetch_page(index_url)
            (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
            soup = BeautifulSoup(index_html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if "gateway.dll" in href or ".htm" in href:
                    url = urljoin(index_url, href)
                    safe = _slugify(text or href.split("=")[-1])[:60]
                    try:
                        html = self._fetch_page(url)
                        (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                    except Exception:
                        pass
        except Exception:
            pass

    def _fetch_rhode_island(self, raw_dir: Path) -> None:
        """RI: webserver.rilegislature.gov/Statutes/."""
        index_url = "https://webserver.rilegislature.gov/Statutes/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "TITLE" in href.upper() and "/" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href)[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        if sa["href"].endswith(".htm"):
                            surl = urljoin(url, sa["href"])
                            sname = _slugify(sa["href"].replace(".htm", ""))[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_south_carolina(self, raw_dir: Path) -> None:
        """SC: scstatehouse.gov - follow title → chapter links."""
        base = "https://www.scstatehouse.gov"
        for title_num in range(1, 63):
            url = f"{base}/code/title{title_num}.php"
            try:
                html = self._fetch_page(url)
                if len(html) < 1000:
                    continue
                # Parse to find chapter HTML links like /code/t01c001.php
                soup = BeautifulSoup(html, "html.parser")
                tdir = raw_dir / f"title-{title_num}"
                tdir.mkdir(exist_ok=True)
                (tdir / "index.html").write_text(html, encoding="utf-8")

                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "/code/t" in href and href.endswith(".php"):
                        ch_url = urljoin(url, href)
                        ch_name = href.split("/")[-1].replace(".php", "")
                        try:
                            ch_html = self._fetch_page(ch_url)
                            if len(ch_html) > 1000:
                                (tdir / f"{ch_name}.html").write_text(ch_html, encoding="utf-8")
                        except Exception:
                            pass
            except Exception:
                pass

    def _fetch_washington(self, raw_dir: Path) -> None:
        """WA: app.leg.wa.gov/rcw/."""
        index_url = "https://app.leg.wa.gov/rcw/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "default.aspx" in href or "/rcw/" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("=")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_west_virginia(self, raw_dir: Path) -> None:
        """WV: code.wvlegislature.gov."""
        index_url = "https://code.wvlegislature.gov/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "code.wvlegislature.gov/" in href and href.count("/") >= 4:
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-2])[:60]
                try:
                    html = self._fetch_page(href)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_wisconsin(self, raw_dir: Path) -> None:
        """WI: docs.legis.wisconsin.gov/statutes/statutes/."""
        index_url = "https://docs.legis.wisconsin.gov/statutes/statutes"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/statutes/statutes/" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_oklahoma(self, raw_dir: Path) -> None:
        """OK: oscn.net."""
        for title_num in range(1, 86):
            url = f"https://www.oscn.net/applications/oscn/index.asp?ftdb=STOKST{title_num:02d}&level=1"
            try:
                html = self._fetch_page(url)
                if len(html) > 1000:
                    (raw_dir / f"title-{title_num:02d}.html").write_text(html, encoding="utf-8")
            except Exception:
                pass

    def _fetch_illinois(self, raw_dir: Path) -> None:
        """IL: ilga.gov."""
        index_url = "https://www.ilga.gov/legislation/ilcs/ilcs.asp"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "ilcs" in href.lower() and ("ChapterAct" in href or "fullchapter" in href.lower()):
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href)[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_massachusetts(self, raw_dir: Path) -> None:
        """MA: malegislature.gov."""
        index_url = "https://malegislature.gov/Laws/GeneralLaws"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/Laws/GeneralLaws/Part" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        if "/Chapter" in sa["href"]:
                            surl = urljoin(url, sa["href"])
                            sname = _slugify(sa.get_text(strip=True) or sa["href"].split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_michigan(self, raw_dir: Path) -> None:
        """MI: legislature.mi.gov."""
        index_url = "https://www.legislature.mi.gov/Laws/MCL"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "MCL" in href and ("Chapter" in href or "Section" in href or "mcl-" in href.lower()):
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_missouri(self, raw_dir: Path) -> None:
        """MO: revisor.mo.gov."""
        index_url = "https://revisor.mo.gov/main/Home.aspx"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "OneChapter" in href or "OneTitle" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href)[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_montana(self, raw_dir: Path) -> None:
        """MT: leg.mt.gov."""
        index_url = "https://leg.mt.gov/bills/mca/index.html"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/mca/title_" in href or "/mca/" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_ohio(self, raw_dir: Path) -> None:
        """OH: codes.ohio.gov."""
        index_url = "https://codes.ohio.gov/ohio-revised-code"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "ohio-revised-code" in href and "chapter" in href.lower():
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_pennsylvania(self, raw_dir: Path) -> None:
        """PA: palegis.us consolidated statutes."""
        for title_num in range(1, 76):
            url = f"https://www.palegis.us/statutes/consolidated/view-statute?3&iFrame=true&txtType=HTM&ttl={title_num:02d}"
            try:
                html = self._fetch_page(url)
                if len(html) > 500:
                    (raw_dir / f"title-{title_num:02d}.html").write_text(html, encoding="utf-8")
            except Exception:
                pass

    def _fetch_utah(self, raw_dir: Path) -> None:
        """UT: le.utah.gov/xcode/."""
        for title_num in range(1, 79):
            url = f"https://le.utah.gov/xcode/Title{title_num}/Title{title_num}.html"
            try:
                html = self._fetch_page(url)
                if len(html) > 500:
                    tdir = raw_dir / f"title-{title_num}"
                    tdir.mkdir(exist_ok=True)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for a in tsoup.find_all("a", href=True):
                        if "Chapter" in a["href"] and ".html" in a["href"]:
                            surl = urljoin(url, a["href"])
                            sname = _slugify(a["href"].replace(".html", ""))[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
            except Exception:
                pass

    def _fetch_new_york(self, raw_dir: Path) -> None:
        """NY: NY Senate OpenLegislation API."""
        api_base = "https://legislation.nysenate.gov/api/3"
        try:
            laws_html = self._fetch_page(f"{api_base}/laws")
            (raw_dir / "laws.json").write_text(laws_html, encoding="utf-8")
            laws = json.loads(laws_html)
            items = laws.get("result", {}).get("items", [])
            for item in items:
                law_id = item.get("lawId", "")
                if not law_id:
                    continue
                try:
                    law_url = f"{api_base}/laws/{law_id}?full=true"
                    law_html = self._fetch_page(law_url)
                    (raw_dir / f"{law_id}.json").write_text(law_html, encoding="utf-8")
                except Exception as e:
                    logger.debug("Skip NY law %s: %s", law_id, e)
        except Exception as e:
            logger.warning("Failed NY API: %s", e)

    def _fetch_south_dakota(self, raw_dir: Path) -> None:
        """SD: sdlegislature.gov API."""
        for title_num in range(1, 63):
            url = f"https://sdlegislature.gov/api/Statutes/{title_num}.html"
            try:
                html = self._fetch_page(url)
                if len(html) > 200:
                    (raw_dir / f"title-{title_num}.html").write_text(html, encoding="utf-8")
            except Exception:
                pass

    def _fetch_kansas(self, raw_dir: Path) -> None:
        """KS: ksrevisor.org."""
        index_url = "https://www.ksrevisor.org/statutes.html"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/statutes/chapters/" in href or "ksa" in href.lower():
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass

    def _fetch_maine(self, raw_dir: Path) -> None:
        """ME: legislature.maine.gov/statutes/."""
        index_url = "https://legislature.maine.gov/statutes/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/statutes/" in href and href != index_url:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        if ".html" in sa["href"] or ".htm" in sa["href"]:
                            surl = urljoin(url, sa["href"])
                            sname = _slugify(sa.get_text(strip=True) or sa["href"])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_louisiana(self, raw_dir: Path) -> None:
        """LA: legis.la.gov."""
        index_url = "https://www.legis.la.gov/legis/LawSearch.aspx"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "Law.aspx" in href:
                url = urljoin(index_url, href)
                text = a.get_text(strip=True)
                safe = _slugify(text or href.split("=")[-1])[:60]
                try:
                    html = self._fetch_page(url)
                    (raw_dir / f"{safe}.html").write_text(html, encoding="utf-8")
                except Exception:
                    pass


# ================================================================
# Handler registries
# ================================================================

_FETCH_HANDLERS: dict[str, callable] = {
    "alaska": OfficialWebsiteIngestor._fetch_alaska,
    "arizona": OfficialWebsiteIngestor._fetch_arizona,
    "california": OfficialWebsiteIngestor._fetch_california,
    "colorado": OfficialWebsiteIngestor._fetch_colorado,
    "delaware": OfficialWebsiteIngestor._fetch_delaware,
    "hawaii": OfficialWebsiteIngestor._fetch_hawaii,
    "idaho": OfficialWebsiteIngestor._fetch_idaho,
    "illinois": OfficialWebsiteIngestor._fetch_illinois,
    "indiana": OfficialWebsiteIngestor._fetch_indiana,
    "iowa": OfficialWebsiteIngestor._fetch_iowa,
    "kansas": OfficialWebsiteIngestor._fetch_kansas,
    "kentucky": OfficialWebsiteIngestor._fetch_kentucky,
    "louisiana": OfficialWebsiteIngestor._fetch_louisiana,
    "maine": OfficialWebsiteIngestor._fetch_maine,
    "massachusetts": OfficialWebsiteIngestor._fetch_massachusetts,
    "michigan": OfficialWebsiteIngestor._fetch_michigan,
    "minnesota": OfficialWebsiteIngestor._fetch_minnesota,
    "missouri": OfficialWebsiteIngestor._fetch_missouri,
    "montana": OfficialWebsiteIngestor._fetch_montana,
    "nevada": OfficialWebsiteIngestor._fetch_nevada,
    "new-hampshire": OfficialWebsiteIngestor._fetch_new_hampshire,
    "new-jersey": OfficialWebsiteIngestor._fetch_new_jersey,
    "new-york": OfficialWebsiteIngestor._fetch_new_york,
    "north-carolina": OfficialWebsiteIngestor._fetch_north_carolina,
    "north-dakota": OfficialWebsiteIngestor._fetch_north_dakota,
    "ohio": OfficialWebsiteIngestor._fetch_ohio,
    "oklahoma": OfficialWebsiteIngestor._fetch_oklahoma,
    "oregon": OfficialWebsiteIngestor._fetch_oregon,
    "pennsylvania": OfficialWebsiteIngestor._fetch_pennsylvania,
    "rhode-island": OfficialWebsiteIngestor._fetch_rhode_island,
    "south-carolina": OfficialWebsiteIngestor._fetch_south_carolina,
    "south-dakota": OfficialWebsiteIngestor._fetch_south_dakota,
    "texas": OfficialWebsiteIngestor._fetch_texas,
    "utah": OfficialWebsiteIngestor._fetch_utah,
    "vermont": OfficialWebsiteIngestor._fetch_vermont,
    "virginia": OfficialWebsiteIngestor._fetch_virginia,
    "washington": OfficialWebsiteIngestor._fetch_washington,
    "west-virginia": OfficialWebsiteIngestor._fetch_west_virginia,
    "wisconsin": OfficialWebsiteIngestor._fetch_wisconsin,
    "wyoming": OfficialWebsiteIngestor._fetch_wyoming,
}

_PARSE_HANDLERS: dict[str, callable] = {
    # Most states use the generic parser. Add custom parsers here if needed.
}


# ================================================================
# Shared utilities - improved section extraction
# ================================================================

def extract_sections_from_soup(soup: BeautifulSoup) -> list[Section]:
    """Extract statute sections from parsed HTML.

    Uses multiple strategies to handle different state website formats.
    """
    sections = []
    seen = set()

    # Strategy 1: Section number patterns in text content
    _SECTION_PATTERNS = [
        # § 1-1-1. Heading text
        re.compile(r"(?:§+\s*)([\d\-\.a-zA-Z:]+)\s*[.\-–—:\s]+(.*)", re.DOTALL),
        # SECTION 1-1-10. Heading (SC, other states with uppercase)
        re.compile(r"SECTION\s+([\d\-\.a-zA-Z:]+)\s*[.\-–—:\s]+(.*)", re.DOTALL),
        # NRS 0.010 (Nevada style)
        re.compile(r"NRS\s+([\d\.]+[A-Z]?)\s+(.*)", re.DOTALL),
        # Section 1-1-1. or Sec. 1-1-1.
        re.compile(r"Sec(?:tion)?\.?\s*([\d\-\.a-zA-Z:]+)\s*[.\-–—:\s]+(.*)", re.DOTALL),
        # Numbered like "1-101." or "12.01." at start of line
        re.compile(r"^([\d]+[\-\.]\d[\d\-\.a-zA-Z]*)\s*[.\-–—:\s]+(.*)", re.DOTALL),
        # ORS style "001.010" or "1.010"
        re.compile(r"^(\d{1,3}\.\d{3,}[a-zA-Z]?)\s*[.\s]+(.*)", re.DOTALL),
        # Art./Article prefix
        re.compile(r"Art(?:icle)?\.?\s*([\d\-\.a-zA-Z]+)\s*[.\-–—:\s]+(.*)", re.DOTALL),
    ]

    for elem in soup.find_all(["p", "div", "li", "span", "td", "h2", "h3", "h4", "h5", "dt", "dd", "b", "strong"]):
        text = elem.get_text(strip=True)
        if not text or len(text) < 5:
            continue

        for pattern in _SECTION_PATTERNS:
            match = pattern.match(text)
            if match:
                num = clean_section_number(match.group(1))
                if not num or num in seen:
                    break
                # Skip if the "number" is too long (probably not a section)
                if len(num) > 30:
                    break
                seen.add(num)

                rest = match.group(2)
                lines = rest.split("\n", 1)
                heading = lines[0].strip()[:300]
                body = lines[1].strip() if len(lines) > 1 else ""

                if len(heading) > 150 and "." in heading:
                    dot = heading.find(".", 30)
                    if dot > 0:
                        body = heading[dot+1:].strip() + ("\n" + body if body else "")
                        heading = heading[:dot]

                sections.append(Section(
                    id=f"section-{_slugify(num)}",
                    number=num,
                    heading=heading,
                    text=clean_text(body or rest),
                ))
                break

    # Strategy 2: If no sections found, look for headings with section-like text
    if not sections:
        for heading_tag in soup.find_all(["h1", "h2", "h3", "h4", "h5"]):
            htext = heading_tag.get_text(strip=True)
            for pattern in _SECTION_PATTERNS:
                match = pattern.match(htext)
                if match:
                    num = clean_section_number(match.group(1))
                    if not num or num in seen or len(num) > 30:
                        break
                    seen.add(num)
                    heading = match.group(2).strip()[:300]

                    # Collect body text from siblings
                    body_parts = []
                    sib = heading_tag.find_next_sibling()
                    while sib and sib.name not in ("h1", "h2", "h3", "h4", "h5"):
                        t = sib.get_text(strip=True)
                        if t:
                            body_parts.append(t)
                        sib = sib.find_next_sibling()
                        if len(body_parts) > 50:
                            break

                    sections.append(Section(
                        id=f"section-{_slugify(num)}",
                        number=num,
                        heading=heading,
                        text=clean_text("\n\n".join(body_parts)),
                    ))
                    break

    return sections


def _slugify(text: str) -> str:
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-") or "unknown"

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
            rate_limiter=RateLimiter(requests_per_second=3.0, burst=5),
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
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            # Skip binary/PDF files that were saved as .html
            if content.startswith("%PDF") or "\x00" in content[:1000]:
                return []
            soup = BeautifulSoup(content, "html.parser")
            return extract_sections_from_soup(soup)
        except Exception as e:
            logger.debug("Failed to parse %s: %s", path, e)
            return []

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
        """AZ: azleg.gov - direct title URLs."""
        # AZ titles numbered 1-49
        for title_num in range(1, 50):
            url = f"https://www.azleg.gov/arsDetail/?title={title_num}"
            try:
                html = self._fetch_page(url)
                if len(html) > 500:
                    tdir = raw_dir / f"title-{title_num:02d}"
                    tdir.mkdir(exist_ok=True)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    soup = BeautifulSoup(html, "html.parser")
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.get_text(strip=True)
                        if ".htm" in href:
                            aurl = urljoin(url, href)
                            safe = _slugify(text or href.split("/")[-1])[:60]
                            try:
                                ahtml = self._fetch_page(aurl)
                                (tdir / f"{safe}.html").write_text(ahtml, encoding="utf-8")
                            except Exception:
                                pass
            except Exception:
                pass

    def _fetch_california(self, raw_dir: Path) -> None:
        """CA: Use Justia (official site is JS-rendered)."""
        self._fetch_justia(raw_dir, "california")

    def _fetch_colorado(self, raw_dir: Path) -> None:
        """CO: Use Justia (official site is Drupal CMS, shallow data)."""
        self._fetch_justia(raw_dir, "colorado")

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
        """OR: oregonlegislature.gov - direct ORS chapter HTML files."""
        base = "https://www.oregonlegislature.gov/bills_laws/ors"
        # ORS chapters are numbered 001-999, directly accessible
        for ch in range(1, 1000):
            url = f"{base}/ors{ch:03d}.html"
            try:
                html = self._fetch_page(url)
                if len(html) > 500:
                    (raw_dir / f"ors{ch:03d}.html").write_text(html, encoding="utf-8")
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
        """HI: Use Justia (official site has complex volume/chapter numbering)."""
        self._fetch_justia(raw_dir, "hawaii")

    def _fetch_indiana(self, raw_dir: Path) -> None:
        """IN: Use Justia (official site is JS-rendered)."""
        self._fetch_justia(raw_dir, "indiana")

    def _fetch_iowa(self, raw_dir: Path) -> None:
        """IA: Use Justia (official site has dynamic chapter URLs)."""
        self._fetch_justia(raw_dir, "iowa")

    def _fetch_kentucky(self, raw_dir: Path) -> None:
        """KY: apps.legislature.ky.gov - title → chapter → statute pages."""
        base_url = "https://apps.legislature.ky.gov/law/statutes/"
        index_html = self._fetch_page(base_url + "index.aspx")
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        # Follow title/chapter links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "statute.aspx" in href or "chapter.aspx" in href or "title.aspx" in href:
                url = urljoin(base_url + "index.aspx", href)
                safe = _slugify(text or href.split("=")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    # Follow deeper links to actual statute text
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        st = sa.get_text(strip=True)
                        if "statute.aspx" in sh or "chapter.aspx" in sh:
                            surl = urljoin(url, sh)
                            sname = _slugify(st or sh.split("=")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_minnesota(self, raw_dir: Path) -> None:
        """MN: revisor.mn.gov - part pages → chapter pages with sections."""
        index_url = "https://www.revisor.mn.gov/statutes/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        # Follow /statutes/part/ links to get topic pages
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/statutes/part/" in href or "/statutes/cite/" in href:
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    # Follow chapter/cite links within part pages
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        st = sa.get_text(strip=True)
                        if "/statutes/cite/" in sh:
                            surl = urljoin(url, sh)
                            sname = _slugify(st or sh.split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug("Skip MN part: %s", e)

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
        """NJ: Use Justia (state site uses legacy Rocket NXT gateway)."""
        self._fetch_justia(raw_dir, "new-jersey")

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
        """TX: Use Justia (official site is JS-rendered)."""
        self._fetch_justia(raw_dir, "texas")

    def _fetch_vermont(self, raw_dir: Path) -> None:
        """VT: legislature.vermont.gov/statutes/ - title → chapter → section."""
        base_url = "https://legislature.vermont.gov/statutes/"
        index_html = self._fetch_page(base_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        # Follow title links, then follow chapter links within each title
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "/statutes/title/" in href:
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
                        st = sa.get_text(strip=True)
                        if "/statutes/section/" in sh or "/statutes/chapter/" in sh:
                            surl = urljoin(url, sh)
                            sname = _slugify(st or sh.split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_virginia(self, raw_dir: Path) -> None:
        """VA: Use Justia (official site needs deeper crawl)."""
        self._fetch_justia(raw_dir, "virginia")

    def _fetch_wyoming(self, raw_dir: Path) -> None:
        """WY: Use Justia (official site uses DLL gateway / PDFs)."""
        self._fetch_justia(raw_dir, "wyoming")

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
        """WA: app.leg.wa.gov/rcw/ - title pages → chapter pages with sections."""
        index_url = "https://app.leg.wa.gov/rcw/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        # Follow title links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "cite=" in href:
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("=")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    # Follow chapter links within title pages
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        st = sa.get_text(strip=True)
                        if "cite=" in sh and sh != href:
                            surl = urljoin(url, sh)
                            sname = _slugify(st or sh.split("=")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_west_virginia(self, raw_dir: Path) -> None:
        """WV: code.wvlegislature.gov - direct chapter URLs."""
        base = "https://code.wvlegislature.gov"
        # WV uses chapter numbers 1-62 with article sub-pages
        for ch in range(1, 63):
            url = f"{base}/{ch}/"
            try:
                html = self._fetch_page(url)
                if len(html) < 500:
                    continue
                tdir = raw_dir / f"chapter-{ch:02d}"
                tdir.mkdir(exist_ok=True)
                (tdir / "index.html").write_text(html, encoding="utf-8")
                soup = BeautifulSoup(html, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if f"/{ch}/" in href and href.rstrip("/") != f"/{ch}":
                        aurl = urljoin(url, href)
                        safe = _slugify(text or href.split("/")[-1])[:60]
                        try:
                            ahtml = self._fetch_page(aurl)
                            (tdir / f"{safe}.html").write_text(ahtml, encoding="utf-8")
                        except Exception:
                            pass
            except Exception:
                pass

    def _fetch_wisconsin(self, raw_dir: Path) -> None:
        """WI: docs.legis.wisconsin.gov - direct chapter URLs."""
        base = "https://docs.legis.wisconsin.gov/statutes/statutes"
        # WI statutes organized by chapter numbers
        for ch in range(1, 1000):
            url = f"{base}/{ch}"
            try:
                html = self._fetch_page(url)
                if len(html) > 1000:
                    (raw_dir / f"chapter-{ch:03d}.html").write_text(html, encoding="utf-8")
            except Exception:
                pass

    def _fetch_oklahoma(self, raw_dir: Path) -> None:
        """OK: Use Justia (official site requires per-section dynamic fetching)."""
        self._fetch_justia(raw_dir, "oklahoma")

    def _fetch_illinois(self, raw_dir: Path) -> None:
        """IL: Use Justia (official ILGA site doesn't expose full text links)."""
        self._fetch_justia(raw_dir, "illinois")

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
        """MI: Use Justia (official site has session-based URLs)."""
        self._fetch_justia(raw_dir, "michigan")

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
        """MT: Use Justia (official site has anti-scraping measures)."""
        self._fetch_justia(raw_dir, "montana")

    def _fetch_ohio(self, raw_dir: Path) -> None:
        """OH: codes.ohio.gov - title → chapter → section pages."""
        base = "https://codes.ohio.gov"
        index_url = f"{base}/ohio-revised-code"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        # Follow title links like ohio-revised-code/title-1
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "ohio-revised-code/" in href and href.rstrip("/") != "ohio-revised-code":
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("/")[-1])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    # Follow chapter links within title pages
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        st = sa.get_text(strip=True)
                        if "chapter" in sh.lower() or "section" in sh.lower():
                            surl = urljoin(url, sh)
                            sname = _slugify(st or sh.split("/")[-1])[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception as e:
                    logger.debug("Skip OH title: %s", e)

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
        """UT: Use Justia (official site returns 404s)."""
        self._fetch_justia(raw_dir, "utah")

    def _fetch_new_york(self, raw_dir: Path) -> None:
        """NY: Use Justia (official API doesn't return parseable HTML)."""
        self._fetch_justia(raw_dir, "new-york")

    def _fetch_south_dakota(self, raw_dir: Path) -> None:
        """SD: Use Justia (official site has external links, not full text)."""
        self._fetch_justia(raw_dir, "south-dakota")

    def _fetch_kansas(self, raw_dir: Path) -> None:
        """KS: Use Justia (official site has session-based URLs)."""
        self._fetch_justia(raw_dir, "kansas")

    def _fetch_maine(self, raw_dir: Path) -> None:
        """ME: legislature.maine.gov/statutes/ - title pages with chapters."""
        index_url = "https://legislature.maine.gov/statutes/"
        index_html = self._fetch_page(index_url)
        (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
        soup = BeautifulSoup(index_html, "html.parser")
        # Links like "1/title1ch0sec0.html"
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if "title" in href.lower() and href.endswith(".html"):
                url = urljoin(index_url, href)
                safe = _slugify(text or href.split("/")[-1].replace(".html", ""))[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")
                    # Follow chapter links within title pages
                    tsoup = BeautifulSoup(html, "html.parser")
                    for sa in tsoup.find_all("a", href=True):
                        sh = sa["href"]
                        st = sa.get_text(strip=True)
                        if sh.endswith(".html") or sh.endswith(".htm"):
                            surl = urljoin(url, sh)
                            sname = _slugify(st or sh.split("/")[-1].replace(".html", ""))[:60]
                            try:
                                shtml = self._fetch_page(surl)
                                (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                            except Exception:
                                pass
                except Exception:
                    pass

    def _fetch_louisiana(self, raw_dir: Path) -> None:
        """LA: Use Justia (official site has dynamic ASPX pages)."""
        self._fetch_justia(raw_dir, "louisiana")

    def _fetch_florida(self, raw_dir: Path) -> None:
        """FL: leg.state.fl.us - chapter display pages."""
        base = "http://www.leg.state.fl.us/statutes/index.cfm"
        # FL chapters: use Title_Statutes URL pattern
        for ch in range(1, 1013):
            # FL URL pattern uses hundreds-based folders
            folder_start = (ch // 100) * 100
            folder_end = folder_start + 99
            url = f"{base}?App_mode=Display_Statute&URL={folder_start:04d}-{folder_end:04d}/{ch:04d}/{ch:04d}.html"
            try:
                html = self._fetch_page(url)
                if len(html) > 500:
                    (raw_dir / f"chapter-{ch:04d}.html").write_text(html, encoding="utf-8")
            except Exception:
                pass

    def _fetch_alabama(self, raw_dir: Path) -> None:
        """AL: Use Justia (official site DNS unreliable)."""
        self._fetch_justia(raw_dir, "alabama")

    def _fetch_arkansas(self, raw_dir: Path) -> None:
        """AR: Use Justia as fallback (state site links to LexisNexis)."""
        self._fetch_justia(raw_dir, "arkansas")

    def _fetch_georgia(self, raw_dir: Path) -> None:
        """GA: Use Justia (official site is LexisNexis JS-only)."""
        self._fetch_justia(raw_dir, "georgia")

    def _fetch_mississippi(self, raw_dir: Path) -> None:
        """MS: Use Justia (official site is LexisNexis JS-only)."""
        self._fetch_justia(raw_dir, "mississippi")

    def _fetch_tennessee(self, raw_dir: Path) -> None:
        """TN: Use Justia (official site is LexisNexis JS-only)."""
        self._fetch_justia(raw_dir, "tennessee")

    def _fetch_maryland(self, raw_dir: Path) -> None:
        """MD: Use Justia as fallback (state site sections not well structured)."""
        self._fetch_justia(raw_dir, "maryland")

    def _fetch_new_mexico(self, raw_dir: Path) -> None:
        """NM: Use Justia as fallback (nmonesource is JS-heavy SPA)."""
        self._fetch_justia(raw_dir, "new-mexico")

    def _fetch_guam(self, raw_dir: Path) -> None:
        """Guam: Use Justia for Guam Code Annotated."""
        self._fetch_justia(raw_dir, "guam")

    def _fetch_puerto_rico(self, raw_dir: Path) -> None:
        """PR: Use Justia for Laws of Puerto Rico."""
        self._fetch_justia(raw_dir, "puerto-rico")

    def _fetch_us_virgin_islands(self, raw_dir: Path) -> None:
        """USVI: Use Justia for Virgin Islands Code."""
        self._fetch_justia(raw_dir, "virgin-islands")

    def _fetch_justia(self, raw_dir: Path, state_slug: str) -> None:
        """Generic Justia fetcher: law.justia.com/codes/{state}/ - 3 levels deep.

        Handles two Justia page structures:
        A) Index has direct title links (e.g., /codes/state/title-1/)
        B) Index only has year links (e.g., /codes/state/2024/) - follows most recent year first

        Then: title pages -> chapter pages -> chapter pages have section listings.
        """
        import re as _re
        base = f"https://law.justia.com/codes/{state_slug}/"
        year_pat = _re.compile(r"/codes/" + _re.escape(state_slug) + r"/(\d{4})")
        skip_pats = ["accounts.justia.com", "/signin", "/login"]

        def _is_skip(href):
            return any(skip in href for skip in skip_pats)

        def _extract_links(soup_obj, parent_url, exclude_urls=None):
            """Extract state-path links from a page, skipping accounts/login."""
            links = []
            seen = set(exclude_urls or [])
            for a in soup_obj.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if not text or len(text) < 2 or _is_skip(href):
                    continue
                if f"/codes/{state_slug}/" in href:
                    url = urljoin(parent_url, href)
                    if url not in seen and url.rstrip("/") != parent_url.rstrip("/"):
                        seen.add(url)
                        links.append((url, text))
            return links

        try:
            index_html = self._fetch_page(base)
            (raw_dir / "index.html").write_text(index_html, encoding="utf-8")
            soup = BeautifulSoup(index_html, "html.parser")

            # Separate year links from title links
            year_links = []
            title_links = []
            all_seen = {base.rstrip("/")}
            for url, text in _extract_links(soup, base):
                m = year_pat.search(url)
                if m:
                    year_links.append((int(m.group(1)), url, text))
                else:
                    title_links.append((url, text))
                    all_seen.add(url.rstrip("/"))

            # If no direct title links, follow the most recent year to find them
            if not title_links and year_links:
                year_links.sort(reverse=True)
                best_year, year_url, _ = year_links[0]
                logger.info("Justia %s: no direct titles, following year %d", state_slug, best_year)
                year_html = self._fetch_page(year_url)
                year_soup = BeautifulSoup(year_html, "html.parser")
                for url, text in _extract_links(year_soup, year_url, all_seen):
                    if not year_pat.search(url):
                        title_links.append((url, text))
                        all_seen.add(url.rstrip("/"))

            logger.info("Justia %s: found %d title links", state_slug, len(title_links))

            # Level 2+3: For each title, fetch title page then chapter pages
            for title_url, title_text in title_links:
                safe = _slugify(title_text or title_url.split("/")[-2])[:60]
                tdir = raw_dir / safe
                tdir.mkdir(exist_ok=True)
                try:
                    html = self._fetch_page(title_url)
                    (tdir / "index.html").write_text(html, encoding="utf-8")

                    # Find chapter links from title page
                    tsoup = BeautifulSoup(html, "html.parser")
                    for ch_url, ch_text in _extract_links(tsoup, title_url, all_seen):
                        if year_pat.search(ch_url):
                            continue
                        sname = _slugify(ch_text or ch_url.split("/")[-2])[:60]
                        try:
                            shtml = self._fetch_page(ch_url)
                            (tdir / f"{sname}.html").write_text(shtml, encoding="utf-8")
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception as e:
            logger.warning("Failed Justia fetch for %s: %s", state_slug, e)

    def _fetch_connecticut(self, raw_dir: Path) -> None:
        """CT: Use Justia (official site only has TOC pages)."""
        self._fetch_justia(raw_dir, "connecticut")

    def _fetch_nebraska(self, raw_dir: Path) -> None:
        """NE: Use Justia (official site requires per-section fetching)."""
        self._fetch_justia(raw_dir, "nebraska")


# ================================================================
# Handler registries
# ================================================================

_FETCH_HANDLERS: dict[str, callable] = {
    "alabama": OfficialWebsiteIngestor._fetch_alabama,
    "alaska": OfficialWebsiteIngestor._fetch_alaska,
    "arizona": OfficialWebsiteIngestor._fetch_arizona,
    "arkansas": OfficialWebsiteIngestor._fetch_arkansas,
    "california": OfficialWebsiteIngestor._fetch_california,
    "colorado": OfficialWebsiteIngestor._fetch_colorado,
    "connecticut": OfficialWebsiteIngestor._fetch_connecticut,
    "delaware": OfficialWebsiteIngestor._fetch_delaware,
    "florida": OfficialWebsiteIngestor._fetch_florida,
    "georgia": OfficialWebsiteIngestor._fetch_georgia,
    "hawaii": OfficialWebsiteIngestor._fetch_hawaii,
    "idaho": OfficialWebsiteIngestor._fetch_idaho,
    "illinois": OfficialWebsiteIngestor._fetch_illinois,
    "indiana": OfficialWebsiteIngestor._fetch_indiana,
    "iowa": OfficialWebsiteIngestor._fetch_iowa,
    "kansas": OfficialWebsiteIngestor._fetch_kansas,
    "kentucky": OfficialWebsiteIngestor._fetch_kentucky,
    "louisiana": OfficialWebsiteIngestor._fetch_louisiana,
    "maine": OfficialWebsiteIngestor._fetch_maine,
    "maryland": OfficialWebsiteIngestor._fetch_maryland,
    "massachusetts": OfficialWebsiteIngestor._fetch_massachusetts,
    "michigan": OfficialWebsiteIngestor._fetch_michigan,
    "minnesota": OfficialWebsiteIngestor._fetch_minnesota,
    "mississippi": OfficialWebsiteIngestor._fetch_mississippi,
    "missouri": OfficialWebsiteIngestor._fetch_missouri,
    "montana": OfficialWebsiteIngestor._fetch_montana,
    "nebraska": OfficialWebsiteIngestor._fetch_nebraska,
    "nevada": OfficialWebsiteIngestor._fetch_nevada,
    "new-hampshire": OfficialWebsiteIngestor._fetch_new_hampshire,
    "new-jersey": OfficialWebsiteIngestor._fetch_new_jersey,
    "new-mexico": OfficialWebsiteIngestor._fetch_new_mexico,
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
    "tennessee": OfficialWebsiteIngestor._fetch_tennessee,
    "texas": OfficialWebsiteIngestor._fetch_texas,
    "utah": OfficialWebsiteIngestor._fetch_utah,
    "vermont": OfficialWebsiteIngestor._fetch_vermont,
    "virginia": OfficialWebsiteIngestor._fetch_virginia,
    "washington": OfficialWebsiteIngestor._fetch_washington,
    "west-virginia": OfficialWebsiteIngestor._fetch_west_virginia,
    "wisconsin": OfficialWebsiteIngestor._fetch_wisconsin,
    "wyoming": OfficialWebsiteIngestor._fetch_wyoming,
    "guam": OfficialWebsiteIngestor._fetch_guam,
    "puerto-rico": OfficialWebsiteIngestor._fetch_puerto_rico,
    "us-virgin-islands": OfficialWebsiteIngestor._fetch_us_virgin_islands,
}

def _parse_new_york_impl(self, raw_path: Path) -> list[Title]:
    """Parse NY JSON API responses into titles."""
    titles = []
    for json_file in sorted(raw_path.glob("*.json")):
        if json_file.name == "laws.json":
            continue
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            result = data.get("result", {})
            documents = result.get("documents", {}).get("items", [])
            if not documents:
                continue
            law_id = json_file.stem
            sections = []
            seen = set()
            for doc in documents:
                doc_type = doc.get("docType", "")
                if doc_type != "SECTION":
                    continue
                num = doc.get("docLevelId", "")
                if not num or num in seen:
                    continue
                seen.add(num)
                heading = doc.get("title", "")
                text = doc.get("text", "")
                sections.append(Section(
                    id=f"section-{_slugify(num)}",
                    number=num,
                    heading=heading,
                    text=clean_text(text),
                ))
            if sections:
                info = result.get("info", {})
                heading = info.get("name", law_id)
                titles.append(Title(
                    id=f"title-{_slugify(law_id)}",
                    number=law_id,
                    heading=heading,
                    chapters=[Chapter(
                        id=f"chapter-{_slugify(law_id)}",
                        number=law_id,
                        heading=heading,
                        sections=sections,
                    )],
                ))
        except Exception as e:
            logger.warning("Failed to parse NY law %s: %s", json_file.name, e)
    return titles


OfficialWebsiteIngestor._parse_new_york = _parse_new_york_impl


def _parse_idaho_impl(self, raw_path: Path) -> list[Title]:
    """Parse Idaho's table-based section listings (section number in link, heading in adjacent td)."""
    titles = []
    for tdir in sorted(d for d in raw_path.iterdir() if d.is_dir()):
        chapters = []
        for html_file in sorted(tdir.glob("*.html")):
            content = html_file.read_text(encoding="utf-8", errors="replace")
            if content.startswith("%PDF") or "\x00" in content[:1000]:
                continue
            soup = BeautifulSoup(content, "html.parser")
            sections = []
            seen = set()
            # Find table rows with links to /SECT patterns
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "SECT" not in href.upper():
                    continue
                num = a.get_text(strip=True)
                if not num or num in seen or len(num) > 30:
                    continue
                # Get heading from adjacent td
                td = a.find_parent("td")
                if td:
                    row = td.find_parent("tr")
                    if row:
                        tds = row.find_all("td")
                        heading = ""
                        for cell in tds:
                            cell_text = cell.get_text(strip=True)
                            if cell_text and cell_text != num and len(cell_text) > 2:
                                heading = cell_text
                                break
                        seen.add(num)
                        sections.append(Section(
                            id=f"section-{_slugify(num)}",
                            number=num,
                            heading=heading.strip().rstrip("."),
                            text="",
                        ))
            if sections:
                ch_name = html_file.stem
                chapters.append(Chapter(
                    id=f"chapter-{ch_name}",
                    number=ch_name,
                    heading=ch_name.replace("-", " ").title(),
                    sections=sections,
                ))
        if chapters:
            titles.append(Title(
                id=f"title-{tdir.name}",
                number=tdir.name,
                heading=tdir.name.replace("-", " ").title(),
                chapters=chapters,
            ))
    return titles


def _parse_missouri_impl(self, raw_path: Path) -> list[Title]:
    """Parse Missouri's table-based format (section link + heading in adjacent td)."""
    titles = []
    for html_file in sorted(raw_path.glob("*.html")):
        if html_file.name == "index.html":
            continue
        content = html_file.read_text(encoding="utf-8", errors="replace")
        if content.startswith("%PDF") or "\x00" in content[:1000]:
            continue
        soup = BeautifulSoup(content, "html.parser")
        sections = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "PageSelect.aspx" not in href or "section=" not in href:
                continue
            num = a.get_text(strip=True)
            if not num or num in seen or len(num) > 30:
                continue
            # Get heading from next td sibling
            td = a.find_parent("td")
            heading = ""
            if td:
                next_td = td.find_next_sibling("td")
                if next_td:
                    heading = next_td.get_text(strip=True)
                    # Remove date suffix like "(8/28/1939)"
                    heading = re.sub(r"\s*\(\d+/\d+/\d+\)\s*$", "", heading)
            seen.add(num)
            sections.append(Section(
                id=f"section-{_slugify(num)}",
                number=num,
                heading=heading.strip(),
                text="",
            ))
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


def _parse_washington_impl(self, raw_path: Path) -> list[Title]:
    """Parse Washington's RCW table-based format (cite links + headings in adjacent td)."""
    titles = []
    for html_file in sorted(raw_path.glob("*.html")):
        if html_file.name == "index.html":
            continue
        content = html_file.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(content, "html.parser")
        sections = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "cite=" not in href:
                continue
            num = a.get_text(strip=True)
            if not num or num in seen or len(num) > 30:
                continue
            td = a.find_parent("td")
            heading = ""
            if td:
                next_td = td.find_next_sibling("td")
                if next_td:
                    heading = next_td.get_text(strip=True)
            seen.add(num)
            sections.append(Section(
                id=f"section-{_slugify(num)}",
                number=num,
                heading=heading.strip(),
                text="",
            ))
        # Also try generic extraction
        if not sections:
            sections = extract_sections_from_soup(soup)
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


def _parse_illinois_impl(self, raw_path: Path) -> list[Title]:
    """Parse Illinois ILCS format - sections from full chapter text pages."""
    titles = []
    for tdir in sorted(d for d in raw_path.iterdir() if d.is_dir()):
        chapters = []
        for html_file in sorted(tdir.glob("*.html")):
            if html_file.name == "index.html":
                continue
            content = html_file.read_text(encoding="utf-8", errors="replace")
            if content.startswith("%PDF") or "\x00" in content[:1000]:
                continue
            soup = BeautifulSoup(content, "html.parser")
            sections = []
            seen = set()
            # ILCS format: "Sec. 1-101. Heading" or "(5 ILCS 100/1-5)"
            ilcs_pat = re.compile(r"Sec\.?\s*([\d\-\.a-zA-Z]+)\s*[.\s]+(.*)")
            ilcs_cite_pat = re.compile(r"\([\d]+\s+ILCS\s+[\d]+/([\d\-\.a-zA-Z]+)\)")
            for elem in soup.find_all(["p", "div", "span", "b", "td", "a"]):
                text = elem.get_text(strip=True)
                if not text or len(text) < 5:
                    continue
                match = ilcs_pat.match(text)
                if match:
                    num = clean_section_number(match.group(1))
                    if num and num not in seen and len(num) <= 30:
                        seen.add(num)
                        heading = match.group(2).split("\n")[0].strip()[:300]
                        sections.append(Section(
                            id=f"section-{_slugify(num)}",
                            number=num,
                            heading=heading,
                            text="",
                        ))
            # Fallback to generic
            if not sections:
                sections = extract_sections_from_soup(soup)
            if sections:
                ch_name = html_file.stem
                chapters.append(Chapter(
                    id=f"chapter-{ch_name}",
                    number=ch_name,
                    heading=ch_name.replace("-", " ").title(),
                    sections=sections,
                ))
        # Also check index.html
        if not chapters:
            idx = tdir / "index.html"
            if idx.exists():
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
    return titles


def _parse_justia_impl(self, raw_path: Path) -> list[Title]:
    """Parse Justia law pages - used as fallback for LexisNexis-hosted states."""
    titles = []
    for tdir in sorted(d for d in raw_path.iterdir() if d.is_dir()):
        chapters = []
        for html_file in sorted(tdir.glob("*.html")):
            if html_file.name == "index.html":
                continue
            content = html_file.read_text(encoding="utf-8", errors="replace")
            if content.startswith("%PDF") or "\x00" in content[:1000]:
                continue
            soup = BeautifulSoup(content, "html.parser")
            sections = extract_sections_from_soup(soup)
            # Also try Justia-specific: look for links with section patterns
            if not sections:
                seen = set()
                for a in soup.find_all("a", href=True):
                    text = a.get_text(strip=True)
                    if not text:
                        continue
                    # Justia format: "Section 1-1-1 - Heading" or "§ 1-1. Heading"
                    for pat in [
                        re.compile(r"(?:§+\s*)([\d\-\.a-zA-Z:]+)\s*[\-–—.\s]+(.*)", re.DOTALL),
                        re.compile(r"Section\s+([\d\-\.a-zA-Z:]+)\s*[\-–—.\s]+(.*)", re.DOTALL),
                        re.compile(r"^([\d]+[\-\.]\d[\d\-\.a-zA-Z]*)\s*[\-–—.\s]+(.*)", re.DOTALL),
                    ]:
                        match = pat.match(text)
                        if match:
                            num = clean_section_number(match.group(1))
                            if num and num not in seen and len(num) <= 30:
                                seen.add(num)
                                heading = match.group(2).strip()[:300]
                                sections.append(Section(
                                    id=f"section-{_slugify(num)}",
                                    number=num,
                                    heading=heading,
                                    text="",
                                ))
                            break
            if sections:
                ch_name = html_file.stem
                chapters.append(Chapter(
                    id=f"chapter-{ch_name}",
                    number=ch_name,
                    heading=ch_name.replace("-", " ").title(),
                    sections=sections,
                ))
        # Also parse flat files
        if not chapters:
            idx = tdir / "index.html"
            if idx.exists():
                content = idx.read_text(encoding="utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")
                sections = extract_sections_from_soup(soup)
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
        content = html_file.read_text(encoding="utf-8", errors="replace")
        if content.startswith("%PDF") or "\x00" in content[:1000]:
            continue
        soup = BeautifulSoup(content, "html.parser")
        sections = extract_sections_from_soup(soup)
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


_PARSE_HANDLERS: dict[str, callable] = {
    "idaho": _parse_idaho_impl,
    "missouri": _parse_missouri_impl,
    "washington": _parse_washington_impl,
    # All Justia-backed states use the same parser
    "alabama": _parse_justia_impl,
    "arkansas": _parse_justia_impl,
    "california": _parse_justia_impl,
    "colorado": _parse_justia_impl,
    "connecticut": _parse_justia_impl,
    "georgia": _parse_justia_impl,
    "guam": _parse_justia_impl,
    "hawaii": _parse_justia_impl,
    "illinois": _parse_justia_impl,
    "indiana": _parse_justia_impl,
    "iowa": _parse_justia_impl,
    "kansas": _parse_justia_impl,
    "louisiana": _parse_justia_impl,
    "maryland": _parse_justia_impl,
    "michigan": _parse_justia_impl,
    "mississippi": _parse_justia_impl,
    "montana": _parse_justia_impl,
    "nebraska": _parse_justia_impl,
    "new-jersey": _parse_justia_impl,
    "new-mexico": _parse_justia_impl,
    "new-york": _parse_justia_impl,
    "oklahoma": _parse_justia_impl,
    "puerto-rico": _parse_justia_impl,
    "south-dakota": _parse_justia_impl,
    "tennessee": _parse_justia_impl,
    "texas": _parse_justia_impl,
    "us-virgin-islands": _parse_justia_impl,
    "utah": _parse_justia_impl,
    "virginia": _parse_justia_impl,
    "wyoming": _parse_justia_impl,
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
        # NRS 0.010 (Nevada style - may use EN SPACE \u2002 and replacement chars)
        re.compile(r"NRS[\s\u2002\u00a0]+([\d\.]+[A-Z]?)[\s\u2002\u00a0\ufffd]+([A-Za-z].*)", re.DOTALL),
        # "Section: 1:1 Heading" or "Section: 1-A:1 Heading" (NH style)
        re.compile(r"Section:\s+([\d\-a-zA-Z:]+)\s+(.*)", re.DOTALL),
        # Section 1-1-1. or Sec. 1-1-1.
        re.compile(r"Sec(?:tion)?\.?\s*([\d\-\.a-zA-Z:]+)\s*[.\-–—:\s]+(.*)", re.DOTALL),
        # KRS style ".1-101 Short title" (dot-prefixed)
        re.compile(r"\.([\d]+-[\d]+[a-zA-Z]?)\s+(.*)", re.DOTALL),
        # Numbered like "1-101." or "12.01." at start of line
        re.compile(r"^([\d]+[\-\.]\d[\d\-\.a-zA-Z]*)\s*[.\-–—:\s]+(.*)", re.DOTALL),
        # ORS style "001.010" or "1.010"
        re.compile(r"^(\d{1,3}\.\d{3,}[a-zA-Z]?)\s*[.\s]+(.*)", re.DOTALL),
        # Art./Article prefix
        re.compile(r"Art(?:icle)?\.?\s*([\d\-\.a-zA-Z]+)\s*[.\-–—:\s]+(.*)", re.DOTALL),
    ]

    for elem in soup.find_all(["p", "div", "li", "span", "td", "h2", "h3", "h4", "h5", "dt", "dd", "b", "strong", "a"]):
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

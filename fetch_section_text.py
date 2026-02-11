"""Fetch actual statute text from Justia individual section pages.

Two strategies for discovering section URLs:
1. Extract from cached chapter HTML pages (where available)
2. Construct from toc.json + Justia URL patterns (fallback)

Usage:
    python fetch_section_text.py alabama arkansas maryland
    python fetch_section_text.py --all --max-sections 5000
"""
import json
import logging
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / "cache"
DATA_DIR = ROOT / "data" / "states"

# Rate limiting
MIN_DELAY = 1.5  # seconds between requests per worker
WORKERS = 4  # parallel curl processes
CURL_TIMEOUT = 45

# HTTP cache for fetched section pages
SECTION_CACHE_DIR = CACHE_DIR / "sections"


def curl_fetch(url: str) -> str | None:
    """Fetch a URL with curl, bypassing Cloudflare TLS fingerprinting."""
    cloudflare_markers = ["Just a moment", "Checking your browser", "cf-browser-verification"]
    for attempt in range(3):
        if attempt > 0:
            time.sleep(2 * attempt)
        try:
            result = subprocess.run(
                [
                    "curl", "-sL", "--max-time", str(CURL_TIMEOUT),
                    "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "-H", "Accept-Language: en-US,en;q=0.5",
                    "-H", "Accept-Encoding: identity",
                    url,
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=CURL_TIMEOUT + 15,
            )
            if result.returncode == 0 and len(result.stdout) > 500:
                if any(m in result.stdout[:2000] for m in cloudflare_markers):
                    logger.debug("Cloudflare challenge for %s (attempt %d)", url, attempt + 1)
                    continue
                return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
    return None


def extract_text_from_section_page(html: str) -> tuple[str, str]:
    """Extract statute heading and text from a Justia section page.

    Returns (heading, text) tuple.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Primary: #codes-content div
    content = soup.select_one("#codes-content")
    if not content:
        content = soup.select_one(".codes-listing")
    if not content:
        return "", ""

    # Remove navigation elements
    for nav in content.select("nav, .pagination, .breadcrumb, .sidebar"):
        nav.decompose()

    # Extract heading (first h1/h2/h3 in content)
    heading = ""
    heading_el = content.find(["h1", "h2", "h3"])
    if heading_el:
        heading = heading_el.get_text(strip=True)

    # Extract body text
    text_parts = []
    for elem in content.find_all(["p", "div", "blockquote"]):
        classes = " ".join(elem.get("class", []))
        if any(skip in classes for skip in ["nav", "breadcrumb", "pagination", "sidebar", "share"]):
            continue
        text = elem.get_text(strip=True)
        if text and len(text) > 10:
            if text.startswith("Section ") and " - " in text and len(text) < 150:
                continue
            text_parts.append(text)

    full_text = "\n\n".join(text_parts)

    # If the simple approach got too little, grab all content text
    if len(full_text) < 100:
        full_text = content.get_text(separator="\n\n", strip=True)
        for boilerplate in [
            "Disclaimer: These codes may not be the most recent version.",
            "the state's website for the most current",
            "There may be more current",
        ]:
            if boilerplate in full_text:
                idx = full_text.find(boilerplate)
                end = full_text.find("\n\n", idx)
                if end > 0:
                    full_text = full_text[:idx] + full_text[end:]
                else:
                    full_text = full_text[:idx]

    return heading.strip(), full_text.strip()


def get_section_urls_from_cache(state: str) -> list[tuple[str, str, str]]:
    """Extract section URLs from cached chapter HTML pages.

    If no section links are found directly, extracts chapter URLs from cached
    pages, fetches those chapter pages in parallel, and extracts section links
    from the fetched chapter pages.

    Returns list of (section_url, section_number, chapter_key) tuples.
    """
    raw_dir = CACHE_DIR / "raw" / state / "official"
    if not raw_dir.exists():
        return []

    section_urls = []
    chapter_urls = []  # Fallback: chapter URLs to fetch for section links
    seen_urls = set()
    seen_chapter_urls = set()
    section_pat = re.compile(r"/section[-/]")
    chapter_pat = re.compile(r"/chapter[-/]")

    for html_file in raw_dir.rglob("*.html"):
        if html_file.name in ("index.html", "next.html"):
            continue
        try:
            content = html_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        if "Just a moment" in content[:2000]:
            continue

        soup = BeautifulSoup(content, "html.parser")

        rel = html_file.relative_to(raw_dir)
        parts = list(rel.parts)
        if len(parts) >= 2:
            chapter_key = f"{parts[0]}/{html_file.stem}"
        else:
            chapter_key = html_file.stem

        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if not text or f"/codes/{state}/" not in href:
                continue
            if re.search(r"/\d{4}/", href):
                continue

            full_url = urljoin("https://law.justia.com/", href)

            if section_pat.search(href):
                if full_url not in seen_urls:
                    seen_urls.add(full_url)
                    sec_match = re.match(
                        r"(?:Section|§+|Sec\.?)\s*([\d\-\.a-zA-Z:]+)",
                        text,
                        re.IGNORECASE,
                    )
                    sec_num = sec_match.group(1) if sec_match else text[:40]
                    section_urls.append((full_url, sec_num, chapter_key))
            elif chapter_pat.search(href):
                if full_url not in seen_chapter_urls:
                    seen_chapter_urls.add(full_url)
                    chapter_urls.append((full_url, text, chapter_key))

    # If we found section URLs directly, return them
    if section_urls:
        return section_urls

    # Otherwise, fetch the chapter pages in parallel to find section links
    if not chapter_urls:
        return []

    logger.info(
        "%s: no section links in cache, fetching %d chapter pages...",
        state, len(chapter_urls),
    )

    def fetch_chapter_for_sections(ch_url_text_key):
        ch_url, ch_text, ch_key = ch_url_text_key
        time.sleep(MIN_DELAY)
        html = curl_fetch(ch_url)
        if not html:
            return []
        ch_soup = BeautifulSoup(html, "html.parser")
        results = []
        for a in ch_soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if not text or not section_pat.search(href):
                continue
            if f"/codes/{state}/" not in href:
                continue
            full = urljoin("https://law.justia.com/", href)
            sec_match = re.match(
                r"(?:Section|§+|Sec\.?)\s*([\d\-\.a-zA-Z:]+)",
                text,
                re.IGNORECASE,
            )
            sec_num = sec_match.group(1) if sec_match else text[:40]
            results.append((full, sec_num, f"{ch_key}/{ch_text}"))
        return results

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [
            executor.submit(fetch_chapter_for_sections, item)
            for item in chapter_urls
        ]
        for future in as_completed(futures):
            for sec_url, sec_num, ch_key in future.result():
                if sec_url not in seen_urls:
                    seen_urls.add(sec_url)
                    section_urls.append((sec_url, sec_num, ch_key))

    logger.info("%s: found %d section URLs from chapter pages", state, len(section_urls))
    return section_urls


def _extract_child_links(html: str, state: str, parent_url: str) -> list[tuple[str, str]]:
    """Extract child navigation links from a Justia page.

    Returns list of (url, link_text) for links that are children of parent_url.
    Filters out year links, external links, and non-child links.
    """
    soup = BeautifulSoup(html, "html.parser")
    parent_path = parent_url.rstrip("/").replace("https://law.justia.com", "")
    state_prefix = f"/codes/{state}/"
    seen = set()
    results = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if not text or state_prefix not in href:
            continue

        full = urljoin("https://law.justia.com/", href).rstrip("/") + "/"
        full_path = full.replace("https://law.justia.com", "").rstrip("/")

        # Skip year links, self-links, already-seen
        if re.search(r"/\d{4}/", full):
            continue
        if full_path == parent_path or full in seen:
            continue
        # Skip "Next" navigation
        if text.strip() == "Next":
            continue
        # Must be a child of the parent
        if not full_path.startswith(parent_path + "/"):
            continue

        seen.add(full)
        results.append((full, text.strip()))

    return results


def get_section_urls_from_justia_discovery(state: str) -> list[tuple[str, str, str]]:
    """Discover section URLs by crawling Justia's hierarchy.

    Handles variable depth: state → code → title → chapter → section.
    Crawls until it finds pages with section links.

    Returns list of (section_url, section_number, chapter_key) tuples.
    """
    base_url = f"https://law.justia.com/codes/{state}/"
    section_pat = re.compile(r"/section[-/]")
    section_urls = []
    seen_sections = set()

    logger.info("%s: discovering section URLs from Justia...", state)

    # Fetch state index page
    index_html = curl_fetch(base_url)
    if not index_html:
        logger.warning("%s: failed to fetch Justia index", state)
        return []

    # Some states have year-versioned indexes. Check for that first.
    soup = BeautifulSoup(index_html, "html.parser")
    has_year_link = False
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if f"/codes/{state}/2024/" in href or f"/codes/{state}/2023/" in href:
            has_year_link = True
            break

    # If year-versioned, we need to fetch the year page first to get code links
    if has_year_link:
        # The non-year child links are what we want
        level1_links = _extract_child_links(index_html, state, base_url)
        if not level1_links:
            # Try fetching the latest year page
            for year in ["2024", "2023", "2022"]:
                year_url = f"https://law.justia.com/codes/{state}/{year}/"
                time.sleep(MIN_DELAY)
                year_html = curl_fetch(year_url)
                if year_html:
                    # Extract non-year links from the year page
                    year_soup = BeautifulSoup(year_html, "html.parser")
                    for a in year_soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.get_text(strip=True)
                        if (f"/codes/{state}/" in href and
                                not re.search(r"/\d{4}/", href) and
                                text and text != "Next" and
                                href != f"/codes/{state}/"):
                            full = urljoin("https://law.justia.com/", href).rstrip("/") + "/"
                            if full not in [u for u, _ in level1_links]:
                                level1_links.append((full, text))
                    break
    else:
        level1_links = _extract_child_links(index_html, state, base_url)

    logger.info("%s: found %d level-1 URLs", state, len(level1_links))

    # BFS through the hierarchy until we find section links
    # Queue: (url, text, breadcrumb_for_chapter_key)
    pages_to_check = [(url, text, text) for url, text in level1_links]
    pages_checked = set()

    while pages_to_check:
        batch = pages_to_check[:50]  # Process in batches
        pages_to_check = pages_to_check[50:]
        next_level = []

        for page_url, page_text, chapter_key in batch:
            if page_url in pages_checked:
                continue
            pages_checked.add(page_url)

            time.sleep(MIN_DELAY)
            html = curl_fetch(page_url)
            if not html:
                continue

            page_soup = BeautifulSoup(html, "html.parser")

            # Check if this page has section links
            page_section_urls = []
            for a in page_soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if not text or not section_pat.search(href):
                    continue
                if f"/codes/{state}/" not in href:
                    continue

                full = urljoin("https://law.justia.com/", href).rstrip("/") + "/"
                if full in seen_sections:
                    continue
                seen_sections.add(full)

                sec_match = re.match(
                    r"(?:Section|§+|Sec\.?)\s*([\d\-\.a-zA-Z:]+)",
                    text,
                    re.IGNORECASE,
                )
                sec_num = sec_match.group(1) if sec_match else text[:40]
                page_section_urls.append((full, sec_num, chapter_key))

            if page_section_urls:
                section_urls.extend(page_section_urls)
            else:
                # No section links found. Go deeper.
                child_links = _extract_child_links(html, state, page_url)
                for child_url, child_text in child_links:
                    if child_url not in pages_checked:
                        next_level.append((child_url, child_text, f"{chapter_key}/{child_text}"))

        if next_level:
            pages_to_check = next_level + pages_to_check

        if section_urls and len(pages_checked) % 20 == 0:
            logger.info(
                "%s: discovered %d sections so far (%d pages checked)",
                state, len(section_urls), len(pages_checked),
            )

    logger.info("%s: discovered %d section URLs from %d pages", state, len(section_urls), len(pages_checked))
    return section_urls


def fetch_and_extract(url: str) -> tuple[str, str, str]:
    """Fetch a section URL and extract text. Returns (url, heading, text)."""
    # Check cache first
    cache_key = url.replace("https://law.justia.com/codes/", "").strip("/").replace("/", "_")
    cache_file = SECTION_CACHE_DIR / f"{cache_key}.html"
    if cache_file.exists():
        try:
            html = cache_file.read_text(encoding="utf-8", errors="replace")
            if html and len(html) > 500 and "Just a moment" not in html[:2000]:
                heading, text = extract_text_from_section_page(html)
                return url, heading, text
        except Exception:
            pass

    time.sleep(MIN_DELAY)
    html = curl_fetch(url)
    if not html:
        return url, "", ""

    # Cache the response
    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(html, encoding="utf-8")
    except Exception:
        pass

    heading, text = extract_text_from_section_page(html)
    return url, heading, text


def update_content_json_direct(state: str, section_data: list[tuple[str, str, str, str]]) -> int:
    """Update content JSON with fetched section text using direct URL-to-section matching.

    section_data: list of (url, section_number, heading, text) tuples.
    Returns number of sections updated.
    """
    content_dir = DATA_DIR / state / "content"
    if not content_dir.exists():
        return 0

    # Build lookup: section_number -> (url, heading, text)
    # Use the URL's last path segment as the primary key
    url_lookup = {}
    for url, sec_num, heading, text in section_data:
        if not text:
            continue
        url_slug = url.rstrip("/").split("/")[-1]  # e.g., "section-1-1-1"
        url_lookup[url_slug] = (url, heading, text)
        # Also index by section number
        url_lookup[sec_num] = (url, heading, text)

    updated = 0
    for json_file in content_dir.rglob("*.json"):
        try:
            with open(json_file, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        modified = False
        for section in data.get("sections", []):
            sec_num = section.get("number", "").strip()
            if not sec_num:
                continue

            # Try exact match on section number first
            match = url_lookup.get(sec_num)

            # Try constructing the expected slug
            if not match:
                num_slug = "section-" + re.sub(r"[^a-z0-9]", "-", sec_num.lower()).strip("-")
                match = url_lookup.get(num_slug)

            if match:
                url, heading, text = match
                if text and len(text) > len(section.get("text", "")):
                    section["text"] = text
                    if heading and not section.get("heading"):
                        section["heading"] = heading
                    section["source_url"] = url
                    modified = True
                    updated += 1

        if modified:
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

    return updated


def process_state(state: str, max_sections: int = 0) -> tuple[str, int, int, int]:
    """Fetch section text for a state. Returns (state, total_urls, fetched, updated)."""
    start = time.time()

    # Strategy 1: Try cached chapter pages first
    section_urls = get_section_urls_from_cache(state)

    # Strategy 2: If no cached section links, discover from Justia
    if not section_urls:
        section_urls = get_section_urls_from_justia_discovery(state)

    if not section_urls:
        logger.warning("%s: no section URLs found", state)
        return state, 0, 0, 0

    total = len(section_urls)
    if max_sections and total > max_sections:
        section_urls = section_urls[:max_sections]
        logger.info("%s: limiting to %d/%d sections", state, max_sections, total)

    logger.info("%s: fetching text for %d sections...", state, len(section_urls))

    # Fetch section pages in parallel
    results = []
    fetched = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(fetch_and_extract, url): (url, num, ch)
            for url, num, ch in section_urls
        }
        for future in as_completed(futures):
            url, heading, text = future.result()
            orig_url, num, ch = futures[future]
            results.append((url, num, heading, text))
            if text:
                fetched += 1
            if fetched % 50 == 0 and fetched > 0:
                elapsed = time.time() - start
                rate = fetched / elapsed * 3600
                logger.info(
                    "%s: %d/%d fetched (%.0f/hr), %d with text",
                    state, len(results), len(section_urls), rate, fetched,
                )

    # Update content JSON files
    updated = update_content_json_direct(state, results)

    elapsed = time.time() - start
    logger.info(
        "%s: done in %.0fs. %d URLs, %d fetched with text, %d sections updated",
        state, elapsed, total, fetched, updated,
    )
    return state, total, fetched, updated


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch section text from Justia")
    parser.add_argument("states", nargs="*", help="State slugs to process")
    parser.add_argument("--all", action="store_true", help="Process all states")
    parser.add_argument(
        "--max-sections", type=int, default=0,
        help="Max sections per state (0=unlimited)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")
    args = parser.parse_args()

    global WORKERS
    WORKERS = args.workers

    if args.all:
        states = []
        for state in sorted(os.listdir(str(DATA_DIR))):
            mf = DATA_DIR / state / "manifest.json"
            if mf.exists():
                with open(mf, encoding="utf-8") as f:
                    m = json.load(f)
                secs = m["stats"].get("sections", 0)
                if secs > 0:
                    states.append((secs, state))
        states.sort()
        state_list = [s for _, s in states]
    elif args.states:
        state_list = args.states
    else:
        parser.print_help()
        return

    print(f"Processing {len(state_list)} states...")
    sys.stdout.flush()

    for i, state in enumerate(state_list, 1):
        print(f"\n[{i}/{len(state_list)}] {state}...")
        sys.stdout.flush()
        state_name, total, fetched, updated = process_state(
            state, max_sections=args.max_sections,
        )
        print(f"  {state}: {total} URLs, {fetched} fetched, {updated} sections updated")
        sys.stdout.flush()


if __name__ == "__main__":
    main()

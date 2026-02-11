"""Fetch statute text for states that need Justia URL discovery.

For states where cached chapter pages don't have section links, this script:
1. Uses cached title/code pages to find chapter URLs
2. Fetches chapter pages to find section URLs
3. Fetches section pages and extracts text
4. Writes content JSON directly (doesn't match existing content)

Usage:
    python fetch_section_text_discovery.py texas florida california
    python fetch_section_text_discovery.py --all --max-sections 5000
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
SECTION_CACHE_DIR = CACHE_DIR / "sections"

MIN_DELAY = 1.5
WORKERS = 4
CURL_TIMEOUT = 45


def curl_fetch(url: str) -> str | None:
    """Fetch a URL with curl, bypassing Cloudflare."""
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
                    continue
                return result.stdout
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
    return None


def extract_text_from_section_page(html: str) -> tuple[str, str]:
    """Extract statute heading and text from a Justia section page."""
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("#codes-content")
    if not content:
        content = soup.select_one(".codes-listing")
    if not content:
        return "", ""

    for nav in content.select("nav, .pagination, .breadcrumb, .sidebar"):
        nav.decompose()

    heading = ""
    heading_el = content.find(["h1", "h2", "h3"])
    if heading_el:
        heading = heading_el.get_text(strip=True)

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


def discover_section_urls(state: str) -> list[tuple[str, str, str, str]]:
    """Discover section URLs for a state using cached pages + Justia fetching.

    Returns list of (section_url, section_number, title_slug, chapter_slug) tuples.
    """
    raw_dir = CACHE_DIR / "raw" / state / "official"
    section_pat = re.compile(r"/section[-/]")
    chapter_pat = re.compile(r"/chapter[-/]")

    # Step 1: Extract chapter URLs from cached pages
    chapter_urls = []
    seen = set()

    if raw_dir.exists():
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
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if not text or f"/codes/{state}/" not in href:
                    continue
                if re.search(r"/\d{4}/", href):
                    continue

                full = urljoin("https://law.justia.com/", href)
                if chapter_pat.search(href) and full not in seen:
                    seen.add(full)
                    # Parse title and chapter from URL
                    parts = full.replace(f"https://law.justia.com/codes/{state}/", "").strip("/").split("/")
                    title_slug = parts[0] if parts else ""
                    chapter_slug = parts[-1] if len(parts) > 1 else ""
                    chapter_urls.append((full, text, title_slug, chapter_slug))

    # If no chapter URLs from cache, try fetching Justia index
    if not chapter_urls:
        logger.info("%s: no chapter URLs in cache, fetching from Justia...", state)
        base_url = f"https://law.justia.com/codes/{state}/"
        index_html = curl_fetch(base_url)
        if not index_html:
            return []

        # Check for year-versioned index
        soup = BeautifulSoup(index_html, "html.parser")
        code_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if (f"/codes/{state}/" in href and
                    not re.search(r"/\d{4}/", href) and
                    text and text != "Next" and
                    href != f"/codes/{state}/"):
                full = urljoin("https://law.justia.com/", href)
                if full not in seen:
                    seen.add(full)
                    code_links.append((full, text))

        # If no non-year links, fetch latest year page
        if not code_links:
            for year in ["2025", "2024", "2023", "2022"]:
                year_url = f"https://law.justia.com/codes/{state}/{year}/"
                time.sleep(MIN_DELAY)
                year_html = curl_fetch(year_url)
                if year_html:
                    year_soup = BeautifulSoup(year_html, "html.parser")
                    for a in year_soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.get_text(strip=True)
                        if (f"/codes/{state}/" in href and
                                not re.search(r"/\d{4}/", href) and
                                text and text != "Next" and
                                href != f"/codes/{state}/"):
                            full = urljoin("https://law.justia.com/", href)
                            if full not in seen:
                                seen.add(full)
                                code_links.append((full, text))
                    break

        # Fetch code/title pages to find chapter URLs
        logger.info("%s: fetching %d code/title pages...", state, len(code_links))
        for code_url, code_text in code_links:
            time.sleep(MIN_DELAY)
            html = curl_fetch(code_url)
            if not html:
                continue
            code_soup = BeautifulSoup(html, "html.parser")
            code_path = code_url.replace("https://law.justia.com/codes/", "").strip("/")

            for a in code_soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)
                if not text or f"/codes/{state}/" not in href or text == "Next":
                    continue
                if re.search(r"/\d{4}/", href):
                    continue
                full = urljoin("https://law.justia.com/", href)
                full_path = full.replace("https://law.justia.com/codes/", "").strip("/")

                # Must be a child of this code page
                if full_path.startswith(code_path + "/") and full not in seen:
                    seen.add(full)
                    parts = full_path.replace(f"{state}/", "").split("/")
                    title_slug = parts[0] if parts else code_text
                    chapter_slug = parts[-1] if len(parts) > 1 else ""

                    # Check if this is a chapter (has section links) or title (has chapter links)
                    if chapter_pat.search(href):
                        chapter_urls.append((full, text, title_slug, chapter_slug))
                    else:
                        # This might be a title page, need to go deeper
                        time.sleep(MIN_DELAY)
                        title_html = curl_fetch(full)
                        if title_html:
                            title_soup = BeautifulSoup(title_html, "html.parser")
                            for a2 in title_soup.find_all("a", href=True):
                                href2 = a2["href"]
                                text2 = a2.get_text(strip=True)
                                if (text2 and f"/codes/{state}/" in href2 and
                                        chapter_pat.search(href2) and text2 != "Next"):
                                    full2 = urljoin("https://law.justia.com/", href2)
                                    if full2 not in seen:
                                        seen.add(full2)
                                        parts2 = full2.replace(f"https://law.justia.com/codes/{state}/", "").strip("/").split("/")
                                        chapter_urls.append((
                                            full2, text2,
                                            parts2[0] if parts2 else title_slug,
                                            parts2[-1] if len(parts2) > 1 else "",
                                        ))

    logger.info("%s: found %d chapter URLs", state, len(chapter_urls))

    # Step 2: Fetch chapter pages in parallel to get section URLs
    section_urls = []
    seen_sections = set()

    def fetch_chapter_sections(item):
        ch_url, ch_text, title_slug, chapter_slug = item
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
            sec_num = sec_match.group(1) if sec_match else text[:60]
            results.append((full, sec_num, title_slug, chapter_slug))
        return results

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = [executor.submit(fetch_chapter_sections, item) for item in chapter_urls]
        for future in as_completed(futures):
            for sec_url, sec_num, title_slug, chapter_slug in future.result():
                if sec_url not in seen_sections:
                    seen_sections.add(sec_url)
                    section_urls.append((sec_url, sec_num, title_slug, chapter_slug))

    logger.info("%s: discovered %d section URLs", state, len(section_urls))
    return section_urls


def fetch_and_extract_cached(url: str) -> tuple[str, str, str]:
    """Fetch a section URL with caching. Returns (url, heading, text)."""
    cache_key = url.replace("https://law.justia.com/codes/", "").strip("/").replace("/", "_")
    cache_file = SECTION_CACHE_DIR / f"{cache_key}.html"

    if cache_file.exists():
        try:
            html = cache_file.read_text(encoding="utf-8", errors="replace")
            if html and len(html) > 500 and "Just a moment" not in html[:2000]:
                heading, text = extract_text_from_section_page(html)
                if text:
                    return url, heading, text
        except Exception:
            pass

    time.sleep(MIN_DELAY)
    html = curl_fetch(url)
    if not html:
        return url, "", ""

    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(html, encoding="utf-8")
    except Exception:
        pass

    heading, text = extract_text_from_section_page(html)
    return url, heading, text


def slugify(text: str) -> str:
    """Convert text to a URL-friendly slug."""
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug[:80] if slug else "unknown"


def write_content_json(state: str, sections_by_chapter: dict) -> tuple[int, int]:
    """Write content JSON files organized by title/chapter.

    sections_by_chapter: {(title_slug, chapter_slug): [(number, heading, text, url), ...]}
    Returns (files_written, sections_written).
    """
    content_dir = DATA_DIR / state / "content"
    content_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0
    sections_written = 0

    for (title_slug, chapter_slug), sections in sections_by_chapter.items():
        title_dir = content_dir / f"title-{slugify(title_slug)}"
        title_dir.mkdir(parents=True, exist_ok=True)

        chapter_name = f"chapter-{slugify(chapter_slug)}" if chapter_slug else "chapter-main"
        json_file = title_dir / f"{chapter_name}.json"

        # Read existing file if it exists
        existing_sections = {}
        if json_file.exists():
            try:
                with open(json_file, encoding="utf-8") as f:
                    data = json.load(f)
                for sec in data.get("sections", []):
                    existing_sections[sec.get("number", "")] = sec
            except Exception:
                pass

        # Merge new sections with existing
        for number, heading, text, url in sections:
            if number in existing_sections:
                existing = existing_sections[number]
                if text and len(text) > len(existing.get("text", "")):
                    existing["text"] = text
                    existing["source_url"] = url
                    if heading and not existing.get("heading"):
                        existing["heading"] = heading
            else:
                existing_sections[number] = {
                    "id": f"section-{slugify(number)}",
                    "number": number,
                    "heading": heading,
                    "text": text,
                    "source_url": url,
                }

        # Write the file
        output = {
            "state": state,
            "path": f"title-{slugify(title_slug)}/{chapter_name}",
            "sections": list(existing_sections.values()),
        }

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        files_written += 1
        sections_written += len(sections)

    return files_written, sections_written


def process_state(state: str, max_sections: int = 0) -> tuple[str, int, int, int]:
    """Process a state: discover URLs, fetch text, write content."""
    start = time.time()

    # Discover section URLs
    section_urls = discover_section_urls(state)
    if not section_urls:
        logger.warning("%s: no section URLs found", state)
        return state, 0, 0, 0

    total = len(section_urls)
    if max_sections and total > max_sections:
        section_urls = section_urls[:max_sections]
        logger.info("%s: limiting to %d/%d sections", state, max_sections, total)

    logger.info("%s: fetching text for %d sections...", state, len(section_urls))

    # Fetch section pages in parallel
    sections_by_chapter = {}
    fetched = 0
    done_count = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {}
        for url, sec_num, title_slug, chapter_slug in section_urls:
            future = executor.submit(fetch_and_extract_cached, url)
            futures[future] = (url, sec_num, title_slug, chapter_slug)

        for future in as_completed(futures):
            url, heading, text = future.result()
            orig_url, sec_num, title_slug, chapter_slug = futures[future]
            done_count += 1

            if text:
                fetched += 1
                key = (title_slug, chapter_slug)
                if key not in sections_by_chapter:
                    sections_by_chapter[key] = []
                sections_by_chapter[key].append((sec_num, heading, text, url))

            if fetched % 100 == 0 and fetched > 0:
                elapsed = time.time() - start
                rate = fetched / elapsed * 3600
                logger.info(
                    "%s: %d/%d done, %d with text (%.0f/hr)",
                    state, done_count, len(section_urls), fetched, rate,
                )

    # Write content JSON
    files_written, sections_written = write_content_json(state, sections_by_chapter)

    elapsed = time.time() - start
    logger.info(
        "%s: done in %.0fs. %d URLs, %d fetched, %d files, %d sections written",
        state, elapsed, total, fetched, files_written, sections_written,
    )
    return state, total, fetched, sections_written


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch section text via Justia discovery")
    parser.add_argument("states", nargs="*", help="State slugs to process")
    parser.add_argument("--all", action="store_true", help="Process all states needing discovery")
    parser.add_argument(
        "--max-sections", type=int, default=0,
        help="Max sections per state (0=unlimited)",
    )
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers")
    args = parser.parse_args()

    global WORKERS
    WORKERS = args.workers

    # States that need discovery (no section links in cache)
    discovery_states = {
        "alaska", "arizona", "california", "colorado", "delaware", "florida",
        "idaho", "illinois", "indiana", "kentucky", "louisiana", "maine",
        "massachusetts", "minnesota", "missouri", "montana", "nebraska",
        "nevada", "new-hampshire", "new-jersey", "new-york", "north-carolina",
        "north-dakota", "ohio", "oregon", "pennsylvania", "puerto-rico",
        "rhode-island", "south-carolina", "texas", "vermont", "washington",
        "west-virginia", "wisconsin",
    }

    if args.all:
        states = []
        for state in sorted(os.listdir(str(DATA_DIR))):
            if state in discovery_states:
                mf = DATA_DIR / state / "manifest.json"
                if mf.exists():
                    states.append(state)
        state_list = states
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
        state_name, total, fetched, written = process_state(
            state, max_sections=args.max_sections,
        )
        print(f"  {state}: {total} URLs, {fetched} fetched, {written} sections written")
        sys.stdout.flush()


if __name__ == "__main__":
    main()

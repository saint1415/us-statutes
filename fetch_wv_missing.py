"""Fetch missing West Virginia section text by constructing URLs from section numbers.

WV sections follow pattern: chapter-{chap}/article-{article}/section-{number}/
Section numbers like "2-2-1b" -> chapter=2, article=2, section=2-2-1b.
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

from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data" / "states" / "west-virginia" / "content"
CACHE_DIR = ROOT / "cache" / "sections"

MIN_DELAY = 1.5
WORKERS = 4
CURL_TIMEOUT = 45


def curl_fetch(url: str) -> str | None:
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
    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one("#codes-content")
    if not content:
        content = soup.select_one(".codes-listing")
    if not content:
        return "", ""

    heading = ""
    h1 = content.select_one("h1")
    if h1:
        heading = h1.get_text(strip=True)

    text_parts = []
    for p in content.find_all("p"):
        text = p.get_text(strip=True)
        if text:
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


def section_to_url(section_number: str) -> str:
    """Convert a WV section number to its Justia URL.
    WV sections: "2-2-1b" -> chapter-2/article-2/section-2-2-1b/
    """
    parts = section_number.split("-")
    if len(parts) >= 2:
        chapter = parts[0]
        article = parts[1]
    else:
        chapter = parts[0]
        article = "1"
    return f"https://law.justia.com/codes/west-virginia/chapter-{chapter}/article-{article}/section-{section_number}/"


def fetch_section(section_number: str) -> tuple[str, str, str, str]:
    url = section_to_url(section_number)
    cache_key = url.replace("https://law.justia.com/codes/", "").strip("/").replace("/", "_")
    cache_file = CACHE_DIR / f"{cache_key}.html"

    if cache_file.exists():
        try:
            html = cache_file.read_text(encoding="utf-8", errors="replace")
            if html and len(html) > 500 and "Just a moment" not in html[:2000]:
                heading, text = extract_text_from_section_page(html)
                if text:
                    return section_number, heading, text, url
        except Exception:
            pass

    time.sleep(MIN_DELAY)
    html = curl_fetch(url)
    if not html:
        return section_number, "", "", url

    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(html, encoding="utf-8")
    except Exception:
        pass

    heading, text = extract_text_from_section_page(html)
    return section_number, heading, text, url


def main():
    missing = []
    total_sections = 0
    already_have = 0

    for title_dir in sorted(os.listdir(DATA_DIR)):
        title_path = DATA_DIR / title_dir
        if not title_path.is_dir():
            continue
        for fname in sorted(os.listdir(title_path)):
            if not fname.endswith(".json"):
                continue
            fpath = title_path / fname
            with open(fpath, encoding="utf-8", errors="replace") as f:
                data = json.load(f)
            for i, s in enumerate(data.get("sections", [])):
                total_sections += 1
                if not s.get("text"):
                    missing.append((title_dir, fname, i, s["number"]))
                else:
                    already_have += 1

    logger.info("Total sections: %d, already have text: %d, missing: %d",
                total_sections, already_have, len(missing))

    if not missing:
        logger.info("Nothing to do!")
        return

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    start_time = time.time()
    done = 0
    with_text = 0
    updates = {}

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {}
        for title_dir, fname, idx, sec_num in missing:
            future = executor.submit(fetch_section, sec_num)
            futures[future] = (title_dir, fname, idx)

        for future in as_completed(futures):
            title_dir, fname, idx = futures[future]
            sec_num, heading, text, url = future.result()
            done += 1

            key = (title_dir, fname)
            if key not in updates:
                updates[key] = []
            updates[key].append((idx, heading, text, url))

            if text:
                with_text += 1

            if done % 100 == 0:
                elapsed = time.time() - start_time
                rate = done / elapsed * 3600 if elapsed > 0 else 0
                logger.info("%d/%d done, %d with text (%d/hr)",
                           done, len(missing), with_text, int(rate))

    logger.info("Writing updates to %d content files...", len(updates))
    files_updated = 0
    for (title_dir, fname), section_updates in updates.items():
        fpath = DATA_DIR / title_dir / fname
        with open(fpath, encoding="utf-8", errors="replace") as f:
            data = json.load(f)

        changed = False
        for idx, heading, text, url in section_updates:
            if text and idx < len(data.get("sections", [])):
                section = data["sections"][idx]
                if not section.get("text"):
                    section["text"] = text
                    if heading and not section.get("heading"):
                        section["heading"] = heading
                    section["source_url"] = url
                    changed = True

        if changed:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            files_updated += 1

    logger.info("Done! Updated %d files. %d/%d missing sections now have text.",
                files_updated, with_text, len(missing))


if __name__ == "__main__":
    main()

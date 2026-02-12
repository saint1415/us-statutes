#!/usr/bin/env python3
"""Fetch missing Florida sections that require part-based URLs.

FL chapters on Justia are sometimes split into parts (e.g., /chapter-468/part-i/).
Sections in these chapters need the part in the URL path.
This script discovers parts and fetches section text.
"""
import json, os, glob, re, subprocess, time, logging, html as html_mod
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

WORKERS = 4
DELAY = 1.5
BASE = 'https://law.justia.com/codes/florida'


def extract_content(raw_html):
    """Extract text from Justia codes-content div."""
    match = re.search(r'id="codes-content"[^>]*>(.*)', raw_html, re.DOTALL)
    if not match:
        return None
    content = match.group(1)
    depth = 0
    end = len(content)
    i = 0
    while i < len(content):
        if content[i:i+4] == '<div':
            depth += 1
            i += 4
        elif content[i:i+6] == '</div>':
            if depth == 0:
                end = i
                break
            depth -= 1
            i += 6
        else:
            i += 1
    content = content[:end]
    text = re.sub(r'<[^>]+>', ' ', content)
    text = html_mod.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text if len(text) > 20 else None


def curl_get(url):
    """Fetch URL with browser headers."""
    result = subprocess.run([
        'curl', '-s', '-L', '--max-time', '15',
        '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        '-H', 'Accept: text/html,application/xhtml+xml',
        '-H', 'Accept-Language: en-US,en;q=0.9',
        url
    ], capture_output=True, text=True, timeout=20, encoding='utf-8', errors='replace')
    return result.stdout


def discover_section_urls(chapters_with_missing):
    """Discover section URLs by crawling chapter and part pages.

    Returns dict: section_number -> full_url
    """
    with open('florida_title_mapping.json') as f:
        title_map = json.load(f)

    section_urls = {}

    for chapter, missing_sections in chapters_with_missing.items():
        title_slug = title_map.get(chapter)
        if not title_slug:
            continue

        chapter_url = f'{BASE}/{title_slug}/chapter-{chapter}/'
        time.sleep(DELAY)
        html = curl_get(chapter_url)
        if not html:
            continue

        # Find part links
        parts = re.findall(r'href="(/codes/florida/[^"]*?/part-[^/"]+/)"', html)

        if parts:
            # Chapter has parts, crawl each part for section links
            for part_path in parts:
                part_url = f'https://law.justia.com{part_path}'
                time.sleep(DELAY)
                part_html = curl_get(part_url)
                if not part_html:
                    continue

                # Find section links in the part page
                sec_links = re.findall(r'href="(/codes/florida/[^"]*?/section-(\d+-\d+[a-z0-9]*)/)"', part_html)
                for sec_path, sec_slug in sec_links:
                    # Convert slug back to section number: 468-1115 -> 468.1115
                    sec_num = sec_slug.replace('-', '.', 1)
                    if sec_num in missing_sections:
                        section_urls[sec_num] = f'https://law.justia.com{sec_path}'

            log.info(f'  Chapter {chapter}: {len(parts)} parts, found {sum(1 for s in missing_sections if s in section_urls)} of {len(missing_sections)} missing section URLs')
        else:
            # No parts, sections should be directly accessible (but weren't found by standard fetch)
            # Try section links directly on the chapter page
            sec_links = re.findall(r'href="(/codes/florida/[^"]*?/section-(\d+-\d+[a-z0-9]*)/)"', html)
            for sec_path, sec_slug in sec_links:
                sec_num = sec_slug.replace('-', '.', 1)
                if sec_num in missing_sections:
                    section_urls[sec_num] = f'https://law.justia.com{sec_path}'

    return section_urls


def main():
    content_dir = 'data/states/florida/content'

    # Collect missing sections
    missing = []  # (file_path, section_index, section_number)
    for f in glob.glob(os.path.join(content_dir, '**/*.json'), recursive=True):
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            for i, sec in enumerate(data.get('sections', [])):
                if not sec.get('text', '').strip():
                    missing.append((f, i, sec.get('number', '')))
        except:
            pass

    log.info(f'Florida: {len(missing)} sections missing text')
    if not missing:
        return

    # Group by chapter
    chapters_with_missing = {}
    for fp, idx, num in missing:
        parts = num.split('.')
        if parts:
            ch = parts[0]
            if ch not in chapters_with_missing:
                chapters_with_missing[ch] = set()
            chapters_with_missing[ch].add(num)

    log.info(f'Florida: {len(chapters_with_missing)} chapters with missing sections')

    # Phase 1: Discover section URLs
    log.info('Phase 1: Discovering section URLs from chapter/part pages...')
    section_urls = discover_section_urls(chapters_with_missing)
    log.info(f'Discovered URLs for {len(section_urls)} of {len(missing)} sections')

    if not section_urls:
        return

    # Phase 2: Fetch section text
    log.info('Phase 2: Fetching section text...')
    results = {}
    total = len(section_urls)
    filled = 0
    start = time.time()

    items = list(section_urls.items())
    BATCH = 500
    for batch_start in range(0, len(items), BATCH):
        batch = items[batch_start:batch_start + BATCH]
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {}
            for sec_num, url in batch:
                def fetch(url=url, sec_num=sec_num):
                    time.sleep(DELAY / WORKERS)
                    raw = curl_get(url)
                    text = extract_content(raw) if raw and len(raw) > 500 else None
                    return sec_num, text, url
                futures[pool.submit(fetch)] = sec_num

            for fut in as_completed(futures):
                sec_num, text, url = fut.result()
                if text:
                    results[sec_num] = (text, url)
                    filled += 1

        done = batch_start + len(batch)
        elapsed = time.time() - start
        rate = done / (elapsed / 3600) if elapsed > 0 else 0
        log.info(f'Florida: {done}/{total} fetched, {filled} with text ({rate:.0f}/hr)')

    # Phase 3: Write results
    by_file = {}
    for fpath, idx, num in missing:
        if fpath not in by_file:
            by_file[fpath] = []
        by_file[fpath].append((idx, num))

    files_updated = 0
    sections_updated = 0
    for fpath, sections in by_file.items():
        try:
            with open(fpath, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            modified = False
            for idx, num in sections:
                if num in results and idx < len(data.get('sections', [])):
                    text, url = results[num]
                    data['sections'][idx]['text'] = text
                    data['sections'][idx]['source_url'] = url
                    modified = True
                    sections_updated += 1
            if modified:
                with open(fpath, 'w', encoding='utf-8') as fh:
                    json.dump(data, fh, indent=2, ensure_ascii=False)
                files_updated += 1
        except Exception as e:
            log.error(f'Error writing {fpath}: {e}')

    log.info(f'Florida: done. {sections_updated}/{len(missing)} sections updated, {files_updated} files')


if __name__ == '__main__':
    main()

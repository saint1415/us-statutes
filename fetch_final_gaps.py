#!/usr/bin/env python3
"""Fetch remaining missing sections with fixed HTML parser.

Handles: WV (case fix), SC Title 62 (article URLs), DE, FL, PA.
Also supports --rescrape mode to re-fetch truncated sections for FL/PA.
"""
import json, os, glob, re, subprocess, time, logging, sys, html
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

WORKERS = 4
DELAY = 1.5
BASE = 'https://law.justia.com/codes'


def extract_content(raw_html):
    """Extract text from Justia codes-content div using div-depth tracking."""
    match = re.search(r'id="codes-content"[^>]*>(.*)', raw_html, re.DOTALL)
    if not match:
        return None
    content = match.group(1)
    # Track div depth to find matching </div>
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
    # Strip HTML tags
    text = re.sub(r'<[^>]+>', ' ', content)
    # Decode HTML entities
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text if len(text) > 20 else None


# Florida chapter -> title mapping
FL_TITLE_MAP = None
def get_fl_title_map():
    global FL_TITLE_MAP
    if FL_TITLE_MAP is None:
        with open('florida_title_mapping.json', 'r') as f:
            FL_TITLE_MAP = json.load(f)
    return FL_TITLE_MAP


def section_to_url(state, section_number, file_path=''):
    """Build Justia URL for a section."""
    if state == 'west-virginia':
        parts = section_number.split('-')
        if len(parts) >= 2:
            chapter = parts[0]
            article = parts[1]
            slug = section_number.lower()
            return f'{BASE}/{state}/chapter-{chapter}/article-{article.lower()}/section-{slug}/'
        return None

    elif state == 'south-carolina':
        parts = section_number.split('-')
        if len(parts) >= 2:
            title = parts[0]
            chapter = parts[1]
            # Title 62 uses article-based URLs
            if title == '62':
                return f'{BASE}/{state}/title-62/article-{chapter}/section-{section_number}/'
            return f'{BASE}/{state}/title-{title}/chapter-{chapter}/section-{section_number}/'
        return None

    elif state == 'delaware':
        # Extract title and chapter from file path
        m = re.search(r'title-(\d+)', file_path.replace(os.sep, '/'))
        cm = re.search(r'chapter-(\w+)', file_path.replace(os.sep, '/'))
        if m and cm:
            title = m.group(1)
            chapter = cm.group(1)
            slug = section_number.lower().replace('.', '-')
            return f'{BASE}/{state}/title-{title}/chapter-{chapter}/section-{slug}/'
        return None

    elif state == 'florida':
        parts = section_number.split('.')
        if len(parts) >= 1:
            chapter = parts[0]
            title_map = get_fl_title_map()
            title_slug = title_map.get(chapter)
            if not title_slug:
                return None
            slug = section_number.replace('.', '-')
            return f'{BASE}/{state}/{title_slug}/chapter-{chapter}/section-{slug}/'
        return None

    elif state == 'pennsylvania':
        m = re.search(r'title-(?:title-)?(\d+)', file_path.replace(os.sep, '/'))
        if not m:
            return None
        title = str(int(m.group(1)))
        try:
            sec_num = int(re.match(r'^(\d+)', section_number).group(1))
            chapter = sec_num // 100
        except (ValueError, AttributeError):
            return None
        if chapter == 0:
            return None
        return f'{BASE}/{state}/title-{title}/chapter-{chapter}/section-{section_number}/'

    return None


def fetch_text(state, section_number, file_path=''):
    """Fetch section text from Justia with fixed parser."""
    url = section_to_url(state, section_number, file_path)
    if not url:
        return section_number, None, None
    try:
        result = subprocess.run([
            'curl', '-s', '-L', '--max-time', '15',
            '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            '-H', 'Accept: text/html,application/xhtml+xml',
            '-H', 'Accept-Language: en-US,en;q=0.9',
            url
        ], capture_output=True, text=True, timeout=20, encoding='utf-8', errors='replace')
        raw = result.stdout
        if not raw or len(raw) < 500:
            return section_number, None, url
        text = extract_content(raw)
        return section_number, text, url
    except Exception:
        return section_number, None, url


def collect_sections(state, rescrape=False, truncation_threshold=50):
    """Collect sections to fetch.

    If rescrape=True, also include sections with text shorter than truncation_threshold
    (likely truncated by the old parser bug).
    """
    content_dir = f'data/states/{state}/content'
    targets = []  # (file_path, section_index, section_number)
    for f in glob.glob(os.path.join(content_dir, '**/*.json'), recursive=True):
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            for i, sec in enumerate(data.get('sections', [])):
                text = sec.get('text', '').strip()
                num = sec.get('number', '')
                if not text:
                    targets.append((f, i, num))
                elif rescrape and len(text) < truncation_threshold:
                    targets.append((f, i, num))
        except:
            pass
    return targets


def process_state(state, rescrape=False):
    """Fetch missing (and optionally truncated) sections for a state."""
    targets = collect_sections(state, rescrape=rescrape)

    # Filter to those with valid URLs
    valid = [(fp, idx, num) for fp, idx, num in targets if section_to_url(state, num, fp)]
    skipped = len(targets) - len(valid)

    log.info(f'{state}: {len(targets)} targets ({len(valid)} with URLs, {skipped} skipped)')
    if not valid:
        return

    # Sample URLs
    for fp, idx, num in valid[:3]:
        log.info(f'  Sample: {num} -> {section_to_url(state, num, fp)}')

    # Fetch in batches
    BATCH = 500
    filled = 0
    total = len(valid)
    start = time.time()
    results = {}  # section_number -> (text, url)

    for batch_start in range(0, total, BATCH):
        batch = valid[batch_start:batch_start + BATCH]
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {}
            for fpath, idx, num in batch:
                fut = pool.submit(fetch_text, state, num, fpath)
                futures[fut] = num
                time.sleep(DELAY / WORKERS)

            for fut in as_completed(futures):
                num, text, url = fut.result()
                if text:
                    results[num] = (text, url)
                    filled += 1

        done = batch_start + len(batch)
        elapsed = time.time() - start
        rate = done / (elapsed / 3600) if elapsed > 0 else 0
        log.info(f'{state}: {done}/{total} done, {filled} with text ({rate:.0f}/hr)')

    # Write results back
    by_file = {}
    for fpath, idx, num in valid:
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

    log.info(f'{state}: done. {sections_updated}/{total} sections updated, {files_updated} files')


if __name__ == '__main__':
    rescrape = '--rescrape' in sys.argv
    states = [s for s in sys.argv[1:] if not s.startswith('--')]
    if not states:
        states = ['west-virginia', 'south-carolina', 'delaware']
    for state in states:
        process_state(state, rescrape=rescrape)

#!/usr/bin/env python3
"""Fetch missing South Carolina section text from Justia."""
import json, os, glob, re, subprocess, time, logging, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

STATE = 'south-carolina'
BASE = f'https://law.justia.com/codes/{STATE}'
CONTENT_DIR = f'data/states/{STATE}/content'
WORKERS = 4
DELAY = 1.5

class ContentParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_content = False
        self.depth = 0
        self.text = []
    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if d.get('id') == 'codes-content':
            self.in_content = True
            self.depth = 1
        elif self.in_content:
            self.depth += 1
    def handle_endtag(self, tag):
        if self.in_content:
            self.depth -= 1
            if self.depth <= 0:
                self.in_content = False
    def handle_data(self, data):
        if self.in_content:
            self.text.append(data.strip())

def section_to_url(section_number):
    """Convert SC section number like 1-1-10 to Justia URL."""
    parts = section_number.split('-')
    if len(parts) >= 2:
        title = parts[0]
        chapter = parts[1]
    else:
        return None
    return f'{BASE}/title-{title}/chapter-{chapter}/section-{section_number}/'

def fetch_text(section_number):
    """Fetch section text from Justia."""
    url = section_to_url(section_number)
    if not url:
        return section_number, None
    time.sleep(DELAY)
    try:
        result = subprocess.run([
            'curl', '-s', '-L', '--max-time', '15',
            '-H', 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            '-H', 'Accept: text/html,application/xhtml+xml',
            '-H', 'Accept-Language: en-US,en;q=0.9',
            url
        ], capture_output=True, text=True, timeout=20, encoding='utf-8', errors='replace')
        html = result.stdout
        if not html or len(html) < 500:
            return section_number, None
        parser = ContentParser()
        parser.feed(html)
        text = ' '.join(t for t in parser.text if t).strip()
        text = re.sub(r'\s+', ' ', text)
        if len(text) > 20:
            return section_number, text
        return section_number, None
    except Exception:
        return section_number, None

def main():
    # Collect all sections without text
    missing = []  # (file_path, section_index, section_number)
    for f in glob.glob(os.path.join(CONTENT_DIR, '**/*.json'), recursive=True):
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            for i, sec in enumerate(data.get('sections', [])):
                if not sec.get('text', '').strip():
                    missing.append((f, i, sec.get('number', '')))
        except:
            pass

    log.info(f'{STATE}: {len(missing)} sections missing text')
    if not missing:
        return

    # Process in batches for progress logging
    BATCH = 500
    filled = 0
    failed = 0
    total = len(missing)
    start = time.time()
    results = {}  # section_number -> text

    for batch_start in range(0, total, BATCH):
        batch = missing[batch_start:batch_start + BATCH]
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(fetch_text, num): (fpath, idx, num) for fpath, idx, num in batch}
            for fut in as_completed(futures):
                num, text = fut.result()
                if text:
                    results[num] = text
                    filled += 1
                else:
                    failed += 1

        done = batch_start + len(batch)
        elapsed = time.time() - start
        rate = done / (elapsed / 3600) if elapsed > 0 else 0
        log.info(f'{STATE}: {done}/{total} done, {filled} with text ({rate:.0f}/hr)')

    # Write results back to files
    by_file = {}
    for fpath, idx, num in missing:
        if fpath not in by_file:
            by_file[fpath] = []
        by_file[fpath].append((idx, num))

    files_updated = 0
    for fpath, sections in by_file.items():
        try:
            with open(fpath, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            modified = False
            for idx, num in sections:
                if num in results:
                    data['sections'][idx]['text'] = results[num]
                    data['sections'][idx]['source_url'] = section_to_url(num)
                    modified = True
            if modified:
                with open(fpath, 'w', encoding='utf-8') as fh:
                    json.dump(data, fh, indent=2, ensure_ascii=False)
                files_updated += 1
        except Exception as e:
            log.error(f'Error writing {fpath}: {e}')

    log.info(f'{STATE}: done. {filled}/{total} sections filled, {files_updated} files updated')

if __name__ == '__main__':
    main()

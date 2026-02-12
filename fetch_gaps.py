#!/usr/bin/env python3
"""Fetch missing section text for multiple gap states from Justia."""
import json, os, glob, re, subprocess, time, logging, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from html.parser import HTMLParser

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

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

# Florida chapter -> title mapping
FL_TITLE_MAP = None
def get_fl_title_map():
    global FL_TITLE_MAP
    if FL_TITLE_MAP is None:
        with open('florida_title_mapping.json', 'r') as f:
            FL_TITLE_MAP = json.load(f)
    return FL_TITLE_MAP

def section_to_url(state, section_number, file_path=''):
    """Convert section number to Justia URL based on state."""
    base = f'https://law.justia.com/codes/{state}'

    if state == 'wisconsin':
        # Section 3.004 -> chapter 3
        parts = section_number.split('.')
        if len(parts) >= 1:
            chapter = parts[0]
            slug = section_number.replace('.', '-')
            return f'{base}/chapter-{chapter}/section-{slug}/'
        return None

    elif state == 'florida':
        # Section 1.01 -> need title mapping, chapter = first part
        parts = section_number.split('.')
        if len(parts) >= 1:
            chapter = parts[0]
            title_map = get_fl_title_map()
            title_slug = title_map.get(chapter)
            if not title_slug:
                return None
            slug = section_number.replace('.', '-')
            return f'{base}/{title_slug}/chapter-{chapter}/section-{slug}/'
        return None

    elif state == 'pennsylvania':
        # Extract title from file path (e.g., title-1/chapter-xxx.json)
        m = re.search(r'title-(\d+)', file_path.replace(os.sep, '/'))
        if m:
            title = m.group(1)
            # Extract chapter from file path
            cm = re.search(r'chapter-(\d+)', file_path.replace(os.sep, '/'))
            chapter = cm.group(1) if cm else ''
            return f'{base}/title-{title}/chapter-{chapter}/section-{section_number}/'
        return None

    elif state == 'west-virginia':
        # Section 3-4A-11 -> chapter=3, article=4A
        parts = section_number.split('-')
        if len(parts) >= 2:
            chapter = parts[0]
            article = parts[1]
            return f'{base}/chapter-{chapter}/article-{article}/section-{section_number}/'
        return None

    elif state == 'north-carolina':
        # Section 105-339.1 -> chapter 105
        parts = section_number.split('-')
        if len(parts) >= 1:
            chapter = parts[0]
            slug = section_number.replace('.', '-')
            return f'{base}/chapter-{chapter}/section-{slug}/'
        return None

    return None

def fetch_text(state, section_number, file_path=''):
    """Fetch section text from Justia."""
    url = section_to_url(state, section_number, file_path)
    if not url:
        return section_number, None, url
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
            return section_number, None, url
        parser = ContentParser()
        parser.feed(html)
        text = ' '.join(t for t in parser.text if t).strip()
        text = re.sub(r'\s+', ' ', text)
        if len(text) > 20:
            return section_number, text, url
        return section_number, None, url
    except Exception:
        return section_number, None, url

def process_state(state):
    """Process all missing sections for a state."""
    content_dir = f'data/states/{state}/content'

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

    log.info(f'{state}: {len(missing)} sections missing text')
    if not missing:
        return

    # Filter to sections that can generate URLs
    valid = [(fp, idx, num) for fp, idx, num in missing if section_to_url(state, num, fp)]
    log.info(f'{state}: {len(valid)} sections with valid URLs (skipping {len(missing) - len(valid)})')
    if not valid:
        return

    # Fetch
    filled = 0
    total = len(valid)
    start = time.time()
    results = {}  # section_number -> (text, url)

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {}
        for fpath, idx, num in valid:
            fut = pool.submit(fetch_text, state, num, fpath)
            futures[fut] = num
            time.sleep(DELAY / WORKERS)

        for i, fut in enumerate(as_completed(futures), 1):
            num, text, url = fut.result()
            if text:
                results[num] = (text, url)
                filled += 1
            if i % 100 == 0:
                elapsed = time.time() - start
                rate = i / (elapsed / 3600)
                log.info(f'{state}: {i}/{total} done, {filled} with text ({rate:.0f}/hr)')

    # Write results back
    by_file = {}
    for fpath, idx, num in valid:
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
                    text, url = results[num]
                    data['sections'][idx]['text'] = text
                    data['sections'][idx]['source_url'] = url
                    modified = True
            if modified:
                with open(fpath, 'w', encoding='utf-8') as fh:
                    json.dump(data, fh, indent=2, ensure_ascii=False)
                files_updated += 1
        except Exception as e:
            log.error(f'Error writing {fpath}: {e}')

    log.info(f'{state}: done. {filled}/{total} sections filled, {files_updated} files updated')

if __name__ == '__main__':
    states = sys.argv[1:]
    if not states:
        states = ['wisconsin', 'florida', 'pennsylvania', 'west-virginia', 'north-carolina']
    for state in states:
        process_state(state)

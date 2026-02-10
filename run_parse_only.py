"""Parse-only: re-parse cached data without fetching. Much faster."""
import sys
import time
import json
import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

import yaml

from pipeline.ingestion.base import StructureLevel
from pipeline.normalization.normalizer import write_state

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data" / "states"
CACHE_DIR = ROOT_DIR / "cache"

with open("pipeline/config/sources.yaml") as f:
    sources = yaml.safe_load(f)["jurisdictions"]
with open("pipeline/config/state_metadata.yaml") as f:
    meta_list = yaml.safe_load(f)["states"]
    metadata = {s["slug"]: s for s in meta_list}


def get_ingestor(slug):
    from pipeline.ingestion.dc_council import DCCouncilIngestor
    from pipeline.ingestion.state_provided import StateProvidedIngestor
    from pipeline.ingestion.official_website import OfficialWebsiteIngestor

    source_config = sources[slug]
    source_type = source_config["source_type"]
    state_meta = metadata.get(slug, {})
    config = {
        **source_config,
        "state_abbr": state_meta.get("abbr", ""),
        "state_name": state_meta.get("name", ""),
        "structure": [
            StructureLevel(level=s["level"], label=s["label"])
            for s in source_config.get("structure", [])
        ],
    }
    cls_map = {
        "dc_council": DCCouncilIngestor,
        "state_provided": StateProvidedIngestor,
        "official_website": OfficialWebsiteIngestor,
    }
    cls = cls_map.get(source_type)
    if cls is None:
        raise ValueError(f"Unknown source type: {source_type}")
    return cls(state=slug, config=config, cache_dir=CACHE_DIR)


def parse_state(slug):
    """Parse cached data without fetching."""
    start = time.time()
    try:
        ingestor = get_ingestor(slug)
        source_type = sources[slug]["source_type"]

        # Find cached raw data
        if source_type == "official_website":
            raw_path = CACHE_DIR / "raw" / slug / "official"
        elif source_type == "state_provided":
            raw_path = CACHE_DIR / "raw" / slug / "html"
            if not raw_path.exists():
                raw_path = CACHE_DIR / "raw" / slug
        elif source_type == "dc_council":
            raw_path = CACHE_DIR / "raw" / slug
        else:
            return slug, 0, 0, "unknown source_type"

        if not raw_path.exists():
            return slug, 0, 0, "no cache"

        # Count files
        file_count = sum(1 for _ in raw_path.rglob("*") if _.is_file())
        if file_count == 0:
            return slug, 0, 0, "empty cache"

        state_code = ingestor.parse(raw_path)
        state_data_dir = DATA_DIR / slug
        write_state(state_code, state_data_dir, None)
        elapsed = time.time() - start

        mf = state_data_dir / "manifest.json"
        if mf.exists():
            with open(mf) as f:
                m = json.load(f)
            return slug, m["stats"]["sections"], elapsed, None
        return slug, 0, elapsed, "no manifest"
    except Exception as e:
        elapsed = time.time() - start
        return slug, 0, elapsed, str(e)[:120]


# Parse all states with cached data
states_to_parse = []
for slug in sorted(sources.keys()):
    source_type = sources[slug]["source_type"]
    if source_type == "official_website":
        cache_path = CACHE_DIR / "raw" / slug / "official"
    elif source_type == "state_provided":
        cache_path = CACHE_DIR / "raw" / slug / "html"
        if not cache_path.exists():
            cache_path = CACHE_DIR / "raw" / slug
    elif source_type == "dc_council":
        cache_path = CACHE_DIR / "raw" / slug
    else:
        continue
    if cache_path.exists():
        file_count = sum(1 for _ in cache_path.rglob("*") if _.is_file())
        if file_count > 0:
            states_to_parse.append((slug, file_count))

print(f"Parsing {len(states_to_parse)} states with cached data...")
sys.stdout.flush()

with ThreadPoolExecutor(max_workers=6) as executor:
    futures = {executor.submit(parse_state, s): s for s, _ in states_to_parse}
    done = 0
    for future in as_completed(futures):
        done += 1
        slug, sections, elapsed, error = future.result()
        status = f"{sections:>6} sections" if not error else f"ERROR: {error[:80]}"
        print(f"  [{done}/{len(states_to_parse)}] {slug:<20} {status} ({elapsed:.1f}s)")
        sys.stdout.flush()

print("\n=== Final Summary ===")
total = 0
states_with_data = 0
for state in sorted(os.listdir("data/states")):
    mf = f"data/states/{state}/manifest.json"
    if os.path.exists(mf):
        with open(mf) as f:
            m = json.load(f)
        s = m["stats"]["sections"]
        total += s
        if s > 0:
            states_with_data += 1
            print(f"  {state}: {s:,}")
print(f"\n{states_with_data} states with data, {total:,} total sections")

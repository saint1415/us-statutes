"""Fetch + ingest states that need new downloads."""
import sys
import time
import json
import os
import logging
from pathlib import Path

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
    from pipeline.ingestion.official_website import OfficialWebsiteIngestor
    source_config = sources[slug]
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
    return OfficialWebsiteIngestor(state=slug, config=config, cache_dir=CACHE_DIR)


def run_state(slug):
    start = time.time()
    try:
        ingestor = get_ingestor(slug)
        state_code = ingestor.ingest()
        write_state(state_code, DATA_DIR / slug, None)
        elapsed = time.time() - start
        mf = DATA_DIR / slug / "manifest.json"
        if mf.exists():
            with open(mf) as f:
                m = json.load(f)
            return slug, m["stats"]["sections"], elapsed, None
        return slug, 0, elapsed, "no manifest"
    except Exception as e:
        elapsed = time.time() - start
        return slug, 0, elapsed, str(e)[:120]


# States that need fetching (no cached data or stale cache)
states = sys.argv[1:] if len(sys.argv) > 1 else [
    "ohio", "montana", "illinois", "minnesota", "maine", "oregon",
    "wisconsin", "west-virginia", "texas", "louisiana", "indiana",
    "alabama", "florida", "wyoming",
    "arizona", "hawaii", "iowa", "kansas", "michigan",
    "new-york", "utah", "maryland", "arkansas", "new-mexico",
    "georgia", "mississippi", "tennessee", "new-jersey",
    "puerto-rico", "guam", "us-virgin-islands",
]

for slug in states:
    if slug not in sources:
        print(f"  {slug:<20} SKIPPED (not in sources.yaml)")
        continue
    print(f"  Starting {slug}...", end="", flush=True)
    slug, sections, elapsed, error = run_state(slug)
    status = f"{sections:>6} sections" if not error else f"ERROR: {error[:80]}"
    print(f"\r  {slug:<20} {status} ({elapsed:.0f}s)")
    sys.stdout.flush()

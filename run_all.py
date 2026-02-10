"""Batch ingest all states with progress tracking."""
import sys
import time
import json
import os
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.WARNING, format="%(message)s", stream=sys.stderr)

import yaml

from pipeline.ingestion.base import StructureLevel
from pipeline.normalization.normalizer import write_state

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data" / "states"
CACHE_DIR = ROOT_DIR / "cache"

with open("pipeline/config/sources.yaml") as f:
    sources = yaml.safe_load(f)["jurisdictions"]

with open("pipeline/config/state_metadata.yaml") as f:
    meta_data = yaml.safe_load(f)["states"]
    metadata = {s["slug"]: s for s in meta_data}


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


def run_state(slug):
    start = time.time()
    try:
        ingestor = get_ingestor(slug)
        state_code = ingestor.ingest()
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


# Skip states that are already done well
skip = {"district-of-columbia", "pennsylvania", "south-carolina", "delaware", "massachusetts", "connecticut", "nebraska"}

# Quick wins: have lots of cached data, just need re-parsing
quick_wins = ["north-carolina", "new-hampshire", "rhode-island", "nevada", "kentucky", "idaho", "north-dakota"]
# Need re-fetch with fixed handlers
refetch = ["ohio", "montana", "illinois", "minnesota", "maine", "oregon", "wisconsin",
           "west-virginia", "texas", "louisiana", "indiana", "alabama", "florida", "wyoming"]
# Fresh fetch needed
fresh = ["arizona", "hawaii", "iowa", "kansas", "michigan", "new-york", "utah", "maryland", "arkansas", "new-mexico"]
# Re-parse with updated patterns
reparse = ["alaska", "missouri", "oklahoma", "washington", "vermont", "virginia", "colorado", "california", "south-dakota"]
# Tricky sites
tricky = ["georgia", "mississippi", "tennessee", "new-jersey"]
# Territories
territories = ["puerto-rico", "guam", "us-virgin-islands"]

ordered = quick_wins + reparse + refetch + fresh + tricky + territories
ordered = [s for s in ordered if s not in skip and s in sources]

print(f"Running {len(ordered)} states...")
sys.stdout.flush()

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {executor.submit(run_state, s): s for s in ordered}
    done = 0
    for future in as_completed(futures):
        done += 1
        slug, sections, elapsed, error = future.result()
        status = f"{sections:>6} sections" if not error else f"ERROR: {error[:80]}"
        print(f"  [{done}/{len(ordered)}] {slug:<20} {status} ({elapsed:.0f}s)")
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

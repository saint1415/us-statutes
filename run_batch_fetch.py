"""Batch fetch + parse for states that need data."""
import sys
import time
import json
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(levelname)s %(name)s: %(message)s")

import yaml
from pipeline.ingestion.base import StructureLevel
from pipeline.normalization.normalizer import write_state

ROOT_DIR = Path(__file__).resolve().parent
os.chdir(ROOT_DIR)
DATA_DIR = ROOT_DIR / "data" / "states"
CACHE_DIR = ROOT_DIR / "cache"

with open("pipeline/config/sources.yaml") as f:
    sources = yaml.safe_load(f)["jurisdictions"]
with open("pipeline/config/state_metadata.yaml") as f:
    meta_list = yaml.safe_load(f)["states"]
    metadata = {s["slug"]: s for s in meta_list}


def ingest_state(slug):
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
        return slug, 0, "unknown source type"

    ingestor = cls(state=slug, config=config, cache_dir=CACHE_DIR)
    start = time.time()
    try:
        raw_path = ingestor.fetch()
        state_code = ingestor.parse(raw_path)
        state_data_dir = DATA_DIR / slug
        write_state(state_code, state_data_dir, None)
        elapsed = time.time() - start
        mf = state_data_dir / "manifest.json"
        if mf.exists():
            with open(mf) as f:
                m = json.load(f)
            return slug, m["stats"]["sections"], elapsed
        return slug, 0, elapsed
    except Exception as e:
        elapsed = time.time() - start
        return slug, 0, f"ERROR ({elapsed:.0f}s): {str(e)[:200]}"


# Get states from command line, or use the default list
if len(sys.argv) > 1:
    states = sys.argv[1:]
else:
    # States that need data (0 sections or missing)
    states = [
        # Justia-backed (LexisNexis states)
        "georgia", "mississippi", "tennessee", "arkansas", "new-jersey",
        "new-mexico", "maryland",
        # States with handlers that need fresh fetch
        "florida", "texas", "alabama", "indiana", "louisiana",
        "west-virginia", "wyoming", "utah", "new-york", "illinois",
        # States needing deeper fetch
        "vermont", "virginia", "south-dakota", "montana",
        "washington", "california", "colorado",
        # Remaining states
        "hawaii", "iowa", "kansas", "michigan",
        # Territories
        "guam", "puerto-rico", "us-virgin-islands",
    ]

print(f"Fetching {len(states)} states...", flush=True)
for i, slug in enumerate(states):
    print(f"  [{i+1}/{len(states)}] Starting {slug}...", end=" ", flush=True)
    result = ingest_state(slug)
    if isinstance(result[2], str) and "ERROR" in str(result[2]):
        print(f"FAILED: {result[2]}", flush=True)
    else:
        print(f"{result[1]:,} sections ({result[2]:.0f}s)", flush=True)

# Final summary
print("\n=== Final Summary ===", flush=True)
total = 0
count = 0
for state in sorted(os.listdir("data/states")):
    mf = f"data/states/{state}/manifest.json"
    if os.path.exists(mf):
        with open(mf) as f:
            m = json.load(f)
        s = m["stats"]["sections"]
        total += s
        if s > 0:
            count += 1
            print(f"  {state}: {s:,}", flush=True)
print(f"\n{count} states with data, {total:,} total sections", flush=True)

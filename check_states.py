"""Quick status check of all states."""
import os, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

total = 0
states_with_data = 0
states_zero = []
states_low = []
for state in sorted(os.listdir("data/states")):
    mf = f"data/states/{state}/manifest.json"
    if os.path.exists(mf):
        with open(mf) as f:
            m = json.load(f)
        s = m["stats"]["sections"]
        total += s
        if s > 0:
            states_with_data += 1
            marker = " <<<LOW" if s < 200 else ""
            print(f"  {state:<25} {s:>8,}{marker}")
            if s < 200:
                states_low.append(state)
        else:
            states_zero.append(state)

print(f"\n{states_with_data} states with data, {total:,} total sections")
print(f"\nStates with 0 sections: {', '.join(states_zero)}")
print(f"\nStates with <200 sections: {', '.join(states_low)}")

# Check cache status
import yaml
with open("pipeline/config/sources.yaml") as f:
    sources = yaml.safe_load(f)["jurisdictions"]

all_states = set(sources.keys())
have_data = set(state for state in os.listdir("data/states") if os.path.exists(f"data/states/{state}/manifest.json"))
missing = all_states - have_data
if missing:
    print(f"\nNot yet attempted: {', '.join(sorted(missing))}")

# Check cache sizes
print("\nCache status:")
for state in sorted(all_states):
    cache_path = f"cache/raw/{state}/official"
    if os.path.exists(cache_path):
        count = sum(1 for _ in os.scandir(cache_path) if _.is_file())
        # Also check subdirs
        for d in os.scandir(cache_path):
            if d.is_dir():
                count += sum(1 for _ in os.scandir(d.path) if _.is_file())
        if count > 0:
            print(f"  {state:<25} {count:>6} cached files")

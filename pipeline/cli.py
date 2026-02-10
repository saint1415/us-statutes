"""CLI entry point for the US Statutes ingestion pipeline."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import click
import yaml

from pipeline.ingestion.base import BaseIngestor, StructureLevel
from pipeline.normalization.normalizer import write_state, build_manifest
from pipeline.utils.cache import HttpCache
from pipeline.utils.rate_limiter import RateLimiter

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "pipeline" / "config"
DATA_DIR = ROOT_DIR / "data" / "states"
CACHE_DIR = ROOT_DIR / "cache"


def _load_sources() -> dict:
    with open(CONFIG_DIR / "sources.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["jurisdictions"]


def _load_metadata() -> dict:
    with open(CONFIG_DIR / "state_metadata.yaml", encoding="utf-8") as f:
        data = yaml.safe_load(f)["states"]
    return {s["slug"]: s for s in data}


def _get_ingestor(state_slug: str, source_config: dict, metadata: dict) -> BaseIngestor:
    """Instantiate the appropriate ingestor for a state."""
    from pipeline.ingestion.dc_council import DCCouncilIngestor
    from pipeline.ingestion.justia import JustiaIngestor
    from pipeline.ingestion.law_resource_org import LawResourceOrgIngestor
    from pipeline.ingestion.internet_archive import InternetArchiveIngestor
    from pipeline.ingestion.state_provided import StateProvidedIngestor
    from pipeline.ingestion.official_website import OfficialWebsiteIngestor

    source_type = source_config["source_type"]
    state_meta = metadata.get(state_slug, {})

    config = {
        **source_config,
        "state_abbr": state_meta.get("abbr", ""),
        "state_name": state_meta.get("name", ""),
        "structure": [
            StructureLevel(level=s["level"], label=s["label"])
            for s in source_config.get("structure", [])
        ],
    }

    ingestor_map = {
        "dc_council": DCCouncilIngestor,
        "justia": JustiaIngestor,
        "law_resource_org": LawResourceOrgIngestor,
        "internet_archive": InternetArchiveIngestor,
        "official_website": OfficialWebsiteIngestor,
        "state_provided": StateProvidedIngestor,
    }

    cls = ingestor_map.get(source_type)
    if cls is None:
        raise ValueError(f"Unknown source type: {source_type}")

    return cls(state=state_slug, config=config, cache_dir=CACHE_DIR)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose: bool):
    """US Statutes ingestion pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )


@cli.command()
@click.option("--state", "-s", help="State slug (e.g., 'alabama', 'district-of-columbia')")
@click.option("--source-type", "-t", help="Source type: justia, law_resource_org, internet_archive, state_provided, dc_council")
@click.option("--all", "ingest_all", is_flag=True, help="Ingest all states")
@click.option("--data-dir", type=click.Path(), default=None, help="Output data directory")
@click.option("--content-dir", type=click.Path(), default=None, help="Output content directory (for data branch)")
def ingest(state: str | None, source_type: str | None, ingest_all: bool, data_dir: str | None, content_dir: str | None):
    """Ingest statute data for one or more states."""
    sources = _load_sources()
    metadata = _load_metadata()

    out_data = Path(data_dir) if data_dir else DATA_DIR
    out_content = Path(content_dir) if content_dir else None

    # Determine which states to process
    if ingest_all:
        states_to_process = list(sources.keys())
    elif source_type:
        states_to_process = [
            slug for slug, cfg in sources.items()
            if cfg["source_type"] == source_type
        ]
    elif state:
        if state not in sources:
            click.echo(f"Error: Unknown state '{state}'. Available: {', '.join(sorted(sources.keys()))}", err=True)
            sys.exit(1)
        states_to_process = [state]
    else:
        click.echo("Error: Specify --state, --source-type, or --all", err=True)
        sys.exit(1)

    click.echo(f"Processing {len(states_to_process)} state(s)...")

    successes = []
    failures = []

    for slug in states_to_process:
        try:
            click.echo(f"\n{'='*60}")
            click.echo(f"Ingesting: {slug}")
            click.echo(f"{'='*60}")

            ingestor = _get_ingestor(slug, sources[slug], metadata)
            state_code = ingestor.ingest()

            state_data_dir = out_data / slug
            state_content_dir = (out_content / slug / "content") if out_content else None
            write_state(state_code, state_data_dir, state_content_dir)

            successes.append(slug)
            click.echo(f"  OK: {slug}")

        except Exception as e:
            failures.append((slug, str(e)))
            logging.getLogger(__name__).exception("Failed to ingest %s", slug)
            click.echo(f"  FAIL: {slug}: {e}", err=True)

    # Update master index
    _update_master_index(out_data)

    click.echo(f"\n{'='*60}")
    click.echo(f"Done: {len(successes)} succeeded, {len(failures)} failed")
    if failures:
        for slug, err in failures:
            click.echo(f"  FAIL: {slug}: {err}", err=True)


@cli.command("build-index")
@click.option("--output", "-o", default="site/pagefind", help="Output directory for search index")
@click.option("--data-dir", type=click.Path(), default=None, help="Data directory with content files")
def build_index(output: str, data_dir: str | None):
    """Build Pagefind search index from ingested content."""
    from pipeline.search.build_index import build_search_index

    content_root = Path(data_dir) if data_dir else DATA_DIR
    output_path = Path(output)
    build_search_index(content_root, output_path)


def _update_master_index(data_dir: Path) -> None:
    """Rebuild data/index.json from all state manifests."""
    index = {"states": []}

    for state_dir in sorted(data_dir.iterdir()):
        manifest_path = state_dir / "manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            index["states"].append({
                "state": manifest["state"],
                "state_abbr": manifest["state_abbr"],
                "code_name": manifest["code_name"],
                "source": manifest["source"],
                "last_updated": manifest["last_updated"],
                "stats": manifest["stats"],
            })

    index_path = data_dir.parent / "index.json"
    index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")
    logging.getLogger(__name__).info("Updated master index: %d states", len(index["states"]))


if __name__ == "__main__":
    cli()

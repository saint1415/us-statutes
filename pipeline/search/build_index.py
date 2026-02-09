#!/usr/bin/env python3
"""Build Pagefind search index from statute JSON content.

Pagefind works by indexing HTML files. This script generates lightweight HTML
stubs from content JSON files, runs Pagefind over them, then cleans up.

Usage:
    python build_index.py --content-dir data/states --output-dir site/pagefind --site-dir site

Can also be called as:
    python -m pipeline.search.build_index ...
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import sys
import tempfile
from html import escape
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_html_stubs(content_dir: Path, stubs_dir: Path) -> int:
    """Generate HTML files from content JSON for Pagefind indexing.

    Args:
        content_dir: Root directory containing state subdirectories with content.
        stubs_dir: Output directory for HTML stubs.

    Returns:
        Number of HTML files generated.
    """
    count = 0

    for state_dir in sorted(content_dir.iterdir()):
        if not state_dir.is_dir():
            continue

        state = state_dir.name

        # Load manifest for metadata
        manifest_path = state_dir / "manifest.json"
        manifest = {}
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        state_name = manifest.get("code_name", state.replace("-", " ").title())
        state_abbr = manifest.get("state_abbr", "")

        # Find content JSON files
        content_path = state_dir / "content"
        if not content_path.exists():
            continue

        for json_file in content_path.rglob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                sections = data.get("sections", [])
                chapter_path = data.get("path", "")

                for section in sections:
                    html = _section_to_html(
                        section, state, state_name, state_abbr, chapter_path
                    )
                    # Write HTML stub
                    section_id = section.get("id", "unknown")
                    stub_path = stubs_dir / state / chapter_path / f"{section_id}.html"
                    stub_path.parent.mkdir(parents=True, exist_ok=True)
                    stub_path.write_text(html, encoding="utf-8")
                    count += 1

            except Exception as e:
                logger.warning("Failed to process %s: %s", json_file, e)

    return count


def _section_to_html(
    section: dict, state: str, state_name: str, state_abbr: str, chapter_path: str
) -> str:
    """Convert a section dict to an HTML document for Pagefind indexing."""
    number = escape(section.get("number", ""))
    heading = escape(section.get("heading", ""))
    text = escape(section.get("text", ""))
    section_id = escape(section.get("id", ""))

    # The URL that search results should link to
    url = f"/#/states/{state}/{chapter_path}#{section_id}"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>\u00a7 {number} {heading} - {escape(state_name)}</title>
</head>
<body>
<article data-pagefind-body data-pagefind-filter-state="{escape(state)}" data-pagefind-meta-url="{escape(url)}">
<h1>\u00a7 {number} {heading}</h1>
<p data-pagefind-meta="state:{escape(state)}, abbr:{escape(state_abbr)}">{escape(state_name)}</p>
<div>{text}</div>
</article>
</body>
</html>"""


def run_pagefind(stubs_dir: Path, output_dir: Path, site_dir: Path) -> bool:
    """Run Pagefind CLI to build the search index.

    Args:
        stubs_dir: Directory containing HTML stubs.
        output_dir: Where to write the Pagefind index.
        site_dir: The site root (for relative path computation).

    Returns:
        True if Pagefind ran successfully.
    """
    # Pagefind needs to run on the combined site directory
    # Copy stubs into a temp site dir, run pagefind, copy index back
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        result = subprocess.run(
            [
                "pagefind",
                "--site", str(stubs_dir),
                "--output-path", str(output_dir),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            logger.error("Pagefind failed: %s", result.stderr)
            return False

        logger.info("Pagefind output: %s", result.stdout)
        return True

    except FileNotFoundError:
        logger.error(
            "Pagefind CLI not found. Install with: npm install -g pagefind"
        )
        return False
    except subprocess.TimeoutExpired:
        logger.error("Pagefind timed out after 300s")
        return False


def build_search_index(content_dir: Path, output_dir: Path, site_dir: Path | None = None) -> None:
    """Full search index build pipeline.

    Args:
        content_dir: Directory containing state subdirectories with content JSON.
        output_dir: Where to write the Pagefind index files.
        site_dir: The site root directory.
    """
    if site_dir is None:
        site_dir = output_dir.parent

    with tempfile.TemporaryDirectory() as tmpdir:
        stubs_dir = Path(tmpdir) / "stubs"
        stubs_dir.mkdir()

        logger.info("Generating HTML stubs from %s", content_dir)
        count = generate_html_stubs(content_dir, stubs_dir)
        logger.info("Generated %d HTML stubs", count)

        if count == 0:
            logger.warning("No content found to index")
            return

        logger.info("Running Pagefind...")
        success = run_pagefind(stubs_dir, output_dir, site_dir)
        if success:
            logger.info("Search index built at %s", output_dir)
        else:
            logger.error("Failed to build search index")


def main():
    parser = argparse.ArgumentParser(description="Build Pagefind search index")
    parser.add_argument("--content-dir", required=True, help="Content directory")
    parser.add_argument("--output-dir", required=True, help="Output directory for Pagefind index")
    parser.add_argument("--site-dir", default=None, help="Site root directory")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    site_dir = Path(args.site_dir) if args.site_dir else None
    build_search_index(Path(args.content_dir), Path(args.output_dir), site_dir)


if __name__ == "__main__":
    main()

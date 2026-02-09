# US Statutes

Browseable, searchable collection of all 50 US state statutes + DC, served as a static site on GitHub Pages.

## Overview

This project ingests state statute text from multiple sources (bulk downloads, official XML, and web scraping), normalizes it into a consistent JSON format, and presents it through a lightweight static frontend with full-text search powered by [Pagefind](https://pagefind.app).

## Architecture

- **Pipeline** (`pipeline/`): Python ingestion pipeline that fetches, parses, and normalizes statute data
- **Data** (`data/`): JSON manifests, table-of-contents, and content files organized by state
- **Site** (`site/`): Static HTML/JS/CSS frontend with hash-based routing
- **CI/CD** (`.github/workflows/`): Automated ingestion and deployment via GitHub Actions

### Data Branch Strategy

Full statute text (~1-2 GB) lives on the `data` branch. The `main` branch holds code, frontend, and lightweight TOC/manifest files. The deploy workflow merges both branches for GitHub Pages.

## Quick Start

### Browse the site

Visit: https://saint1415.github.io/us-statutes/

### Run the pipeline locally

```bash
# Install dependencies
pip install -r pipeline/requirements.txt

# Ingest a single state (DC is the fastest)
python -m pipeline.cli ingest --state district-of-columbia

# Ingest all bulk-download states
python -m pipeline.cli ingest --source-type bulk

# Ingest all Justia-scraped states
python -m pipeline.cli ingest --source-type justia

# Build search index
python -m pipeline.cli build-index --output site/pagefind
```

### Serve locally

```bash
python -m http.server -d site 8000
# Open http://localhost:8000
```

## Data Sources

| Source | States | Method |
|--------|--------|--------|
| Justia | ~31 states | Web scraping |
| Law.Resource.Org | AR, CA, CO, DE, GA, ID, MS, OR, TN | XML bulk download |
| Internet Archive | KY, NC, VT, VA, WY | Archive.org download |
| State-provided | CT, FL, MD, NE | Official bulk data |
| DC Council | DC | GitHub XML |

## License

CC0 / Public Domain. The laws of the United States are public domain.

"""Normalize a StateCode into manifest.json, toc.json, and content chapter JSON files."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pipeline.ingestion.base import StateCode

logger = logging.getLogger(__name__)


def _slugify(text: str) -> str:
    """Create a URL-safe slug from text."""
    import re

    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


def build_manifest(state_code: StateCode) -> dict:
    """Build manifest.json content from a StateCode."""
    total_chapters = sum(len(t.chapters) for t in state_code.titles)
    total_sections = sum(
        len(c.sections) for t in state_code.titles for c in t.chapters
    )

    return {
        "state": state_code.state,
        "state_abbr": state_code.state_abbr,
        "code_name": state_code.code_name,
        "source": state_code.source,
        "source_url": state_code.source_url,
        "last_updated": state_code.last_updated.isoformat() if state_code.last_updated else None,
        "year": state_code.year,
        "structure": [
            {"level": s.level, "label": s.label} for s in state_code.structure
        ],
        "stats": {
            "titles": len(state_code.titles),
            "chapters": total_chapters,
            "sections": total_sections,
        },
    }


def build_toc(state_code: StateCode) -> dict:
    """Build toc.json content from a StateCode (no full text)."""
    children = []
    for title in state_code.titles:
        title_node = {
            "id": title.id,
            "number": title.number,
            "heading": title.heading,
            "children": [],
        }
        for chapter in title.chapters:
            chapter_node = {
                "id": chapter.id,
                "number": chapter.number,
                "heading": chapter.heading,
                "section_count": len(chapter.sections),
                "children": [
                    {
                        "id": section.id,
                        "number": section.number,
                        "heading": section.heading,
                    }
                    for section in chapter.sections
                ],
            }
            title_node["children"].append(chapter_node)
        children.append(title_node)

    return {"state": state_code.state, "children": children}


def build_content_chapters(state_code: StateCode) -> list[tuple[str, dict]]:
    """Build content chapter JSON files.

    Returns:
        List of (relative_path, content_dict) tuples.
        e.g., ("title-1/chapter-1.json", {...})
    """
    chapters = []
    for title in state_code.titles:
        for chapter in title.chapters:
            path = f"{title.id}/{chapter.id}.json"
            content = {
                "state": state_code.state,
                "path": f"{title.id}/{chapter.id}",
                "sections": [
                    {
                        "id": section.id,
                        "number": section.number,
                        "heading": section.heading,
                        "text": section.text,
                        "history": section.history,
                        "source_url": section.source_url,
                    }
                    for section in chapter.sections
                ],
            }
            chapters.append((path, content))
    return chapters


def write_state(state_code: StateCode, data_dir: Path, content_dir: Path | None = None) -> None:
    """Write all normalized output files for a state.

    Args:
        state_code: Parsed state code data.
        data_dir: Root data directory (e.g., data/states/<state>/).
                  manifest.json and toc.json are written here.
        content_dir: Directory for content chapter files (e.g., data/states/<state>/content/).
                     If None, defaults to data_dir / "content".
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    if content_dir is None:
        content_dir = data_dir / "content"
    content_dir.mkdir(parents=True, exist_ok=True)

    # Write manifest
    manifest = build_manifest(state_code)
    manifest_path = data_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Wrote %s", manifest_path)

    # Write TOC
    toc = build_toc(state_code)
    toc_path = data_dir / "toc.json"
    toc_path.write_text(json.dumps(toc, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("Wrote %s", toc_path)

    # Write content chapters
    chapters = build_content_chapters(state_code)
    for rel_path, content in chapters:
        chapter_path = content_dir / rel_path
        chapter_path.parent.mkdir(parents=True, exist_ok=True)
        chapter_path.write_text(json.dumps(content, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info(
        "Wrote %d content chapter files to %s",
        len(chapters),
        content_dir,
    )

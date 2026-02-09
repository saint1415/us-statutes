"""Text cleaning utilities for statute normalization."""

import html
import re


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<p\b[^>]*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return text


def normalize_whitespace(text: str) -> str:
    """Collapse runs of whitespace while preserving paragraph breaks."""
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_section_number(number: str) -> str:
    """Normalize a section number (strip '§' prefix, extra whitespace)."""
    number = number.replace("§", "").replace("\u00a7", "")
    number = re.sub(r"\s+", " ", number).strip()
    return number


def clean_text(text: str) -> str:
    """Full cleaning pipeline: strip HTML, normalize whitespace."""
    text = strip_html(text)
    text = normalize_whitespace(text)
    return text

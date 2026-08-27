"""Splits fetched docs into Chunk rows with heading-path metadata.

Deliberately boring: no tokenizer dependency. Splitting is heading-aware
(so `section` is a real breadcrumb) then a plain character sliding window
within each section. `len(text) // 4` is documented as a rough token
approximation, not a real count.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

_MD_HEADING_RE = re.compile(r"^(#{1,4})\s+(.*)$")
_HTML_HEADING_TAGS = ("h1", "h2", "h3", "h4")


@dataclass(frozen=True, slots=True)
class Chunk:
    chunk_id: str
    doc_id: str
    project: str
    version: str
    section: str
    source_path: str
    chunk_index: int
    text: str


def doc_id_for(project: str, version: str, source_path: str) -> str:
    key = f"{project}:{version}:{source_path}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


def _chunk_id(doc_id: str, chunk_index: int) -> str:
    key = f"{doc_id}:{chunk_index}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


@dataclass
class _Section:
    heading_path: str
    text: str


def _split_markdown_sections(text: str) -> list[_Section]:
    """Group lines under the nearest heading, joining nested headings with ' > '."""
    stack: list[str] = []
    sections: list[_Section] = []
    body_lines: list[str] = []

    def flush() -> None:
        body = "\n".join(body_lines).strip()
        if body:
            sections.append(_Section(heading_path=" > ".join(stack) or "(intro)", text=body))
        body_lines.clear()

    for line in text.splitlines():
        match = _MD_HEADING_RE.match(line)
        if match is None:
            body_lines.append(line)
            continue
        flush()
        level, title = len(match.group(1)), match.group(2).strip()
        del stack[level - 1 :]
        stack.append(title)
    flush()
    return sections


def _split_html_sections(html: str) -> list[_Section]:
    soup = BeautifulSoup(html, "html.parser")
    stack: list[str] = []
    sections: list[_Section] = []

    body = soup.body or soup
    body_parts: list[str] = []

    def flush() -> None:
        text = " ".join(body_parts).strip()
        if text:
            sections.append(_Section(heading_path=" > ".join(stack) or "(intro)", text=text))
        body_parts.clear()

    for node in body.find_all(True):
        if node.name in _HTML_HEADING_TAGS:
            flush()
            level = int(node.name[1])
            title = node.get_text(strip=True)
            del stack[level - 1 :]
            stack.append(title)
        elif node.name in ("p", "li", "pre", "code", "td"):
            text = node.get_text(strip=True)
            if text:
                body_parts.append(text)
    flush()
    return sections


def _window(text: str, max_chars: int, overlap_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    windows = []
    start = 0
    step = max_chars - overlap_chars
    while start < len(text):
        windows.append(text[start : start + max_chars])
        start += step
    return windows


def _chunks_from_sections(
    sections: list[_Section],
    *,
    project: str,
    version: str,
    source_path: str,
    max_chars: int,
    overlap_chars: int,
) -> list[Chunk]:
    doc_id = doc_id_for(project, version, source_path)
    chunks: list[Chunk] = []
    index = 0
    for section in sections:
        for window_text in _window(section.text, max_chars, overlap_chars):
            chunks.append(
                Chunk(
                    chunk_id=_chunk_id(doc_id, index),
                    doc_id=doc_id,
                    project=project,
                    version=version,
                    section=section.heading_path,
                    source_path=source_path,
                    chunk_index=index,
                    text=window_text,
                )
            )
            index += 1
    return chunks


def chunk_markdown(
    text: str,
    *,
    project: str,
    version: str,
    source_path: str,
    max_chars: int = 1600,
    overlap_chars: int = 200,
) -> list[Chunk]:
    sections = _split_markdown_sections(text)
    return _chunks_from_sections(
        sections,
        project=project,
        version=version,
        source_path=source_path,
        max_chars=max_chars,
        overlap_chars=overlap_chars,
    )


def chunk_html(
    html: str,
    *,
    project: str,
    version: str,
    source_path: str,
    max_chars: int = 1600,
    overlap_chars: int = 200,
) -> list[Chunk]:
    sections = _split_html_sections(html)
    return _chunks_from_sections(
        sections,
        project=project,
        version=version,
        source_path=source_path,
        max_chars=max_chars,
        overlap_chars=overlap_chars,
    )

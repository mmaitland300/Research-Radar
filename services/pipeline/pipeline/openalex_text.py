from __future__ import annotations

from html import unescape
from typing import Any, Mapping


_MOJIBAKE_MARKERS = (
    "Ã",
    "Â",
    "â€",
    "ðŸ",
)


def _normalize_whitespace(s: str) -> str:
    return " ".join(str(s).split())


def _iterative_html_unescape(text: str, *, max_passes: int = 3) -> str:
    current = text
    for _ in range(max_passes):
        decoded = unescape(current)
        if decoded == current:
            break
        current = decoded
    return current


def _repair_mojibake(text: str) -> str:
    if not any(marker in text for marker in _MOJIBAKE_MARKERS):
        return text
    for codec in ("cp1252", "latin-1"):
        try:
            repaired = text.encode(codec).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if repaired != text:
            return repaired
    return text


def clean_openalex_text(value: str | None) -> str:
    if value is None:
        return ""
    text = _normalize_whitespace(str(value))
    if not text:
        return ""
    text = _repair_mojibake(text)
    text = _iterative_html_unescape(text)
    text = _repair_mojibake(text)
    text = _normalize_whitespace(text)
    return text


def reconstruct_abstract_from_inverted_index(index: Mapping[str, Any]) -> str:
    """Rebuild abstract plain text from OpenAlex `abstract_inverted_index` (word -> positions)."""
    positions: list[tuple[int, str]] = []
    for word, idxs in index.items():
        if not isinstance(word, str):
            continue
        if not isinstance(idxs, list):
            continue
        for pos in idxs:
            if isinstance(pos, int):
                positions.append((pos, clean_openalex_text(word)))
    if not positions:
        return ""
    positions.sort(key=lambda x: x[0])
    return clean_openalex_text(" ".join(w for _, w in positions))


def abstract_plain_text(work: Mapping[str, Any]) -> str:
    """Prefer inline abstract fields; otherwise decode `abstract_inverted_index`."""
    direct = work.get("abstract") or work.get("abstract_text")
    if isinstance(direct, str) and direct.strip():
        return clean_openalex_text(direct)
    inverted = work.get("abstract_inverted_index")
    if isinstance(inverted, Mapping):
        return reconstruct_abstract_from_inverted_index(inverted)
    return ""

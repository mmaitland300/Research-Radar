from __future__ import annotations

from typing import Any, Mapping


def _normalize_whitespace(s: str) -> str:
    return " ".join(str(s).split())


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
                positions.append((pos, word))
    if not positions:
        return ""
    positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in positions)


def abstract_plain_text(work: Mapping[str, Any]) -> str:
    """Prefer inline abstract fields; otherwise decode `abstract_inverted_index`."""
    direct = work.get("abstract") or work.get("abstract_text")
    if isinstance(direct, str) and direct.strip():
        return direct
    inverted = work.get("abstract_inverted_index")
    if isinstance(inverted, Mapping):
        return reconstruct_abstract_from_inverted_index(inverted)
    return ""

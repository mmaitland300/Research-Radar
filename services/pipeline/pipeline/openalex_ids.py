"""OpenAlex identifier helpers that do not depend on database modules."""

from __future__ import annotations

import re

_W_TOKEN = re.compile(r"(W\d+)", re.IGNORECASE)


def normalize_w_token(value: str | None) -> str | None:
    if not value:
        return None
    m = _W_TOKEN.search(str(value).strip())
    if not m:
        return None
    return m.group(1).upper()


__all__ = ["normalize_w_token"]

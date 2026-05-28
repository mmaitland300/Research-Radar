"""Hash helpers for committed audit artifacts.

Some audit JSON/Markdown artifacts are committed as text. On Windows worktrees
with core.autocrlf=true, Git can materialize those files with CRLF while CI sees
LF. These helpers keep provenance checks strict while allowing that exact text
line-ending difference.
"""

from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file_variants_for_text_artifact(path: Path) -> set[str]:
    raw = Path(path).read_bytes()
    variants = {hashlib.sha256(raw).hexdigest()}
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return variants

    lf_text = text.replace("\r\n", "\n").replace("\r", "\n")
    variants.add(hashlib.sha256(lf_text.encode("utf-8")).hexdigest())
    variants.add(hashlib.sha256(lf_text.replace("\n", "\r\n").encode("utf-8")).hexdigest())
    return variants


def recorded_sha256_matches_text_artifact(path: Path, recorded_sha256: str) -> bool:
    return str(recorded_sha256).strip().lower() in sha256_file_variants_for_text_artifact(path)

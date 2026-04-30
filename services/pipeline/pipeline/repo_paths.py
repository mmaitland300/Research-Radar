"""Repository-root path helpers for portable provenance in committed artifacts."""

from __future__ import annotations

from pathlib import Path


def default_repo_root() -> Path:
    """Research Radar repo root (parent of ``services``)."""
    return Path(__file__).resolve().parents[3]


def portable_repo_path(path: Path, *, repo_root: Path | None = None) -> str:
    """Return a repo-relative path when ``path`` resolves under ``repo_root``, else ``resolve().as_posix()``."""
    root = (repo_root if repo_root is not None else default_repo_root()).resolve()
    resolved = path.resolve()
    try:
        return resolved.relative_to(root).as_posix()
    except ValueError:
        return resolved.as_posix()

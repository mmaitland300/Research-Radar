"""Helpers for CLI parser source-shape tests."""

from __future__ import annotations

from pathlib import Path


def read_cli_parser_source(package_root: Path) -> str:
    parser_files = (
        "core_parsers.py",
        "review_parsers.py",
        "bridge_parsers.py",
        "corpus_parsers.py",
        "ml_label_parsers.py",
        "ml_text_parsers.py",
        "ml_hybrid_parsers.py",
        "ml_fresh_parsers.py",
        "ml_shadow_parsers.py",
        "ml_bridge_parsers.py",
        "ml_review_parsers.py",
    )
    cli_app = package_root / "pipeline" / "cli_app"
    return "\n".join((cli_app / name).read_text(encoding="utf-8") for name in parser_files)

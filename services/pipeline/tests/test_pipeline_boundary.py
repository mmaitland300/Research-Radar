from __future__ import annotations

from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def test_top_level_cli_dispatch_uses_legacy_ml_boundary() -> None:
    dispatch_source = (PACKAGE_ROOT / "pipeline" / "cli_app" / "dispatch.py").read_text(
        encoding="utf-8"
    )

    assert "handle_ml_legacy_commands" in dispatch_source
    assert "pipeline.cli_app.ml_legacy_dispatch" in dispatch_source
    assert "pipeline.cli_app.ml_bridge_dispatch" not in dispatch_source
    assert "pipeline.cli_app.ml_fresh_dispatch" not in dispatch_source
    assert "pipeline.cli_app.ml_hybrid_dispatch" not in dispatch_source
    assert "pipeline.cli_app.ml_label_dispatch" not in dispatch_source
    assert "pipeline.cli_app.ml_review_dispatch" not in dispatch_source
    assert "pipeline.cli_app.ml_shadow_dispatch" not in dispatch_source
    assert "pipeline.cli_app.ml_text_dispatch" not in dispatch_source


def test_legacy_ml_dispatch_keeps_existing_handler_order() -> None:
    import pipeline.cli_app.ml_legacy_dispatch as legacy_dispatch

    handler_names = [
        handler.__name__ for handler in legacy_dispatch._ML_LEGACY_HANDLERS
    ]

    assert handler_names == [
        "handle_ml_review_commands",
        "handle_ml_bridge_commands",
        "handle_ml_text_commands",
        "handle_ml_hybrid_commands",
        "handle_ml_shadow_commands",
        "handle_ml_fresh_commands",
        "handle_ml_label_commands",
    ]

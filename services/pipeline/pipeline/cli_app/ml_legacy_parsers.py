from __future__ import annotations

from pipeline.cli_app.ml_bridge_parsers import register_ml_bridge_parsers
from pipeline.cli_app.ml_fresh_parsers import register_ml_fresh_parsers
from pipeline.cli_app.ml_hybrid_parsers import register_ml_hybrid_parsers
from pipeline.cli_app.ml_label_parsers import register_ml_label_parsers
from pipeline.cli_app.ml_review_parsers import register_ml_review_parsers
from pipeline.cli_app.ml_shadow_parsers import register_ml_shadow_parsers
from pipeline.cli_app.ml_text_parsers import register_ml_text_parsers


def register_ml_legacy_parsers(subparsers) -> None:
    register_ml_label_parsers(subparsers)
    register_ml_text_parsers(subparsers)
    register_ml_hybrid_parsers(subparsers)
    register_ml_fresh_parsers(subparsers)
    register_ml_shadow_parsers(subparsers)
    register_ml_bridge_parsers(subparsers)
    register_ml_review_parsers(subparsers)

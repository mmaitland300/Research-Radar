from __future__ import annotations

from pipeline.cli_app.bridge_parsers import (
    register_bridge_analysis_parsers,
    register_bridge_operational_arguments,
    register_bridge_operational_parsers,
)
from pipeline.cli_app.corpus_parsers import register_corpus_parsers
from pipeline.cli_app.review_parsers import register_review_parsers


def register_product_parsers(subparsers) -> None:
    register_review_parsers(subparsers)
    bridge_parsers = register_bridge_operational_parsers(subparsers)
    register_corpus_parsers(subparsers)
    register_bridge_operational_arguments(bridge_parsers)
    register_bridge_analysis_parsers(subparsers, bridge_parsers)

from __future__ import annotations

from pipeline.cli_app.core_parsers import register_core_parsers
from pipeline.cli_app.ml_legacy_parsers import register_ml_legacy_parsers
from pipeline.cli_app.product_parsers import register_product_parsers


def register_parsers(subparsers) -> None:
    register_core_parsers(subparsers)
    register_product_parsers(subparsers)
    register_ml_legacy_parsers(subparsers)

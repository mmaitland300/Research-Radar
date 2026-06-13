from __future__ import annotations

import argparse
import sys

import psycopg

from pipeline.clustering_persistence import count_included_missing_cluster_assignment
from pipeline.cli_app.dispatch import dispatch_command
from pipeline.cli_app.parsers import register_parsers
from pipeline.embedding_persistence import (
    count_included_works_for_snapshot,
    count_missing_embedding_candidates,
)
from pipeline.recommendation_review_worksheet import write_recommendation_review_worksheet
from pipeline.work_text_repair import run_work_text_repair_cli


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research Radar pipeline utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_parsers(subparsers)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dispatch_command(args, parser, psycopg_module=psycopg, compat_module=sys.modules[__name__])


if __name__ == "__main__":
    main()

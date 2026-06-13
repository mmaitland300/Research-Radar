from __future__ import annotations


def register_review_parsers(subparsers) -> None:
    worksheet_parser = subparsers.add_parser(
        "recommendation-review-worksheet",
        help="Write a CSV of top recommendations for one succeeded ranking run (manual review scaffold)",
    )
    worksheet_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Succeeded materialized ranking run id (required; no default or latest resolution)",
    )
    worksheet_parser.add_argument(
        "--family",
        required=True,
        choices=sorted(["emerging", "bridge", "undercited"]),
        help="Recommendation family column to export",
    )
    worksheet_parser.add_argument(
        "--limit",
        type=int,
        required=True,
        help="Max rows (ordered by final_score desc, work_id asc)",
    )
    worksheet_parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path (e.g. docs/audit/manual-review/bridge_run.csv)",
    )
    worksheet_parser.add_argument(
        "--bridge-eligible-only",
        action="store_true",
        help="Bridge family only: filter worksheet rows to bridge_eligible IS TRUE.",
    )
    worksheet_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    summary_parser = subparsers.add_parser(
        "recommendation-review-summary",
        help="Validate and summarize a filled recommendation review worksheet CSV (human labels)",
    )
    summary_parser.add_argument(
        "--input",
        required=True,
        help="Path to a completed worksheet CSV (same columns as recommendation-review-worksheet)",
    )
    summary_parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON summary (e.g. docs/audit/manual-review/bridge_RUN_summary.json)",
    )
    summary_parser.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Write summary with is_complete=false when labels are blank/invalid; default is strict (exit 2)",
    )
    summary_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write a short human-readable Markdown summary",
    )
    rollup_parser = subparsers.add_parser(
        "recommendation-review-rollup",
        help="Combine completed family review summaries into one run-level evaluation artifact",
    )
    rollup_parser.add_argument(
        "--summary",
        action="append",
        required=False,
        help="Path to family summary JSON (repeat for each family)",
    )
    rollup_parser.add_argument(
        "--bridge-summary",
        default=None,
        help="Explicit bridge family summary JSON path",
    )
    rollup_parser.add_argument(
        "--emerging-summary",
        default=None,
        help="Explicit emerging family summary JSON path",
    )
    rollup_parser.add_argument(
        "--undercited-summary",
        default=None,
        help="Explicit undercited family summary JSON path",
    )
    rollup_parser.add_argument(
        "--bridge-diagnostics",
        default=None,
        help="Optional bridge signal diagnostics JSON for eligible-only distinctness evidence",
    )
    rollup_parser.add_argument(
        "--bridge-worksheet",
        default=None,
        help="Optional bridge worksheet CSV used to validate bridge_eligible_only review pool",
    )
    rollup_parser.add_argument(
        "--output",
        required=True,
        help="Path to write rollup JSON (e.g. docs/audit/manual-review/rank_x_rollup.json)",
    )
    rollup_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write rollup Markdown",
    )

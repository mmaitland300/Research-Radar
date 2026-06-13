from __future__ import annotations


def register_ml_fresh_parsers(subparsers) -> None:
    ml_fresh_eval_labeling_plan_hybrid_parser = subparsers.add_parser(
        "ml-fresh-eval-labeling-plan-hybrid",
        help="Write a plan-only fresh eval labeling/remediation artifact for hybrid validation",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--fresh-eval-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh eval labeling plan JSON",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--plan-version",
        default="ml-fresh-eval-labeling-plan-hybrid-v1",
        help="Plan version string to write (default: ml-fresh-eval-labeling-plan-hybrid-v1)",
    )
    ml_fresh_eval_labeling_plan_hybrid_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser = subparsers.add_parser(
        "ml-fresh-eval-labeling-worksheet-hybrid",
        help="Write reviewer-blank CSV plus row_id-keyed context for fresh hybrid eval labeling",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--fresh-eval-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--output",
        required=True,
        help="Path to write reviewer CSV",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--context-output",
        required=True,
        help="Path to write row_id-keyed JSON sidecar",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--rows",
        type=int,
        default=120,
        help="Requested worksheet row count, capped at available unlabeled eligible works (default: 120)",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--seed",
        type=int,
        default=20260519,
        help="Deterministic sampling seed (default: 20260519)",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--worksheet-version",
        default="ml-fresh-eval-labeling-worksheet-hybrid-v1",
        help="Worksheet version string to write (default: ml-fresh-eval-labeling-worksheet-hybrid-v1)",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--review-pool-variant",
        default="ml_fresh_hybrid_eval_v1",
        help="Review pool variant to write into CSV/context rows (default: ml_fresh_hybrid_eval_v1)",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--database-url",
        default=None,
        help="Optional local Postgres URL for read-only row metadata enrichment; hosted production URLs are refused",
    )
    ml_fresh_eval_labeling_worksheet_hybrid_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser = subparsers.add_parser(
        "ml-fresh-eval-positive-topup-worksheet-hybrid",
        help="Write reviewer-blank positive top-up CSV plus context for fresh hybrid eval labeling",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--fresh-eval-surface",
        required=True,
        help="Path to v9 ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v9 JSON",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--output",
        required=True,
        help="Path to write reviewer CSV",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--context-output",
        required=True,
        help="Path to write row_id-keyed JSON sidecar",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--worksheet-version",
        default="ml-fresh-eval-positive-topup-worksheet-hybrid-v1",
        help="Worksheet version string to write (default: ml-fresh-eval-positive-topup-worksheet-hybrid-v1)",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--review-pool-variant",
        default="ml_fresh_hybrid_positive_topup_v1",
        help="Review pool variant to write into CSV/context rows (default: ml_fresh_hybrid_positive_topup_v1)",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--seed",
        type=int,
        default=20260519,
        help="Deterministic row_id seed (default: 20260519)",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--requested-rows",
        type=int,
        default=0,
        help="Requested top-up row count; 0 means all remaining unlabeled eligible works (default: 0)",
    )
    ml_fresh_eval_positive_topup_worksheet_hybrid_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_candidate_source_expansion_plan_parser = subparsers.add_parser(
        "ml-fresh-candidate-source-expansion-plan",
        help="Write a plan-only artifact for expanding fresh product-candidate sources before hybrid validation",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--fresh-product-candidate-ranking-source",
        required=True,
        help="Path to ml-fresh-product-candidate-ranking-source-v1 JSON",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--fresh-eval-labeling-plan",
        required=True,
        help="Path to ml-fresh-eval-labeling-plan-hybrid-v1 JSON",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh candidate source expansion plan JSON",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--plan-version",
        default="ml-fresh-candidate-source-expansion-plan-v1",
        help="Plan version string to write (default: ml-fresh-candidate-source-expansion-plan-v1)",
    )
    ml_fresh_candidate_source_expansion_plan_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser = subparsers.add_parser(
        "ml-fresh-hybrid-corpus-candidate-plan",
        help="Write a dry-run OpenAlex corpus candidate plan for fresh hybrid validation",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--fresh-product-candidate-source-build",
        required=True,
        help="Path to ml-fresh-product-candidate-source-build-v1 JSON",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--fresh-candidate-source-expansion-plan",
        required=True,
        help="Path to ml-fresh-candidate-source-expansion-plan-v1 JSON",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh hybrid corpus candidate plan JSON",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--plan-version",
        default="ml-fresh-hybrid-corpus-candidate-plan-v1",
        help="Plan version string to write (default: ml-fresh-hybrid-corpus-candidate-plan-v1)",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--target-min",
        type=int,
        default=160,
        help="Soft minimum selected candidates (default: 160)",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--target-max",
        type=int,
        default=500,
        help="Hard cap on selected candidates (default: 500)",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--mailto",
        default=None,
        help="Optional OpenAlex User-Agent contact; raw mailto is not stored in artifacts",
    )
    ml_fresh_hybrid_corpus_candidate_plan_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_source_split_tiny_baseline_parser = subparsers.add_parser(
        "ml-source-split-tiny-baseline",
        help="Offline source-split tiny baseline: train emerging rank-shaped labels, test blind rows",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--family",
        required=True,
        choices=["emerging"],
        help="Family context and paper_scores lookup family for blind rows",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--output",
        required=True,
        help="Path to write source-split JSON",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write source-split Markdown",
    )
    ml_source_split_tiny_baseline_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_source_split_error_parser = subparsers.add_parser(
        "ml-source-split-error-analysis",
        help="Offline blind-row error analysis for the frozen source-split tiny baseline",
    )
    ml_source_split_error_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON",
    )
    ml_source_split_error_parser.add_argument(
        "--source-split-artifact",
        required=True,
        help="Path to ml-source-split-tiny-baseline JSON artifact",
    )
    ml_source_split_error_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id, must match source artifact provenance",
    )
    ml_source_split_error_parser.add_argument(
        "--family",
        required=True,
        choices=["emerging"],
        help="Score family, must match source artifact provenance",
    )
    ml_source_split_error_parser.add_argument(
        "--output",
        required=True,
        help="Path to write error-analysis JSON",
    )
    ml_source_split_error_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write error-analysis Markdown",
    )
    ml_source_split_error_parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Max detail rows per list (default 10)",
    )
    ml_source_split_error_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

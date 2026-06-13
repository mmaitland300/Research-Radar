from __future__ import annotations


def register_ml_review_parsers(subparsers) -> None:
    ml_blind_family_context_parser = subparsers.add_parser(
        "ml-blind-family-context-eval",
        help=(
            "Read-only blind-source family-context diagnostic for ml_blind_snapshot_audit rows "
            "(uses worksheet context scores/ranks; no DB, no ranking, no training, not validation)"
        ),
    )
    ml_blind_family_context_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON containing blind worksheet rows with context fields",
    )
    ml_blind_family_context_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="ranking_run_id whose blind-snapshot rows to evaluate (e.g. rank-ee2ba6c816)",
    )
    ml_blind_family_context_parser.add_argument(
        "--output",
        required=True,
        help="Path to write blind family-context diagnostic JSON",
    )
    ml_blind_family_context_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )

    ml_contrastive_ws_parser = subparsers.add_parser(
        "ml-contrastive-review-worksheet",
        help="Read-only CSV/Markdown worksheet to expand contrastive offline audit label coverage for one ranking_run_id",
    )
    ml_contrastive_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v1.json)",
    )
    ml_contrastive_ws_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no implicit latest selection)",
    )
    ml_contrastive_ws_parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path (e.g. docs/audit/manual-review/ml_contrastive_<run>_review.csv)",
    )
    ml_contrastive_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_contrastive_ws_parser.add_argument(
        "--per-family",
        type=int,
        default=15,
        help="Max rows per recommendation family (default 15)",
    )
    ml_contrastive_ws_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ml_gap_ws_parser = subparsers.add_parser(
        "ml-targeted-gap-review-worksheet",
        help="Read-only emerging-family gap worksheet for extra contrastive labels (one ranking_run_id; no training)",
    )
    ml_gap_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v2.json)",
    )
    ml_gap_ws_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no implicit latest selection)",
    )
    ml_gap_ws_parser.add_argument(
        "--family",
        default="emerging",
        choices=["emerging"],
        help="Recommendation family (only emerging is supported)",
    )
    ml_gap_ws_parser.add_argument(
        "--target-gap",
        required=True,
        choices=sorted(["good_or_acceptable", "surprising_or_useful"]),
        help="Primary gap name for provenance / Markdown (good_or_acceptable or surprising_or_useful)",
    )
    ml_gap_ws_parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path",
    )
    ml_gap_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_gap_ws_parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Max worksheet rows (default 25)",
    )
    ml_gap_ws_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ml_blind_ws_parser = subparsers.add_parser(
        "ml-blind-snapshot-review-worksheet",
        help="Read-only deterministic non-rank-driven blind sample worksheet for offline manual labeling",
    )
    ml_blind_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON used to exclude already fully labeled work_ids",
    )
    ml_blind_ws_parser.add_argument(
        "--corpus-snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version (sample pool source)",
    )
    ml_blind_ws_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Explicit embedding artifact version (must match cluster + ranking run)",
    )
    ml_blind_ws_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Explicit succeeded clustering_runs.cluster_version for cluster strata",
    )
    ml_blind_ws_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Ranking run id (provenance + ranking-context columns only; never used as sampling order)",
    )
    ml_blind_ws_parser.add_argument(
        "--rows",
        type=int,
        default=60,
        help="Target worksheet row count (1-500; default 60)",
    )
    ml_blind_ws_parser.add_argument(
        "--seed",
        type=int,
        required=True,
        help="Deterministic sampling seed",
    )
    ml_blind_ws_parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path (e.g. docs/audit/manual-review/ml_blind_snapshot_review_v1.csv)",
    )
    ml_blind_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_blind_ws_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_blind_ws_v2_parser = subparsers.add_parser(
        "ml-blind-snapshot-review-worksheet-v2",
        help="Write reviewer-blind v2 CSV plus ranking-context JSON sidecar (read-only DB; no labels inferred)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON used to exclude already fully labeled work_ids",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--corpus-snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version (sample pool source)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Explicit embedding artifact version (must match cluster + ranking run)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Explicit succeeded clustering_runs.cluster_version for cluster strata",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Ranking run id used only for hidden sidecar context and off-worksheet sampling strata",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--rows",
        type=int,
        default=60,
        help="Target worksheet row count (1-500; default 60)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--seed",
        type=int,
        default=20260512,
        help="Deterministic sampling seed (default 20260512; distinct from v1)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--output",
        required=True,
        help="Reviewer CSV output path (e.g. docs/audit/manual-review/ml_blind_snapshot_review_v2.csv)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--context-output",
        required=True,
        help="Hidden context sidecar JSON path (e.g. docs/audit/manual-review/ml_blind_snapshot_review_v2_context.json)",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_blind_ws_v2_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_hard_negative_ws_parser = subparsers.add_parser(
        "ml-hard-negative-review-worksheet",
        help="Write reviewer-blind hard-negative / near-miss CSV plus hidden sidecar (read-only DB; no training)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON used to conservatively exclude already labeled works",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--corpus-snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version (sample pool source)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Explicit embedding artifact version (must match cluster + ranking run)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Explicit succeeded clustering_runs.cluster_version for cluster strata",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Ranking run id used only for hidden sidecar context and off-worksheet sampling signals",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--rows",
        type=int,
        default=60,
        help="Target worksheet row count (1-500; default 60; emits fewer on credible-pool shortfall)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--seed",
        type=int,
        default=20260513,
        help="Deterministic sampling seed (default 20260513)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--output",
        required=True,
        help="Reviewer CSV output path (e.g. docs/audit/manual-review/ml_hard_negative_review_v1.csv)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--context-output",
        required=True,
        help="Hidden context sidecar JSON path (e.g. docs/audit/manual-review/ml_hard_negative_review_v1_context.json)",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_hard_negative_ws_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_negative_ws_parser = subparsers.add_parser(
        "ml-bridge-negative-mining-worksheet",
        help="Write reviewer-blind bridge negative-mining CSV plus hidden sidecar (read-only DB; no training)",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON used to exclude already labeled bridge works",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--ranking-run-id",
        default="rank-83787b91ef",
        help="Bridge ranking run id to mine (default: rank-83787b91ef)",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--rows",
        type=int,
        default=70,
        help="Target worksheet row count (1-200; default 70)",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--seed",
        type=int,
        default=20260531,
        help="Deterministic sampling seed (default 20260531)",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--output",
        required=True,
        help="Reviewer CSV output path",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--context-output",
        required=True,
        help="Hidden context sidecar JSON path",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_bridge_negative_ws_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_top_ranked_ws_parser = subparsers.add_parser(
        "ml-bridge-top-ranked-validation-worksheet",
        help="Write bridge top-ranked + contrastive validation CSV plus hidden sidecar (read-only DB; no training)",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON used to exclude already-labeled bridge works from contrastive stratum",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--ranking-run-id",
        default="rank-83787b91ef",
        help="Bridge ranking run id (default: rank-83787b91ef)",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top-ranked rows to include (default 20)",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--contrastive-n",
        type=int,
        default=10,
        help="Number of contrastive borderline rows to include (default 10)",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--contrastive-rank-max",
        type=int,
        default=40,
        help="Upper family_rank bound for contrastive stratum (default 40)",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--output",
        required=True,
        help="Reviewer CSV output path",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--context-output",
        required=True,
        help="Hidden context sidecar JSON path",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_bridge_top_ranked_ws_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_external_near_miss_ws_parser = subparsers.add_parser(
        "ml-external-near-miss-review-worksheet",
        help=(
            "Write reviewer-blind external near-miss CSV plus hidden sidecar "
            "(OpenAlex read-only; outside committed snapshot; no training)"
        ),
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v6 JSON used to exclude already labeled or seen works",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--corpus-snapshot-version",
        default="source-snapshot-v2-candidate-plan-20260428",
        help=(
            "Committed source snapshot identity used for outside-217 exclusion "
            "(default: source-snapshot-v2-candidate-plan-20260428)"
        ),
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--source-snapshot-candidate-plan",
        default=None,
        help=(
            "Candidate-plan manifest containing selected_candidates[].openalex_id for the 217-work exclusion set "
            "(default: docs/audit/corpus-v2-candidate-plan-20260428.json)"
        ),
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--rows",
        type=int,
        default=60,
        help="Target worksheet row count (1-500; emits fewer on credible-pool shortfall)",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--seed",
        type=int,
        default=20260514,
        help="Deterministic sampling seed (default 20260514)",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--output",
        required=True,
        help="Reviewer CSV output path (e.g. docs/audit/manual-review/ml_external_near_miss_review_v1.csv)",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--context-output",
        required=True,
        help="Hidden context sidecar JSON path (e.g. docs/audit/manual-review/ml_external_near_miss_review_v1_context.json)",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Companion Markdown path",
    )
    ml_external_near_miss_ws_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent (default: OPENALEX_MAILTO env or local development placeholder)",
    )

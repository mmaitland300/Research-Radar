from __future__ import annotations


def register_ml_bridge_parsers(subparsers) -> None:
    ml_tiny_baseline_rollup_parser = subparsers.add_parser(
        "ml-tiny-baseline-rollup",
        help="Offline emerging rollup: fold robustness + ablations vs heuristic (read-only DB)",
    )
    ml_tiny_baseline_rollup_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON",
    )
    ml_tiny_baseline_rollup_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id",
    )
    ml_tiny_baseline_rollup_parser.add_argument(
        "--family",
        required=True,
        choices=["emerging"],
        help="Only emerging is supported",
    )
    ml_tiny_baseline_rollup_parser.add_argument(
        "--output",
        required=True,
        help="Path to write rollup JSON",
    )
    ml_tiny_baseline_rollup_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write rollup Markdown",
    )
    ml_tiny_baseline_rollup_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_tiny_baseline_disagreement_parser = subparsers.add_parser(
        "ml-tiny-baseline-disagreement",
        help="Offline emerging audit: promote/demote vs final_score using OOF learned_full logits (read-only DB)",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--family",
        required=True,
        choices=["emerging"],
        help="Only emerging is supported",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--target",
        default=None,
        choices=sorted(["good_or_acceptable", "surprising_or_useful"]),
        help="Single manual target for OOF model (omit if --all-targets)",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--all-targets",
        action="store_true",
        help="Run both good_or_acceptable and surprising_or_useful in one artifact",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Max rows to list in top promotions/demotions per target (default 25)",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--output",
        required=True,
        help="Path to write disagreement JSON",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write Markdown summary",
    )
    ml_tiny_baseline_disagreement_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_label_readiness_parser = subparsers.add_parser(
        "ml-label-readiness-matrix",
        help="Read-only label coverage / offline-baseline readiness by ranking_run_id (no training, no ranking)",
    )
    ml_label_readiness_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v1.json)",
    )
    ml_label_readiness_parser.add_argument(
        "--output",
        required=True,
        help="Path to write readiness matrix JSON",
    )
    ml_label_readiness_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_label_readiness_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_recommendable_scorer_parser = subparsers.add_parser(
        "ml-offline-bridge-recommendable-scorer-v1",
        help=(
            "Train/evaluate an offline diagnostic bridge_recommendable scorer on the v12 "
            "bridge negative-mining slice (SELECT-only embeddings, no writes)"
        ),
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v12.json",
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v9.json",
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to second snapshot embeddings provenance JSON",
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--output",
        required=True,
        help="Path to write offline bridge recommendable scorer JSON",
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_recommendable_scorer_parser.add_argument(
        "--random-seed",
        type=int,
        default=20260531,
        help="Random seed for StratifiedKFold and LogisticRegression (default: 20260531)",
    )
    ml_bridge_scorer_v2_parser = subparsers.add_parser(
        "ml-offline-bridge-recommendable-scorer-v2",
        help=(
            "Train/evaluate an offline diagnostic bridge_recommendable scorer on the combined v13 "
            "bridge slice (70 negative-mining + 30 top-ranked, SELECT-only embeddings, no writes)"
        ),
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v13.json",
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v10.json",
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to second snapshot embeddings provenance JSON",
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--output",
        required=True,
        help="Path to write offline bridge recommendable scorer v2 JSON",
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_scorer_v2_parser.add_argument(
        "--random-seed",
        type=int,
        default=20260531,
        help="Random seed for StratifiedKFold and LogisticRegression (default: 20260531)",
    )
    ml_bridge_scorer_v3_parser = subparsers.add_parser(
        "ml-offline-bridge-recommendable-scorer-v3",
        help=(
            "Train/evaluate offline diagnostic bridge_recommendable scorer on v14 three-pool "
            "bridge audit slice (deduped 130 primary, row-level 160 audit, SELECT-only embeddings)"
        ),
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v14.json",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v11.json",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to second snapshot embeddings provenance JSON",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--v2-baseline",
        default=None,
        help="Optional path to docs/audit/ml-offline-bridge-recommendable-scorer-v2.json for drift/regression check",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--output",
        required=True,
        help="Path to write offline bridge recommendable scorer v3 JSON",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_scorer_v3_parser.add_argument(
        "--random-seed",
        type=int,
        default=20260602,
        help="Random seed for StratifiedKFold and LogisticRegression (default: 20260602)",
    )
    ml_bridge_scorer_v3_regularization_parser = subparsers.add_parser(
        "ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity",
        help=(
            "Offline diagnostic regularization sensitivity sweep for bridge_recommendable scorer v3 "
            "(deduped 130 primary slice, SELECT-only embeddings, no writes)"
        ),
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v14.json",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v11.json",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to second snapshot embeddings provenance JSON",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--v3-baseline",
        required=True,
        help="Read-only path to docs/audit/ml-offline-bridge-recommendable-scorer-v3.json",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--v3-baseline-git-ref",
        default=None,
        help="Optional git ref for hashing the committed v3 baseline artifact instead of working-tree bytes",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--v2-baseline",
        default=None,
        help="Optional path to docs/audit/ml-offline-bridge-recommendable-scorer-v2.json for v2 work-id set check",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--output",
        required=True,
        help="Path to write regularization sensitivity JSON",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_scorer_v3_regularization_parser.add_argument(
        "--random-seed",
        type=int,
        default=20260602,
        help="Random seed for StratifiedKFold and LogisticRegression (default: 20260602)",
    )
    ml_bounded_hybrid_bridge_eval_parser = subparsers.add_parser(
        "ml-offline-bounded-hybrid-bridge-eval-v1",
        help=(
            "Evaluate offline bounded bridge hybrids on the v12 bridge negative-mining labeled slice "
            "(file-only, OOF probabilities only, no DB writes)"
        ),
    )
    ml_bounded_hybrid_bridge_eval_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v12.json",
    )
    ml_bounded_hybrid_bridge_eval_parser.add_argument(
        "--bridge-scorer",
        required=True,
        help="Path to docs/audit/ml-offline-bridge-recommendable-scorer-v1.json",
    )
    ml_bounded_hybrid_bridge_eval_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v9.json",
    )
    ml_bounded_hybrid_bridge_eval_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to second snapshot embeddings provenance JSON",
    )
    ml_bounded_hybrid_bridge_eval_parser.add_argument(
        "--output",
        required=True,
        help="Path to write bounded hybrid bridge eval JSON",
    )
    ml_bounded_hybrid_bridge_eval_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )

    ml_bridge_score_hybrid_eval_parser = subparsers.add_parser(
        "ml-offline-bridge-score-hybrid-eval",
        help=(
            "Offline diagnostic: compare v2 ML OOF probabilities with bridge_score from a "
            "clustering-enabled ranking run (SELECT-only DB access, no writes)"
        ),
    )
    ml_bridge_score_hybrid_eval_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v13.json",
    )
    ml_bridge_score_hybrid_eval_parser.add_argument(
        "--v2-scorer",
        required=True,
        help="Path to docs/audit/ml-offline-bridge-recommendable-scorer-v2.json",
    )
    ml_bridge_score_hybrid_eval_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Ranking run ID with bridge_score populated (produced by cluster-works + ranking-run)",
    )
    ml_bridge_score_hybrid_eval_parser.add_argument(
        "--output",
        required=True,
        help="Path to write bridge_score hybrid eval JSON",
    )
    ml_bridge_score_hybrid_eval_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_bridge_score_hybrid_eval_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ml_bridge_hybrid_eval_v3_parser = subparsers.add_parser(
        "ml-offline-bridge-hybrid-eval-v3",
        help=(
            "Offline diagnostic: combine C=0.001 v3 OOF probabilities with bridge_score on "
            "shadow-pilot rows (file-only, no DB writes)"
        ),
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--sensitivity-artifact",
        required=True,
        help="Path to ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json",
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v14.json",
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v11.json",
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json",
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--v2-scorer",
        default=None,
        help="Optional path to v2 baseline scorer JSON for provenance",
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--output",
        required=True,
        help="Path to write hybrid eval v3 JSON",
    )
    ml_bridge_hybrid_eval_v3_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )

    ml_bridge_hybrid_rank_pct_eval_v3_parser = subparsers.add_parser(
        "ml-offline-bridge-hybrid-rank-pct-eval-v3",
        help=(
            "Offline diagnostic: rank-percentile blend of C=0.001 v3 ML + bridge_score on "
            "shadow-pilot rows (528-pool scope when shadow pilot + DB provided)"
        ),
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--sensitivity-artifact",
        required=True,
        help="Path to ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v14.json",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v11.json",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--shadow-pilot-artifact",
        default=None,
        help=(
            "Optional path to ml-bridge-shadow-pilot-v1.json for full 528-candidate "
            "rank-percentile scope"
        ),
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL for full-pool v3 inference (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--linear-hybrid-eval-v3",
        default=None,
        help="Optional path to linear hybrid eval v3 JSON for provenance cross-reference",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--output",
        required=True,
        help="Path to write rank-percentile hybrid eval v3 JSON",
    )
    ml_bridge_hybrid_rank_pct_eval_v3_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )

    ml_bridge_rank_pct_controlled_rollout_eval_parser = subparsers.add_parser(
        "ml-bridge-rank-pct-hybrid-controlled-rollout-eval",
        help=(
            "Offline controlled rollout replay for replacing current Bridge top-20 with "
            "rank-percentile hybrid top-20 (SELECT-only embeddings, no writes)"
        ),
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--shadow-pilot-artifact",
        required=True,
        help="Path to docs/audit/ml-bridge-shadow-pilot-v1.json",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--sensitivity-artifact",
        required=True,
        help="Path to ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--rank-pct-eval-artifact",
        required=True,
        help="Path to docs/audit/ml-offline-bridge-hybrid-rank-pct-eval-v3-v1.json",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v14.json",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v11.json",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL for full-pool v3 inference (default: DATABASE_URL or PG* env)",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--output",
        required=True,
        help="Path to write controlled rollout eval JSON",
    )
    ml_bridge_rank_pct_controlled_rollout_eval_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )

    ml_bridge_rank_pct_serving_plan_parser = subparsers.add_parser(
        "ml-bridge-rank-pct-hybrid-serving-plan",
        help=(
            "File-only Bridge rank-percentile hybrid serving plan from the controlled rollout eval "
            "(no serving/API/web changes)"
        ),
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--controlled-rollout-eval",
        required=True,
        help="Path to docs/audit/ml-bridge-rank-pct-hybrid-controlled-rollout-eval-v1.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--rank-pct-eval-artifact",
        required=True,
        help="Path to docs/audit/ml-offline-bridge-hybrid-rank-pct-eval-v3-v1.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--linear-hybrid-eval-v3",
        required=True,
        help="Path to docs/audit/ml-offline-bridge-hybrid-eval-v3-v1.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--sensitivity-artifact",
        required=True,
        help="Path to ml-offline-bridge-recommendable-scorer-v3-regularization-sensitivity-v1.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to docs/audit/ml-label-dataset-v14.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--readiness-matrix",
        required=True,
        help="Path to docs/audit/ml-label-readiness-matrix-v11.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--embeddings-provenance",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write Bridge serving plan JSON",
    )
    ml_bridge_rank_pct_serving_plan_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )

    ml_bridge_shadow_pilot_parser = subparsers.add_parser(
        "ml-bridge-shadow-pilot",
        help=(
            "Offline bridge shadow pilot: apply frozen v2 ML model to all Bridge candidates, "
            "compute hybrid_bridge_score_50_50, compare top-20 lists, generate worksheet "
            "(SELECT-only DB access, no writes, no serving change)"
        ),
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--v2-scorer",
        required=True,
        help="Path to docs/audit/ml-offline-bridge-recommendable-scorer-v2.json",
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Ranking run ID with bridge_score populated (e.g. rank-5a7efa5ca3)",
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--output",
        required=True,
        help="Path to write shadow pilot JSON artifact",
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown review",
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--worksheet-csv",
        default=None,
        help="Optional path to write blank disagreement worksheet CSV",
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--worksheet-sidecar",
        default=None,
        help="Optional path to write worksheet context sidecar JSON",
    )
    ml_bridge_shadow_pilot_parser.add_argument(
        "--database-url",
        default=None,
        help="Read-only Postgres URL (default: DATABASE_URL or PG* env)",
    )

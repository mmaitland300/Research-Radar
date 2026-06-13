from __future__ import annotations


def register_ml_shadow_parsers(subparsers) -> None:
    ml_shadow_scorer_generalization_second_surface_parser = subparsers.add_parser(
        "ml-shadow-scorer-generalization-second-surface",
        help="Discover/select a distinct second fresh surface for ml-shadow-scorer-v1 generalization",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--online-shadow-policy",
        required=True,
        help="Path to ml-shadow-scorer-v1-online-shadow-policy JSON",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v10 JSON",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--offline-production-candidate-scoring-v3",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v3 JSON",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--first-validated-surface",
        default=None,
        help="Optional ml-fresh-eval-surface-hybrid-v1 JSON for first validated surface overlap IDs",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--database-url",
        default=None,
        help="Optional local Postgres URL for SELECT-only discovery",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--ranking-run-id",
        default=None,
        help="Optional explicit ranking_run_id probe; still validated for distinctness",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--family",
        default="emerging",
        help="Recommendation family to inspect (default: emerging)",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second-surface discovery JSON",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--surface-version",
        default="ml-shadow-scorer-v1-generalization-second-surface-v1",
        help="Surface version string to write (default: ml-shadow-scorer-v1-generalization-second-surface-v1)",
    )
    ml_shadow_scorer_generalization_second_surface_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-surface-labeling-worksheet",
        help="Write reviewer-blank CSV/context for all second-surface confirmatory-eligible works",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v10 JSON",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--offline-production-candidate-scoring-v3",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v3 JSON",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--first-validated-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--fresh-surface-policy",
        default=None,
        help="Optional path to ml-fresh-eval-surface-policy-hybrid-v1 JSON for threshold provenance",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--database-url",
        default=None,
        help="Optional local Postgres URL for SELECT-only worksheet context",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--requested-rows",
        type=int,
        default=0,
        help="Rows to include; 0 means all confirmatory-eligible rows (default: 0)",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--worksheet-version",
        default="ml-shadow-scorer-second-surface-labeling-worksheet-v1",
        help="Worksheet version string to write",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--review-pool-variant",
        default="ml_shadow_scorer_second_surface_generalization_v1",
        help="Review pool variant string to write",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--seed",
        type=int,
        default=20260522,
        help="Deterministic row_id seed (default: 20260522)",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--output",
        required=True,
        help="Path to write reviewer-blank CSV worksheet",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--context-output",
        required=True,
        help="Path to write row context sidecar JSON",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_surface_labeling_worksheet_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-candidate-source-expansion-plan",
        help="Write a plan-only artifact to expand/create a second fresh candidate source for shadow generalization",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--online-shadow-policy",
        required=True,
        help="Path to ml-shadow-scorer-v1-online-shadow-policy JSON",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second candidate-source expansion plan JSON",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--plan-version",
        default="ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1",
        help="Plan version string to write (default: ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1)",
    )
    ml_shadow_scorer_second_candidate_source_expansion_plan_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-surface-learned-probability-coverage-plan",
        help="Write a plan-only artifact for second-surface learned-probability coverage",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v11 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--second-snapshot-embeddings",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--offline-audit-embedding-scorer",
        required=True,
        help="Path to ml-offline-audit-embedding-scorer-v2 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--online-shadow-policy",
        required=True,
        help="Path to ml-shadow-scorer-v1-online-shadow-policy JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write learned-probability coverage plan JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--plan-version",
        default="ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1",
        help=(
            "Plan version string to write "
            "(default: ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1)"
        ),
    )
    ml_shadow_scorer_second_surface_learned_probability_coverage_plan_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-hybrid-candidate-plan",
        help="Write a dry-run OpenAlex candidate acquisition plan for the second fresh shadow-generalization surface",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--second-candidate-source-expansion-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-candidate-source-expansion-plan-v1 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v10 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--offline-production-candidate-scoring-v3",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v3 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--first-validated-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--target-min",
        type=int,
        default=180,
        help="Minimum planned candidate count target (default: 180)",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--target-max",
        type=int,
        default=600,
        help="Maximum planned candidate count target (default: 600)",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--mailto",
        default=None,
        help="Optional OpenAlex User-Agent contact; raw mailto is not stored in artifacts",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second hybrid candidate plan JSON",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--plan-version",
        default="ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1",
        help="Plan version string to write (default: ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1)",
    )
    ml_shadow_scorer_second_hybrid_candidate_plan_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_product_candidate_ranking_source_parser = subparsers.add_parser(
        "ml-fresh-product-candidate-ranking-source",
        help="Freeze a read-only fresh product-candidate ranking source for hybrid validation",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--fresh-eval-labeling-plan",
        required=True,
        help="Path to ml-fresh-eval-labeling-plan-hybrid-v1 JSON",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--family",
        default="emerging",
        help="Product-candidate family to inspect (default: emerging)",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--ranking-run-id",
        default=None,
        help="Optional explicit ranking_run_id to freeze instead of discovering the largest source",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--min-confirmatory-candidate-works",
        type=int,
        default=None,
        help="Minimum fresh confirmatory candidate works (default: policy threshold)",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh product-candidate ranking source JSON",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--source-version",
        default="ml-fresh-product-candidate-ranking-source-v1",
        help="Source version string to write (default: ml-fresh-product-candidate-ranking-source-v1)",
    )
    ml_fresh_product_candidate_ranking_source_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-surface-learned-probability-apply",
        help="Apply frozen audit embedding scorer to second-surface embeddings and write audit-only probabilities",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--learned-probability-coverage-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v11 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--second-snapshot-embeddings",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--offline-audit-embedding-scorer",
        required=True,
        help="Path to ml-offline-audit-embedding-scorer-v2 JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--database-url",
        default=None,
        help="Optional local Postgres URL for SELECT-only probability application",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--ranking-run-id",
        default="rank-83787b91ef",
        help="Ranking run to score (default: rank-83787b91ef)",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--family",
        default="emerging",
        help="Recommendation family to score (default: emerging)",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--corpus-snapshot-version",
        default="source-snapshot-shadow-generalization-v1-20260521",
        help="Corpus snapshot version to read (default: source-snapshot-shadow-generalization-v1-20260521)",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--embedding-version",
        default="shadow-generalization-text-embedding-v1",
        help="Embedding version to read (default: shadow-generalization-text-embedding-v1)",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--output",
        required=True,
        help="Path to write learned-probability application JSON",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--artifact-version",
        default="ml-shadow-scorer-v1-second-surface-learned-probability-v1",
        help="Artifact version string to write (default: ml-shadow-scorer-v1-second-surface-learned-probability-v1)",
    )
    ml_shadow_scorer_second_surface_learned_probability_apply_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-surface-generalization-audit",
        help="Audit ml-shadow-scorer-v1 on the selected second fresh shadow surface",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--learned-probability-artifact",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-surface-learned-probability-v1 JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v11 JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--shadow-scorer-spec",
        required=True,
        help="Path to ml-shadow-scorer-v1-spec JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--online-shadow-policy",
        required=True,
        help="Path to ml-shadow-scorer-v1-online-shadow-policy JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second-surface generalization audit JSON",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--artifact-version",
        default="ml-shadow-scorer-v1-second-surface-generalization-audit-v1",
        help=(
            "Artifact version string to write "
            "(default: ml-shadow-scorer-v1-second-surface-generalization-audit-v1)"
        ),
    )
    ml_shadow_scorer_second_surface_generalization_audit_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser = subparsers.add_parser(
        "ml-shadow-scorer-online-shadow-runtime-disabled",
        help="Record disabled-by-default ml-shadow-scorer-v1 online shadow runtime implementation",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--generalization-audit-gates",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-gates-v1 JSON",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--second-surface-generalization-audit",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-surface-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--online-shadow-policy",
        required=True,
        help="Path to ml-shadow-scorer-v1-online-shadow-policy JSON",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--shadow-scorer-spec",
        required=True,
        help="Path to ml-shadow-scorer-v1-spec JSON",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--output",
        required=True,
        help="Path to write disabled runtime implementation JSON",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--runtime-version",
        default="ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1",
        help="Runtime version string to write (default: ml-shadow-scorer-v1-online-shadow-runtime-disabled-v1)",
    )
    ml_shadow_scorer_online_shadow_runtime_disabled_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-candidate-plan-ingest",
        help="Ingest committed second hybrid candidate plan into a local eval-only source snapshot",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--second-hybrid-candidate-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1 JSON",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--snapshot-version",
        default=None,
        help="Source snapshot version to create (default: source-snapshot-shadow-generalization-v1-YYYYMMDD)",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report only; do not connect or write DB rows",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second candidate plan ingest JSON",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--ingest-version",
        default="ml-shadow-scorer-v1-second-candidate-plan-ingest-v1",
        help="Ingest version string to write (default: ml-shadow-scorer-v1-second-candidate-plan-ingest-v1)",
    )
    ml_shadow_scorer_second_candidate_plan_ingest_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_product_candidate_source_build_parser = subparsers.add_parser(
        "ml-fresh-product-candidate-source-build",
        help="Build a frozen artifact-only fresh product-candidate source for hybrid validation",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--fresh-candidate-source-expansion-plan",
        required=True,
        help="Path to ml-fresh-candidate-source-expansion-plan-v1 JSON",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--family",
        default="emerging",
        help="Product-candidate family to build from (default: emerging)",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--min-confirmatory-eligible-works",
        type=int,
        default=None,
        help="Minimum confirmatory-eligible works (default: policy/expansion-plan threshold)",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--mode",
        choices=["artifact_only_freeze", "eval_db_source_create"],
        default="artifact_only_freeze",
        help="Build mode (default: artifact_only_freeze; eval_db_source_create is reserved in v1)",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--write-eval-db-source",
        action="store_true",
        help="Reserved write path; unsupported in v1 unless a safe eval-only DB writer is reused",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh product-candidate source build JSON",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--build-version",
        default="ml-fresh-product-candidate-source-build-v1",
        help="Build version string to write (default: ml-fresh-product-candidate-source-build-v1)",
    )
    ml_fresh_product_candidate_source_build_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser = subparsers.add_parser(
        "ml-fresh-hybrid-candidate-plan-ingest",
        help="Ingest committed fresh hybrid candidate plan into a local eval-only source snapshot",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--fresh-hybrid-corpus-candidate-plan",
        required=True,
        help="Path to ml-fresh-hybrid-corpus-candidate-plan-v1 JSON",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh hybrid candidate plan ingest JSON",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--snapshot-version",
        default=None,
        help="Source snapshot version to create (default: source-snapshot-fresh-hybrid-v1-YYYYMMDD)",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--ingest-version",
        default="ml-fresh-hybrid-candidate-plan-ingest-v1",
        help="Ingest version string to write (default: ml-fresh-hybrid-candidate-plan-ingest-v1)",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report only; do not connect or write DB rows",
    )
    ml_fresh_hybrid_candidate_plan_ingest_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )

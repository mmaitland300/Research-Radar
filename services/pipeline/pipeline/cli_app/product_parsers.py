from __future__ import annotations


def register_product_parsers(subparsers) -> None:
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

    bridge_readiness_parser = subparsers.add_parser(
        "bridge-experiment-readiness",
        help="Join recommendation review rollup with paper_scores top-k overlap for bridge weight go/no-go",
    )
    bridge_readiness_parser.add_argument(
        "--rollup",
        required=True,
        help="Path to rank-level recommendation review rollup JSON",
    )
    bridge_readiness_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (must match rollup provenance and ranking_runs row)",
    )
    bridge_readiness_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_readiness_parser.add_argument(
        "--output",
        required=True,
        help="Path to write readiness JSON",
    )
    bridge_readiness_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write readiness Markdown",
    )
    bridge_readiness_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    bridge_diag_parser = subparsers.add_parser(
        "bridge-signal-diagnostics",
        help="Read-only bridge signal diagnostics for one explicit ranking_run_id (paper_scores + ranking_runs)",
    )
    bridge_weight_compare_parser = subparsers.add_parser(
        "bridge-weight-experiment-compare",
        help="Read-only baseline vs experiment comparison for a small bridge-weight run",
    )
    bridge_weight_delta_parser = subparsers.add_parser(
        "bridge-weight-experiment-delta-worksheet",
        help="Read-only CSV worksheet for unlabeled moved-in eligible bridge experiment rows",
    )
    bridge_objective_label_coverage_parser = subparsers.add_parser(
        "bridge-objective-label-coverage",
        help="Read-only objective-experiment label coverage correction and one-row review scaffold",
    )
    bridge_objective_labeled_outcome_parser = subparsers.add_parser(
        "bridge-objective-labeled-outcome",
        help="Read-only labeled outcome rollup for objective experiment (baseline + prior delta + one-row label)",
    )
    bridge_weight_delta_summary_parser = subparsers.add_parser(
        "bridge-weight-experiment-delta-summary",
        help="Validate and summarize a completed bridge-weight delta review worksheet CSV",
    )
    bridge_sensitivity_parser = subparsers.add_parser(
        "bridge-eligibility-sensitivity",
        help="Read-only threshold sweep for bridge eligibility distinctness on one explicit ranking_run_id",
    )
    corpus_expansion_parser = subparsers.add_parser(
        "corpus-expansion-preview",
        help="OpenAlex read-only: bucket strategies, sample works, and expansion recommendations (no DB or snapshot)",
    )
    corpus_expansion_parser.add_argument(
        "--output",
        required=True,
        help="JSON output path (e.g. docs/audit/corpus-expansion-preview-YYYYMMDD.json)",
    )
    corpus_expansion_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Markdown output path (e.g. docs/audit/corpus-expansion-preview-YYYYMMDD.md)",
    )
    corpus_expansion_parser.add_argument(
        "--mailto",
        default=None,
        help=(
            "Optional contact for User-Agent metadata (never stored in artifacts). Live mode: also set "
            "OPENALEX_API_KEY (preferred) and/or OPENALEX_MAILTO / this flag."
        ),
    )
    corpus_expansion_parser.add_argument(
        "--per-bucket-sample",
        type=int,
        default=20,
        help="Works per bucket for preview list (10–25; default 20)",
    )
    corpus_expansion_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Offline: no live OpenAlex calls; empty samples and zero counts (tests/CI)",
    )

    corpus_v2_plan_parser = subparsers.add_parser(
        "corpus-v2-candidate-plan",
        help="OpenAlex dry-run: filtered, deduped corpus-v2 candidate plan (no DB, no snapshot, no policy change)",
    )
    corpus_v2_plan_parser.add_argument(
        "--output",
        required=True,
        help="JSON output path (e.g. docs/audit/corpus-v2-candidate-plan-YYYYMMDD.json)",
    )
    corpus_v2_plan_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Markdown output path (e.g. docs/audit/corpus-v2-candidate-plan-YYYYMMDD.md)",
    )
    corpus_v2_plan_parser.add_argument(
        "--mailto",
        default=None,
        help=(
            "Optional contact for User-Agent metadata (never stored in artifacts). Live mode: set OPENALEX_API_KEY "
            "(preferred) and/or pass this or OPENALEX_MAILTO."
        ),
    )
    corpus_v2_plan_parser.add_argument(
        "--per-bucket-limit",
        type=int,
        default=100,
        help="Max raw works fetched per expansion bucket (default 100)",
    )
    corpus_v2_plan_parser.add_argument(
        "--target-min",
        type=int,
        default=200,
        help="Soft minimum selected candidates (caveat if below; default 200)",
    )
    corpus_v2_plan_parser.add_argument(
        "--target-max",
        type=int,
        default=500,
        help="Hard cap on total selected candidates after dedup (default 500)",
    )
    corpus_v2_plan_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Offline: no live OpenAlex calls; empty plan (tests/CI)",
    )

    ismir_preview_parser = subparsers.add_parser(
        "ismir-ingest-preview",
        help="OpenAlex dry-run for the ISMIR tranche: source-id vs search coverage, overlap, attribution, approved ingest set (no DB)",
    )
    ismir_preview_parser.add_argument(
        "--output",
        required=True,
        help="JSON output path (e.g. artifacts/ismir-ingest-preview-YYYYMMDD.json)",
    )
    ismir_preview_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Markdown output path (e.g. artifacts/ismir-ingest-preview-YYYYMMDD.md)",
    )
    ismir_preview_parser.add_argument(
        "--mailto",
        default=None,
        help=(
            "Optional contact for User-Agent metadata (never stored in artifacts). Live mode: set OPENALEX_API_KEY "
            "(preferred) and/or pass this or OPENALEX_MAILTO."
        ),
    )
    ismir_preview_parser.add_argument(
        "--max-works-per-bucket",
        type=int,
        default=400,
        help="Max raw works fetched per bucket (source-id and search; default 400)",
    )
    ismir_preview_parser.add_argument(
        "--target-min",
        type=int,
        default=1,
        help="Soft minimum approved candidates (caveat if below; default 1)",
    )
    ismir_preview_parser.add_argument(
        "--target-max",
        type=int,
        default=500,
        help="Hard cap on approved candidates after dedup (default 500)",
    )
    ismir_preview_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Offline: no live OpenAlex calls; empty plan (tests/CI)",
    )

    corpus_v2_ingest_parser = subparsers.add_parser(
        "corpus-v2-ingest-from-plan",
        help="Postgres import: approved corpus-v2 candidate plan to explicit source snapshot",
    )
    corpus_v2_ingest_parser.add_argument(
        "--candidate-plan",
        required=True,
        help="Approved corpus-v2 candidate-plan JSON path",
    )
    corpus_v2_ingest_parser.add_argument(
        "--snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version to create",
    )
    corpus_v2_ingest_parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON ingest summary",
    )
    corpus_v2_ingest_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write Markdown ingest summary",
    )
    corpus_v2_ingest_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    corpus_v2_hydrate_parser = subparsers.add_parser(
        "corpus-v2-hydrate-openalex",
        help="Hydrate one corpus-v2 snapshot with OpenAlex metadata/text (no embeddings/clustering/ranking)",
    )
    corpus_v2_hydrate_parser.add_argument(
        "--snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version to hydrate",
    )
    corpus_v2_hydrate_parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON hydration summary",
    )
    corpus_v2_hydrate_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write Markdown hydration summary",
    )
    corpus_v2_hydrate_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    corpus_v2_hydrate_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Offline mode for tests/CI: skip live OpenAlex calls and keep works unchanged unless mocked in tests",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser = subparsers.add_parser(
        "ml-fresh-hybrid-snapshot-hydrate",
        help="Hydrate fresh hybrid source snapshot metadata/text with audit provenance",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--fresh-hybrid-candidate-plan-ingest",
        required=True,
        help="Path to ml-fresh-hybrid-candidate-plan-ingest-v1 JSON",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--fresh-hybrid-corpus-candidate-plan",
        required=True,
        help="Path to ml-fresh-hybrid-corpus-candidate-plan-v1 JSON",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--snapshot-version",
        default=None,
        help="Source snapshot version to hydrate (default: value from ingest artifact)",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--mailto",
        default=None,
        help="Optional OpenAlex User-Agent contact; raw mailto is not stored in artifacts",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Deterministic offline OpenAlex hydration for tests/dry runs",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and read before-counts only; no OpenAlex calls and no writes",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh hybrid hydration JSON",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--hydration-version",
        default="ml-fresh-hybrid-snapshot-hydration-v1",
        help="Hydration version string to write (default: ml-fresh-hybrid-snapshot-hydration-v1)",
    )
    ml_fresh_hybrid_snapshot_hydrate_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-snapshot-hydration",
        help="Hydrate the second shadow-generalization source snapshot metadata/text with audit provenance",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--second-candidate-plan-ingest",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-candidate-plan-ingest-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--second-hybrid-candidate-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--snapshot-version",
        default=None,
        help="Source snapshot version to hydrate (default: source-snapshot-shadow-generalization-v1-20260521)",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--mailto",
        default=None,
        help="Optional OpenAlex User-Agent contact; raw mailto is not stored in artifacts",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Deterministic offline OpenAlex hydration for tests/dry runs",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and read before-counts only; no OpenAlex calls and no writes",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second snapshot hydration JSON",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--hydration-version",
        default="ml-shadow-scorer-v1-second-snapshot-hydration-v1",
        help="Hydration version string to write (default: ml-shadow-scorer-v1-second-snapshot-hydration-v1)",
    )
    ml_shadow_scorer_second_snapshot_hydration_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    corpus_v2_embed_parser = subparsers.add_parser(
        "corpus-v2-embed",
        help="Generate versioned title+abstract embeddings for one hydrated corpus-v2 snapshot",
    )
    corpus_v2_embed_parser.add_argument(
        "--snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version to embed",
    )
    corpus_v2_embed_parser.add_argument(
        "--embedding-version",
        required=True,
        help="New explicit embedding artifact label (do not reuse v1)",
    )
    corpus_v2_embed_parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON embedding coverage summary",
    )
    corpus_v2_embed_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write Markdown embedding coverage summary",
    )
    corpus_v2_embed_parser.add_argument(
        "--model",
        default="text-embedding-3-small",
        help="Embedding model label for the provider request (default: text-embedding-3-small)",
    )
    corpus_v2_embed_parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Texts per embedding request batch",
    )
    corpus_v2_embed_parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete and regenerate existing rows for this snapshot/version",
    )
    corpus_v2_embed_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    corpus_v2_compose_parser = subparsers.add_parser(
        "corpus-v2-compose-snapshot",
        help="Create a composed snapshot by copying membership rows from source snapshots (no work row moves)",
    )
    corpus_v2_compose_parser.add_argument(
        "--snapshot-version",
        required=True,
        help="New source_snapshot_versions.source_snapshot_version to create",
    )
    corpus_v2_compose_parser.add_argument(
        "--from-snapshot",
        action="append",
        required=True,
        dest="from_snapshots",
        help="Source snapshot to copy included memberships from (repeatable)",
    )
    corpus_v2_compose_parser.add_argument(
        "--output",
        required=True,
        help="JSON summary output path (e.g. artifacts/snapshot-compose-YYYYMMDD.json)",
    )
    corpus_v2_compose_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Markdown summary output path",
    )
    corpus_v2_compose_parser.add_argument(
        "--note",
        default=None,
        help="Optional note stored on the new source_snapshot_versions row",
    )
    corpus_v2_compose_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ml_fresh_hybrid_snapshot_embed_parser = subparsers.add_parser(
        "ml-fresh-hybrid-snapshot-embed",
        help="Generate embeddings for a fresh hybrid eval-only source snapshot with audit provenance",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--fresh-hybrid-snapshot-hydration",
        required=True,
        help="Path to ml-fresh-hybrid-snapshot-hydration-v1 JSON",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--fresh-hybrid-candidate-plan-ingest",
        required=True,
        help="Path to ml-fresh-hybrid-candidate-plan-ingest-v1 JSON",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--snapshot-version",
        default=None,
        help="Source snapshot version to embed (default: value from hydration artifact)",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--embedding-version",
        default="fresh-hybrid-text-embedding-v1",
        help="Embedding version label (default: fresh-hybrid-text-embedding-v1)",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--mock-embeddings",
        action="store_true",
        help="Use deterministic local embeddings for tests/dry runs; no embedding API calls",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and count only; no embedding API calls and no writes",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap works embedded for tests/smoke only; omit for full snapshot coverage",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh hybrid embeddings JSON",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--artifact-version",
        default="ml-fresh-hybrid-snapshot-embeddings-v1",
        help="Artifact version string to write (default: ml-fresh-hybrid-snapshot-embeddings-v1)",
    )
    ml_fresh_hybrid_snapshot_embed_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-snapshot-embeddings",
        help="Generate embeddings for the second shadow-generalization source snapshot with audit provenance",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--second-snapshot-hydration",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-hydration-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--second-candidate-plan-ingest",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-candidate-plan-ingest-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--second-hybrid-candidate-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--snapshot-version",
        default=None,
        help="Source snapshot version to embed (default: source-snapshot-shadow-generalization-v1-20260521)",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--embedding-version",
        default="shadow-generalization-text-embedding-v1",
        help="Embedding version label (default: shadow-generalization-text-embedding-v1)",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--mock-embeddings",
        action="store_true",
        help="Use deterministic local embeddings for tests/dry runs; no embedding API calls",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and count only; no embedding API calls and no writes",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap works embedded for tests/smoke only; omit for full snapshot coverage",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second snapshot embeddings JSON",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--artifact-version",
        default="ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
        help="Artifact version string to write (default: ml-shadow-scorer-v1-second-snapshot-embeddings-v1)",
    )
    ml_shadow_scorer_second_snapshot_embeddings_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser = subparsers.add_parser(
        "ml-shadow-scorer-second-product-candidate-ranking",
        help="Run eval-only product-candidate ranking for the second shadow-generalization snapshot",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--second-snapshot-embeddings",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-embeddings-v1 JSON",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--second-snapshot-hydration",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-snapshot-hydration-v1 JSON",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--second-candidate-plan-ingest",
        required=True,
        help="Path to ml-shadow-scorer-v1-second-candidate-plan-ingest-v1 JSON",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--generalization-audit-plan",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-audit-v1 JSON",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--snapshot-version",
        default="source-snapshot-shadow-generalization-v1-20260521",
        help="Source snapshot version to rank (default: source-snapshot-shadow-generalization-v1-20260521)",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--embedding-version",
        default="shadow-generalization-text-embedding-v1",
        help="Embedding version label (default: shadow-generalization-text-embedding-v1)",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--ranking-version",
        default="shadow-generalization-product-candidate-ranking-v1",
        help="Ranking version label (default: shadow-generalization-product-candidate-ranking-v1)",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--family",
        default="emerging",
        help="Recommendation family used for handoff and reporting (default: emerging)",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and DB preflight only; no ranking_runs or paper_scores writes",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--output",
        required=True,
        help="Path to write second product-candidate ranking JSON",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--artifact-version",
        default="ml-shadow-scorer-v1-second-product-candidate-ranking-v1",
        help="Artifact version string to write (default: ml-shadow-scorer-v1-second-product-candidate-ranking-v1)",
    )
    ml_shadow_scorer_second_product_candidate_ranking_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser = subparsers.add_parser(
        "ml-fresh-hybrid-product-candidate-ranking",
        help="Run eval-only product-candidate ranking for the fresh hybrid snapshot with audit provenance",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--fresh-hybrid-snapshot-embeddings",
        required=True,
        help="Path to ml-fresh-hybrid-snapshot-embeddings-v1 JSON",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--fresh-hybrid-snapshot-hydration",
        required=True,
        help="Path to ml-fresh-hybrid-snapshot-hydration-v1 JSON",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--fresh-hybrid-candidate-plan-ingest",
        required=True,
        help="Path to ml-fresh-hybrid-candidate-plan-ingest-v1 JSON",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--snapshot-version",
        default="source-snapshot-fresh-hybrid-v1-20260518",
        help="Source snapshot version to rank (default: source-snapshot-fresh-hybrid-v1-20260518)",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--embedding-version",
        default="fresh-hybrid-text-embedding-v1",
        help="Embedding version label (default: fresh-hybrid-text-embedding-v1)",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--ranking-version",
        default="fresh-hybrid-product-candidate-ranking-v1",
        help="Ranking version label (default: fresh-hybrid-product-candidate-ranking-v1)",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--family",
        default="emerging",
        help="Recommendation family used for handoff commands (default: emerging)",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and planned parameters only; no ranking_runs or paper_scores writes",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh hybrid product-candidate ranking JSON",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--artifact-version",
        default="ml-fresh-hybrid-product-candidate-ranking-v1",
        help="Artifact version string to write (default: ml-fresh-hybrid-product-candidate-ranking-v1)",
    )
    ml_fresh_hybrid_product_candidate_ranking_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    cluster_inspection_parser = subparsers.add_parser(
        "cluster-inspection",
        help="Read-only cluster coherence/provenance inspection for one explicit snapshot + embedding + cluster identity",
    )
    cluster_inspection_parser.add_argument(
        "--corpus-snapshot-version",
        required=True,
        help="Explicit source_snapshot_versions.source_snapshot_version to inspect",
    )
    cluster_inspection_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Explicit embedding artifact version to verify/inspect",
    )
    cluster_inspection_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Explicit succeeded clustering_runs.cluster_version to inspect",
    )
    cluster_inspection_parser.add_argument(
        "--output",
        required=True,
        help="Path to write JSON inspection artifact",
    )
    cluster_inspection_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write Markdown inspection artifact",
    )
    cluster_inspection_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    bridge_diag_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no latest fallback)",
    )
    bridge_diag_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_diag_parser.add_argument(
        "--output",
        required=True,
        help="Path to write diagnostics JSON",
    )
    bridge_diag_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write diagnostics Markdown",
    )
    bridge_diag_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_sensitivity_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no latest fallback)",
    )
    bridge_sensitivity_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size used for overlap diagnostics (default 20)",
    )
    bridge_sensitivity_parser.add_argument(
        "--output",
        required=True,
        help="Path to write sensitivity JSON",
    )
    bridge_sensitivity_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write sensitivity Markdown",
    )
    bridge_sensitivity_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_objective_sim_parser = subparsers.add_parser(
        "bridge-objective-redesign-simulation",
        help="Read-only simulation of alternative bridge objectives (SELECT-only DB; no ranking writes)",
    )
    bridge_objective_sim_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (e.g. zero-weight baseline run)",
    )
    bridge_objective_sim_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Emerging / eligible overlap window (must be 20 for this simulation)",
    )
    bridge_objective_sim_parser.add_argument(
        "--output",
        required=True,
        help="Path to write simulation JSON",
    )
    bridge_objective_sim_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write simulation Markdown",
    )
    bridge_objective_sim_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root for default audit paths (default: cwd, or parent if cwd is services/pipeline)",
    )
    bridge_objective_sim_parser.add_argument(
        "--sensitivity-json",
        default=None,
        help="bridge_eligibility_sensitivity JSON (default: docs/audit/manual-review/... under repo root)",
    )
    bridge_objective_sim_parser.add_argument(
        "--failure-analysis-json",
        default=None,
        help="bridge_eligibility_failure_analysis JSON (default under docs/audit/manual-review/)",
    )
    bridge_objective_sim_parser.add_argument(
        "--bridge-worksheet-csv",
        default=None,
        help="Labeled bridge eligible top-20 CSV (default: bridge_eligible_<run>_top20.csv)",
    )
    bridge_objective_sim_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_weight_compare_parser.add_argument(
        "--baseline-ranking-run-id",
        required=True,
        help="Baseline ranking_run_id (typically zero bridge weight)",
    )
    bridge_weight_compare_parser.add_argument(
        "--experiment-ranking-run-id",
        required=True,
        help="Experiment ranking_run_id (small positive bridge weight)",
    )
    bridge_weight_compare_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_weight_compare_parser.add_argument(
        "--output",
        required=True,
        help="Path to write comparison JSON",
    )
    bridge_weight_compare_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write comparison Markdown",
    )
    bridge_weight_compare_parser.add_argument(
        "--baseline-bridge-worksheet",
        "--labeled-bridge-worksheet",
        dest="baseline_bridge_worksheet",
        default=None,
        help=(
            "Optional labeled bridge worksheet CSV path for already-reviewed baseline/delta rows. "
            "Default: docs/audit/manual-review/bridge_eligible_<baseline_run_id>_top20.csv"
        ),
    )
    bridge_weight_compare_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_objective_compare_parser = subparsers.add_parser(
        "bridge-objective-experiment-compare",
        help="Read-only baseline vs experiment when only bridge_eligibility_mode differs (same bridge weight)",
    )
    bridge_objective_compare_parser.add_argument(
        "--baseline-ranking-run-id",
        required=True,
        help="Baseline ranking_run_id",
    )
    bridge_objective_compare_parser.add_argument(
        "--experiment-ranking-run-id",
        required=True,
        help="Experiment ranking_run_id (different bridge_eligibility_mode)",
    )
    bridge_objective_compare_parser.add_argument(
        "--k",
        type=int,
        default=20,
        help="Top-k size from paper_scores (default 20)",
    )
    bridge_objective_compare_parser.add_argument(
        "--output",
        required=True,
        help="Path to write comparison JSON",
    )
    bridge_objective_compare_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write comparison Markdown",
    )
    bridge_objective_compare_parser.add_argument(
        "--baseline-bridge-worksheet",
        "--labeled-bridge-worksheet",
        dest="baseline_bridge_worksheet",
        required=True,
        help="Baseline labeled bridge eligible top-k CSV (for new-unlabeled detection)",
    )
    bridge_objective_compare_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_weight_delta_parser.add_argument(
        "--comparison",
        required=True,
        help="Bridge weight or objective experiment comparison JSON artifact",
    )
    bridge_weight_delta_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline labeled bridge eligible worksheet CSV",
    )
    bridge_weight_delta_parser.add_argument(
        "--experiment-diagnostics",
        required=True,
        help="Experiment bridge signal diagnostics JSON artifact",
    )
    bridge_weight_delta_parser.add_argument(
        "--output",
        required=True,
        help="Path to write delta review worksheet CSV",
    )
    bridge_weight_delta_parser.add_argument(
        "--baseline-ranking-run-id",
        default=None,
        help="Optional guard: expected baseline ranking_run_id",
    )
    bridge_weight_delta_parser.add_argument(
        "--experiment-ranking-run-id",
        default=None,
        help="Optional guard: expected experiment ranking_run_id",
    )
    bridge_weight_delta_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--comparison",
        required=True,
        help="Objective experiment comparison JSON artifact",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline labeled bridge eligible worksheet CSV",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--prior-delta-worksheet",
        required=True,
        help="Previously labeled bridge-weight delta worksheet CSV",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--output",
        required=True,
        help="Path to write label coverage correction JSON",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write label coverage correction Markdown",
    )
    bridge_objective_label_coverage_parser.add_argument(
        "--review-output",
        required=True,
        help="Path to write one-row unlabeled review worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline labeled bridge eligible worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--prior-delta-worksheet",
        required=True,
        help="Prior bridge-weight delta labeled worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--objective-delta-worksheet",
        required=True,
        help="Objective one-row labeled worksheet CSV",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--objective-comparison",
        required=True,
        help="Objective experiment comparison JSON artifact",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--output",
        required=True,
        help="Path to write objective labeled outcome JSON",
    )
    bridge_objective_labeled_outcome_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write objective labeled outcome Markdown",
    )
    bridge_weight_delta_summary_parser.add_argument(
        "--input",
        required=True,
        help="Completed delta review worksheet CSV path",
    )
    bridge_weight_delta_summary_parser.add_argument(
        "--output",
        required=True,
        help="Path to write delta summary JSON",
    )
    bridge_weight_delta_summary_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write delta summary Markdown",
    )
    bridge_weight_response_rollup_parser = subparsers.add_parser(
        "bridge-weight-response-rollup",
        help="Synthesize zero / w005 / w010 bridge-weight experiments into one JSON (+ optional Markdown) artifact",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--baseline-review-rollup",
        required=True,
        help="Path to baseline rank review rollup JSON (zero-weight run)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--compare-zero-vs-w005",
        required=True,
        help="Path to bridge_weight_experiment compare JSON (zero vs 0.05)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--delta-review-summary",
        required=True,
        help="Path to completed delta review summary JSON (0.05 experiment)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--compare-w005-vs-w010",
        required=True,
        help="Path to compare JSON (0.05 vs 0.10)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--compare-zero-vs-w010",
        required=True,
        help="Path to compare JSON (zero vs 0.10)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--labeled-baseline-bridge-worksheet",
        required=True,
        help="CSV path with baseline bridge eligible labels (paper_id column)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--delta-review-csv",
        required=True,
        help="Completed delta review worksheet CSV (paper_id column)",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--output",
        required=True,
        help="Path to write bridge weight response rollup JSON",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write rollup Markdown",
    )
    bridge_weight_response_rollup_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL for label coverage check (default: DATABASE_URL or PG* env)",
    )
    bridge_weight_labeled_outcome_parser = subparsers.add_parser(
        "bridge-weight-labeled-outcome",
        help="Eligible-bridge top-20 label quality by weight using baseline + delta worksheets (read-only)",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--baseline-bridge-worksheet",
        required=True,
        help="Baseline bridge eligible top-20 labeled CSV (rank-ee2ba6c816)",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--delta-review-csv",
        required=True,
        help="Completed 0.05 delta review CSV",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--response-rollup",
        required=True,
        help="bridge_weight_response_rollup JSON (movement + stack)",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--compare-zero-vs-w005",
        required=True,
        help="Compare JSON zero vs 0.05",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--compare-w005-vs-w010",
        required=True,
        help="Compare JSON 0.05 vs 0.10",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--compare-zero-vs-w010",
        required=True,
        help="Compare JSON zero vs 0.10",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--diagnostics-zero",
        required=True,
        help="bridge_signal_diagnostics JSON for rank-ee2ba6c816",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--diagnostics-w005",
        required=True,
        help="bridge_signal_diagnostics JSON for rank-bc1123e00c",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--diagnostics-w010",
        required=True,
        help="bridge_signal_diagnostics JSON for rank-9a02c81d40",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--output",
        required=True,
        help="Path to write labeled outcome JSON",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write labeled outcome Markdown",
    )
    bridge_weight_labeled_outcome_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL to resolve work_id→paper_id for eligible head (default: DATABASE_URL or PG* env)",
    )

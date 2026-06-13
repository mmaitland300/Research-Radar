from __future__ import annotations


def register_corpus_parsers(subparsers) -> None:
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

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from pipeline.bootstrap_loader import database_url_from_env, load_resolved_policy_from_database, run_bootstrap_ingest
from pipeline.clustering_persistence import count_included_missing_cluster_assignment
from pipeline.clustering_run import execute_clustering_run
from pipeline.embedding_persistence import (
    count_included_works_for_snapshot,
    count_missing_embedding_candidates,
    latest_corpus_snapshot_version_with_works,
)
from pipeline.embedding_run import execute_embedding_run
from pipeline.ranking_run import (
    BRIDGE_ELIGIBILITY_MODE_CURRENT,
    SUPPORTED_BRIDGE_ELIGIBILITY_MODES,
    MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY,
    execute_ranking_run,
    validate_bridge_eligibility_mode,
    validate_bridge_weight_for_bridge_family,
)
from pipeline.recommendation_review_worksheet import (
    WorksheetError,
    write_recommendation_review_worksheet,
)
from pipeline.recommendation_review_summary import (
    ReviewSummaryError,
    run_recommendation_review_summary,
)
from pipeline.recommendation_review_rollup import (
    ReviewRollupError,
    run_recommendation_review_rollup,
)
from pipeline.bridge_experiment_readiness import (
    BridgeExperimentReadinessError,
    run_bridge_experiment_readiness,
)
from pipeline.bridge_signal_diagnostics import (
    BridgeSignalDiagnosticsError,
    run_bridge_signal_diagnostics,
)
from pipeline.bridge_objective_experiment_compare import (
    BridgeObjectiveExperimentCompareError,
    run_bridge_objective_experiment_compare,
)
from pipeline.bridge_objective_label_coverage import (
    BridgeObjectiveLabelCoverageError,
    run_bridge_objective_label_coverage,
)
from pipeline.bridge_objective_labeled_outcome import (
    BridgeObjectiveLabeledOutcomeError,
    run_bridge_objective_labeled_outcome,
)
from pipeline.bridge_weight_experiment_compare import (
    BridgeWeightExperimentCompareError,
    run_bridge_weight_experiment_compare,
)
from pipeline.bridge_weight_experiment_delta_worksheet import (
    BridgeWeightExperimentDeltaWorksheetError,
    write_bridge_weight_experiment_delta_worksheet,
)
from pipeline.bridge_weight_experiment_delta_summary import (
    BridgeWeightExperimentDeltaSummaryError,
    run_bridge_weight_experiment_delta_summary,
)
from pipeline.bridge_weight_response_rollup import (
    BridgeWeightResponseRollupError,
    run_bridge_weight_response_rollup,
)
from pipeline.bridge_weight_labeled_outcome import (
    BridgeWeightLabeledOutcomeError,
    run_bridge_weight_labeled_outcome,
)
from pipeline.bridge_eligibility_sensitivity import (
    BridgeEligibilitySensitivityError,
    run_bridge_eligibility_sensitivity,
)
from pipeline.bridge_objective_redesign_simulation import (
    BridgeObjectiveRedesignSimulationError,
    run_bridge_objective_redesign_simulation,
)
from pipeline.cluster_inspection import (
    ClusterInspectionError,
    run_cluster_inspection,
)
from pipeline.work_text_repair import run_work_text_repair_cli
from pipeline.jobs import (
    create_bootstrap_bundle,
    write_bootstrap_plan,
    write_ingest_artifacts,
    write_source_resolution_manifest,
    write_source_resolution_results,
)
from pipeline.openalex import build_bootstrap_work_plans, build_source_resolution_plans
from pipeline.policy import CorpusPolicy, corpus_policy_with_openalex_source_ids
from pipeline.source_resolution import resolve_all_sources, slug_to_openalex_id_map


def main() -> None:
    parser = argparse.ArgumentParser(description="Research Radar pipeline utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    policy_parser = subparsers.add_parser("print-policy", help="Print the active corpus policy")
    policy_parser.add_argument("--json", action="store_true", help="Print as JSON")

    bootstrap_parser = subparsers.add_parser("bootstrap-plan", help="Write bootstrap snapshot, ingest run, and query plans")
    bootstrap_parser.add_argument("--output", default="artifacts", help="Output directory")
    bootstrap_parser.add_argument("--note", default="Bootstrap ingest planning run", help="Snapshot note")
    src = bootstrap_parser.add_mutually_exclusive_group(required=False)
    src.add_argument(
        "--resolve-openalex",
        action="store_true",
        help="Resolve canonical source IDs via OpenAlex /sources (required unless DB or coded IDs exist)",
    )
    src.add_argument(
        "--database-source-ids",
        action="store_true",
        help="Load openalex_source_id from Postgres source_policies (after a prior bootstrap-run resolve)",
    )
    bootstrap_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL for --database-source-ids (default: DATABASE_URL or PG* env)",
    )
    bootstrap_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent when using --resolve-openalex",
    )

    run_parser = subparsers.add_parser(
        "bootstrap-run",
        help="Execute OpenAlex bootstrap: raw pages, Postgres load, manifest (needs DATABASE_URL or PG*)",
    )
    run_parser.add_argument("--output", default="artifacts", help="Manifest and snapshot metadata directory")
    run_parser.add_argument("--raw-root", default="artifacts", help="Root directory for raw OpenAlex page JSON")
    run_parser.add_argument("--note", default="API bootstrap ingest", help="Snapshot note")
    run_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL env or PGHOST/PGUSER/PGPASSWORD/PGDATABASE)",
    )
    run_parser.add_argument(
        "--max-pages-per-source",
        type=int,
        default=None,
        help="Cap pages per venue plan (for smoke tests; default: paginate until exhausted)",
    )
    run_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent (default: OPENALEX_MAILTO env)",
    )

    embed_parser = subparsers.add_parser(
        "embed-works",
        help="Write one embedding per included work from title + abstract",
    )
    embed_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label stored on embeddings rows (e.g. v1-title-abstract-1536)",
    )
    embed_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    embed_parser.add_argument(
        "--model",
        default="text-embedding-3-small",
        help="Embedding model label for the provider request (default: text-embedding-3-small)",
    )
    embed_parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Texts per embedding request batch",
    )
    embed_parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Cap missing works processed on this run (smoke tests only; omit for full snapshot coverage)",
    )
    embed_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    ranking_parser = subparsers.add_parser(
        "ranking-run",
        help="Create ranking_runs row, write stub paper_scores, finalize (Step 2 plumbing)",
    )
    ranking_parser.add_argument(
        "--ranking-version",
        required=True,
        help="Algorithm / config label (e.g. v0-heuristic-no-embeddings)",
    )
    ranking_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    ranking_parser.add_argument(
        "--embedding-version",
        default="none-v0",
        help="Embedding artifact version label stored on the run",
    )
    ranking_parser.add_argument(
        "--cluster-version",
        default=None,
        help="Optional succeeded clustering_runs.cluster_version for ML2-5a bridge_score column (must match snapshot + embedding-version)",
    )
    ranking_parser.add_argument("--note", default=None, help="Optional run notes")
    ranking_parser.add_argument(
        "--low-cite-min-year",
        type=int,
        default=2019,
        help="Undercited family: min publication year (default 2019; see docs/candidate-pool-low-cite.md)",
    )
    ranking_parser.add_argument(
        "--low-cite-max-citations",
        type=int,
        default=30,
        help="Undercited family: max citation_count inclusive (default 30)",
    )
    ranking_parser.add_argument(
        "--bridge-weight-for-family-bridge",
        type=lambda s: validate_bridge_weight_for_bridge_family(float(s)),
        default=0.0,
        metavar="W",
        help=(
            "Bridge family only: weight on cluster-boundary bridge_score in final_score (ML2-5b). "
            f"Default 0.0 (ML2-5a). Range [0.0, {MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY}]."
        ),
    )
    ranking_parser.add_argument(
        "--bridge-eligibility-mode",
        type=lambda s: validate_bridge_eligibility_mode(s),
        default=BRIDGE_ELIGIBILITY_MODE_CURRENT,
        choices=sorted(SUPPORTED_BRIDGE_ELIGIBILITY_MODES),
        help=(
            "Bridge eligibility policy for bridge-family rows. "
            "Default current behavior; use top50_cross_cluster_gte_0_40 for stricter threshold sweep mode."
        ),
    )
    ranking_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    cluster_parser = subparsers.add_parser(
        "cluster-works",
        help="Cluster embedded included works for a snapshot/version identity",
    )
    cluster_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label to cluster (must already exist in embeddings table)",
    )
    cluster_parser.add_argument(
        "--cluster-version",
        required=True,
        help="Cluster assignment artifact label written to clusters + clustering_runs",
    )
    cluster_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    cluster_parser.add_argument(
        "--cluster-count",
        type=int,
        default=12,
        help="Target number of clusters for kmeans-l2-v0 (default 12)",
    )
    cluster_parser.add_argument(
        "--max-iterations",
        type=int,
        default=20,
        help="Maximum kmeans iterations (default 20)",
    )
    cluster_parser.add_argument("--note", default=None, help="Optional run notes")
    cluster_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    repair_parser = subparsers.add_parser(
        "repair-works-text",
        help="Re-apply title/abstract text cleanup (mojibake, HTML entities) to included works in a snapshot",
    )
    repair_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    repair_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would change without writing",
    )
    repair_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

    cov_parser = subparsers.add_parser(
        "embedding-coverage",
        help="Report how many included works in a snapshot have rows in embeddings for a version",
    )
    cov_parser.add_argument(
        "--embedding-version",
        required=True,
        help="Embedding artifact label (same as embed-works and cluster-works)",
    )
    cov_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Target snapshot; default = latest snapshot that has included works",
    )
    cov_parser.add_argument(
        "--fail-on-gaps",
        action="store_true",
        help="Exit with code 1 if any included work is missing an embedding, or (with --cluster-version) any included work lacks a cluster row",
    )
    cov_parser.add_argument(
        "--cluster-version",
        default=None,
        help="Optional cluster artifact label: report included works missing a clusters row (after cluster-works)",
    )
    cov_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )

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
    ml_label_dataset_parser = subparsers.add_parser(
        "ml-label-dataset",
        help="Export versioned manual-label dataset JSON (+ optional Markdown) from audit review CSVs (no DB, no ranking)",
    )
    ml_label_dataset_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v1.json)",
    )
    ml_label_dataset_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown data card",
    )
    ml_label_dataset_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root containing docs/audit/manual-review (default: parent of services/pipeline)",
    )
    ml_label_dataset_parser.add_argument(
        "--manual-review-dir",
        default=None,
        help="Directory of manual-review CSV worksheets (default: <repo-root>/docs/audit/manual-review)",
    )
    ml_label_dataset_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v1",
        help="Version string written on each row and the payload (default: ml-label-dataset-v1; e.g. ml-label-dataset-v2)",
    )
    ml_label_dataset_v5_parser = subparsers.add_parser(
        "ml-label-dataset-v5-reviewer-blind-ingest",
        help=(
            "Build ml-label-dataset v5 from an existing base dataset plus the validated "
            "reviewer-blind v2 labeled worksheet and row_id-keyed sidecar"
        ),
    )
    ml_label_dataset_v5_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v5, docs/audit/ml-label-dataset-v4.json)",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank reviewer-blind v2 CSV template",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed reviewer-blind v2 labeled CSV",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to reviewer-blind v2 hidden context sidecar JSON",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v5 JSON",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown data card",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v5_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v5",
        help="Version string for the new artifact and appended v2 rows (default: ml-label-dataset-v5)",
    )
    ml_label_dataset_v6_parser = subparsers.add_parser(
        "ml-label-dataset-v6-hard-negative-ingest",
        help=(
            "Build ml-label-dataset v6 from an existing base dataset plus the validated "
            "hard-negative labeled worksheet and row_id-keyed sidecar"
        ),
    )
    ml_label_dataset_v6_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v6, docs/audit/ml-label-dataset-v5.json)",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank hard-negative CSV template",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed hard-negative labeled CSV",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to hard-negative hidden context sidecar JSON",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v6 JSON",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown data card",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v6_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v6",
        help="Version string for the new artifact and appended hard-negative rows (default: ml-label-dataset-v6)",
    )
    ml_label_dataset_v7_parser = subparsers.add_parser(
        "ml-label-dataset-v7-external-near-miss-ingest",
        help=(
            "Build ml-label-dataset v7 from an existing base dataset plus the validated "
            "external near-miss labeled worksheet and row_id-keyed sidecar"
        ),
    )
    ml_label_dataset_v7_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v7, docs/audit/ml-label-dataset-v6.json)",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank external near-miss CSV template",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed external near-miss labeled CSV",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to external near-miss hidden context sidecar JSON",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v7 JSON",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown data card",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v7_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v7",
        help="Version string for the new artifact and appended external near-miss rows (default: ml-label-dataset-v7)",
    )
    ml_label_dataset_v8_parser = subparsers.add_parser(
        "ml-label-dataset-v8-transfer-gap-ingest",
        help=(
            "Build ml-label-dataset v8 from an existing v7 dataset plus the validated "
            "transfer-gap labeled worksheet and row_id-keyed sidecar"
        ),
    )
    ml_label_dataset_v8_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v8, docs/audit/ml-label-dataset-v7.json)",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank transfer-gap CSV template",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed transfer-gap labeled CSV",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to transfer-gap hidden context sidecar JSON",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v8 JSON",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown data card",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v8_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v8",
        help="Version string for the new artifact and appended transfer-gap rows (default: ml-label-dataset-v8)",
    )
    ml_offline_baseline_parser = subparsers.add_parser(
        "ml-offline-baseline-eval",
        help="Read-only offline label baseline metrics (join ml-label-dataset to paper_scores for one ranking_run_id)",
    )
    ml_offline_baseline_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v1.json)",
    )
    ml_offline_baseline_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id to join (no implicit latest selection)",
    )
    ml_offline_baseline_parser.add_argument(
        "--output",
        required=True,
        help="Path to write offline baseline eval JSON",
    )
    ml_offline_baseline_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_offline_baseline_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_external_feature_coverage_parser = subparsers.add_parser(
        "ml-external-feature-coverage",
        help="Read-only feature coverage diagnostic for ml_external_near_miss_audit rows (no training, no ranking writes)",
    )
    ml_external_feature_coverage_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v7.json)",
    )
    ml_external_feature_coverage_parser.add_argument(
        "--context-sidecar",
        default=None,
        help="Optional external near-miss context sidecar JSON for SHA and row_id parity checks",
    )
    ml_external_feature_coverage_parser.add_argument(
        "--embedding-version",
        default="v2-title-abstract-1536-cleantext-r1",
        help="Embedding version to check in embeddings table (default: v2-title-abstract-1536-cleantext-r1)",
    )
    ml_external_feature_coverage_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_external_feature_coverage_parser.add_argument(
        "--output",
        required=True,
        help="Path to write feature coverage JSON",
    )
    ml_external_feature_coverage_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_external_text_corpus_parser = subparsers.add_parser(
        "ml-external-text-corpus",
        help="Hydrate ml_external_near_miss_audit rows from OpenAlex into a read-only text corpus artifact",
    )
    ml_external_text_corpus_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON (e.g. docs/audit/ml-label-dataset-v7.json)",
    )
    ml_external_text_corpus_parser.add_argument(
        "--context-sidecar",
        default=None,
        help="Optional external near-miss context sidecar JSON for SHA and row_id parity checks",
    )
    ml_external_text_corpus_parser.add_argument(
        "--output",
        required=True,
        help="Path to write hydrated text corpus JSON",
    )
    ml_external_text_corpus_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_external_text_corpus_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent (default: OPENALEX_MAILTO env or local placeholder)",
    )
    ml_external_text_corpus_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Skip live OpenAlex HTTP and build deterministic mock hydration from dataset/context previews",
    )
    ml_labeled_text_corpus_parser = subparsers.add_parser(
        "ml-labeled-text-corpus",
        help="Build observation-level text corpus for explicitly labeled audit rows (no DB, embeddings, or ranking)",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--external-text-corpus",
        default=None,
        help="Optional frozen external text corpus JSON for row_id reuse",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--output",
        required=True,
        help="Path to write labeled text corpus JSON",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--mailto",
        default=None,
        help="Contact for OpenAlex User-Agent (default: OPENALEX_MAILTO env or local placeholder)",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Skip live OpenAlex HTTP and build deterministic mock hydration for non-reused rows",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional cap on selected labeled audit rows for dev/CI; omit for full corpus",
    )
    ml_labeled_text_corpus_parser.add_argument(
        "--corpus-version",
        default="ml-labeled-text-corpus-v1",
        help="Corpus version string to write (default: ml-labeled-text-corpus-v1)",
    )
    ml_labeled_text_corpus_normalize_parser = subparsers.add_parser(
        "ml-labeled-text-corpus-normalize",
        help="Normalize ml-labeled-text-corpus-v1 into canonical title+abstract text v2 (no DB, HTTP, embeddings, or ranking)",
    )
    ml_labeled_text_corpus_normalize_parser.add_argument(
        "--source-corpus",
        required=True,
        help="Path to ml-labeled-text-corpus-v1 JSON",
    )
    ml_labeled_text_corpus_normalize_parser.add_argument(
        "--output",
        required=True,
        help="Path to write normalized labeled text corpus JSON",
    )
    ml_labeled_text_corpus_normalize_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_labeled_text_corpus_normalize_parser.add_argument(
        "--source-corpus-version",
        default="ml-labeled-text-corpus-v1",
        help="Expected source corpus metadata.corpus_version (default: ml-labeled-text-corpus-v1)",
    )
    ml_labeled_text_corpus_normalize_parser.add_argument(
        "--corpus-version",
        default="ml-labeled-text-corpus-v2",
        help="Output corpus version string (default: ml-labeled-text-corpus-v2)",
    )
    ml_external_text_embeddings_parser = subparsers.add_parser(
        "ml-external-text-embeddings",
        help="Vectorize a frozen external text corpus artifact into an offline embedding artifact (no DB, no ranking)",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--text-corpus",
        required=True,
        help="Path to frozen external text corpus JSON (e.g. docs/audit/ml-external-text-corpus-v7.json)",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--output",
        required=True,
        help="Path to write external text embeddings JSON",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--embedding-model",
        default="text-embedding-3-small",
        help="Embedding model name (default: text-embedding-3-small)",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--expected-dimensions",
        type=int,
        default=1536,
        help="Expected embedding vector dimensions (default: 1536)",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--batch-size",
        type=int,
        default=16,
        help="Embedding batch size (default: 16)",
    )
    ml_external_text_embeddings_parser.add_argument(
        "--mock-embeddings",
        action="store_true",
        help="Skip live OpenAI and emit deterministic fake vectors for tests/dry runs",
    )
    ml_text_only_baseline_parser = subparsers.add_parser(
        "ml-text-only-baseline",
        help="Offline text-only diagnostic over frozen external embeddings and v7 labels (no DB, no ranking)",
    )
    ml_text_only_baseline_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-external-text-embeddings-v7 JSON",
    )
    ml_text_only_baseline_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v7 JSON",
    )
    ml_text_only_baseline_parser.add_argument(
        "--output",
        required=True,
        help="Path to write text-only baseline JSON",
    )
    ml_text_only_baseline_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_text_only_baseline_parser.add_argument(
        "--random-seed",
        type=int,
        default=0,
        help="Random seed for shuffled stratified CV and random baseline (default: 0)",
    )
    ml_text_only_baseline_parser.add_argument(
        "--cv-folds",
        type=int,
        default=5,
        help="Requested stratified CV folds (reduced to min class count per target; default: 5)",
    )
    ml_labeled_text_embeddings_parser = subparsers.add_parser(
        "ml-labeled-text-embeddings",
        help="Vectorize ml-labeled-text-corpus-v1 into a frozen offline embedding artifact (no DB, no ranking)",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--text-corpus",
        required=True,
        help="Path to ml-labeled-text-corpus-v1 JSON",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--output",
        required=True,
        help="Path to write labeled text embeddings JSON",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--embedding-model",
        default="text-embedding-3-small",
        help="Embedding model name (default: text-embedding-3-small)",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--expected-dimensions",
        type=int,
        default=1536,
        help="Expected embedding vector dimensions (default: 1536)",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--batch-size",
        type=int,
        default=16,
        help="Embedding batch size (default: 16)",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--mock-embeddings",
        action="store_true",
        help="Skip live OpenAI and emit deterministic fake vectors for tests/dry runs",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--source-corpus-version",
        default="ml-labeled-text-corpus-v1",
        help="Expected text corpus metadata.corpus_version (default: ml-labeled-text-corpus-v1)",
    )
    ml_labeled_text_embeddings_parser.add_argument(
        "--embedding-artifact-version",
        default="ml-labeled-text-embeddings-v1",
        help="Embedding artifact version string to write (default: ml-labeled-text-embeddings-v1)",
    )
    ml_text_baseline_cross_pool_parser = subparsers.add_parser(
        "ml-text-baseline-cross-pool",
        help="Offline source-transfer diagnostic over labeled text embeddings and v7 labels (no DB, no ranking)",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-labeled-text-embeddings-v1 JSON",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v7 JSON",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--output",
        required=True,
        help="Path to write cross-pool baseline JSON",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--random-seed",
        type=int,
        default=0,
        help="Random seed for shuffled stratified CV (default: 0)",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--expected-embedding-artifact-version",
        default="ml-labeled-text-embeddings-v1",
        help="Expected embedding metadata.embedding_artifact_version (default: ml-labeled-text-embeddings-v1)",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--expected-label-dataset-version",
        default="ml-label-dataset-v7",
        help="Expected label dataset_version (default: ml-label-dataset-v7)",
    )
    ml_text_baseline_cross_pool_parser.add_argument(
        "--baseline-version",
        default="ml-text-baseline-cross-pool-v1",
        help="Baseline version string to write (default: ml-text-baseline-cross-pool-v1)",
    )
    ml_text_transfer_readiness_parser = subparsers.add_parser(
        "ml-text-transfer-readiness",
        help="Read-only synthesis of text transfer audit artifacts (no DB, embeddings, ranking, or training)",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--cross-pool",
        required=True,
        help="Path to ml-text-baseline-cross-pool-v1 JSON",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v7 JSON",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--text-corpus-v2",
        default=None,
        help="Optional path to ml-labeled-text-corpus-v2 JSON for text-format evidence",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--embeddings-v1",
        default=None,
        help="Optional path to ml-labeled-text-embeddings-v1 JSON for provenance only",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--output",
        required=True,
        help="Path to write text transfer readiness JSON",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--expected-cross-pool-version",
        default="ml-text-baseline-cross-pool-v1",
        help="Expected cross-pool metadata.baseline_version (default: ml-text-baseline-cross-pool-v1)",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--expected-label-dataset-version",
        default="ml-label-dataset-v7",
        help="Expected label dataset_version (default: ml-label-dataset-v7)",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--expected-text-corpus-version",
        default="ml-labeled-text-corpus-v2",
        help="Expected text corpus metadata.corpus_version (default: ml-labeled-text-corpus-v2)",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--expected-embeddings-version",
        default="ml-labeled-text-embeddings-v1",
        help="Expected embeddings metadata.embedding_artifact_version (default: ml-labeled-text-embeddings-v1)",
    )
    ml_text_transfer_readiness_parser.add_argument(
        "--readiness-version",
        default="ml-text-transfer-readiness-v1",
        help="Readiness version string to write (default: ml-text-transfer-readiness-v1)",
    )
    ml_production_readiness_plan_parser = subparsers.add_parser(
        "ml-production-readiness-plan",
        help="Write a deterministic production ML gate/spec artifact (no DB, training, embeddings, ranking, or splits)",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--transfer-readiness",
        required=True,
        help="Path to ml-text-transfer-readiness-v1 JSON",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v7 JSON",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--conflict-policy",
        default=None,
        help="Optional conflict policy Markdown path (default: docs/audit/ml-label-conflict-policy.md)",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--cross-pool",
        default=None,
        help="Optional ml-text-baseline-cross-pool-v1 JSON for provenance validation",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--text-corpus-v2",
        default=None,
        help="Optional ml-labeled-text-corpus-v2 JSON for provenance validation",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--embeddings-v1",
        default=None,
        help="Optional ml-labeled-text-embeddings-v1 JSON for provenance validation only",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--output",
        required=True,
        help="Path to write production readiness plan JSON",
    )
    ml_production_readiness_plan_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_label_split_policy_parser = subparsers.add_parser(
        "ml-label-split-policy",
        help="Write a machine-checkable offline ML label split policy spec (no folds, DB, training, or ranking)",
    )
    ml_label_split_policy_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_label_split_policy_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy.md",
    )
    ml_label_split_policy_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_label_split_policy_parser.add_argument(
        "--transfer-readiness",
        default=None,
        help="Optional transfer-readiness JSON for evidence-only provenance",
    )
    ml_label_split_policy_parser.add_argument(
        "--output",
        required=True,
        help="Path to write split policy JSON",
    )
    ml_label_split_policy_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_label_split_policy_parser.add_argument(
        "--policy-version",
        default="ml-label-split-policy-v1",
        help="Policy version string to write (default: ml-label-split-policy-v1)",
    )
    ml_label_split_policy_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_offline_ranker_experiment_parser = subparsers.add_parser(
        "ml-offline-ranker-experiment",
        help="Run offline grouped-CV ranker experiment over frozen labeled text embeddings (no DB/ranking/production output)",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-labeled-text-embeddings-v3 JSON",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--output",
        required=True,
        help="Path to write offline ranker experiment JSON",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--target",
        default="good_or_acceptable",
        help="Target to evaluate; v1 supports only good_or_acceptable",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--random-seed",
        type=int,
        default=None,
        help="Random seed; default uses split policy recommended seed, else 0",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--cv-folds",
        type=int,
        default=5,
        help="Requested grouped CV folds (default: 5)",
    )
    ml_offline_ranker_experiment_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_offline_metric_gates_parser = subparsers.add_parser(
        "ml-offline-metric-gates",
        help="Evaluate machine-checkable offline metric gates over an audit-pool ranker experiment (no training/ranking/DB)",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--ranker-experiment",
        required=True,
        help="Path to ml-offline-ranker-experiment-v1 JSON",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--transfer-readiness",
        default=None,
        help="Optional ml-text-transfer-readiness-v8 JSON for advisory evidence only",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--output",
        required=True,
        help="Path to write offline metric gates JSON",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--gates-version",
        default="ml-offline-metric-gates-v1",
        help="Gates version string to write (default: ml-offline-metric-gates-v1)",
    )
    ml_offline_metric_gates_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_tiny_baseline_parser = subparsers.add_parser(
        "ml-tiny-baseline",
        help="Offline-only emerging tiny baseline (stratified CV vs final_score heuristic; read-only DB)",
    )
    ml_tiny_baseline_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset JSON",
    )
    ml_tiny_baseline_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Explicit ranking_run_id (no implicit latest selection)",
    )
    ml_tiny_baseline_parser.add_argument(
        "--family",
        required=True,
        choices=["emerging"],
        help="Only emerging is supported for this experiment",
    )
    ml_tiny_baseline_parser.add_argument(
        "--target",
        required=True,
        choices=sorted(["good_or_acceptable", "surprising_or_useful"]),
        help="Manual target (refuses bridge_like_yes_or_partial)",
    )
    ml_tiny_baseline_parser.add_argument(
        "--output",
        required=True,
        help="Path to write tiny baseline JSON",
    )
    ml_tiny_baseline_parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional path to write companion Markdown summary",
    )
    ml_tiny_baseline_parser.add_argument(
        "--database-url",
        default=None,
        help="Postgres URL (default: DATABASE_URL or PG* env)",
    )
    ml_offline_production_candidate_scoring_parser = subparsers.add_parser(
        "ml-offline-production-candidate-scoring",
        help="Read-only production-candidate offline scoring diagnostic over an existing paper_scores pool",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--metric-gates",
        required=True,
        help="Path to ml-offline-metric-gates-v1 JSON",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--audit-ranker-experiment",
        required=True,
        help="Path to ml-offline-ranker-experiment-v1 JSON",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-labeled-text-embeddings-v3 JSON",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--ranking-run-id",
        required=True,
        help="Existing ranking_run_id to inspect (no new ranking run)",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--family",
        default="emerging",
        choices=["emerging"],
        help="Candidate recommendation family (default: emerging; v1 supports only emerging)",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--output",
        required=True,
        help="Path to write production-candidate scoring JSON",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--target",
        default="good_or_acceptable",
        help="Target to evaluate (default: good_or_acceptable; v1 supports only this target)",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--experiment-version",
        default=None,
        help="Experiment version string to write (default is selected from scoring mode)",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--scoring-mode",
        default="heuristic_and_coverage_only",
        choices=[
            "heuristic_and_coverage_only",
            "heuristic_and_audit_embedding_scorer",
            "heuristic_and_holdout_embedding_scorer",
        ],
        help="Scoring mode (default: heuristic_and_coverage_only)",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--audit-embedding-scorer-export",
        default=None,
        help="Path to audit embedding scorer JSON; required for learned scoring modes",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--holdout-assignment",
        default=None,
        help="Path to ml-learned-scorer-holdout-assignment-v1 JSON; required for heuristic_and_holdout_embedding_scorer",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--holdout-policy",
        default=None,
        help="Optional ml-learned-scorer-holdout-policy-v1 JSON provenance for holdout scoring",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--production-candidate-metric-gates-v2",
        default=None,
        help="Optional ml-offline-production-candidate-metric-gates-v2 JSON provenance for holdout scoring",
    )
    ml_offline_production_candidate_scoring_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_offline_production_candidate_metric_gates_parser = subparsers.add_parser(
        "ml-offline-production-candidate-metric-gates",
        help="Evaluate product-candidate offline metric gates over an existing scoring diagnostic (no DB/training/ranking)",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--production-candidate-scoring",
        required=True,
        help="Path to ml-offline-production-candidate-scoring JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--offline-metric-gates",
        required=True,
        help="Path to ml-offline-metric-gates-v1 JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--output",
        required=True,
        help="Path to write product-candidate metric gates JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--gates-version",
        default="ml-offline-production-candidate-metric-gates-v1",
        choices=[
            "ml-offline-production-candidate-metric-gates-v1",
            "ml-offline-production-candidate-metric-gates-v2",
            "ml-offline-production-candidate-metric-gates-v3",
        ],
        help=(
            "Gates version string to write "
            "(default: ml-offline-production-candidate-metric-gates-v1)"
        ),
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--audit-embedding-scorer-export",
        default=None,
        help="Path to audit embedding scorer JSON; required for gates v2/v3",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--holdout-assignment",
        default=None,
        help="Path to ml-learned-scorer-holdout-assignment-v1 JSON; required for gates v3",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--holdout-policy",
        default=None,
        help="Path to ml-learned-scorer-holdout-policy-v1 JSON; required for gates v3",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--production-candidate-metric-gates-v1",
        default=None,
        help="Optional path to prior ml-offline-production-candidate-metric-gates-v1 JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--production-candidate-metric-gates-v2",
        default=None,
        help="Optional path to prior ml-offline-production-candidate-metric-gates-v2 JSON",
    )
    ml_offline_production_candidate_metric_gates_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_offline_audit_embedding_scorer_export_parser = subparsers.add_parser(
        "ml-offline-audit-embedding-scorer-export",
        help="Export a JSON-only audit embedding scorer (no DB/ranking/product-candidate scoring)",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-labeled-text-embeddings-v3 JSON",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--production-candidate-metric-gates",
        default=None,
        help="Path to ml-offline-production-candidate-metric-gates-v1 JSON",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--fit-mode",
        choices=["full_fit_audit_corpus", "holdout_bound_train_only"],
        default="full_fit_audit_corpus",
        help="Fit mode (default: full_fit_audit_corpus)",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--holdout-assignment",
        default=None,
        help="Path to ml-learned-scorer-holdout-assignment-v1 JSON; required for holdout_bound_train_only",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--holdout-policy",
        default=None,
        help="Path to ml-learned-scorer-holdout-policy-v1 JSON; required for holdout_bound_train_only",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--audit-embedding-scorer-export-v1",
        default=None,
        help="Optional ml-offline-audit-embedding-scorer-v1 JSON for full-fit reference metrics only",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--ranker-experiment",
        default=None,
        help="Optional ml-offline-ranker-experiment-v1 JSON for reference CV aggregate metrics only",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--output",
        required=True,
        help="Path to write audit embedding scorer JSON",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--target",
        default="good_or_acceptable",
        help="Target to fit (default: good_or_acceptable; v1 hard-fails other targets)",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--random-seed",
        type=int,
        default=None,
        help="Random seed; default uses split policy recommended seed",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--scorer-version",
        default=None,
        help="Scorer version string to write (default depends on --fit-mode)",
    )
    ml_offline_audit_embedding_scorer_export_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_learned_scorer_holdout_policy_parser = subparsers.add_parser(
        "ml-learned-scorer-holdout-policy",
        help="Write learned scorer holdout boundary policy (no assignments, training, DB, ranking, or shadow/prod changes)",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-labeled-text-embeddings-v3 JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--production-candidate-scoring",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v2 JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--production-candidate-metric-gates",
        required=True,
        help="Path to ml-offline-production-candidate-metric-gates-v2 JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--output",
        required=True,
        help="Path to write learned scorer holdout policy JSON",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--policy-version",
        default="ml-learned-scorer-holdout-policy-v1",
        help="Policy version string to write (default: ml-learned-scorer-holdout-policy-v1)",
    )
    ml_learned_scorer_holdout_policy_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_learned_scorer_holdout_assignment_parser = subparsers.add_parser(
        "ml-learned-scorer-holdout-assignment",
        help="Materialize learned scorer holdout train/eval assignments (no training, DB, ranking, shadow, or prod)",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--embeddings",
        required=True,
        help="Path to ml-labeled-text-embeddings-v3 JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--production-candidate-scoring",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v2 JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--holdout-policy",
        required=True,
        help="Path to ml-learned-scorer-holdout-policy-v1 JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--production-candidate-metric-gates",
        required=True,
        help="Path to ml-offline-production-candidate-metric-gates-v2 JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--output",
        required=True,
        help="Path to write learned scorer holdout assignment JSON",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--assignment-version",
        default="ml-learned-scorer-holdout-assignment-v1",
        help="Assignment version string to write (default: ml-learned-scorer-holdout-assignment-v1)",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--strategy-id",
        default="product_candidate_snapshot_holdout",
        help="Holdout strategy id (default: product_candidate_snapshot_holdout)",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional provenance seed; default comes from holdout policy, then split policy, then 20260515",
    )
    ml_learned_scorer_holdout_assignment_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser = subparsers.add_parser(
        "ml-hybrid-scorer-offline-experiment-spec",
        help="Write hybrid scorer offline experiment pre-registration spec (no scoring, training, DB, shadow, or prod)",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--production-candidate-scoring",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v3 JSON",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--production-candidate-metric-gates",
        required=True,
        help="Path to ml-offline-production-candidate-metric-gates-v3 JSON",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--holdout-assignment",
        required=True,
        help="Path to ml-learned-scorer-holdout-assignment-v1 JSON",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--split-policy",
        required=True,
        help="Path to ml-label-split-policy-v1 JSON",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--label-dataset",
        default=None,
        help="Optional ml-label-dataset-v8 JSON provenance/inventory input",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--holdout-policy",
        default=None,
        help="Optional ml-learned-scorer-holdout-policy-v1 JSON provenance input",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--audit-embedding-scorer-export",
        default=None,
        help="Optional ml-offline-audit-embedding-scorer-v2 JSON provenance input",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--output",
        required=True,
        help="Path to write hybrid scorer offline experiment spec JSON",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--spec-version",
        default="ml-hybrid-scorer-offline-experiment-v1-spec",
        help="Spec version string to write (default: ml-hybrid-scorer-offline-experiment-v1-spec)",
    )
    ml_hybrid_scorer_offline_experiment_spec_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_hybrid_scorer_offline_experiment_parser = subparsers.add_parser(
        "ml-hybrid-scorer-offline-experiment",
        help="Execute pre-registered hybrid scorer offline experiment from scoring v3 JSON only",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--production-candidate-scoring",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v3 JSON",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--production-candidate-metric-gates",
        required=True,
        help="Path to ml-offline-production-candidate-metric-gates-v3 JSON",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--experiment-spec",
        required=True,
        help="Path to ml-hybrid-scorer-offline-experiment-v1-spec JSON",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--holdout-assignment",
        required=True,
        help="Path to ml-learned-scorer-holdout-assignment-v1 JSON",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--holdout-policy",
        default=None,
        help="Optional ml-learned-scorer-holdout-policy-v1 JSON provenance input",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--output",
        required=True,
        help="Path to write hybrid scorer offline experiment JSON",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--experiment-version",
        default="ml-hybrid-scorer-offline-experiment-v1",
        help="Experiment version string to write (default: ml-hybrid-scorer-offline-experiment-v1)",
    )
    ml_hybrid_scorer_offline_experiment_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_hybrid_scorer_metric_gates_parser = subparsers.add_parser(
        "ml-hybrid-scorer-metric-gates",
        help="Evaluate hybrid scorer offline experiment metric gates (JSON-only; no training, DB, shadow, or prod)",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--hybrid-experiment",
        required=True,
        help="Path to ml-hybrid-scorer-offline-experiment-v1 JSON",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--experiment-spec",
        required=True,
        help="Path to ml-hybrid-scorer-offline-experiment-v1-spec JSON",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--production-candidate-metric-gates",
        required=True,
        help="Path to ml-offline-production-candidate-metric-gates-v3 JSON",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--holdout-assignment",
        default=None,
        help="Optional ml-learned-scorer-holdout-assignment-v1 JSON provenance/eval-SHA check",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--output",
        required=True,
        help="Path to write hybrid scorer metric gates JSON",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--gates-version",
        default="ml-hybrid-scorer-metric-gates-v1",
        help="Gates version string to write (default: ml-hybrid-scorer-metric-gates-v1)",
    )
    ml_hybrid_scorer_metric_gates_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_fresh_eval_surface_policy_hybrid_parser = subparsers.add_parser(
        "ml-fresh-eval-surface-policy-hybrid",
        help="Write fresh eval surface policy for hybrid validation (policy only; no DB, scoring, training, or shadow)",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--hybrid-metric-gates",
        required=True,
        help="Path to ml-hybrid-scorer-metric-gates-v1 JSON",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--hybrid-experiment",
        required=True,
        help="Path to ml-hybrid-scorer-offline-experiment-v1 JSON",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--hybrid-experiment-spec",
        required=True,
        help="Path to ml-hybrid-scorer-offline-experiment-v1-spec JSON",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--production-candidate-scoring",
        required=True,
        help="Path to ml-offline-production-candidate-scoring-v3 JSON",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--holdout-assignment",
        required=True,
        help="Path to ml-learned-scorer-holdout-assignment-v1 JSON",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh eval surface policy JSON",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--policy-version",
        default="ml-fresh-eval-surface-policy-hybrid-v1",
        help="Policy version string to write (default: ml-fresh-eval-surface-policy-hybrid-v1)",
    )
    ml_fresh_eval_surface_policy_hybrid_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_transfer_gap_review_parser = subparsers.add_parser(
        "ml-transfer-gap-review-worksheet",
        help="Write transfer-gap manual review CSV + sidecar (no training, ranking, ingest, or splits)",
    )
    ml_transfer_gap_review_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_transfer_gap_review_parser.add_argument("--label-dataset", required=True, help="Path to ml-label-dataset-v7 JSON")
    ml_transfer_gap_review_parser.add_argument("--conflict-policy", required=True, help="Path to conflict policy Markdown")
    ml_transfer_gap_review_parser.add_argument("--output", required=True, help="Path to write reviewer CSV")
    ml_transfer_gap_review_parser.add_argument("--context-output", required=True, help="Path to write row_id-keyed sidecar JSON")
    ml_transfer_gap_review_parser.add_argument("--markdown-output", required=True, help="Path to write Markdown handoff")
    ml_transfer_gap_review_parser.add_argument("--rows", type=int, default=60, help="Requested rows, 1-120 (default: 60)")
    ml_transfer_gap_review_parser.add_argument("--seed", type=int, default=20260515, help="Deterministic sample seed")
    ml_transfer_gap_review_parser.add_argument(
        "--source-snapshot-candidate-plan",
        default=None,
        help="Optional corpus-v2 candidate plan manifest for outside-snapshot exclusion",
    )
    ml_transfer_gap_review_parser.add_argument(
        "--corpus-snapshot-version",
        required=True,
        help="Corpus snapshot version for external exclusion and optional DB channel",
    )
    ml_transfer_gap_review_parser.add_argument("--mailto", default=None, help="Optional OpenAlex polite-pool mailto")
    ml_transfer_gap_review_parser.add_argument(
        "--mock-openalex",
        action="store_true",
        help="Use deterministic mock OpenAlex responses for tests/dry runs",
    )
    ml_transfer_gap_review_parser.add_argument("--ranking-run-id", default=None, help="Optional ranking run id for P3 DB channel")
    ml_transfer_gap_review_parser.add_argument("--embedding-version", default=None, help="Optional embedding version for P3 DB channel")
    ml_transfer_gap_review_parser.add_argument("--cluster-version", default=None, help="Optional cluster version for P3 DB channel")
    ml_transfer_gap_review_parser.add_argument(
        "--database-url",
        default=None,
        help="Optional Postgres URL for read-only P3 DB channel (default: DATABASE_URL/PG env if set)",
    )
    ml_transfer_gap_review_parser.add_argument(
        "--mock-db",
        action="store_true",
        help="Use deterministic mock DB candidates for tests/dry runs",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser = subparsers.add_parser(
        "ml-fresh-eval-surface-hybrid-materialize",
        help="Materialize a read-only fresh product-candidate eval surface inventory for hybrid validation",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v8 JSON",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to ml-label-conflict-policy Markdown",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--ranking-run-id",
        default=None,
        help="Optional existing ranking_run_id to evaluate; omitted means deterministic discovery",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--family",
        default="emerging",
        help="Product-candidate family to materialize (default: emerging)",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--corpus-snapshot-version",
        default=None,
        help="Optional corpus snapshot version that the selected candidate source must match/report",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL (default: DATABASE_URL or PG* env); hosted production URLs are refused",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh eval surface materialization JSON",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--surface-version",
        default="ml-fresh-eval-surface-hybrid-v1",
        help="Surface version string to write (default: ml-fresh-eval-surface-hybrid-v1)",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
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

    args = parser.parse_args()

    if args.command == "ml-blind-snapshot-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_blind_snapshot_review_worksheet import (
            MAX_ROWS as _BLIND_MAX_ROWS,
            MIN_ROWS as _BLIND_MIN_ROWS,
            MLBlindSnapshotReviewWorksheetError,
            run_ml_blind_snapshot_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version is required and must be non-empty")
        clv = (args.cluster_version or "").strip()
        if not clv:
            parser.error("--cluster-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _BLIND_MIN_ROWS or nrows > _BLIND_MAX_ROWS:
            parser.error(f"--rows must be between {_BLIND_MIN_ROWS} and {_BLIND_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_blind_snapshot_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                corpus_snapshot_version=snap,
                embedding_version=emb,
                cluster_version=clv,
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                markdown_output_path=out_md,
            )
        except MLBlindSnapshotReviewWorksheetError as e:
            print(f"ml-blind-snapshot-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-blind-snapshot-review-worksheet-v2":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_blind_snapshot_review_worksheet import (
            MAX_ROWS as _BLIND_MAX_ROWS,
            MIN_ROWS as _BLIND_MIN_ROWS,
            MLBlindSnapshotReviewWorksheetError,
        )
        from pipeline.ml_blind_snapshot_review_worksheet_v2 import run_ml_blind_snapshot_review_worksheet_v2_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version is required and must be non-empty")
        clv = (args.cluster_version or "").strip()
        if not clv:
            parser.error("--cluster-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _BLIND_MIN_ROWS or nrows > _BLIND_MAX_ROWS:
            parser.error(f"--rows must be between {_BLIND_MIN_ROWS} and {_BLIND_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_blind_snapshot_review_worksheet_v2_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                corpus_snapshot_version=snap,
                embedding_version=emb,
                cluster_version=clv,
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
            )
        except MLBlindSnapshotReviewWorksheetError as e:
            print(f"ml-blind-snapshot-review-worksheet-v2: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-hard-negative-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_hard_negative_review_worksheet import (
            MAX_ROWS as _HARD_NEG_MAX_ROWS,
            MIN_ROWS as _HARD_NEG_MIN_ROWS,
            MLHardNegativeReviewWorksheetError,
            run_ml_hard_negative_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version is required and must be non-empty")
        clv = (args.cluster_version or "").strip()
        if not clv:
            parser.error("--cluster-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _HARD_NEG_MIN_ROWS or nrows > _HARD_NEG_MAX_ROWS:
            parser.error(f"--rows must be between {_HARD_NEG_MIN_ROWS} and {_HARD_NEG_MAX_ROWS}")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_hard_negative_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                corpus_snapshot_version=snap,
                embedding_version=emb,
                cluster_version=clv,
                ranking_run_id=rid,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
            )
        except MLHardNegativeReviewWorksheetError as e:
            print(f"ml-hard-negative-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-external-near-miss-review-worksheet":
        from pipeline.ml_external_near_miss_review_worksheet import (
            MAX_ROWS as _EXT_MAX_ROWS,
            MIN_ROWS as _EXT_MIN_ROWS,
            MLExternalNearMissReviewWorksheetError,
            run_ml_external_near_miss_review_worksheet_cli,
        )

        snap = (args.corpus_snapshot_version or "").strip()
        if not snap:
            parser.error("--corpus-snapshot-version is required and must be non-empty")
        nrows = int(args.rows)
        if nrows < _EXT_MIN_ROWS or nrows > _EXT_MAX_ROWS:
            parser.error(f"--rows must be between {_EXT_MIN_ROWS} and {_EXT_MAX_ROWS}")
        candidate_plan = Path(args.source_snapshot_candidate_plan) if args.source_snapshot_candidate_plan else None
        out_csv = Path(args.output)
        out_ctx = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            debug = run_ml_external_near_miss_review_worksheet_cli(
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                corpus_snapshot_version=snap,
                candidate_plan_path=candidate_plan,
                rows=nrows,
                seed=int(args.seed),
                csv_output_path=out_csv,
                context_output_path=out_ctx,
                markdown_output_path=out_md,
                mailto=args.mailto,
            )
        except MLExternalNearMissReviewWorksheetError as e:
            print(f"ml-external-near-miss-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_ctx.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(int(debug.get("achieved_rows", 0)))
        return

    if args.command == "ml-targeted-gap-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_targeted_gap_review_worksheet import (
            MLTargetedGapReviewWorksheetError,
            run_ml_targeted_gap_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        lim = int(args.limit)
        if lim < 1 or lim > 200:
            parser.error("--limit must be between 1 and 200")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        try:
            run_ml_targeted_gap_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                target_gap=str(args.target_gap),
                output_csv=Path(args.output),
                markdown_output=Path(args.markdown_output),
                limit=lim,
            )
        except MLTargetedGapReviewWorksheetError as e:
            print(f"ml-targeted-gap-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "ml-contrastive-review-worksheet":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_contrastive_review_worksheet import (
            MLContrastiveReviewWorksheetError,
            run_ml_contrastive_review_worksheet_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        pf = int(args.per_family)
        if pf < 1 or pf > 200:
            parser.error("--per-family must be between 1 and 200")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        label_path = Path(args.label_dataset)
        try:
            run_ml_contrastive_review_worksheet_cli(
                database_url=dsn,
                label_dataset_path=label_path,
                ranking_run_id=rid,
                output_csv=Path(args.output),
                markdown_output=Path(args.markdown_output),
                per_family=pf,
            )
        except MLContrastiveReviewWorksheetError as e:
            print(f"ml-contrastive-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-readiness-matrix":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_label_readiness_matrix import MLLabelReadinessMatrixError, run_ml_label_readiness_matrix_cli

        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_label_readiness_matrix_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLLabelReadinessMatrixError as e:
            print(f"ml-label-readiness-matrix: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-blind-family-context-eval":
        from pipeline.ml_blind_family_context_eval import (
            MLBlindFamilyContextEvalError,
            run_ml_blind_family_context_eval_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_blind_family_context_eval_cli(
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLBlindFamilyContextEvalError as e:
            print(f"ml-blind-family-context-eval: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-offline-baseline-eval":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_baseline_eval import MLOfflineBaselineEvalError, run_ml_offline_baseline_eval_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_offline_baseline_eval_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLOfflineBaselineEvalError as e:
            print(f"ml-offline-baseline-eval: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-external-feature-coverage":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_external_feature_coverage import (
            MLExternalFeatureCoverageError,
            run_ml_external_feature_coverage_cli,
        )

        emb = (args.embedding_version or "").strip()
        if not emb:
            parser.error("--embedding-version must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        sidecar = Path(args.context_sidecar) if args.context_sidecar else None
        try:
            run_ml_external_feature_coverage_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                context_sidecar_path=sidecar,
                embedding_version=emb,
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLExternalFeatureCoverageError as e:
            print(f"ml-external-feature-coverage: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-external-text-corpus":
        from pipeline.ml_external_text_corpus import (
            MLExternalTextCorpusError,
            run_ml_external_text_corpus_cli,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        sidecar = Path(args.context_sidecar) if args.context_sidecar else None
        try:
            run_ml_external_text_corpus_cli(
                label_dataset_path=Path(args.label_dataset),
                context_sidecar_path=sidecar,
                output_json=out_json,
                markdown_output=out_md,
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
            )
        except MLExternalTextCorpusError as e:
            print(f"ml-external-text-corpus: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-labeled-text-corpus":
        from pipeline.ml_labeled_text_corpus import (
            MLLabeledTextCorpusError,
            write_ml_labeled_text_corpus,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        external = Path(args.external_text_corpus) if args.external_text_corpus else None
        try:
            write_ml_labeled_text_corpus(
                label_dataset_path=Path(args.label_dataset),
                external_text_corpus_path=external,
                output_path=out_json,
                markdown_output_path=out_md,
                corpus_version=str(args.corpus_version),
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                max_rows=args.max_rows,
            )
        except MLLabeledTextCorpusError as e:
            print(f"ml-labeled-text-corpus: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-labeled-text-corpus-normalize":
        from pipeline.ml_labeled_text_corpus_normalize import (
            MLLabeledTextCorpusNormalizeError,
            write_ml_labeled_text_corpus_normalize,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_labeled_text_corpus_normalize(
                source_corpus_path=Path(args.source_corpus),
                output_path=out_json,
                markdown_output_path=out_md,
                source_corpus_version=str(args.source_corpus_version),
                corpus_version=str(args.corpus_version),
            )
        except MLLabeledTextCorpusNormalizeError as e:
            print(f"ml-labeled-text-corpus-normalize: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-external-text-embeddings":
        from pipeline.ml_external_text_embeddings import (
            MLExternalTextEmbeddingsError,
            write_external_text_embeddings,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_external_text_embeddings(
                text_corpus_path=Path(args.text_corpus),
                output_path=out_json,
                markdown_output_path=out_md,
                embedding_model=str(args.embedding_model),
                expected_dimensions=int(args.expected_dimensions),
                batch_size=int(args.batch_size),
                mock_embeddings=bool(args.mock_embeddings),
            )
        except MLExternalTextEmbeddingsError as e:
            print(f"ml-external-text-embeddings: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-text-only-baseline":
        from pipeline.ml_text_only_baseline import (
            MLTextOnlyBaselineError,
            write_ml_text_only_baseline,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_text_only_baseline(
                embeddings_path=Path(args.embeddings),
                label_dataset_path=Path(args.label_dataset),
                output_path=out_json,
                markdown_output_path=out_md,
                random_seed=int(args.random_seed),
                cv_folds=int(args.cv_folds),
            )
        except MLTextOnlyBaselineError as e:
            print(f"ml-text-only-baseline: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-labeled-text-embeddings":
        from pipeline.ml_labeled_text_embeddings import (
            MLLabeledTextEmbeddingsError,
            write_ml_labeled_text_embeddings,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_labeled_text_embeddings(
                text_corpus_path=Path(args.text_corpus),
                output_path=out_json,
                markdown_output_path=out_md,
                source_corpus_version=str(args.source_corpus_version),
                embedding_artifact_version=str(args.embedding_artifact_version),
                embedding_model=str(args.embedding_model),
                expected_dimensions=int(args.expected_dimensions),
                batch_size=int(args.batch_size),
                mock_embeddings=bool(args.mock_embeddings),
            )
        except MLLabeledTextEmbeddingsError as e:
            print(f"ml-labeled-text-embeddings: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-text-baseline-cross-pool":
        from pipeline.ml_text_baseline_cross_pool import (
            MLTextBaselineCrossPoolError,
            write_ml_text_baseline_cross_pool,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            write_ml_text_baseline_cross_pool(
                embeddings_path=Path(args.embeddings),
                label_dataset_path=Path(args.label_dataset),
                output_path=out_json,
                markdown_output_path=out_md,
                expected_embedding_artifact_version=str(args.expected_embedding_artifact_version),
                expected_label_dataset_version=str(args.expected_label_dataset_version),
                baseline_version=str(args.baseline_version),
                random_seed=int(args.random_seed),
            )
        except MLTextBaselineCrossPoolError as e:
            print(f"ml-text-baseline-cross-pool: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-text-transfer-readiness":
        from pipeline.ml_text_transfer_readiness import (
            MLTextTransferReadinessError,
            write_ml_text_transfer_readiness,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        text_corpus_v2 = Path(args.text_corpus_v2) if args.text_corpus_v2 else None
        embeddings_v1 = Path(args.embeddings_v1) if args.embeddings_v1 else None
        try:
            write_ml_text_transfer_readiness(
                cross_pool_path=Path(args.cross_pool),
                label_dataset_path=Path(args.label_dataset),
                text_corpus_v2_path=text_corpus_v2,
                embeddings_v1_path=embeddings_v1,
                output_path=out_json,
                markdown_output_path=out_md,
                expected_cross_pool_version=str(args.expected_cross_pool_version),
                expected_label_dataset_version=str(args.expected_label_dataset_version),
                expected_text_corpus_version=str(args.expected_text_corpus_version),
                expected_embeddings_version=str(args.expected_embeddings_version),
                readiness_version=str(args.readiness_version),
            )
        except MLTextTransferReadinessError as e:
            print(f"ml-text-transfer-readiness: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-production-readiness-plan":
        from pipeline.ml_production_readiness_plan import (
            MLProductionReadinessPlanError,
            write_ml_production_readiness_plan,
        )

        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        conflict_policy = Path(args.conflict_policy) if args.conflict_policy else None
        cross_pool = Path(args.cross_pool) if args.cross_pool else None
        text_corpus_v2 = Path(args.text_corpus_v2) if args.text_corpus_v2 else None
        embeddings_v1 = Path(args.embeddings_v1) if args.embeddings_v1 else None
        try:
            write_ml_production_readiness_plan(
                transfer_readiness_path=Path(args.transfer_readiness),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=conflict_policy,
                cross_pool_path=cross_pool,
                text_corpus_v2_path=text_corpus_v2,
                embeddings_v1_path=embeddings_v1,
                output_path=out_json,
                markdown_output_path=out_md,
            )
        except MLProductionReadinessPlanError as e:
            print(f"ml-production-readiness-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-split-policy":
        from pipeline.ml_label_split_policy import MLLabelSplitPolicyError, write_ml_label_split_policy

        transfer_readiness = Path(args.transfer_readiness) if args.transfer_readiness else None
        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_label_split_policy(
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                transfer_readiness_path=transfer_readiness,
                output_path=out_json,
                markdown_output_path=out_md,
                policy_version=str(args.policy_version),
                repo_root=repo_root,
            )
        except MLLabelSplitPolicyError as e:
            print(f"ml-label-split-policy: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-offline-ranker-experiment":
        from pipeline.ml_offline_ranker_experiment import (
            MLOfflineRankerExperimentError,
            write_ml_offline_ranker_experiment,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_offline_ranker_experiment(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                output_path=out_json,
                markdown_output_path=out_md,
                target=str(args.target),
                random_seed=args.random_seed,
                cv_folds=int(args.cv_folds),
                repo_root=repo_root,
            )
        except MLOfflineRankerExperimentError as e:
            print(f"ml-offline-ranker-experiment: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-offline-metric-gates":
        from pipeline.ml_offline_metric_gates import MLOfflineMetricGatesError, write_ml_offline_metric_gates

        repo_root = Path(args.repo_root) if args.repo_root else None
        transfer_readiness = Path(args.transfer_readiness) if args.transfer_readiness else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_offline_metric_gates(
                ranker_experiment_path=Path(args.ranker_experiment),
                split_policy_path=Path(args.split_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                transfer_readiness_path=transfer_readiness,
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                repo_root=repo_root,
            )
        except MLOfflineMetricGatesError as e:
            print(f"ml-offline-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-offline-production-candidate-scoring":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_offline_production_candidate_scoring import (
            MLOfflineProductionCandidateScoringError,
            run_ml_offline_production_candidate_scoring_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        repo_root = Path(args.repo_root) if args.repo_root else None
        audit_embedding_scorer_export = (
            Path(args.audit_embedding_scorer_export) if args.audit_embedding_scorer_export else None
        )
        holdout_assignment = Path(args.holdout_assignment) if args.holdout_assignment else None
        holdout_policy = Path(args.holdout_policy) if args.holdout_policy else None
        gates_v2 = Path(args.production_candidate_metric_gates_v2) if args.production_candidate_metric_gates_v2 else None
        if args.scoring_mode == "heuristic_and_audit_embedding_scorer" and audit_embedding_scorer_export is None:
            parser.error("--audit-embedding-scorer-export is required when --scoring-mode heuristic_and_audit_embedding_scorer")
        if args.scoring_mode == "heuristic_and_holdout_embedding_scorer":
            if audit_embedding_scorer_export is None:
                parser.error("--audit-embedding-scorer-export is required when --scoring-mode heuristic_and_holdout_embedding_scorer")
            if holdout_assignment is None:
                parser.error("--holdout-assignment is required when --scoring-mode heuristic_and_holdout_embedding_scorer")
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            run_ml_offline_production_candidate_scoring_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                metric_gates_path=Path(args.metric_gates),
                audit_ranker_experiment_path=Path(args.audit_ranker_experiment),
                embeddings_path=Path(args.embeddings),
                ranking_run_id=rid,
                family=str(args.family),
                target=str(args.target),
                output_path=out_json,
                markdown_output_path=out_md,
                experiment_version=str(args.experiment_version) if args.experiment_version else None,
                scoring_mode=str(args.scoring_mode),
                audit_embedding_scorer_export_path=audit_embedding_scorer_export,
                holdout_assignment_path=holdout_assignment,
                holdout_policy_path=holdout_policy,
                production_candidate_metric_gates_v2_path=gates_v2,
                repo_root=repo_root,
            )
        except MLOfflineProductionCandidateScoringError as e:
            print(f"ml-offline-production-candidate-scoring: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-offline-production-candidate-metric-gates":
        from pipeline.ml_offline_production_candidate_metric_gates import (
            MLOfflineProductionCandidateMetricGatesError,
            write_ml_offline_production_candidate_metric_gates,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        audit_scorer_export = (
            Path(args.audit_embedding_scorer_export) if args.audit_embedding_scorer_export else None
        )
        holdout_assignment = Path(args.holdout_assignment) if args.holdout_assignment else None
        holdout_policy = Path(args.holdout_policy) if args.holdout_policy else None
        prior_v1_gates = (
            Path(args.production_candidate_metric_gates_v1)
            if args.production_candidate_metric_gates_v1
            else None
        )
        prior_v2_gates = (
            Path(args.production_candidate_metric_gates_v2)
            if args.production_candidate_metric_gates_v2
            else None
        )
        if (
            str(args.gates_version) == "ml-offline-production-candidate-metric-gates-v2"
            and audit_scorer_export is None
        ):
            parser.error(
                "--audit-embedding-scorer-export is required when --gates-version "
                "ml-offline-production-candidate-metric-gates-v2"
            )
        if str(args.gates_version) == "ml-offline-production-candidate-metric-gates-v3":
            if audit_scorer_export is None:
                parser.error(
                    "--audit-embedding-scorer-export is required when --gates-version "
                    "ml-offline-production-candidate-metric-gates-v3"
                )
            if holdout_assignment is None:
                parser.error(
                    "--holdout-assignment is required when --gates-version "
                    "ml-offline-production-candidate-metric-gates-v3"
                )
            if holdout_policy is None:
                parser.error(
                    "--holdout-policy is required when --gates-version "
                    "ml-offline-production-candidate-metric-gates-v3"
                )
        try:
            write_ml_offline_production_candidate_metric_gates(
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                offline_metric_gates_path=Path(args.offline_metric_gates),
                split_policy_path=Path(args.split_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                audit_embedding_scorer_export_path=audit_scorer_export,
                production_candidate_metric_gates_v1_path=prior_v1_gates,
                production_candidate_metric_gates_v2_path=prior_v2_gates,
                holdout_assignment_path=holdout_assignment,
                holdout_policy_path=holdout_policy,
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                repo_root=repo_root,
            )
        except MLOfflineProductionCandidateMetricGatesError as e:
            print(f"ml-offline-production-candidate-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-offline-audit-embedding-scorer-export":
        from pipeline.ml_offline_audit_embedding_scorer_export import (
            MLOfflineAuditEmbeddingScorerExportError,
            write_ml_offline_audit_embedding_scorer_export,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        ranker_experiment = Path(args.ranker_experiment) if args.ranker_experiment else None
        fit_mode = str(args.fit_mode)
        production_candidate_metric_gates = (
            Path(args.production_candidate_metric_gates) if args.production_candidate_metric_gates else None
        )
        holdout_assignment = Path(args.holdout_assignment) if args.holdout_assignment else None
        holdout_policy = Path(args.holdout_policy) if args.holdout_policy else None
        v1_reference = (
            Path(args.audit_embedding_scorer_export_v1) if args.audit_embedding_scorer_export_v1 else None
        )
        if fit_mode == "full_fit_audit_corpus" and production_candidate_metric_gates is None:
            parser.error(
                "ml-offline-audit-embedding-scorer-export: --production-candidate-metric-gates is required "
                "when --fit-mode full_fit_audit_corpus"
            )
        if fit_mode == "holdout_bound_train_only" and (holdout_assignment is None or holdout_policy is None):
            parser.error(
                "ml-offline-audit-embedding-scorer-export: --holdout-assignment and --holdout-policy are required "
                "when --fit-mode holdout_bound_train_only"
            )
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_offline_audit_embedding_scorer_export(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                production_candidate_metric_gates_path=production_candidate_metric_gates,
                holdout_assignment_path=holdout_assignment,
                holdout_policy_path=holdout_policy,
                audit_embedding_scorer_export_v1_path=v1_reference,
                ranker_experiment_path=ranker_experiment,
                output_path=out_json,
                markdown_output_path=out_md,
                target=str(args.target),
                random_seed=args.random_seed,
                scorer_version=str(args.scorer_version) if args.scorer_version else None,
                fit_mode=fit_mode,
                repo_root=repo_root,
            )
        except MLOfflineAuditEmbeddingScorerExportError as e:
            print(f"ml-offline-audit-embedding-scorer-export: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-learned-scorer-holdout-policy":
        from pipeline.ml_learned_scorer_holdout_policy import (
            MLLearnedScorerHoldoutPolicyError,
            write_ml_learned_scorer_holdout_policy,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_learned_scorer_holdout_policy(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                output_path=out_json,
                markdown_output_path=out_md,
                policy_version=str(args.policy_version),
                repo_root=repo_root,
            )
        except MLLearnedScorerHoldoutPolicyError as e:
            print(f"ml-learned-scorer-holdout-policy: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-learned-scorer-holdout-assignment":
        from pipeline.ml_learned_scorer_holdout_assignment import (
            MLLearnedScorerHoldoutAssignmentError,
            write_ml_learned_scorer_holdout_assignment,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_learned_scorer_holdout_assignment(
                label_dataset_path=Path(args.label_dataset),
                split_policy_path=Path(args.split_policy),
                embeddings_path=Path(args.embeddings),
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                holdout_policy_path=Path(args.holdout_policy),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                output_path=out_json,
                markdown_output_path=out_md,
                assignment_version=str(args.assignment_version),
                strategy_id=str(args.strategy_id),
                seed=args.seed,
                repo_root=repo_root,
            )
        except MLLearnedScorerHoldoutAssignmentError as e:
            print(f"ml-learned-scorer-holdout-assignment: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-hybrid-scorer-offline-experiment-spec":
        from pipeline.ml_hybrid_scorer_offline_experiment_spec import (
            MLHybridScorerOfflineExperimentSpecError,
            write_ml_hybrid_scorer_offline_experiment_spec,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_hybrid_scorer_offline_experiment_spec(
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                holdout_assignment_path=Path(args.holdout_assignment),
                split_policy_path=Path(args.split_policy),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                label_dataset_path=Path(args.label_dataset) if args.label_dataset else None,
                holdout_policy_path=Path(args.holdout_policy) if args.holdout_policy else None,
                audit_embedding_scorer_export_path=(
                    Path(args.audit_embedding_scorer_export) if args.audit_embedding_scorer_export else None
                ),
                output_path=out_json,
                markdown_output_path=out_md,
                spec_version=str(args.spec_version),
                repo_root=repo_root,
            )
        except MLHybridScorerOfflineExperimentSpecError as e:
            print(f"ml-hybrid-scorer-offline-experiment-spec: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-hybrid-scorer-offline-experiment":
        from pipeline.ml_hybrid_scorer_offline_experiment import (
            MLHybridScorerOfflineExperimentError,
            write_ml_hybrid_scorer_offline_experiment,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_hybrid_scorer_offline_experiment(
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                experiment_spec_path=Path(args.experiment_spec),
                holdout_assignment_path=Path(args.holdout_assignment),
                holdout_policy_path=Path(args.holdout_policy) if args.holdout_policy else None,
                output_path=out_json,
                markdown_output_path=out_md,
                experiment_version=str(args.experiment_version),
                repo_root=repo_root,
            )
        except MLHybridScorerOfflineExperimentError as e:
            print(f"ml-hybrid-scorer-offline-experiment: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-hybrid-scorer-metric-gates":
        from pipeline.ml_hybrid_scorer_metric_gates import (
            MLHybridScorerMetricGatesError,
            write_ml_hybrid_scorer_metric_gates,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_hybrid_scorer_metric_gates(
                hybrid_experiment_path=Path(args.hybrid_experiment),
                experiment_spec_path=Path(args.experiment_spec),
                production_candidate_metric_gates_path=Path(args.production_candidate_metric_gates),
                production_readiness_plan_path=Path(args.production_readiness_plan),
                holdout_assignment_path=Path(args.holdout_assignment) if args.holdout_assignment else None,
                output_path=out_json,
                markdown_output_path=out_md,
                gates_version=str(args.gates_version),
                repo_root=repo_root,
            )
        except MLHybridScorerMetricGatesError as e:
            print(f"ml-hybrid-scorer-metric-gates: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-eval-surface-policy-hybrid":
        from pipeline.ml_fresh_eval_surface_policy_hybrid import (
            MLFreshEvalSurfacePolicyHybridError,
            write_ml_fresh_eval_surface_policy_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_eval_surface_policy_hybrid(
                hybrid_metric_gates_path=Path(args.hybrid_metric_gates),
                hybrid_experiment_path=Path(args.hybrid_experiment),
                hybrid_experiment_spec_path=Path(args.hybrid_experiment_spec),
                production_candidate_scoring_path=Path(args.production_candidate_scoring),
                holdout_assignment_path=Path(args.holdout_assignment),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                policy_version=str(args.policy_version),
                repo_root=repo_root,
            )
        except MLFreshEvalSurfacePolicyHybridError as e:
            print(f"ml-fresh-eval-surface-policy-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-eval-surface-hybrid-materialize":
        from pipeline.ml_fresh_eval_surface_hybrid_materialize import (
            MLFreshEvalSurfaceHybridMaterializeError,
            write_ml_fresh_eval_surface_hybrid_materialize,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_eval_surface_hybrid_materialize(
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                ranking_run_id=args.ranking_run_id,
                family=str(args.family),
                corpus_snapshot_version=args.corpus_snapshot_version,
                database_url=args.database_url,
                surface_version=str(args.surface_version),
                repo_root=repo_root,
            )
        except MLFreshEvalSurfaceHybridMaterializeError as e:
            print(f"ml-fresh-eval-surface-hybrid-materialize: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-eval-labeling-plan-hybrid":
        from pipeline.ml_fresh_eval_labeling_plan_hybrid import (
            MLFreshEvalLabelingPlanHybridError,
            write_ml_fresh_eval_labeling_plan_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_eval_labeling_plan_hybrid(
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLFreshEvalLabelingPlanHybridError as e:
            print(f"ml-fresh-eval-labeling-plan-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-eval-labeling-worksheet-hybrid":
        from pipeline.ml_fresh_eval_labeling_worksheet_hybrid import (
            MLFreshEvalLabelingWorksheetHybridError,
            write_ml_fresh_eval_labeling_worksheet_hybrid,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_csv = Path(args.output)
        out_context = Path(args.context_output)
        out_md = Path(args.markdown_output)
        try:
            payload = write_ml_fresh_eval_labeling_worksheet_hybrid(
                fresh_eval_surface_path=Path(args.fresh_eval_surface),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_csv,
                context_output_path=out_context,
                markdown_output_path=out_md,
                rows=int(args.rows),
                seed=int(args.seed),
                worksheet_version=str(args.worksheet_version),
                review_pool_variant=str(args.review_pool_variant),
                database_url=args.database_url,
                repo_root=repo_root,
            )
        except MLFreshEvalLabelingWorksheetHybridError as e:
            print(f"ml-fresh-eval-labeling-worksheet-hybrid: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_csv.resolve(), file=sys.stderr)
        print(out_context.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        print(f"achieved_rows={payload['metadata']['achieved_rows']}", file=sys.stderr)
        return

    if args.command == "ml-fresh-candidate-source-expansion-plan":
        from pipeline.ml_fresh_candidate_source_expansion_plan import (
            MLFreshCandidateSourceExpansionPlanError,
            write_ml_fresh_candidate_source_expansion_plan,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_candidate_source_expansion_plan(
                fresh_product_candidate_ranking_source_path=Path(args.fresh_product_candidate_ranking_source),
                fresh_eval_labeling_plan_path=Path(args.fresh_eval_labeling_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                repo_root=repo_root,
            )
        except MLFreshCandidateSourceExpansionPlanError as e:
            print(f"ml-fresh-candidate-source-expansion-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-product-candidate-source-build":
        from pipeline.ml_fresh_product_candidate_source_build import (
            MLFreshProductCandidateSourceBuildError,
            write_ml_fresh_product_candidate_source_build,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_product_candidate_source_build(
                fresh_candidate_source_expansion_plan_path=Path(args.fresh_candidate_source_expansion_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                database_url=args.database_url,
                family=str(args.family),
                min_confirmatory_eligible_works=args.min_confirmatory_eligible_works,
                mode=str(args.mode),
                write_eval_db_source=bool(args.write_eval_db_source),
                build_version=str(args.build_version),
                repo_root=repo_root,
            )
        except MLFreshProductCandidateSourceBuildError as e:
            print(f"ml-fresh-product-candidate-source-build: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-hybrid-corpus-candidate-plan":
        from pipeline.ml_fresh_hybrid_corpus_candidate_plan import (
            MLFreshHybridCorpusCandidatePlanError,
            write_ml_fresh_hybrid_corpus_candidate_plan,
        )

        if args.target_max < args.target_min:
            parser.error("--target-max must be >= --target-min")
        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_hybrid_corpus_candidate_plan(
                fresh_product_candidate_source_build_path=Path(args.fresh_product_candidate_source_build),
                fresh_candidate_source_expansion_plan_path=Path(args.fresh_candidate_source_expansion_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                plan_version=str(args.plan_version),
                target_min=int(args.target_min),
                target_max=int(args.target_max),
                mailto=args.mailto,
                repo_root=repo_root,
            )
        except MLFreshHybridCorpusCandidatePlanError as e:
            print(f"ml-fresh-hybrid-corpus-candidate-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-hybrid-candidate-plan-ingest":
        from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
            MLFreshHybridCandidatePlanIngestError,
            write_ml_fresh_hybrid_candidate_plan_ingest,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_hybrid_candidate_plan_ingest(
                fresh_hybrid_corpus_candidate_plan_path=Path(args.fresh_hybrid_corpus_candidate_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                snapshot_version=args.snapshot_version,
                database_url=args.database_url,
                ingest_version=str(args.ingest_version),
                dry_run=bool(args.dry_run),
                repo_root=repo_root,
            )
        except MLFreshHybridCandidatePlanIngestError as e:
            print(f"ml-fresh-hybrid-candidate-plan-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-transfer-gap-review-worksheet":
        from pipeline.ml_transfer_gap_review_worksheet import (
            MLTransferGapReviewWorksheetError,
            run_ml_transfer_gap_review_worksheet_cli,
        )

        source_plan = Path(args.source_snapshot_candidate_plan) if args.source_snapshot_candidate_plan else None
        try:
            run_ml_transfer_gap_review_worksheet_cli(
                production_readiness_plan_path=Path(args.production_readiness_plan),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=Path(args.output),
                context_output_path=Path(args.context_output),
                markdown_output_path=Path(args.markdown_output),
                rows=int(args.rows),
                seed=int(args.seed),
                source_snapshot_candidate_plan_path=source_plan,
                corpus_snapshot_version=str(args.corpus_snapshot_version),
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                ranking_run_id=args.ranking_run_id,
                embedding_version=args.embedding_version,
                cluster_version=args.cluster_version,
                database_url=args.database_url,
                mock_db=bool(args.mock_db),
            )
        except MLTransferGapReviewWorksheetError as e:
            print(f"ml-transfer-gap-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.context_output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "ml-tiny-baseline":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_tiny_baseline import MLTinyBaselineError, run_ml_tiny_baseline_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_tiny_baseline_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                target=str(args.target),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLTinyBaselineError as e:
            print(f"ml-tiny-baseline: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-source-split-tiny-baseline":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_source_split_tiny_baseline import (
            MLSourceSplitTinyBaselineError,
            run_ml_source_split_tiny_baseline_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_source_split_tiny_baseline_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                ranking_run_id=rid,
                family=str(args.family),
                output_json=out_json,
                markdown_output=out_md,
            )
        except MLSourceSplitTinyBaselineError as e:
            print(f"ml-source-split-tiny-baseline: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-source-split-error-analysis":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_source_split_error_analysis import (
            MLSourceSplitErrorAnalysisError,
            run_ml_source_split_error_analysis_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        top_n = int(getattr(args, "top_n", 10) or 10)
        if top_n < 1 or top_n > 100:
            parser.error("--top-n must be between 1 and 100")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_source_split_error_analysis_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                source_split_artifact_path=Path(args.source_split_artifact),
                ranking_run_id=rid,
                family=str(args.family),
                output_json=out_json,
                markdown_output=out_md,
                top_n=top_n,
            )
        except MLSourceSplitErrorAnalysisError as e:
            print(f"ml-source-split-error-analysis: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-fresh-product-candidate-ranking-source":
        from pipeline.ml_fresh_product_candidate_ranking_source import (
            MLFreshProductCandidateRankingSourceError,
            write_ml_fresh_product_candidate_ranking_source,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output)
        try:
            write_ml_fresh_product_candidate_ranking_source(
                fresh_eval_labeling_plan_path=Path(args.fresh_eval_labeling_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                label_dataset_path=Path(args.label_dataset),
                conflict_policy_path=Path(args.conflict_policy),
                output_path=out_json,
                markdown_output_path=out_md,
                database_url=args.database_url,
                family=str(args.family),
                ranking_run_id=args.ranking_run_id,
                min_confirmatory_candidate_works=args.min_confirmatory_candidate_works,
                source_version=str(args.source_version),
                repo_root=repo_root,
            )
        except MLFreshProductCandidateRankingSourceError as e:
            print(f"ml-fresh-product-candidate-ranking-source: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-tiny-baseline-rollup":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_tiny_baseline import MLTinyBaselineError
        from pipeline.ml_tiny_baseline_rollup import MLTinyBaselineRollupError, run_ml_tiny_baseline_rollup_cli

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_tiny_baseline_rollup_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                output_json=out_json,
                markdown_output=out_md,
            )
        except (MLTinyBaselineRollupError, MLTinyBaselineError) as e:
            code = getattr(e, "code", 2)
            print(f"ml-tiny-baseline-rollup: {e}", file=sys.stderr)
            raise SystemExit(code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-tiny-baseline-disagreement":
        from pipeline import bootstrap_loader as _bootstrap_loader
        from pipeline.ml_tiny_baseline import MLTinyBaselineError
        from pipeline.ml_tiny_baseline_disagreement import (
            MLTinyBaselineDisagreementError,
            TARGET_ORDER as _disag_targets,
            run_ml_tiny_baseline_disagreement_cli,
        )

        rid = (args.ranking_run_id or "").strip()
        if not rid:
            parser.error("--ranking-run-id is required and must be non-empty")
        if bool(getattr(args, "all_targets", False)) and getattr(args, "target", None):
            parser.error("use either --all-targets or --target, not both")
        if not getattr(args, "all_targets", False) and not getattr(args, "target", None):
            parser.error("provide --target or --all-targets")
        targets = tuple(_disag_targets) if getattr(args, "all_targets", False) else (str(args.target),)
        top_n = int(getattr(args, "top_n", 25) or 25)
        if top_n < 1 or top_n > 200:
            parser.error("--top-n must be between 1 and 200")
        dsn = args.database_url or _bootstrap_loader.database_url_from_env()
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        try:
            run_ml_tiny_baseline_disagreement_cli(
                database_url=dsn,
                label_dataset_path=Path(args.label_dataset),
                ranking_run_id=rid,
                family=str(args.family),
                targets=targets,
                top_n=top_n,
                output_json=out_json,
                markdown_output=out_md,
            )
        except (MLTinyBaselineDisagreementError, MLTinyBaselineError) as e:
            code = getattr(e, "code", 2)
            print(f"ml-tiny-baseline-disagreement: {e}", file=sys.stderr)
            raise SystemExit(code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v5-reviewer-blind-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v5_reviewer_blind_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v5"
        try:
            write_ml_label_dataset_v5_reviewer_blind_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v5-reviewer-blind-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v6-hard-negative-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v6_hard_negative_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v6"
        try:
            write_ml_label_dataset_v6_hard_negative_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v6-hard-negative-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v7-external-near-miss-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v7_external_near_miss_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v7"
        try:
            write_ml_label_dataset_v7_external_near_miss_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v7-external-near-miss-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset-v8-transfer-gap-ingest":
        from pipeline.ml_label_dataset import MLLabelDatasetError, write_ml_label_dataset_v8_transfer_gap_ingest

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or "ml-label-dataset-v8"
        try:
            write_ml_label_dataset_v8_transfer_gap_ingest(
                repo_root=repo_root,
                base_dataset_path=Path(args.base_dataset),
                blank_worksheet_path=Path(args.blank_worksheet),
                labeled_worksheet_path=Path(args.labeled_worksheet),
                context_sidecar_path=Path(args.context_sidecar),
                conflict_policy_path=Path(args.conflict_policy),
                json_path=out_json,
                markdown_path=out_md,
                dataset_version=dver,
            )
        except MLLabelDatasetError as e:
            print(f"ml-label-dataset-v8-transfer-gap-ingest: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "ml-label-dataset":
        from pipeline.ml_label_dataset import write_ml_label_dataset

        repo_root = Path(args.repo_root).resolve() if args.repo_root else Path(__file__).resolve().parents[3]
        manual_dir = Path(args.manual_review_dir).resolve() if args.manual_review_dir else None
        out_json = Path(args.output)
        out_md = Path(args.markdown_output) if args.markdown_output else None
        dver = (args.dataset_version or "").strip() or None
        write_ml_label_dataset(
            repo_root=repo_root,
            json_path=out_json,
            markdown_path=out_md,
            manual_review_dir=manual_dir,
            dataset_version=dver,
        )
        print(out_json.resolve(), file=sys.stderr)
        if out_md is not None:
            print(out_md.resolve(), file=sys.stderr)
        return

    if args.command == "corpus-expansion-preview":
        from pipeline.corpus_expansion_preview import run_corpus_expansion_preview_from_cli

        if args.per_bucket_sample < 10 or args.per_bucket_sample > 25:
            parser.error("--per-bucket-sample must be between 10 and 25")
        run_corpus_expansion_preview_from_cli(
            output=Path(args.output),
            markdown_output=Path(args.markdown_output),
            mailto=(args.mailto or "").strip(),
            per_bucket_sample=int(args.per_bucket_sample),
            mock_openalex=bool(args.mock_openalex),
        )
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "corpus-v2-candidate-plan":
        from pipeline.corpus_v2_candidate_plan import run_corpus_v2_candidate_plan_from_cli

        if args.target_max < args.target_min:
            parser.error("--target-max must be >= --target-min")
        if args.per_bucket_limit < 1 or args.per_bucket_limit > 500:
            parser.error("--per-bucket-limit must be between 1 and 500")
        run_corpus_v2_candidate_plan_from_cli(
            output=Path(args.output),
            markdown_output=Path(args.markdown_output),
            mailto=(args.mailto or "").strip(),
            per_bucket_limit=int(args.per_bucket_limit),
            target_min=int(args.target_min),
            target_max=int(args.target_max),
            mock_openalex=bool(args.mock_openalex),
        )
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "corpus-v2-ingest-from-plan":
        from pipeline.corpus_v2_ingest_from_plan import CorpusV2IngestError, run_corpus_v2_ingest_from_plan

        try:
            summary = run_corpus_v2_ingest_from_plan(
                candidate_plan_path=Path(args.candidate_plan),
                snapshot_version=args.snapshot_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
            )
        except CorpusV2IngestError as e:
            print(f"corpus-v2-ingest-from-plan: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["ingest_run_id"])
        return

    if args.command == "corpus-v2-hydrate-openalex":
        from pipeline.corpus_v2_hydrate_openalex import CorpusV2HydrateError, run_corpus_v2_hydrate_openalex

        try:
            summary = run_corpus_v2_hydrate_openalex(
                snapshot_version=args.snapshot_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
                mock_openalex=bool(args.mock_openalex),
            )
        except CorpusV2HydrateError as e:
            print(f"corpus-v2-hydrate-openalex: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["hydration_run_id"])
        return

    if args.command == "ml-fresh-hybrid-snapshot-hydrate":
        from pipeline.ml_fresh_hybrid_snapshot_hydration import (
            MLFreshHybridSnapshotHydrationError,
            write_ml_fresh_hybrid_snapshot_hydration,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_fresh_hybrid_snapshot_hydration(
                fresh_hybrid_candidate_plan_ingest_path=Path(args.fresh_hybrid_candidate_plan_ingest),
                fresh_hybrid_corpus_candidate_plan_path=Path(args.fresh_hybrid_corpus_candidate_plan),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                database_url=args.database_url,
                mailto=args.mailto,
                mock_openalex=bool(args.mock_openalex),
                dry_run=bool(args.dry_run),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                hydration_version=str(args.hydration_version),
                repo_root=repo_root,
            )
        except MLFreshHybridSnapshotHydrationError as e:
            print(f"ml-fresh-hybrid-snapshot-hydrate: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(payload["metadata"]["snapshot_version"])
        print(payload["hydration_result"]["recommended_next_stage"])
        return

    if args.command == "corpus-v2-embed":
        from pipeline.corpus_v2_embed import CorpusV2EmbedError, run_corpus_v2_embed

        try:
            summary = run_corpus_v2_embed(
                snapshot_version=args.snapshot_version,
                embedding_version=args.embedding_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
                model=args.model,
                batch_size=int(args.batch_size),
                replace=bool(args.replace),
            )
        except CorpusV2EmbedError as e:
            print(f"corpus-v2-embed: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(summary["snapshot_version"])
        print(summary["embedding_version"])
        print(summary["embedded_count"])
        return

    if args.command == "ml-fresh-hybrid-snapshot-embed":
        from pipeline.ml_fresh_hybrid_snapshot_embeddings import (
            MLFreshHybridSnapshotEmbeddingsError,
            write_ml_fresh_hybrid_snapshot_embeddings,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_fresh_hybrid_snapshot_embeddings(
                fresh_hybrid_snapshot_hydration_path=Path(args.fresh_hybrid_snapshot_hydration),
                fresh_hybrid_candidate_plan_ingest_path=Path(args.fresh_hybrid_candidate_plan_ingest),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                embedding_version=str(args.embedding_version),
                database_url=args.database_url,
                mock_embeddings=bool(args.mock_embeddings),
                dry_run=bool(args.dry_run),
                limit=args.limit,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLFreshHybridSnapshotEmbeddingsError as e:
            print(f"ml-fresh-hybrid-snapshot-embed: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(payload["metadata"]["snapshot_version"])
        print(payload["metadata"]["embedding_version"])
        print(payload["embedding_result"]["recommended_next_stage"])
        return

    if args.command == "ml-fresh-hybrid-product-candidate-ranking":
        from pipeline.ml_fresh_hybrid_product_candidate_ranking import (
            MLFreshHybridProductCandidateRankingError,
            write_ml_fresh_hybrid_product_candidate_ranking,
        )

        repo_root = Path(args.repo_root) if args.repo_root else None
        try:
            payload = write_ml_fresh_hybrid_product_candidate_ranking(
                fresh_hybrid_snapshot_embeddings_path=Path(args.fresh_hybrid_snapshot_embeddings),
                fresh_hybrid_snapshot_hydration_path=Path(args.fresh_hybrid_snapshot_hydration),
                fresh_hybrid_candidate_plan_ingest_path=Path(args.fresh_hybrid_candidate_plan_ingest),
                fresh_surface_policy_path=Path(args.fresh_surface_policy),
                snapshot_version=args.snapshot_version,
                embedding_version=str(args.embedding_version),
                ranking_version=str(args.ranking_version),
                family=str(args.family),
                database_url=args.database_url,
                dry_run=bool(args.dry_run),
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                artifact_version=str(args.artifact_version),
                repo_root=repo_root,
            )
        except MLFreshHybridProductCandidateRankingError as e:
            print(f"ml-fresh-hybrid-product-candidate-ranking: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(payload["ranking_result"]["ranking_run_id"])
        print(payload["ranking_result"]["paper_scores_written_count"])
        print(payload["ranking_result"]["recommended_next_stage"])
        return

    if args.command == "cluster-inspection":
        try:
            payload = run_cluster_inspection(
                corpus_snapshot_version=args.corpus_snapshot_version,
                embedding_version=args.embedding_version,
                cluster_version=args.cluster_version,
                output_path=Path(args.output),
                markdown_output_path=Path(args.markdown_output),
                database_url=args.database_url,
            )
        except ClusterInspectionError as e:
            print(f"cluster-inspection: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(payload["provenance"]["corpus_snapshot_version"])
        print(payload["provenance"]["cluster_version"])
        return

    if args.command == "recommendation-review-worksheet":
        if args.limit < 1 or args.limit > 200:
            parser.error("--limit must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        if bool(args.bridge_eligible_only) and args.family != "bridge":
            parser.error("--bridge-eligible-only is only valid with --family bridge")
        try:
            write_recommendation_review_worksheet(
                output_path=Path(args.output),
                database_url=args.database_url,
                ranking_run_id=rrid,
                family=args.family,
                limit=int(args.limit),
                bridge_eligible_only=bool(args.bridge_eligible_only),
            )
        except WorksheetError as e:
            print(f"recommendation-review-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        return

    if args.command == "recommendation-review-summary":
        try:
            run_recommendation_review_summary(
                input_path=Path(args.input),
                output_path=Path(args.output),
                allow_incomplete=bool(args.allow_incomplete),
                markdown_path=Path(args.markdown_output)
                if args.markdown_output
                else None,
            )
        except ReviewSummaryError as e:
            print(f"recommendation-review-summary: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "recommendation-review-rollup":
        summary_paths: list[Path]
        explicit = [args.bridge_summary, args.emerging_summary, args.undercited_summary]
        if any(explicit):
            if not all(explicit):
                parser.error(
                    "--bridge-summary, --emerging-summary, and --undercited-summary must be provided together"
                )
            summary_paths = [
                Path(args.bridge_summary),
                Path(args.emerging_summary),
                Path(args.undercited_summary),
            ]
            if args.summary:
                parser.error("Use either repeated --summary or explicit family summary flags, not both")
        else:
            if not args.summary:
                parser.error("Provide at least one --summary or explicit family summary flags")
            summary_paths = [Path(x) for x in args.summary]
        try:
            run_recommendation_review_rollup(
                summary_paths=summary_paths,
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output)
                if args.markdown_output
                else None,
                bridge_diagnostics_path=Path(args.bridge_diagnostics)
                if args.bridge_diagnostics
                else None,
                bridge_worksheet_path=Path(args.bridge_worksheet)
                if args.bridge_worksheet
                else None,
            )
        except ReviewRollupError as e:
            print(f"recommendation-review-rollup: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "bridge-experiment-readiness":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            run_bridge_experiment_readiness(
                rollup_path=Path(args.rollup),
                ranking_run_id=rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeExperimentReadinessError as e:
            print(f"bridge-experiment-readiness: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    if args.command == "bridge-signal-diagnostics":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            run_bridge_signal_diagnostics(
                ranking_run_id=rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeSignalDiagnosticsError as e:
            print(f"bridge-signal-diagnostics: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-eligibility-sensitivity":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        try:
            run_bridge_eligibility_sensitivity(
                ranking_run_id=rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeEligibilitySensitivityError as e:
            print(f"bridge-eligibility-sensitivity: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-objective-redesign-simulation":
        if int(args.k) != 20:
            parser.error("bridge-objective-redesign-simulation: --k must be 20")
        rrid = (args.ranking_run_id or "").strip()
        if not rrid:
            parser.error("--ranking-run-id is required and must not be blank")
        if args.repo_root:
            repo_root = Path(args.repo_root).resolve()
        else:
            cwd = Path.cwd().resolve()
            if (cwd / "docs" / "audit").is_dir():
                repo_root = cwd
            elif (cwd.parent / "docs" / "audit").is_dir():
                repo_root = cwd.parent
            elif (cwd.parent.parent / "docs" / "audit").is_dir():
                repo_root = cwd.parent.parent
            else:
                repo_root = cwd
        mr = repo_root / "docs" / "audit" / "manual-review"
        k = int(args.k)
        sens = Path(args.sensitivity_json).resolve() if args.sensitivity_json else mr / f"bridge_eligibility_sensitivity_{rrid}_top{k}.json"
        failp = (
            Path(args.failure_analysis_json).resolve()
            if args.failure_analysis_json
            else mr / f"bridge_eligibility_failure_analysis_{rrid}_top{k}.json"
        )
        csvp = (
            Path(args.bridge_worksheet_csv).resolve()
            if args.bridge_worksheet_csv
            else mr / f"bridge_eligible_{rrid}_top{k}.csv"
        )
        for p, label in ((sens, "sensitivity-json"), (failp, "failure-analysis-json"), (csvp, "bridge-worksheet-csv")):
            if not p.is_file():
                print(f"bridge-objective-redesign-simulation: missing {label}: {p}", file=sys.stderr)
                raise SystemExit(2)
        try:
            run_bridge_objective_redesign_simulation(
                ranking_run_id=rrid,
                k=k,
                sensitivity_json_path=sens,
                failure_analysis_json_path=failp,
                bridge_worksheet_csv_path=csvp,
                output_json_path=Path(args.output),
                markdown_path=Path(args.markdown_output),
                database_url=args.database_url,
            )
        except BridgeObjectiveRedesignSimulationError as e:
            print(f"bridge-objective-redesign-simulation: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-experiment-compare":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        baseline_rrid = (args.baseline_ranking_run_id or "").strip()
        experiment_rrid = (args.experiment_ranking_run_id or "").strip()
        if not baseline_rrid:
            parser.error("--baseline-ranking-run-id is required and must not be blank")
        if not experiment_rrid:
            parser.error("--experiment-ranking-run-id is required and must not be blank")
        try:
            run_bridge_weight_experiment_compare(
                baseline_ranking_run_id=baseline_rrid,
                experiment_ranking_run_id=experiment_rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
                baseline_bridge_worksheet_path=Path(args.baseline_bridge_worksheet)
                if args.baseline_bridge_worksheet
                else None,
            )
        except BridgeWeightExperimentCompareError as e:
            print(f"bridge-weight-experiment-compare: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-objective-experiment-compare":
        if args.k < 1 or args.k > 200:
            parser.error("--k must be between 1 and 200")
        baseline_rrid = (args.baseline_ranking_run_id or "").strip()
        experiment_rrid = (args.experiment_ranking_run_id or "").strip()
        if not baseline_rrid:
            parser.error("--baseline-ranking-run-id is required and must not be blank")
        if not experiment_rrid:
            parser.error("--experiment-ranking-run-id is required and must not be blank")
        try:
            run_bridge_objective_experiment_compare(
                baseline_ranking_run_id=baseline_rrid,
                experiment_ranking_run_id=experiment_rrid,
                k=int(args.k),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
                baseline_bridge_worksheet_path=Path(args.baseline_bridge_worksheet),
            )
        except BridgeObjectiveExperimentCompareError as e:
            print(f"bridge-objective-experiment-compare: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-experiment-delta-worksheet":
        baseline_rrid = (args.baseline_ranking_run_id or "").strip() or None
        experiment_rrid = (args.experiment_ranking_run_id or "").strip() or None
        try:
            rows = write_bridge_weight_experiment_delta_worksheet(
                comparison_path=Path(args.comparison),
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                diagnostics_path=Path(args.experiment_diagnostics),
                output_path=Path(args.output),
                database_url=args.database_url,
                baseline_ranking_run_id=baseline_rrid,
                experiment_ranking_run_id=experiment_rrid,
            )
        except BridgeWeightExperimentDeltaWorksheetError as e:
            print(f"bridge-weight-experiment-delta-worksheet: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(len(rows))
        return
    if args.command == "bridge-objective-label-coverage":
        try:
            payload, rows = run_bridge_objective_label_coverage(
                comparison_path=Path(args.comparison),
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                prior_delta_worksheet_path=Path(args.prior_delta_worksheet),
                output_json_path=Path(args.output),
                output_markdown_path=Path(args.markdown_output),
                output_review_csv_path=Path(args.review_output),
            )
        except BridgeObjectiveLabelCoverageError as e:
            print(f"bridge-objective-label-coverage: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        print(Path(args.markdown_output).resolve(), file=sys.stderr)
        print(Path(args.review_output).resolve(), file=sys.stderr)
        print(int(payload.get("summary", {}).get("truly_unlabeled_moved_in_count", len(rows))))
        return
    if args.command == "bridge-objective-labeled-outcome":
        try:
            run_bridge_objective_labeled_outcome(
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                prior_delta_worksheet_path=Path(args.prior_delta_worksheet),
                objective_delta_worksheet_path=Path(args.objective_delta_worksheet),
                objective_comparison_path=Path(args.objective_comparison),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
            )
        except BridgeObjectiveLabeledOutcomeError as e:
            print(f"bridge-objective-labeled-outcome: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-experiment-delta-summary":
        try:
            run_bridge_weight_experiment_delta_summary(
                input_path=Path(args.input),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
            )
        except BridgeWeightExperimentDeltaSummaryError as e:
            print(f"bridge-weight-experiment-delta-summary: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-response-rollup":
        try:
            run_bridge_weight_response_rollup(
                baseline_review_rollup_path=Path(args.baseline_review_rollup),
                compare_zero_vs_w005_path=Path(args.compare_zero_vs_w005),
                delta_review_summary_path=Path(args.delta_review_summary),
                compare_w005_vs_w010_path=Path(args.compare_w005_vs_w010),
                compare_zero_vs_w010_path=Path(args.compare_zero_vs_w010),
                labeled_baseline_bridge_worksheet_path=Path(args.labeled_baseline_bridge_worksheet),
                delta_review_csv_path=Path(args.delta_review_csv),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeWeightResponseRollupError as e:
            print(f"bridge-weight-response-rollup: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return
    if args.command == "bridge-weight-labeled-outcome":
        try:
            run_bridge_weight_labeled_outcome(
                baseline_worksheet_path=Path(args.baseline_bridge_worksheet),
                delta_worksheet_path=Path(args.delta_review_csv),
                response_rollup_path=Path(args.response_rollup),
                compare_zero_vs_w005_path=Path(args.compare_zero_vs_w005),
                compare_w005_vs_w010_path=Path(args.compare_w005_vs_w010),
                compare_zero_vs_w010_path=Path(args.compare_zero_vs_w010),
                diagnostics_rank_zero_path=Path(args.diagnostics_zero),
                diagnostics_rank_w005_path=Path(args.diagnostics_w005),
                diagnostics_rank_w010_path=Path(args.diagnostics_w010),
                output_path=Path(args.output),
                markdown_path=Path(args.markdown_output) if args.markdown_output else None,
                database_url=args.database_url,
            )
        except BridgeWeightLabeledOutcomeError as e:
            print(f"bridge-weight-labeled-outcome: {e}", file=sys.stderr)
            raise SystemExit(e.code) from e
        print(Path(args.output).resolve(), file=sys.stderr)
        if args.markdown_output:
            print(Path(args.markdown_output).resolve(), file=sys.stderr)
        return

    policy = CorpusPolicy()

    if args.command == "print-policy":
        if args.json:
            import json

            print(json.dumps(policy.as_dict(), indent=2))
        else:
            print(policy)
        return

    if args.command == "bootstrap-plan":
        output_dir = Path(args.output)
        outcomes = None
        policy_model = policy
        if args.resolve_openalex:
            outcomes = resolve_all_sources(policy, mailto=args.mailto)
            policy_model = corpus_policy_with_openalex_source_ids(policy, slug_to_openalex_id_map(outcomes))
        elif args.database_source_ids:
            dsn = args.database_url or database_url_from_env()
            policy_model = load_resolved_policy_from_database(dsn, policy)
        elif any(not s.openalex_source_id for s in policy.source_policies):
            parser.error(
                "bootstrap-plan needs canonical OpenAlex source ids: use --resolve-openalex, "
                "--database-source-ids, or set openalex_source_id on each SourcePolicy in policy.py"
            )

        snapshot, ingest_run = create_bootstrap_bundle(policy=policy_model, note=args.note)
        write_ingest_artifacts(output_dir, snapshot, ingest_run)
        write_source_resolution_manifest(output_dir, snapshot, build_source_resolution_plans(policy))
        if outcomes is not None:
            write_source_resolution_results(output_dir, snapshot, outcomes)
        write_bootstrap_plan(output_dir, snapshot, build_bootstrap_work_plans(policy_model))
        print(snapshot.source_snapshot_version)
        print(ingest_run.ingest_run_id)
        return

    if args.command == "bootstrap-run":
        output_dir = Path(args.output)
        raw_root = Path(args.raw_root)
        finalized = run_bootstrap_ingest(
            policy=policy,
            output_dir=output_dir,
            raw_root=raw_root,
            note=args.note,
            database_url=args.database_url,
            mailto=args.mailto,
            max_pages_per_source=args.max_pages_per_source,
        )
        print(finalized.ingest_run_id)
        print(finalized.source_snapshot_version)
        return

    if args.command == "embed-works":
        summary = execute_embedding_run(
            database_url=args.database_url,
            embedding_version=args.embedding_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            model=args.model,
            batch_size=args.batch_size,
            limit=args.limit,
        )
        lines = [
            f"embedding_version={summary.embedding_version}",
            f"corpus_snapshot_version={summary.corpus_snapshot_version}",
            f"model={summary.model}",
            f"total_included_works={summary.total_included_works}",
            f"already_embedded_before_run={summary.already_embedded_works}",
            f"missing_before_run={summary.missing_embedding_works}",
            f"candidate_works_this_run={summary.candidate_works}",
            f"planned_batches={summary.planned_batches}",
            f"batches_committed={summary.batch_count}",
            f"rows_written_this_run={summary.rows_written}",
            f"still_missing_after_run={summary.still_missing_after_run}",
        ]
        print("\n".join(lines), file=sys.stderr)
        print(summary.embedding_version)
        print(summary.corpus_snapshot_version)
        print(summary.rows_written)
        return

    if args.command == "ranking-run":
        finalized = execute_ranking_run(
            database_url=args.database_url,
            ranking_version=args.ranking_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            embedding_version=args.embedding_version,
            cluster_version=args.cluster_version,
            bridge_weight_for_bridge_family=args.bridge_weight_for_family_bridge,
            bridge_eligibility_mode=args.bridge_eligibility_mode,
            note=args.note,
            low_cite_min_year=args.low_cite_min_year,
            low_cite_max_citations=args.low_cite_max_citations,
        )
        print(finalized.ranking_run_id)
        print(finalized.corpus_snapshot_version)
        return

    if args.command == "cluster-works":
        finalized = execute_clustering_run(
            database_url=args.database_url,
            cluster_version=args.cluster_version,
            embedding_version=args.embedding_version,
            corpus_snapshot_version=args.corpus_snapshot_version,
            cluster_count=args.cluster_count,
            max_iterations=args.max_iterations,
            note=args.note,
        )
        lines = [
            f"cluster_version={finalized.cluster_version}",
            f"embedding_version={finalized.embedding_version}",
            f"corpus_snapshot_version={finalized.corpus_snapshot_version}",
            f"algorithm={finalized.algorithm}",
            f"status={finalized.status}",
            f"total_input_works={finalized.counts.total_input_works}",
            f"clustered_works={finalized.counts.clustered_works}",
            f"cluster_count={finalized.counts.cluster_count}",
        ]
        print("\n".join(lines), file=sys.stderr)
        print(finalized.cluster_version)
        print(finalized.corpus_snapshot_version)
        return

    if args.command == "repair-works-text":
        snap, scanned, updated = run_work_text_repair_cli(
            database_url=args.database_url,
            corpus_snapshot_version=args.corpus_snapshot_version,
            dry_run=args.dry_run,
        )
        mode = "dry-run" if args.dry_run else "committed"
        print(
            f"repair-works-text ({mode}): corpus_snapshot_version={snap} "
            f"scanned={scanned} rows_changed={updated}",
            file=sys.stderr,
        )
        print(snap)
        print(updated)
        return

    if args.command == "embedding-coverage":
        dsn = args.database_url or database_url_from_env()
        with psycopg.connect(dsn) as conn:
            snap = args.corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
            if snap is None:
                parser.error("No corpus snapshot with included works found.")
            total = count_included_works_for_snapshot(conn, snap)
            missing = count_missing_embedding_candidates(
                conn,
                corpus_snapshot_version=snap,
                embedding_version=args.embedding_version,
            )
            missing_cluster: int | None = None
            if args.cluster_version:
                cr = conn.execute(
                    """
                    SELECT embedding_version, status
                    FROM clustering_runs
                    WHERE cluster_version = %s
                      AND corpus_snapshot_version = %s
                    """,
                    (args.cluster_version, snap),
                ).fetchone()
                if cr is None:
                    print(
                        "embedding-coverage: error: no clustering_runs row for "
                        f"cluster_version={args.cluster_version!r} and "
                        f"corpus_snapshot_version={snap!r}.",
                        file=sys.stderr,
                    )
                    sys.exit(2)
                run_emb, run_status = str(cr[0]), str(cr[1])
                if run_emb != args.embedding_version:
                    print(
                        "embedding-coverage: warning: clustering_runs.embedding_version="
                        f"{run_emb!r} differs from --embedding-version={args.embedding_version!r}.",
                        file=sys.stderr,
                    )
                if run_status != "succeeded":
                    print(
                        "embedding-coverage: warning: clustering_runs.status="
                        f"{run_status!r} (expected succeeded).",
                        file=sys.stderr,
                    )
                missing_cluster = count_included_missing_cluster_assignment(
                    conn,
                    corpus_snapshot_version=snap,
                    cluster_version=args.cluster_version,
                )
        embedded = total - missing
        lines = [
            f"corpus_snapshot_version={snap}",
            f"embedding_version={args.embedding_version}",
            f"included_works={total}",
            f"with_embedding={embedded}",
            f"missing_embedding={missing}",
        ]
        if args.cluster_version and missing_cluster is not None:
            lines.extend(
                [
                    f"cluster_version={args.cluster_version}",
                    f"with_cluster_assignment={total - missing_cluster}",
                    f"missing_cluster_assignment={missing_cluster}",
                ]
            )
        print("\n".join(lines), file=sys.stderr)
        print(snap)
        print(missing)
        gap = missing > 0 or (
            args.cluster_version is not None and missing_cluster is not None and missing_cluster > 0
        )
        if args.fail_on_gaps and gap:
            sys.exit(1)
        return


if __name__ == "__main__":
    main()

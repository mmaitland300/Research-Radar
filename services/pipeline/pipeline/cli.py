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
    ml_label_dataset_v9_parser = subparsers.add_parser(
        "ml-label-dataset-v9-fresh-hybrid-ingest",
        help=(
            "Build ml-label-dataset v9 from v8 plus the validated fresh-hybrid labeled worksheet "
            "and row_id-keyed context sidecar"
        ),
    )
    ml_label_dataset_v9_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v9, docs/audit/ml-label-dataset-v8.json)",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank fresh hybrid eval CSV template",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed fresh hybrid eval labeled CSV",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to fresh hybrid eval row_id-keyed context sidecar JSON",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--fresh-eval-surface",
        default=None,
        help="Optional path to ml-fresh-eval-surface-hybrid-v1 JSON for candidate surface SHA validation",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v9 JSON",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown data card",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v9_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v9",
        help="Version string for the new artifact and appended fresh-hybrid rows (default: ml-label-dataset-v9)",
    )
    ml_label_dataset_v10_parser = subparsers.add_parser(
        "ml-label-dataset-v10-fresh-positive-topup-ingest",
        help=(
            "Build ml-label-dataset v10 from v9 plus the validated fresh-hybrid positive top-up "
            "labeled worksheet and row_id-keyed context sidecar"
        ),
    )
    ml_label_dataset_v10_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v10, docs/audit/ml-label-dataset-v9.json)",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank fresh hybrid positive top-up CSV template",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed fresh hybrid positive top-up labeled CSV",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to fresh hybrid positive top-up row_id-keyed context sidecar JSON",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--fresh-eval-surface",
        default=None,
        help="Optional path to v9 ml-fresh-eval-surface-hybrid-v1 JSON for candidate surface SHA validation",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v10 JSON",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown data card",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v10_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v10",
        help="Version string for the new artifact and appended positive top-up rows (default: ml-label-dataset-v10)",
    )
    ml_label_dataset_v11_parser = subparsers.add_parser(
        "ml-label-dataset-v11-shadow-generalization-ingest",
        help=(
            "Build ml-label-dataset v11 from v10 plus the completed second-surface "
            "shadow-generalization labeled worksheet"
        ),
    )
    ml_label_dataset_v11_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v11, docs/audit/ml-label-dataset-v10.json)",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank second-surface shadow-generalization CSV template",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed second-surface shadow-generalization labeled CSV",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to second-surface shadow-generalization row_id-keyed context sidecar JSON",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--generalization-second-surface",
        required=True,
        help="Path to ml-shadow-scorer-v1-generalization-second-surface-v1 JSON",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v11 JSON",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown data card",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v11_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v11",
        help="Version string for the new artifact and appended shadow-generalization rows (default: ml-label-dataset-v11)",
    )
    ml_label_dataset_v12_parser = subparsers.add_parser(
        "ml-label-dataset-v12-bridge-negative-mining-ingest",
        help=(
            "Build ml-label-dataset v12 from v11 plus the completed bridge negative-mining "
            "labeled worksheet"
        ),
    )
    ml_label_dataset_v12_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v12, docs/audit/ml-label-dataset-v11.json)",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank bridge negative-mining CSV template",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed bridge negative-mining labeled CSV",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to bridge negative-mining row_id-keyed context sidecar JSON",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--conflict-policy",
        required=True,
        help="Path to label conflict policy Markdown (provenance only; no conflict resolution)",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v12 JSON",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown data card",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v12_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v12",
        help="Version string for the new artifact and appended bridge negative-mining rows (default: ml-label-dataset-v12)",
    )
    ml_label_dataset_v13_parser = subparsers.add_parser(
        "ml-label-dataset-v13-bridge-top-ranked-ingest",
        help=(
            "Build ml-label-dataset v13 from v12 plus the completed bridge top-ranked validation "
            "labeled worksheet"
        ),
    )
    ml_label_dataset_v13_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v13, docs/audit/ml-label-dataset-v12.json)",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank bridge top-ranked validation CSV template",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed bridge top-ranked validation labeled CSV",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to bridge top-ranked validation row_id-keyed context sidecar JSON",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v13 JSON",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown data card",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v13_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v13",
        help="Version string for the new artifact and appended bridge top-ranked rows (default: ml-label-dataset-v13)",
    )
    ml_label_dataset_v14_parser = subparsers.add_parser(
        "ml-label-dataset-v14-bridge-shadow-pilot-ingest",
        help=(
            "Build ml-label-dataset v14 from v13 plus the completed bridge shadow-pilot "
            "disagreement labeled worksheet"
        ),
    )
    ml_label_dataset_v14_parser.add_argument(
        "--base-dataset",
        required=True,
        help="Path to base ml-label-dataset JSON (for v14, docs/audit/ml-label-dataset-v13.json)",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--blank-worksheet",
        required=True,
        help="Path to blank bridge shadow-pilot disagreement CSV template",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--labeled-worksheet",
        required=True,
        help="Path to completed bridge shadow-pilot disagreement labeled CSV",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--context-sidecar",
        required=True,
        help="Path to bridge shadow-pilot work_id-keyed context sidecar JSON",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-label-dataset v14 JSON",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown data card",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root (default: parent of services/pipeline)",
    )
    ml_label_dataset_v14_parser.add_argument(
        "--dataset-version",
        default="ml-label-dataset-v14",
        help="Version string for the new artifact and appended bridge shadow-pilot rows (default: ml-label-dataset-v14)",
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
        help="Path to label dataset JSON (default expected version is ml-label-dataset-v8; use --expected-label-dataset-version for v9 fresh-hybrid reruns)",
    )
    ml_fresh_eval_surface_hybrid_materialize_parser.add_argument(
        "--expected-label-dataset-version",
        default="ml-label-dataset-v8",
        help="Expected label dataset_version (default: ml-label-dataset-v8; set ml-label-dataset-v9 for fresh-hybrid label reruns)",
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
    ml_hybrid_validation_on_fresh_surface_parser = subparsers.add_parser(
        "ml-hybrid-validation-on-fresh-surface",
        help="Execute frozen hybrid validation metrics on the ready fresh eval surface (SELECT-only DB)",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--fresh-eval-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--label-dataset",
        required=True,
        help="Path to ml-label-dataset-v10 JSON",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--audit-embedding-scorer-export",
        required=True,
        help="Path to ml-offline-audit-embedding-scorer-v2 JSON",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--fresh-hybrid-snapshot-embeddings",
        required=True,
        help="Path to ml-fresh-hybrid-snapshot-embeddings-v1 JSON",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--hybrid-experiment-spec",
        default=None,
        help="Optional ml-hybrid-scorer-offline-experiment-v1-spec JSON for provenance validation",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--hybrid-metric-gates",
        default=None,
        help="Optional ml-hybrid-scorer-metric-gates-v1 JSON for provenance hashing only",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--database-url",
        default=None,
        help="Local Postgres URL for SELECT-only embedding reads (default: DATABASE_URL or PG* env)",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--output",
        required=True,
        help="Path to write fresh-surface hybrid validation JSON",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--validation-version",
        default="ml-hybrid-validation-on-fresh-surface-v1",
        help="Validation version string to write (default: ml-hybrid-validation-on-fresh-surface-v1)",
    )
    ml_hybrid_validation_on_fresh_surface_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_hybrid_validation_metric_gates_parser = subparsers.add_parser(
        "ml-hybrid-validation-metric-gates",
        help="Evaluate fresh-surface hybrid validation metric gates without rerunning scoring",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--hybrid-validation-on-fresh-surface",
        dest="hybrid_validation_on_fresh_surface",
        default=None,
        help="Path to ml-hybrid-validation-on-fresh-surface-v1 JSON",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--hybrid-validation",
        dest="hybrid_validation_on_fresh_surface",
        default=None,
        help="Alias for --hybrid-validation-on-fresh-surface",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--fresh-eval-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--fresh-surface-policy",
        required=True,
        help="Path to ml-fresh-eval-surface-policy-hybrid-v1 JSON",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--production-readiness-plan",
        required=True,
        help="Path to ml-production-readiness-plan-v1 JSON",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--output",
        required=True,
        help="Path to write hybrid validation metric gates JSON",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--gates-version",
        default="ml-hybrid-validation-metric-gates-v1",
        help="Gates version string to write (default: ml-hybrid-validation-metric-gates-v1)",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--hybrid-experiment-spec",
        default=None,
        help="Optional ml-hybrid-scorer-offline-experiment-v1-spec JSON for arm cross-check",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--hybrid-scorer-metric-gates",
        default=None,
        help="Optional ml-hybrid-scorer-metric-gates-v1 JSON for provenance hashing",
    )
    ml_hybrid_validation_metric_gates_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_v1_audit_parser = subparsers.add_parser(
        "ml-shadow-scorer-v1-audit",
        help="Replay the disabled-by-default ml-shadow-scorer-v1 formula against fresh validation rows",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--shadow-scorer-spec",
        required=True,
        help="Path to ml-shadow-scorer-v1-spec JSON",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--hybrid-validation-on-fresh-surface",
        required=True,
        help="Path to ml-hybrid-validation-on-fresh-surface-v1 JSON",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--fresh-eval-surface",
        required=True,
        help="Path to ml-fresh-eval-surface-hybrid-v1 JSON",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-shadow-scorer-v1 implementation audit JSON",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--implementation-version",
        default="ml-shadow-scorer-v1-implementation",
        help="Implementation version string to write (default: ml-shadow-scorer-v1-implementation)",
    )
    ml_shadow_scorer_v1_audit_parser.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root for portable provenance paths",
    )
    ml_shadow_scorer_v1_audit_output_parser = subparsers.add_parser(
        "ml-shadow-scorer-v1-audit-output",
        help="Write isolated offline ml-shadow-scorer-v1 audit output rows from fresh validation candidates",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--shadow-scorer-execution-readiness-gates",
        required=True,
        help="Path to ml-shadow-scorer-v1-execution-readiness-gates JSON",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--shadow-scorer-implementation",
        required=True,
        help="Path to ml-shadow-scorer-v1-implementation JSON",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--shadow-scorer-spec",
        required=True,
        help="Path to ml-shadow-scorer-v1-spec JSON",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--hybrid-validation-on-fresh-surface",
        required=True,
        help="Path to ml-hybrid-validation-on-fresh-surface-v1 JSON",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--output",
        required=True,
        help="Path to write ml-shadow-scorer-v1 audit output JSON",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--markdown-output",
        required=True,
        help="Path to write companion Markdown summary",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
        "--artifact-version",
        default="ml-shadow-scorer-v1-audit-output",
        help="Artifact version string to write (default: ml-shadow-scorer-v1-audit-output)",
    )
    ml_shadow_scorer_v1_audit_output_parser.add_argument(
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


    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dispatch_command(args, parser, psycopg_module=psycopg, compat_module=sys.modules[__name__])


if __name__ == "__main__":
    main()

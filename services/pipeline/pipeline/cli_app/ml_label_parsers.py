from __future__ import annotations


def register_ml_label_parsers(subparsers) -> None:
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

from __future__ import annotations


def register_ml_text_parsers(subparsers) -> None:
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

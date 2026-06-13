from __future__ import annotations


def register_ml_hybrid_parsers(subparsers) -> None:
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

"""Hybrid scorer offline experiment pre-registration spec.

This module writes a deterministic spec artifact only. It reads existing JSON
evidence, pre-registers label-blind hybrid arms, and does not train, score,
query a database, generate embeddings, run ranking, or authorize shadow/prod.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment_spec"
SPEC_VERSION = "ml-hybrid-scorer-offline-experiment-v1-spec"
SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
SCORING_VERSION = "ml-offline-production-candidate-scoring-v3"
SCORING_MODE = "heuristic_and_holdout_embedding_scorer"
METRIC_GATES_ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
METRIC_GATES_VERSION = "ml-offline-production-candidate-metric-gates-v3"
RECOMMENDED_NEXT_STAGE = "create_hybrid_scorer_offline_experiment_v1"
HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE = "ml_learned_scorer_holdout_assignment"
HOLDOUT_ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"
SPLIT_POLICY_ARTIFACT_TYPE = "ml_label_split_policy"
SPLIT_POLICY_VERSION = "ml-label-split-policy-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
HOLDOUT_POLICY_ARTIFACT_TYPE = "ml_learned_scorer_holdout_policy"
HOLDOUT_POLICY_VERSION = "ml-learned-scorer-holdout-policy-v1"
AUDIT_SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
AUDIT_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
AUDIT_SCORER_FIT_MODE = "holdout_bound_train_only"
TARGET = "good_or_acceptable"

CAVEATS = (
    "Spec only; no scoring executed.",
    "v3 eval has already been observed, so future hybrid run is diagnostic/exploratory on this same surface unless a new holdout is defined.",
    "Single-reviewer audit labels.",
    "One ranking run/family.",
    "Positive-heavy P@k.",
    "Not live recommender validation.",
    "Not shadow readiness.",
    "No production authorization.",
)


class MLHybridScorerOfflineExperimentSpecError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLHybridScorerOfflineExperimentSpecError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLHybridScorerOfflineExperimentSpecError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLHybridScorerOfflineExperimentSpecError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLHybridScorerOfflineExperimentSpecError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_scoring(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("scoring_mode") != SCORING_MODE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scoring metadata.scoring_mode={SCORING_MODE!r}, got {metadata.get('scoring_mode')!r}"
        )
    if metadata.get("target") != TARGET:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scoring metadata.target={TARGET!r}, got {metadata.get('target')!r}"
        )
    if _get(payload, "leakage_report.train_rows_used_in_metrics") != 0:
        raise MLHybridScorerOfflineExperimentSpecError("scoring leakage_report.train_rows_used_in_metrics must be 0")
    if _get(payload, "leakage_report.train_works_used_in_metrics") != 0:
        raise MLHybridScorerOfflineExperimentSpecError("scoring leakage_report.train_works_used_in_metrics must be 0")
    eval_only = _get(payload, "learned_or_embedding_metrics.eval_only")
    if eval_only is None:
        eval_only = _get(payload, "scoring_mode_details.eval_only")
    if eval_only is not True:
        raise MLHybridScorerOfflineExperimentSpecError("scoring learned metrics must be eval_only")
    if not str(metadata.get("eval_work_set_sha256") or "").strip():
        raise MLHybridScorerOfflineExperimentSpecError("scoring metadata.eval_work_set_sha256 must be present")
    return metadata


def _validate_gates(payload: Mapping[str, Any], *, scoring_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates")
    if metadata.get("artifact_type") != METRIC_GATES_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected gates metadata.artifact_type={METRIC_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != METRIC_GATES_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected gates metadata.gates_version={METRIC_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    required_true = (
        "product_candidate_heuristic_gates_passed",
        "held_out_learned_validity_passed",
        "heuristic_non_regression_passed",
        "independent_learned_validation_passed",
    )
    for key in required_true:
        if payload.get(key) is not True:
            raise MLHybridScorerOfflineExperimentSpecError(f"gates {key} must be true")
    if payload.get("material_lift_passed") is not False:
        raise MLHybridScorerOfflineExperimentSpecError(
            "gates material_lift_passed must be false for this no-lift hybrid spec path"
        )
    if payload.get("recommended_next_stage") != RECOMMENDED_NEXT_STAGE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"gates recommended_next_stage must be {RECOMMENDED_NEXT_STAGE!r}"
        )
    gates_eval_sha = metadata.get("eval_work_set_sha256")
    if gates_eval_sha is not None and gates_eval_sha != scoring_eval_sha:
        raise MLHybridScorerOfflineExperimentSpecError("gates eval_work_set_sha256 must match scoring")
    return metadata


def _validate_assignment(payload: Mapping[str, Any], *, scoring_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("artifact_type") != HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected assignment metadata.artifact_type={HOLDOUT_ASSIGNMENT_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("assignment_version") != HOLDOUT_ASSIGNMENT_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected assignment metadata.assignment_version={HOLDOUT_ASSIGNMENT_VERSION!r}, got {metadata.get('assignment_version')!r}"
        )
    if _get(payload, "leakage_report.global_zero_assertion") is not True:
        raise MLHybridScorerOfflineExperimentSpecError("assignment leakage_report.global_zero_assertion must be true")
    if metadata.get("eval_work_set_sha256") != scoring_eval_sha:
        raise MLHybridScorerOfflineExperimentSpecError("assignment metadata.eval_work_set_sha256 must match scoring")
    return metadata


def _validate_split_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            "expected production readiness plan metadata.artifact_type="
            f"{PRODUCTION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected production readiness plan metadata.plan_version={PRODUCTION_PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    good = _get(payload, "targets.good_or_acceptable")
    if not isinstance(good, Mapping) or good.get("production_eligible") is not False:
        raise MLHybridScorerOfflineExperimentSpecError("production plan must keep good_or_acceptable production_eligible false")
    if payload.get("production_default_authorized") is True:
        raise MLHybridScorerOfflineExperimentSpecError("production plan must not authorize production default")
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> None:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )


def _validate_holdout_policy(payload: Mapping[str, Any], *, scoring_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-policy")
    if metadata.get("artifact_type") != HOLDOUT_POLICY_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected holdout policy metadata.artifact_type={HOLDOUT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != HOLDOUT_POLICY_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected holdout policy metadata.policy_version={HOLDOUT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    eval_shas = (
        _get(payload, "dataset_inventory.product_candidate_eval_work_set_sha256"),
        _get(payload, "primary_holdout_strategy.eval_work_set_definition.eval_work_set_sha256"),
    )
    if any(value is not None and value != scoring_eval_sha for value in eval_shas):
        raise MLHybridScorerOfflineExperimentSpecError("holdout policy eval_work_set_sha256 must match scoring")
    return metadata


def _validate_audit_scorer(payload: Mapping[str, Any], *, scoring_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export")
    if metadata.get("artifact_type") != AUDIT_SCORER_ARTIFACT_TYPE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scorer metadata.artifact_type={AUDIT_SCORER_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("scorer_version") != AUDIT_SCORER_VERSION:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scorer metadata.scorer_version={AUDIT_SCORER_VERSION!r}, got {metadata.get('scorer_version')!r}"
        )
    if metadata.get("fit_mode") != AUDIT_SCORER_FIT_MODE:
        raise MLHybridScorerOfflineExperimentSpecError(
            f"expected scorer metadata.fit_mode={AUDIT_SCORER_FIT_MODE!r}, got {metadata.get('fit_mode')!r}"
        )
    if metadata.get("eval_work_set_sha256") != scoring_eval_sha:
        raise MLHybridScorerOfflineExperimentSpecError("scorer metadata.eval_work_set_sha256 must match scoring")
    return metadata


def _precision_at(payload: Mapping[str, Any], path_prefix: str, k: int) -> Any:
    return _get(payload, f"{path_prefix}.precision_recall_at_k.{k}.precision")


def _delta(learned: Any, heuristic: Any, supplied: Any) -> float | None:
    if _is_number(supplied):
        return float(supplied)
    if _is_number(learned) and _is_number(heuristic):
        return float(learned) - float(heuristic)
    return None


def _positive_prevalence(scoring_payload: Mapping[str, Any], gates_payload: Mapping[str, Any]) -> float | None:
    gates_prevalence = _get(gates_payload, "coverage_summary.positive_work_prevalence")
    if _is_number(gates_prevalence):
        return float(gates_prevalence)
    positive = _get(scoring_payload, "label_join_summary.labeled_eval_subset_positive_work_count")
    total = _get(scoring_payload, "label_join_summary.labeled_eval_subset_work_count")
    if _is_number(positive) and _is_number(total) and total > 0:
        return float(positive) / float(total)
    return None


def _evidence_summary(
    *,
    scoring_payload: Mapping[str, Any],
    gates_payload: Mapping[str, Any],
    assignment_payload: Mapping[str, Any],
) -> dict[str, Any]:
    thresholds = _get(gates_payload, "metadata.thresholds")
    if not isinstance(thresholds, Mapping):
        thresholds = {}
    comparison = _get(scoring_payload, "learned_or_embedding_metrics.comparison_to_heuristic")
    if not isinstance(comparison, Mapping):
        comparison = {}
    heuristic = {
        "roc_auc": _get(scoring_payload, "heuristic_metrics.roc_auc_mann_whitney"),
        "average_precision": _get(scoring_payload, "heuristic_metrics.average_precision"),
        "precision_at_5": _precision_at(scoring_payload, "heuristic_metrics", 5),
        "precision_at_10": _precision_at(scoring_payload, "heuristic_metrics", 10),
        "precision_at_20": _precision_at(scoring_payload, "heuristic_metrics", 20),
    }
    learned = {
        "roc_auc": _get(scoring_payload, "learned_or_embedding_metrics.metrics.roc_auc_mann_whitney"),
        "average_precision": _get(scoring_payload, "learned_or_embedding_metrics.metrics.average_precision"),
        "precision_at_5": _precision_at(scoring_payload, "learned_or_embedding_metrics.metrics", 5),
        "precision_at_10": _precision_at(scoring_payload, "learned_or_embedding_metrics.metrics", 10),
        "precision_at_20": _precision_at(scoring_payload, "learned_or_embedding_metrics.metrics", 20),
    }
    deltas = {
        "delta_roc_auc": _delta(learned["roc_auc"], heuristic["roc_auc"], comparison.get("delta_roc_auc")),
        "delta_average_precision": _delta(
            learned["average_precision"],
            heuristic["average_precision"],
            comparison.get("delta_average_precision"),
        ),
        "delta_precision_at_5": _delta(
            learned["precision_at_5"],
            heuristic["precision_at_5"],
            comparison.get("delta_precision_at_5"),
        ),
        "delta_precision_at_10": _delta(
            learned["precision_at_10"],
            heuristic["precision_at_10"],
            comparison.get("delta_precision_at_10"),
        ),
        "delta_precision_at_20": _delta(
            learned["precision_at_20"],
            heuristic["precision_at_20"],
            comparison.get("delta_precision_at_20"),
        ),
    }
    min_delta_roc = thresholds.get("minimum_delta_roc_auc_for_material_lift", 0.03)
    min_delta_ap = thresholds.get("minimum_delta_average_precision_for_material_lift", 0.02)
    roc_gap = float(min_delta_roc) - float(deltas["delta_roc_auc"]) if _is_number(deltas["delta_roc_auc"]) else None
    ap_gap = (
        float(min_delta_ap) - float(deltas["delta_average_precision"])
        if _is_number(deltas["delta_average_precision"])
        else None
    )
    return {
        "heuristic_metrics": heuristic,
        "holdout_learned_metrics": learned,
        "deltas_learned_minus_heuristic": deltas,
        "material_lift_thresholds": {
            "minimum_delta_roc_auc_for_material_lift": min_delta_roc,
            "minimum_delta_average_precision_for_material_lift": min_delta_ap,
        },
        "material_lift_gaps": {
            "roc_auc_gap_to_material_lift": roc_gap,
            "average_precision_gap_to_material_lift": ap_gap,
        },
        "positive_work_prevalence": _positive_prevalence(scoring_payload, gates_payload),
        "eval_work_count": _get(scoring_payload, "candidate_pool_summary.candidate_unique_canonical_work_count")
        or _get(assignment_payload, "metadata.eval_work_count"),
        "eval_work_set_sha256": _get(scoring_payload, "metadata.eval_work_set_sha256"),
        "leakage_zero_confirmation": {
            "scoring_train_rows_used_in_metrics": _get(scoring_payload, "leakage_report.train_rows_used_in_metrics"),
            "scoring_train_works_used_in_metrics": _get(scoring_payload, "leakage_report.train_works_used_in_metrics"),
            "assignment_global_zero_assertion": _get(assignment_payload, "leakage_report.global_zero_assertion"),
            "assignment_train_eval_work_overlap_count": _get(
                assignment_payload, "leakage_report.train_eval_work_overlap_count"
            ),
        },
        "independent_validation_passed_but_material_lift_failed": {
            "independent_learned_validation_passed": gates_payload.get("independent_learned_validation_passed"),
            "material_lift_passed": gates_payload.get("material_lift_passed"),
        },
    }


def _hybrid_experiment_rationale() -> dict[str, Any]:
    return {
        "heuristic_final_score_is_already_strong": True,
        "text_only_learned_scorer_roughly_matches_heuristic_without_material_lift": True,
        "why_hybrid_is_next": (
            "hybrid scoring is the natural next offline research step because final_score and learned probability may "
            "carry complementary signal"
        ),
        "not_shadow_readiness": True,
    }


def _feature_policy() -> dict[str, Any]:
    return {
        "allowed_features": [
            {
                "name": "final_score",
                "source": "frozen scoring v3 candidate pool",
            },
            {
                "name": "audit_embedding_probability_work",
                "source": "scoring v3 holdout scorer v2 application",
            },
        ],
        "allowed_label_blind_transforms": [
            {
                "name": "rank_percentile",
                "scope": "full candidate pool",
                "current_pool_work_count": 217,
                "same_pool_as_scoring_v3": True,
                "definition": _rank_pct_definition(),
            },
            {"name": "z_score", "scope": "full candidate pool"},
            {"name": "min_max_scaling", "scope": "full candidate pool"},
            {
                "name": "logit_audit_embedding_probability_work",
                "definition": "logit(audit_embedding_probability_work) with clipping",
            },
        ],
        "forbidden_features_or_methods": [
            "labels or derived targets as features",
            "reviewer_notes",
            "review_pool_variant",
            "sample_reason",
            "row_id",
            "assignment as predictive feature",
            "post-hoc transforms chosen using eval-label performance",
            "supervised fitting or weight search on eval labels",
            "DB/ranking writes",
        ],
    }


def _candidate_and_eval_policy(eval_sha: str) -> dict[str, Any]:
    return {
        "candidate_score_transforms_scope": "full scoring v3 candidate pool",
        "metrics_scope": "eval-assignment labeled works only",
        "all_arms_use_same_eval_work_set_and_label_denominators": True,
        "unlabeled_candidate_works_allowed_in_distribution_summaries": True,
        "unlabeled_candidate_works_excluded_from_label_metric_denominators": True,
        "positive_heavy_eval_interpretation": "P@k is advisory; ROC-AUC/AP carry more signal",
        "future_experiment_must_use_eval_work_set_sha256": eval_sha,
    }


def _pre_registered_hybrid_arms() -> list[dict[str, Any]]:
    return [
        {
            "arm_id": "heuristic_final_score_baseline",
            "score_formula": "final_score",
            "baseline": True,
        },
        {
            "arm_id": "holdout_embedding_probability_baseline",
            "score_formula": "audit_embedding_probability_work",
            "baseline": True,
        },
        {
            "arm_id": "hybrid_rank_mean_50_50",
            "score_formula": "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
            "baseline": False,
        },
        {
            "arm_id": "hybrid_rank_mean_75_25_heuristic",
            "score_formula": "0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)",
            "baseline": False,
        },
        {
            "arm_id": "hybrid_rank_mean_25_75_heuristic",
            "score_formula": "0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)",
            "baseline": False,
        },
    ]


def _rank_pct_definition() -> dict[str, Any]:
    return {
        "name": "rank_pct",
        "definition": "percentile rank within full candidate pool",
        "higher_score_is_better": True,
        "tie_policy": "deterministic average-rank or stable documented tie policy in future executable experiment",
    }


def _forbidden_designs() -> list[str]:
    return [
        "supervised hybrid on eval labels",
        "choosing weights after seeing v3/gates metrics and claiming validation",
        "picking the best pre-registered arm after seeing eval metrics and treating that as confirmatory validation",
        "using product-candidate eval labels to train a combiner",
        "shadow deployment",
        "production default change",
        "silent label conflict resolution",
    ]


def _future_experiment_contract() -> dict[str, Any]:
    return {
        "future_command": "ml-hybrid-scorer-offline-experiment",
        "not_implemented_here": True,
        "expected_inputs": [
            "ml-offline-production-candidate-scoring-v3.json",
            "ml-offline-production-candidate-metric-gates-v3.json",
            "ml-hybrid-scorer-offline-experiment-v1-spec.json",
            "ml-learned-scorer-holdout-assignment-v1.json",
        ],
        "expected_outputs": [
            "docs/audit/ml-hybrid-scorer-offline-experiment-v1.json",
            "docs/audit/ml-hybrid-scorer-offline-experiment-v1.md",
        ],
        "must": [
            "compute all pre-registered arms exactly",
            "use no supervised fitting",
            "compute rank percentiles on the full candidate pool",
            "report metrics for heuristic, learned, and hybrid arms on identical eval works",
            "report score distributions for all candidate works",
            "preserve not-validation caveats",
            "keep shadow/prod blocked",
        ],
    }


def _future_gate_contract() -> dict[str, Any]:
    return {
        "future_command": "ml-hybrid-scorer-metric-gates",
        "not_implemented_here": True,
        "material_lift_threshold": {
            "hybrid_beats_heuristic_by_roc_auc": 0.03,
            "or_hybrid_beats_heuristic_by_average_precision": 0.02,
        },
        "p_at_10_non_regression": "advisory if both arms are saturated at 1.0",
        "best_arm_on_seen_eval_is_exploratory_only": True,
        "shadow_remains_blocked_until_later_explicit_shadow_spec_after_stronger_evidence": True,
        "recommended_next_stage_if_no_lift": "collect_labels_or_features_or_new_eval_surface",
    }


def _shadow_and_production_blockers() -> dict[str, bool]:
    return {
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "missing_hybrid_experiment_results": True,
        "missing_hybrid_metric_gates": True,
        "missing_ml_shadow_scorer_v1": True,
        "no_production_model_artifact": True,
    }


def build_ml_hybrid_scorer_offline_experiment_spec_payload(
    *,
    production_candidate_scoring_path: Path,
    production_candidate_metric_gates_path: Path,
    holdout_assignment_path: Path,
    split_policy_path: Path,
    production_readiness_plan_path: Path,
    label_dataset_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    audit_embedding_scorer_export_path: Path | None = None,
    spec_version: str = SPEC_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    scoring_path = Path(production_candidate_scoring_path).resolve()
    gates_path = Path(production_candidate_metric_gates_path).resolve()
    assignment_path = Path(holdout_assignment_path).resolve()
    split_path = Path(split_policy_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    label_path = Path(label_dataset_path).resolve() if label_dataset_path else None
    holdout_policy_resolved = Path(holdout_policy_path).resolve() if holdout_policy_path else None
    scorer_path = Path(audit_embedding_scorer_export_path).resolve() if audit_embedding_scorer_export_path else None

    scoring_payload = _load_json_object(scoring_path)
    gates_payload = _load_json_object(gates_path)
    assignment_payload = _load_json_object(assignment_path)
    split_policy_payload = _load_json_object(split_path)
    production_plan_payload = _load_json_object(plan_path)

    scoring_metadata = _validate_scoring(scoring_payload)
    eval_sha = str(scoring_metadata.get("eval_work_set_sha256"))
    gates_metadata = _validate_gates(gates_payload, scoring_eval_sha=eval_sha)
    assignment_metadata = _validate_assignment(assignment_payload, scoring_eval_sha=eval_sha)
    split_policy_metadata = _validate_split_policy(split_policy_payload)
    plan_metadata = _validate_production_plan(production_plan_payload)

    inputs = [
        _input_record("production_candidate_scoring", scoring_path, repo_root=root),
        _input_record("production_candidate_metric_gates", gates_path, repo_root=root),
        _input_record("holdout_assignment", assignment_path, repo_root=root),
        _input_record("split_policy", split_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    label_dataset_version = None
    if label_path is not None:
        label_payload = _load_json_object(label_path)
        _validate_label_dataset(label_payload)
        label_dataset_version = label_payload.get("dataset_version")
        inputs.append(_input_record("label_dataset", label_path, repo_root=root))

    holdout_policy_version = None
    if holdout_policy_resolved is not None:
        holdout_policy_payload = _load_json_object(holdout_policy_resolved)
        holdout_policy_metadata = _validate_holdout_policy(holdout_policy_payload, scoring_eval_sha=eval_sha)
        holdout_policy_version = holdout_policy_metadata.get("policy_version")
        inputs.append(_input_record("holdout_policy", holdout_policy_resolved, repo_root=root))

    audit_scorer_version = None
    if scorer_path is not None:
        scorer_payload = _load_json_object(scorer_path)
        scorer_metadata = _validate_audit_scorer(scorer_payload, scoring_eval_sha=eval_sha)
        audit_scorer_version = scorer_metadata.get("scorer_version")
        inputs.append(_input_record("audit_embedding_scorer_export", scorer_path, repo_root=root))

    evidence_summary = _evidence_summary(
        scoring_payload=scoring_payload,
        gates_payload=gates_payload,
        assignment_payload=assignment_payload,
    )
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "spec_version": spec_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_scoring_version": scoring_metadata.get("experiment_version"),
        "source_metric_gates_version": gates_metadata.get("gates_version"),
        "holdout_assignment_version": assignment_metadata.get("assignment_version"),
        "split_policy_version": split_policy_metadata.get("policy_version"),
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "label_dataset_version": label_dataset_version,
        "holdout_policy_version": holdout_policy_version,
        "audit_embedding_scorer_version": audit_scorer_version,
        "eval_work_set_sha256": eval_sha,
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "evidence_summary": evidence_summary,
        "hybrid_experiment_rationale": _hybrid_experiment_rationale(),
        "feature_policy": _feature_policy(),
        "candidate_and_eval_policy": _candidate_and_eval_policy(eval_sha),
        "pre_registered_hybrid_arms": _pre_registered_hybrid_arms(),
        "forbidden_designs": _forbidden_designs(),
        "future_experiment_contract": _future_experiment_contract(),
        "future_gate_contract": _future_gate_contract(),
        "shadow_and_production_blockers": _shadow_and_production_blockers(),
        "interpretation": {
            "summary": (
                "Hybrid scorer offline experiment v1 is pre-registered because the holdout learned scorer passed "
                "validity and non-regression but did not materially beat the heuristic baseline."
            ),
            "next_authorized_step": "ml-hybrid-scorer-offline-experiment-v1",
            "not_claimed": [
                "hybrid scoring execution",
                "validation beyond the already observed v3 eval surface",
                "shadow readiness",
                "production readiness",
                "live recommender quality",
            ],
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_hybrid_scorer_offline_experiment_spec(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    evidence = payload["evidence_summary"]
    heuristic = evidence["heuristic_metrics"]
    learned = evidence["holdout_learned_metrics"]
    deltas = evidence["deltas_learned_minus_heuristic"]
    gaps = evidence["material_lift_gaps"]
    blockers = payload["shadow_and_production_blockers"]

    lines = [
        f"# Hybrid Scorer Offline Experiment Spec ({metadata['spec_version']})",
        "",
        "## Executive Summary",
        "",
        "This pre-registers the next offline hybrid scorer experiment after v3 gates. It does not run hybrid scoring, fit weights, or authorize shadow or production.",
        "",
        f"- **Eval work-set SHA:** `{metadata['eval_work_set_sha256']}`",
        f"- **Next authorized step:** `{payload['interpretation']['next_authorized_step']}`",
        f"- **Shadow scoring allowed:** {blockers['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {blockers['production_default_allowed']}",
        "",
        "## Why Hybrid Is Next",
        "",
        payload["hybrid_experiment_rationale"]["why_hybrid_is_next"].capitalize() + ".",
        "The heuristic final_score is already strong, and the text-only holdout learned scorer roughly matches it without material lift. A hybrid arm may expose complementary signal, but this is not shadow readiness.",
        "",
        "## Evidence Summary",
        "",
        "| Metric | Heuristic | Holdout learned | Delta |",
        "| --- | ---: | ---: | ---: |",
        f"| ROC-AUC | {_fmt(heuristic['roc_auc'])} | {_fmt(learned['roc_auc'])} | {_fmt(deltas['delta_roc_auc'])} |",
        f"| Average precision | {_fmt(heuristic['average_precision'])} | {_fmt(learned['average_precision'])} | {_fmt(deltas['delta_average_precision'])} |",
        f"| Precision@5 | {_fmt(heuristic['precision_at_5'])} | {_fmt(learned['precision_at_5'])} | {_fmt(deltas['delta_precision_at_5'])} |",
        f"| Precision@10 | {_fmt(heuristic['precision_at_10'])} | {_fmt(learned['precision_at_10'])} | {_fmt(deltas['delta_precision_at_10'])} |",
        f"| Precision@20 | {_fmt(heuristic['precision_at_20'])} | {_fmt(learned['precision_at_20'])} | {_fmt(deltas['delta_precision_at_20'])} |",
        "",
        f"Positive work prevalence: `{_fmt(evidence['positive_work_prevalence'])}`.",
        "",
        "## Material Lift Gaps",
        "",
        "| Gap | Value |",
        "| --- | ---: |",
        f"| ROC-AUC gap to material lift | {_fmt(gaps['roc_auc_gap_to_material_lift'])} |",
        f"| Average precision gap to material lift | {_fmt(gaps['average_precision_gap_to_material_lift'])} |",
        "",
        "## Allowed/Forbidden Feature Table",
        "",
        "| Type | Items |",
        "| --- | --- |",
        f"| Allowed features | {', '.join(item['name'] for item in payload['feature_policy']['allowed_features'])} |",
        f"| Label-blind transforms | {', '.join(item['name'] for item in payload['feature_policy']['allowed_label_blind_transforms'])} |",
        f"| Forbidden | {', '.join(payload['feature_policy']['forbidden_features_or_methods'])} |",
        "",
        "Rank percentiles are computed on the full candidate pool, not only labeled eval works.",
        "",
        "## Pre-Registered Hybrid Arms",
        "",
        "| Arm | Formula |",
        "| --- | --- |",
    ]
    for arm in payload["pre_registered_hybrid_arms"]:
        lines.append(f"| `{arm['arm_id']}` | `{arm['score_formula']}` |")

    lines.extend(
        [
            "",
            "## Candidate/Eval Policy",
            "",
            "- Candidate score transforms are computed over the full scoring v3 candidate pool.",
            "- Metrics are computed only on eval-assignment labeled works.",
            "- All arms must use the same eval work set and label denominators.",
            "- Unlabeled candidate works may appear in score-distribution summaries, not label-metric denominators.",
            "- Positive-heavy eval means P@k is advisory; ROC-AUC/AP carry more signal.",
            "",
            "## Future Experiment Contract",
            "",
            f"- **Command:** `{payload['future_experiment_contract']['future_command']}`",
            "- Compute all pre-registered arms exactly.",
            "- Use no supervised fitting.",
            "- Compute rank percentiles on the full candidate pool.",
            "- Report heuristic, learned, and hybrid metrics on identical eval works.",
            "",
            "## Future Gates Sketch",
            "",
            f"- **Command:** `{payload['future_gate_contract']['future_command']}`",
            "- Hybrid material lift requires ROC-AUC delta >= 0.03 or AP delta >= 0.02 versus heuristic.",
            "- P@10 non-regression is advisory when both arms are saturated at 1.0.",
            f"- Best arm on seen eval is exploratory only: {payload['future_gate_contract']['best_arm_on_seen_eval_is_exploratory_only']}",
            "- If no lift: `collect_labels_or_features_or_new_eval_surface`.",
            "",
            "## Forbidden Designs",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["forbidden_designs"])
    lines.extend(
        [
            "",
            "## Not Shadow / Not Production Caveats",
            "",
            f"- Missing hybrid experiment results: {blockers['missing_hybrid_experiment_results']}",
            f"- Missing hybrid metric gates: {blockers['missing_hybrid_metric_gates']}",
            f"- Missing `ml-shadow-scorer-v1`: {blockers['missing_ml_shadow_scorer_v1']}",
            f"- No production model artifact: {blockers['no_production_model_artifact']}",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_hybrid_scorer_offline_experiment_spec(
    *,
    production_candidate_scoring_path: Path,
    production_candidate_metric_gates_path: Path,
    holdout_assignment_path: Path,
    split_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    label_dataset_path: Path | None = None,
    holdout_policy_path: Path | None = None,
    audit_embedding_scorer_export_path: Path | None = None,
    spec_version: str = SPEC_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_hybrid_scorer_offline_experiment_spec_payload(
        production_candidate_scoring_path=production_candidate_scoring_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        holdout_assignment_path=holdout_assignment_path,
        split_policy_path=split_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        label_dataset_path=label_dataset_path,
        holdout_policy_path=holdout_policy_path,
        audit_embedding_scorer_export_path=audit_embedding_scorer_export_path,
        spec_version=spec_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_hybrid_scorer_offline_experiment_spec(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "SPEC_VERSION",
    "MLHybridScorerOfflineExperimentSpecError",
    "build_ml_hybrid_scorer_offline_experiment_spec_payload",
    "markdown_from_ml_hybrid_scorer_offline_experiment_spec",
    "write_ml_hybrid_scorer_offline_experiment_spec",
]

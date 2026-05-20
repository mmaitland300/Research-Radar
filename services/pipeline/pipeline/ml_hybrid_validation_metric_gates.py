"""Metric gates for fresh-surface hybrid validation.

This evaluator reads the executed fresh-surface hybrid validation artifact and
checks the frozen primary arm against the fresh-surface policy. It does not
query databases, rerun scoring, train, generate embeddings, import labels, or
authorize shadow/prod execution.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_hybrid_validation_metric_gates"
GATES_VERSION = "ml-hybrid-validation-metric-gates-v1"
HYBRID_VALIDATION_ARTIFACT_TYPE = "ml_hybrid_validation_on_fresh_surface"
HYBRID_VALIDATION_VERSION = "ml-hybrid-validation-on-fresh-surface-v1"
SURFACE_ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
SPEC_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment_spec"
SPEC_VERSION = "ml-hybrid-scorer-offline-experiment-v1-spec"
LABEL_DATASET_VERSION = "ml-label-dataset-v10"

PRIMARY_CONFIRMATORY_ARM = "hybrid_rank_mean_50_50"
EXPECTED_ARM_FORMULAS: tuple[tuple[str, str], ...] = (
    ("heuristic_final_score_baseline", "final_score"),
    ("holdout_embedding_probability_baseline", "audit_embedding_probability_work"),
    (
        "hybrid_rank_mean_50_50",
        "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
    ),
    (
        "hybrid_rank_mean_75_25_heuristic",
        "0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)",
    ),
    (
        "hybrid_rank_mean_25_75_heuristic",
        "0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)",
    ),
)
BASELINE_ARM_IDS = ("heuristic_final_score_baseline", "holdout_embedding_probability_baseline")
K_VALUES = (5, 10, 20)
DEFAULT_MIN_DELTA_ROC_AUC = 0.03
DEFAULT_MIN_DELTA_AVERAGE_PRECISION = 0.02
GATE_STATUS_ENUM = ("pass", "fail")

CAVEATS = (
    "Not live recommender validation.",
    "Fresh confirmatory surface; old 217 overlaps are excluded from confirmatory metrics.",
    "Frozen holdout scorer v2 applied without refit.",
    "Single-reviewer audit labels.",
    "Primary confirmatory arm is fixed at hybrid_rank_mean_50_50; best-arm selection remains exploratory.",
    "Passing these gates authorizes only drafting a shadow scorer spec.",
    "No shadow execution, production default, API/web change, or model deployment is authorized.",
)


class MLHybridValidationMetricGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLHybridValidationMetricGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLHybridValidationMetricGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLHybridValidationMetricGatesError(f"{name} JSON missing metadata object")
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


def _float_or_none(value: Any) -> float | None:
    if _is_number(value):
        return float(value)
    return None


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLHybridValidationMetricGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _gate(
    gate_id: str,
    title: str,
    category: str,
    passed: bool,
    threshold: Any,
    observed_value: Any,
    source_field_paths: Sequence[str],
    rationale: str,
    blocking_for: Sequence[str],
    required_for: Sequence[str],
) -> dict[str, Any]:
    status = "pass" if passed else "fail"
    return {
        "gate_id": gate_id,
        "title": title,
        "category": category,
        "status": status,
        "threshold": threshold,
        "observed_value": observed_value,
        "source_field_paths": list(source_field_paths),
        "rationale": rationale,
        "blocking_for": list(blocking_for),
        "required_for": list(required_for),
    }


def _validate_hybrid_validation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-validation-on-fresh-surface")
    if metadata.get("artifact_type") != HYBRID_VALIDATION_ARTIFACT_TYPE:
        raise MLHybridValidationMetricGatesError("hybrid validation metadata.artifact_type mismatch")
    if metadata.get("validation_version") != HYBRID_VALIDATION_VERSION:
        raise MLHybridValidationMetricGatesError("hybrid validation metadata.validation_version mismatch")
    if payload.get("recommended_next_stage") != "run_hybrid_validation_metric_gates_v1":
        raise MLHybridValidationMetricGatesError(
            "hybrid validation recommended_next_stage must be run_hybrid_validation_metric_gates_v1"
        )
    if _get(payload, "summary.confirmatory_validation_passed") is not False:
        raise MLHybridValidationMetricGatesError("validation summary.confirmatory_validation_passed must be false before gates")
    if _get(payload, "summary.confirmatory_validation_passed_reason") != "metric_gates_not_run":
        raise MLHybridValidationMetricGatesError(
            "validation summary.confirmatory_validation_passed_reason must be metric_gates_not_run"
        )
    if _get(payload, "confirmatory_decision_inputs.confirmatory_metrics_ready_for_gates") is not True:
        raise MLHybridValidationMetricGatesError("confirmatory_metrics_ready_for_gates must be true")
    if _get(payload, "confirmatory_decision_inputs.primary_confirmatory_arm") != PRIMARY_CONFIRMATORY_ARM:
        raise MLHybridValidationMetricGatesError("primary_confirmatory_arm must be hybrid_rank_mean_50_50")
    if _get(payload, "confirmatory_decision_inputs.best_arm_selection_is_exploratory_only") is not True:
        raise MLHybridValidationMetricGatesError("best_arm_selection_is_exploratory_only must be true")
    return metadata


def _validate_fresh_surface(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-surface")
    if metadata.get("artifact_type") != SURFACE_ARTIFACT_TYPE:
        raise MLHybridValidationMetricGatesError("fresh surface metadata.artifact_type mismatch")
    if metadata.get("surface_version") != SURFACE_VERSION:
        raise MLHybridValidationMetricGatesError("fresh surface metadata.surface_version mismatch")
    if metadata.get("status") != "materialized_ready":
        raise MLHybridValidationMetricGatesError("fresh surface metadata.status must be materialized_ready")
    if payload.get("ready_for_hybrid_validation_scoring") is not True:
        raise MLHybridValidationMetricGatesError("fresh surface ready_for_hybrid_validation_scoring must be true")
    if metadata.get("label_dataset_version") != LABEL_DATASET_VERSION:
        raise MLHybridValidationMetricGatesError("fresh surface metadata.label_dataset_version must be ml-label-dataset-v10")
    if metadata.get("expected_label_dataset_version") not in (None, LABEL_DATASET_VERSION):
        raise MLHybridValidationMetricGatesError(
            "fresh surface metadata.expected_label_dataset_version must be ml-label-dataset-v10 when present"
        )
    threshold_check = payload.get("threshold_check")
    if not isinstance(threshold_check, Mapping) or not threshold_check:
        raise MLHybridValidationMetricGatesError("fresh surface threshold_check must be a non-empty object")
    failed = [key for key, item in threshold_check.items() if not (isinstance(item, Mapping) and item.get("passed") is True)]
    if failed:
        raise MLHybridValidationMetricGatesError(f"fresh surface threshold_check entries failed: {failed[:10]}")
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, float]]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLHybridValidationMetricGatesError("fresh surface policy metadata.artifact_type mismatch")
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLHybridValidationMetricGatesError("fresh surface policy metadata.policy_version mismatch")
    if _get(payload, "frozen_hybrid_arms.primary_confirmatory_arm") != PRIMARY_CONFIRMATORY_ARM:
        raise MLHybridValidationMetricGatesError("policy frozen primary arm must be hybrid_rank_mean_50_50")
    if _get(payload, "frozen_hybrid_arms.secondary_reporting_arm") != "hybrid_rank_mean_25_75_heuristic":
        raise MLHybridValidationMetricGatesError(
            "policy frozen secondary reporting arm must be hybrid_rank_mean_25_75_heuristic"
        )
    thresholds = _get(payload, "gate_linkage.material_lift_thresholds")
    roc = DEFAULT_MIN_DELTA_ROC_AUC
    ap = DEFAULT_MIN_DELTA_AVERAGE_PRECISION
    if isinstance(thresholds, Mapping):
        if _is_number(thresholds.get("delta_roc_auc_gte")):
            roc = float(thresholds["delta_roc_auc_gte"])
        if _is_number(thresholds.get("or_delta_average_precision_gte")):
            ap = float(thresholds["or_delta_average_precision_gte"])
    return metadata, {
        "minimum_primary_delta_roc_auc_for_material_lift": roc,
        "minimum_primary_delta_average_precision_for_material_lift": ap,
    }


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLHybridValidationMetricGatesError("production readiness plan metadata.artifact_type mismatch")
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLHybridValidationMetricGatesError("production readiness plan metadata.plan_version mismatch")
    return metadata


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    good = _get(payload, "targets.good_or_acceptable")
    good_blocked = isinstance(good, Mapping) and good.get("production_eligible") is False
    default_unauthorized = payload.get("production_default_authorized") is not True
    return bool(good_blocked and default_unauthorized)


def _arm_formula_pairs(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    arms = payload.get("pre_registered_arms_executed") or payload.get("pre_registered_hybrid_arms")
    if not isinstance(arms, list):
        return []
    out: list[tuple[str, str]] = []
    for item in arms:
        if isinstance(item, Mapping):
            arm_id = str(item.get("arm_id") or "").strip()
            formula = str(item.get("score_formula") or "").strip()
            if arm_id:
                out.append((arm_id, formula))
    return out


def _validate_optional_spec(spec_payload: Mapping[str, Any], validation_payload: Mapping[str, Any]) -> None:
    metadata = _metadata(spec_payload, name="hybrid-experiment-spec")
    if metadata.get("artifact_type") != SPEC_ARTIFACT_TYPE:
        raise MLHybridValidationMetricGatesError("hybrid experiment spec metadata.artifact_type mismatch")
    if metadata.get("spec_version") != SPEC_VERSION:
        raise MLHybridValidationMetricGatesError("hybrid experiment spec metadata.spec_version mismatch")
    spec_pairs = _arm_formula_pairs({"pre_registered_arms_executed": spec_payload.get("pre_registered_hybrid_arms")})
    validation_pairs = _arm_formula_pairs(validation_payload)
    if spec_pairs != validation_pairs or validation_pairs != list(EXPECTED_ARM_FORMULAS):
        raise MLHybridValidationMetricGatesError("pre-registered arms do not match validation executed arms")


def _metric_for_arm(payload: Mapping[str, Any], arm_id: str) -> Mapping[str, Any]:
    metrics = _get(payload, f"arm_metrics.{arm_id}")
    return metrics if isinstance(metrics, Mapping) else {}


def _precision_at(metrics: Mapping[str, Any], k: int) -> float | None:
    return _float_or_none(_get(metrics, f"precision_recall_at_k.{k}.precision"))


def _metric_delta(value: Any, baseline: Any) -> float | None:
    left = _float_or_none(value)
    right = _float_or_none(baseline)
    if left is None or right is None:
        return None
    return left - right


def _primary_deltas(validation_payload: Mapping[str, Any]) -> dict[str, float | None]:
    heuristic = _metric_for_arm(validation_payload, "heuristic_final_score_baseline")
    primary = _metric_for_arm(validation_payload, PRIMARY_CONFIRMATORY_ARM)
    return {
        "delta_roc_auc": _metric_delta(primary.get("roc_auc_mann_whitney"), heuristic.get("roc_auc_mann_whitney")),
        "delta_average_precision": _metric_delta(primary.get("average_precision"), heuristic.get("average_precision")),
        "delta_precision_at_5": _metric_delta(_precision_at(primary, 5), _precision_at(heuristic, 5)),
        "delta_precision_at_10": _metric_delta(_precision_at(primary, 10), _precision_at(heuristic, 10)),
        "delta_precision_at_20": _metric_delta(_precision_at(primary, 20), _precision_at(heuristic, 20)),
    }


def _metric_completeness(metrics: Mapping[str, Any]) -> dict[str, bool]:
    checks = {
        "roc_auc_mann_whitney": _is_number(metrics.get("roc_auc_mann_whitney")),
        "average_precision": _is_number(metrics.get("average_precision")),
        "positive_work_count": _is_number(metrics.get("positive_work_count")),
        "negative_work_count": _is_number(metrics.get("negative_work_count")),
        "scored_labeled_work_count": _is_number(metrics.get("scored_labeled_work_count")),
    }
    for k in K_VALUES:
        checks[f"precision_at_{k}"] = _is_number(_precision_at(metrics, k))
    return checks


def _all_metric_completeness(validation_payload: Mapping[str, Any]) -> dict[str, dict[str, bool]]:
    return {
        "heuristic_final_score_baseline": _metric_completeness(
            _metric_for_arm(validation_payload, "heuristic_final_score_baseline")
        ),
        "holdout_embedding_probability_baseline": _metric_completeness(
            _metric_for_arm(validation_payload, "holdout_embedding_probability_baseline")
        ),
        PRIMARY_CONFIRMATORY_ARM: _metric_completeness(_metric_for_arm(validation_payload, PRIMARY_CONFIRMATORY_ARM)),
    }


def _top_k_non_regression(validation_payload: Mapping[str, Any]) -> dict[str, Any]:
    heuristic = _metric_for_arm(validation_payload, "heuristic_final_score_baseline")
    primary = _metric_for_arm(validation_payload, PRIMARY_CONFIRMATORY_ARM)
    observed: dict[str, Any] = {}
    passed = True
    for k in K_VALUES:
        h = _precision_at(heuristic, k)
        p = _precision_at(primary, k)
        ok = _is_number(h) and _is_number(p) and float(p) >= float(h)
        observed[f"precision_at_{k}"] = {"heuristic": h, "primary_hybrid": p, "passed": ok}
        passed = passed and ok
    return {"passed": passed, "observed": observed}


def _leakage_ok(validation_payload: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    observed = {
        "old_217_overlap_excluded_from_confirmatory_metrics": _get(
            validation_payload, "leakage_report.old_217_overlap_excluded_from_confirmatory_metrics"
        ),
        "confirmatory_rows_with_previous_eval_overlap_count": _get(
            validation_payload, "leakage_report.confirmatory_rows_with_previous_eval_overlap_count"
        ),
        "supervised_fit_used": _get(validation_payload, "leakage_report.supervised_fit_used"),
        "eval_label_weight_tuning_used": _get(validation_payload, "leakage_report.eval_label_weight_tuning_used"),
        "scorer_refit_used": _get(validation_payload, "leakage_report.scorer_refit_used"),
    }
    passed = (
        observed["old_217_overlap_excluded_from_confirmatory_metrics"] is True
        and observed["confirmatory_rows_with_previous_eval_overlap_count"] == 0
        and observed["supervised_fit_used"] is False
        and observed["eval_label_weight_tuning_used"] is False
        and observed["scorer_refit_used"] is False
    )
    return passed, observed


def _build_gates(
    *,
    validation_payload: Mapping[str, Any],
    surface_payload: Mapping[str, Any],
    policy_payload: Mapping[str, Any],
    production_plan_payload: Mapping[str, Any],
    thresholds: Mapping[str, float],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validation_metadata = _metadata(validation_payload, name="hybrid-validation")
    surface_metadata = _metadata(surface_payload, name="fresh-surface")
    policy_metadata = _metadata(policy_payload, name="fresh-policy")

    candidate_sha = str(_get(validation_payload, "metadata.candidate_pool_work_set_sha256") or "")
    surface_candidate_sha = str(_get(surface_payload, "candidate_pool.candidate_work_set_sha256") or "")
    old_sha = str(policy_metadata.get("disallowed_eval_work_set_sha256") or "")
    surface_thresholds = surface_payload.get("threshold_check")
    all_surface_thresholds_pass = isinstance(surface_thresholds, Mapping) and all(
        isinstance(item, Mapping) and item.get("passed") is True for item in surface_thresholds.values()
    )
    freshness_sha_ok = bool(candidate_sha and old_sha and candidate_sha != old_sha and surface_candidate_sha == candidate_sha)
    leakage_passed, leakage_observed = _leakage_ok(validation_payload)
    completeness = _all_metric_completeness(validation_payload)
    completeness_passed = all(all(item.values()) for item in completeness.values())
    deltas = _primary_deltas(validation_payload)
    material_passed = (
        (_is_number(deltas.get("delta_roc_auc")) and float(deltas["delta_roc_auc"]) >= thresholds["minimum_primary_delta_roc_auc_for_material_lift"])
        or (
            _is_number(deltas.get("delta_average_precision"))
            and float(deltas["delta_average_precision"]) >= thresholds["minimum_primary_delta_average_precision_for_material_lift"]
        )
    )
    top_k = _top_k_non_regression(validation_payload)
    primary_arm_ok = (
        _get(validation_payload, "confirmatory_decision_inputs.primary_confirmatory_arm") == PRIMARY_CONFIRMATORY_ARM
        and _get(policy_payload, "frozen_hybrid_arms.primary_confirmatory_arm") == PRIMARY_CONFIRMATORY_ARM
    )
    best_exploratory = _get(validation_payload, "confirmatory_decision_inputs.best_arm_selection_is_exploratory_only") is True
    production_blocked = _production_plan_blocked(production_plan_payload)
    shadow_prod_false = (
        validation_payload.get("shadow_scoring_allowed") is False
        and validation_payload.get("production_default_allowed") is False
        and _get(validation_payload, "shadow_and_production_blockers.shadow_scoring_allowed") is False
        and _get(validation_payload, "shadow_and_production_blockers.production_default_allowed") is False
    )

    g01 = (
        validation_metadata.get("validation_version") == HYBRID_VALIDATION_VERSION
        and surface_metadata.get("surface_version") == SURFACE_VERSION
        and policy_metadata.get("policy_version") == POLICY_VERSION
    )
    g02 = (
        surface_metadata.get("status") == "materialized_ready"
        and surface_payload.get("ready_for_hybrid_validation_scoring") is True
        and all_surface_thresholds_pass
    )
    g03 = leakage_passed and freshness_sha_ok
    g04 = primary_arm_ok
    g05 = completeness_passed
    g06 = material_passed
    g07 = bool(top_k["passed"])
    g08 = best_exploratory
    g09 = all([g01, g02, g03, g04, g05, g06, g07, g08])
    shadow_blockers = [
        "missing_ml_shadow_scorer_v1_spec",
        "missing_ml_shadow_scorer_v1_implementation",
        "production_default_blocked",
        "no_production_model_artifact",
    ]
    if not g09:
        shadow_blockers.insert(0, "confirmatory_validation_not_complete")
    g10 = shadow_prod_false and ("confirmatory_validation_not_complete" not in shadow_blockers if g09 else True)
    g11 = production_blocked

    gates = [
        _gate(
            "G01_input_scope",
            "Input Scope",
            "scope",
            g01,
            {"validation_version": HYBRID_VALIDATION_VERSION, "surface_version": SURFACE_VERSION, "policy_version": POLICY_VERSION},
            {
                "validation_version": validation_metadata.get("validation_version"),
                "surface_version": surface_metadata.get("surface_version"),
                "policy_version": policy_metadata.get("policy_version"),
            },
            ["validation.metadata.validation_version", "fresh_surface.metadata.surface_version", "policy.metadata.policy_version"],
            "The gates must evaluate the fresh-surface validation v1 artifact against policy v1.",
            ["confirmatory_validation"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G02_fresh_surface_readiness",
            "Fresh Surface Readiness",
            "surface",
            g02,
            {"status": "materialized_ready", "ready_for_hybrid_validation_scoring": True, "all_thresholds_passed": True},
            {
                "status": surface_metadata.get("status"),
                "ready_for_hybrid_validation_scoring": surface_payload.get("ready_for_hybrid_validation_scoring"),
                "threshold_check": surface_thresholds,
            },
            ["fresh_surface.metadata.status", "fresh_surface.ready_for_hybrid_validation_scoring", "fresh_surface.threshold_check"],
            "Fresh validation can only be confirmed on a materialized-ready surface with candidate and label thresholds met.",
            ["confirmatory_validation"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G03_leakage_and_freshness",
            "Leakage And Freshness",
            "leakage",
            g03,
            {
                "old_217_overlap_excluded_from_confirmatory_metrics": True,
                "confirmatory_overlap_count": 0,
                "candidate_pool_work_set_sha256_differs_from_old_217": True,
                "supervised_fit_used": False,
                "eval_label_weight_tuning_used": False,
                "scorer_refit_used": False,
            },
            {**leakage_observed, "candidate_pool_work_set_sha256": candidate_sha, "disallowed_eval_work_set_sha256": old_sha},
            ["validation.leakage_report", "validation.metadata.candidate_pool_work_set_sha256", "policy.metadata.disallowed_eval_work_set_sha256"],
            "Confirmatory metrics must exclude the old eval surface and avoid fitting, retuning, or scorer refit.",
            ["confirmatory_validation", "shadow_spec_readiness"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G04_frozen_primary_arm",
            "Frozen Primary Arm",
            "policy",
            g04,
            {"primary_confirmatory_arm": PRIMARY_CONFIRMATORY_ARM},
            {
                "validation_primary_arm": _get(validation_payload, "confirmatory_decision_inputs.primary_confirmatory_arm"),
                "policy_primary_arm": _get(policy_payload, "frozen_hybrid_arms.primary_confirmatory_arm"),
            },
            ["validation.confirmatory_decision_inputs.primary_confirmatory_arm", "policy.frozen_hybrid_arms.primary_confirmatory_arm"],
            "The primary confirmatory decision must use the frozen 50/50 arm, not a post-hoc best arm.",
            ["confirmatory_validation"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G05_metric_completeness",
            "Metric Completeness",
            "metrics",
            g05,
            {"required_metrics": ["ROC-AUC", "AP", "P@5", "P@10", "P@20", "positive/negative/scored counts"]},
            completeness,
            ["validation.arm_metrics.heuristic_final_score_baseline", "validation.arm_metrics.holdout_embedding_probability_baseline", f"validation.arm_metrics.{PRIMARY_CONFIRMATORY_ARM}"],
            "Heuristic, holdout learned, and primary hybrid metrics must all be present before gate evaluation.",
            ["confirmatory_validation"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G06_primary_material_lift_vs_heuristic",
            "Primary Material Lift Vs Heuristic",
            "metrics",
            g06,
            dict(thresholds),
            {"primary_arm": PRIMARY_CONFIRMATORY_ARM, "deltas": deltas},
            [f"validation.comparisons_vs_heuristic.{PRIMARY_CONFIRMATORY_ARM}", "validation.arm_metrics"],
            "The frozen primary hybrid arm must beat heuristic by the policy ROC-AUC or AP material-lift threshold.",
            ["confirmatory_validation", "shadow_spec_readiness"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G07_primary_top_k_non_regression",
            "Primary Top-K Non-Regression",
            "metrics",
            g07,
            {"primary_precision_at_k_gte_heuristic_for": list(K_VALUES)},
            top_k["observed"],
            [f"validation.arm_metrics.{PRIMARY_CONFIRMATORY_ARM}.precision_recall_at_k", "validation.arm_metrics.heuristic_final_score_baseline.precision_recall_at_k"],
            "Top-k precision should not regress for the primary hybrid arm on the fresh surface.",
            ["confirmatory_validation"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G08_best_arm_exploratory_only",
            "Best Arm Exploratory Only",
            "interpretation",
            g08,
            {"best_arm_selection_is_exploratory_only": True},
            {"best_arm_selection_is_exploratory_only": _get(validation_payload, "confirmatory_decision_inputs.best_arm_selection_is_exploratory_only")},
            ["validation.confirmatory_decision_inputs.best_arm_selection_is_exploratory_only"],
            "Best-arm reporting remains exploratory and cannot override the frozen primary arm.",
            ["shadow_spec_readiness", "production_default"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G09_confirmatory_validation_decision",
            "Confirmatory Validation Decision",
            "decision",
            g09,
            {"G01_through_G08_all_pass": True},
            {"G01_through_G08": {gate_id: value for gate_id, value in zip([f"G{i:02d}" for i in range(1, 9)], [g01, g02, g03, g04, g05, g06, g07, g08], strict=True)}},
            ["this.gates.G01_input_scope", "this.gates.G08_best_arm_exploratory_only"],
            "Confirmatory validation passes only if all scope, readiness, leakage, frozen-arm, metric, lift, and interpretation gates pass.",
            ["shadow_spec_readiness"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G10_shadow_and_production_blockers",
            "Shadow And Production Blockers",
            "policy",
            g10,
            {"shadow_scoring_allowed": False, "production_default_allowed": False},
            {
                "shadow_scoring_allowed": False,
                "production_default_allowed": False,
                "shadow_blockers": shadow_blockers,
            },
            ["validation.shadow_scoring_allowed", "validation.production_default_allowed", "this.shadow_blockers"],
            "Passing gates may authorize only drafting a shadow spec; execution and production remain blocked.",
            ["shadow_execution", "production_default"],
            ["hybrid_validation_metric_gates"],
        ),
        _gate(
            "G11_production_readiness_alignment",
            "Production Readiness Alignment",
            "policy",
            g11,
            {"good_or_acceptable.production_eligible": False, "production_default_authorized": False},
            {
                "good_or_acceptable.production_eligible": _get(production_plan_payload, "targets.good_or_acceptable.production_eligible"),
                "production_default_authorized": production_plan_payload.get("production_default_authorized", False),
            },
            ["production_readiness_plan.targets.good_or_acceptable.production_eligible", "production_readiness_plan.production_default_authorized"],
            "The production readiness plan must still block production default changes.",
            ["production_default"],
            ["hybrid_validation_metric_gates"],
        ),
    ]
    return gates, {
        "candidate_pool_work_set_sha256": candidate_sha,
        "disallowed_eval_work_set_sha256": old_sha,
        "primary_deltas": deltas,
        "primary_material_lift_passed": g06,
        "confirmatory_validation_passed": g09,
        "top_k_non_regression": top_k,
        "shadow_blockers": shadow_blockers,
        "production_readiness_blocked": production_blocked,
    }


def _failed_gate_ids(gates: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(gate["gate_id"]) for gate in gates if gate.get("status") == "fail"]


def _best_arm(payload: Mapping[str, Any], key: str) -> Any:
    return _get(payload, f"confirmatory_decision_inputs.{key}") or _get(payload, f"summary.{key}")


def build_ml_hybrid_validation_metric_gates_payload(
    *,
    hybrid_validation_on_fresh_surface_path: Path,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    output_gates_version: str = GATES_VERSION,
    hybrid_experiment_spec_path: Path | None = None,
    hybrid_scorer_metric_gates_path: Path | None = None,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    validation_path = Path(hybrid_validation_on_fresh_surface_path).resolve()
    surface_path = Path(fresh_eval_surface_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    spec_path = Path(hybrid_experiment_spec_path).resolve() if hybrid_experiment_spec_path is not None else None
    prior_gates_path = Path(hybrid_scorer_metric_gates_path).resolve() if hybrid_scorer_metric_gates_path is not None else None

    validation_payload = _load_json_object(validation_path)
    surface_payload = _load_json_object(surface_path)
    policy_payload = _load_json_object(policy_path)
    plan_payload = _load_json_object(plan_path)

    validation_metadata = _validate_hybrid_validation(validation_payload)
    surface_metadata = _validate_fresh_surface(surface_payload)
    policy_metadata, thresholds = _validate_policy(policy_payload)
    plan_metadata = _validate_production_plan(plan_payload)
    if spec_path is not None:
        _validate_optional_spec(_load_json_object(spec_path), validation_payload)
    if prior_gates_path is not None:
        _load_json_object(prior_gates_path)

    inputs = [
        _input_record("hybrid_validation_on_fresh_surface", validation_path, repo_root=root),
        _input_record("fresh_eval_surface", surface_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    if spec_path is not None:
        inputs.append(_input_record("hybrid_experiment_spec", spec_path, repo_root=root))
    if prior_gates_path is not None:
        inputs.append(_input_record("hybrid_scorer_metric_gates", prior_gates_path, repo_root=root))

    gates, gate_summary = _build_gates(
        validation_payload=validation_payload,
        surface_payload=surface_payload,
        policy_payload=policy_payload,
        production_plan_payload=plan_payload,
        thresholds=thresholds,
    )
    failed = _failed_gate_ids(gates)
    confirmatory_passed = bool(gate_summary["confirmatory_validation_passed"])
    recommended = "draft_ml_shadow_scorer_v1_spec" if confirmatory_passed else "collect_labels_features_or_new_eval_surface"
    heuristic_metrics = dict(_metric_for_arm(validation_payload, "heuristic_final_score_baseline"))
    holdout_metrics = dict(_metric_for_arm(validation_payload, "holdout_embedding_probability_baseline"))
    primary_metrics = dict(_metric_for_arm(validation_payload, PRIMARY_CONFIRMATORY_ARM))
    blocked_reasons = list(failed)
    if not confirmatory_passed:
        blocked_reasons.append("confirmatory_validation_not_complete")
    blocked_reasons.extend(["missing_ml_shadow_scorer_v1_spec", "production_default_blocked"])
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": output_gates_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_hybrid_validation_version": validation_metadata.get("validation_version"),
        "source_fresh_surface_version": surface_metadata.get("surface_version"),
        "source_policy_version": policy_metadata.get("policy_version"),
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "thresholds": dict(thresholds),
        "gate_status_enum": list(GATE_STATUS_ENUM),
        "candidate_pool_work_set_sha256": gate_summary["candidate_pool_work_set_sha256"],
        "disallowed_eval_work_set_sha256": gate_summary["disallowed_eval_work_set_sha256"],
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "gates": gates,
        "primary_confirmatory_arm": PRIMARY_CONFIRMATORY_ARM,
        "primary_hybrid_material_lift_passed": bool(gate_summary["primary_material_lift_passed"]),
        "confirmatory_validation_passed": confirmatory_passed,
        "fresh_surface_hybrid_validation_passed": confirmatory_passed,
        "best_arm_selection_is_exploratory_only": True,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": recommended,
        "blocked_reasons": blocked_reasons,
        "shadow_blockers": gate_summary["shadow_blockers"],
        "comparison_summary": {
            "heuristic_baseline_metrics": heuristic_metrics,
            "holdout_embedding_baseline_metrics": holdout_metrics,
            "primary_hybrid_arm": {
                "arm_id": PRIMARY_CONFIRMATORY_ARM,
                "metrics": primary_metrics,
                "deltas_vs_heuristic": gate_summary["primary_deltas"],
                "material_lift_passed": bool(gate_summary["primary_material_lift_passed"]),
            },
            "best_arm_by_roc_auc": _best_arm(validation_payload, "best_arm_by_roc_auc"),
            "best_arm_by_average_precision": _best_arm(validation_payload, "best_arm_by_average_precision"),
            "candidate_eval_coverage": dict(validation_payload.get("candidate_eval_coverage", {}))
            if isinstance(validation_payload.get("candidate_eval_coverage"), Mapping)
            else {},
            "label_join_summary": dict(validation_payload.get("label_join_summary", {}))
            if isinstance(validation_payload.get("label_join_summary"), Mapping)
            else {},
            "leakage_report": dict(validation_payload.get("leakage_report", {}))
            if isinstance(validation_payload.get("leakage_report"), Mapping)
            else {},
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "missing_ml_shadow_scorer_v1_spec": True,
            "missing_ml_shadow_scorer_v1_implementation": True,
            "production_default_blocked": True,
            "no_production_model_artifact": True,
            "confirmatory_validation_not_complete": not confirmatory_passed,
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def markdown_from_ml_hybrid_validation_metric_gates(payload: Mapping[str, Any]) -> str:
    meta = payload["metadata"]
    comparison = payload["comparison_summary"]
    heuristic = comparison["heuristic_baseline_metrics"]
    primary = comparison["primary_hybrid_arm"]
    primary_metrics = primary["metrics"]
    deltas = primary["deltas_vs_heuristic"]
    lines = [
        f"# Hybrid Validation Metric Gates ({meta['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This deterministic evaluator checks the frozen primary hybrid arm on the fresh 143-work confirmatory denominator. Passing these gates authorizes only drafting a shadow scorer spec; it does not authorize shadow execution or production default.",
        "",
        f"- **Primary confirmatory arm:** `{payload['primary_confirmatory_arm']}`",
        f"- **Primary material lift passed:** {payload['primary_hybrid_material_lift_passed']}",
        f"- **Confirmatory validation passed:** {payload['confirmatory_validation_passed']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['production_default_allowed']}",
        "",
        "## Gate Checklist",
        "",
        "| Gate | Status | Rationale |",
        "| --- | --- | --- |",
    ]
    for gate in payload["gates"]:
        lines.append(f"| {gate['gate_id']} {gate['title']} | {gate['status']} | {gate['rationale']} |")
    lines.extend(
        [
            "",
            "## Primary Hybrid Vs Heuristic",
            "",
            "| Metric | Heuristic | Primary hybrid | Delta |",
            "| --- | ---: | ---: | ---: |",
            f"| ROC-AUC | {_fmt(heuristic.get('roc_auc_mann_whitney'))} | {_fmt(primary_metrics.get('roc_auc_mann_whitney'))} | {_fmt(deltas.get('delta_roc_auc'))} |",
            f"| Average precision | {_fmt(heuristic.get('average_precision'))} | {_fmt(primary_metrics.get('average_precision'))} | {_fmt(deltas.get('delta_average_precision'))} |",
            f"| P@5 | {_fmt(_precision_at(heuristic, 5))} | {_fmt(_precision_at(primary_metrics, 5))} | {_fmt(deltas.get('delta_precision_at_5'))} |",
            f"| P@10 | {_fmt(_precision_at(heuristic, 10))} | {_fmt(_precision_at(primary_metrics, 10))} | {_fmt(deltas.get('delta_precision_at_10'))} |",
            f"| P@20 | {_fmt(_precision_at(heuristic, 20))} | {_fmt(_precision_at(primary_metrics, 20))} | {_fmt(deltas.get('delta_precision_at_20'))} |",
            "",
            "## Best Arm Note",
            "",
            f"- Best by ROC-AUC: `{_get(comparison, 'best_arm_by_roc_auc.arm_id')}`.",
            f"- Best by AP: `{_get(comparison, 'best_arm_by_average_precision.arm_id')}`.",
            "- Best-arm reporting is exploratory only; the confirmatory decision is based on `hybrid_rank_mean_50_50`.",
            "",
            "## Not Shadow / Not Production",
            "",
            "- Shadow scoring remains blocked.",
            "- Production default remains blocked.",
            "- A future `ml-shadow-scorer-v1` spec is the next authorized drafting step only if these gates pass.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in meta["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_hybrid_validation_metric_gates(
    *,
    hybrid_validation_on_fresh_surface_path: Path,
    fresh_eval_surface_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    hybrid_experiment_spec_path: Path | None = None,
    hybrid_scorer_metric_gates_path: Path | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_hybrid_validation_metric_gates_payload(
        hybrid_validation_on_fresh_surface_path=hybrid_validation_on_fresh_surface_path,
        fresh_eval_surface_path=fresh_eval_surface_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        output_gates_version=gates_version,
        hybrid_experiment_spec_path=hybrid_experiment_spec_path,
        hybrid_scorer_metric_gates_path=hybrid_scorer_metric_gates_path,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_hybrid_validation_metric_gates(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "GATES_VERSION",
    "MLHybridValidationMetricGatesError",
    "build_ml_hybrid_validation_metric_gates_payload",
    "markdown_from_ml_hybrid_validation_metric_gates",
    "write_ml_hybrid_validation_metric_gates",
]

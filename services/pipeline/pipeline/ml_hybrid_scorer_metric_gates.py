"""Hybrid scorer metric gate evaluator.

This command reads an already-executed hybrid scorer offline experiment and its
pre-registration spec. It does not train, fit, score, query databases, run
ranking, or authorize shadow/prod.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_hybrid_scorer_metric_gates"
GATES_VERSION = "ml-hybrid-scorer-metric-gates-v1"
THRESHOLDS_VERSION = "ml-hybrid-scorer-metric-gates-v1-thresholds"

EXPERIMENT_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment"
EXPERIMENT_VERSION = "ml-hybrid-scorer-offline-experiment-v1"
SPEC_ARTIFACT_TYPE = "ml_hybrid_scorer_offline_experiment_spec"
SPEC_VERSION = "ml-hybrid-scorer-offline-experiment-v1-spec"
PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
PRODUCT_CANDIDATE_GATES_VERSION = "ml-offline-production-candidate-metric-gates-v3"
SCORING_VERSION = "ml-offline-production-candidate-scoring-v3"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
HOLDOUT_ASSIGNMENT_VERSION = "ml-learned-scorer-holdout-assignment-v1"

EXPECTED_ARM_IDS = (
    "heuristic_final_score_baseline",
    "holdout_embedding_probability_baseline",
    "hybrid_rank_mean_50_50",
    "hybrid_rank_mean_75_25_heuristic",
    "hybrid_rank_mean_25_75_heuristic",
)
HYBRID_ARM_IDS = (
    "hybrid_rank_mean_50_50",
    "hybrid_rank_mean_75_25_heuristic",
    "hybrid_rank_mean_25_75_heuristic",
)
MATERIAL_LIFT_ROC_AUC = 0.03
MATERIAL_LIFT_AVERAGE_PRECISION = 0.02
POSITIVE_PREVALENCE_ADVISORY_THRESHOLD = 0.85
SHADOW_BLOCKERS = (
    "best_arm_on_seen_eval_exploratory_only",
    "confirmatory_validation_not_complete",
    "missing_ml_shadow_scorer_v1",
    "production_default_blocked",
    "no_production_model_artifact",
)
GATE_STATUS_ENUM = ("pass", "fail", "advisory_warn", "not_applicable")
CAVEATS = (
    "Not live recommender validation.",
    "Hybrid lift on already-seen eval surface.",
    "Best-arm selection exploratory.",
    "Single reviewer.",
    "One ranking run/family.",
    "Positive-heavy P@k.",
    "No shadow/production authorization.",
)


class MLHybridScorerMetricGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLHybridScorerMetricGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLHybridScorerMetricGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLHybridScorerMetricGatesError(f"{name} JSON missing metadata object")
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
        raise MLHybridScorerMetricGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_experiment(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="hybrid-experiment")
    if metadata.get("artifact_type") != EXPERIMENT_ARTIFACT_TYPE:
        raise MLHybridScorerMetricGatesError(
            f"expected experiment metadata.artifact_type={EXPERIMENT_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != EXPERIMENT_VERSION:
        raise MLHybridScorerMetricGatesError(
            f"expected experiment metadata.experiment_version={EXPERIMENT_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if not isinstance(_get(payload, "summary.hybrid_material_lift_passed"), bool):
        raise MLHybridScorerMetricGatesError("experiment summary.hybrid_material_lift_passed must be boolean")
    if not str(metadata.get("eval_work_set_sha256") or "").strip():
        raise MLHybridScorerMetricGatesError("experiment metadata.eval_work_set_sha256 must be present")
    return metadata


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="experiment-spec")
    if metadata.get("artifact_type") != SPEC_ARTIFACT_TYPE:
        raise MLHybridScorerMetricGatesError(
            f"expected spec metadata.artifact_type={SPEC_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("spec_version") != SPEC_VERSION:
        raise MLHybridScorerMetricGatesError(
            f"expected spec metadata.spec_version={SPEC_VERSION!r}, got {metadata.get('spec_version')!r}"
        )
    if _get(payload, "future_gate_contract.best_arm_on_seen_eval_is_exploratory_only") is not True:
        raise MLHybridScorerMetricGatesError(
            "spec future_gate_contract.best_arm_on_seen_eval_is_exploratory_only must be true"
        )
    return metadata


def _validate_product_candidate_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates")
    if metadata.get("artifact_type") != PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE:
        raise MLHybridScorerMetricGatesError(
            "expected production candidate gates metadata.artifact_type="
            f"{PRODUCT_CANDIDATE_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != PRODUCT_CANDIDATE_GATES_VERSION:
        raise MLHybridScorerMetricGatesError(
            f"expected production candidate gates metadata.gates_version={PRODUCT_CANDIDATE_GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("independent_learned_validation_passed") is not True:
        raise MLHybridScorerMetricGatesError("production candidate gates independent_learned_validation_passed must be true")
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLHybridScorerMetricGatesError(
            f"expected production plan metadata.artifact_type={PRODUCTION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLHybridScorerMetricGatesError(
            f"expected production plan metadata.plan_version={PRODUCTION_PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    return metadata


def _validate_assignment(payload: Mapping[str, Any], *, eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("assignment_version") != HOLDOUT_ASSIGNMENT_VERSION:
        raise MLHybridScorerMetricGatesError(
            f"expected assignment metadata.assignment_version={HOLDOUT_ASSIGNMENT_VERSION!r}, got {metadata.get('assignment_version')!r}"
        )
    if metadata.get("eval_work_set_sha256") != eval_sha:
        raise MLHybridScorerMetricGatesError("assignment metadata.eval_work_set_sha256 must match experiment/spec")
    return metadata


def _gate(
    gate_id: str,
    title: str,
    category: str,
    status: str,
    threshold: Any,
    observed_value: Any,
    source_field_paths: Sequence[str],
    rationale: str,
    blocking_for: Sequence[str],
    required_for: Sequence[str],
    *,
    advisory_text: str | None = None,
) -> dict[str, Any]:
    if status not in GATE_STATUS_ENUM:
        raise MLHybridScorerMetricGatesError(f"invalid gate status {status!r}")
    gate: dict[str, Any] = {
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
    if advisory_text:
        gate["advisory_text"] = advisory_text
    return gate


def _arm_ids_from_spec(spec_payload: Mapping[str, Any]) -> list[str]:
    arms = spec_payload.get("pre_registered_hybrid_arms")
    if not isinstance(arms, list):
        return []
    out: list[str] = []
    for item in arms:
        if isinstance(item, Mapping):
            arm_id = str(item.get("arm_id") or "").strip()
            if arm_id:
                out.append(arm_id)
    return out


def _arm_ids_from_experiment(experiment_payload: Mapping[str, Any]) -> list[str]:
    executed = experiment_payload.get("pre_registered_arms_executed")
    if isinstance(executed, list):
        out: list[str] = []
        for item in executed:
            if isinstance(item, Mapping):
                arm_id = str(item.get("arm_id") or "").strip()
                if arm_id:
                    out.append(arm_id)
            elif isinstance(item, str) and item.strip():
                out.append(item.strip())
        if out:
            return out
    arm_results = experiment_payload.get("arm_results")
    if isinstance(arm_results, Mapping):
        return sorted(str(key) for key in arm_results.keys())
    arm_metrics = experiment_payload.get("arm_metrics")
    if isinstance(arm_metrics, Mapping):
        return sorted(str(key) for key in arm_metrics.keys())
    return []


def _metrics_for_arm(experiment_payload: Mapping[str, Any], arm_id: str) -> Mapping[str, Any]:
    arm_metrics = experiment_payload.get("arm_metrics")
    if isinstance(arm_metrics, Mapping):
        metrics = arm_metrics.get(arm_id)
        if isinstance(metrics, Mapping):
            return metrics
    arm_results = experiment_payload.get("arm_results")
    if isinstance(arm_results, Mapping):
        result = arm_results.get(arm_id)
        if isinstance(result, Mapping):
            metrics = result.get("metrics")
            if isinstance(metrics, Mapping):
                return metrics
            return result
    return {}


def _comparison_for_arm(experiment_payload: Mapping[str, Any], arm_id: str) -> Mapping[str, Any]:
    comparisons = experiment_payload.get("comparisons_vs_heuristic")
    if isinstance(comparisons, Mapping):
        comparison = comparisons.get(arm_id)
        if isinstance(comparison, Mapping):
            return comparison
    return {}


def _precision_at(metrics: Mapping[str, Any], k: int) -> float | None:
    return _float_or_none(_get(metrics, f"precision_recall_at_k.{k}.precision"))


def _metric_delta(left: Any, right: Any) -> float | None:
    left_float = _float_or_none(left)
    right_float = _float_or_none(right)
    if left_float is None or right_float is None:
        return None
    return left_float - right_float


def _hybrid_arm_deltas(experiment_payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    heuristic = _metrics_for_arm(experiment_payload, "heuristic_final_score_baseline")
    out: dict[str, dict[str, Any]] = {}
    for arm_id in HYBRID_ARM_IDS:
        metrics = _metrics_for_arm(experiment_payload, arm_id)
        comparison = _comparison_for_arm(experiment_payload, arm_id)
        delta_roc = _float_or_none(comparison.get("delta_roc_auc"))
        if delta_roc is None:
            delta_roc = _metric_delta(metrics.get("roc_auc_mann_whitney"), heuristic.get("roc_auc_mann_whitney"))
        delta_ap = _float_or_none(comparison.get("delta_average_precision"))
        if delta_ap is None:
            delta_ap = _metric_delta(metrics.get("average_precision"), heuristic.get("average_precision"))
        material_flag = comparison.get("material_lift_passed_against_heuristic") is True
        material = material_flag or (
            (_is_number(delta_roc) and float(delta_roc) >= MATERIAL_LIFT_ROC_AUC)
            or (_is_number(delta_ap) and float(delta_ap) >= MATERIAL_LIFT_AVERAGE_PRECISION)
        )
        out[arm_id] = {
            "delta_roc_auc": delta_roc,
            "delta_average_precision": delta_ap,
            "delta_precision_at_5": comparison.get("delta_precision_at_5"),
            "delta_precision_at_10": comparison.get("delta_precision_at_10"),
            "delta_precision_at_20": comparison.get("delta_precision_at_20"),
            "material_lift_passed_against_heuristic": material,
        }
    return out


def _best_hybrid_arm(
    experiment_payload: Mapping[str, Any],
    *,
    metric_field: str,
) -> dict[str, Any] | None:
    best: tuple[float, str, Mapping[str, Any]] | None = None
    for arm_id in HYBRID_ARM_IDS:
        metrics = _metrics_for_arm(experiment_payload, arm_id)
        value = _float_or_none(metrics.get(metric_field))
        if value is None:
            continue
        candidate = (value, arm_id, metrics)
        if best is None or (candidate[0], candidate[1]) > (best[0], best[1]):
            best = candidate
    if best is None:
        return None
    return {
        "arm_id": best[1],
        metric_field: best[0],
        "metrics": dict(best[2]),
    }


def _production_plan_blocked(plan_payload: Mapping[str, Any]) -> bool:
    good = _get(plan_payload, "targets.good_or_acceptable")
    good_blocked = isinstance(good, Mapping) and good.get("production_eligible") is False
    default_unauthorized = plan_payload.get("production_default_authorized") is not True
    return bool(good_blocked and default_unauthorized)


def _build_gates(
    *,
    experiment_payload: Mapping[str, Any],
    spec_payload: Mapping[str, Any],
    product_candidate_gates_payload: Mapping[str, Any],
    production_plan_payload: Mapping[str, Any],
    eval_sha: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    exp_metadata = _metadata(experiment_payload, name="hybrid-experiment")
    spec_metadata = _metadata(spec_payload, name="experiment-spec")
    gates_metadata = _metadata(product_candidate_gates_payload, name="production-candidate-metric-gates")
    source_scoring_version = exp_metadata.get("source_scoring_version")
    source_metric_gates_version = exp_metadata.get("source_metric_gates_version")
    expected_ids = list(EXPECTED_ARM_IDS)
    spec_ids = _arm_ids_from_spec(spec_payload)
    executed_ids = _arm_ids_from_experiment(experiment_payload)
    arm_deltas = _hybrid_arm_deltas(experiment_payload)
    passing_arms = [
        arm_id
        for arm_id in HYBRID_ARM_IDS
        if arm_deltas.get(arm_id, {}).get("material_lift_passed_against_heuristic") is True
    ]
    hybrid_material_lift_passed = bool(passing_arms)
    exploratory = _get(experiment_payload, "summary.best_arm_selection_is_exploratory_only") is True
    supervised_fit_used = _get(experiment_payload, "leakage_report.supervised_fit_used")
    eval_label_weight_tuning_used = _get(experiment_payload, "leakage_report.eval_label_weight_tuning_used")
    no_supervised = supervised_fit_used is False and eval_label_weight_tuning_used is False
    production_blocked = _production_plan_blocked(production_plan_payload)
    best_by_roc = _best_hybrid_arm(experiment_payload, metric_field="roc_auc_mann_whitney")
    best_by_ap = _best_hybrid_arm(experiment_payload, metric_field="average_precision")
    best_hybrid = best_by_roc["metrics"] if best_by_roc else {}
    heuristic = _metrics_for_arm(experiment_payload, "heuristic_final_score_baseline")
    holdout = _metrics_for_arm(experiment_payload, "holdout_embedding_probability_baseline")
    positive_prevalence = _float_or_none(_get(experiment_payload, "candidate_eval_coverage.positive_work_prevalence"))
    heuristic_p10 = _precision_at(heuristic, 10)
    best_hybrid_p10 = _precision_at(best_hybrid, 10) if best_hybrid else None
    p10_saturated = (
        heuristic_p10 == 1.0
        and best_hybrid_p10 == 1.0
        and _is_number(positive_prevalence)
        and float(positive_prevalence) > POSITIVE_PREVALENCE_ADVISORY_THRESHOLD
    )
    arm_sets_match = (
        len(spec_ids) == len(expected_ids)
        and len(executed_ids) == len(expected_ids)
        and set(spec_ids) == set(expected_ids)
        and set(executed_ids) == set(expected_ids)
    )

    gates = [
        _gate(
            "G01_input_scope",
            "Input Scope",
            "scope",
            "pass"
            if (
                exp_metadata.get("experiment_version") == EXPERIMENT_VERSION
                and spec_metadata.get("spec_version") == SPEC_VERSION
                and source_scoring_version == SCORING_VERSION
                and source_metric_gates_version == PRODUCT_CANDIDATE_GATES_VERSION
            )
            else "fail",
            {
                "experiment_version": EXPERIMENT_VERSION,
                "spec_version": SPEC_VERSION,
                "source_scoring_version": SCORING_VERSION,
                "source_metric_gates_version": PRODUCT_CANDIDATE_GATES_VERSION,
            },
            {
                "experiment_version": exp_metadata.get("experiment_version"),
                "spec_version": spec_metadata.get("spec_version"),
                "source_scoring_version": source_scoring_version,
                "source_metric_gates_version": source_metric_gates_version,
            },
            [
                "experiment.metadata.experiment_version",
                "spec.metadata.spec_version",
                "experiment.metadata.source_scoring_version",
                "experiment.metadata.source_metric_gates_version",
            ],
            "The hybrid gates must evaluate the v1 experiment bound to scoring v3 and product-candidate gates v3.",
            ["hybrid_metric_gates"],
            ["hybrid_metric_gates"],
        ),
        _gate(
            "G02_no_supervised_fit",
            "No Supervised Fit",
            "leakage",
            "pass" if no_supervised else "fail",
            {"supervised_fit_used": False, "eval_label_weight_tuning_used": False},
            {
                "supervised_fit_used": supervised_fit_used,
                "eval_label_weight_tuning_used": eval_label_weight_tuning_used,
            },
            ["experiment.leakage_report.supervised_fit_used", "experiment.leakage_report.eval_label_weight_tuning_used"],
            "The hybrid experiment must be label-blind rank fusion, with no fitting or eval-label weight tuning.",
            ["hybrid_metric_gates", "shadow_scoring"],
            ["hybrid_metric_gates"],
        ),
        _gate(
            "G03_pre_registered_arms_executed",
            "Pre-Registered Arms Executed",
            "spec",
            "pass" if arm_sets_match else "fail",
            {"expected_arm_ids": expected_ids},
            {"spec_arm_ids": spec_ids, "executed_arm_ids": executed_ids},
            ["spec.pre_registered_hybrid_arms", "experiment.pre_registered_arms_executed", "experiment.arm_metrics"],
            "The executed arms must exactly match the pre-registered baseline and hybrid formulas.",
            ["hybrid_metric_gates"],
            ["hybrid_metric_gates"],
        ),
        _gate(
            "G04_hybrid_material_lift_vs_heuristic",
            "Hybrid Material Lift Vs Heuristic",
            "metrics",
            "pass" if hybrid_material_lift_passed else "fail",
            {
                "minimum_hybrid_delta_roc_auc_for_material_lift": MATERIAL_LIFT_ROC_AUC,
                "minimum_hybrid_delta_average_precision_for_material_lift": MATERIAL_LIFT_AVERAGE_PRECISION,
                "hybrid_arm_ids_only": list(HYBRID_ARM_IDS),
            },
            {"per_hybrid_arm_deltas": arm_deltas, "passing_arms": passing_arms},
            ["experiment.comparisons_vs_heuristic", "experiment.arm_metrics"],
            "Only pre-registered hybrid arms can satisfy the material-lift bar; baselines do not count.",
            ["shadow_scoring_readiness"],
            ["hybrid_material_lift"],
        ),
        _gate(
            "G05_best_arm_exploratory_only",
            "Best Arm Exploratory Only",
            "interpretation",
            "pass" if exploratory else "fail",
            {"best_arm_selection_is_exploratory_only": True},
            {"best_arm_selection_is_exploratory_only": _get(experiment_payload, "summary.best_arm_selection_is_exploratory_only")},
            ["experiment.summary.best_arm_selection_is_exploratory_only"],
            "Best-arm selection on the already-seen surface must not be treated as confirmatory validation.",
            ["shadow_scoring", "production_default"],
            ["hybrid_metric_gates"],
        ),
        _gate(
            "G06_confirmatory_validation_status",
            "Confirmatory Validation Status",
            "validation",
            "not_applicable",
            {"confirmatory_validation_required_for_shadow": True},
            {
                "confirmatory_validation_passed": False,
                "eval_surface_already_observed_in_scoring_v3": True,
                "fresh_holdout_not_defined": True,
            },
            ["experiment.metadata.eval_work_set_sha256", "spec.future_gate_contract"],
            "Material lift on the known surface is exploratory; confirmatory validation requires a new eval surface/holdout policy.",
            ["shadow_scoring", "production_default"],
            ["future_confirmatory_validation"],
        ),
        _gate(
            "G07_top_k_saturation_advisory",
            "Top-K Saturation Advisory",
            "advisory",
            "advisory_warn" if p10_saturated else "pass",
            {
                "heuristic_precision_at_10": 1.0,
                "best_hybrid_precision_at_10": 1.0,
                "positive_prevalence_gt": POSITIVE_PREVALENCE_ADVISORY_THRESHOLD,
            },
            {
                "heuristic_precision_at_10": heuristic_p10,
                "best_hybrid_precision_at_10": best_hybrid_p10,
                "positive_prevalence": positive_prevalence,
            },
            ["experiment.arm_metrics", "experiment.candidate_eval_coverage.positive_work_prevalence"],
            "P@10 saturation on a positive-heavy surface is advisory and does not fail gates.",
            [],
            [],
            advisory_text="P@10 is saturated for heuristic and best hybrid on a positive-heavy eval set." if p10_saturated else None,
        ),
        _gate(
            "G08_positive_prevalence_advisory",
            "Positive Prevalence Advisory",
            "advisory",
            "advisory_warn"
            if _is_number(positive_prevalence) and float(positive_prevalence) > POSITIVE_PREVALENCE_ADVISORY_THRESHOLD
            else "pass",
            {"positive_work_prevalence_max_without_advisory": POSITIVE_PREVALENCE_ADVISORY_THRESHOLD},
            {"positive_work_prevalence": positive_prevalence},
            ["experiment.candidate_eval_coverage.positive_work_prevalence"],
            "High positive prevalence can inflate P@k and makes top-k evidence advisory.",
            [],
            [],
            advisory_text="Positive work prevalence is above the advisory threshold."
            if _is_number(positive_prevalence) and float(positive_prevalence) > POSITIVE_PREVALENCE_ADVISORY_THRESHOLD
            else None,
        ),
        _gate(
            "G09_shadow_blockers_documented",
            "Shadow Blockers Documented",
            "policy",
            "pass",
            {"shadow_scoring_allowed": False, "required_shadow_blockers": list(SHADOW_BLOCKERS)},
            {"shadow_scoring_allowed": False, "shadow_blockers": list(SHADOW_BLOCKERS)},
            ["artifact.shadow_scoring_allowed", "artifact.shadow_blockers"],
            "Even with hybrid lift, this artifact keeps shadow blocked until confirmatory validation and a shadow contract exist.",
            ["shadow_scoring"],
            ["hybrid_metric_gates"],
        ),
        _gate(
            "G10_production_readiness_alignment",
            "Production Readiness Alignment",
            "policy",
            "pass" if production_blocked else "fail",
            {"good_or_acceptable.production_eligible": False, "production_default_authorized": False},
            {
                "good_or_acceptable.production_eligible": _get(
                    production_plan_payload, "targets.good_or_acceptable.production_eligible"
                ),
                "production_default_authorized": production_plan_payload.get("production_default_authorized", False),
            },
            ["production_readiness_plan.targets.good_or_acceptable.production_eligible"],
            "The production readiness plan must keep production/default behavior blocked.",
            ["production_default"],
            ["hybrid_metric_gates"],
        ),
    ]
    summaries = {
        "hybrid_material_lift_passed": hybrid_material_lift_passed,
        "hybrid_arms_passing_material_lift": passing_arms,
        "best_hybrid_arm_by_roc_auc": best_by_roc,
        "best_hybrid_arm_by_average_precision": best_by_ap,
        "best_hybrid_precision_at_10": best_hybrid_p10,
        "heuristic_precision_at_10": heuristic_p10,
        "positive_prevalence": positive_prevalence,
        "eval_work_count": _get(experiment_payload, "candidate_eval_coverage.labeled_eval_metric_work_count"),
        "eval_work_set_sha256": eval_sha,
        "heuristic_baseline_metrics": dict(heuristic),
        "holdout_embedding_baseline_metrics": dict(holdout),
        "product_candidate_gates_independent_validation_passed": product_candidate_gates_payload.get(
            "independent_learned_validation_passed"
        ),
        "source_metric_gates_version": gates_metadata.get("gates_version"),
    }
    return gates, summaries


def _thresholds(*, heuristic_p10: float | None, best_hybrid_p10: float | None) -> dict[str, Any]:
    return {
        "minimum_hybrid_delta_roc_auc_for_material_lift": MATERIAL_LIFT_ROC_AUC,
        "minimum_hybrid_delta_average_precision_for_material_lift": MATERIAL_LIFT_AVERAGE_PRECISION,
        "high_positive_work_prevalence_advisory_threshold": POSITIVE_PREVALENCE_ADVISORY_THRESHOLD,
        "p_at_10_saturated_advisory": heuristic_p10 == 1.0 and best_hybrid_p10 == 1.0,
    }


def _failed_gate_ids(gates: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(gate["gate_id"]) for gate in gates if gate.get("status") == "fail"]


def build_ml_hybrid_scorer_metric_gates_payload(
    *,
    hybrid_experiment_path: Path,
    experiment_spec_path: Path,
    production_candidate_metric_gates_path: Path,
    production_readiness_plan_path: Path,
    output_gates_version: str = GATES_VERSION,
    holdout_assignment_path: Path | None = None,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    experiment_path = Path(hybrid_experiment_path).resolve()
    spec_path = Path(experiment_spec_path).resolve()
    product_gates_path = Path(production_candidate_metric_gates_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    assignment_path = Path(holdout_assignment_path).resolve() if holdout_assignment_path else None

    experiment_payload = _load_json_object(experiment_path)
    spec_payload = _load_json_object(spec_path)
    product_gates_payload = _load_json_object(product_gates_path)
    plan_payload = _load_json_object(plan_path)

    experiment_metadata = _validate_experiment(experiment_payload)
    spec_metadata = _validate_spec(spec_payload)
    product_gates_metadata = _validate_product_candidate_gates(product_gates_payload)
    plan_metadata = _validate_production_plan(plan_payload)
    eval_sha = str(experiment_metadata.get("eval_work_set_sha256"))
    if spec_metadata.get("eval_work_set_sha256") != eval_sha:
        raise MLHybridScorerMetricGatesError("spec metadata.eval_work_set_sha256 must match experiment")
    gates_eval_sha = product_gates_metadata.get("eval_work_set_sha256")
    if gates_eval_sha is not None and gates_eval_sha != eval_sha:
        raise MLHybridScorerMetricGatesError("production candidate gates eval_work_set_sha256 must match experiment")

    inputs = [
        _input_record("hybrid_experiment", experiment_path, repo_root=root),
        _input_record("experiment_spec", spec_path, repo_root=root),
        _input_record("production_candidate_metric_gates", product_gates_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    assignment_version = None
    if assignment_path is not None:
        assignment_payload = _load_json_object(assignment_path)
        assignment_metadata = _validate_assignment(assignment_payload, eval_sha=eval_sha)
        assignment_version = assignment_metadata.get("assignment_version")
        inputs.append(_input_record("holdout_assignment", assignment_path, repo_root=root))

    gates, summaries = _build_gates(
        experiment_payload=experiment_payload,
        spec_payload=spec_payload,
        product_candidate_gates_payload=product_gates_payload,
        production_plan_payload=plan_payload,
        eval_sha=eval_sha,
    )
    failed_gate_ids = _failed_gate_ids(gates)
    hybrid_material_lift_passed = summaries["hybrid_material_lift_passed"]
    confirmatory_validation_passed = False
    recommended = (
        "create_fresh_eval_surface_for_hybrid_validation_v1"
        if hybrid_material_lift_passed and not confirmatory_validation_passed
        else "collect_labels_or_features_or_new_eval_surface"
    )
    blocked_reasons = list(failed_gate_ids)
    blocked_reasons.extend(
        [
            "confirmatory_validation_not_complete",
            "best_arm_exploratory_only",
            "missing_ml_shadow_scorer_v1",
            "production_default_blocked",
        ]
    )
    best_hybrid_metrics = (
        summaries["best_hybrid_arm_by_roc_auc"]["metrics"]
        if isinstance(summaries.get("best_hybrid_arm_by_roc_auc"), Mapping)
        else {}
    )
    comparison_to_heuristic = (
        _comparison_for_arm(experiment_payload, str(summaries["best_hybrid_arm_by_roc_auc"]["arm_id"]))
        if isinstance(summaries.get("best_hybrid_arm_by_roc_auc"), Mapping)
        else {}
    )
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": output_gates_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_hybrid_experiment_version": experiment_metadata.get("experiment_version"),
        "source_experiment_spec_version": spec_metadata.get("spec_version"),
        "source_product_candidate_metric_gates_version": product_gates_metadata.get("gates_version"),
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "holdout_assignment_version": assignment_version,
        "eval_work_set_sha256": eval_sha,
        "thresholds_version": THRESHOLDS_VERSION,
        "thresholds": _thresholds(
            heuristic_p10=summaries["heuristic_precision_at_10"],
            best_hybrid_p10=summaries["best_hybrid_precision_at_10"],
        ),
        "gate_status_enum": list(GATE_STATUS_ENUM),
        "strategic_framing": {
            "hybrid_material_lift_encouraging_but_exploratory": bool(hybrid_material_lift_passed),
            "passing_hybrid_gates_does_not_authorize_shadow": True,
            "next_step_fresh_eval_surface_not_deployment": True,
        },
        "caveats": list(CAVEATS),
    }
    shadow_blockers = list(SHADOW_BLOCKERS)
    return {
        "metadata": metadata,
        "gates": gates,
        "hybrid_material_lift_passed": hybrid_material_lift_passed,
        "best_arm_selection_is_exploratory_only": True,
        "confirmatory_validation_passed": confirmatory_validation_passed,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "best_hybrid_arm_by_roc_auc": summaries["best_hybrid_arm_by_roc_auc"],
        "best_hybrid_arm_by_average_precision": summaries["best_hybrid_arm_by_average_precision"],
        "hybrid_arms_passing_material_lift": summaries["hybrid_arms_passing_material_lift"],
        "recommended_next_stage": recommended,
        "blocked_reasons": blocked_reasons,
        "shadow_blockers": shadow_blockers,
        "comparison_summary": {
            "heuristic_baseline_metrics": summaries["heuristic_baseline_metrics"],
            "holdout_embedding_baseline_metrics": summaries["holdout_embedding_baseline_metrics"],
            "best_hybrid_arm": {
                "arm_id": summaries["best_hybrid_arm_by_roc_auc"]["arm_id"]
                if isinstance(summaries.get("best_hybrid_arm_by_roc_auc"), Mapping)
                else None,
                "metrics": best_hybrid_metrics,
                "deltas_vs_heuristic": dict(comparison_to_heuristic),
            },
            "deltas": dict(comparison_to_heuristic),
            "hybrid_arms_passing_material_lift": summaries["hybrid_arms_passing_material_lift"],
            "positive_prevalence": summaries["positive_prevalence"],
            "eval_work_count": summaries["eval_work_count"],
            "eval_work_set_sha256": eval_sha,
            "interpretation_note": (
                "v2 full-fit ROC 1.0 was overlap-inflated while hybrid 50_50 ROC 0.846 is honest "
                "rank fusion on the same known surface."
            ),
        },
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_not_complete": True,
            "best_arm_on_seen_eval_exploratory_only": True,
            "missing_ml_shadow_scorer_v1": True,
            "production_default_blocked": True,
            "no_production_model_artifact": True,
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_hybrid_scorer_metric_gates(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    comparison = payload["comparison_summary"]
    best = payload["best_hybrid_arm_by_roc_auc"]
    best_ap = payload["best_hybrid_arm_by_average_precision"]
    best_metrics = best["metrics"] if isinstance(best, Mapping) and isinstance(best.get("metrics"), Mapping) else {}
    deltas = comparison["deltas"]
    lines = [
        f"# Hybrid Scorer Metric Gates ({metadata['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This deterministic evaluator checks whether pre-registered hybrid arms clear material lift on the already-seen v3 eval surface. Passing these gates does not authorize shadow or production.",
        "",
        f"- **Hybrid material lift passed:** {payload['hybrid_material_lift_passed']}",
        f"- **Confirmatory validation passed:** {payload['confirmatory_validation_passed']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['production_default_allowed']}",
        "",
        "## Three-Decision Table",
        "",
        "| Decision | Result | Meaning |",
        "| --- | --- | --- |",
        f"| Material lift | {payload['hybrid_material_lift_passed']} | At least one pre-registered hybrid arm beats heuristic by ROC-AUC >= 0.03 or AP >= 0.02. |",
        f"| Best-arm exploratory | {payload['best_arm_selection_is_exploratory_only']} | Best-arm choice happened on an already-seen eval surface. |",
        f"| Confirmatory validation | {payload['confirmatory_validation_passed']} | Fresh eval surface has not been defined or evaluated. |",
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
            "## Best Hybrid Vs Heuristic",
            "",
            "| Metric | Heuristic | Best hybrid | Delta |",
            "| --- | ---: | ---: | ---: |",
            f"| ROC-AUC | {_fmt(_get(comparison, 'heuristic_baseline_metrics.roc_auc_mann_whitney'))} | {_fmt(best_metrics.get('roc_auc_mann_whitney'))} | {_fmt(deltas.get('delta_roc_auc'))} |",
            f"| Average precision | {_fmt(_get(comparison, 'heuristic_baseline_metrics.average_precision'))} | {_fmt(best_metrics.get('average_precision'))} | {_fmt(deltas.get('delta_average_precision'))} |",
            f"| P@10 | {_fmt(_precision_at(comparison['heuristic_baseline_metrics'], 10))} | {_fmt(_precision_at(best_metrics, 10))} | {_fmt(deltas.get('delta_precision_at_10'))} |",
            "",
            f"Best hybrid by ROC-AUC: `{best['arm_id']}`. Best hybrid by AP: `{best_ap['arm_id']}`.",
            "",
            "## Arms Passing Lift",
            "",
        ]
    )
    passing = payload["hybrid_arms_passing_material_lift"]
    if passing:
        lines.extend(f"- `{arm_id}`" for arm_id in passing)
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Exploratory Warning",
            "",
            "This is an encouraging offline diagnostic on a known eval surface. It is not confirmatory validation, and the chosen best arm must be checked on a fresh eval surface before any shadow-spec work.",
            "",
            "## Not Shadow / Not Production",
            "",
            f"- Shadow scoring allowed: {payload['shadow_scoring_allowed']}",
            f"- Production default allowed: {payload['production_default_allowed']}",
            "- Missing `ml-shadow-scorer-v1` contract.",
            "- No production model artifact.",
            "",
            "## Recommended Next Stage",
            "",
            f"`{payload['recommended_next_stage']}`",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_hybrid_scorer_metric_gates(
    *,
    hybrid_experiment_path: Path,
    experiment_spec_path: Path,
    production_candidate_metric_gates_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    holdout_assignment_path: Path | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_hybrid_scorer_metric_gates_payload(
        hybrid_experiment_path=hybrid_experiment_path,
        experiment_spec_path=experiment_spec_path,
        production_candidate_metric_gates_path=production_candidate_metric_gates_path,
        production_readiness_plan_path=production_readiness_plan_path,
        output_gates_version=gates_version,
        holdout_assignment_path=holdout_assignment_path,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_hybrid_scorer_metric_gates(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "GATES_VERSION",
    "MLHybridScorerMetricGatesError",
    "build_ml_hybrid_scorer_metric_gates_payload",
    "markdown_from_ml_hybrid_scorer_metric_gates",
    "write_ml_hybrid_scorer_metric_gates",
]

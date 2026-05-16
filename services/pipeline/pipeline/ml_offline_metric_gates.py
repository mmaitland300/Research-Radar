"""Offline metric gate evaluator for audit-pool ranker experiments.

This is a deterministic spec/evidence artifact writer. It reads existing JSON
artifacts and evaluates fixed thresholds. It does not train, score new rows,
call network services, access Postgres, or change production behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_offline_metric_gates"
GATES_VERSION = "ml-offline-metric-gates-v1"
RANKER_ARTIFACT_TYPE = "ml_offline_ranker_experiment"
RANKER_VERSION = "ml-offline-ranker-experiment-v1"
SPLIT_POLICY_ARTIFACT_TYPE = "ml_label_split_policy"
SPLIT_POLICY_VERSION = "ml-label-split-policy-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
TRANSFER_READINESS_ARTIFACT_TYPE = "ml_text_transfer_readiness"
TRANSFER_READINESS_VERSION = "ml-text-transfer-readiness-v8"
TARGET = "good_or_acceptable"
FORBIDDEN_TARGET = "surprising_or_useful"

THRESHOLDS_VERSION = "ml-offline-metric-gates-v1-thresholds"
THRESHOLDS: dict[str, float | int] = {
    "minimum_positive_work_groups": 2,
    "minimum_negative_work_groups": 2,
    "minimum_eligible_observations": 300,
    "minimum_observation_positive_count": 50,
    "minimum_observation_negative_count": 30,
    "minimum_work_any_positive_count": 40,
    "minimum_work_any_negative_count": 20,
    "majority_baseline_balanced_accuracy_margin": 0.15,
    "embedding_logistic_roc_auc_floor": 0.70,
    "embedding_logistic_average_precision_floor": 0.75,
    "embedding_logistic_balanced_accuracy_std_ceiling": 0.08,
}

CAVEATS = (
    "Not validation.",
    "Audit pools only.",
    "Single-reviewer labels.",
    "Production blocked.",
    "Shadow blocked.",
    "No ranking/API/web changes.",
)

REQUIRED_AUDIT_GATE_IDS = (
    "G01_target_scope",
    "G02_policy_compliance",
    "G03_leakage_zero",
    "G04_minimum_work_groups",
    "G05_class_balance_floor",
    "G06_majority_baseline_margin",
    "G07_roc_auc_floor",
    "G08_average_precision_floor",
    "G09_fold_stability",
    "G10_duplicate_pressure_reported",
    "G11_audit_pool_scope_acknowledged",
    "G13_production_readiness_plan_alignment",
)

BLOCKING_AUDIT_GATE_IDS_EXCEPT_G05 = tuple(gate for gate in REQUIRED_AUDIT_GATE_IDS if gate != "G05_class_balance_floor")


class MLOfflineMetricGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineMetricGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineMetricGatesError(f"Expected JSON object in {path}")
    return payload


def _input_record(name: str, path: Path, *, repo_root: Path | None = None) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLOfflineMetricGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLOfflineMetricGatesError(f"{name} JSON missing metadata object")
    return metadata


def _validate_ranker(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="ranker-experiment")
    if metadata.get("artifact_type") != RANKER_ARTIFACT_TYPE:
        raise MLOfflineMetricGatesError(
            f"expected ranker metadata.artifact_type={RANKER_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != RANKER_VERSION:
        raise MLOfflineMetricGatesError(
            f"expected ranker metadata.experiment_version={RANKER_VERSION!r}, got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("target") != TARGET:
        raise MLOfflineMetricGatesError(f"expected ranker metadata.target={TARGET!r}, got {metadata.get('target')!r}")
    return metadata


def _validate_split_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineMetricGatesError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLOfflineMetricGatesError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLOfflineMetricGatesError(
            "expected production-readiness-plan metadata.artifact_type="
            f"{PRODUCTION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLOfflineMetricGatesError(
            f"expected production-readiness-plan metadata.plan_version={PRODUCTION_PLAN_VERSION!r}, "
            f"got {metadata.get('plan_version')!r}"
        )
    return metadata


def _validate_transfer_readiness(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="transfer-readiness")
    if metadata.get("artifact_type") != TRANSFER_READINESS_ARTIFACT_TYPE:
        raise MLOfflineMetricGatesError(
            "expected transfer-readiness metadata.artifact_type="
            f"{TRANSFER_READINESS_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("readiness_version") != TRANSFER_READINESS_VERSION:
        raise MLOfflineMetricGatesError(
            f"expected transfer-readiness metadata.readiness_version={TRANSFER_READINESS_VERSION!r}, "
            f"got {metadata.get('readiness_version')!r}"
        )
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _is_true(value: Any) -> bool:
    return value is True


def _is_false(value: Any) -> bool:
    return value is False


def _status(condition: bool) -> str:
    return "pass" if condition else "fail"


def _gate(
    gate_id: str,
    *,
    title: str,
    category: str,
    status: str,
    threshold: Any,
    observed_value: Any,
    source_field_paths: Sequence[str],
    rationale: str,
    blocking_for: Sequence[str],
    required_for: Sequence[str],
    advisory_text: str | None = None,
    next_action: str | None = None,
) -> dict[str, Any]:
    out = {
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
    if advisory_text is not None:
        out["advisory_text"] = advisory_text
    if next_action is not None:
        out["next_action"] = next_action
    return out


def _nonzero_pool_count(ranker_payload: Mapping[str, Any]) -> int:
    counts = _get(ranker_payload, "dataset_inventory.review_pool_variant_counts")
    if not isinstance(counts, Mapping):
        return 0
    total = 0
    for value in counts.values():
        if isinstance(value, (int, float)) and value > 0:
            total += 1
    return total


def _experiment_scope(ranker_payload: Mapping[str, Any]) -> str:
    if _nonzero_pool_count(ranker_payload) > 1:
        return "audit_pool_offline_ranker"
    explicit = _get(ranker_payload, "metadata.experiment_scope")
    if explicit:
        return str(explicit)
    return "single_pool_offline_ranker"


def _metric_path(model: str, metric: str, stat: str) -> str:
    return f"models.{model}.aggregate.observation_metrics_mean_std.{metric}.{stat}"


def _build_gates(
    *,
    ranker_payload: Mapping[str, Any],
    split_policy_payload: Mapping[str, Any],
    production_plan_payload: Mapping[str, Any],
    transfer_payload: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []

    g01_observed = {
        "target": _get(ranker_payload, "metadata.target"),
        "allowed_targets_for_v1_split": split_policy_payload.get("allowed_targets_for_v1_split"),
        "forbidden_targets": split_policy_payload.get("forbidden_targets"),
    }
    g01_pass = (
        g01_observed["target"] == TARGET
        and g01_observed["allowed_targets_for_v1_split"] == [TARGET]
        and FORBIDDEN_TARGET in (g01_observed["forbidden_targets"] or [])
    )
    gates.append(
        _gate(
            "G01_target_scope",
            title="Target Scope",
            category="policy",
            status=_status(g01_pass),
            threshold={"target": TARGET, "allowed_targets_for_v1_split": [TARGET], "forbidden_targets_include": FORBIDDEN_TARGET},
            observed_value=g01_observed,
            source_field_paths=[
                "ranker.metadata.target",
                "split_policy.allowed_targets_for_v1_split",
                "split_policy.forbidden_targets",
            ],
            rationale="v1 gates only evaluate good_or_acceptable and require surprising_or_useful to remain excluded.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    g02_observed = {
        "grouped_split_used": _get(ranker_payload, "policy_compliance.grouped_split_used"),
        "row_level_random_split_used": _get(ranker_payload, "policy_compliance.row_level_random_split_used"),
        "production_artifact_written": _get(ranker_payload, "policy_compliance.production_artifact_written"),
        "leakage_checks_passed": _get(ranker_payload, "policy_compliance.leakage_checks_passed"),
        "allowed_target_verified": _get(ranker_payload, "policy_compliance.allowed_target_verified"),
        "forbidden_targets_verified": _get(ranker_payload, "policy_compliance.forbidden_targets_verified"),
    }
    g02_pass = (
        _is_true(g02_observed["grouped_split_used"])
        and _is_false(g02_observed["row_level_random_split_used"])
        and _is_false(g02_observed["production_artifact_written"])
        and _is_true(g02_observed["leakage_checks_passed"])
        and _is_true(g02_observed["allowed_target_verified"])
        and _is_true(g02_observed["forbidden_targets_verified"])
    )
    gates.append(
        _gate(
            "G02_policy_compliance",
            title="Policy Compliance",
            category="policy",
            status=_status(g02_pass),
            threshold={
                "grouped_split_used": True,
                "row_level_random_split_used": False,
                "production_artifact_written": False,
                "leakage_checks_passed": True,
                "allowed_target_verified": True,
                "forbidden_targets_verified": True,
            },
            observed_value=g02_observed,
            source_field_paths=[f"ranker.policy_compliance.{key}" for key in g02_observed],
            rationale="The ranker experiment must prove it followed the split policy and wrote no production artifact.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    per_fold = _get(ranker_payload, "leakage_report.per_fold")
    fold_overlaps = [
        fold.get("leakage_work_overlap_count")
        for fold in per_fold
        if isinstance(fold, Mapping)
    ] if isinstance(per_fold, list) else []
    g03_observed = {
        "global_leakage_work_overlap_count": _get(ranker_payload, "leakage_report.global_leakage_work_overlap_count"),
        "global_zero_assertion": _get(ranker_payload, "leakage_report.global_zero_assertion"),
        "per_fold_leakage_work_overlap_counts": fold_overlaps,
    }
    g03_pass = (
        g03_observed["global_leakage_work_overlap_count"] == 0
        and _is_true(g03_observed["global_zero_assertion"])
        and bool(fold_overlaps)
        and all(value == 0 for value in fold_overlaps)
    )
    gates.append(
        _gate(
            "G03_leakage_zero",
            title="Zero Work Leakage",
            category="leakage",
            status=_status(g03_pass),
            threshold={"global": 0, "per_fold": 0},
            observed_value=g03_observed,
            source_field_paths=[
                "ranker.leakage_report.global_leakage_work_overlap_count",
                "ranker.leakage_report.global_zero_assertion",
                "ranker.leakage_report.per_fold[].leakage_work_overlap_count",
            ],
            rationale="No canonical work may appear in both train and eval folds.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    pos_work = _get(ranker_payload, "dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.positive")
    neg_work = _get(ranker_payload, "dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.negative")
    if pos_work is None or neg_work is None:
        pos_work = _get(ranker_payload, "metadata.group_level_stratification_class_counts.positive")
        neg_work = _get(ranker_payload, "metadata.group_level_stratification_class_counts.negative")
    g04_pass = (
        isinstance(pos_work, (int, float))
        and isinstance(neg_work, (int, float))
        and pos_work >= THRESHOLDS["minimum_positive_work_groups"]
        and neg_work >= THRESHOLDS["minimum_negative_work_groups"]
    )
    gates.append(
        _gate(
            "G04_minimum_work_groups",
            title="Minimum Work Groups",
            category="data",
            status=_status(g04_pass),
            threshold={
                "positive_work_groups_min": THRESHOLDS["minimum_positive_work_groups"],
                "negative_work_groups_min": THRESHOLDS["minimum_negative_work_groups"],
            },
            observed_value={"positive_work_groups": pos_work, "negative_work_groups": neg_work},
            source_field_paths=[
                "ranker.dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.positive",
                "ranker.dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.negative",
                "ranker.metadata.group_level_stratification_class_counts",
            ],
            rationale="Grouped CV needs at least two positive and two negative canonical work groups.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    g05_observed = {
        "eligible_observations": _get(ranker_payload, "dataset_inventory.eligible_observations"),
        "observation_positive": _get(ranker_payload, "dataset_inventory.target_class_counts.observation_level.positive"),
        "observation_negative": _get(ranker_payload, "dataset_inventory.target_class_counts.observation_level.negative"),
        "work_any_positive": _get(ranker_payload, "dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.positive"),
        "work_any_negative": _get(ranker_payload, "dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.negative"),
    }
    g05_pass = (
        g05_observed["eligible_observations"] >= THRESHOLDS["minimum_eligible_observations"]
        and g05_observed["observation_positive"] >= THRESHOLDS["minimum_observation_positive_count"]
        and g05_observed["observation_negative"] >= THRESHOLDS["minimum_observation_negative_count"]
        and g05_observed["work_any_positive"] >= THRESHOLDS["minimum_work_any_positive_count"]
        and g05_observed["work_any_negative"] >= THRESHOLDS["minimum_work_any_negative_count"]
    )
    gates.append(
        _gate(
            "G05_class_balance_floor",
            title="Class Balance Floor",
            category="data",
            status=_status(g05_pass),
            threshold={
                "eligible_observations_min": THRESHOLDS["minimum_eligible_observations"],
                "observation_positive_min": THRESHOLDS["minimum_observation_positive_count"],
                "observation_negative_min": THRESHOLDS["minimum_observation_negative_count"],
                "work_any_positive_min": THRESHOLDS["minimum_work_any_positive_count"],
                "work_any_negative_min": THRESHOLDS["minimum_work_any_negative_count"],
            },
            observed_value=g05_observed,
            source_field_paths=[
                "ranker.dataset_inventory.eligible_observations",
                "ranker.dataset_inventory.target_class_counts.observation_level.positive",
                "ranker.dataset_inventory.target_class_counts.observation_level.negative",
                "ranker.dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.positive",
                "ranker.dataset_inventory.target_class_counts.work_group_reporting_level.any_positive.negative",
            ],
            rationale="The audit experiment needs enough observations and grouped positives/negatives to be worth interpreting.",
            blocking_for=["product_candidate_experiment"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    emb_ba = _get(ranker_payload, _metric_path("embedding_logistic", "balanced_accuracy", "mean"))
    maj_ba = _get(ranker_payload, _metric_path("majority_class", "balanced_accuracy", "mean"))
    margin = emb_ba - maj_ba if isinstance(emb_ba, (int, float)) and isinstance(maj_ba, (int, float)) else None
    g06_pass = isinstance(margin, (int, float)) and margin >= THRESHOLDS["majority_baseline_balanced_accuracy_margin"]
    gates.append(
        _gate(
            "G06_majority_baseline_margin",
            title="Majority Baseline Margin",
            category="metric",
            status=_status(g06_pass),
            threshold={"balanced_accuracy_margin_min": THRESHOLDS["majority_baseline_balanced_accuracy_margin"]},
            observed_value={"embedding_logistic_balanced_accuracy": emb_ba, "majority_class_balanced_accuracy": maj_ba, "margin": margin},
            source_field_paths=[
                f"ranker.{_metric_path('embedding_logistic', 'balanced_accuracy', 'mean')}",
                f"ranker.{_metric_path('majority_class', 'balanced_accuracy', 'mean')}",
            ],
            rationale="The embedding classifier must clear the trivial majority baseline by a meaningful balanced-accuracy margin.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    auc_mean = _get(ranker_payload, _metric_path("embedding_logistic", "roc_auc", "mean"))
    auc_n = _get(ranker_payload, _metric_path("embedding_logistic", "roc_auc", "n"))
    skipped = _get(ranker_payload, "models.embedding_logistic.aggregate.folds_skipped")
    effective_folds = _get(ranker_payload, "metadata.effective_cv_folds")
    g07_pass = (
        isinstance(auc_mean, (int, float))
        and auc_mean >= THRESHOLDS["embedding_logistic_roc_auc_floor"]
        and auc_n == effective_folds
        and skipped == 0
    )
    gates.append(
        _gate(
            "G07_roc_auc_floor",
            title="ROC-AUC Floor",
            category="metric",
            status=_status(g07_pass),
            threshold={"roc_auc_mean_min": THRESHOLDS["embedding_logistic_roc_auc_floor"], "roc_auc_n_equals_effective_folds": True},
            observed_value={"roc_auc_mean": auc_mean, "roc_auc_n": auc_n, "folds_skipped": skipped, "effective_cv_folds": effective_folds},
            source_field_paths=[
                f"ranker.{_metric_path('embedding_logistic', 'roc_auc', 'mean')}",
                f"ranker.{_metric_path('embedding_logistic', 'roc_auc', 'n')}",
                "ranker.models.embedding_logistic.aggregate.folds_skipped",
                "ranker.metadata.effective_cv_folds",
            ],
            rationale="Every fold must produce evaluable ROC-AUC above the audit threshold.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    ap_mean = _get(ranker_payload, _metric_path("embedding_logistic", "average_precision", "mean"))
    g08_pass = isinstance(ap_mean, (int, float)) and ap_mean >= THRESHOLDS["embedding_logistic_average_precision_floor"]
    gates.append(
        _gate(
            "G08_average_precision_floor",
            title="Average Precision Floor",
            category="metric",
            status=_status(g08_pass),
            threshold={"average_precision_mean_min": THRESHOLDS["embedding_logistic_average_precision_floor"]},
            observed_value={"average_precision_mean": ap_mean},
            source_field_paths=[f"ranker.{_metric_path('embedding_logistic', 'average_precision', 'mean')}"],
            rationale="The embedding classifier must produce strong precision-recall ranking signal on audit folds.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    ba_std = _get(ranker_payload, _metric_path("embedding_logistic", "balanced_accuracy", "std"))
    folds_evaluated = _get(ranker_payload, "models.embedding_logistic.aggregate.folds_evaluated")
    skipped = _get(ranker_payload, "models.embedding_logistic.aggregate.folds_skipped")
    g09_pass = (
        isinstance(ba_std, (int, float))
        and ba_std <= THRESHOLDS["embedding_logistic_balanced_accuracy_std_ceiling"]
        and folds_evaluated == effective_folds
        and skipped == 0
    )
    gates.append(
        _gate(
            "G09_fold_stability",
            title="Fold Stability",
            category="metric",
            status=_status(g09_pass),
            threshold={"balanced_accuracy_std_max": THRESHOLDS["embedding_logistic_balanced_accuracy_std_ceiling"], "folds_skipped": 0},
            observed_value={"balanced_accuracy_std": ba_std, "folds_evaluated": folds_evaluated, "folds_skipped": skipped, "effective_cv_folds": effective_folds},
            source_field_paths=[
                f"ranker.{_metric_path('embedding_logistic', 'balanced_accuracy', 'std')}",
                "ranker.models.embedding_logistic.aggregate.folds_evaluated",
                "ranker.models.embedding_logistic.aggregate.folds_skipped",
                "ranker.metadata.effective_cv_folds",
            ],
            rationale="Audit performance must be reasonably stable across grouped folds.",
            blocking_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    pressure = _get(ranker_payload, "dataset_inventory.duplicate_observation_pressure")
    eligible = _get(ranker_payload, "dataset_inventory.eligible_observations")
    unique = _get(ranker_payload, "dataset_inventory.unique_eligible_canonical_work_count")
    conflict_groups = _get(
        ranker_payload,
        "dataset_inventory.target_class_counts.work_group_reporting_level.conflicting_target_work_group_count",
    )
    expected_pressure = eligible - unique if isinstance(eligible, (int, float)) and isinstance(unique, (int, float)) else None
    g10_pass = pressure is not None and pressure == expected_pressure and conflict_groups is not None
    gates.append(
        _gate(
            "G10_duplicate_pressure_reported",
            title="Duplicate Pressure Reported",
            category="reporting",
            status=_status(g10_pass),
            threshold={"duplicate_observation_pressure_equals_eligible_minus_unique_work_count": True, "conflicting_target_work_group_count_present": True},
            observed_value={
                "duplicate_observation_pressure": pressure,
                "eligible_observations": eligible,
                "unique_eligible_canonical_work_count": unique,
                "expected_duplicate_observation_pressure": expected_pressure,
                "conflicting_target_work_group_count": conflict_groups,
            },
            source_field_paths=[
                "ranker.dataset_inventory.duplicate_observation_pressure",
                "ranker.dataset_inventory.eligible_observations",
                "ranker.dataset_inventory.unique_eligible_canonical_work_count",
                "ranker.dataset_inventory.target_class_counts.work_group_reporting_level.conflicting_target_work_group_count",
            ],
            rationale="Duplicate/conflicting observations may exist; this gate only checks that their pressure is visible.",
            blocking_for=["product_candidate_experiment"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    pool_count = _nonzero_pool_count(ranker_payload)
    g11_pass = pool_count > 1
    gates.append(
        _gate(
            "G11_audit_pool_scope_acknowledged",
            title="Audit Pool Scope Acknowledged",
            category="scope",
            status=_status(g11_pass),
            threshold={"minimum_nonzero_review_pool_variant_buckets": 2},
            observed_value={
                "nonzero_review_pool_variant_bucket_count": pool_count,
                "review_pool_variant_counts": _get(ranker_payload, "dataset_inventory.review_pool_variant_counts"),
            },
            source_field_paths=["ranker.dataset_inventory.review_pool_variant_counts"],
            rationale="The input is a mixed audit-pool experiment, not a product-candidate ranking experiment.",
            blocking_for=["shadow_scoring", "production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
            advisory_text="High audit-pool CV metrics do not imply product-candidate or live ranking success.",
        )
    )

    gates.append(
        _gate(
            "G12_product_candidate_experiment_required",
            title="Product-Candidate Experiment Required",
            category="scope",
            status="not_evaluated",
            threshold=None,
            observed_value={"product_candidate_offline_ranker_artifact_present": False},
            source_field_paths=[],
            rationale="Product-shaped candidate pool and ranking-relevant workflow metrics have not been measured by this audit experiment.",
            blocking_for=["shadow_scoring", "production_default"],
            required_for=["product_candidate_experiment", "shadow_scoring", "production_default"],
            next_action="run production-candidate offline scoring experiment before shadow scoring",
        )
    )

    good = _get(production_plan_payload, "targets.good_or_acceptable")
    surprising = _get(production_plan_payload, "targets.surprising_or_useful")
    if not isinstance(good, Mapping):
        good = {}
    if not isinstance(surprising, Mapping):
        surprising = {}
    good_status = str(good.get("status") or "")
    surprising_status = str(surprising.get("status") or "")
    g13_pass = (
        ("primary" in good_status or "offline" in str(good.get("allowed_next_stage") or ""))
        and good.get("production_eligible") is False
        and ("deferred" in surprising_status or "excluded" in surprising_status)
        and surprising.get("production_eligible") is False
    )
    gates.append(
        _gate(
            "G13_production_readiness_plan_alignment",
            title="Production Readiness Plan Alignment",
            category="policy",
            status=_status(g13_pass),
            threshold={
                "good_or_acceptable_research_candidate": True,
                "good_or_acceptable_production_eligible": False,
                "surprising_or_useful_deferred_or_excluded": True,
                "surprising_or_useful_production_eligible": False,
            },
            observed_value={
                "overall_status": _get(production_plan_payload, "metadata.overall_status"),
                "good_or_acceptable": {
                    "status": good.get("status"),
                    "allowed_next_stage": good.get("allowed_next_stage"),
                    "production_eligible": good.get("production_eligible"),
                },
                "surprising_or_useful": {
                    "status": surprising.get("status"),
                    "production_eligible": surprising.get("production_eligible"),
                },
            },
            source_field_paths=[
                "production_readiness_plan.metadata.overall_status",
                "production_readiness_plan.targets.good_or_acceptable.status",
                "production_readiness_plan.targets.good_or_acceptable.allowed_next_stage",
                "production_readiness_plan.targets.good_or_acceptable.production_eligible",
                "production_readiness_plan.targets.surprising_or_useful.status",
                "production_readiness_plan.targets.surprising_or_useful.production_eligible",
            ],
            rationale="Audit gates may advance offline research only when the production plan still blocks production eligibility.",
            blocking_for=["production_default"],
            required_for=["audit_gate_pass", "product_candidate_experiment"],
        )
    )

    if transfer_payload is not None:
        gates.append(
            _gate(
                "G14_transfer_readiness_advisory",
                title="Transfer Readiness Advisory",
                category="advisory",
                status="advisory_warn",
                threshold=None,
                observed_value={
                    "readiness_version": _get(transfer_payload, "metadata.readiness_version"),
                    "good_or_acceptable_flags": _get(transfer_payload, "heuristic_readiness_flags.good_or_acceptable"),
                    "surprising_or_useful_flags": _get(transfer_payload, "heuristic_readiness_flags.surprising_or_useful"),
                    "good_or_acceptable_cross_pool_synthesis": _get(transfer_payload, "cross_pool_synthesis.good_or_acceptable"),
                    "surprising_or_useful_cross_pool_synthesis": _get(transfer_payload, "cross_pool_synthesis.surprising_or_useful"),
                    "production_recommender_missing_gates": transfer_payload.get("production_recommender_missing_gates"),
                },
                source_field_paths=[
                    "transfer_readiness.metadata.readiness_version",
                    "transfer_readiness.heuristic_readiness_flags.good_or_acceptable",
                    "transfer_readiness.heuristic_readiness_flags.surprising_or_useful",
                    "transfer_readiness.cross_pool_synthesis.good_or_acceptable",
                    "transfer_readiness.cross_pool_synthesis.surprising_or_useful",
                    "transfer_readiness.production_recommender_missing_gates",
                ],
                rationale="Transfer-readiness is recorded as advisory context. Audit ranker gates do not treat transfer-readiness as pass/fail evidence in v1.",
                blocking_for=["shadow_scoring", "production_default"],
                required_for=[],
            )
        )

    return gates


def _gate_status_map(gates: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    return {str(gate["gate_id"]): str(gate["status"]) for gate in gates}


def _overall_outcomes(gates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = _gate_status_map(gates)
    failed_required = [gate_id for gate_id in REQUIRED_AUDIT_GATE_IDS if statuses.get(gate_id) != "pass"]
    audit_passed = not failed_required
    blocking_failures = [gate_id for gate_id in BLOCKING_AUDIT_GATE_IDS_EXCEPT_G05 if statuses.get(gate_id) != "pass"]
    if blocking_failures:
        recommended = "blocked_pending_audit_gate_failures"
    elif statuses.get("G05_class_balance_floor") != "pass":
        recommended = "continue_labeling_rubric"
    else:
        recommended = "proceed_to_production_candidate_offline_scoring"
    return {
        "audit_ranker_gates_passed": audit_passed,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": recommended,
        "blocked_reasons": failed_required,
        "shadow_blockers": [
            "G12_product_candidate_experiment_required",
            "missing ml-shadow-scorer-v1",
            "production default blocked by readiness plan",
        ],
    }


def build_ml_offline_metric_gates_payload(
    *,
    ranker_experiment_path: Path,
    split_policy_path: Path,
    production_readiness_plan_path: Path,
    output_version: str = GATES_VERSION,
    transfer_readiness_path: Path | None = None,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    ranker_path = Path(ranker_experiment_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    transfer_path = Path(transfer_readiness_path).resolve() if transfer_readiness_path is not None else None

    ranker_payload = _load_json_object(ranker_path)
    split_policy_payload = _load_json_object(policy_path)
    production_plan_payload = _load_json_object(plan_path)
    transfer_payload = _load_json_object(transfer_path) if transfer_path is not None else None

    ranker_metadata = _validate_ranker(ranker_payload)
    split_metadata = _validate_split_policy(split_policy_payload)
    plan_metadata = _validate_production_plan(production_plan_payload)
    transfer_metadata = _validate_transfer_readiness(transfer_payload) if transfer_payload is not None else None

    inputs = [
        _input_record("ranker_experiment", ranker_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    if transfer_path is not None:
        inputs.append(_input_record("transfer_readiness", transfer_path, repo_root=root))

    experiment_scope = _experiment_scope(ranker_payload)
    gates = _build_gates(
        ranker_payload=ranker_payload,
        split_policy_payload=split_policy_payload,
        production_plan_payload=production_plan_payload,
        transfer_payload=transfer_payload,
    )
    outcomes = _overall_outcomes(gates)
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": output_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "experiment_scope": experiment_scope,
        "ranker_experiment_version": ranker_metadata.get("experiment_version"),
        "split_policy_version": split_metadata.get("policy_version"),
        "production_readiness_plan_version": plan_metadata.get("plan_version"),
        "transfer_readiness_version": transfer_metadata.get("readiness_version") if transfer_metadata else None,
        "thresholds_version": THRESHOLDS_VERSION,
        "thresholds": dict(THRESHOLDS),
        "strategic_framing": {
            "input_experiment_scope": "audit_pool_offline_ranker",
            "input_uses": [
                "mixed review pools",
                "observation-level labels",
                "text embeddings",
                "classification metrics",
                "grouped CV",
            ],
            "not_product_ranking_experiment": True,
            "not_live_recommender_validation": True,
            "passing_audit_gates_recommends_only": "proceeding to a production-candidate offline scoring experiment",
            "shadow_scoring_blocked_until": [
                "audit gates pass",
                "product-candidate experiment gates pass in a future artifact",
                "ml-shadow-scorer-v1 exists in a future artifact",
            ],
            "production_default_blocked_in_v1": True,
        },
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "gates": gates,
        **outcomes,
        "product_candidate_experiment_requirements": [
            "product-shaped candidate pool",
            "ranking-relevant metrics such as top-k precision/recall, PR@k, calibration, and comparison to current heuristic ranking",
            "candidate pool definition",
            "no audit-only pool mixing without citation",
            "no production default change",
        ],
    }


def _gate_counts(gates: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for gate in gates:
        status = str(gate.get("status"))
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _fmt(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.3f}"


def markdown_from_ml_offline_metric_gates(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    gates = payload["gates"]
    ranker_metrics = {
        "balanced_accuracy": None,
        "roc_auc": None,
        "average_precision": None,
    }
    for gate in gates:
        if gate["gate_id"] == "G06_majority_baseline_margin":
            ranker_metrics["balanced_accuracy"] = gate["observed_value"]["embedding_logistic_balanced_accuracy"]
        if gate["gate_id"] == "G07_roc_auc_floor":
            ranker_metrics["roc_auc"] = gate["observed_value"]["roc_auc_mean"]
        if gate["gate_id"] == "G08_average_precision_floor":
            ranker_metrics["average_precision"] = gate["observed_value"]["average_precision_mean"]
    leakage_gate = next(gate for gate in gates if gate["gate_id"] == "G03_leakage_zero")
    duplicate_gate = next(gate for gate in gates if gate["gate_id"] == "G10_duplicate_pressure_reported")
    lines = [
        f"# Offline Metric Gates ({metadata['gates_version']})",
        "",
        "## Executive Summary",
        "",
        f"- **Experiment scope:** `{metadata['experiment_scope']}`",
        f"- **Audit ranker gates passed:** {payload['audit_ranker_gates_passed']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['production_default_allowed']}",
        "",
        "This is an audit-pool offline gate evaluation over mixed review pools, observation-level labels, text embeddings, classification metrics, and grouped CV. It is not a product-ranking experiment or validation of live recommender quality.",
        "",
        "## Headline Ranker Metrics",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Balanced accuracy | {_fmt(ranker_metrics['balanced_accuracy'])} |",
        f"| ROC-AUC | {_fmt(ranker_metrics['roc_auc'])} |",
        f"| Average precision | {_fmt(ranker_metrics['average_precision'])} |",
        f"| Leakage overlap | {leakage_gate['observed_value']['global_leakage_work_overlap_count']} |",
        f"| Duplicate observation pressure | {duplicate_gate['observed_value']['duplicate_observation_pressure']} |",
        "",
        "## Gate Checklist",
        "",
        "| Gate | Status | Rationale |",
        "| --- | --- | --- |",
    ]
    for gate in gates:
        lines.append(f"| `{gate['gate_id']}` | {gate['status']} | {gate['rationale']} |")
    lines.extend(
        [
            "",
            "## Not Ship / Not Shadow Yet",
            "",
            "Passing audit gates may only recommend a production-candidate offline scoring experiment. Shadow scoring remains blocked until audit gates pass, product-candidate experiment gates pass in a future artifact, and `ml-shadow-scorer-v1` exists. Production defaults remain blocked in all v1 outcomes.",
            "",
            "## Product-Candidate Experiment Requirements",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["product_candidate_experiment_requirements"])
    lines.extend(
        [
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.extend(
        [
            "",
            f"Gate status counts: `{_gate_counts(gates)}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_ml_offline_metric_gates(
    *,
    ranker_experiment_path: Path,
    split_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    transfer_readiness_path: Path | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_metric_gates_payload(
        ranker_experiment_path=ranker_experiment_path,
        split_policy_path=split_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        transfer_readiness_path=transfer_readiness_path,
        output_version=gates_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_offline_metric_gates(payload), encoding="utf-8", newline="\n")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "GATES_VERSION",
    "MLOfflineMetricGatesError",
    "build_ml_offline_metric_gates_payload",
    "markdown_from_ml_offline_metric_gates",
    "write_ml_offline_metric_gates",
]

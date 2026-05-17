"""Product-candidate offline metric gate evaluator.

This is a deterministic spec/evidence artifact writer. It reads existing JSON
artifacts and evaluates fixed thresholds. It does not train, score new rows,
call network services, access databases, or change production behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_offline_production_candidate_metric_gates"
GATES_VERSION = "ml-offline-production-candidate-metric-gates-v1"
GATES_VERSION_V2 = "ml-offline-production-candidate-metric-gates-v2"
SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
SCORING_VERSION = "ml-offline-production-candidate-scoring-v1"
SCORING_VERSION_V2 = "ml-offline-production-candidate-scoring-v2"
OFFLINE_METRIC_GATES_ARTIFACT_TYPE = "ml_offline_metric_gates"
OFFLINE_METRIC_GATES_VERSION = "ml-offline-metric-gates-v1"
SPLIT_POLICY_ARTIFACT_TYPE = "ml_label_split_policy"
SPLIT_POLICY_VERSION = "ml-label-split-policy-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"
AUDIT_SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
AUDIT_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v1"
TARGET = "good_or_acceptable"
FORBIDDEN_TARGET = "surprising_or_useful"
SCORING_MODE_HEURISTIC = "heuristic_and_coverage_only"
SCORING_MODE_AUDIT_EMBEDDING = "heuristic_and_audit_embedding_scorer"

THRESHOLDS_VERSION = "ml-offline-production-candidate-metric-gates-v1-thresholds"
THRESHOLDS: dict[str, float | int] = {
    "minimum_candidate_unique_work_count": 100,
    "minimum_candidate_label_coverage_rate": 0.80,
    "minimum_labeled_eval_work_count": 100,
    "minimum_labeled_eval_negative_work_count": 20,
    "minimum_heuristic_roc_auc": 0.70,
    "minimum_heuristic_average_precision": 0.85,
    "minimum_precision_at_10": 0.80,
    "maximum_missing_embedding_rate_for_labeled_observations": 0.05,
    "high_positive_work_prevalence_advisory_threshold": 0.85,
}
THRESHOLDS_VERSION_V2 = "ml-offline-production-candidate-metric-gates-v2-thresholds"
THRESHOLDS_V2: dict[str, float | int] = {
    "minimum_candidate_unique_work_count": 100,
    "minimum_candidate_label_coverage_rate": 0.80,
    "minimum_labeled_eval_work_count": 100,
    "minimum_labeled_eval_negative_work_count": 20,
    "minimum_heuristic_roc_auc": 0.70,
    "minimum_heuristic_average_precision": 0.85,
    "minimum_heuristic_precision_at_10": 0.80,
    "maximum_missing_embedding_rate_for_labeled_observations": 0.05,
    "high_positive_work_prevalence_advisory_threshold": 0.85,
    "minimum_learned_roc_auc": 0.70,
    "minimum_learned_average_precision": 0.85,
    "minimum_learned_precision_at_10": 0.80,
    "near_perfect_learned_roc_auc_advisory_threshold": 0.99,
    "near_perfect_learned_average_precision_advisory_threshold": 0.99,
}

GATE_STATUS_ENUM = ("pass", "fail", "skip", "not_applicable", "not_evaluated", "advisory_warn")
REQUIRED_HEURISTIC_GATE_IDS = (
    "G01_input_scope",
    "G02_prior_audit_gates_passed",
    "G03_candidate_pool_size",
    "G04_label_coverage",
    "G05_negative_coverage",
    "G06_embedding_coverage",
    "G07_heuristic_roc_auc",
    "G08_heuristic_average_precision",
    "G09_top_k_precision",
    "G11_shadow_blockers_documented",
    "G12_production_readiness_alignment",
)
REQUIRED_HEURISTIC_GATE_IDS_V2 = (
    "G01_input_scope",
    "G02_prior_audit_gates_passed",
    "G03_candidate_pool_size",
    "G04_label_coverage",
    "G05_negative_coverage",
    "G06_embedding_coverage",
    "G07_heuristic_roc_auc",
    "G08_heuristic_average_precision",
    "G09_heuristic_top_k_precision",
    "G13_shadow_blockers_documented",
    "G14_production_readiness_alignment",
)
SHADOW_BLOCKERS = (
    "learned_scorer_not_evaluated",
    "missing_ml_shadow_scorer_v1",
    "production_default_blocked",
    "no_production_model_artifact",
)
SHADOW_BLOCKERS_V2 = (
    "independent_learned_validation_not_evaluated",
    "missing_ml_shadow_scorer_v1",
    "production_default_blocked",
    "no_production_model_artifact",
)
CAVEATS = (
    "Not validation.",
    "Product-candidate diagnostic only.",
    "Existing product-candidate pool reused read-only; no new ranking was run.",
    "Heuristic evidence is not learned model evidence.",
    "Learned scorer not evaluated in heuristic_and_coverage_only mode.",
    "Shadow scoring and production default remain blocked.",
    "No API/web changes.",
)
CAVEATS_V2 = (
    "Not validation.",
    "Product-candidate offline diagnostic only.",
    "Learned scorer trained on audit corpus; labeled overlap not independent holdout.",
    "Near-perfect learned metrics may reflect training overlap.",
    "Heuristic final_score and learned audit scorer are separate evidence lines.",
    "Shadow and production default blocked.",
    "No ranking/API/web changes.",
)


class MLOfflineProductionCandidateMetricGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLOfflineProductionCandidateMetricGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLOfflineProductionCandidateMetricGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLOfflineProductionCandidateMetricGatesError(f"{name} JSON missing metadata object")
    return metadata


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLOfflineProductionCandidateMetricGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _input_sha(metadata: Mapping[str, Any], name: str) -> str | None:
    inputs = metadata.get("inputs")
    if not isinstance(inputs, Sequence) or isinstance(inputs, (str, bytes)):
        return None
    for item in inputs:
        if isinstance(item, Mapping) and item.get("name") == name and isinstance(item.get("sha256"), str):
            return str(item["sha256"])
    return None


def _require_input_sha(metadata: Mapping[str, Any], name: str, expected_sha: str, *, artifact_name: str) -> None:
    actual_sha = _input_sha(metadata, name)
    if actual_sha != expected_sha:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"{artifact_name} metadata.inputs missing {name!r} sha256 {expected_sha!r}; got {actual_sha!r}"
        )


def _validate_scoring(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION!r}, "
            f"got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("target") != TARGET:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.target={TARGET!r}, got {metadata.get('target')!r}"
        )
    if not metadata.get("scoring_mode"):
        raise MLOfflineProductionCandidateMetricGatesError("scoring metadata.scoring_mode must be present")
    return metadata


def _validate_scoring_v2(
    payload: Mapping[str, Any],
    *,
    scorer_sha256: str,
    scorer_metadata: Mapping[str, Any],
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-scoring")
    if metadata.get("artifact_type") != SCORING_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.artifact_type={SCORING_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("experiment_version") != SCORING_VERSION_V2:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.experiment_version={SCORING_VERSION_V2!r}, "
            f"got {metadata.get('experiment_version')!r}"
        )
    if metadata.get("scoring_mode") != SCORING_MODE_AUDIT_EMBEDDING:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.scoring_mode={SCORING_MODE_AUDIT_EMBEDDING!r}, "
            f"got {metadata.get('scoring_mode')!r}"
        )
    if metadata.get("target") != TARGET:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scoring metadata.target={TARGET!r}, got {metadata.get('target')!r}"
        )
    if _get(payload, "scoring_mode_details.learned_product_scores_produced") is not True:
        raise MLOfflineProductionCandidateMetricGatesError(
            "scoring scoring_mode_details.learned_product_scores_produced must be true for v2 gates"
        )
    if _get(payload, "scoring_mode_details.product_candidate_rows_used_for_training") != 0:
        raise MLOfflineProductionCandidateMetricGatesError(
            "scoring scoring_mode_details.product_candidate_rows_used_for_training must be 0"
        )
    if _get(payload, "scoring_mode_details.audit_embedding_scorer_version") != AUDIT_SCORER_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            "scoring scoring_mode_details.audit_embedding_scorer_version must be "
            f"{AUDIT_SCORER_VERSION!r}"
        )
    if _get(payload, "scoring_mode_details.audit_embedding_scorer_sha256") != scorer_sha256:
        raise MLOfflineProductionCandidateMetricGatesError(
            "supplied scorer sha256 must match scoring_mode_details.audit_embedding_scorer_sha256"
        )
    _require_input_sha(
        metadata,
        "audit_embedding_scorer_export",
        scorer_sha256,
        artifact_name="production-candidate-scoring",
    )

    scoring_embedding_sha = _input_sha(metadata, "embeddings")
    scorer_embedding_sha = scorer_metadata.get("embedding_artifact_sha256")
    if scoring_embedding_sha and scorer_embedding_sha and scoring_embedding_sha != scorer_embedding_sha:
        raise MLOfflineProductionCandidateMetricGatesError(
            "scorer metadata.embedding_artifact_sha256 must match scoring metadata.inputs embeddings sha256"
        )
    scoring_label_sha = _input_sha(metadata, "label_dataset")
    scorer_label_sha = scorer_metadata.get("label_dataset_sha256")
    if scoring_label_sha and scorer_label_sha and scoring_label_sha != scorer_label_sha:
        raise MLOfflineProductionCandidateMetricGatesError(
            "scorer metadata.label_dataset_sha256 must match scoring metadata.inputs label_dataset sha256"
        )
    return metadata


def _validate_offline_metric_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="offline-metric-gates")
    if metadata.get("artifact_type") != OFFLINE_METRIC_GATES_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            "expected offline metric gates metadata.artifact_type="
            f"{OFFLINE_METRIC_GATES_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != OFFLINE_METRIC_GATES_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected offline metric gates metadata.gates_version={OFFLINE_METRIC_GATES_VERSION!r}, "
            f"got {metadata.get('gates_version')!r}"
        )
    if payload.get("audit_ranker_gates_passed") is not True:
        raise MLOfflineProductionCandidateMetricGatesError("offline metric gates audit_ranker_gates_passed must be true")
    return metadata


def _validate_split_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="split-policy")
    if metadata.get("artifact_type") != SPLIT_POLICY_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected split policy metadata.artifact_type={SPLIT_POLICY_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != SPLIT_POLICY_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected split policy metadata.policy_version={SPLIT_POLICY_VERSION!r}, "
            f"got {metadata.get('policy_version')!r}"
        )
    return metadata


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-readiness-plan")
    if metadata.get("artifact_type") != PRODUCTION_PLAN_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            "expected production-readiness-plan metadata.artifact_type="
            f"{PRODUCTION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PRODUCTION_PLAN_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected production-readiness-plan metadata.plan_version={PRODUCTION_PLAN_VERSION!r}, "
            f"got {metadata.get('plan_version')!r}"
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


def _validate_audit_embedding_scorer(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="audit-embedding-scorer-export")
    if metadata.get("artifact_type") != AUDIT_SCORER_ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scorer metadata.artifact_type={AUDIT_SCORER_ARTIFACT_TYPE!r}, "
            f"got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("scorer_version") != AUDIT_SCORER_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scorer metadata.scorer_version={AUDIT_SCORER_VERSION!r}, "
            f"got {metadata.get('scorer_version')!r}"
        )
    if metadata.get("fit_mode") != "full_fit_audit_corpus":
        raise MLOfflineProductionCandidateMetricGatesError(
            "scorer metadata.fit_mode must be 'full_fit_audit_corpus'"
        )
    if metadata.get("target") != TARGET:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected scorer metadata.target={TARGET!r}, got {metadata.get('target')!r}"
        )
    policy = payload.get("policy_compliance")
    if not isinstance(policy, Mapping):
        raise MLOfflineProductionCandidateMetricGatesError("scorer missing policy_compliance object")
    required_false = (
        "product_candidate_pool_used_for_training",
        "shadow_scoring_authorized",
        "production_artifact_written",
    )
    for key in required_false:
        if policy.get(key) is not False:
            raise MLOfflineProductionCandidateMetricGatesError(f"scorer policy_compliance.{key} must be false")

    dimensions = metadata.get("embedding_dimensions")
    feature_count = _get(payload, "scorer.scaler.feature_count")
    coefficients = _get(payload, "scorer.classifier.coefficients_standardized_space")
    if _is_number(dimensions) and _is_number(feature_count) and int(dimensions) != int(feature_count):
        raise MLOfflineProductionCandidateMetricGatesError(
            "scorer metadata.embedding_dimensions must match scorer.scaler.feature_count"
        )
    if _is_number(feature_count) and isinstance(coefficients, Sequence) and not isinstance(coefficients, (str, bytes)):
        if len(coefficients) != int(feature_count):
            raise MLOfflineProductionCandidateMetricGatesError(
                "scorer classifier coefficient length must match scorer.scaler.feature_count"
            )
    return metadata


def _validate_prior_v1_metric_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="production-candidate-metric-gates-v1")
    if metadata.get("artifact_type") != ARTIFACT_TYPE:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected prior v1 gates metadata.artifact_type={ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("gates_version") != GATES_VERSION:
        raise MLOfflineProductionCandidateMetricGatesError(
            f"expected prior v1 gates metadata.gates_version={GATES_VERSION!r}, got {metadata.get('gates_version')!r}"
        )
    if payload.get("product_candidate_heuristic_gates_passed") is not True:
        raise MLOfflineProductionCandidateMetricGatesError(
            "prior v1 product-candidate metric gates product_candidate_heuristic_gates_passed must be true"
        )
    return metadata


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _nonempty(value: Any) -> bool:
    return bool(str(value or "").strip())


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
    if status not in GATE_STATUS_ENUM:
        raise MLOfflineProductionCandidateMetricGatesError(f"invalid gate status for {gate_id}: {status}")
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


def _learned_product_scores_produced(scoring_payload: Mapping[str, Any]) -> Any:
    value = _get(scoring_payload, "scoring_mode_details.learned_product_scores_produced")
    if value is None:
        value = _get(scoring_payload, "learned_or_embedding_metrics.learned_product_scores_produced")
    return value


def _positive_prevalence(scoring_payload: Mapping[str, Any]) -> float | None:
    positive = _get(scoring_payload, "label_join_summary.labeled_eval_subset_positive_work_count")
    total = _get(scoring_payload, "label_join_summary.labeled_eval_subset_work_count")
    if _is_number(positive) and _is_number(total) and total > 0:
        return float(positive) / float(total)
    return None


def _missing_embedding_rate(scoring_payload: Mapping[str, Any]) -> float | None:
    missing = _get(scoring_payload, "embedding_join_summary.missing_embedding_count")
    observations = _get(scoring_payload, "embedding_join_summary.labeled_candidate_observation_count")
    if _is_number(missing) and _is_number(observations):
        return float(missing) / float(max(1, observations))
    return None


def _blocked_overall_status(value: Any) -> bool:
    status = str(value or "").strip().lower()
    return status in {"research_only", "blocked", "production_blocked", "not_production_ready"} or (
        "research" in status or "block" in status or "not_production" in status
    )


def _build_gates(
    *,
    scoring_payload: Mapping[str, Any],
    offline_metric_gates_payload: Mapping[str, Any],
    production_plan_payload: Mapping[str, Any],
    shadow_scoring_allowed: bool,
    production_default_allowed: bool,
    shadow_blockers: Sequence[str],
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []

    scoring_metadata = _metadata(scoring_payload, name="production-candidate-scoring")
    candidate_pool_definition = scoring_payload.get("candidate_pool_definition")
    g01_observed = {
        "metadata_present": isinstance(scoring_metadata, Mapping),
        "candidate_pool_definition_present": isinstance(candidate_pool_definition, Mapping),
        "metadata_ranking_run_id": scoring_metadata.get("ranking_run_id"),
        "metadata_family": scoring_metadata.get("family"),
        "candidate_pool_ranking_run_id": _get(scoring_payload, "candidate_pool_definition.ranking_run_id"),
        "candidate_pool_family": _get(scoring_payload, "candidate_pool_definition.family"),
    }
    g01_pass = (
        g01_observed["metadata_present"]
        and g01_observed["candidate_pool_definition_present"]
        and _nonempty(g01_observed["metadata_ranking_run_id"])
        and _nonempty(g01_observed["metadata_family"])
        and _nonempty(g01_observed["candidate_pool_ranking_run_id"])
        and _nonempty(g01_observed["candidate_pool_family"])
    )
    gates.append(
        _gate(
            "G01_input_scope",
            title="Input Scope",
            category="scope",
            status=_status(bool(g01_pass)),
            threshold={
                "metadata_present": True,
                "candidate_pool_definition_present": True,
                "ranking_run_id_present": True,
                "family_present": True,
            },
            observed_value=g01_observed,
            source_field_paths=[
                "scoring.metadata",
                "scoring.metadata.ranking_run_id",
                "scoring.metadata.family",
                "scoring.candidate_pool_definition",
                "scoring.candidate_pool_definition.ranking_run_id",
                "scoring.candidate_pool_definition.family",
            ],
            rationale="The diagnostic must identify the product-candidate pool, ranking run, and family it evaluated.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    g02_observed = {
        "audit_ranker_gates_passed": offline_metric_gates_payload.get("audit_ranker_gates_passed"),
        "recommended_next_stage": offline_metric_gates_payload.get("recommended_next_stage"),
    }
    g02_pass = (
        g02_observed["audit_ranker_gates_passed"] is True
        and g02_observed["recommended_next_stage"] == "proceed_to_production_candidate_offline_scoring"
    )
    gates.append(
        _gate(
            "G02_prior_audit_gates_passed",
            title="Prior Audit Gates Passed",
            category="policy",
            status=_status(g02_pass),
            threshold={
                "audit_ranker_gates_passed": True,
                "recommended_next_stage": "proceed_to_production_candidate_offline_scoring",
            },
            observed_value=g02_observed,
            source_field_paths=[
                "offline_metric_gates.audit_ranker_gates_passed",
                "offline_metric_gates.recommended_next_stage",
            ],
            rationale="Product-candidate scoring may only be interpreted after the audit-pool ranker gates cleared.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    candidate_count = _get(scoring_payload, "candidate_pool_summary.candidate_unique_canonical_work_count")
    g03_pass = _is_number(candidate_count) and candidate_count >= THRESHOLDS["minimum_candidate_unique_work_count"]
    gates.append(
        _gate(
            "G03_candidate_pool_size",
            title="Candidate Pool Size",
            category="coverage",
            status=_status(g03_pass),
            threshold={"candidate_unique_work_count_min": THRESHOLDS["minimum_candidate_unique_work_count"]},
            observed_value={"candidate_unique_canonical_work_count": candidate_count},
            source_field_paths=["scoring.candidate_pool_summary.candidate_unique_canonical_work_count"],
            rationale="The existing product-candidate pool must contain enough distinct works for a diagnostic gate.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    label_coverage = _get(scoring_payload, "label_join_summary.candidate_work_labeled_coverage_rate")
    labeled_work_count = _get(scoring_payload, "label_join_summary.labeled_eval_subset_work_count")
    g04_pass = (
        _is_number(label_coverage)
        and _is_number(labeled_work_count)
        and label_coverage >= THRESHOLDS["minimum_candidate_label_coverage_rate"]
        and labeled_work_count >= THRESHOLDS["minimum_labeled_eval_work_count"]
    )
    gates.append(
        _gate(
            "G04_label_coverage",
            title="Label Coverage",
            category="coverage",
            status=_status(g04_pass),
            threshold={
                "candidate_label_coverage_rate_min": THRESHOLDS["minimum_candidate_label_coverage_rate"],
                "labeled_eval_work_count_min": THRESHOLDS["minimum_labeled_eval_work_count"],
            },
            observed_value={
                "candidate_work_labeled_coverage_rate": label_coverage,
                "labeled_eval_subset_work_count": labeled_work_count,
            },
            source_field_paths=[
                "scoring.label_join_summary.candidate_work_labeled_coverage_rate",
                "scoring.label_join_summary.labeled_eval_subset_work_count",
            ],
            rationale="The product-candidate pool needs broad label overlap and enough labeled works to interpret heuristics.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    negative_count = _get(scoring_payload, "label_join_summary.labeled_eval_subset_negative_work_count")
    g05_pass = _is_number(negative_count) and negative_count >= THRESHOLDS["minimum_labeled_eval_negative_work_count"]
    gates.append(
        _gate(
            "G05_negative_coverage",
            title="Negative Coverage",
            category="coverage",
            status=_status(g05_pass),
            threshold={"labeled_eval_negative_work_count_min": THRESHOLDS["minimum_labeled_eval_negative_work_count"]},
            observed_value={"labeled_eval_subset_negative_work_count": negative_count},
            source_field_paths=["scoring.label_join_summary.labeled_eval_subset_negative_work_count"],
            rationale="At least a small negative set is needed so high top-k precision is not the only evidence.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    missing_rate = _missing_embedding_rate(scoring_payload)
    g06_pass = (
        missing_rate is not None
        and missing_rate <= THRESHOLDS["maximum_missing_embedding_rate_for_labeled_observations"]
    )
    gates.append(
        _gate(
            "G06_embedding_coverage",
            title="Embedding Coverage",
            category="coverage",
            status=_status(g06_pass),
            threshold={
                "missing_embedding_rate_max": THRESHOLDS[
                    "maximum_missing_embedding_rate_for_labeled_observations"
                ]
            },
            observed_value={
                "missing_embedding_count": _get(scoring_payload, "embedding_join_summary.missing_embedding_count"),
                "labeled_candidate_observation_count": _get(
                    scoring_payload, "embedding_join_summary.labeled_candidate_observation_count"
                ),
                "missing_embedding_rate": missing_rate,
            },
            source_field_paths=[
                "scoring.embedding_join_summary.missing_embedding_count",
                "scoring.embedding_join_summary.labeled_candidate_observation_count",
            ],
            rationale="The labeled candidate observations must mostly have embeddings before exporting an audit scorer next.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    roc_auc = _get(scoring_payload, "heuristic_metrics.roc_auc_mann_whitney")
    g07_pass = _is_number(roc_auc) and roc_auc >= THRESHOLDS["minimum_heuristic_roc_auc"]
    gates.append(
        _gate(
            "G07_heuristic_roc_auc",
            title="Heuristic ROC-AUC",
            category="metric",
            status=_status(g07_pass),
            threshold={"heuristic_roc_auc_min": THRESHOLDS["minimum_heuristic_roc_auc"]},
            observed_value={
                "roc_auc_mann_whitney": roc_auc,
                "roc_auc_reason": _get(scoring_payload, "heuristic_metrics.roc_auc_reason"),
            },
            source_field_paths=[
                "scoring.heuristic_metrics.roc_auc_mann_whitney",
                "scoring.heuristic_metrics.roc_auc_reason",
            ],
            rationale="The persisted heuristic score must separate positive and negative labeled works above the floor.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    average_precision = _get(scoring_payload, "heuristic_metrics.average_precision")
    g08_pass = _is_number(average_precision) and average_precision >= THRESHOLDS["minimum_heuristic_average_precision"]
    gates.append(
        _gate(
            "G08_heuristic_average_precision",
            title="Heuristic Average Precision",
            category="metric",
            status=_status(g08_pass),
            threshold={"heuristic_average_precision_min": THRESHOLDS["minimum_heuristic_average_precision"]},
            observed_value={
                "average_precision": average_precision,
                "average_precision_reason": _get(scoring_payload, "heuristic_metrics.average_precision_reason"),
            },
            source_field_paths=[
                "scoring.heuristic_metrics.average_precision",
                "scoring.heuristic_metrics.average_precision_reason",
            ],
            rationale="The heuristic final score needs strong precision-recall evidence on the labeled overlap.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    precision_at_10 = _get(scoring_payload, "heuristic_metrics.precision_recall_at_k.10.precision")
    p10_reason = _get(scoring_payload, "heuristic_metrics.precision_recall_at_k.10.reason")
    g09_pass = _is_number(precision_at_10) and precision_at_10 >= THRESHOLDS["minimum_precision_at_10"]
    p10_rationale = "Top-10 precision on labeled works must clear the minimum heuristic threshold."
    if not _is_number(precision_at_10):
        p10_rationale = "Top-10 precision is missing or null, so the top-k heuristic gate fails."
    gates.append(
        _gate(
            "G09_top_k_precision",
            title="Top-K Precision",
            category="metric",
            status=_status(g09_pass),
            threshold={"precision_at_10_min": THRESHOLDS["minimum_precision_at_10"]},
            observed_value={"precision_at_10": precision_at_10, "reason": p10_reason},
            source_field_paths=[
                'scoring.heuristic_metrics.precision_recall_at_k["10"].precision',
                'scoring.heuristic_metrics.precision_recall_at_k["10"].reason',
            ],
            rationale=p10_rationale,
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    scoring_mode = scoring_metadata.get("scoring_mode")
    learned_produced = _learned_product_scores_produced(scoring_payload)
    learned_thresholds = _get(scoring_payload, "learned_or_embedding_metrics.learned_metric_thresholds")
    if learned_thresholds is None:
        learned_thresholds = _get(scoring_payload, "scoring_mode_details.learned_metric_thresholds")
    learned_thresholds_satisfied = _get(
        scoring_payload, "learned_or_embedding_metrics.learned_metric_thresholds_satisfied"
    )
    if learned_thresholds_satisfied is None:
        learned_thresholds_satisfied = _get(scoring_payload, "scoring_mode_details.learned_metric_thresholds_satisfied")
    if learned_produced is True and isinstance(learned_thresholds, Mapping) and learned_thresholds_satisfied is True:
        g10_status = "pass"
        g10_rationale = "Learned product scores were produced and future-version learned metric thresholds were satisfied."
    elif scoring_mode == SCORING_MODE_HEURISTIC and learned_produced is False:
        g10_status = "not_evaluated"
        g10_rationale = (
            "v1 heuristic_and_coverage_only mode does not evaluate learned product scores; this does not affect "
            "product_candidate_heuristic_gates_passed."
        )
    else:
        g10_status = "fail"
        g10_rationale = "Learned product scores are not supported by defined and satisfied learned thresholds in this v1 gate."
    gates.append(
        _gate(
            "G10_learned_scorer_status",
            title="Learned Scorer Status",
            category="learned_scorer",
            status=g10_status,
            threshold={
                "learned_product_scores_produced": True,
                "learned_metric_thresholds_defined": True,
                "learned_metric_thresholds_satisfied": True,
            },
            observed_value={
                "scoring_mode": scoring_mode,
                "scoring_mode_details_learned_product_scores_produced": _get(
                    scoring_payload, "scoring_mode_details.learned_product_scores_produced"
                ),
                "learned_or_embedding_metrics_learned_product_scores_produced": _get(
                    scoring_payload, "learned_or_embedding_metrics.learned_product_scores_produced"
                ),
                "learned_metric_thresholds_defined": isinstance(learned_thresholds, Mapping),
                "learned_metric_thresholds_satisfied": learned_thresholds_satisfied,
            },
            source_field_paths=[
                "scoring.metadata.scoring_mode",
                "scoring.scoring_mode_details.learned_product_scores_produced",
                "scoring.learned_or_embedding_metrics.learned_product_scores_produced",
                "scoring.learned_or_embedding_metrics.learned_metric_thresholds",
                "scoring.learned_or_embedding_metrics.learned_metric_thresholds_satisfied",
            ],
            rationale=g10_rationale,
            blocking_for=["shadow_scoring", "production_default"],
            required_for=["learned_scorer_product_candidate_gates"],
            next_action="create_frozen_audit_embedding_scorer_export_v1" if g10_status != "pass" else None,
        )
    )

    scoring_shadow_blockers = scoring_payload.get("blockers_to_shadow")
    advisory_text = None
    if isinstance(scoring_shadow_blockers, Sequence) and not isinstance(scoring_shadow_blockers, (str, bytes)):
        blocker_text = " ".join(str(item).lower() for item in scoring_shadow_blockers)
        if "metric gates not yet evaluated" in blocker_text:
            advisory_text = (
                "The scoring artifact still says product-candidate metric gates were not yet evaluated; this v1 "
                "artifact supersedes that blocker for heuristic gates only."
            )
    required_blockers_present = set(SHADOW_BLOCKERS).issubset(set(shadow_blockers))
    g11_pass = shadow_scoring_allowed is False and required_blockers_present
    gates.append(
        _gate(
            "G11_shadow_blockers_documented",
            title="Shadow Blockers Documented",
            category="policy",
            status=_status(g11_pass),
            threshold={
                "shadow_scoring_allowed": False,
                "shadow_blockers_include": list(SHADOW_BLOCKERS),
            },
            observed_value={
                "shadow_scoring_allowed": shadow_scoring_allowed,
                "shadow_blockers": list(shadow_blockers),
                "scoring_blockers_to_shadow": scoring_shadow_blockers if isinstance(scoring_shadow_blockers, list) else None,
            },
            source_field_paths=[
                "this.shadow_scoring_allowed",
                "this.shadow_blockers",
                "scoring.blockers_to_shadow",
            ],
            rationale="The gate artifact must explicitly keep shadow blocked even when heuristic gates pass.",
            blocking_for=["shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
            advisory_text=advisory_text,
        )
    )

    good = _get(production_plan_payload, "targets.good_or_acceptable")
    surprising = _get(production_plan_payload, "targets.surprising_or_useful")
    if not isinstance(good, Mapping):
        good = {}
    if not isinstance(surprising, Mapping):
        surprising = {}
    overall_status = _get(production_plan_payload, "metadata.overall_status")
    surprising_status = str(surprising.get("status") or "").strip().lower()
    surprising_deferred_or_excluded = "deferred" in surprising_status or "excluded" in surprising_status
    g12_pass = (
        good.get("production_eligible") is False
        and surprising_deferred_or_excluded
        and _blocked_overall_status(overall_status)
    )
    gates.append(
        _gate(
            "G12_production_readiness_alignment",
            title="Production Readiness Alignment",
            category="policy",
            status=_status(g12_pass),
            threshold={
                "good_or_acceptable_production_eligible": False,
                "surprising_or_useful_deferred_or_excluded": True,
                "overall_status_blocked_posture": True,
            },
            observed_value={
                "overall_status": overall_status,
                "good_or_acceptable": {
                    "status": good.get("status"),
                    "allowed_next_stage": good.get("allowed_next_stage"),
                    "production_eligible": good.get("production_eligible"),
                },
                "surprising_or_useful": {
                    "status": surprising.get("status"),
                    "allowed_next_stage": surprising.get("allowed_next_stage"),
                    "production_eligible": surprising.get("production_eligible"),
                },
            },
            source_field_paths=[
                "production_readiness_plan.metadata.overall_status",
                "production_readiness_plan.targets.good_or_acceptable.production_eligible",
                "production_readiness_plan.targets.surprising_or_useful.status",
            ],
            rationale="Heuristic product-candidate gates may advance only while the production plan remains blocked.",
            blocking_for=["production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    positive_rate = _positive_prevalence(scoring_payload)
    if positive_rate is None:
        g13_status = "not_applicable"
    elif positive_rate > THRESHOLDS["high_positive_work_prevalence_advisory_threshold"]:
        g13_status = "advisory_warn"
    else:
        g13_status = "pass"
    gates.append(
        _gate(
            "G13_positive_prevalence_advisory",
            title="Positive Prevalence Advisory",
            category="advisory",
            status=g13_status,
            threshold={
                "high_positive_work_prevalence_advisory_threshold": THRESHOLDS[
                    "high_positive_work_prevalence_advisory_threshold"
                ]
            },
            observed_value={
                "labeled_eval_subset_positive_work_count": _get(
                    scoring_payload, "label_join_summary.labeled_eval_subset_positive_work_count"
                ),
                "labeled_eval_subset_work_count": _get(
                    scoring_payload, "label_join_summary.labeled_eval_subset_work_count"
                ),
                "positive_work_prevalence": positive_rate,
            },
            source_field_paths=[
                "scoring.label_join_summary.labeled_eval_subset_positive_work_count",
                "scoring.label_join_summary.labeled_eval_subset_work_count",
            ],
            rationale="High P@k may be prevalence-driven on a positive-heavy labeled subset.",
            blocking_for=[],
            required_for=[],
            advisory_text="This advisory does not fail heuristic gates." if g13_status == "advisory_warn" else None,
        )
    )

    return gates


def _build_gates_v2(
    *,
    scoring_payload: Mapping[str, Any],
    offline_metric_gates_payload: Mapping[str, Any],
    production_plan_payload: Mapping[str, Any],
    scorer_payload: Mapping[str, Any],
    scorer_sha256: str,
    shadow_scoring_allowed: bool,
    production_default_allowed: bool,
    shadow_blockers: Sequence[str],
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    scoring_metadata = _metadata(scoring_payload, name="production-candidate-scoring")
    scorer_metadata = _metadata(scorer_payload, name="audit-embedding-scorer-export")

    candidate_pool_definition = scoring_payload.get("candidate_pool_definition")
    g01_observed = {
        "metadata_present": isinstance(scoring_metadata, Mapping),
        "candidate_pool_definition_present": isinstance(candidate_pool_definition, Mapping),
        "experiment_version": scoring_metadata.get("experiment_version"),
        "scoring_mode": scoring_metadata.get("scoring_mode"),
        "target": scoring_metadata.get("target"),
        "metadata_ranking_run_id": scoring_metadata.get("ranking_run_id"),
        "metadata_family": scoring_metadata.get("family"),
        "candidate_pool_ranking_run_id": _get(scoring_payload, "candidate_pool_definition.ranking_run_id"),
        "candidate_pool_family": _get(scoring_payload, "candidate_pool_definition.family"),
    }
    g01_pass = (
        g01_observed["metadata_present"]
        and g01_observed["candidate_pool_definition_present"]
        and g01_observed["experiment_version"] == SCORING_VERSION_V2
        and g01_observed["scoring_mode"] == SCORING_MODE_AUDIT_EMBEDDING
        and g01_observed["target"] == TARGET
        and _nonempty(g01_observed["metadata_ranking_run_id"])
        and _nonempty(g01_observed["metadata_family"])
        and _nonempty(g01_observed["candidate_pool_ranking_run_id"])
        and _nonempty(g01_observed["candidate_pool_family"])
    )
    gates.append(
        _gate(
            "G01_input_scope",
            title="Input Scope",
            category="scope",
            status=_status(bool(g01_pass)),
            threshold={
                "experiment_version": SCORING_VERSION_V2,
                "scoring_mode": SCORING_MODE_AUDIT_EMBEDDING,
                "target": TARGET,
                "candidate_pool_definition_present": True,
                "ranking_run_id_present": True,
                "family_present": True,
            },
            observed_value=g01_observed,
            source_field_paths=[
                "scoring.metadata.experiment_version",
                "scoring.metadata.scoring_mode",
                "scoring.metadata.target",
                "scoring.metadata.ranking_run_id",
                "scoring.metadata.family",
                "scoring.candidate_pool_definition",
            ],
            rationale="The v2 diagnostic must identify the learned-scored product-candidate pool, ranking run, and family.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    audit_ranker_passed = offline_metric_gates_payload.get("audit_ranker_gates_passed")
    gates.append(
        _gate(
            "G02_prior_audit_gates_passed",
            title="Prior Audit Gates Passed",
            category="policy",
            status=_status(audit_ranker_passed is True),
            threshold={"audit_ranker_gates_passed": True},
            observed_value={"audit_ranker_gates_passed": audit_ranker_passed},
            source_field_paths=["offline_metric_gates.audit_ranker_gates_passed"],
            rationale="The audit-ranker evidence line must have passed before interpreting product-candidate diagnostics.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    candidate_count = _get(scoring_payload, "candidate_pool_summary.candidate_unique_canonical_work_count")
    g03_pass = _is_number(candidate_count) and candidate_count >= THRESHOLDS_V2["minimum_candidate_unique_work_count"]
    gates.append(
        _gate(
            "G03_candidate_pool_size",
            title="Candidate Pool Size",
            category="coverage",
            status=_status(g03_pass),
            threshold={"candidate_unique_work_count_min": THRESHOLDS_V2["minimum_candidate_unique_work_count"]},
            observed_value={"candidate_unique_canonical_work_count": candidate_count},
            source_field_paths=["scoring.candidate_pool_summary.candidate_unique_canonical_work_count"],
            rationale="The existing product-candidate pool must contain enough distinct works for a diagnostic gate.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    label_coverage = _get(scoring_payload, "label_join_summary.candidate_work_labeled_coverage_rate")
    labeled_work_count = _get(scoring_payload, "label_join_summary.labeled_eval_subset_work_count")
    g04_pass = (
        _is_number(label_coverage)
        and _is_number(labeled_work_count)
        and label_coverage >= THRESHOLDS_V2["minimum_candidate_label_coverage_rate"]
        and labeled_work_count >= THRESHOLDS_V2["minimum_labeled_eval_work_count"]
    )
    gates.append(
        _gate(
            "G04_label_coverage",
            title="Label Coverage",
            category="coverage",
            status=_status(g04_pass),
            threshold={
                "candidate_label_coverage_rate_min": THRESHOLDS_V2["minimum_candidate_label_coverage_rate"],
                "labeled_eval_work_count_min": THRESHOLDS_V2["minimum_labeled_eval_work_count"],
            },
            observed_value={
                "candidate_work_labeled_coverage_rate": label_coverage,
                "labeled_eval_subset_work_count": labeled_work_count,
            },
            source_field_paths=[
                "scoring.label_join_summary.candidate_work_labeled_coverage_rate",
                "scoring.label_join_summary.labeled_eval_subset_work_count",
            ],
            rationale="The learned application diagnostic needs broad labeled overlap and enough labeled works.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    negative_count = _get(scoring_payload, "label_join_summary.labeled_eval_subset_negative_work_count")
    g05_pass = _is_number(negative_count) and negative_count >= THRESHOLDS_V2["minimum_labeled_eval_negative_work_count"]
    gates.append(
        _gate(
            "G05_negative_coverage",
            title="Negative Coverage",
            category="coverage",
            status=_status(g05_pass),
            threshold={"labeled_eval_negative_work_count_min": THRESHOLDS_V2["minimum_labeled_eval_negative_work_count"]},
            observed_value={"labeled_eval_subset_negative_work_count": negative_count},
            source_field_paths=["scoring.label_join_summary.labeled_eval_subset_negative_work_count"],
            rationale="Negative labeled works are required to interpret separation metrics beyond positive-heavy top-k evidence.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    missing_rate = _missing_embedding_rate(scoring_payload)
    g06_pass = (
        missing_rate is not None
        and missing_rate <= THRESHOLDS_V2["maximum_missing_embedding_rate_for_labeled_observations"]
    )
    gates.append(
        _gate(
            "G06_embedding_coverage",
            title="Embedding Coverage",
            category="coverage",
            status=_status(g06_pass),
            threshold={
                "missing_embedding_rate_max": THRESHOLDS_V2[
                    "maximum_missing_embedding_rate_for_labeled_observations"
                ]
            },
            observed_value={
                "missing_embedding_count": _get(scoring_payload, "embedding_join_summary.missing_embedding_count"),
                "labeled_candidate_observation_count": _get(
                    scoring_payload, "embedding_join_summary.labeled_candidate_observation_count"
                ),
                "missing_embedding_rate": missing_rate,
            },
            source_field_paths=[
                "scoring.embedding_join_summary.missing_embedding_count",
                "scoring.embedding_join_summary.labeled_candidate_observation_count",
            ],
            rationale="The learned scorer can only be evaluated if labeled candidate observations mostly have embeddings.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    roc_auc = _get(scoring_payload, "heuristic_metrics.roc_auc_mann_whitney")
    g07_pass = _is_number(roc_auc) and roc_auc >= THRESHOLDS_V2["minimum_heuristic_roc_auc"]
    gates.append(
        _gate(
            "G07_heuristic_roc_auc",
            title="Heuristic ROC-AUC",
            category="metric",
            status=_status(g07_pass),
            threshold={"heuristic_roc_auc_min": THRESHOLDS_V2["minimum_heuristic_roc_auc"]},
            observed_value={
                "roc_auc_mann_whitney": roc_auc,
                "roc_auc_reason": _get(scoring_payload, "heuristic_metrics.roc_auc_reason"),
            },
            source_field_paths=[
                "scoring.heuristic_metrics.roc_auc_mann_whitney",
                "scoring.heuristic_metrics.roc_auc_reason",
            ],
            rationale="The existing heuristic final_score line should still clear its minimum diagnostic separation floor.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    average_precision = _get(scoring_payload, "heuristic_metrics.average_precision")
    g08_pass = _is_number(average_precision) and average_precision >= THRESHOLDS_V2["minimum_heuristic_average_precision"]
    gates.append(
        _gate(
            "G08_heuristic_average_precision",
            title="Heuristic Average Precision",
            category="metric",
            status=_status(g08_pass),
            threshold={"heuristic_average_precision_min": THRESHOLDS_V2["minimum_heuristic_average_precision"]},
            observed_value={
                "average_precision": average_precision,
                "average_precision_reason": _get(scoring_payload, "heuristic_metrics.average_precision_reason"),
            },
            source_field_paths=[
                "scoring.heuristic_metrics.average_precision",
                "scoring.heuristic_metrics.average_precision_reason",
            ],
            rationale="The heuristic final_score line remains a separate minimum evidence check.",
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    precision_at_10 = _get(scoring_payload, "heuristic_metrics.precision_recall_at_k.10.precision")
    p10_reason = _get(scoring_payload, "heuristic_metrics.precision_recall_at_k.10.reason")
    g09_pass = _is_number(precision_at_10) and precision_at_10 >= THRESHOLDS_V2["minimum_heuristic_precision_at_10"]
    gates.append(
        _gate(
            "G09_heuristic_top_k_precision",
            title="Heuristic Top-K Precision",
            category="metric",
            status=_status(g09_pass),
            threshold={"heuristic_precision_at_10_min": THRESHOLDS_V2["minimum_heuristic_precision_at_10"]},
            observed_value={"precision_at_10": precision_at_10, "reason": p10_reason},
            source_field_paths=[
                'scoring.heuristic_metrics.precision_recall_at_k["10"].precision',
                'scoring.heuristic_metrics.precision_recall_at_k["10"].reason',
            ],
            rationale=(
                "Top-10 precision on labeled works must clear the minimum heuristic threshold."
                if _is_number(precision_at_10)
                else "Top-10 precision is missing or null, so the top-k heuristic gate fails."
            ),
            blocking_for=["product_candidate_heuristic_gates", "shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    learned_roc_auc = _get(scoring_payload, "learned_or_embedding_metrics.metrics.roc_auc_mann_whitney")
    learned_average_precision = _get(scoring_payload, "learned_or_embedding_metrics.metrics.average_precision")
    learned_precision_at_10 = _get(
        scoring_payload, "learned_or_embedding_metrics.metrics.precision_recall_at_k.10.precision"
    )
    learned_score_name = _get(scoring_payload, "learned_or_embedding_metrics.metrics.score_name")
    learned_aggregation_policy = _get(scoring_payload, "scoring_mode_details.learned_score_aggregation_policy")
    g10_checks = {
        "scoring_mode_details_learned_product_scores_produced": _get(
            scoring_payload, "scoring_mode_details.learned_product_scores_produced"
        )
        is True,
        "product_candidate_rows_used_for_training_zero": _get(
            scoring_payload, "scoring_mode_details.product_candidate_rows_used_for_training"
        )
        == 0,
        "learned_or_embedding_metrics_learned_product_scores_produced": _get(
            scoring_payload, "learned_or_embedding_metrics.learned_product_scores_produced"
        )
        is True,
        "learned_roc_auc_floor": _is_number(learned_roc_auc)
        and learned_roc_auc >= THRESHOLDS_V2["minimum_learned_roc_auc"],
        "learned_average_precision_floor": _is_number(learned_average_precision)
        and learned_average_precision >= THRESHOLDS_V2["minimum_learned_average_precision"],
        "learned_precision_at_10_floor": _is_number(learned_precision_at_10)
        and learned_precision_at_10 >= THRESHOLDS_V2["minimum_learned_precision_at_10"],
        "score_name_expected": learned_score_name == "audit_embedding_probability_work",
    }
    gates.append(
        _gate(
            "G10_learned_scorer_application",
            title="Learned Scorer Application",
            category="learned_scorer",
            status=_status(all(g10_checks.values())),
            threshold={
                "learned_product_scores_produced": True,
                "product_candidate_rows_used_for_training": 0,
                "score_name": "audit_embedding_probability_work",
                "minimum_learned_roc_auc": THRESHOLDS_V2["minimum_learned_roc_auc"],
                "minimum_learned_average_precision": THRESHOLDS_V2["minimum_learned_average_precision"],
                "minimum_learned_precision_at_10": THRESHOLDS_V2["minimum_learned_precision_at_10"],
            },
            observed_value={
                "checks": g10_checks,
                "learned_score_aggregation_policy": learned_aggregation_policy,
                "roc_auc_mann_whitney": learned_roc_auc,
                "average_precision": learned_average_precision,
                "precision_at_10": learned_precision_at_10,
                "score_name": learned_score_name,
            },
            source_field_paths=[
                "scoring.scoring_mode_details.learned_product_scores_produced",
                "scoring.scoring_mode_details.product_candidate_rows_used_for_training",
                "scoring.scoring_mode_details.learned_score_aggregation_policy",
                "scoring.learned_or_embedding_metrics.learned_product_scores_produced",
                "scoring.learned_or_embedding_metrics.metrics.score_name",
                "scoring.learned_or_embedding_metrics.metrics.roc_auc_mann_whitney",
                "scoring.learned_or_embedding_metrics.metrics.average_precision",
                'scoring.learned_or_embedding_metrics.metrics.precision_recall_at_k["10"].precision',
            ],
            rationale=(
                "The frozen scorer was applied with minimum diagnostic floors on the labeled overlap; "
                "this is not independent validation."
            ),
            blocking_for=["learned_scorer_application_gates", "shadow_scoring", "production_default"],
            required_for=["learned_scorer_application_gates"],
        )
    )

    scoring_scorer_sha = _get(scoring_payload, "scoring_mode_details.audit_embedding_scorer_sha256")
    scorer_input_sha = _input_sha(scoring_metadata, "audit_embedding_scorer_export")
    g11_checks = {
        "supplied_scorer_sha_matches_scoring_details": scorer_sha256 == scoring_scorer_sha,
        "supplied_scorer_sha_matches_scoring_inputs": scorer_sha256 == scorer_input_sha,
        "scorer_artifact_type": scorer_metadata.get("artifact_type") == AUDIT_SCORER_ARTIFACT_TYPE,
        "scorer_version": scorer_metadata.get("scorer_version") == AUDIT_SCORER_VERSION,
        "fit_mode": scorer_metadata.get("fit_mode") == "full_fit_audit_corpus",
        "target": scorer_metadata.get("target") == TARGET,
        "product_candidate_pool_used_for_training": _get(
            scorer_payload, "policy_compliance.product_candidate_pool_used_for_training"
        )
        is False,
        "shadow_scoring_authorized": _get(scorer_payload, "policy_compliance.shadow_scoring_authorized") is False,
        "production_artifact_written": _get(scorer_payload, "policy_compliance.production_artifact_written") is False,
    }
    gates.append(
        _gate(
            "G11_scorer_provenance",
            title="Scorer Provenance",
            category="provenance",
            status=_status(all(g11_checks.values())),
            threshold={
                "scorer_artifact_type": AUDIT_SCORER_ARTIFACT_TYPE,
                "scorer_version": AUDIT_SCORER_VERSION,
                "fit_mode": "full_fit_audit_corpus",
                "target": TARGET,
                "product_candidate_pool_used_for_training": False,
                "shadow_scoring_authorized": False,
                "production_artifact_written": False,
                "sha256_matches_scoring": True,
            },
            observed_value={
                "checks": g11_checks,
                "supplied_scorer_sha256": scorer_sha256,
                "scoring_mode_details_scorer_sha256": scoring_scorer_sha,
                "scoring_inputs_scorer_sha256": scorer_input_sha,
                "embedding_dimensions": scorer_metadata.get("embedding_dimensions"),
            },
            source_field_paths=[
                "scoring.scoring_mode_details.audit_embedding_scorer_sha256",
                "scoring.metadata.inputs.audit_embedding_scorer_export.sha256",
                "scorer.metadata.artifact_type",
                "scorer.metadata.scorer_version",
                "scorer.metadata.fit_mode",
                "scorer.metadata.target",
                "scorer.policy_compliance.product_candidate_pool_used_for_training",
                "scorer.policy_compliance.shadow_scoring_authorized",
                "scorer.policy_compliance.production_artifact_written",
            ],
            rationale="The scoring artifact must point to the exact frozen audit scorer export that is supplied here.",
            blocking_for=["learned_scorer_application_gates", "shadow_scoring", "production_default"],
            required_for=["learned_scorer_application_gates"],
        )
    )

    gates.append(
        _gate(
            "G12_independent_validation_status",
            title="Independent Validation Status",
            category="validation",
            status="not_evaluated",
            threshold={"independent_holdout_policy_present": True},
            observed_value={
                "scorer_fit_mode": scorer_metadata.get("fit_mode"),
                "product_candidate_pool_used_for_training": _get(
                    scorer_payload, "policy_compliance.product_candidate_pool_used_for_training"
                ),
                "labeled_overlap_subset_of_audit_training_corpus": True,
                "independent_holdout_policy_present": False,
            },
            source_field_paths=[
                "scorer.metadata.fit_mode",
                "scorer.policy_compliance.product_candidate_pool_used_for_training",
                "scoring.metadata.inputs.label_dataset",
                "scoring.metadata.inputs.embeddings",
            ],
            rationale=(
                "The v2 overlap uses the same label dataset and embedding rows as the audit-training universe; "
                "these metrics are not holdout validation."
            ),
            blocking_for=["shadow_scoring", "production_default"],
            required_for=[],
        )
    )

    scoring_shadow_blockers = scoring_payload.get("blockers_to_shadow")
    advisory_text = None
    if isinstance(scoring_shadow_blockers, Sequence) and not isinstance(scoring_shadow_blockers, (str, bytes)):
        blocker_text = " ".join(str(item).lower() for item in scoring_shadow_blockers)
        if "learned metric gates not yet evaluated" in blocker_text or "metric gates not yet evaluated" in blocker_text:
            advisory_text = (
                "The scoring artifact still lists unevaluated product-candidate gates; this v2 artifact supersedes "
                "that blocker for offline learned application gates only."
            )
    required_blockers_present = set(SHADOW_BLOCKERS_V2).issubset(set(shadow_blockers))
    g13_pass = shadow_scoring_allowed is False and required_blockers_present
    gates.append(
        _gate(
            "G13_shadow_blockers_documented",
            title="Shadow Blockers Documented",
            category="policy",
            status=_status(g13_pass),
            threshold={
                "shadow_scoring_allowed": False,
                "shadow_blockers_include": list(SHADOW_BLOCKERS_V2),
            },
            observed_value={
                "shadow_scoring_allowed": shadow_scoring_allowed,
                "shadow_blockers": list(shadow_blockers),
                "scoring_blockers_to_shadow": scoring_shadow_blockers if isinstance(scoring_shadow_blockers, list) else None,
            },
            source_field_paths=[
                "this.shadow_scoring_allowed",
                "this.shadow_blockers",
                "scoring.blockers_to_shadow",
            ],
            rationale="The gate artifact must explicitly keep shadow blocked even when learned application gates pass.",
            blocking_for=["shadow_scoring", "production_default"],
            required_for=["product_candidate_heuristic_gates"],
            advisory_text=advisory_text,
        )
    )

    good = _get(production_plan_payload, "targets.good_or_acceptable")
    surprising = _get(production_plan_payload, "targets.surprising_or_useful")
    if not isinstance(good, Mapping):
        good = {}
    if not isinstance(surprising, Mapping):
        surprising = {}
    overall_status = _get(production_plan_payload, "metadata.overall_status")
    surprising_status = str(surprising.get("status") or "").strip().lower()
    surprising_deferred_or_excluded = "deferred" in surprising_status or "excluded" in surprising_status
    g14_pass = (
        good.get("production_eligible") is False
        and surprising_deferred_or_excluded
        and _blocked_overall_status(overall_status)
    )
    gates.append(
        _gate(
            "G14_production_readiness_alignment",
            title="Production Readiness Alignment",
            category="policy",
            status=_status(g14_pass),
            threshold={
                "good_or_acceptable_production_eligible": False,
                "surprising_or_useful_deferred_or_excluded": True,
                "overall_status_blocked_posture": True,
            },
            observed_value={
                "overall_status": overall_status,
                "good_or_acceptable": {
                    "status": good.get("status"),
                    "allowed_next_stage": good.get("allowed_next_stage"),
                    "production_eligible": good.get("production_eligible"),
                },
                "surprising_or_useful": {
                    "status": surprising.get("status"),
                    "allowed_next_stage": surprising.get("allowed_next_stage"),
                    "production_eligible": surprising.get("production_eligible"),
                },
            },
            source_field_paths=[
                "production_readiness_plan.metadata.overall_status",
                "production_readiness_plan.targets.good_or_acceptable.production_eligible",
                "production_readiness_plan.targets.surprising_or_useful.status",
            ],
            rationale="Offline product-candidate gates may advance only while the production plan remains blocked.",
            blocking_for=["production_default"],
            required_for=["product_candidate_heuristic_gates"],
        )
    )

    positive_rate = _positive_prevalence(scoring_payload)
    if positive_rate is None:
        g15_status = "not_applicable"
    elif positive_rate > THRESHOLDS_V2["high_positive_work_prevalence_advisory_threshold"]:
        g15_status = "advisory_warn"
    else:
        g15_status = "pass"
    gates.append(
        _gate(
            "G15_positive_prevalence_advisory",
            title="Positive Prevalence Advisory",
            category="advisory",
            status=g15_status,
            threshold={
                "high_positive_work_prevalence_advisory_threshold": THRESHOLDS_V2[
                    "high_positive_work_prevalence_advisory_threshold"
                ]
            },
            observed_value={
                "labeled_eval_subset_positive_work_count": _get(
                    scoring_payload, "label_join_summary.labeled_eval_subset_positive_work_count"
                ),
                "labeled_eval_subset_work_count": _get(
                    scoring_payload, "label_join_summary.labeled_eval_subset_work_count"
                ),
                "positive_work_prevalence": positive_rate,
            },
            source_field_paths=[
                "scoring.label_join_summary.labeled_eval_subset_positive_work_count",
                "scoring.label_join_summary.labeled_eval_subset_work_count",
            ],
            rationale="High P@k may be prevalence-driven on a positive-heavy labeled subset.",
            blocking_for=[],
            required_for=[],
            advisory_text="This advisory does not fail pass/fail gates." if g15_status == "advisory_warn" else None,
        )
    )

    near_perfect = (
        (_is_number(learned_roc_auc) and learned_roc_auc >= THRESHOLDS_V2["near_perfect_learned_roc_auc_advisory_threshold"])
        or (
            _is_number(learned_average_precision)
            and learned_average_precision >= THRESHOLDS_V2["near_perfect_learned_average_precision_advisory_threshold"]
        )
    )
    gates.append(
        _gate(
            "G16_near_perfect_learned_metrics_advisory",
            title="Near-Perfect Learned Metrics Advisory",
            category="advisory",
            status="advisory_warn" if near_perfect else "pass",
            threshold={
                "near_perfect_learned_roc_auc_advisory_threshold": THRESHOLDS_V2[
                    "near_perfect_learned_roc_auc_advisory_threshold"
                ],
                "near_perfect_learned_average_precision_advisory_threshold": THRESHOLDS_V2[
                    "near_perfect_learned_average_precision_advisory_threshold"
                ],
            },
            observed_value={
                "learned_roc_auc": learned_roc_auc,
                "learned_average_precision": learned_average_precision,
            },
            source_field_paths=[
                "scoring.learned_or_embedding_metrics.metrics.roc_auc_mann_whitney",
                "scoring.learned_or_embedding_metrics.metrics.average_precision",
            ],
            rationale="Near-perfect learned metrics may reflect audit-corpus training overlap, not generalization.",
            blocking_for=[],
            required_for=[],
            advisory_text="This advisory does not fail pass/fail gates." if near_perfect else None,
        )
    )

    return gates


def _gate_status_map(gates: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    return {str(gate["gate_id"]): str(gate["status"]) for gate in gates}


def _overall_outcomes(gates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = _gate_status_map(gates)
    failed_required = [gate_id for gate_id in REQUIRED_HEURISTIC_GATE_IDS if statuses.get(gate_id) != "pass"]
    heuristic_passed = not failed_required
    learned_passed = statuses.get("G10_learned_scorer_status") == "pass"
    if failed_required:
        recommended = "blocked_pending_product_candidate_heuristic_gate_failures"
    elif not learned_passed:
        recommended = "create_frozen_audit_embedding_scorer_export_v1"
    else:
        recommended = "draft_ml_shadow_scorer_v1_spec"

    blocked_reasons = list(failed_required)
    if statuses.get("G10_learned_scorer_status") == "not_evaluated":
        blocked_reasons.append("G10_learned_scorer_status:not_evaluated")
    elif not learned_passed:
        blocked_reasons.append("G10_learned_scorer_status")
    blocked_reasons.append("missing_ml_shadow_scorer_v1")

    return {
        "product_candidate_heuristic_gates_passed": heuristic_passed,
        "learned_scorer_product_candidate_gates_passed": learned_passed,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": recommended,
        "blocked_reasons": blocked_reasons,
        "shadow_blockers": list(SHADOW_BLOCKERS),
    }


def _overall_outcomes_v2(gates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = _gate_status_map(gates)
    failed_required = [gate_id for gate_id in REQUIRED_HEURISTIC_GATE_IDS_V2 if statuses.get(gate_id) != "pass"]
    heuristic_passed = not failed_required
    learned_passed = statuses.get("G10_learned_scorer_application") == "pass" and statuses.get(
        "G11_scorer_provenance"
    ) == "pass"
    independent_passed = False

    if failed_required:
        recommended = "blocked_pending_product_candidate_heuristic_gate_failures"
    elif not learned_passed:
        recommended = "revisit_scorer_export_or_features"
    elif statuses.get("G12_independent_validation_status") == "not_evaluated":
        recommended = "create_learned_scorer_holdout_policy_v1"
    else:
        recommended = "draft_ml_shadow_scorer_v1_spec"

    failed_gate_ids = [str(gate["gate_id"]) for gate in gates if gate.get("status") == "fail"]
    blocked_reasons = list(dict.fromkeys(failed_gate_ids + failed_required))
    blocked_reasons.extend(
        [
            "G12_independent_validation_status:not_evaluated",
            "missing_ml_shadow_scorer_v1",
            "production_default_blocked",
        ]
    )

    return {
        "product_candidate_heuristic_gates_passed": heuristic_passed,
        "learned_scorer_application_gates_passed": learned_passed,
        "independent_learned_validation_passed": independent_passed,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": recommended,
        "blocked_reasons": blocked_reasons,
        "shadow_blockers": list(SHADOW_BLOCKERS_V2),
    }


def build_ml_offline_production_candidate_metric_gates_payload(
    *,
    production_candidate_scoring_path: Path,
    offline_metric_gates_path: Path,
    split_policy_path: Path,
    production_readiness_plan_path: Path,
    audit_embedding_scorer_export_path: Path | None = None,
    production_candidate_metric_gates_v1_path: Path | None = None,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if gates_version not in {GATES_VERSION, GATES_VERSION_V2}:
        raise MLOfflineProductionCandidateMetricGatesError(f"unsupported gates_version: {gates_version!r}")
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    scoring_path = Path(production_candidate_scoring_path).resolve()
    offline_gates_path = Path(offline_metric_gates_path).resolve()
    policy_path = Path(split_policy_path).resolve()
    plan_path = Path(production_readiness_plan_path).resolve()
    scorer_path = Path(audit_embedding_scorer_export_path).resolve() if audit_embedding_scorer_export_path else None
    prior_v1_path = (
        Path(production_candidate_metric_gates_v1_path).resolve()
        if production_candidate_metric_gates_v1_path
        else None
    )

    scoring_payload = _load_json_object(scoring_path)
    offline_metric_gates_payload = _load_json_object(offline_gates_path)
    split_policy_payload = _load_json_object(policy_path)
    production_plan_payload = _load_json_object(plan_path)

    offline_metric_gates_metadata = _validate_offline_metric_gates(offline_metric_gates_payload)
    split_policy_metadata = _validate_split_policy(split_policy_payload)
    production_plan_metadata = _validate_production_plan(production_plan_payload)
    inputs = [
        _input_record("production_candidate_scoring", scoring_path, repo_root=root),
        _input_record("offline_metric_gates", offline_gates_path, repo_root=root),
        _input_record("split_policy", policy_path, repo_root=root),
        _input_record("production_readiness_plan", plan_path, repo_root=root),
    ]
    coverage_summary = {
        "candidate_unique_canonical_work_count": _get(
            scoring_payload, "candidate_pool_summary.candidate_unique_canonical_work_count"
        ),
        "candidate_work_labeled_coverage_rate": _get(
            scoring_payload, "label_join_summary.candidate_work_labeled_coverage_rate"
        ),
        "labeled_eval_subset_work_count": _get(scoring_payload, "label_join_summary.labeled_eval_subset_work_count"),
        "labeled_eval_subset_positive_work_count": _get(
            scoring_payload, "label_join_summary.labeled_eval_subset_positive_work_count"
        ),
        "labeled_eval_subset_negative_work_count": _get(
            scoring_payload, "label_join_summary.labeled_eval_subset_negative_work_count"
        ),
        "missing_embedding_rate": _missing_embedding_rate(scoring_payload),
    }
    heuristic_metric_summary = {
        "roc_auc_mann_whitney": _get(scoring_payload, "heuristic_metrics.roc_auc_mann_whitney"),
        "average_precision": _get(scoring_payload, "heuristic_metrics.average_precision"),
        "precision_at_10": _get(scoring_payload, "heuristic_metrics.precision_recall_at_k.10.precision"),
        "positive_work_prevalence": _positive_prevalence(scoring_payload),
    }

    if gates_version == GATES_VERSION:
        scoring_metadata = _validate_scoring(scoring_payload)
        base_outcomes = {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "shadow_blockers": list(SHADOW_BLOCKERS),
        }
        gates = _build_gates(
            scoring_payload=scoring_payload,
            offline_metric_gates_payload=offline_metric_gates_payload,
            production_plan_payload=production_plan_payload,
            shadow_scoring_allowed=base_outcomes["shadow_scoring_allowed"],
            production_default_allowed=base_outcomes["production_default_allowed"],
            shadow_blockers=base_outcomes["shadow_blockers"],
        )
        outcomes = _overall_outcomes(gates)

        metadata = {
            "artifact_type": ARTIFACT_TYPE,
            "gates_version": gates_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "target": scoring_metadata.get("target"),
            "ranking_run_id": scoring_metadata.get("ranking_run_id"),
            "family": scoring_metadata.get("family"),
            "scoring_mode": scoring_metadata.get("scoring_mode"),
            "production_candidate_scoring_version": scoring_metadata.get("experiment_version"),
            "offline_metric_gates_version": offline_metric_gates_metadata.get("gates_version"),
            "split_policy_version": split_policy_metadata.get("policy_version"),
            "production_readiness_plan_version": production_plan_metadata.get("plan_version"),
            "thresholds_version": THRESHOLDS_VERSION,
            "thresholds": dict(THRESHOLDS),
            "gate_status_enum": list(GATE_STATUS_ENUM),
            "strategic_framing": {
                "evaluates_existing_product_candidate_pool": True,
                "existing_pool_source": "paper_scores",
                "read_only_reuse": True,
                "not_live_recommender_quality": True,
                "expected_v1_scoring_mode": SCORING_MODE_HEURISTIC,
                "heuristic_final_score_evidence_may_pass": True,
                "learned_arm_evaluated": False,
                "shadow_scoring_allowed_always_false_in_v1": True,
                "production_default_allowed_always_false_in_v1": True,
                "passing_heuristic_gates_authorizes_only": "create_frozen_audit_embedding_scorer_export_v1",
                "passing_heuristic_gates_does_not_authorize": ["shadow_scoring", "production_default"],
            },
            "caveats": list(CAVEATS),
        }

        return {
            "metadata": metadata,
            "gates": gates,
            **outcomes,
            "coverage_summary": coverage_summary,
            "heuristic_metric_summary": heuristic_metric_summary,
        }

    if scorer_path is None:
        raise MLOfflineProductionCandidateMetricGatesError(
            "--audit-embedding-scorer-export is required for ml-offline-production-candidate-metric-gates-v2"
        )
    scorer_payload = _load_json_object(scorer_path)
    scorer_metadata = _validate_audit_embedding_scorer(scorer_payload)
    scorer_record = _input_record("audit_embedding_scorer_export", scorer_path, repo_root=root)
    scorer_sha256 = scorer_record["sha256"]
    scoring_metadata = _validate_scoring_v2(
        scoring_payload,
        scorer_sha256=scorer_sha256,
        scorer_metadata=scorer_metadata,
    )
    inputs.append(scorer_record)
    if prior_v1_path is not None:
        prior_v1_payload = _load_json_object(prior_v1_path)
        _validate_prior_v1_metric_gates(prior_v1_payload)
        inputs.append(_input_record("production_candidate_metric_gates_v1", prior_v1_path, repo_root=root))

    base_outcomes_v2 = {
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "shadow_blockers": list(SHADOW_BLOCKERS_V2),
    }
    gates = _build_gates_v2(
        scoring_payload=scoring_payload,
        offline_metric_gates_payload=offline_metric_gates_payload,
        production_plan_payload=production_plan_payload,
        scorer_payload=scorer_payload,
        scorer_sha256=scorer_sha256,
        shadow_scoring_allowed=base_outcomes_v2["shadow_scoring_allowed"],
        production_default_allowed=base_outcomes_v2["production_default_allowed"],
        shadow_blockers=base_outcomes_v2["shadow_blockers"],
    )
    outcomes = _overall_outcomes_v2(gates)
    learned_metrics = _get(scoring_payload, "learned_or_embedding_metrics.metrics")
    if not isinstance(learned_metrics, Mapping):
        learned_metrics = {}
    comparison_to_heuristic = _get(scoring_payload, "learned_or_embedding_metrics.comparison_to_heuristic")
    if not isinstance(comparison_to_heuristic, Mapping):
        comparison_to_heuristic = {}

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": gates_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "target": scoring_metadata.get("target"),
        "ranking_run_id": scoring_metadata.get("ranking_run_id"),
        "family": scoring_metadata.get("family"),
        "scoring_mode": scoring_metadata.get("scoring_mode"),
        "production_candidate_scoring_version": scoring_metadata.get("experiment_version"),
        "offline_metric_gates_version": offline_metric_gates_metadata.get("gates_version"),
        "split_policy_version": split_policy_metadata.get("policy_version"),
        "production_readiness_plan_version": production_plan_metadata.get("plan_version"),
        "audit_embedding_scorer_version": scorer_metadata.get("scorer_version"),
        "audit_embedding_scorer_sha256": scorer_sha256,
        "thresholds_version": THRESHOLDS_VERSION_V2,
        "thresholds": dict(THRESHOLDS_V2),
        "gate_status_enum": list(GATE_STATUS_ENUM),
        "strategic_framing": {
            "learned_audit_scorer_full_fit_on_audit_labeled_v8_corpus": True,
            "learned_audit_scorer_artifact": AUDIT_SCORER_VERSION,
            "product_candidate_labeled_overlap_uses_same_label_dataset_and_embedding_rows_by_row_id": True,
            "strong_learned_metrics_mean": (
                "successful scorer application and minimum diagnostic separation on the labeled overlap"
            ),
            "strong_learned_metrics_do_not_mean": ["independent_validation", "shadow_readiness"],
            "passing_v2_gates_authorizes_only": "create_learned_scorer_holdout_policy_v1",
            "passing_v2_gates_does_not_authorize": ["shadow_scoring", "production_default"],
            "existing_pool_source": "paper_scores",
            "read_only_reuse": True,
            "not_live_recommender_quality": True,
        },
        "caveats": list(CAVEATS_V2),
    }

    return {
        "metadata": metadata,
        "gates": gates,
        **outcomes,
        "coverage_summary": coverage_summary,
        "heuristic_metric_summary": heuristic_metric_summary,
        "learned_metric_summary": {
            "metric_level": learned_metrics.get("metric_level"),
            "score_name": learned_metrics.get("score_name"),
            "roc_auc_mann_whitney": learned_metrics.get("roc_auc_mann_whitney"),
            "average_precision": learned_metrics.get("average_precision"),
            "precision_at_10": _get(learned_metrics, "precision_recall_at_k.10.precision"),
            "positive_work_prevalence": _positive_prevalence(scoring_payload),
        },
        "comparison_to_heuristic": {
            "delta_roc_auc": comparison_to_heuristic.get("delta_roc_auc"),
            "delta_average_precision": comparison_to_heuristic.get("delta_average_precision"),
            "delta_precision_at_10": comparison_to_heuristic.get("delta_precision_at_10"),
            "side_by_side": comparison_to_heuristic.get("side_by_side"),
        },
    }


def _gate_counts(gates: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for gate in gates:
        status = str(gate.get("status"))
        counts[status] = counts.get(status, 0) + 1
    return dict(sorted(counts.items()))


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _compact(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, (int, bool, str)):
        return str(value)
    return json.dumps(value, sort_keys=True)


def _markdown_v2(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    gates = payload["gates"]
    coverage = payload["coverage_summary"]
    heuristic = payload["heuristic_metric_summary"]
    learned = payload["learned_metric_summary"]
    comparison = payload.get("comparison_to_heuristic", {})
    if not isinstance(comparison, Mapping):
        comparison = {}
    validation_gate = next(gate for gate in gates if gate["gate_id"] == "G12_independent_validation_status")
    advisory_gates = [
        gate
        for gate in gates
        if gate["gate_id"]
        in {"G15_positive_prevalence_advisory", "G16_near_perfect_learned_metrics_advisory"}
        and gate["status"] == "advisory_warn"
    ]

    lines = [
        f"# Product-Candidate Learned Metric Gates ({metadata['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This evaluates the v2 product-candidate offline diagnostic where the frozen audit embedding scorer was applied to the existing labeled product-candidate overlap.",
        "",
        "The learned audit scorer was full-fit on the audit-labeled v8 corpus (`ml-offline-audit-embedding-scorer-v1`). The v2 product-candidate labeled overlap uses the same label dataset and embedding rows by `row_id` as that training universe. Strong learned metrics here prove successful scorer application and minimum diagnostic separation on the overlap; they are not independent validation and not shadow readiness.",
        "",
        f"- **Product-candidate heuristic gates passed:** {payload['product_candidate_heuristic_gates_passed']}",
        f"- **Learned scorer application gates passed:** {payload['learned_scorer_application_gates_passed']}",
        f"- **Independent learned validation passed:** {payload['independent_learned_validation_passed']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['production_default_allowed']}",
        "",
        "Passing v2 gates authorizes defining a learned-scorer holdout/split policy next. It does not authorize shadow implementation.",
        "",
        "## Gate Checklist",
        "",
        "| Gate | Title | Status | Required for | Rationale |",
        "| --- | --- | --- | --- | --- |",
    ]
    for gate in gates:
        required_for = ", ".join(gate["required_for"]) or "advisory"
        lines.append(
            f"| `{gate['gate_id']}` | {gate['title']} | {gate['status']} | {required_for} | {gate['rationale']} |"
        )

    lines.extend(
        [
            "",
            "## Coverage",
            "",
            "| Measure | Value |",
            "| --- | ---: |",
            f"| Candidate unique works | {_fmt(coverage['candidate_unique_canonical_work_count'])} |",
            f"| Candidate label coverage rate | {_fmt(coverage['candidate_work_labeled_coverage_rate'])} |",
            f"| Labeled eval works | {_fmt(coverage['labeled_eval_subset_work_count'])} |",
            f"| Labeled positive works | {_fmt(coverage['labeled_eval_subset_positive_work_count'])} |",
            f"| Labeled negative works | {_fmt(coverage['labeled_eval_subset_negative_work_count'])} |",
            f"| Missing embedding rate | {_fmt(coverage['missing_embedding_rate'])} |",
            "",
            "## Heuristic Summary",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| ROC-AUC (Mann-Whitney) | {_fmt(heuristic['roc_auc_mann_whitney'])} |",
            f"| Average precision | {_fmt(heuristic['average_precision'])} |",
            f"| Precision@10 | {_fmt(heuristic['precision_at_10'])} |",
            f"| Positive work prevalence | {_fmt(heuristic['positive_work_prevalence'])} |",
            "",
            "## Learned Application Summary",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| Score name | `{_compact(learned['score_name'])}` |",
            f"| Metric level | `{_compact(learned['metric_level'])}` |",
            f"| ROC-AUC (Mann-Whitney) | {_fmt(learned['roc_auc_mann_whitney'])} |",
            f"| Average precision | {_fmt(learned['average_precision'])} |",
            f"| Precision@10 | {_fmt(learned['precision_at_10'])} |",
            "",
            "## Heuristic vs Learned Comparison",
            "",
            "| Delta | Value |",
            "| --- | ---: |",
            f"| ROC-AUC delta | {_fmt(comparison.get('delta_roc_auc'))} |",
            f"| Average precision delta | {_fmt(comparison.get('delta_average_precision'))} |",
            f"| Precision@10 delta | {_fmt(comparison.get('delta_precision_at_10'))} |",
            "",
            "## Independent Validation Status",
            "",
            f"- **Gate status:** {validation_gate['status']}",
            "- The labeled overlap is not an independent holdout because it uses the same audit-labeled corpus and embedding rows as the full-fit scorer training universe.",
            "- Shadow scoring and production default remain blocked until an independent learned validation policy and artifact exist.",
            "",
        ]
    )

    if advisory_gates:
        lines.extend(["## Advisories", ""])
        for gate in advisory_gates:
            lines.append(f"- `{gate['gate_id']}`: {gate['rationale']}")
        lines.append("")

    lines.extend(
        [
            "## Not Shadow / Not Production",
            "",
            "- This is not shadow scoring.",
            "- This is not production scoring.",
            "- No `ml-shadow-scorer-v1` contract exists.",
            "- No production model artifact exists.",
            "- No ranking/API/web changes were made.",
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
    lines.extend(["", f"Gate status counts: `{_gate_counts(gates)}`", ""])
    return "\n".join(lines)


def markdown_from_ml_offline_production_candidate_metric_gates(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    if metadata["gates_version"] == GATES_VERSION_V2:
        return _markdown_v2(payload)
    gates = payload["gates"]
    coverage = payload["coverage_summary"]
    metrics = payload["heuristic_metric_summary"]
    learned_gate = next(gate for gate in gates if gate["gate_id"] == "G10_learned_scorer_status")
    advisory_gate = next(gate for gate in gates if gate["gate_id"] == "G13_positive_prevalence_advisory")

    lines = [
        f"# Product-Candidate Offline Metric Gates ({metadata['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This evaluates the product-candidate offline diagnostic on the existing `paper_scores` pool. It is read-only reuse, not live recommender validation.",
        "",
        f"- **Product-candidate heuristic gates passed:** {payload['product_candidate_heuristic_gates_passed']}",
        f"- **Learned scorer product-candidate gates passed:** {payload['learned_scorer_product_candidate_gates_passed']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['production_default_allowed']}",
        "",
        "v1 expects `heuristic_and_coverage_only`: heuristic `final_score` evidence may pass, but the learned arm is not evaluated. Passing heuristic gates authorizes only the next offline artifact, `create_frozen_audit_embedding_scorer_export_v1`, not shadow.",
        "",
        "## Gate Checklist",
        "",
        "| Gate | Title | Status | Required for | Rationale |",
        "| --- | --- | --- | --- | --- |",
    ]
    for gate in gates:
        required_for = ", ".join(gate["required_for"]) or "advisory"
        lines.append(
            f"| `{gate['gate_id']}` | {gate['title']} | {gate['status']} | {required_for} | {gate['rationale']} |"
        )

    lines.extend(
        [
            "",
            "## Product-Candidate Coverage Summary",
            "",
            "| Measure | Value |",
            "| --- | ---: |",
            f"| Candidate unique works | {_fmt(coverage['candidate_unique_canonical_work_count'])} |",
            f"| Candidate label coverage rate | {_fmt(coverage['candidate_work_labeled_coverage_rate'])} |",
            f"| Labeled eval works | {_fmt(coverage['labeled_eval_subset_work_count'])} |",
            f"| Labeled positive works | {_fmt(coverage['labeled_eval_subset_positive_work_count'])} |",
            f"| Labeled negative works | {_fmt(coverage['labeled_eval_subset_negative_work_count'])} |",
            f"| Missing embedding rate | {_fmt(coverage['missing_embedding_rate'])} |",
            "",
            "## Heuristic Metric Summary",
            "",
            "| Metric | Value |",
            "| --- | ---: |",
            f"| ROC-AUC (Mann-Whitney) | {_fmt(metrics['roc_auc_mann_whitney'])} |",
            f"| Average precision | {_fmt(metrics['average_precision'])} |",
            f"| Precision@10 | {_fmt(metrics['precision_at_10'])} |",
            f"| Positive work prevalence | {_fmt(metrics['positive_work_prevalence'])} |",
            "",
            "## Learned Scorer Status",
            "",
            f"- **Gate status:** {learned_gate['status']}",
            f"- **Scoring mode:** `{_compact(learned_gate['observed_value'].get('scoring_mode'))}`",
            f"- **Rationale:** {learned_gate['rationale']}",
            "",
        ]
    )

    if advisory_gate["status"] == "advisory_warn":
        lines.extend(
            [
                "## Positive Prevalence Advisory",
                "",
                "The labeled eval subset is positive-heavy, so high P@k may be prevalence-driven. This advisory does not fail heuristic gates.",
                "",
            ]
        )

    lines.extend(
        [
            "## Not Shadow / Not Production",
            "",
            "- This is not shadow scoring.",
            "- This is not production scoring.",
            "- `shadow_scoring_allowed` is always false in v1.",
            "- `production_default_allowed` is always false in v1.",
            "- No `ml-shadow-scorer-v1` contract exists.",
            "- No production model artifact exists.",
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
    lines.extend(["", f"Gate status counts: `{_gate_counts(gates)}`", ""])
    return "\n".join(lines)


def write_ml_offline_production_candidate_metric_gates(
    *,
    production_candidate_scoring_path: Path,
    offline_metric_gates_path: Path,
    split_policy_path: Path,
    production_readiness_plan_path: Path,
    audit_embedding_scorer_export_path: Path | None = None,
    production_candidate_metric_gates_v1_path: Path | None = None,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_offline_production_candidate_metric_gates_payload(
        production_candidate_scoring_path=production_candidate_scoring_path,
        offline_metric_gates_path=offline_metric_gates_path,
        split_policy_path=split_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        audit_embedding_scorer_export_path=audit_embedding_scorer_export_path,
        production_candidate_metric_gates_v1_path=production_candidate_metric_gates_v1_path,
        gates_version=gates_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_offline_production_candidate_metric_gates(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "GATES_VERSION",
    "GATES_VERSION_V2",
    "MLOfflineProductionCandidateMetricGatesError",
    "build_ml_offline_production_candidate_metric_gates_payload",
    "markdown_from_ml_offline_production_candidate_metric_gates",
    "write_ml_offline_production_candidate_metric_gates",
]

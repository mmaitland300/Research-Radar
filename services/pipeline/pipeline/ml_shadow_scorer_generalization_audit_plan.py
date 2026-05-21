"""Generalization audit plan for ml-shadow-scorer-v1.

This command drafts the second-surface generalization audit plan required
before any online shadow runtime implementation. It reads committed JSON
artifacts only. It does not query databases, execute scoring, materialize a
surface, run ranking, train, generate embeddings, ingest labels, implement
online shadowing, or authorize production behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"

ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"
AUDIT_OUTPUT_GATES_ARTIFACT_TYPE = "ml_shadow_scorer_v1_audit_output_gates"
AUDIT_OUTPUT_GATES_VERSION = "ml-shadow-scorer-v1-audit-output-gates"
SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
FRESH_SURFACE_POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
FRESH_SURFACE_POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

SCORER_ID = "ml-shadow-scorer-v1"
FORMULA_ID = "hybrid_rank_mean_50_50"
RANKING_RUN_ID = "rank-9f4b2a2084"
FAMILY = "emerging"
CORPUS_SNAPSHOT_VERSION = "source-snapshot-fresh-hybrid-v1-20260518"
EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"
VALIDATED_CANDIDATE_POOL_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
ONLINE_POLICY_NEXT_STAGE = "draft_ml_shadow_scorer_v1_generalization_audit_v1"
PASSED_NEXT_STAGE = "materialize_or_select_second_fresh_surface_for_shadow_generalization_v1"

CAVEATS = (
    "Plan only; no generalization audit execution occurs.",
    "No second surface is materialized or selected by this command.",
    "No online shadow runtime, API/web behavior, production default, scorer execution, ranking, training, embeddings, or label ingest is authorized.",
    "First-surface evidence does not generalize until a second-surface audit and gates pass.",
)


class MLShadowScorerGeneralizationAuditPlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerGeneralizationAuditPlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerGeneralizationAuditPlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerGeneralizationAuditPlanError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerGeneralizationAuditPlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    if metadata.get("artifact_type") != artifact_type:
        raise MLShadowScorerGeneralizationAuditPlanError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerGeneralizationAuditPlanError(f"{name} metadata.{version_field} must be {version}")
    return metadata


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerGeneralizationAuditPlanError(f"{name} must be {expected!r}, got {observed!r}")


def _matching_outcome_value(payload: Mapping[str, Any], field: str) -> Any:
    top_level_present = field in payload
    top_level = payload.get(field)
    nested = _get(payload, f"overall_outcomes.{field}")
    if top_level_present and nested is not None and top_level != nested:
        raise MLShadowScorerGeneralizationAuditPlanError(
            f"{field} disagrees between top-level and overall_outcomes"
        )
    return top_level if top_level_present else nested


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    good = _get(payload, "targets.good_or_acceptable")
    good_blocked = isinstance(good, Mapping) and good.get("production_eligible") is False
    default_unauthorized = payload.get("production_default_authorized") is not True
    return bool(good_blocked and default_unauthorized)


def _formula_components_ok(spec_payload: Mapping[str, Any]) -> bool:
    if _get(spec_payload, "scoring_formula.formula_id") != FORMULA_ID:
        return False
    components = _get(spec_payload, "scoring_formula.components")
    if not isinstance(components, list) or len(components) != 2:
        return False
    expected = {
        "final_score_rank_pct": ("rank_pct(final_score)", 0.5),
        "audit_embedding_probability_rank_pct": ("rank_pct(audit_embedding_probability_work)", 0.5),
    }
    seen: set[str] = set()
    for component in components:
        if not isinstance(component, Mapping):
            return False
        name = str(component.get("name") or "")
        expected_source_weight = expected.get(name)
        if expected_source_weight is None:
            return False
        source, weight = expected_source_weight
        if component.get("source") != source or component.get("weight") != weight:
            return False
        seen.add(name)
    return seen == set(expected)


def _validate_online_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    checks = {
        "online_shadow_execution_policy_defined": payload.get("online_shadow_execution_policy_defined") is True,
        "online_shadow_execution_enabled": payload.get("online_shadow_execution_enabled") is False,
        "runtime_implementation_authorized": payload.get("runtime_implementation_authorized") is False,
        "recommended_next_stage": payload.get("recommended_next_stage") == ONLINE_POLICY_NEXT_STAGE,
        "generalization_required": _get(
            payload, "generalization_requirement_before_runtime.required_before_runtime_implementation"
        )
        is True,
        "missing_generalization_audit_on_second_surface": _get(
            payload, "shadow_and_production_blockers.missing_generalization_audit_on_second_surface"
        )
        is True,
        "shadow_scoring_allowed": payload.get("shadow_scoring_allowed") is False,
        "production_default_allowed": payload.get("production_default_allowed") is False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerGeneralizationAuditPlanError(f"online shadow policy pre-checks failed: {failed}")
    return metadata


def _validate_audit_output_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-audit-output-gates",
        artifact_type=AUDIT_OUTPUT_GATES_ARTIFACT_TYPE,
        version_field="gates_version",
        version=AUDIT_OUTPUT_GATES_VERSION,
    )
    expected = {
        "shadow_audit_output_gates_passed": True,
        "offline_audit_output_ready": True,
        "validation_replay_exact": True,
    }
    for field, expected_value in expected.items():
        if _matching_outcome_value(payload, field) != expected_value:
            raise MLShadowScorerGeneralizationAuditPlanError(
                f"audit output gates {field} must be {expected_value!r}"
            )
    return metadata


def _validate_spec(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="shadow-scorer-spec",
        artifact_type=SPEC_ARTIFACT_TYPE,
        version_field="spec_version",
        version=SPEC_VERSION,
    )
    if not _formula_components_ok(payload):
        raise MLShadowScorerGeneralizationAuditPlanError("shadow scorer spec formula must be hybrid_rank_mean_50_50")
    _require_equal("spec shadow_scoring_allowed", payload.get("shadow_scoring_allowed"), False)
    _require_equal("spec production_default_allowed", payload.get("production_default_allowed"), False)
    return metadata


def _validate_fresh_surface_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, Any], dict[str, Any]]:
    metadata = _validate_identity(
        payload,
        name="fresh-surface-policy",
        artifact_type=FRESH_SURFACE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=FRESH_SURFACE_POLICY_VERSION,
    )
    lift = _get(payload, "gate_linkage.material_lift_thresholds")
    if not isinstance(lift, Mapping):
        raise MLShadowScorerGeneralizationAuditPlanError("fresh policy material_lift_thresholds must be present")
    if lift.get("delta_roc_auc_gte") != 0.03 or lift.get("or_delta_average_precision_gte") != 0.02:
        raise MLShadowScorerGeneralizationAuditPlanError(
            "fresh policy material_lift_thresholds must include 0.03 ROC-AUC or 0.02 AP"
        )
    thresholds = _get(payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLShadowScorerGeneralizationAuditPlanError("fresh policy minimum_confirmatory_label_thresholds must be present")
    required_thresholds = {
        "minimum_candidate_work_count": 100,
        "minimum_confirmatory_labeled_work_count": 100,
        "minimum_confirmatory_positive_work_count": 50,
        "minimum_confirmatory_negative_work_count": 20,
        "minimum_distinct_negative_work_count": 20,
        "minimum_confirmatory_label_coverage_rate": 0.6,
    }
    missing_or_wrong = {
        key: thresholds.get(key)
        for key, expected in required_thresholds.items()
        if thresholds.get(key) != expected
    }
    if missing_or_wrong:
        raise MLShadowScorerGeneralizationAuditPlanError(
            f"fresh policy label thresholds missing or unexpected: {missing_or_wrong}"
        )
    return metadata, dict(lift), dict(thresholds)


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="production-readiness-plan",
        artifact_type=PRODUCTION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PRODUCTION_PLAN_VERSION,
    )
    if not _production_plan_blocked(payload):
        raise MLShadowScorerGeneralizationAuditPlanError("production readiness plan must remain blocked")
    return metadata


def _existing_validated_surface() -> dict[str, Any]:
    return {
        "surface_artifact": "ml-fresh-eval-surface-hybrid-v1",
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": VALIDATED_CANDIDATE_POOL_SHA,
        "primary_confirmatory_arm": FORMULA_ID,
        "can_be_reused_for_generalization_evidence": False,
        "note": "this surface proves first-snapshot validity only; it cannot satisfy second-surface generalization",
    }


def _second_surface_requirements() -> dict[str, Any]:
    return {
        "candidate_pool_work_set_sha256_must_differ_from": VALIDATED_CANDIDATE_POOL_SHA,
        "ranking_run_id_must_differ_from": RANKING_RUN_ID,
        "corpus_snapshot_version_should_differ_from": CORPUS_SNAPSHOT_VERSION,
        "corpus_snapshot_version_same_snapshot_requires_future_metadata_justification": True,
        "family": FAMILY,
        "family_change_requires_explicit_justification": True,
        "must_not_reuse_materialized_surface": "ml-fresh-eval-surface-hybrid-v1",
        "old_217_work_eval_overlap_excluded_from_confirmatory_denominators": True,
        "canonical_openalex_work_level_grouping_required": True,
        "full_final_score_coverage_required": True,
        "full_audit_embedding_probability_work_coverage_required": True,
        "learned_probability_source": "pre-existing or approved upstream scorer application; no generation inside generalization audit",
        "scorer_refit_allowed": False,
        "label_derived_features_allowed": False,
        "weight_tuning_allowed": False,
        "frozen_formula": FORMULA_ID,
    }


def _candidate_source_requirements(label_thresholds: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "preferred_source": "later/newer product-candidate ranking run in family emerging",
        "must_meet_confirmatory_label_thresholds_after_exclusions": True,
        "minimum_confirmatory_eligible_candidate_works_after_exclusions": label_thresholds[
            "minimum_candidate_work_count"
        ],
        "minimum_labeled_confirmatory_works_before_metric_gates_can_pass": label_thresholds[
            "minimum_confirmatory_labeled_work_count"
        ],
        "overlap_counts_to_record": [
            "old_217_overlap",
            "rank_9f4b2a2084_overlap",
            "combined_prior_surface_overlap",
        ],
        "identifiers_to_record": [
            "candidate_pool_work_set_sha256",
            "ranking_run_id",
            "corpus_snapshot_version",
            "embedding_version",
        ],
    }


def _label_requirements(label_thresholds: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "work_level_target": "good_or_acceptable",
        "minimum_confirmatory_labeled_work_count": label_thresholds["minimum_confirmatory_labeled_work_count"],
        "minimum_confirmatory_positive_work_count": label_thresholds["minimum_confirmatory_positive_work_count"],
        "minimum_confirmatory_negative_work_count": label_thresholds["minimum_confirmatory_negative_work_count"],
        "minimum_distinct_negative_work_count": label_thresholds["minimum_distinct_negative_work_count"],
        "minimum_confirmatory_label_coverage_rate": label_thresholds["minimum_confirmatory_label_coverage_rate"],
        "minimum_confirmatory_candidate_work_count": label_thresholds["minimum_candidate_work_count"],
        "label_conflicts_must_be_reported": True,
        "unresolved_conflicts_block_future_gate_pass": True,
        "labels_used_for_metric_evaluation_only": True,
        "labels_used_as_scoring_features": False,
    }


def _scorer_replay_requirements() -> dict[str, Any]:
    return {
        "formula": "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
        "formula_id": FORMULA_ID,
        "rank_pct_scope": "full second-surface candidate pool",
        "higher_is_better": True,
        "ties": "average-rank ties",
        "audit_embedding_probability_work_source": (
            "already exists or approved upstream application of frozen ml-offline-audit-embedding-scorer-v2 "
            "to pre-existing embeddings"
        ),
        "embedding_generation_inside_audit_allowed": False,
        "learned_scorer_refit_allowed": False,
        "learned_probability_creation_inside_audit_allowed": False,
        "weight_tuning_allowed": False,
        "incomplete_learned_probability_coverage_action": "block execution or emit blocked/skipped artifact per policy skip contract",
    }


def _metric_requirements(lift_thresholds: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "primary_arm": FORMULA_ID,
        "baseline": "heuristic final_score",
        "metric_denominator": "confirmatory-eligible labeled works only",
        "metrics": ["ROC-AUC", "average_precision", "precision_at_5", "precision_at_10", "precision_at_20"],
        "material_lift": {
            "delta_roc_auc_gte": lift_thresholds["delta_roc_auc_gte"],
            "or_delta_average_precision_gte": lift_thresholds["or_delta_average_precision_gte"],
        },
        "p_at_k_non_regression": "advisory when saturated",
        "reporting_required": [
            "score distributions",
            "top-k overlap with heuristic",
            "rank displacement",
            "coverage summaries",
        ],
    }


def _overlap_reporting_requirements() -> dict[str, Any]:
    return {
        "report_old_217_eval_set_overlap": True,
        "report_rank_9f4b2a2084_surface_overlap": True,
        "report_combined_prior_surface_overlap": True,
        "report_sha_level_overlap": True,
        "confirmatory_denominators_exclude_previous_eval_overlap": True,
    }


def _generalization_gate_contract() -> dict[str, Any]:
    return {
        "future_artifact": "ml-shadow-scorer-v1-generalization-audit-gates",
        "passes_only_if": [
            "second surface requirements pass",
            "label requirements pass",
            "scorer replay spec-compliant and exact where replay targets exist",
            "no leakage/refit/tuning/embedding generation inside audit",
            "material lift passes vs heuristic on second surface",
            "shadow/prod/runtime remain blocked",
        ],
        "on_pass_may_recommend": "implement_online_shadow_runtime_disabled_by_default",
    }


def _allowed_inputs() -> list[str]:
    return [
        "canonical_openalex_work_id",
        "final_score",
        "audit_embedding_probability_work",
        "ranking_run_id",
        "family",
        "corpus_snapshot_version",
        "embedding_version",
        "display metadata title/year/source for audit only",
        "labels only for metric evaluation",
    ]


def _forbidden_inputs() -> list[str]:
    return [
        "labels as scoring features",
        "good_or_acceptable",
        "label_any_positive",
        "reviewer_notes",
        "row_id",
        "sample_reason",
        "review_pool_variant",
        "holdout assignment",
        "weight tuning from labels",
        "embedding generation inside audit",
        "learned scorer refit",
    ]


def _planned_commit_sequence() -> list[dict[str, Any]]:
    return [
        {
            "order": 1,
            "commit": "feat(eval): draft ml-shadow-scorer-v1 generalization audit v1",
            "this_commit": True,
        },
        {
            "order": 2,
            "commit": "feat(eval): materialize or select second fresh surface for shadow generalization v1",
            "this_commit": False,
        },
        {
            "order": 3,
            "commit": "feat(eval): audit ml-shadow-scorer-v1 on second fresh surface",
            "this_commit": False,
        },
        {
            "order": 4,
            "commit": "feat(eval): add ml-shadow-scorer-v1 generalization audit gates",
            "this_commit": False,
        },
        {
            "order": 5,
            "commit": "feat(eval): implement online shadow runtime disabled by default",
            "only_after_step_4_passes": True,
            "this_commit": False,
        },
    ]


def build_ml_shadow_scorer_generalization_audit_plan_payload(
    *,
    online_shadow_policy_path: Path,
    shadow_scorer_audit_output_gates_path: Path,
    shadow_scorer_spec_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    policy_path = Path(online_shadow_policy_path).resolve()
    audit_output_gates_path = Path(shadow_scorer_audit_output_gates_path).resolve()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    fresh_policy_path = Path(fresh_surface_policy_path).resolve()
    production_plan_path = Path(production_readiness_plan_path).resolve()

    policy_payload = _load_json_object(policy_path)
    audit_output_gates_payload = _load_json_object(audit_output_gates_path)
    spec_payload = _load_json_object(spec_path)
    fresh_policy_payload = _load_json_object(fresh_policy_path)
    production_plan_payload = _load_json_object(production_plan_path)

    policy_metadata = _validate_online_policy(policy_payload)
    audit_output_gates_metadata = _validate_audit_output_gates(audit_output_gates_payload)
    spec_metadata = _validate_spec(spec_payload)
    fresh_policy_metadata, lift_thresholds, label_thresholds = _validate_fresh_surface_policy(fresh_policy_payload)
    production_plan_metadata = _validate_production_plan(production_plan_payload)

    inputs = [
        _input_record("online_shadow_policy", policy_path, repo_root=root),
        _input_record("shadow_scorer_audit_output_gates", audit_output_gates_path, repo_root=root),
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("fresh_surface_policy", fresh_policy_path, repo_root=root),
        _input_record("production_readiness_plan", production_plan_path, repo_root=root),
    ]

    blockers = {
        "missing_generalization_audit_plan_v1": False,
        "missing_generalization_audit_on_second_surface": True,
        "missing_generalization_audit_gates": True,
        "missing_online_shadow_implementation_disabled_by_default": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "runtime_implementation_authorized": False,
    }

    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "plan_version": plan_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_online_shadow_policy_version": policy_metadata.get("policy_version"),
        "source_audit_output_gates_version": audit_output_gates_metadata.get("gates_version"),
        "source_shadow_scorer_spec_version": spec_metadata.get("spec_version"),
        "source_fresh_surface_policy_version": fresh_policy_metadata.get("policy_version"),
        "source_production_readiness_plan_version": production_plan_metadata.get("plan_version"),
        "candidate_pool_work_set_sha256": VALIDATED_CANDIDATE_POOL_SHA,
        "scorer_id": SCORER_ID,
        "formula_id": FORMULA_ID,
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "evidence_summary": {
            "online_shadow_execution_policy_defined": policy_payload.get("online_shadow_execution_policy_defined"),
            "online_shadow_execution_enabled": policy_payload.get("online_shadow_execution_enabled"),
            "runtime_implementation_authorized": policy_payload.get("runtime_implementation_authorized"),
            "audit_output_gates_passed": _matching_outcome_value(
                audit_output_gates_payload, "shadow_audit_output_gates_passed"
            ),
            "offline_audit_output_ready": _matching_outcome_value(
                audit_output_gates_payload, "offline_audit_output_ready"
            ),
            "validation_replay_exact": _matching_outcome_value(audit_output_gates_payload, "validation_replay_exact"),
            "production_plan_blocked": _production_plan_blocked(production_plan_payload),
        },
        "existing_validated_surface": _existing_validated_surface(),
        "second_surface_requirements": _second_surface_requirements(),
        "candidate_source_requirements": _candidate_source_requirements(label_thresholds),
        "label_requirements": _label_requirements(label_thresholds),
        "scorer_replay_requirements": _scorer_replay_requirements(),
        "metric_requirements": _metric_requirements(lift_thresholds),
        "overlap_reporting_requirements": _overlap_reporting_requirements(),
        "generalization_gate_contract": _generalization_gate_contract(),
        "allowed_inputs": _allowed_inputs(),
        "forbidden_inputs": _forbidden_inputs(),
        "planned_commit_sequence": _planned_commit_sequence(),
        "blocked_actions": [
            "online_shadow_execution",
            "runtime_implementation",
            "surface_materialization_in_this_command",
            "generalization_audit_execution",
            "production_default_change",
            "api_web_change",
            "user_visible_ranking_change",
            "scorer_execution",
            "ranking_execution",
            "embedding_generation",
            "learned_scorer_refit",
            "weight_tuning",
            "label_ingest",
        ],
        "shadow_and_production_blockers": blockers,
        "generalization_audit_plan_defined": True,
        "generalization_audit_executed": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": PASSED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_generalization_audit_plan(payload: Mapping[str, Any]) -> str:
    existing = payload["existing_validated_surface"]
    second = payload["second_surface_requirements"]
    labels = payload["label_requirements"]
    metrics = payload["metric_requirements"]
    lines = [
        f"# ML Shadow Scorer v1 Generalization Audit Plan ({payload['metadata']['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This plan defines the second-surface generalization audit required before any online shadow runtime implementation. It is plan-only and does not execute scoring, materialize a surface, or authorize runtime behavior.",
        "",
        f"- Generalization audit plan defined: {payload['generalization_audit_plan_defined']}",
        f"- Generalization audit executed: {payload['generalization_audit_executed']}",
        f"- Runtime implementation authorized: {payload['runtime_implementation_authorized']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Why Generalization Is Required",
        "",
        "The existing evidence is bound to one fresh surface. A second fresh surface must show the frozen formula behaves well outside `rank-9f4b2a2084` before runtime implementation can be considered.",
        "",
        "## Existing Validated Surface",
        "",
        f"- Ranking run: `{existing['ranking_run_id']}`",
        f"- Family: `{existing['family']}`",
        f"- Snapshot: `{existing['corpus_snapshot_version']}`",
        f"- Embedding version: `{existing['embedding_version']}`",
        f"- Candidate SHA: `{existing['candidate_pool_work_set_sha256']}`",
        f"- Can be reused for generalization evidence: {existing['can_be_reused_for_generalization_evidence']}",
        "",
        "## Second-Surface Requirements",
        "",
        f"- Candidate SHA must differ from `{second['candidate_pool_work_set_sha256_must_differ_from']}`",
        f"- Ranking run must differ from `{second['ranking_run_id_must_differ_from']}`",
        f"- Snapshot should differ from `{second['corpus_snapshot_version_should_differ_from']}` unless justified",
        f"- Frozen formula: `{second['frozen_formula']}`",
        f"- Full final_score coverage required: {second['full_final_score_coverage_required']}",
        f"- Full learned-probability coverage required: {second['full_audit_embedding_probability_work_coverage_required']}",
        "",
        "## Label Thresholds",
        "",
    ]
    for key, value in labels.items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Metric Requirements",
            "",
            f"- Primary arm: `{metrics['primary_arm']}`",
            f"- Baseline: `{metrics['baseline']}`",
            f"- Metric denominator: {metrics['metric_denominator']}",
            f"- Material lift: delta ROC-AUC >= {metrics['material_lift']['delta_roc_auc_gte']} OR delta AP >= {metrics['material_lift']['or_delta_average_precision_gte']}",
            "",
            "## Scorer Replay Requirements",
            "",
        ]
    )
    for key, value in payload["scorer_replay_requirements"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Overlap Reporting", ""])
    for key, value in payload["overlap_reporting_requirements"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Future Gate Contract", ""])
    lines.extend(f"- {item}" for item in payload["generalization_gate_contract"]["passes_only_if"])
    lines.extend(["", "## Planned Commit Sequence", ""])
    for item in payload["planned_commit_sequence"]:
        suffix = " (this commit)" if item.get("this_commit") else ""
        lines.append(f"{item['order']}. {item['commit']}{suffix}")
    lines.extend(["", "## Blockers", ""])
    for key, value in payload["shadow_and_production_blockers"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_generalization_audit_plan(
    *,
    online_shadow_policy_path: Path,
    shadow_scorer_audit_output_gates_path: Path,
    shadow_scorer_spec_path: Path,
    fresh_surface_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_generalization_audit_plan_payload(
        online_shadow_policy_path=online_shadow_policy_path,
        shadow_scorer_audit_output_gates_path=shadow_scorer_audit_output_gates_path,
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        fresh_surface_policy_path=fresh_surface_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_generalization_audit_plan(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "PLAN_VERSION",
    "MLShadowScorerGeneralizationAuditPlanError",
    "build_ml_shadow_scorer_generalization_audit_plan_payload",
    "markdown_from_ml_shadow_scorer_generalization_audit_plan",
    "write_ml_shadow_scorer_generalization_audit_plan",
]

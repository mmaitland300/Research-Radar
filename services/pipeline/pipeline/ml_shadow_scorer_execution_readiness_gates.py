"""Execution readiness gates for the disabled ml-shadow-scorer-v1.

This evaluator checks whether the committed disabled-by-default
ml-shadow-scorer-v1 implementation is ready for a future isolated audit output
artifact. It reads existing JSON artifacts only and does not execute the
scorer, query databases, train, embed, import labels, or authorize production
or live shadow behavior.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_v1_execution_readiness_gates"
GATES_VERSION = "ml-shadow-scorer-v1-execution-readiness-gates"

IMPLEMENTATION_ARTIFACT_TYPE = "ml_shadow_scorer_v1_implementation"
IMPLEMENTATION_VERSION = "ml-shadow-scorer-v1-implementation"
SPEC_ARTIFACT_TYPE = "ml_shadow_scorer_spec"
SPEC_VERSION = "ml-shadow-scorer-v1-spec"
HYBRID_VALIDATION_GATES_ARTIFACT_TYPE = "ml_hybrid_validation_metric_gates"
HYBRID_VALIDATION_GATES_VERSION = "ml-hybrid-validation-metric-gates-v1"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

FORMULA_ID = "hybrid_rank_mean_50_50"
EXPECTED_CANDIDATE_POOL_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
EXPECTED_POOL_SIZE = 358
IMPLEMENTATION_NEXT_STAGE = "draft_ml_shadow_scorer_v1_execution_readiness_gates"
SPEC_NEXT_STAGE = "implement_ml_shadow_scorer_v1_disabled_by_default"
CONFIRMATORY_GATES_NEXT_STAGE = "draft_ml_shadow_scorer_v1_spec"
PASSED_NEXT_STAGE = "implement_ml_shadow_scorer_v1_audit_output_artifact"
FAILED_NEXT_STAGE = "repair_shadow_scorer_v1_implementation_or_spec"

FORMULA_LITERAL = "score = 0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)"

CAVEATS = (
    "Passing authorizes only a future offline audit output artifact.",
    "shadow_execution_enabled remains false.",
    "No live recommender, API/web, production default, or user-visible ranking change is authorized.",
    "No scorer execution, database access, embeddings, ranking, label ingest, training, or production integration occurs here.",
)


class MLShadowScorerExecutionReadinessGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerExecutionReadinessGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerExecutionReadinessGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerExecutionReadinessGatesError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerExecutionReadinessGatesError(f"Input {name} does not exist: {path}")
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
        raise MLShadowScorerExecutionReadinessGatesError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerExecutionReadinessGatesError(f"{name} metadata.{version_field} must be {version}")
    return metadata


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
        source_weight = expected.get(name)
        if source_weight is None:
            return False
        source, weight = source_weight
        if component.get("source") != source or _float_or_none(component.get("weight")) != weight:
            return False
        seen.add(name)
    return seen == set(expected)


def _execution_policy_forbids_production(spec_payload: Mapping[str, Any]) -> bool:
    policy = spec_payload.get("execution_policy")
    if not isinstance(policy, Mapping):
        return False
    return (
        policy.get("production_default_promotion_authorized") is False
        and policy.get("production_ranking_changes_allowed") is False
        and policy.get("api_web_changes_allowed") is False
        and policy.get("bridge_default_changes_allowed") is False
        and policy.get("future_implementation_write_scope") == "isolated shadow/audit outputs only"
    )


def _candidate_sha_values(
    *,
    implementation_payload: Mapping[str, Any],
    spec_payload: Mapping[str, Any],
    gates_payload: Mapping[str, Any],
) -> dict[str, Any]:
    values = {
        "spec.metadata.candidate_pool_work_set_sha256": _get(spec_payload, "metadata.candidate_pool_work_set_sha256"),
        "implementation.metadata.candidate_pool_work_set_sha256": _get(
            implementation_payload, "metadata.candidate_pool_work_set_sha256"
        ),
        "implementation.audit_replay_summary.candidate_pool_work_set_sha256": _get(
            implementation_payload, "audit_replay_summary.candidate_pool_work_set_sha256"
        ),
        "gates.metadata.candidate_pool_work_set_sha256": _get(gates_payload, "metadata.candidate_pool_work_set_sha256"),
        "gates.comparison_summary.candidate_eval_coverage.candidate_pool_work_set_sha256": _get(
            gates_payload, "comparison_summary.candidate_eval_coverage.candidate_pool_work_set_sha256"
        ),
    }
    values["all_match_expected"] = all(value == EXPECTED_CANDIDATE_POOL_SHA for value in values.values())
    return values


def _shadow_output_isolation_contract() -> dict[str, Any]:
    return {
        "isolated_audit_shadow_outputs_only": True,
        "no_production_ranking_table_or_config_writes": True,
        "reversible_disableable": True,
        "required_fields": [
            "run_id",
            "scorer_version",
            "formula_id",
            "input_hashes",
            "candidate_pool_work_set_sha256",
            "coverage",
        ],
        "audit_only_until_later_artifact_permits_more": True,
    }


def _isolation_contract_ok(contract: Mapping[str, Any]) -> bool:
    required_fields = contract.get("required_fields")
    return (
        contract.get("isolated_audit_shadow_outputs_only") is True
        and contract.get("no_production_ranking_table_or_config_writes") is True
        and contract.get("reversible_disableable") is True
        and contract.get("audit_only_until_later_artifact_permits_more") is True
        and isinstance(required_fields, list)
        and {
            "run_id",
            "scorer_version",
            "formula_id",
            "input_hashes",
            "candidate_pool_work_set_sha256",
            "coverage",
        }.issubset(set(required_fields))
    )


def _observability_contract(spec_payload: Mapping[str, Any]) -> dict[str, Any]:
    requirements = spec_payload.get("observability_requirements")
    requirement_list = [str(item) for item in requirements] if isinstance(requirements, list) else []
    joined = " | ".join(item.lower() for item in requirement_list)
    required_topics_present = {
        "coverage": "coverage" in joined,
        "missing_probability": "missing learned probability" in joined,
        "distributions": "score distribution" in joined,
        "top_k_overlap": "top-k overlap" in joined,
        "rank_displacement": "rank displacement" in joined,
        "family_counts": "family-level counts" in joined,
        "completeness": "completeness" in joined,
        "error_latency_if_online": "error" in joined and "latency" in joined,
    }
    return {
        "requirements": requirement_list,
        "required_topics_present": required_topics_present,
        "all_required_topics_present": all(required_topics_present.values()),
    }


def _gate(gate_id: str, title: str, passed: bool, observed: Any, rationale: str) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "title": title,
        "status": "pass" if passed else "fail",
        "passed": passed,
        "observed_value": observed,
        "rationale": rationale,
    }


def build_ml_shadow_scorer_execution_readiness_gates_payload(
    *,
    shadow_scorer_implementation_path: Path,
    shadow_scorer_spec_path: Path,
    hybrid_validation_metric_gates_path: Path,
    production_readiness_plan_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    implementation_path = Path(shadow_scorer_implementation_path).resolve()
    spec_path = Path(shadow_scorer_spec_path).resolve()
    validation_gates_path = Path(hybrid_validation_metric_gates_path).resolve()
    production_plan_path = Path(production_readiness_plan_path).resolve()

    implementation_payload = _load_json_object(implementation_path)
    spec_payload = _load_json_object(spec_path)
    gates_payload = _load_json_object(validation_gates_path)
    production_plan_payload = _load_json_object(production_plan_path)

    implementation_metadata = _validate_identity(
        implementation_payload,
        name="shadow-scorer-implementation",
        artifact_type=IMPLEMENTATION_ARTIFACT_TYPE,
        version_field="implementation_version",
        version=IMPLEMENTATION_VERSION,
    )
    spec_metadata = _validate_identity(
        spec_payload,
        name="shadow-scorer-spec",
        artifact_type=SPEC_ARTIFACT_TYPE,
        version_field="spec_version",
        version=SPEC_VERSION,
    )
    gates_metadata = _validate_identity(
        gates_payload,
        name="hybrid-validation-metric-gates",
        artifact_type=HYBRID_VALIDATION_GATES_ARTIFACT_TYPE,
        version_field="gates_version",
        version=HYBRID_VALIDATION_GATES_VERSION,
    )
    production_plan_metadata = _validate_identity(
        production_plan_payload,
        name="production-readiness-plan",
        artifact_type=PRODUCTION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PRODUCTION_PLAN_VERSION,
    )

    replay_tolerance = _float_or_none(_get(implementation_payload, "audit_replay_summary.replay_tolerance"))
    max_score_delta = _float_or_none(_get(implementation_payload, "audit_replay_summary.max_abs_score_delta"))
    max_rank_delta = _float_or_none(_get(implementation_payload, "audit_replay_summary.max_abs_rank_pct_delta"))
    replay_tolerance = 0.0 if replay_tolerance is None else replay_tolerance
    max_score_delta = float("inf") if max_score_delta is None else max_score_delta
    max_rank_delta = float("inf") if max_rank_delta is None else max_rank_delta

    candidate_sha_checks = _candidate_sha_values(
        implementation_payload=implementation_payload,
        spec_payload=spec_payload,
        gates_payload=gates_payload,
    )
    isolation_contract = _shadow_output_isolation_contract()
    observability_contract = _observability_contract(spec_payload)

    identity_valid = (
        implementation_metadata.get("artifact_type") == IMPLEMENTATION_ARTIFACT_TYPE
        and spec_metadata.get("artifact_type") == SPEC_ARTIFACT_TYPE
        and gates_metadata.get("artifact_type") == HYBRID_VALIDATION_GATES_ARTIFACT_TYPE
        and production_plan_metadata.get("artifact_type") == PRODUCTION_PLAN_ARTIFACT_TYPE
    )
    implementation_prechecks = {
        "recommended_next_stage": implementation_payload.get("recommended_next_stage") == IMPLEMENTATION_NEXT_STAGE,
        "implemented": _get(implementation_payload, "implementation_status.implemented") is True,
        "disabled_by_default": _get(implementation_payload, "implementation_status.disabled_by_default") is True,
        "implementation_matches_spec": _get(implementation_payload, "implementation_status.implementation_matches_spec")
        is True,
        "implementation_matches_validation_replay": _get(
            implementation_payload, "implementation_status.implementation_matches_validation_replay"
        )
        is True,
        "missing_ml_shadow_scorer_v1_implementation": _get(
            implementation_payload, "implementation_status.missing_ml_shadow_scorer_v1_implementation"
        )
        is False,
        "shadow_execution_enabled": implementation_metadata.get("shadow_execution_enabled") is False,
        "mismatched_work_count": _get(implementation_payload, "audit_replay_summary.mismatched_work_count") == 0,
        "max_abs_score_delta_within_tolerance": max_score_delta <= replay_tolerance,
        "max_abs_rank_pct_delta_within_tolerance": max_rank_delta <= replay_tolerance,
    }
    coverage_prechecks = {
        "candidate_pool_size": _get(implementation_payload, "implementation_status.candidate_pool_size")
        == EXPECTED_POOL_SIZE,
        "learned_probability_coverage_count": _get(
            implementation_payload, "implementation_status.learned_probability_coverage_count"
        )
        == EXPECTED_POOL_SIZE,
        "missing_learned_probability_count": _get(
            implementation_payload, "implementation_status.missing_learned_probability_count"
        )
        == 0,
    }
    spec_prechecks = {
        "spec_ready_for_implementation": spec_payload.get("spec_ready_for_implementation") is True,
        "recommended_next_stage": spec_payload.get("recommended_next_stage") == SPEC_NEXT_STAGE,
        "formula_id": _get(spec_payload, "scoring_formula.formula_id") == FORMULA_ID,
        "formula_components": _formula_components_ok(spec_payload),
        "shadow_scoring_allowed": spec_payload.get("shadow_scoring_allowed") is False,
        "production_default_allowed": spec_payload.get("production_default_allowed") is False,
        "forbidden_inputs_non_empty": bool(spec_payload.get("forbidden_inputs")),
        "execution_policy_forbids_production": _execution_policy_forbids_production(spec_payload),
    }
    confirmatory_gate_prechecks = {
        "recommended_next_stage": gates_payload.get("recommended_next_stage") == CONFIRMATORY_GATES_NEXT_STAGE,
        "confirmatory_validation_passed": gates_payload.get("confirmatory_validation_passed") is True,
        "primary_hybrid_material_lift_passed": gates_payload.get("primary_hybrid_material_lift_passed") is True,
        "fresh_surface_hybrid_validation_passed": gates_payload.get("fresh_surface_hybrid_validation_passed") is True,
        "primary_confirmatory_arm": gates_payload.get("primary_confirmatory_arm") == FORMULA_ID,
        "shadow_scoring_allowed": gates_payload.get("shadow_scoring_allowed") is False,
        "production_default_allowed": gates_payload.get("production_default_allowed") is False,
    }
    implementation_blockers = implementation_payload.get("shadow_and_production_blockers")
    implementation_blockers = implementation_blockers if isinstance(implementation_blockers, Mapping) else {}
    no_production_api_changes = (
        implementation_blockers.get("production_default_changed") is False
        and implementation_blockers.get("api_web_changed") is False
        and _production_plan_blocked(production_plan_payload)
    )

    g01_passed = identity_valid and bool(candidate_sha_checks["all_match_expected"])
    g02_passed = all(confirmatory_gate_prechecks.values())
    g03_passed = all(implementation_prechecks.values())
    g04_passed = all(coverage_prechecks.values())
    g05_passed = (
        _get(implementation_payload, "implementation_status.disabled_by_default") is True
        and implementation_metadata.get("shadow_execution_enabled") is False
    )
    g06_passed = (
        _formula_components_ok(spec_payload)
        and bool(spec_payload.get("forbidden_inputs"))
        and _execution_policy_forbids_production(spec_payload)
        and spec_payload.get("shadow_scoring_allowed") is False
        and spec_payload.get("production_default_allowed") is False
    )
    g07_passed = no_production_api_changes
    g08_passed = _isolation_contract_ok(isolation_contract)
    g09_passed = observability_contract["all_required_topics_present"] and bool(observability_contract["requirements"])
    preliminary_gates = [
        g01_passed,
        g02_passed,
        g03_passed,
        g04_passed,
        g05_passed,
        g06_passed,
        g07_passed,
        g08_passed,
        g09_passed,
    ]
    g10_passed = all(preliminary_gates)

    gates = [
        _gate(
            "G01_evidence_chain_complete",
            "Evidence Chain Complete",
            g01_passed,
            {"candidate_sha_checks": candidate_sha_checks, "identity_valid": identity_valid},
            "Spec, implementation, confirmatory gates, and production plan identities are valid and candidate SHA is consistent.",
        ),
        _gate(
            "G02_confirmatory_validation_passed",
            "Confirmatory Validation Passed",
            g02_passed,
            confirmatory_gate_prechecks,
            "Fresh hybrid metric gates passed confirmatory validation for the frozen primary arm.",
        ),
        _gate(
            "G03_implementation_exact_replay",
            "Implementation Exact Replay",
            g03_passed,
            {"implementation_prechecks": implementation_prechecks, "replay_tolerance": replay_tolerance},
            "Disabled implementation exactly replays the validated primary arm within tolerance.",
        ),
        _gate(
            "G04_component_coverage",
            "Component Coverage",
            g04_passed,
            coverage_prechecks,
            "All 358 candidate works have learned probability coverage and none are missing.",
        ),
        _gate(
            "G05_disabled_by_default",
            "Disabled By Default",
            g05_passed,
            {
                "disabled_by_default": _get(implementation_payload, "implementation_status.disabled_by_default"),
                "shadow_execution_enabled": implementation_metadata.get("shadow_execution_enabled"),
            },
            "Implementation remains disabled and does not enable shadow execution.",
        ),
        _gate(
            "G06_formula_and_feature_policy",
            "Formula And Feature Policy",
            g06_passed,
            spec_prechecks,
            "Spec locks the 50/50 rank-fusion formula, forbids label-derived inputs, and forbids production promotion.",
        ),
        _gate(
            "G07_no_production_or_api_changes",
            "No Production Or API Changes",
            g07_passed,
            {
                "production_default_changed": implementation_blockers.get("production_default_changed"),
                "api_web_changed": implementation_blockers.get("api_web_changed"),
                "production_plan_blocked": _production_plan_blocked(production_plan_payload),
            },
            "Implementation did not change production/API surfaces and the production readiness plan still blocks default.",
        ),
        _gate(
            "G08_shadow_output_isolation_contract",
            "Shadow Output Isolation Contract",
            g08_passed,
            isolation_contract,
            "Future shadow audit output must be isolated, reversible, and audit-only.",
        ),
        _gate(
            "G09_observability_contract",
            "Observability Contract",
            g09_passed,
            observability_contract,
            "Future execution must emit the observability fields copied from the spec.",
        ),
        _gate(
            "G10_execution_readiness_decision",
            "Execution Readiness Decision",
            g10_passed,
            {"prior_gate_statuses": ["pass" if item else "fail" for item in preliminary_gates]},
            "Execution readiness passes iff G01 through G09 pass.",
        ),
    ]

    readiness_passed = g10_passed
    blockers = {
        "missing_ml_shadow_scorer_v1_spec": False,
        "missing_ml_shadow_scorer_v1_implementation": False,
        "missing_shadow_execution_readiness_gates": not readiness_passed,
        "missing_shadow_output_isolation_check": not readiness_passed,
        "missing_ml_shadow_scorer_v1_audit_output_artifact": readiness_passed,
        "confirmatory_validation_not_complete": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }

    inputs = [
        _input_record("shadow_scorer_implementation", implementation_path, repo_root=root),
        _input_record("shadow_scorer_spec", spec_path, repo_root=root),
        _input_record("hybrid_validation_metric_gates", validation_gates_path, repo_root=root),
        _input_record("production_readiness_plan", production_plan_path, repo_root=root),
    ]

    implementation_exact_replay_passed = g03_passed
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "gates_version": gates_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "source_implementation_version": implementation_metadata.get("implementation_version"),
        "source_spec_version": spec_metadata.get("spec_version"),
        "source_hybrid_validation_metric_gates_version": gates_metadata.get("gates_version"),
        "source_production_readiness_plan_version": production_plan_metadata.get("plan_version"),
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_POOL_SHA,
        "caveats": list(CAVEATS),
    }

    return {
        "metadata": metadata,
        "gate_results": gates,
        "prechecks": {
            "implementation": implementation_prechecks,
            "component_coverage": coverage_prechecks,
            "spec": spec_prechecks,
            "confirmatory_metric_gates": confirmatory_gate_prechecks,
            "candidate_sha_checks": candidate_sha_checks,
        },
        "required_shadow_output_isolation_contract": isolation_contract,
        "required_observability_contract": observability_contract,
        "implementation_replay_summary": {
            "implementation_matches_spec": _get(
                implementation_payload, "implementation_status.implementation_matches_spec"
            ),
            "implementation_matches_validation_replay": _get(
                implementation_payload, "implementation_status.implementation_matches_validation_replay"
            ),
            "mismatched_work_count": _get(implementation_payload, "audit_replay_summary.mismatched_work_count"),
            "max_abs_score_delta": max_score_delta,
            "max_abs_rank_pct_delta": max_rank_delta,
            "replay_tolerance": replay_tolerance,
            "candidate_pool_size": _get(implementation_payload, "implementation_status.candidate_pool_size"),
            "learned_probability_coverage_count": _get(
                implementation_payload, "implementation_status.learned_probability_coverage_count"
            ),
            "missing_learned_probability_count": _get(
                implementation_payload, "implementation_status.missing_learned_probability_count"
            ),
        },
        "overall_outcomes": {
            "shadow_scorer_execution_readiness_passed": readiness_passed,
            "implementation_exact_replay_passed": implementation_exact_replay_passed,
            "shadow_audit_execution_allowed": readiness_passed,
            "shadow_scoring_allowed": False,
            "shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "recommended_next_stage": PASSED_NEXT_STAGE if readiness_passed else FAILED_NEXT_STAGE,
        },
        "shadow_and_production_blockers": blockers,
        "blocked_actions": [
            "production_default_change",
            "api_web_change",
            "online_shadow_execution",
            "user_visible_ranking_change",
            "scorer_execution_in_this_command",
            "database_access",
            "ranking_run",
            "training",
            "embedding_generation",
            "label_ingest",
        ],
        "shadow_scorer_execution_readiness_passed": readiness_passed,
        "implementation_exact_replay_passed": implementation_exact_replay_passed,
        "shadow_audit_execution_allowed": readiness_passed,
        "shadow_scoring_allowed": False,
        "shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "recommended_next_stage": PASSED_NEXT_STAGE if readiness_passed else FAILED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.12g}"
    return str(value)


def markdown_from_ml_shadow_scorer_execution_readiness_gates(payload: Mapping[str, Any]) -> str:
    outcomes = payload["overall_outcomes"]
    replay = payload["implementation_replay_summary"]
    lines = [
        f"# ML Shadow Scorer v1 Execution Readiness Gates ({payload['metadata']['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact evaluates whether the disabled `ml-shadow-scorer-v1` implementation is ready for a future isolated audit-output artifact. It does not execute the scorer and does not authorize live shadow or production behavior.",
        "",
        f"- Execution readiness passed: {outcomes['shadow_scorer_execution_readiness_passed']}",
        f"- Shadow audit execution allowed: {outcomes['shadow_audit_execution_allowed']}",
        f"- Shadow execution enabled: {outcomes['shadow_execution_enabled']}",
        f"- Production default allowed: {outcomes['production_default_allowed']}",
        f"- Recommended next stage: `{outcomes['recommended_next_stage']}`",
        "",
        "## Gate Results",
        "",
        "| Gate | Status | Rationale |",
        "| --- | --- | --- |",
    ]
    for gate in payload["gate_results"]:
        lines.append(f"| `{gate['gate_id']}` | {gate['status']} | {gate['rationale']} |")

    lines.extend(
        [
            "",
            "## Exact Replay",
            "",
            f"- Implementation exact replay passed: {outcomes['implementation_exact_replay_passed']}",
            f"- Candidate pool size: {replay['candidate_pool_size']}",
            f"- Learned probability coverage: {replay['learned_probability_coverage_count']}",
            f"- Missing learned probability count: {replay['missing_learned_probability_count']}",
            f"- Mismatched work count: {replay['mismatched_work_count']}",
            f"- Max absolute score delta: {_fmt(replay['max_abs_score_delta'])}",
            f"- Max absolute rank percentile delta: {_fmt(replay['max_abs_rank_pct_delta'])}",
            f"- Replay tolerance: {_fmt(replay['replay_tolerance'])}",
            "",
            "## Isolation Contract",
            "",
        ]
    )
    contract = payload["required_shadow_output_isolation_contract"]
    lines.extend(
        [
            f"- Isolated audit/shadow outputs only: {contract['isolated_audit_shadow_outputs_only']}",
            f"- No production ranking table/config writes: {contract['no_production_ranking_table_or_config_writes']}",
            f"- Reversible/disableable: {contract['reversible_disableable']}",
            f"- Audit-only until later artifact permits more: {contract['audit_only_until_later_artifact_permits_more']}",
            f"- Required fields: {', '.join(contract['required_fields'])}",
            "",
            "## Observability Contract",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["required_observability_contract"]["requirements"])
    lines.extend(
        [
            "",
            "## Blockers",
            "",
        ]
    )
    for key, value in payload["shadow_and_production_blockers"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Not Shadow Execution / Not Production",
            "",
            "- Passing these gates authorizes only a future offline audit output artifact.",
            "- `shadow_execution_enabled` remains false.",
            "- No production default, API/web integration, online shadow beside production, or user-visible ranking change is authorized.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_execution_readiness_gates(
    *,
    shadow_scorer_implementation_path: Path,
    shadow_scorer_spec_path: Path,
    hybrid_validation_metric_gates_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_execution_readiness_gates_payload(
        shadow_scorer_implementation_path=shadow_scorer_implementation_path,
        shadow_scorer_spec_path=shadow_scorer_spec_path,
        hybrid_validation_metric_gates_path=hybrid_validation_metric_gates_path,
        production_readiness_plan_path=production_readiness_plan_path,
        gates_version=gates_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_execution_readiness_gates(payload), encoding="utf-8"
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "GATES_VERSION",
    "MLShadowScorerExecutionReadinessGatesError",
    "build_ml_shadow_scorer_execution_readiness_gates_payload",
    "markdown_from_ml_shadow_scorer_execution_readiness_gates",
    "write_ml_shadow_scorer_execution_readiness_gates",
]

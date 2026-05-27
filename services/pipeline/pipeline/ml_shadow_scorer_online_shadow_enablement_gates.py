"""Definition artifact for future ml-shadow-scorer-v1 online shadow enablement gates.

This module reads committed runtime, runtime-isolation, policy, generalization,
and production-readiness artifacts to define the future enablement gate
contract. It does not execute online shadow gates, run runtime code, query data
stores, write shadow outputs, or authorize production behavior.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    RANKING_RUN_ID,
    RUNTIME_VERSION,
)
from pipeline.ml_shadow_scorer_runtime_isolation_verification import VERIFICATION_VERSION
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_enablement_gates"
GATES_VERSION = "ml-shadow-scorer-v1-online-shadow-enablement-gates-v1"

VERIFICATION_ARTIFACT_TYPE = "ml_shadow_scorer_runtime_isolation_verification"
RUNTIME_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_runtime_disabled"
GENERALIZATION_GATES_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_gates"
GENERALIZATION_GATES_VERSION = "ml-shadow-scorer-v1-generalization-audit-gates-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"
PRODUCTION_PLAN_ARTIFACT_TYPE = "ml_production_readiness_plan"
PRODUCTION_PLAN_VERSION = "ml-production-readiness-plan-v1"

RECOMMENDED_NEXT_STAGE = "run_ml_shadow_scorer_v1_online_shadow_enablement_gates_v1"

CAVEATS = (
    "This defines gates only; it does not execute online shadow enablement gates.",
    "This does not run online shadow execution.",
    "This does not authorize production default, API/web behavior, or user-visible ranking changes.",
    "Production readiness remains separate and research_only.",
    "Policy JSON may have stale historical blocker fields; current blocker truth is from gates + runtime isolation verification.",
    "A future gates execution may still fail or route to more hardening.",
    "Any future online shadow execution must remain disabled by default until explicitly authorized.",
)

BLOCKED_ACTIONS = (
    "online_shadow_execution",
    "runtime_execution",
    "production_default_change",
    "api_web_change",
    "user_visible_ranking_change",
    "database_writes",
    "shadow_output_table_creation",
    "ranking_run_creation",
    "embedding_generation",
    "learned_probability_generation",
    "scorer_refit",
    "label_ingest",
)

PREREQUISITE_GATE_PREFIXES = tuple(f"E{index:02d}_" for index in range(1, 10))

EXPECTED_OBSERVABILITY_CONTRACT_KEYS = (
    "component_coverage",
    "missing_learned_probability",
    "score_distributions",
    "top_k_overlap_with_heuristic",
    "rank_displacement",
    "family_counts",
    "output_completeness",
    "runtime_errors",
    "latency",
    "skipped_candidates_and_reasons",
    "skipped_ranking_run_records",
    "write_counts_by_isolated_target",
)


class MLShadowScorerOnlineShadowEnablementGatesError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowEnablementGatesError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowEnablementGatesError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowEnablementGatesError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerOnlineShadowEnablementGatesError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowEnablementGatesError(f"{name} must be {expected!r}, got {observed!r}")


def _non_empty_sequence(value: Any) -> bool:
    return isinstance(value, (list, tuple)) and len(value) > 0


def _non_empty_mapping(value: Any) -> bool:
    return isinstance(value, Mapping) and len(value) > 0


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    _require_equal(f"{name} metadata.artifact_type", metadata.get("artifact_type"), artifact_type)
    _require_equal(f"{name} metadata.{version_field}", metadata.get(version_field), version)
    return metadata


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
    }


def _validate_runtime_isolation(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="runtime-isolation-verification",
        artifact_type=VERIFICATION_ARTIFACT_TYPE,
        version_field="verification_version",
        version=VERIFICATION_VERSION,
    )
    required = {
        "runtime_isolation_verification_passed": True,
        "recommended_next_stage": "draft_online_shadow_execution_enablement_gates_v1",
        "runtime_execution_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
    }
    for path, expected in required.items():
        _require_equal(f"runtime isolation {path}", _get(payload, path), expected)
    for field, expected in _identity_fields().items():
        _require_equal(f"runtime isolation metadata.{field}", metadata.get(field), expected)
    return metadata


def _validate_runtime(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-runtime",
        artifact_type=RUNTIME_ARTIFACT_TYPE,
        version_field="runtime_version",
        version=RUNTIME_VERSION,
    )
    required = {
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": True,
        "runtime_default_state": "off",
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "last_disabled_run.status": "skipped_runtime_disabled",
        "last_disabled_run.shadow_row_count": 0,
        "last_disabled_run.writes_performed": False,
    }
    for path, expected in required.items():
        _require_equal(f"runtime {path}", _get(payload, path), expected)
    for field, expected in _identity_fields().items():
        _require_equal(f"runtime metadata.{field}", metadata.get(field), expected)
    return metadata


def _validate_generalization_gates(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-gates",
        artifact_type=GENERALIZATION_GATES_ARTIFACT_TYPE,
        version_field="gates_version",
        version=GENERALIZATION_GATES_VERSION,
    )
    required = {
        "generalization_audit_gates_passed": True,
        "second_surface_generalization_passed": True,
        "material_lift_gate_passed": True,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
    }
    for path, expected in required.items():
        _require_equal(f"generalization gates {path}", _get(payload, path), expected)
    for field, expected in _identity_fields().items():
        _require_equal(f"generalization gates metadata.{field}", metadata.get(field), expected)
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    required = {
        "online_shadow_execution_policy_defined": True,
        "online_shadow_execution_enabled": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "runtime_isolation_policy.feature_flag": FEATURE_FLAG,
        "runtime_isolation_policy.feature_flag_default_off": True,
        "disable_and_rollback_policy.disable_switch_default": "off",
        "separation_from_production_default_chain.future_online_shadow_gates_do_not_set_production_default_allowed": True,
    }
    for path, expected in required.items():
        _require_equal(f"online shadow policy {path}", _get(payload, path), expected)
    return metadata


def _production_plan_observed_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    good = _get(payload, "targets.good_or_acceptable")
    return {
        "overall_status": payload.get("overall_status") or _get(payload, "metadata.overall_status"),
        "production_default_authorized": payload.get("production_default_authorized"),
        "good_or_acceptable_production_eligible": good.get("production_eligible") if isinstance(good, Mapping) else None,
    }


def _production_plan_blocked(payload: Mapping[str, Any]) -> bool:
    observed = _production_plan_observed_fields(payload)
    return (
        observed["overall_status"] == "research_only"
        and observed["production_default_authorized"] is not True
        and observed["good_or_acceptable_production_eligible"] is False
    )


def _validate_production_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="production-readiness-plan",
        artifact_type=PRODUCTION_PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PRODUCTION_PLAN_VERSION,
    )
    if not _production_plan_blocked(payload):
        raise MLShadowScorerOnlineShadowEnablementGatesError(
            "production readiness plan must remain research_only and blocked"
        )
    return metadata


def _assert_identity_match(
    *,
    runtime_isolation_metadata: Mapping[str, Any],
    runtime_metadata: Mapping[str, Any],
    gates_metadata: Mapping[str, Any],
) -> None:
    for field, expected in _identity_fields().items():
        _require_equal(f"runtime isolation approved identity {field}", runtime_isolation_metadata.get(field), expected)
        _require_equal(f"runtime/gates identity {field}", runtime_metadata.get(field), gates_metadata.get(field))
        _require_equal(
            f"runtime isolation/gates identity {field}",
            runtime_isolation_metadata.get(field),
            gates_metadata.get(field),
        )


def _gate(
    gate_id: str,
    title: str,
    *,
    prerequisite_evidence_present: bool,
    expected_evidence: list[str],
    observed_evidence: Mapping[str, Any],
    rationale: str,
) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "title": title,
        "definition_ready": True,
        "prerequisite_evidence_present": prerequisite_evidence_present,
        "enablement_gate_executed": False,
        "decision": "definition_only_prerequisite_evidence_present" if prerequisite_evidence_present else "definition_only_missing_prerequisite_evidence",
        "expected_evidence": expected_evidence,
        "observed_evidence": dict(observed_evidence),
        "rationale": rationale,
    }


def _verification_gate_result(payload: Mapping[str, Any], gate_id: str) -> Mapping[str, Any] | None:
    results = payload.get("verification_results")
    if not isinstance(results, list):
        return None
    for result in results:
        if isinstance(result, Mapping) and result.get("gate_id") == gate_id:
            return result
    return None


def _observability_contract_ready(policy_payload: Mapping[str, Any]) -> bool:
    contract = policy_payload.get("observability_contract")
    if not isinstance(contract, Mapping):
        return False
    return all(contract.get(key) is True for key in EXPECTED_OBSERVABILITY_CONTRACT_KEYS)


def _write_scope_ready(policy_payload: Mapping[str, Any]) -> bool:
    allowed = policy_payload.get("allowed_write_scope")
    forbidden = policy_payload.get("forbidden_write_scope")
    return (
        isinstance(allowed, Mapping)
        and allowed.get("future_only_after_later_gates") is True
        and _non_empty_sequence(allowed.get("targets"))
        and _non_empty_sequence(allowed.get("required_fields"))
        and _non_empty_sequence(forbidden)
    )


def _future_runtime_verification_requirements_ready(policy_payload: Mapping[str, Any]) -> bool:
    requirements = policy_payload.get("future_runtime_verification_requirements")
    return isinstance(requirements, Mapping) and _non_empty_sequence(requirements.get("must_prove"))


def _all_prerequisite_gates_satisfied(contract: list[Mapping[str, Any]]) -> bool:
    prerequisite_gates = [
        gate
        for gate in contract
        if isinstance(gate.get("gate_id"), str) and gate["gate_id"].startswith(PREREQUISITE_GATE_PREFIXES)
    ]
    return len(prerequisite_gates) == len(PREREQUISITE_GATE_PREFIXES) and all(
        gate.get("prerequisite_evidence_present") is True for gate in prerequisite_gates
    )


def _enablement_gate_contract(
    *,
    verification_payload: Mapping[str, Any],
    runtime_payload: Mapping[str, Any],
    gates_payload: Mapping[str, Any],
    policy_payload: Mapping[str, Any],
    production_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    production_observed = _production_plan_observed_fields(production_payload)
    e04_observed = {
        "runtime_feature_flag": runtime_payload["runtime_feature_flag"],
        "policy_feature_flag": _get(policy_payload, "runtime_isolation_policy.feature_flag"),
        "policy_feature_flag_default_off": _get(policy_payload, "runtime_isolation_policy.feature_flag_default_off"),
        "disable_switch_default": _get(policy_payload, "disable_and_rollback_policy.disable_switch_default"),
    }
    e05_observed = {
        "runtime_online_shadow_execution_enabled": runtime_payload["online_shadow_execution_enabled"],
        "verification_online_shadow_execution_enabled": verification_payload["online_shadow_execution_enabled"],
        "gates_online_shadow_execution_enabled": gates_payload["online_shadow_execution_enabled"],
        "runtime_production_default_allowed": runtime_payload["production_default_allowed"],
        "verification_production_default_allowed": verification_payload["production_default_allowed"],
        "gates_production_default_allowed": gates_payload["production_default_allowed"],
        "policy_production_default_allowed": policy_payload["production_default_allowed"],
        "runtime_api_web_changes_allowed": runtime_payload["api_web_changes_allowed"],
        "verification_api_web_changes_allowed": verification_payload["api_web_changes_allowed"],
        "gates_api_web_changes_allowed": gates_payload["api_web_changes_allowed"],
        "policy_api_web_changes_allowed": policy_payload["api_web_changes_allowed"],
    }
    e06_observed = {
        "allowed_write_scope_present": _non_empty_mapping(policy_payload.get("allowed_write_scope")),
        "allowed_write_scope_future_only_after_later_gates": _get(
            policy_payload,
            "allowed_write_scope.future_only_after_later_gates",
        ),
        "allowed_write_scope_targets_present": _non_empty_sequence(_get(policy_payload, "allowed_write_scope.targets")),
        "allowed_write_scope_required_fields_present": _non_empty_sequence(
            _get(policy_payload, "allowed_write_scope.required_fields")
        ),
        "forbidden_write_scope_present": _non_empty_sequence(policy_payload.get("forbidden_write_scope")),
        "runtime_contract_writes_performed": _get(runtime_payload, "runtime_contract.writes_performed"),
        "last_disabled_run_writes_performed": _get(runtime_payload, "last_disabled_run.writes_performed"),
    }
    e07_observed = {
        "observability_contract_present": _non_empty_mapping(policy_payload.get("observability_contract")),
        "observability_expected_keys_true": _observability_contract_ready(policy_payload),
        "future_runtime_verification_requirements_present": _non_empty_mapping(
            policy_payload.get("future_runtime_verification_requirements")
        ),
        "future_runtime_verification_requirements_must_prove_present": _future_runtime_verification_requirements_ready(
            policy_payload
        ),
    }
    v04_result = _verification_gate_result(verification_payload, "V04_skip_on_incomplete_coverage")
    e08_observed = {
        "runtime_contract_skip_on_incomplete_coverage": _get(runtime_payload, "runtime_contract.skip_on_incomplete_coverage"),
        "runtime_contract_partial_scoring_allowed": _get(runtime_payload, "runtime_contract.partial_scoring_allowed"),
        "verification_incomplete_cases": _get(verification_payload, "verification_summary.incomplete_coverage_cases_verified"),
        "verification_gate_v04_status": str(v04_result.get("status")) if isinstance(v04_result, Mapping) else None,
        "verification_gate_v04_passed": v04_result.get("passed") if isinstance(v04_result, Mapping) else None,
    }
    e09_observed = {
        "future_online_shadow_gates_do_not_set_production_default_allowed": _get(
            policy_payload,
            "separation_from_production_default_chain.future_online_shadow_gates_do_not_set_production_default_allowed",
        ),
        "policy_separation_production_default_allowed": _get(
            policy_payload,
            "separation_from_production_default_chain.production_default_allowed",
        ),
        "production_plan_blocked": _production_plan_blocked(production_payload),
        **production_observed,
    }
    return [
        _gate(
            "E01_generalization_gates_passed",
            "Generalization gates passed",
            prerequisite_evidence_present=True,
            expected_evidence=["generalization_audit_gates_passed true", "second_surface_generalization_passed true"],
            observed_evidence={
                "generalization_audit_gates_passed": gates_payload["generalization_audit_gates_passed"],
                "second_surface_generalization_passed": gates_payload["second_surface_generalization_passed"],
                "material_lift_gate_passed": gates_payload["material_lift_gate_passed"],
            },
            rationale="Second-surface offline generalization evidence is present before enablement can be considered.",
        ),
        _gate(
            "E02_runtime_disabled_by_default_implemented",
            "Runtime disabled by default implemented",
            prerequisite_evidence_present=True,
            expected_evidence=["runtime implementation present", "runtime default state off", "last disabled run skipped"],
            observed_evidence={
                "runtime_implementation_present": runtime_payload["runtime_implementation_present"],
                "runtime_disabled_by_default": runtime_payload["runtime_disabled_by_default"],
                "runtime_default_state": runtime_payload["runtime_default_state"],
                "last_disabled_run": runtime_payload["last_disabled_run"],
            },
            rationale="Runtime code exists but defaults off and does not run unless separately authorized later.",
        ),
        _gate(
            "E03_runtime_isolation_verification_passed",
            "Runtime isolation verification passed",
            prerequisite_evidence_present=True,
            expected_evidence=["runtime isolation verification passed"],
            observed_evidence={
                "runtime_isolation_verification_passed": verification_payload["runtime_isolation_verification_passed"],
                "recommended_next_stage": verification_payload["recommended_next_stage"],
            },
            rationale="Pure runtime probes passed without DB/API/production side effects.",
        ),
        _gate(
            "E04_feature_flag_default_off_and_disable_path_defined",
            "Feature flag default off and disable path defined",
            prerequisite_evidence_present=(
                e04_observed["runtime_feature_flag"] == FEATURE_FLAG
                and e04_observed["policy_feature_flag"] == FEATURE_FLAG
                and e04_observed["policy_feature_flag_default_off"] is True
                and e04_observed["disable_switch_default"] == "off"
            ),
            expected_evidence=["feature flag ML_SHADOW_SCORER_V1_RUNTIME_ENABLED", "default off", "disable switch off"],
            observed_evidence=e04_observed,
            rationale="A future shadow path must stay controlled by the documented default-off flag and disable switch.",
        ),
        _gate(
            "E05_no_production_default_or_api_web_change",
            "No production default or API/web change",
            prerequisite_evidence_present=all(value is False for value in e05_observed.values()),
            expected_evidence=["all current artifacts keep online shadow/prod/API flags false"],
            observed_evidence=e05_observed,
            rationale="The current chain documents no user-visible, API/web, or production default mutation.",
        ),
        _gate(
            "E06_shadow_write_isolation_requirement_documented_not_enabled",
            "Shadow write isolation requirement documented, not enabled",
            prerequisite_evidence_present=(
                _write_scope_ready(policy_payload)
                and e06_observed["runtime_contract_writes_performed"] is False
                and e06_observed["last_disabled_run_writes_performed"] is False
            ),
            expected_evidence=["policy documents isolated shadow/audit write scope", "runtime performs no writes"],
            observed_evidence=e06_observed,
            rationale="Future shadow writes require isolated audit scope, while this definition enables none.",
        ),
        _gate(
            "E07_observability_requirements_defined_for_future_online_run",
            "Observability requirements defined for future online run",
            prerequisite_evidence_present=(
                e07_observed["observability_expected_keys_true"] is True
                and e07_observed["future_runtime_verification_requirements_must_prove_present"] is True
            ),
            expected_evidence=["policy observability contract present"],
            observed_evidence=e07_observed,
            rationale="Future online shadow evaluation must emit observability before any execution gate can pass.",
        ),
        _gate(
            "E08_skip_on_incomplete_coverage_verified",
            "Skip on incomplete coverage verified",
            prerequisite_evidence_present=(
                e08_observed["runtime_contract_skip_on_incomplete_coverage"] is True
                and e08_observed["runtime_contract_partial_scoring_allowed"] is False
                and isinstance(e08_observed["verification_incomplete_cases"], int)
                and e08_observed["verification_incomplete_cases"] > 0
                and e08_observed["verification_gate_v04_status"] == "pass"
                and e08_observed["verification_gate_v04_passed"] is True
            ),
            expected_evidence=["runtime isolation probes skipped incomplete coverage"],
            observed_evidence=e08_observed,
            rationale="Incomplete learned probability or final score coverage must never produce partial shadow scoring.",
        ),
        _gate(
            "E09_production_default_chain_remains_separate",
            "Production default chain remains separate",
            prerequisite_evidence_present=(
                e09_observed["future_online_shadow_gates_do_not_set_production_default_allowed"] is True
                and e09_observed["policy_separation_production_default_allowed"] is False
                and e09_observed["production_plan_blocked"] is True
            ),
            expected_evidence=["policy separation true", "production readiness research_only"],
            observed_evidence=e09_observed,
            rationale="Even future online shadow gates cannot authorize production default promotion.",
        ),
        {
            "gate_id": "E10_online_shadow_enablement_decision_not_executed",
            "title": "Online shadow enablement decision not executed",
            "definition_ready": True,
            "prerequisite_evidence_present": True,
            "enablement_gate_executed": False,
            "decision": "online_shadow_enablement_decision_not_executed",
            "expected_evidence": ["this definition artifact must not execute enablement gates"],
            "observed_evidence": {
                "online_shadow_enablement_gates_defined": True,
                "online_shadow_enablement_gates_executed": False,
                "online_shadow_execution_enabled": False,
                "runtime_execution_authorized": False,
            },
            "rationale": "This artifact defines the future contract only; a separate run command must execute gates later.",
        },
    ]


def build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
    *,
    runtime_isolation_verification_path: Path,
    online_shadow_runtime_path: Path,
    generalization_audit_gates_path: Path,
    online_shadow_policy_path: Path,
    production_readiness_plan_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    verification_path = Path(runtime_isolation_verification_path).resolve()
    runtime_path = Path(online_shadow_runtime_path).resolve()
    gates_path = Path(generalization_audit_gates_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    production_path = Path(production_readiness_plan_path).resolve()

    verification_payload = _load_json_object(verification_path)
    runtime_payload = _load_json_object(runtime_path)
    gates_payload = _load_json_object(gates_path)
    policy_payload = _load_json_object(policy_path)
    production_payload = _load_json_object(production_path)

    verification_metadata = _validate_runtime_isolation(verification_payload)
    runtime_metadata = _validate_runtime(runtime_payload)
    gates_metadata = _validate_generalization_gates(gates_payload)
    policy_metadata = _validate_policy(policy_payload)
    production_metadata = _validate_production_plan(production_payload)
    _assert_identity_match(
        runtime_isolation_metadata=verification_metadata,
        runtime_metadata=runtime_metadata,
        gates_metadata=gates_metadata,
    )

    inputs = [
        _input_record("runtime_isolation_verification", verification_path, repo_root=root),
        _input_record("online_shadow_runtime", runtime_path, repo_root=root),
        _input_record("generalization_audit_gates", gates_path, repo_root=root),
        _input_record("online_shadow_policy", policy_path, repo_root=root),
        _input_record("production_readiness_plan", production_path, repo_root=root),
    ]
    contract = _enablement_gate_contract(
        verification_payload=verification_payload,
        runtime_payload=runtime_payload,
        gates_payload=gates_payload,
        policy_payload=policy_payload,
        production_payload=production_payload,
    )
    all_prerequisite_gates_satisfied = _all_prerequisite_gates_satisfied(contract)
    blockers = {
        "missing_generalization_audit_on_second_surface": False,
        "missing_generalization_audit_gates": False,
        "missing_online_shadow_implementation_disabled_by_default": False,
        "missing_shadow_runtime_isolation_verification": False,
        "missing_online_shadow_enablement_gates": False,
        "missing_online_shadow_execution_authorization": True,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "runtime_implementation_authorized": False,
        "runtime_execution_authorized": False,
    }
    identity = _identity_fields()
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "gates_version": gates_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "source_runtime_isolation_verification_version": verification_metadata.get("verification_version"),
            "source_runtime_version": runtime_metadata.get("runtime_version"),
            "source_generalization_audit_gates_version": gates_metadata.get("gates_version"),
            "source_online_shadow_policy_version": policy_metadata.get("policy_version"),
            "source_production_readiness_plan_version": production_metadata.get("plan_version"),
            "runtime_feature_flag": FEATURE_FLAG,
            **identity,
        },
        "online_shadow_enablement_gates_defined": True,
        "online_shadow_enablement_gates_executed": False,
        "all_prerequisite_gates_satisfied": all_prerequisite_gates_satisfied,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "evidence_summary": {
            "runtime_isolation_verification_passed": verification_payload["runtime_isolation_verification_passed"],
            "runtime_implementation_present": runtime_payload["runtime_implementation_present"],
            "runtime_disabled_by_default": runtime_payload["runtime_disabled_by_default"],
            "runtime_feature_flag": runtime_payload["runtime_feature_flag"],
            "generalization_audit_gates_passed": gates_payload["generalization_audit_gates_passed"],
            "second_surface_generalization_passed": gates_payload["second_surface_generalization_passed"],
            "material_lift_gate_passed": gates_payload["material_lift_gate_passed"],
            "online_shadow_policy_defined": policy_payload["online_shadow_execution_policy_defined"],
            "production_plan_blocked": _production_plan_blocked(production_payload),
            "production_plan_observed": _production_plan_observed_fields(production_payload),
            "policy_scope_note": {
                "policy_contract_scope": "surface-1 validation snapshot (historical evidence anchor)",
                "enablement_identity_scope": "surface-2 second-shadow-generalization run",
                "policy_used_as": "default-off / write-scope / observability contract only",
                "policy_validation_snapshot_scope": policy_payload.get("validation_snapshot_scope"),
            },
            "current_blocker_truth_source": "generalization audit gates + runtime isolation verification",
        },
        "enablement_gate_contract": contract,
        "blocked_actions": list(BLOCKED_ACTIONS),
        "future_online_shadow_execution_requirements": [
            "Explicit execution gates command must run after this definition.",
            "Feature flag must remain default off.",
            "Any future online shadow run must be isolated and audit-only.",
            "Shadow output writes, if ever allowed, must be to an isolated namespace/table/artifact only.",
            "Zero writes to ranking_runs, production/default pins, paper_scores used for production/default, API-visible result tables, labels, embeddings, scorer artifacts, or production config.",
            "Observability must record component coverage, missing learned probability count, score distributions, top-k overlap, rank displacement, family counts, output completeness, skipped candidates/reasons, runtime errors, latency, write counts by isolated target.",
            "Disable path must be tested before and after any future enablement.",
            "Production ranking must remain unchanged whether shadow is on or off.",
            "Passing future online shadow gates does not set production_default_allowed true.",
        ],
        "required_future_runbook": {
            "future_command": "ml-shadow-scorer-online-shadow-enablement-gates-run",
            "required_before_any_execution": True,
            "may_add_future_live_checks": ["DB/write-mode isolation", "observability sink verification", "disable rollback drill"],
            "must_keep_runtime_default_off": True,
        },
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_enablement_gates(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    evidence = payload["evidence_summary"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Enablement Gates ({metadata['gates_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact defines the future online shadow enablement gate contract for ml-shadow-scorer-v1. It does not execute the gates, enable online shadow, authorize runtime execution, or change production behavior.",
        "",
        f"- Enablement gates defined: {payload['online_shadow_enablement_gates_defined']}",
        f"- Enablement gates executed: {payload['online_shadow_enablement_gates_executed']}",
        f"- All prerequisite gates satisfied: {payload['all_prerequisite_gates_satisfied']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Runtime execution authorized: {payload['runtime_execution_authorized']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Evidence Chain",
        "",
        f"- Runtime isolation verification passed: {evidence['runtime_isolation_verification_passed']}",
        f"- Runtime implementation present: {evidence['runtime_implementation_present']}",
        f"- Runtime disabled by default: {evidence['runtime_disabled_by_default']}",
        f"- Runtime feature flag: `{evidence['runtime_feature_flag']}`",
        f"- Generalization audit gates passed: {evidence['generalization_audit_gates_passed']}",
        f"- Production plan blocked: {evidence['production_plan_blocked']}",
        f"- Policy contract scope: {evidence['policy_scope_note']['policy_contract_scope']}",
        f"- Enablement identity scope: {evidence['policy_scope_note']['enablement_identity_scope']}",
        f"- Policy used as: {evidence['policy_scope_note']['policy_used_as']}",
        "",
        "## Enablement Gate Contract",
        "",
    ]
    lines.extend(
        f"- `{gate['gate_id']}`: definition_ready={gate['definition_ready']}, prerequisite_evidence_present={gate['prerequisite_evidence_present']}, executed={gate['enablement_gate_executed']}, decision=`{gate['decision']}`"
        for gate in payload["enablement_gate_contract"]
    )
    lines.extend(["", "## Future Requirements", ""])
    lines.extend(f"- {item}" for item in payload["future_online_shadow_execution_requirements"])
    lines.extend(["", "## Remaining Blockers", ""])
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_enablement_gates(
    *,
    runtime_isolation_verification_path: Path,
    online_shadow_runtime_path: Path,
    generalization_audit_gates_path: Path,
    online_shadow_policy_path: Path,
    production_readiness_plan_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    gates_version: str = GATES_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_enablement_gates_payload(
        runtime_isolation_verification_path=runtime_isolation_verification_path,
        online_shadow_runtime_path=online_shadow_runtime_path,
        generalization_audit_gates_path=generalization_audit_gates_path,
        online_shadow_policy_path=online_shadow_policy_path,
        production_readiness_plan_path=production_readiness_plan_path,
        gates_version=gates_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_enablement_gates(payload),
        encoding="utf-8",
    )
    return payload

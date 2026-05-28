"""Phase 1 no-write pilot plan for ml-shadow-scorer-v1 online shadow execution.

This module validates the bounded pilot grant, disabled runtime artifact, and
online shadow policy, then writes a planning artifact. It does not run the
runtime, enable feature flags, access databases, write outputs, or alter
production behavior.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_grant import (
    ARTIFACT_TYPE as GRANT_ARTIFACT_TYPE,
    GRANT_VERSION,
    OWNER,
    REVIEW_BY,
)
from pipeline.ml_shadow_scorer_online_shadow_policy import (
    ARTIFACT_TYPE as POLICY_ARTIFACT_TYPE,
    POLICY_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    ARTIFACT_TYPE as RUNTIME_ARTIFACT_TYPE,
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FORMULA_ID,
    RANKING_RUN_ID,
    RUNTIME_VERSION,
    SCORER_ID,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1"
RECOMMENDED_NEXT_STAGE = "implement_online_shadow_phase1_no_write_pilot_runner_v1"

AUTHORIZATION_SCOPE = "bounded_non_prod_pilot_only"
NON_PROD_ENVIRONMENTS = "non-prod pilot only"
PHASE_1_WRITE_MODE = "no_writes"

POLICY_CONTRACT_OBSERVABILITY_KEYS = (
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

RUN_LEVEL_OBSERVABILITY_FIELDS = (
    "status",
    "shadow_row_count",
    "writes_performed",
    "production_default_changed",
    "user_visible_ranking_changed",
    "api_web_changes_allowed",
    "runtime_feature_flag_value",
)

CAVEATS = (
    "Plan only; this artifact does not run the pilot.",
    "Grant is scoped to a non-production pilot only.",
    "Online shadow execution remains disabled globally until a later pilot runner operates in the approved pilot environment with the flag on.",
    "No production default or production readiness authorization is granted.",
    "Phase 1 allows no writes; any Phase 2 isolated audit writes require a separate proof and authorization.",
)


class MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
    }


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError("recorded input path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_recorded_records(
    records: Any,
    *,
    repo_root: Path,
    label: str,
) -> list[dict[str, Any]]:
    if not isinstance(records, list) or not records:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"{label} must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"{label}[{index}] must be an object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"{label}[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(f"{label}[{index}].sha256 missing")
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(
                f"{label} input {name} sha256 mismatch: recorded {recorded_sha}, actual {actual_sha}"
            )
        verified.append(
            {
                "name": name,
                "path": portable_repo_path(resolved, repo_root=repo_root),
                "sha256": recorded_sha,
                "verification_status": "confirmed",
            }
        )
    return verified


def _validate_grant(grant_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant_payload, name="authorization-grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    for field, expected in _identity_fields().items():
        _require_equal(f"grant metadata.{field}", metadata.get(field), expected)

    required = {
        "grant_decision.decision": "granted",
        "grant_decision.owner": OWNER,
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "online_shadow_execution_authorized": True,
        "authorization_granted": True,
        "online_shadow_execution_enabled": False,
        "feature_flag_default_off": True,
        "flag_may_be_enabled_only_in_pilot_env": True,
        "pilot_authorization.runtime_execution_authorized": True,
        "pilot_authorization.shadow_scoring_allowed": True,
        "pilot_authorization.environments": NON_PROD_ENVIRONMENTS,
        "pilot_authorization.initial_ranking_run_ids": [RANKING_RUN_ID],
        "write_mode_policy.phase_1": PHASE_1_WRITE_MODE,
        "write_mode_policy.phase_1_writes_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_production_readiness_authorization": True,
        "missing_online_shadow_execution_authorization": False,
        "shadow_and_production_blockers.missing_online_shadow_execution_authorization": False,
        "shadow_and_production_blockers.missing_production_readiness_authorization": True,
        "shadow_and_production_blockers.runtime_execution_authorized": True,
        "shadow_and_production_blockers.shadow_scoring_allowed": True,
        "shadow_and_production_blockers.online_shadow_execution_enabled": False,
        "grant_scope.ranking_run_id": RANKING_RUN_ID,
        "grant_scope.family": FAMILY,
        "grant_scope.candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "grant_scope.formula_id": FORMULA_ID,
        "grant_scope.scorer_id": SCORER_ID,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant_payload, path), expected)
    if _get(grant_payload, "shadow_and_production_blockers.authorization_scope") is not None:
        _require_equal(
            "grant shadow_and_production_blockers.authorization_scope",
            _get(grant_payload, "shadow_and_production_blockers.authorization_scope"),
            AUTHORIZATION_SCOPE,
        )

    observability = grant_payload.get("required_observability")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError("grant required_observability must be an object")
    policy_contract = observability.get("policy_contract")
    if not isinstance(policy_contract, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(
            "grant required_observability.policy_contract must be an object"
        )
    for key in POLICY_CONTRACT_OBSERVABILITY_KEYS:
        _require_equal(f"grant required_observability.policy_contract.{key}", policy_contract.get(key), True)
    run_fields = observability.get("run_level_fields")
    if not isinstance(run_fields, list):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(
            "grant required_observability.run_level_fields must be a list"
        )
    missing_run_fields = [field for field in RUN_LEVEL_OBSERVABILITY_FIELDS if field not in run_fields]
    if missing_run_fields:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotPlanError(
            f"grant required_observability.run_level_fields missing: {', '.join(missing_run_fields)}"
        )
    return metadata


def _validate_runtime(runtime_payload: Mapping[str, Any], grant_metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(runtime_payload, name="online-shadow-runtime")
    _require_equal("runtime metadata.artifact_type", metadata.get("artifact_type"), RUNTIME_ARTIFACT_TYPE)
    _require_equal("runtime metadata.runtime_version", metadata.get("runtime_version"), RUNTIME_VERSION)
    for field, expected in _identity_fields().items():
        _require_equal(f"runtime metadata.{field}", metadata.get(field), expected)
        _require_equal(f"runtime/grant identity {field}", metadata.get(field), grant_metadata.get(field))
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
        _require_equal(f"runtime {path}", _get(runtime_payload, path), expected)
    return metadata


def _validate_policy(policy_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(policy_payload, name="online-shadow-policy")
    _require_equal("policy metadata.artifact_type", metadata.get("artifact_type"), POLICY_ARTIFACT_TYPE)
    _require_equal("policy metadata.policy_version", metadata.get("policy_version"), POLICY_VERSION)
    required = {
        "online_shadow_execution_policy_defined": True,
        "online_shadow_execution_enabled": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "runtime_isolation_policy.feature_flag": FEATURE_FLAG,
        "runtime_isolation_policy.feature_flag_default": "off",
        "runtime_isolation_policy.feature_flag_default_off": True,
    }
    for path, expected in required.items():
        _require_equal(f"policy {path}", _get(policy_payload, path), expected)
    return metadata


def build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
    *,
    authorization_grant_path: Path,
    online_shadow_runtime_path: Path,
    online_shadow_policy_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    grant_path = Path(authorization_grant_path).resolve()
    runtime_path = Path(online_shadow_runtime_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()

    grant_payload = _load_json_object(grant_path)
    runtime_payload = _load_json_object(runtime_path)
    policy_payload = _load_json_object(policy_path)

    grant_metadata = _validate_grant(grant_payload)
    runtime_metadata = _validate_runtime(runtime_payload, grant_metadata)
    policy_metadata = _validate_policy(policy_payload)
    verified_grant_inputs = _verify_recorded_records(
        _get(grant_payload, "metadata.inputs"),
        repo_root=root,
        label="grant metadata.inputs",
    )
    verified_request_inputs = _verify_recorded_records(
        _get(grant_payload, "metadata.verified_request_inputs"),
        repo_root=root,
        label="grant metadata.verified_request_inputs",
    )
    verified_input_chain = _verify_recorded_records(
        _get(grant_payload, "metadata.verified_input_chain"),
        repo_root=root,
        label="grant metadata.verified_input_chain",
    )

    grant_observability = grant_payload["required_observability"]
    grant_blockers = dict(grant_payload["shadow_and_production_blockers"])
    blockers = {
        **grant_blockers,
        "phase1_no_write_pilot_executed": False,
    }
    identity = _identity_fields()
    source_grant = _input_record("authorization_grant", grant_path, repo_root=root)
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": [
                source_grant,
                _input_record("online_shadow_runtime", runtime_path, repo_root=root),
                _input_record("online_shadow_policy", policy_path, repo_root=root),
            ],
            "source_grant_version": grant_metadata.get("grant_version"),
            "source_runtime_version": runtime_metadata.get("runtime_version"),
            "source_policy_version": policy_metadata.get("policy_version"),
            "verified_grant_inputs": verified_grant_inputs,
            "verified_request_inputs": verified_request_inputs,
            "verified_input_chain": verified_input_chain,
            "runtime_feature_flag": FEATURE_FLAG,
            **identity,
        },
        "phase1_no_write_pilot_plan_defined": True,
        "phase1_no_write_pilot_executed": False,
        "online_shadow_execution_authorized": True,
        "online_shadow_execution_enabled": False,
        "runtime_execution_authorized": True,
        "runtime_execution_authorization_scope": AUTHORIZATION_SCOPE,
        "shadow_scoring_allowed": True,
        "shadow_scoring_allowed_scope": AUTHORIZATION_SCOPE,
        "writes_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "grant_summary": {
            "owner": _get(grant_payload, "grant_decision.owner"),
            "review_by": _get(grant_payload, "grant_decision.review_by"),
            "expiry_date": _get(grant_payload, "grant_decision.expiry_date"),
            "grant_scope": grant_payload["grant_scope"],
            "pilot_bounds": grant_payload["pilot_bounds"],
            "write_mode_policy_phase_1": _get(grant_payload, "write_mode_policy.phase_1"),
            "write_mode_policy": grant_payload["write_mode_policy"],
            "basis_artifacts": grant_payload["basis_artifacts"],
            "source_grant_path": source_grant["path"],
            "source_grant_sha256": source_grant["sha256"],
        },
        "phase1_scope": {
            "non_prod_only": True,
            "approved_ranking_run_id": RANKING_RUN_ID,
            "approved_family": FAMILY,
            "approved_candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "approved_formula_id": FORMULA_ID,
            "approved_scorer_id": SCORER_ID,
            "feature_flag": FEATURE_FLAG,
            "feature_flag_default_off": True,
            "flag_on_only_in_pilot_env": True,
            "no_fleet_wide_enable": True,
        },
        "non_prod_environment_requirements": {
            "environment": NON_PROD_ENVIRONMENTS,
            "feature_flag_default_off_globally": True,
            "feature_flag_may_be_on_only_in_pilot_env": True,
            "read_only_prod_inputs": True,
            "no_fleet_wide_enable": True,
            "manual_or_scheduled_jobs_only": True,
            "operator_must_verify_identity_before_run": True,
        },
        "no_write_execution_contract": {
            "phase_1_performs_no_db_writes": True,
            "phase_1_performs_no_runtime_artifact_writes": True,
            "runtime_output_capture": "process logs/test harness output only for operator review",
            "shadow_storage_persistence_allowed": False,
            "writes_allowed": False,
            "forbidden_write_targets": [
                "ranking_runs",
                "paper_scores",
                "embeddings",
                "labels",
                "scorer artifacts",
                "production config",
                "API-visible tables",
                "production/default pins",
            ],
            "phase_2_requires_separate_write_mode_isolation_proof": True,
        },
        "input_contract": {
            "required_fields": [
                "canonical_openalex_work_id",
                "final_score",
                "audit_embedding_probability_work",
                "ranking_run_id",
                "family",
                "candidate_pool_work_set_sha256",
            ],
            "required_identity": identity,
            "forbidden_label_fields": [
                "relevance_label",
                "novelty_label",
                "bridge_like_label",
                "good_or_acceptable",
                "label_any_positive",
                "reviewer_notes",
                "sample_reason",
                "review_pool_variant",
                "holdout assignment",
            ],
            "complete_final_score_and_learned_probability_coverage_required": True,
        },
        "runtime_invocation_contract": {
            "pilot_runner_not_implemented_here": True,
            "future_runner_next_stage": RECOMMENDED_NEXT_STAGE,
            "feature_flag": FEATURE_FLAG,
            "feature_flag_default_off": True,
            "pilot_env_may_set_flag_on": True,
            "global_online_shadow_execution_enabled": False,
            "no_database_url_required": True,
            "no_write_mode": True,
        },
        "observability_plan": {
            "inherited_from_grant": grant_observability,
            "policy_contract": dict(grant_observability["policy_contract"]),
            "run_level_fields": list(grant_observability["run_level_fields"]),
            "phase_1_expectation": {
                "write_counts_by_isolated_target_present": True,
                "all_write_counts_must_be_zero": True,
                "writes_performed_must_be_false": True,
            },
            "must_not_invent_shorter_list_than_grant": True,
            "grant_key_names_used_verbatim": True,
        },
        "preflight_checklist": [
            "Confirm non-prod pilot environment.",
            f"Confirm ranking_run_id is {RANKING_RUN_ID}.",
            f"Confirm family is {FAMILY}.",
            "Confirm final_score and audit_embedding_probability_work coverage is complete.",
            "Confirm no label fields are present.",
            "Confirm no write targets are configured.",
            f"Confirm {FEATURE_FLAG}=off produces skipped_runtime_disabled before pilot.",
            "Confirm production/API/user-visible baselines are unchanged before pilot.",
        ],
        "rollback_disable_drill": {
            "preflight_verify_flag_off_skips_runtime": True,
            "pilot_on_dry_run_non_prod_only": True,
            "postflight_set_flag_off_and_verify_skipped_runtime_disabled": True,
            "stop_pilot_jobs_on_incident_or_revoke": True,
            "production_ranking_unchanged_before_during_after": True,
            "disable_switch": f"{FEATURE_FLAG}=off",
        },
        "pass_fail_criteria": {
            "pass_only_if": [
                "non-prod only",
                "exact identity match",
                "complete final_score + audit_embedding_probability_work coverage",
                "no label fields present",
                "runtime returns rows only in memory",
                "writes_performed == false",
                "production/API/user-visible outputs unchanged",
                "disable drill passes",
                "all grant-required observability fields recorded for the run",
            ],
            "fail_stop_if": [
                "identity mismatch",
                "incomplete coverage",
                "any write attempt",
                "labels present",
                "API/prod/default mutation detected",
                "disable drill fails",
                "runtime error exceeds threshold",
                "any required observability field missing",
            ],
        },
        "out_of_scope": [
            "runtime pilot execution",
            f"enabling {FEATURE_FLAG}",
            "database writes or shadow tables",
            "API/web integration",
            "production/default ranking behavior changes",
            "production readiness authorization",
            "Phase 2 isolated audit writes",
        ],
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    scope = payload["phase1_scope"]
    blockers = payload["shadow_and_production_blockers"]
    observability = payload["observability_plan"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Phase 1 No-Write Pilot Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact defines the no-write non-production pilot plan. It does not run the pilot, enable the feature flag, write outputs, or change production behavior.",
        "",
        f"- Plan defined: {payload['phase1_no_write_pilot_plan_defined']}",
        f"- Pilot executed: {payload['phase1_no_write_pilot_executed']}",
        f"- Online shadow execution authorized: {payload['online_shadow_execution_authorized']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Writes allowed: {payload['writes_allowed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Phase 1 Scope",
        "",
        f"- Ranking run: `{scope['approved_ranking_run_id']}`",
        f"- Family: `{scope['approved_family']}`",
        f"- Candidate SHA: `{scope['approved_candidate_pool_work_set_sha256']}`",
        f"- Formula: `{scope['approved_formula_id']}`",
        f"- Feature flag: `{scope['feature_flag']}`",
        "",
        "## No-Write Contract",
        "",
        f"- No DB writes: {payload['no_write_execution_contract']['phase_1_performs_no_db_writes']}",
        f"- No runtime artifact writes: {payload['no_write_execution_contract']['phase_1_performs_no_runtime_artifact_writes']}",
        f"- Runtime output capture: {payload['no_write_execution_contract']['runtime_output_capture']}",
        "",
        "## Observability",
        "",
        f"- Policy contract keys: {list(observability['policy_contract'])}",
        f"- Run-level fields: {observability['run_level_fields']}",
        "",
        "## Rollback Disable Drill",
        "",
        f"- Disable switch: `{payload['rollback_disable_drill']['disable_switch']}`",
        "- Verify off before and after pilot.",
        "- Stop pilot jobs on incident or revoke.",
        "",
        "## Pass/Fail Criteria",
        "",
        "Pass only if:",
    ]
    lines.extend(f"- {item}" for item in payload["pass_fail_criteria"]["pass_only_if"])
    lines.extend(["", "Fail/stop if:"])
    lines.extend(f"- {item}" for item in payload["pass_fail_criteria"]["fail_stop_if"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Out Of Scope", ""])
    lines.extend(f"- {item}" for item in payload["out_of_scope"])
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan(
    *,
    authorization_grant_path: Path,
    online_shadow_runtime_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    plan_version: str = PLAN_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan_payload(
        authorization_grant_path=authorization_grant_path,
        online_shadow_runtime_path=online_shadow_runtime_path,
        online_shadow_policy_path=online_shadow_policy_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan(payload),
        encoding="utf-8",
    )
    return payload

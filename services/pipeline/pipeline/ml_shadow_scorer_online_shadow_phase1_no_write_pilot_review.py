"""Review artifact for ml-shadow-scorer-v1 Phase 1 no-write pilot results.

This command reads committed audit artifacts only and records whether the
completed no-write pilot run is accepted as evidence. It does not rerun the
runtime, enable feature flags, access databases, recompute scores, persist
shadow rows, authorize Phase 2 writes, or change production/default behavior.
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
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan import (
    ARTIFACT_TYPE as PLAN_ARTIFACT_TYPE,
    AUTHORIZATION_SCOPE,
    PLAN_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run import (
    ARTIFACT_TYPE as RUN_ARTIFACT_TYPE,
    PASS_NEXT_STAGE as RUN_PASS_NEXT_STAGE,
    RUN_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    ARTIFACT_TYPE as RUNTIME_ARTIFACT_TYPE,
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FORMULA_ID,
    RANKING_RUN_ID,
    RUNTIME_VERSION,
    SCORER_ID,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review"
REVIEW_VERSION = "ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1"

ACCEPTED_NEXT_STAGE = "draft_online_shadow_phase2_isolated_audit_write_mode_plan_v1"
REMEDIATION_NEXT_STAGE = "remediate_online_shadow_phase1_pilot_v1"

EXPECTED_POOL_SIZE = 528

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

RUN_LEVEL_FIELDS = (
    "status",
    "shadow_row_count",
    "writes_performed",
    "production_default_changed",
    "user_visible_ranking_changed",
    "api_web_changes_allowed",
    "runtime_feature_flag_value",
    "labels_used_for_scoring",
)

ACCEPTED_EVIDENCE = (
    "runtime succeeded in test-only pilot mode",
    "528/528 rows scored in memory",
    "preflight/postflight disabled runs skipped with zero rows",
    "no writes performed",
    "shadow rows not persisted",
    "required observability present",
    "production/API/user-visible outputs unchanged",
)

LIMITATIONS = (
    "no persistent shadow sink tested",
    "no Phase 2 write-mode isolation proof yet",
    "production readiness remains separate",
)

CAVEATS = (
    "Review only; no runtime execution occurs in this command.",
    "Phase 1 success does not authorize persistent shadow writes.",
    "Phase 1 success does not enable online shadow execution globally.",
    "Phase 1 success does not authorize production default, API/web behavior, or user-visible ranking changes.",
    "Any Phase 2 write path requires a separate isolated audit write-mode plan and proof.",
)


class MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "formula_id": FORMULA_ID,
        "scorer_id": SCORER_ID,
    }


def _validate_identity(metadata: Mapping[str, Any], *, label: str) -> None:
    for field, expected in _identity_fields().items():
        if field in ("formula_id", "scorer_id") and metadata.get(field) is None:
            continue
        _require_equal(f"{label} metadata.{field}", metadata.get(field), expected)


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError("recorded input path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_recorded_records(
    records: Any,
    *,
    repo_root: Path,
    label: str,
    required: bool = True,
) -> list[dict[str, Any]]:
    if records is None and not required:
        return []
    if not isinstance(records, list) or (required and not records):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"{label} must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"{label}[{index}] must be an object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"{label}[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"{label}[{index}].sha256 missing")
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
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


def _validate_plan(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(plan, name="phase1-no-write-pilot-plan")
    _require_equal("plan metadata.artifact_type", metadata.get("artifact_type"), PLAN_ARTIFACT_TYPE)
    _require_equal("plan metadata.plan_version", metadata.get("plan_version"), PLAN_VERSION)
    _validate_identity(metadata, label="plan")
    required = {
        "phase1_no_write_pilot_plan_defined": True,
        "phase1_no_write_pilot_executed": False,
        "shadow_and_production_blockers.phase1_no_write_pilot_executed": False,
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
        "missing_production_readiness_authorization": True,
        "phase1_scope.approved_formula_id": FORMULA_ID,
        "phase1_scope.approved_scorer_id": SCORER_ID,
    }
    for path, expected in required.items():
        _require_equal(f"plan {path}", _get(plan, path), expected)
    return metadata


def _validate_grant(grant: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant, name="authorization-grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    _validate_identity(metadata, label="grant")
    required = {
        "grant_decision.decision": "granted",
        "grant_decision.owner": OWNER,
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "authorization_granted": True,
        "online_shadow_execution_authorized": True,
        "pilot_authorization.environments": "non-prod pilot only",
        "write_mode_policy.phase_1": "no_writes",
        "write_mode_policy.phase_1_writes_allowed": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "grant_scope.formula_id": FORMULA_ID,
        "grant_scope.scorer_id": SCORER_ID,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)
    return metadata


def _validate_runtime(runtime: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(runtime, name="online-shadow-runtime")
    _require_equal("runtime metadata.artifact_type", metadata.get("artifact_type"), RUNTIME_ARTIFACT_TYPE)
    _require_equal("runtime metadata.runtime_version", metadata.get("runtime_version"), RUNTIME_VERSION)
    _validate_identity(metadata, label="runtime")
    required = {
        "runtime_disabled_by_default": True,
        "runtime_default_state": "off",
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
        _require_equal(f"runtime {path}", _get(runtime, path), expected)
    return metadata


def _expected_plan_policy_keys(plan: Mapping[str, Any]) -> list[str]:
    keys = _get(plan, "observability_plan.policy_contract")
    if not isinstance(keys, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError("plan observability_plan.policy_contract missing")
    missing = [key for key in POLICY_CONTRACT_OBSERVABILITY_KEYS if keys.get(key) is not True]
    if missing:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            f"plan observability_plan.policy_contract missing: {', '.join(missing)}"
        )
    return list(keys)


def _expected_plan_run_fields(plan: Mapping[str, Any]) -> list[str]:
    fields = _get(plan, "observability_plan.run_level_fields")
    if not isinstance(fields, list):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError("plan observability_plan.run_level_fields missing")
    missing = [field for field in RUN_LEVEL_FIELDS if field != "labels_used_for_scoring" and field not in fields]
    if missing:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            f"plan observability_plan.run_level_fields missing: {', '.join(missing)}"
        )
    return list(fields) + (["labels_used_for_scoring"] if "labels_used_for_scoring" not in fields else [])


def _validate_observability(run: Mapping[str, Any], plan: Mapping[str, Any]) -> None:
    observability = run.get("observability")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError("run observability must be an object")
    for key in _expected_plan_policy_keys(plan):
        if key not in observability:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(f"run observability missing {key}")
        if _get(run, f"observability.policy_contract_satisfied.{key}") is not True:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
                f"run observability.policy_contract_satisfied.{key} must be true"
            )
    run_level = observability.get("run_level")
    if not isinstance(run_level, Mapping):
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError("run observability.run_level must be an object")
    for field in _expected_plan_run_fields(plan):
        if field not in run_level:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
                f"run observability.run_level missing {field}"
            )
        if _get(run, f"observability.run_level_fields_satisfied.{field}") is not True:
            raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
                f"run observability.run_level_fields_satisfied.{field} must be true"
            )
    write_counts = _get(run, "observability.write_counts_by_isolated_target")
    if not isinstance(write_counts, Mapping) or not write_counts:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            "run observability.write_counts_by_isolated_target must be a non-empty object"
        )
    nonzero = {key: value for key, value in write_counts.items() if value != 0}
    if nonzero:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            f"run write_counts_by_isolated_target must all be zero, got {nonzero}"
        )


def _validate_no_write_verification(run: Mapping[str, Any]) -> None:
    required = {
        "writes_performed": False,
        "writes_allowed": False,
        "no_write_verification.writes_performed": False,
        "no_write_verification.writes_allowed": False,
        "no_write_verification.write_count": 0,
        "no_write_verification.all_write_counts_zero": True,
        "no_write_verification.shadow_rows_persisted": False,
        "no_write_verification.shadow_storage_persistence_allowed": False,
        "pilot_runtime_result.shadow_rows_persisted": False,
        "pilot_runtime_result.shadow_rows_omitted_from_artifact": True,
        "pilot_runtime_result.writes_performed": False,
        "pilot_runtime_result.production_default_changed": False,
        "pilot_runtime_result.user_visible_ranking_changed": False,
    }
    for path, expected in required.items():
        _require_equal(f"run {path}", _get(run, path), expected)
    write_counts = _get(run, "no_write_verification.write_counts_by_isolated_target")
    if not isinstance(write_counts, Mapping) or not write_counts:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            "run no_write_verification.write_counts_by_isolated_target must be a non-empty object"
        )
    nonzero = {key: value for key, value in write_counts.items() if value != 0}
    if nonzero:
        raise MLShadowScorerOnlineShadowPhase1NoWritePilotReviewError(
            f"run no_write_verification write counts must all be zero, got {nonzero}"
        )


def _validate_run(run: Mapping[str, Any], plan: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(run, name="phase1-no-write-pilot-run")
    _require_equal("run metadata.artifact_type", metadata.get("artifact_type"), RUN_ARTIFACT_TYPE)
    _require_equal("run metadata.run_version", metadata.get("run_version"), RUN_VERSION)
    _validate_identity(metadata, label="run")
    required = {
        "phase1_no_write_pilot_executed": True,
        "phase1_no_write_pilot_passed": True,
        "recommended_next_stage": RUN_PASS_NEXT_STAGE,
        "pass_fail_evaluation.passed": True,
        "pass_fail_evaluation.failed_checks": [],
        "pilot_runtime_result.status": "succeeded_test_only",
        "input_join_summary.joined_candidate_count": EXPECTED_POOL_SIZE,
        "input_join_summary.runtime_row_count": EXPECTED_POOL_SIZE,
        "disable_drill_passed": True,
        "disable_drill.passed": True,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "runtime_execution_authorized": True,
        "runtime_execution_authorization_scope": AUTHORIZATION_SCOPE,
        "shadow_scoring_allowed": True,
        "shadow_scoring_allowed_scope": AUTHORIZATION_SCOPE,
    }
    for path, expected in required.items():
        _require_equal(f"run {path}", _get(run, path), expected)
    _validate_no_write_verification(run)
    _validate_observability(run, plan)
    return metadata


def _review_checks(run: Mapping[str, Any]) -> dict[str, bool]:
    return {
        "runtime_succeeded_in_test_only_mode": _get(run, "pilot_runtime_result.status") == "succeeded_test_only",
        "all_rows_scored_in_memory": _get(run, "pilot_runtime_result.shadow_row_count") == EXPECTED_POOL_SIZE
        and _get(run, "input_join_summary.runtime_row_count") == EXPECTED_POOL_SIZE,
        "preflight_postflight_disabled": _get(run, "disable_drill.preflight.status") == "skipped_runtime_disabled"
        and _get(run, "disable_drill.postflight.status") == "skipped_runtime_disabled"
        and _get(run, "disable_drill.preflight.shadow_row_count") == 0
        and _get(run, "disable_drill.postflight.shadow_row_count") == 0,
        "no_writes_performed": run.get("writes_performed") is False
        and _get(run, "no_write_verification.all_write_counts_zero") is True,
        "shadow_rows_not_persisted": _get(run, "pilot_runtime_result.shadow_rows_persisted") is False,
        "required_observability_present": _get(run, "pass_fail_evaluation.checks.all_grant_required_observability_fields_recorded")
        is True,
        "production_api_user_visible_outputs_unchanged": run.get("production_default_allowed") is False
        and run.get("api_web_changes_allowed") is False
        and run.get("user_visible_ranking_changed") is False
        and _get(run, "rollback_summary.production_ranking_unchanged") is True,
        "feature_flag_restored_after_runner": _get(run, "rollback_summary.feature_flag_restored_after_runner") is True,
        "phase2_writes_not_authorized": True,
    }


def _review_decision(run: Mapping[str, Any]) -> dict[str, Any]:
    checks = _review_checks(run)
    failed = [name for name, passed in checks.items() if passed is not True]
    accepted = not failed
    return {
        "decision": "accepted" if accepted else "not_accepted",
        "phase1_no_write_pilot_result_accepted": accepted,
        "checks": checks,
        "failed_review_checks": failed,
        "accepted_evidence": list(ACCEPTED_EVIDENCE) if accepted else [],
        "limitations": list(LIMITATIONS),
    }


def build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review_payload(
    *,
    phase1_no_write_pilot_run_path: Path,
    phase1_no_write_pilot_plan_path: Path,
    authorization_grant_path: Path,
    online_shadow_runtime_path: Path,
    review_version: str = REVIEW_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    run_path = Path(phase1_no_write_pilot_run_path).resolve()
    plan_path = Path(phase1_no_write_pilot_plan_path).resolve()
    grant_path = Path(authorization_grant_path).resolve()
    runtime_path = Path(online_shadow_runtime_path).resolve()

    run = _load_json_object(run_path)
    plan = _load_json_object(plan_path)
    grant = _load_json_object(grant_path)
    runtime = _load_json_object(runtime_path)

    plan_metadata = _validate_plan(plan)
    grant_metadata = _validate_grant(grant)
    runtime_metadata = _validate_runtime(runtime)
    run_metadata = _validate_run(run, plan)

    verified_run_inputs = _verify_recorded_records(
        _get(run, "metadata.inputs"),
        repo_root=root,
        label="run metadata.inputs",
    )
    verified_plan_inputs = _verify_recorded_records(
        _get(plan, "metadata.inputs"),
        repo_root=root,
        label="plan metadata.inputs",
    )
    verified_grant_inputs = _verify_recorded_records(
        _get(grant, "metadata.inputs"),
        repo_root=root,
        label="grant metadata.inputs",
    )
    verified_grant_request_inputs = _verify_recorded_records(
        _get(grant, "metadata.verified_request_inputs"),
        repo_root=root,
        label="grant metadata.verified_request_inputs",
        required=False,
    )
    verified_grant_input_chain = _verify_recorded_records(
        _get(grant, "metadata.verified_input_chain"),
        repo_root=root,
        label="grant metadata.verified_input_chain",
        required=False,
    )
    verified_runtime_inputs = _verify_recorded_records(
        _get(runtime, "metadata.inputs"),
        repo_root=root,
        label="runtime metadata.inputs",
        required=False,
    )

    decision = _review_decision(run)
    accepted = decision["phase1_no_write_pilot_result_accepted"] is True
    source_artifacts = {
        "phase1_no_write_pilot_run": _input_record("phase1_no_write_pilot_run", run_path, repo_root=root),
        "phase1_no_write_pilot_plan": _input_record("phase1_no_write_pilot_plan", plan_path, repo_root=root),
        "authorization_grant": _input_record("authorization_grant", grant_path, repo_root=root),
        "online_shadow_runtime": _input_record("online_shadow_runtime", runtime_path, repo_root=root),
    }
    blockers = {
        **dict(run["shadow_and_production_blockers"]),
        "missing_phase1_no_write_pilot_review": False,
        "missing_phase2_write_mode_isolation_proof": True,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "phase2_writes_authorized": False,
    }
    identity = _identity_fields()
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "review_version": review_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": list(source_artifacts.values()),
            "source_run_version": run_metadata.get("run_version"),
            "source_plan_version": plan_metadata.get("plan_version"),
            "source_grant_version": grant_metadata.get("grant_version"),
            "source_runtime_version": runtime_metadata.get("runtime_version"),
            "verified_run_inputs": verified_run_inputs,
            "verified_plan_inputs": verified_plan_inputs,
            "verified_grant_inputs": verified_grant_inputs,
            "verified_grant_request_inputs": verified_grant_request_inputs,
            "verified_grant_input_chain": verified_grant_input_chain,
            "verified_runtime_inputs": verified_runtime_inputs,
            **identity,
        },
        "phase1_no_write_pilot_review_executed": True,
        "phase1_no_write_pilot_result_accepted": accepted,
        "phase1_no_write_pilot_executed": True,
        "phase1_no_write_pilot_passed": True,
        "online_shadow_execution_authorized": True,
        "online_shadow_execution_enabled": False,
        "runtime_execution_authorized": True,
        "runtime_execution_authorization_scope": AUTHORIZATION_SCOPE,
        "shadow_scoring_allowed": True,
        "shadow_scoring_allowed_scope": AUTHORIZATION_SCOPE,
        "writes_performed": False,
        "phase2_writes_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "review_scope": {
            "review_only": True,
            "runtime_rerun_performed": False,
            "phase2_writes_authorized": False,
            "source_of_truth": "phase1_no_write_pilot_run artifact",
            "accepted_next_stage": ACCEPTED_NEXT_STAGE,
            "remediation_next_stage": REMEDIATION_NEXT_STAGE,
        },
        "source_artifacts": source_artifacts,
        "pilot_result_summary": {
            "runtime_status": _get(run, "pilot_runtime_result.status"),
            "joined_candidate_count": _get(run, "input_join_summary.joined_candidate_count"),
            "runtime_row_count": _get(run, "input_join_summary.runtime_row_count"),
            "shadow_row_count": _get(run, "pilot_runtime_result.shadow_row_count"),
            "phase1_no_write_pilot_passed": run.get("phase1_no_write_pilot_passed"),
            "pass_fail_evaluation_passed": _get(run, "pass_fail_evaluation.passed"),
            "failed_checks": _get(run, "pass_fail_evaluation.failed_checks"),
        },
        "observability_review": {
            "required_policy_contract_keys": _expected_plan_policy_keys(plan),
            "required_run_level_fields": _expected_plan_run_fields(plan),
            "policy_contract_satisfied": _get(run, "observability.policy_contract_satisfied"),
            "run_level_fields_satisfied": _get(run, "observability.run_level_fields_satisfied"),
            "write_counts_by_isolated_target": _get(run, "observability.write_counts_by_isolated_target"),
            "complete": decision["checks"]["required_observability_present"],
        },
        "no_write_review": {
            "writes_allowed": False,
            "writes_performed": run.get("writes_performed"),
            "write_count": _get(run, "no_write_verification.write_count"),
            "all_write_counts_zero": _get(run, "no_write_verification.all_write_counts_zero"),
            "shadow_rows_persisted": _get(run, "pilot_runtime_result.shadow_rows_persisted"),
            "shadow_rows_omitted_from_artifact": _get(run, "pilot_runtime_result.shadow_rows_omitted_from_artifact"),
            "phase2_writes_authorized": False,
        },
        "disable_drill_review": {
            "disable_drill_passed": run.get("disable_drill_passed"),
            "preflight_status": _get(run, "disable_drill.preflight.status"),
            "postflight_status": _get(run, "disable_drill.postflight.status"),
            "environment_restored": _get(run, "disable_drill.environment_restored"),
        },
        "identity_review": {
            "identity_fields": identity,
            "run_plan_grant_runtime_identity_matched": True,
        },
        "blocker_review": {
            "missing_online_shadow_execution_authorization": run.get("missing_online_shadow_execution_authorization"),
            "missing_production_readiness_authorization": run.get("missing_production_readiness_authorization"),
            "online_shadow_execution_enabled": run.get("online_shadow_execution_enabled"),
            "production_default_allowed": run.get("production_default_allowed"),
            "api_web_changes_allowed": run.get("api_web_changes_allowed"),
            "user_visible_ranking_changed": run.get("user_visible_ranking_changed"),
            "phase2_writes_authorized": False,
        },
        "review_decision": decision,
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": ACCEPTED_NEXT_STAGE if accepted else REMEDIATION_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    summary = payload["pilot_result_summary"]
    decision = payload["review_decision"]
    no_write = payload["no_write_review"]
    drill = payload["disable_drill_review"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Phase 1 No-Write Pilot Review ({metadata['review_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact reviews the completed no-write pilot run from committed evidence only. It does not rerun the runtime, enable the feature flag, write shadow rows, or authorize Phase 2 writes.",
        "",
        f"- Review executed: {payload['phase1_no_write_pilot_review_executed']}",
        f"- Phase 1 result accepted: {payload['phase1_no_write_pilot_result_accepted']}",
        f"- Decision: `{decision['decision']}`",
        f"- Runtime status: `{summary['runtime_status']}`",
        f"- Runtime rows: {summary['runtime_row_count']}",
        f"- Writes performed: {payload['writes_performed']}",
        f"- Phase 2 writes authorized: {payload['phase2_writes_authorized']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Accepted Evidence",
        "",
    ]
    evidence = decision["accepted_evidence"] or ["No accepted evidence; see failed review checks."]
    lines.extend(f"- {item}" for item in evidence)
    lines.extend(
        [
            "",
            "## No-Write Review",
            "",
            f"- Writes allowed: {no_write['writes_allowed']}",
            f"- Writes performed: {no_write['writes_performed']}",
            f"- Shadow rows persisted: {no_write['shadow_rows_persisted']}",
            f"- Shadow rows omitted from artifact: {no_write['shadow_rows_omitted_from_artifact']}",
            "",
            "## Disable Drill",
            "",
            f"- Passed: {drill['disable_drill_passed']}",
            f"- Preflight status: `{drill['preflight_status']}`",
            f"- Postflight status: `{drill['postflight_status']}`",
            "",
            "## Limitations",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in decision["limitations"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review(
    *,
    phase1_no_write_pilot_run_path: Path,
    phase1_no_write_pilot_plan_path: Path,
    authorization_grant_path: Path,
    online_shadow_runtime_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    review_version: str = REVIEW_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review_payload(
        phase1_no_write_pilot_run_path=phase1_no_write_pilot_run_path,
        phase1_no_write_pilot_plan_path=phase1_no_write_pilot_plan_path,
        authorization_grant_path=authorization_grant_path,
        online_shadow_runtime_path=online_shadow_runtime_path,
        review_version=review_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review(payload),
        encoding="utf-8",
    )
    return payload

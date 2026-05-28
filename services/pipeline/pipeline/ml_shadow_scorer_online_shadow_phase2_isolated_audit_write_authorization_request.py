"""Request Phase 2 isolated audit write-pilot authorization for ml-shadow-scorer-v1.

This paperwork-only module validates the completed Phase 2 isolated audit
write-mode proof and writes an owner-facing request artifact. It does not run
the runtime, enable feature flags, write shadow-run pilot data, touch database
or API tables, grant authorization, or change production ranking behavior.
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
    REVIEW_BY,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review import (
    ARTIFACT_TYPE as REVIEW_ARTIFACT_TYPE,
    REVIEW_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run import RUN_VERSION as PHASE1_RUN_VERSION
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan import (
    ARTIFACT_TYPE as PLAN_ARTIFACT_TYPE,
    PHASE2_POLICY,
    PLAN_VERSION_PHASE2,
    PRIMARY_TARGET_ROOT,
)
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof import (
    ARTIFACT_TYPE as PROOF_ARTIFACT_TYPE,
    PASS_NEXT_STAGE as PROOF_NEXT_STAGE,
    PROOF_SCOPE,
    PROOF_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FORMULA_ID,
    RANKING_RUN_ID,
    SCORER_ID,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request"
REQUEST_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1"
RECOMMENDED_NEXT_STAGE = "record_online_shadow_phase2_isolated_audit_write_authorization_grant_v1"
AUTHORIZATION_SCOPE = "bounded_non_prod_pilot_only"

FORBIDDEN_WRITE_TARGETS = (
    "ranking_runs",
    "paper_scores",
    "embeddings",
    "labels",
    "scorer_artifacts",
    "production_config",
    "production_default_pins",
    "api_visible_tables",
    "isolated_audit_shadow_tables",
)

EXPECTED_ARTIFACTS_PER_PILOT_RUN = (
    "manifest.json",
    "shadow_rows.jsonl",
    "observability.json",
    "write_counts.json",
)

CAVEATS = (
    "Request artifact only; does not run proof, pilot, or runtime.",
    "Does not create shadow-runs/ files or mutate proof/plan/grant/review artifacts.",
    "Does not clear missing_phase2_isolated_audit_write_pilot_authorization.",
    "Does not set phase2_writes_authorized or phase2_write_pilot_authorized true.",
    f"Does not enable {FEATURE_FLAG} globally.",
    "Does not authorize production readiness, production default, API/web, or user-visible ranking changes.",
    "Proof file-tree writes were proof-only; a future granted pilot is a separate execution step.",
)


class MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"Expected JSON object in {path}"
        )
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"{name} JSON missing metadata object"
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


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _reject_if_explicit_true(name: str, value: Any) -> None:
    if value is True:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"{name} must not be explicitly true"
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


def _validate_metadata_identity(metadata: Mapping[str, Any], *, label: str) -> None:
    for field, expected in _identity_fields().items():
        if metadata.get(field) is not None:
            _require_equal(f"{label} metadata.{field}", metadata.get(field), expected)


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"Input {name} does not exist: {path}"
        )
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            "recorded input path must be a non-empty string"
        )
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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            f"{label} must be a non-empty list"
        )
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
                f"{label}[{index}] must be an object"
            )
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
                f"{label}[{index}].name missing"
            )
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
                f"{label}[{index}].sha256 missing"
            )
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
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


def _validate_proof(proof: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(proof, name="phase2-write-mode-proof")
    _require_equal("proof metadata.artifact_type", metadata.get("artifact_type"), PROOF_ARTIFACT_TYPE)
    _require_equal("proof metadata.proof_version", metadata.get("proof_version"), PROOF_VERSION)
    _validate_metadata_identity(metadata, label="proof")
    required = {
        "phase2_write_mode_proof_executed": True,
        "phase2_write_mode_proof_passed": True,
        "phase2_write_mode_proof_allowed_by_plan": True,
        "phase2_write_mode_proof_scope": PROOF_SCOPE,
        "missing_phase2_write_mode_isolation_proof": False,
        "phase2_writes_authorized": False,
        "phase2_write_pilot_authorized": False,
        "recommended_next_stage": PROOF_NEXT_STAGE,
        "pass_fail_evaluation.passed": True,
        "write_count_verification.forbidden_targets_zero": True,
        "write_count_verification.write_counts_by_isolated_target.isolated_audit_shadow_tables": 0,
        "runtime_writes_performed": False,
    }
    for path, expected in required.items():
        _require_equal(f"proof {path}", _get(proof, path), expected)
    return metadata


def _validate_plan(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(plan, name="phase2-write-mode-plan")
    _require_equal("plan metadata.artifact_type", metadata.get("artifact_type"), PLAN_ARTIFACT_TYPE)
    _require_equal("plan metadata.plan_version", metadata.get("plan_version"), PLAN_VERSION_PHASE2)
    _validate_metadata_identity(metadata, label="plan")
    required = {
        "phase2_isolated_audit_write_mode_plan_defined": True,
        "isolated_write_target.root_path": PRIMARY_TARGET_ROOT,
        "phase2_writes_authorized": False,
    }
    for path, expected in required.items():
        _require_equal(f"plan {path}", _get(plan, path), expected)
    return metadata


def _validate_grant(grant: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant, name="authorization-grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    _validate_metadata_identity(metadata, label="grant")
    required = {
        "authorization_granted": True,
        "grant_decision.decision": "granted",
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "write_mode_policy.phase_2": PHASE2_POLICY,
        "write_mode_policy.phase_2_requires_separate_authorization": True,
        "runtime_execution_authorized": True,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)
    _reject_if_explicit_true("grant phase2_writes_authorized", grant.get("phase2_writes_authorized"))
    return metadata


def _validate_review(review: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(review, name="phase1-review")
    _require_equal("review metadata.artifact_type", metadata.get("artifact_type"), REVIEW_ARTIFACT_TYPE)
    _require_equal("review metadata.review_version", metadata.get("review_version"), REVIEW_VERSION)
    _validate_metadata_identity(metadata, label="review")
    required = {
        "phase1_no_write_pilot_result_accepted": True,
        "review_decision.decision": "accepted",
    }
    for path, expected in required.items():
        _require_equal(f"review {path}", _get(review, path), expected)
    return metadata


def _validate_optional_phase1_run(payload: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if payload is None:
        return None
    metadata = _metadata(payload, name="phase1-run")
    _require_equal("phase1 run metadata.run_version", metadata.get("run_version"), PHASE1_RUN_VERSION)
    _validate_metadata_identity(metadata, label="phase1 run")
    return metadata


def _verify_optional_proof_chains(proof_metadata: Mapping[str, Any], *, repo_root: Path) -> dict[str, list[dict[str, Any]]]:
    chain_keys = (
        "verified_plan_inputs",
        "verified_grant_inputs",
        "verified_review_inputs",
        "verified_learned_inputs",
        "verified_audit_inputs",
        "verified_optional_phase1_run_inputs",
    )
    return {
        key: _verify_recorded_records(
            proof_metadata.get(key),
            repo_root=repo_root,
            label=f"proof metadata.{key}",
            required=False,
        )
        for key in chain_keys
    }


def _copy_file_records(files: Any) -> list[dict[str, Any]]:
    if not isinstance(files, list) or not files:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            "proof isolated_file_writes.files_written must be a non-empty list"
        )
    copied: list[dict[str, Any]] = []
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
                f"proof isolated_file_writes.files_written[{index}] must be an object"
            )
        copied.append(
            {
                "relative_path": record.get("relative_path"),
                "byte_count": record.get("byte_count"),
                "sha256": record.get("sha256"),
                "row_count": record.get("row_count"),
                "write_target": record.get("write_target"),
            }
        )
    return copied


def _proof_summary(proof: Mapping[str, Any]) -> dict[str, Any]:
    write_counts = _get(proof, "write_count_verification.write_counts_by_isolated_target")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            "proof write_count_verification.write_counts_by_isolated_target must be an object"
        )
    files_written = _copy_file_records(_get(proof, "isolated_file_writes.files_written"))
    return {
        "proof_passed": proof.get("phase2_write_mode_proof_passed"),
        "proof_scope": proof.get("phase2_write_mode_proof_scope"),
        "pilot_run_id": _get(proof, "metadata.pilot_run_id"),
        "joined_candidate_count": _get(proof, "input_join_summary.joined_candidate_count"),
        "isolated_file_writes": {
            "file_count": _get(proof, "isolated_file_writes.file_count"),
            "bytes_written": _get(proof, "isolated_file_writes.bytes_written"),
            "files_written": files_written,
        },
        "write_counts_by_isolated_target": dict(write_counts),
        "runtime_writes_performed": proof.get("runtime_writes_performed"),
        "isolated_artifact_tree_writes_performed": proof.get("isolated_artifact_tree_writes_performed"),
        "disable_drill": {"passed": _get(proof, "disable_drill.passed")},
        "rollback_and_cleanup": {"cleanup_completed": _get(proof, "rollback_and_cleanup.cleanup_completed")},
        "durable_evidence_note": (
            "Proof pilot directory may be cleaned; file hashes and counts copied here from the proof are durable evidence."
        ),
    }


def _requested_grant_scope() -> dict[str, Any]:
    return {
        "authorization_scope": AUTHORIZATION_SCOPE,
        "write_target_root": PRIMARY_TARGET_ROOT,
        "pilot_run_subdirectory_pattern": f"{PRIMARY_TARGET_ROOT}<pilot_run_id>/",
        "allowed_write_targets": ["isolated_audit_shadow_artifacts"],
        "forbidden_write_targets": list(FORBIDDEN_WRITE_TARGETS),
        "expected_artifacts_per_pilot_run": list(EXPECTED_ARTIFACTS_PER_PILOT_RUN),
        "pilot_run_id_rules": {
            "regex": "^[A-Za-z0-9._-]+$",
            "strict_child_of_phase2_proof_root": True,
            "no_traversal": True,
            "no_path_separators": True,
        },
        "one_bounded_pilot_run_at_a_time": True,
        "manual_or_scheduled_jobs_only": True,
        "no_fleet_wide_enable": True,
        "feature_flag": FEATURE_FLAG,
        "feature_flag_on_only_in_approved_pilot_env": True,
        "feature_flag_default_off_elsewhere": True,
        "approved_identity": _identity_fields(),
        "same_approved_identity_and_528_row_coverage_as_phase1_and_proof": True,
        "cleanup": "delete/archive only <pilot_run_id> subdirectory; never phase2-proof root",
        "explicitly_out_of_scope_for_future_pilot": [
            "DB DDL",
            "production tables",
            "API/web",
            "production default",
            "fleet enablement",
        ],
    }


def build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request_payload(
    *,
    phase2_write_mode_proof_path: Path,
    phase2_write_mode_plan_path: Path,
    authorization_grant_path: Path,
    phase1_no_write_pilot_review_path: Path,
    phase1_no_write_pilot_run_path: Path | None = None,
    request_version: str = REQUEST_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    proof_path = Path(phase2_write_mode_proof_path).resolve()
    plan_path = Path(phase2_write_mode_plan_path).resolve()
    grant_path = Path(authorization_grant_path).resolve()
    review_path = Path(phase1_no_write_pilot_review_path).resolve()
    optional_run_path = Path(phase1_no_write_pilot_run_path).resolve() if phase1_no_write_pilot_run_path else None

    proof = _load_json_object(proof_path)
    plan = _load_json_object(plan_path)
    grant = _load_json_object(grant_path)
    review = _load_json_object(review_path)
    optional_run = _load_json_object(optional_run_path) if optional_run_path else None

    proof_metadata = _validate_proof(proof)
    plan_metadata = _validate_plan(plan)
    grant_metadata = _validate_grant(grant)
    review_metadata = _validate_review(review)
    optional_run_metadata = _validate_optional_phase1_run(optional_run)

    verified_proof_inputs = _verify_recorded_records(
        proof_metadata.get("inputs"),
        repo_root=root,
        label="proof metadata.inputs",
    )
    verified_plan_inputs = _verify_recorded_records(
        plan_metadata.get("inputs"),
        repo_root=root,
        label="plan metadata.inputs",
    )
    verified_grant_inputs = _verify_recorded_records(
        grant_metadata.get("inputs"),
        repo_root=root,
        label="grant metadata.inputs",
    )
    verified_review_inputs = _verify_recorded_records(
        review_metadata.get("inputs"),
        repo_root=root,
        label="review metadata.inputs",
    )
    verified_optional_phase1_run_inputs = (
        _verify_recorded_records(
            optional_run_metadata.get("inputs"),
            repo_root=root,
            label="optional phase1 run metadata.inputs",
        )
        if optional_run_metadata is not None
        else []
    )
    verified_proof_chains = _verify_optional_proof_chains(proof_metadata, repo_root=root)

    source_artifacts = {
        "phase2_write_mode_proof": _input_record("phase2_write_mode_proof", proof_path, repo_root=root),
        "phase2_write_mode_plan": _input_record("phase2_write_mode_plan", plan_path, repo_root=root),
        "authorization_grant": _input_record("authorization_grant", grant_path, repo_root=root),
        "phase1_no_write_pilot_review": _input_record("phase1_no_write_pilot_review", review_path, repo_root=root),
    }
    if optional_run_path is not None:
        source_artifacts["phase1_no_write_pilot_run"] = _input_record(
            "phase1_no_write_pilot_run",
            optional_run_path,
            repo_root=root,
        )

    proof_blockers = proof.get("shadow_and_production_blockers")
    if not isinstance(proof_blockers, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationRequestError(
            "proof shadow_and_production_blockers must be an object"
        )
    blockers = {
        **dict(proof_blockers),
        "missing_phase2_write_mode_isolation_proof": False,
        "missing_phase2_isolated_audit_write_pilot_authorization": True,
        "phase2_writes_authorized": False,
        "online_shadow_execution_enabled": False,
        "blockers_changed_by_request": [],
        "blockers_unchanged_by_request": True,
    }

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "request_version": request_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": list(source_artifacts.values()),
            "source_proof_version": proof_metadata.get("proof_version"),
            "source_plan_version": plan_metadata.get("plan_version"),
            "source_grant_version": grant_metadata.get("grant_version"),
            "source_review_version": review_metadata.get("review_version"),
            "source_phase1_run_version": optional_run_metadata.get("run_version") if optional_run_metadata else None,
            "verified_proof_inputs": verified_proof_inputs,
            "verified_plan_inputs": verified_plan_inputs,
            "verified_grant_inputs": verified_grant_inputs,
            "verified_review_inputs": verified_review_inputs,
            "verified_optional_phase1_run_inputs": verified_optional_phase1_run_inputs,
            "verified_proof_chains": verified_proof_chains,
            **_identity_fields(),
        },
        "phase2_isolated_audit_write_authorization_requested": True,
        "phase2_isolated_audit_write_authorization_granted": False,
        "phase2_write_pilot_authorized": False,
        "phase2_writes_authorized": False,
        "phase2_write_mode_proof_executed": True,
        "phase2_write_mode_proof_passed": True,
        "missing_phase2_write_mode_isolation_proof": False,
        "missing_phase2_isolated_audit_write_pilot_authorization": True,
        "online_shadow_execution_enabled": False,
        "runtime_writes_performed": False,
        "isolated_artifact_tree_writes_performed": False,
        "writes_performed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_production_readiness_authorization": True,
        "source_artifacts": source_artifacts,
        "proof_summary": _proof_summary(proof),
        "requested_grant_scope": _requested_grant_scope(),
        "phase1_prerequisite_summary": {
            "phase1_no_write_pilot_result_accepted": review.get("phase1_no_write_pilot_result_accepted"),
            "review_decision": _get(review, "review_decision.decision"),
            "proof_cleared_missing_phase2_write_mode_isolation_proof": True,
        },
        "grant_write_mode_policy_summary": {
            "authorization_granted": grant.get("authorization_granted"),
            "runtime_execution_authorized": grant.get("runtime_execution_authorized"),
            "phase_2": _get(grant, "write_mode_policy.phase_2"),
            "phase_2_requires_separate_authorization": _get(
                grant,
                "write_mode_policy.phase_2_requires_separate_authorization",
            ),
            "review_by": _get(grant, "grant_decision.review_by"),
            "expiry_date": _get(grant, "grant_decision.expiry_date"),
        },
        "shadow_and_production_blockers": blockers,
        "remaining_blockers_before_write_pilot": [
            "No Phase 2 write pilot grant artifact.",
            "missing_phase2_isolated_audit_write_pilot_authorization remains true and is unchanged by this artifact.",
            "phase2_writes_authorized remains false.",
            "Feature flag remains default off outside approved pilot environment.",
            "Production readiness and production default authorization remain separate chains.",
        ],
        "consumer_guidance": {
            "proof_executed_truth_source": "phase2 proof artifact pass_fail_evaluation and isolated_file_writes",
            "request_does_not_grant": (
                "phase2_write_pilot_authorized / phase2_writes_authorized stay false"
            ),
            "execution_grant_still_required_for_runtime": (
                "runtime_execution_authorized from execution grant; this request is write-pilot authorization only"
            ),
        },
        "required_future_runbook": {
            "future_command": "ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-authorization-grant",
            "implemented_in_this_task": False,
            "only_future_grant_may_set_phase2_write_pilot_authorized_true": True,
            "only_future_grant_may_clear_missing_phase2_isolated_audit_write_pilot_authorization": True,
            "only_future_grant_may_set_phase2_writes_authorized_true": True,
        },
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    proof = payload["proof_summary"]
    scope = payload["requested_grant_scope"]
    lines = [
        "# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write Authorization Request "
        f"({metadata['request_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact requests owner review for a bounded non-production Phase 2 isolated audit write pilot. It does not grant authorization, run the runtime, create shadow-run files, enable the feature flag, or change production behavior.",
        "",
        f"- Authorization requested: {payload['phase2_isolated_audit_write_authorization_requested']}",
        f"- Authorization granted: {payload['phase2_isolated_audit_write_authorization_granted']}",
        f"- Phase 2 write pilot authorized: {payload['phase2_write_pilot_authorized']}",
        f"- Phase 2 writes authorized: {payload['phase2_writes_authorized']}",
        f"- Proof passed: {payload['phase2_write_mode_proof_passed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Proof Summary",
        "",
        f"- Pilot run id: `{proof['pilot_run_id']}`",
        f"- Joined candidates: {proof['joined_candidate_count']}",
        f"- Proof files written: {proof['isolated_file_writes']['file_count']}",
        f"- Proof bytes written: {proof['isolated_file_writes']['bytes_written']}",
        f"- Runtime writes in proof: {proof['runtime_writes_performed']}",
        f"- Cleanup completed in proof: {proof['rollback_and_cleanup']['cleanup_completed']}",
        "",
        "## Requested Scope",
        "",
        f"- Authorization scope: `{scope['authorization_scope']}`",
        f"- Write target root: `{scope['write_target_root']}`",
        f"- Pilot path pattern: `{scope['pilot_run_subdirectory_pattern']}`",
        f"- Allowed write targets: {', '.join(f'`{item}`' for item in scope['allowed_write_targets'])}",
        f"- Feature flag: `{scope['feature_flag']}`",
        "",
        "## Remaining Blockers",
        "",
    ]
    lines.extend(f"- {item}" for item in payload["remaining_blockers_before_write_pilot"])
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request(
    *,
    phase2_write_mode_proof_path: Path,
    phase2_write_mode_plan_path: Path,
    authorization_grant_path: Path,
    phase1_no_write_pilot_review_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    phase1_no_write_pilot_run_path: Path | None = None,
    request_version: str = REQUEST_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request_payload(
        phase2_write_mode_proof_path=phase2_write_mode_proof_path,
        phase2_write_mode_plan_path=phase2_write_mode_plan_path,
        authorization_grant_path=authorization_grant_path,
        phase1_no_write_pilot_review_path=phase1_no_write_pilot_review_path,
        phase1_no_write_pilot_run_path=phase1_no_write_pilot_run_path,
        request_version=request_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request(payload),
        encoding="utf-8",
    )
    return payload

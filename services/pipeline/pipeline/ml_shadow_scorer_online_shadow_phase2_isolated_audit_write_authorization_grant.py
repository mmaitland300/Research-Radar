"""Grant Phase 2 isolated audit write-pilot authorization for ml-shadow-scorer-v1.

This paperwork-only module validates the Phase 2 write authorization request,
write-mode proof, write-mode plan, and prior execution grant, then writes a
bounded owner grant artifact. It does not run the pilot or runtime, enable
feature flags, create shadow-run files, write databases, touch API-visible
tables, or change production/default ranking behavior.
"""

from __future__ import annotations

from copy import deepcopy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_execution_authorization_grant import (
    ARTIFACT_TYPE as EXECUTION_GRANT_ARTIFACT_TYPE,
    GRANT_VERSION as EXECUTION_GRANT_VERSION,
    OWNER,
    REVIEW_BY,
)
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request import (
    ARTIFACT_TYPE as REQUEST_ARTIFACT_TYPE,
    RECOMMENDED_NEXT_STAGE as REQUEST_NEXT_STAGE,
    REQUEST_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan import (
    ARTIFACT_TYPE as PLAN_ARTIFACT_TYPE,
    PHASE2_POLICY,
    PLAN_VERSION_PHASE2,
    PRIMARY_TARGET_ROOT,
)
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof import (
    ARTIFACT_TYPE as PROOF_ARTIFACT_TYPE,
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

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant"
GRANT_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1"
RECOMMENDED_NEXT_STAGE = "run_online_shadow_phase2_isolated_audit_write_pilot_v1"

PHASE2_WRITE_AUTHORIZATION_SCOPE = "bounded_non_prod_phase2_isolated_audit_write_pilot_only"
PHASE2_WRITES_AUTHORIZATION_SCOPE = "isolated_audit_shadow_artifacts_only"
APPROVED_WRITE_TARGET_TYPE = "isolated_audit_shadow_artifacts"

ALLOWED_FILES = (
    "manifest.json",
    "shadow_rows.jsonl",
    "observability.json",
    "write_counts.json",
)

MINIMUM_FORBIDDEN_WRITE_TARGETS = (
    "isolated_audit_shadow_tables",
    "ranking_runs",
    "paper_scores",
    "embeddings",
    "labels",
    "scorer_artifacts",
    "production_config",
    "production_default_pins",
    "api_visible_tables",
)

WRITE_MODE_FORBIDDEN_TARGETS = (*MINIMUM_FORBIDDEN_WRITE_TARGETS, "user_visible_paths")

CAVEATS = (
    "Grant artifact only; it does not run the pilot or write shadow files.",
    f"The future pilot may write only isolated audit artifacts under {PRIMARY_TARGET_ROOT}<pilot_run_id>/.",
    "phase2_writes_authorized true means bounded isolated audit file-tree writes only, not production write access.",
    "No DB writes or DDL are authorized.",
    "Online shadow execution remains globally disabled.",
    "Production/default/API/user-visible behavior remains unchanged.",
    "Production readiness remains separate.",
)


class MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            f"Expected JSON object in {path}"
        )
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _require_true(name: str, observed: Any) -> None:
    _require_equal(name, observed, True)


def _require_false(name: str, observed: Any) -> None:
    _require_equal(name, observed, False)


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


def _validate_identity(payload: Mapping[str, Any], metadata: Mapping[str, Any], *, label: str) -> None:
    for field, expected in _identity_fields().items():
        observed = metadata.get(field)
        if observed is None and label == "prior execution grant":
            observed = _get(payload, f"grant_scope.{field}")
        if observed is not None:
            _require_equal(f"{label} identity {field}", observed, expected)


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            f"Input {name} does not exist: {path}"
        )
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            f"{label} must be a non-empty list"
        )
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
                f"{label}[{index}] must be an object"
            )
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
                f"{label}[{index}].name missing"
            )
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
                f"{label}[{index}].sha256 missing"
            )
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
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


def _verify_optional_metadata_chains(
    metadata: Mapping[str, Any],
    *,
    repo_root: Path,
    label: str,
) -> dict[str, Any]:
    verified: dict[str, Any] = {}
    for key, value in metadata.items():
        if not key.startswith("verified_"):
            continue
        if isinstance(value, list):
            verified[key] = _verify_recorded_records(
                value,
                repo_root=repo_root,
                label=f"{label} metadata.{key}",
                required=False,
            )
        elif isinstance(value, Mapping):
            nested: dict[str, Any] = {}
            for nested_key, nested_value in value.items():
                if isinstance(nested_value, list):
                    nested[nested_key] = _verify_recorded_records(
                        nested_value,
                        repo_root=repo_root,
                        label=f"{label} metadata.{key}.{nested_key}",
                        required=False,
                    )
            verified[key] = nested
    return verified


def _validate_request(request: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(request, name="phase2-write-authorization-request")
    _require_equal("request metadata.artifact_type", metadata.get("artifact_type"), REQUEST_ARTIFACT_TYPE)
    _require_equal("request metadata.request_version", metadata.get("request_version"), REQUEST_VERSION)
    _validate_identity(request, metadata, label="request")
    required = {
        "phase2_isolated_audit_write_authorization_requested": True,
        "phase2_isolated_audit_write_authorization_granted": False,
        "phase2_write_pilot_authorized": False,
        "phase2_writes_authorized": False,
        "phase2_write_mode_proof_passed": True,
        "missing_phase2_write_mode_isolation_proof": False,
        "missing_phase2_isolated_audit_write_pilot_authorization": True,
        "recommended_next_stage": REQUEST_NEXT_STAGE,
        "online_shadow_execution_enabled": False,
        "writes_performed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "proof_summary.proof_passed": True,
        "proof_summary.joined_candidate_count": 528,
    }
    for path, expected in required.items():
        _require_equal(f"request {path}", _get(request, path), expected)
    allowed = _get(request, "requested_grant_scope.allowed_write_targets")
    _require_equal("request requested_grant_scope.allowed_write_targets", allowed, [APPROVED_WRITE_TARGET_TYPE])
    _require_equal("request requested_grant_scope.write_target_root", _get(request, "requested_grant_scope.write_target_root"), PRIMARY_TARGET_ROOT)
    forbidden = _get(request, "requested_grant_scope.forbidden_write_targets")
    if not isinstance(forbidden, list):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request requested_grant_scope.forbidden_write_targets must be a list"
        )
    missing_forbidden = [target for target in MINIMUM_FORBIDDEN_WRITE_TARGETS if target not in forbidden]
    if missing_forbidden:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request requested_grant_scope.forbidden_write_targets missing: " + ", ".join(missing_forbidden)
        )
    proof_files = _get(request, "proof_summary.isolated_file_writes.files_written")
    if not isinstance(proof_files, list) or not proof_files:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request proof_summary must include file hashes/counts"
        )
    write_counts = _get(request, "proof_summary.write_counts_by_isolated_target")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request proof_summary.write_counts_by_isolated_target must be an object"
        )
    return metadata


def _validate_proof(proof: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(proof, name="phase2-write-mode-proof")
    _require_equal("proof metadata.artifact_type", metadata.get("artifact_type"), PROOF_ARTIFACT_TYPE)
    _require_equal("proof metadata.proof_version", metadata.get("proof_version"), PROOF_VERSION)
    _validate_identity(proof, metadata, label="proof")
    required = {
        "phase2_write_mode_proof_executed": True,
        "phase2_write_mode_proof_passed": True,
        "missing_phase2_write_mode_isolation_proof": False,
        "write_count_verification.forbidden_targets_zero": True,
        "write_count_verification.write_counts_by_isolated_target.isolated_audit_shadow_tables": 0,
        "runtime_writes_performed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
    }
    for path, expected in required.items():
        _require_equal(f"proof {path}", _get(proof, path), expected)
    artifact_writes = _get(proof, "write_count_verification.write_counts_by_isolated_target.isolated_audit_shadow_artifacts")
    if not isinstance(artifact_writes, int) or artifact_writes <= 0:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "proof isolated_audit_shadow_artifacts write count must be positive"
        )
    write_counts = _get(proof, "write_count_verification.write_counts_by_isolated_target")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "proof write_count_verification.write_counts_by_isolated_target must be an object"
        )
    forbidden_nonzero = {
        key: value
        for key, value in write_counts.items()
        if key != APPROVED_WRITE_TARGET_TYPE and value != 0
    }
    if forbidden_nonzero:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            f"proof forbidden write targets must remain zero: {forbidden_nonzero}"
        )
    return metadata


def _validate_plan(plan: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(plan, name="phase2-write-mode-plan")
    _require_equal("plan metadata.artifact_type", metadata.get("artifact_type"), PLAN_ARTIFACT_TYPE)
    _require_equal("plan metadata.plan_version", metadata.get("plan_version"), PLAN_VERSION_PHASE2)
    _validate_identity(plan, metadata, label="plan")
    required = {
        "phase2_isolated_audit_write_mode_plan_defined": True,
        "isolated_write_target.root_path": PRIMARY_TARGET_ROOT,
        "phase2_writes_authorized": False,
        "writes_allowed": False,
    }
    for path, expected in required.items():
        _require_equal(f"plan {path}", _get(plan, path), expected)
    return metadata


def _validate_execution_grant(grant: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant, name="prior execution grant")
    _require_equal("prior grant metadata.artifact_type", metadata.get("artifact_type"), EXECUTION_GRANT_ARTIFACT_TYPE)
    _require_equal("prior grant metadata.grant_version", metadata.get("grant_version"), EXECUTION_GRANT_VERSION)
    _validate_identity(grant, metadata, label="prior execution grant")
    required = {
        "authorization_granted": True,
        "grant_decision.decision": "granted",
        "grant_decision.owner": OWNER,
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "runtime_execution_authorized": True,
        "shadow_scoring_allowed": True,
        "write_mode_policy.phase_2": PHASE2_POLICY,
        "write_mode_policy.phase_2_requires_separate_authorization": True,
    }
    for path, expected in required.items():
        _require_equal(f"prior grant {path}", _get(grant, path), expected)
    if not isinstance(grant.get("required_observability"), Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "prior grant required_observability must be an object"
        )
    if not isinstance(grant.get("rollback_disable_policy"), Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "prior grant rollback_disable_policy must be an object"
        )
    if not isinstance(grant.get("revocation_policy"), Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "prior grant revocation_policy must be an object"
        )
    return metadata


def _copy_proof_summary(request: Mapping[str, Any]) -> dict[str, Any]:
    summary = request.get("proof_summary")
    if not isinstance(summary, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request proof_summary must be an object"
        )
    return deepcopy(dict(summary))


def _basis_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    return _input_record(name, path, repo_root=repo_root)


def _phase2_authorization_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    scope = request.get("requested_grant_scope")
    if not isinstance(scope, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request requested_grant_scope must be an object"
        )
    return {
        "authorization_scope": PHASE2_WRITE_AUTHORIZATION_SCOPE,
        "environments": "non-prod pilot only",
        "approved_write_target_root": PRIMARY_TARGET_ROOT,
        "approved_write_target_type": APPROVED_WRITE_TARGET_TYPE,
        "pilot_run_id_rules": deepcopy(scope.get("pilot_run_id_rules")),
        "allowed_files": list(ALLOWED_FILES),
        "initial_ranking_run_ids": [RANKING_RUN_ID],
        "one_bounded_pilot_run_at_a_time": True,
        "manual_or_scheduled_jobs_only": True,
        "no_fleet_wide_enable": True,
        "feature_flag": FEATURE_FLAG,
        "feature_flag_default_off_elsewhere": True,
        "same_approved_identity_and_528_row_coverage_required": True,
        "cleanup_required": True,
        "cleanup_scope": "delete/archive only <pilot_run_id> subdirectory",
    }


def _blockers_from_request(request: Mapping[str, Any]) -> dict[str, Any]:
    request_blockers = request.get("shadow_and_production_blockers")
    if not isinstance(request_blockers, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteAuthorizationGrantError(
            "request shadow_and_production_blockers must be an object"
        )
    blockers = {
        key: value
        for key, value in dict(request_blockers).items()
        if key not in {"blockers_changed_by_grant", "blockers_changed_by_request", "blockers_unchanged_by_request"}
    }
    blockers.update(
        {
            "missing_online_shadow_execution_authorization": False,
            "missing_phase2_isolated_audit_write_pilot_authorization": False,
            "missing_phase2_write_mode_isolation_proof": False,
            "phase2_write_pilot_authorized": True,
            "phase2_writes_authorized": True,
            "online_shadow_execution_enabled": False,
            "missing_production_readiness_authorization": True,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "runtime_execution_authorized": True,
            "shadow_scoring_allowed": True,
            "authorization_scope": PHASE2_WRITE_AUTHORIZATION_SCOPE,
            "blockers_changed_by_grant": [
                "missing_phase2_isolated_audit_write_pilot_authorization",
                "phase2_write_pilot_authorized",
                "phase2_writes_authorized",
            ],
        }
    )
    return blockers


def build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant_payload(
    *,
    phase2_write_authorization_request_path: Path,
    phase2_write_mode_proof_path: Path,
    phase2_write_mode_plan_path: Path,
    execution_authorization_grant_path: Path,
    grant_version: str = GRANT_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    request_path = Path(phase2_write_authorization_request_path).resolve()
    proof_path = Path(phase2_write_mode_proof_path).resolve()
    plan_path = Path(phase2_write_mode_plan_path).resolve()
    execution_grant_path = Path(execution_authorization_grant_path).resolve()

    request = _load_json_object(request_path)
    proof = _load_json_object(proof_path)
    plan = _load_json_object(plan_path)
    execution_grant = _load_json_object(execution_grant_path)

    request_metadata = _validate_request(request)
    proof_metadata = _validate_proof(proof)
    plan_metadata = _validate_plan(plan)
    execution_grant_metadata = _validate_execution_grant(execution_grant)

    verified_request_inputs = _verify_recorded_records(
        request_metadata.get("inputs"),
        repo_root=root,
        label="request metadata.inputs",
    )
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
    verified_execution_grant_inputs = _verify_recorded_records(
        execution_grant_metadata.get("inputs"),
        repo_root=root,
        label="prior grant metadata.inputs",
    )
    generated = generated_at or _now_iso_z()
    source_artifacts = {
        "phase2_write_authorization_request": _basis_record(
            "phase2_write_authorization_request",
            request_path,
            repo_root=root,
        ),
        "phase2_write_mode_proof": _basis_record("phase2_write_mode_proof", proof_path, repo_root=root),
        "phase2_write_mode_plan": _basis_record("phase2_write_mode_plan", plan_path, repo_root=root),
        "prior_execution_authorization_grant": _basis_record(
            "prior_execution_authorization_grant",
            execution_grant_path,
            repo_root=root,
        ),
    }
    proof_summary = _copy_proof_summary(request)
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "grant_version": grant_version,
            "generated_at": generated,
            "inputs": list(source_artifacts.values()),
            "source_request_version": request_metadata.get("request_version"),
            "source_proof_version": proof_metadata.get("proof_version"),
            "source_plan_version": plan_metadata.get("plan_version"),
            "source_execution_grant_version": execution_grant_metadata.get("grant_version"),
            "verified_request_inputs": verified_request_inputs,
            "verified_request_chains": _verify_optional_metadata_chains(
                request_metadata,
                repo_root=root,
                label="request",
            ),
            "verified_proof_inputs": verified_proof_inputs,
            "verified_proof_chains": _verify_optional_metadata_chains(
                proof_metadata,
                repo_root=root,
                label="proof",
            ),
            "verified_plan_inputs": verified_plan_inputs,
            "verified_execution_grant_inputs": verified_execution_grant_inputs,
            "verified_execution_grant_chains": _verify_optional_metadata_chains(
                execution_grant_metadata,
                repo_root=root,
                label="prior grant",
            ),
            **_identity_fields(),
        },
        "grant_decision": {
            "decision": "granted",
            "owner": OWNER,
            "review_by": REVIEW_BY,
            "expiry_date": REVIEW_BY,
            "granted_at": generated,
        },
        "phase2_isolated_audit_write_authorization_granted": True,
        "phase2_write_pilot_authorized": True,
        "phase2_write_pilot_authorization_scope": PHASE2_WRITE_AUTHORIZATION_SCOPE,
        "phase2_writes_authorized": True,
        "phase2_writes_authorization_scope": PHASE2_WRITES_AUTHORIZATION_SCOPE,
        "missing_phase2_isolated_audit_write_pilot_authorization": False,
        "phase2_write_mode_proof_executed": True,
        "phase2_write_mode_proof_passed": True,
        "missing_phase2_write_mode_isolation_proof": False,
        "online_shadow_execution_enabled": False,
        "runtime_execution_authorized": True,
        "shadow_scoring_allowed": True,
        "writes_performed": False,
        "runtime_writes_performed": False,
        "isolated_artifact_tree_writes_performed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "phase2_write_pilot_authorization": _phase2_authorization_from_request(request),
        "required_observability": deepcopy(execution_grant["required_observability"]),
        "rollback_disable_policy": {
            "flag_off_first": True,
            "disable_switch": f"{FEATURE_FLAG}=off",
            "stop_pilot_jobs": True,
            "cleanup_scope": "delete/archive only <pilot_run_id> subdirectory",
            "never_delete_phase2_proof_root": True,
            "production_ranking_unchanged": True,
            "inherited_prior_execution_grant_policy": deepcopy(execution_grant["rollback_disable_policy"]),
        },
        "revocation_policy": {
            "review_by": REVIEW_BY,
            "flag_off_first": True,
            "revoke_by_superseding_grant": True,
            "revoke_by_denied_phase2_authorization": True,
            "inherited_prior_execution_grant_policy": deepcopy(execution_grant["revocation_policy"]),
        },
        "write_mode_boundaries": {
            "allowed_write_targets": [APPROVED_WRITE_TARGET_TYPE],
            "forbidden_write_targets": list(WRITE_MODE_FORBIDDEN_TARGETS),
            "db_writes_allowed": False,
            "db_ddl_allowed": False,
            "production_api_web_changes_allowed": False,
            "production_default_changes_allowed": False,
        },
        "basis_artifacts": source_artifacts,
        "proof_summary": proof_summary,
        "requested_scope_summary": deepcopy(request["requested_grant_scope"]),
        "shadow_and_production_blockers": _blockers_from_request(request),
        "consumer_guidance": {
            "bounded_non_prod_only": "This grant authorizes only the bounded non-prod isolated artifact-tree write pilot.",
            "scope_narrowing": (
                "Grant authorization_scope narrows the request's bounded_non_prod_pilot_only to Phase 2 isolated audit write pilot only."
            ),
            "phase2_writes_authorized_meaning": (
                "phase2_writes_authorized true means bounded isolated audit file-tree writes for a granted pilot only; "
                "it is not DB, production, API, or global shadow write authorization."
            ),
            "global_online_shadow_execution": "It does not enable global online shadow execution.",
            "forbidden_production_paths": (
                "It does not authorize DB writes, production tables, API/web behavior, production default, or production readiness."
            ),
            "future_pilot_runner_requirements": (
                "The future pilot runner must enforce write target root, pilot_run_id safety, and forbidden write counts."
            ),
            "production_readiness": "Production readiness remains a separate chain.",
        },
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    decision = payload["grant_decision"]
    auth = payload["phase2_write_pilot_authorization"]
    boundaries = payload["write_mode_boundaries"]
    lines = [
        "# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write Authorization Grant "
        f"({metadata['grant_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact grants one bounded non-production Phase 2 isolated audit write pilot. It does not run the pilot, create shadow-run files, enable the feature flag globally, authorize DB writes, or change production behavior.",
        "",
        f"- Decision: `{decision['decision']}`",
        f"- Owner: {decision['owner']}",
        f"- Review by: {decision['review_by']}",
        f"- Expiry date: {decision['expiry_date']}",
        f"- Phase 2 write pilot authorized: {payload['phase2_write_pilot_authorized']}",
        f"- Phase 2 writes authorized: {payload['phase2_writes_authorized']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Authorized Pilot Scope",
        "",
        f"- Authorization scope: `{auth['authorization_scope']}`",
        f"- Environment: {auth['environments']}",
        f"- Write target root: `{auth['approved_write_target_root']}`",
        f"- Write target type: `{auth['approved_write_target_type']}`",
        f"- Allowed files: {', '.join(f'`{item}`' for item in auth['allowed_files'])}",
        f"- Feature flag: `{auth['feature_flag']}`",
        "",
        "## Write Boundaries",
        "",
        f"- Allowed targets: {', '.join(f'`{item}`' for item in boundaries['allowed_write_targets'])}",
        f"- DB writes allowed: {boundaries['db_writes_allowed']}",
        f"- DB DDL allowed: {boundaries['db_ddl_allowed']}",
        f"- Production/API/web changes allowed: {boundaries['production_api_web_changes_allowed']}",
        f"- Production default changes allowed: {boundaries['production_default_changes_allowed']}",
        "",
        "## Rollback",
        "",
        f"- Disable switch: `{payload['rollback_disable_policy']['disable_switch']}`",
        f"- Cleanup scope: {payload['rollback_disable_policy']['cleanup_scope']}",
        f"- Never delete phase2-proof root: {payload['rollback_disable_policy']['never_delete_phase2_proof_root']}",
        "",
        "## Caveats",
        "",
    ]
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant(
    *,
    phase2_write_authorization_request_path: Path,
    phase2_write_mode_proof_path: Path,
    phase2_write_mode_plan_path: Path,
    execution_authorization_grant_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    grant_version: str = GRANT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant_payload(
        phase2_write_authorization_request_path=phase2_write_authorization_request_path,
        phase2_write_mode_proof_path=phase2_write_mode_proof_path,
        phase2_write_mode_plan_path=phase2_write_mode_plan_path,
        execution_authorization_grant_path=execution_authorization_grant_path,
        grant_version=grant_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant(payload),
        encoding="utf-8",
    )
    return payload

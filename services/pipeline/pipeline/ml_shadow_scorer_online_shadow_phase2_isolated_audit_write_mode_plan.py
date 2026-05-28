"""Phase 2 isolated audit write-mode plan for ml-shadow-scorer-v1.

This module records the write-mode contract that must exist before any future
Phase 2 proof can write isolated audit shadow artifacts. It reads committed
JSON audit artifacts only. It does not run the runtime, create the target
shadow-runs tree, persist shadow rows, create database tables, enable feature
flags, authorize Phase 2 writes, or change production/default/API behavior.
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
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review import (
    ACCEPTED_NEXT_STAGE as REVIEW_ACCEPTED_NEXT_STAGE,
    ARTIFACT_TYPE as REVIEW_ARTIFACT_TYPE,
    REVIEW_VERSION,
)
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_run import RUN_VERSION
from pipeline.ml_shadow_scorer_online_shadow_phase1_no_write_pilot_plan import PLAN_VERSION
from pipeline.ml_shadow_scorer_online_shadow_policy import (
    ARTIFACT_TYPE as POLICY_ARTIFACT_TYPE,
    POLICY_VERSION,
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

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan"
PLAN_VERSION_PHASE2 = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1"
RECOMMENDED_NEXT_STAGE = "implement_online_shadow_phase2_isolated_audit_write_mode_proof_v1"

AUTHORIZATION_SCOPE = "bounded_non_prod_pilot_only"
PRIMARY_TARGET_ROOT = "docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/"
PRIMARY_TARGET_RUN_EXAMPLE = "docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/<pilot_run_id>/"
PHASE2_POLICY = "isolated_audit_only_writes_after_phase1_and_write_mode_proof"

REQUIRED_POLICY_ALLOWED_FIELDS = (
    "run_id",
    "scorer_id",
    "scorer_version",
    "formula_id",
    "input_hashes",
    "candidate_pool_work_set_sha256",
    "family",
    "component_coverage",
    "generated_at",
    "snapshot_identifiers",
)

WRITE_COUNT_TARGETS = (
    "ranking_runs",
    "paper_scores",
    "embeddings",
    "labels",
    "scorer_artifacts",
    "production_config",
    "production_default_pins",
    "api_visible_tables",
    "isolated_audit_shadow_artifacts",
    "isolated_audit_shadow_tables",
)

CAVEATS = (
    "Plan only; no writes, no directory creation with data, no runtime persistence, and no feature flag enablement.",
    "Does not authorize Phase 2 writes or clear missing_phase2_write_mode_isolation_proof.",
    "Does not enable online shadow globally or change production/API/user-visible ranking.",
    "Production readiness remains separate.",
    "Primary target is audit file tree only; DB table path is deferred.",
)


class MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _reject_if_explicit_true(name: str, value: Any) -> None:
    if value is True:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"{name} must not be explicitly true")


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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"{label} must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"{label}[{index}] must be object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"{label}[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(f"{label}[{index}].sha256 missing")
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
                f"{label} input {name} missing on disk: {recorded_path}"
            )
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
                f"{label} input {name} sha256 mismatch: recorded {recorded_sha}"
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


def _validate_review(review: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(review, name="phase1-no-write-pilot-review")
    _require_equal("review metadata.artifact_type", metadata.get("artifact_type"), REVIEW_ARTIFACT_TYPE)
    _require_equal("review metadata.review_version", metadata.get("review_version"), REVIEW_VERSION)
    _validate_metadata_identity(metadata, label="review")
    required = {
        "phase1_no_write_pilot_review_executed": True,
        "review_decision.phase1_no_write_pilot_result_accepted": True,
        "review_decision.decision": "accepted",
        "review_scope.accepted_next_stage": REVIEW_ACCEPTED_NEXT_STAGE,
        "phase1_no_write_pilot_executed": True,
        "writes_performed": False,
        "online_shadow_execution_enabled": False,
        "phase1_no_write_pilot_result_accepted": True,
        "missing_production_readiness_authorization": True,
    }
    for path, expected in required.items():
        _require_equal(f"review {path}", _get(review, path), expected)
    _reject_if_explicit_true("review phase2_writes_authorized", review.get("phase2_writes_authorized"))
    _reject_if_explicit_true(
        "review shadow_and_production_blockers.phase2_writes_authorized",
        _get(review, "shadow_and_production_blockers.phase2_writes_authorized"),
    )
    return metadata


def _validate_grant(grant: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(grant, name="authorization-grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    _validate_metadata_identity(metadata, label="grant")
    required = {
        "authorization_granted": True,
        "grant_decision.decision": "granted",
        "grant_decision.owner": OWNER,
        "grant_decision.review_by": REVIEW_BY,
        "grant_decision.expiry_date": REVIEW_BY,
        "write_mode_policy.phase_1": "no_writes",
        "write_mode_policy.phase_1_writes_allowed": False,
        "write_mode_policy.phase_2": PHASE2_POLICY,
        "write_mode_policy.phase_2_requires_separate_authorization": True,
        "missing_online_shadow_execution_authorization": False,
        "missing_production_readiness_authorization": True,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "grant_scope.ranking_run_id": RANKING_RUN_ID,
        "grant_scope.family": FAMILY,
        "grant_scope.candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "grant_scope.formula_id": FORMULA_ID,
        "grant_scope.scorer_id": SCORER_ID,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)
    _reject_if_explicit_true("grant phase2_writes_authorized", grant.get("phase2_writes_authorized"))
    return metadata


def _validate_policy(policy: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(policy, name="online-shadow-policy")
    _require_equal("policy metadata.artifact_type", metadata.get("artifact_type"), POLICY_ARTIFACT_TYPE)
    _require_equal("policy metadata.policy_version", metadata.get("policy_version"), POLICY_VERSION)
    _require_equal("policy online_shadow_execution_policy_defined", policy.get("online_shadow_execution_policy_defined"), True)
    allowed = policy.get("allowed_write_scope")
    if not isinstance(allowed, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            "policy allowed_write_scope must be an object"
        )
    targets = allowed.get("targets")
    if not isinstance(targets, list) or not targets:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            "policy allowed_write_scope.targets must be a non-empty list"
        )
    required_fields = allowed.get("required_fields")
    if not isinstance(required_fields, list) or not required_fields:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            "policy allowed_write_scope.required_fields must be a non-empty list"
        )
    missing_fields = [field for field in REQUIRED_POLICY_ALLOWED_FIELDS if field not in required_fields]
    if missing_fields:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            f"policy allowed_write_scope.required_fields missing: {', '.join(missing_fields)}"
        )
    forbidden = policy.get("forbidden_write_scope")
    if not isinstance(forbidden, list) or not forbidden:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError(
            "policy forbidden_write_scope must be a non-empty list"
        )
    _require_equal("policy runtime feature flag", _get(policy, "runtime_isolation_policy.feature_flag"), FEATURE_FLAG)
    _require_equal(
        "policy runtime feature flag default off",
        _get(policy, "runtime_isolation_policy.feature_flag_default_off"),
        True,
    )
    return metadata


def _optional_payload(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return _load_json_object(path)


def build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan_payload(
    *,
    phase1_no_write_pilot_review_path: Path,
    authorization_grant_path: Path,
    online_shadow_policy_path: Path,
    phase1_no_write_pilot_plan_path: Path | None = None,
    phase1_no_write_pilot_run_path: Path | None = None,
    plan_version: str = PLAN_VERSION_PHASE2,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    review_path = Path(phase1_no_write_pilot_review_path).resolve()
    grant_path = Path(authorization_grant_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    optional_plan_path = Path(phase1_no_write_pilot_plan_path).resolve() if phase1_no_write_pilot_plan_path else None
    optional_run_path = Path(phase1_no_write_pilot_run_path).resolve() if phase1_no_write_pilot_run_path else None

    review = _load_json_object(review_path)
    grant = _load_json_object(grant_path)
    policy = _load_json_object(policy_path)
    optional_plan = _optional_payload(optional_plan_path)
    optional_run = _optional_payload(optional_run_path)

    review_metadata = _validate_review(review)
    grant_metadata = _validate_grant(grant)
    policy_metadata = _validate_policy(policy)

    verified_review_inputs = _verify_recorded_records(
        _get(review, "metadata.inputs"),
        repo_root=root,
        label="review metadata.inputs",
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
    verified_policy_inputs = _verify_recorded_records(
        _get(policy, "metadata.inputs"),
        repo_root=root,
        label="policy metadata.inputs",
    )
    verified_optional_plan_inputs: list[dict[str, Any]] = []
    verified_optional_run_inputs: list[dict[str, Any]] = []
    if optional_plan is not None:
        _require_equal("optional phase1 plan metadata.plan_version", _get(optional_plan, "metadata.plan_version"), PLAN_VERSION)
        verified_optional_plan_inputs = _verify_recorded_records(
            _get(optional_plan, "metadata.inputs"),
            repo_root=root,
            label="optional phase1 plan metadata.inputs",
        )
    if optional_run is not None:
        _require_equal("optional phase1 run metadata.run_version", _get(optional_run, "metadata.run_version"), RUN_VERSION)
        verified_optional_run_inputs = _verify_recorded_records(
            _get(optional_run, "metadata.inputs"),
            repo_root=root,
            label="optional phase1 run metadata.inputs",
        )

    policy_allowed = policy["allowed_write_scope"]
    policy_forbidden = policy["forbidden_write_scope"]
    source_artifacts = {
        "phase1_no_write_pilot_review": _input_record(
            "phase1_no_write_pilot_review",
            review_path,
            repo_root=root,
        ),
        "authorization_grant": _input_record("authorization_grant", grant_path, repo_root=root),
        "online_shadow_policy": _input_record("online_shadow_policy", policy_path, repo_root=root),
    }
    if optional_plan_path is not None:
        source_artifacts["phase1_no_write_pilot_plan"] = _input_record(
            "phase1_no_write_pilot_plan",
            optional_plan_path,
            repo_root=root,
        )
    if optional_run_path is not None:
        source_artifacts["phase1_no_write_pilot_run"] = _input_record(
            "phase1_no_write_pilot_run",
            optional_run_path,
            repo_root=root,
        )

    identity = _identity_fields()
    blockers = {
        **dict(review["shadow_and_production_blockers"]),
        "missing_phase1_no_write_pilot_review": False,
        "missing_phase2_write_mode_isolation_proof": True,
        "phase2_writes_authorized": False,
        "online_shadow_execution_enabled": False,
        "phase2_isolated_audit_write_mode_plan_defined": True,
    }
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "plan_version": plan_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": list(source_artifacts.values()),
            "source_review_version": review_metadata.get("review_version"),
            "source_grant_version": grant_metadata.get("grant_version"),
            "source_policy_version": policy_metadata.get("policy_version"),
            "verified_review_inputs": verified_review_inputs,
            "verified_grant_inputs": verified_grant_inputs,
            "verified_grant_request_inputs": verified_grant_request_inputs,
            "verified_grant_input_chain": verified_grant_input_chain,
            "verified_policy_inputs": verified_policy_inputs,
            "verified_optional_plan_inputs": verified_optional_plan_inputs,
            "verified_optional_run_inputs": verified_optional_run_inputs,
            "runtime_feature_flag": FEATURE_FLAG,
            **identity,
        },
        "phase2_isolated_audit_write_mode_plan_defined": True,
        "phase2_isolated_audit_write_mode_plan_executed": False,
        "phase2_writes_authorized": False,
        "phase2_write_mode_proof_executed": False,
        "phase1_no_write_pilot_result_accepted": True,
        "online_shadow_execution_authorized": True,
        "online_shadow_execution_enabled": False,
        "runtime_execution_authorized": True,
        "runtime_execution_authorization_scope": AUTHORIZATION_SCOPE,
        "shadow_scoring_allowed": True,
        "shadow_scoring_allowed_scope": AUTHORIZATION_SCOPE,
        "writes_allowed": False,
        "writes_performed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_production_readiness_authorization": True,
        "phase1_prerequisite_summary": {
            "phase1_review_accepted": True,
            "phase1_review_decision": _get(review, "review_decision.decision"),
            "phase1_no_write_pilot_executed": review.get("phase1_no_write_pilot_executed"),
            "phase1_writes_performed": review.get("writes_performed"),
            "phase2_writes_authorized_by_review": False,
        },
        "grant_and_policy_basis": {
            "grant_owner": _get(grant, "grant_decision.owner"),
            "grant_review_by": _get(grant, "grant_decision.review_by"),
            "grant_expiry_date": _get(grant, "grant_decision.expiry_date"),
            "grant_phase2_policy": _get(grant, "write_mode_policy.phase_2"),
            "grant_phase2_requires_separate_authorization": _get(
                grant,
                "write_mode_policy.phase_2_requires_separate_authorization",
            ),
            "policy_allowed_write_scope": policy_allowed,
            "policy_forbidden_write_scope": policy_forbidden,
        },
        "phase2_scope": {
            "non_prod_only": True,
            "approved_ranking_run_id": RANKING_RUN_ID,
            "approved_family": FAMILY,
            "approved_candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "approved_formula_id": FORMULA_ID,
            "approved_scorer_id": SCORER_ID,
            "feature_flag": FEATURE_FLAG,
            "feature_flag_default_off": True,
            "flag_on_only_in_pilot_env": True,
            "manual_or_scheduled_jobs_only": True,
            "no_fleet_wide_enable": True,
        },
        "isolated_write_target": {
            "primary_target": "isolated audit artifact tree only",
            "root_path": PRIMARY_TARGET_ROOT,
            "pilot_run_subdirectory_pattern": PRIMARY_TARGET_RUN_EXAMPLE,
            "scope": "non-prod pilot environment only",
            "persisted_content": [
                "run manifest",
                "shadow row export",
                "observability",
                "write-count report",
            ],
            "file_formats": ["JSON", "JSONL"],
            "rationale": "Future proof can stay non-prod, avoid DB DDL, and align with the existing audit artifact workflow.",
            "deferred_alternate_not_authorized": {
                "db_namespace_table_prefix": "ml_shadow_scorer_v1_audit_shadow",
                "requires": [
                    "separate proof design",
                    "DDL/migration controls",
                    "explicit Phase 2 authorization before table writes",
                ],
            },
        },
        "schema_and_namespace": {
            "root_path": PRIMARY_TARGET_ROOT,
            "files": {
                "manifest.json": [
                    "run_id",
                    "scorer_id",
                    "scorer_version",
                    "formula_id",
                    "identity_fields",
                    "input_hashes",
                    "generated_at",
                ],
                "shadow_rows.jsonl_or_json": "audit-only shadow scores aligned with runtime output",
                "observability.json": "runtime and write-mode observability",
                "write_counts.json": "per-target write counts",
            },
            "required_fields_on_every_persisted_record": list(policy_allowed["required_fields"]),
            "per_row_shadow_fields_aligned_with_runtime_output": True,
            "label_fields_allowed": False,
            "audit_only_markers_required": True,
            "must_mark_audit_only": True,
            "future_writes_restricted_to_shadow_runs_subtree": True,
            "committed_gate_paths_may_not_be_overwritten": True,
        },
        "allowed_write_contract": {
            "phase2_writes_authorized_now": False,
            "allowed_only_in_future_proof_step": True,
            "allowed_root": PRIMARY_TARGET_ROOT,
            "allowed_targets": ["isolated audit shadow artifacts"],
            "policy_allowed_targets": list(policy_allowed["targets"]),
            "non_prod_only": True,
            "initial_ranking_run_id": RANKING_RUN_ID,
            "manual_or_scheduled_jobs_only": True,
            "no_fleet_wide_enable": True,
        },
        "forbidden_write_contract": {
            "policy_forbidden_write_scope": list(policy_forbidden),
            "explicitly_forbidden": [
                "ranking_runs",
                "production/default pins",
                "paper_scores used for production",
                "API-visible tables",
                "production config",
                "labels",
                "embeddings",
                "scorer artifacts",
                "user-visible paths",
            ],
            "forbid_overwriting_committed_audit_gate_json": True,
            "only_future_new_files_under_shadow_runs_tree": True,
        },
        "preflight_checklist": [
            "Phase 1 review accepted and this plan artifact committed.",
            "Grant still valid by exact review_by string 2026-08-27.",
            "Target directory exists or is created only under shadow-runs/phase2-proof/ for pilot run_id.",
            "No writes configured to forbidden targets.",
            "Feature flag remains default off and may be on only in pilot env for proof/run.",
            "Complete input coverage is present for 528 rows or the entire run is skipped per policy.",
            "Production/API/user-visible baselines unchanged before proof.",
            "Schema and manifest mapping validated before first file write.",
        ],
        "write_count_observability_plan": {
            "extends_phase1_observability": True,
            "write_counts_by_isolated_target_required": True,
            "per_target_counts": list(WRITE_COUNT_TARGETS),
            "only_target_allowed_to_be_positive_in_future_proof": "isolated_audit_shadow_artifacts",
            "isolated_audit_shadow_tables_expected_count": 0,
            "proof_must_record": [
                "writes_performed",
                "write_count",
                "file_count",
                "bytes_written",
                "write_counts_by_isolated_target",
            ],
            "forbidden_targets_must_remain_zero": True,
        },
        "rollback_and_cleanup_policy": {
            "first_disable_switch": f"{FEATURE_FLAG}=off",
            "stop_pilot_jobs": True,
            "reverify_production_ranking_unchanged_with_writes_on_vs_off": True,
            "cleanup_scope": PRIMARY_TARGET_RUN_EXAMPLE,
            "cleanup_action": "delete or archive pilot run subdirectory only",
            "revoke_via": "superseding grant or denied Phase 2 authorization",
            "production_tables_cleanup_required": False,
            "committed_gate_artifact_cleanup_allowed": False,
        },
        "proof_requirements_for_future_step": {
            "future_command": RECOMMENDED_NEXT_STAGE,
            "non_prod_only": True,
            "must_demonstrate": [
                f"Writes occur only under {PRIMARY_TARGET_ROOT}",
                "All forbidden write_counts remain zero.",
                "Observability complete including write_counts_by_isolated_target.",
                "Preflight checklist satisfied.",
                "Rollback drill disables flag, verifies no further writes, and cleans up pilot subdirectory.",
                "production_default_changed == false",
                "user_visible_ranking_changed == false",
                "api_web_changes_allowed == false",
            ],
            "does_not_authorize_production_or_fleet_wide_enablement": True,
            "separate_phase2_write_authorization_required_before_production_adjacent_write_pilot": True,
        },
        "production_separation_note": {
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "production_readiness_authorization_remains_missing": True,
            "phase2_file_tree_proof_is_not_production_readiness": True,
        },
        "out_of_scope": [
            "running Phase 2 proof or write path",
            "creating shadow-runs pilot data",
            "runtime persistence",
            "global feature flag enablement",
            "database tables or schemas",
            "production/default/API/user-visible changes",
            "Phase 2 write authorization",
            "production readiness authorization",
        ],
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    target = payload["isolated_write_target"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Phase 2 Isolated Audit Write-Mode Plan ({metadata['plan_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact defines the future isolated audit-write contract after the accepted Phase 1 no-write pilot. It does not run the write path, create shadow-run data, enable the feature flag, or authorize Phase 2 writes.",
        "",
        f"- Plan defined: {payload['phase2_isolated_audit_write_mode_plan_defined']}",
        f"- Plan executed: {payload['phase2_isolated_audit_write_mode_plan_executed']}",
        f"- Phase 2 writes authorized: {payload['phase2_writes_authorized']}",
        f"- Phase 2 proof executed: {payload['phase2_write_mode_proof_executed']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Primary Target",
        "",
        f"- Target: {target['primary_target']}",
        f"- Root path: `{target['root_path']}`",
        f"- Pilot run path pattern: `{target['pilot_run_subdirectory_pattern']}`",
        "- Deferred DB table path is not authorized by this plan.",
        "",
        "## Allowed Writes",
        "",
        f"- Future writes allowed now: {payload['allowed_write_contract']['phase2_writes_authorized_now']}",
        f"- Future proof root: `{payload['allowed_write_contract']['allowed_root']}`",
        "- Only isolated audit shadow artifacts may be positive in the future proof write counts.",
        "",
        "## Forbidden Writes",
        "",
    ]
    lines.extend(f"- {item}" for item in payload["forbidden_write_contract"]["explicitly_forbidden"])
    lines.extend(
        [
            "",
            "## Proof Requirements",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["proof_requirements_for_future_step"]["must_demonstrate"])
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan(
    *,
    phase1_no_write_pilot_review_path: Path,
    authorization_grant_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    phase1_no_write_pilot_plan_path: Path | None = None,
    phase1_no_write_pilot_run_path: Path | None = None,
    plan_version: str = PLAN_VERSION_PHASE2,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan_payload(
        phase1_no_write_pilot_review_path=phase1_no_write_pilot_review_path,
        authorization_grant_path=authorization_grant_path,
        online_shadow_policy_path=online_shadow_policy_path,
        phase1_no_write_pilot_plan_path=phase1_no_write_pilot_plan_path,
        phase1_no_write_pilot_run_path=phase1_no_write_pilot_run_path,
        plan_version=plan_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan(payload),
        encoding="utf-8",
    )
    return payload

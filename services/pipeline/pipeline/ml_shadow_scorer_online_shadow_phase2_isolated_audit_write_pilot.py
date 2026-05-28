"""Run the authorized Phase 2 isolated audit write pilot.

This is the first bounded write pilot for ml-shadow-scorer-v1. It writes only
audit artifacts under the gitignored shadow-runs tree and records durable file
hashes in the canonical Phase 2 bundle execution section.
"""

from __future__ import annotations

from copy import deepcopy
import json
import os
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline import ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof as proof_module
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof import (
    EXPECTED_POOL_SIZE,
    FEATURE_FLAG,
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError,
)
from pipeline.ml_shadow_scorer_phase_bundle import (
    MLShadowScorerPhaseBundleError,
    PHASE2_WRITE_TARGET_ROOT,
    POST_PILOT_REVIEW_NEXT_STAGE,
    apply_phase2_write_pilot_execution,
    markdown_from_ml_shadow_scorer_phase_bundle,
    verify_ml_shadow_scorer_phase_bundle_payload,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path
from pipeline.shadow_write_path_guards import (
    ISOLATED_AUDIT_SHADOW_ARTIFACTS,
    ShadowWritePathGuardError,
    assert_forbidden_write_target_counts,
    assert_write_path_allowed,
    phase2_proof_root,
    resolve_pilot_directory,
)

ALLOWED_PILOT_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")


class MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(f"Expected JSON object in {path}")
    return payload


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
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _legacy_index_by_role(bundle: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    index = _get(bundle, "metadata.legacy_artifacts_index")
    if not isinstance(index, list):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            "bundle metadata.legacy_artifacts_index must be a list"
        )
    by_role: dict[str, Mapping[str, Any]] = {}
    for record in index:
        if not isinstance(record, Mapping) or not isinstance(record.get("role"), str):
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
                "bundle legacy artifact records must contain role/path/sha256"
            )
        by_role[str(record["role"])] = record
    return by_role


def _resolve_legacy_artifact(bundle: Mapping[str, Any], role: str, *, repo_root: Path) -> Path:
    record = _legacy_index_by_role(bundle).get(role)
    if not isinstance(record, Mapping):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"bundle legacy artifact missing role {role}"
        )
    recorded_path = record.get("path")
    recorded_sha = record.get("sha256")
    if not isinstance(recorded_path, str) or not isinstance(recorded_sha, str):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"bundle legacy artifact {role} missing path or sha256"
        )
    path = Path(recorded_path)
    resolved = path.resolve() if path.is_absolute() else (repo_root / path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"bundle legacy artifact {role} missing on disk: {recorded_path}"
        )
    if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"bundle legacy artifact {role} sha256 mismatch: recorded {recorded_sha}, actual {sha256_file(resolved)}"
        )
    return resolved


def _validate_authorization(bundle: Mapping[str, Any], grant: Mapping[str, Any]) -> None:
    required = {
        "authorization.phase2_write_pilot_authorized": True,
        "authorization.phase2_writes_authorized": True,
        "authorization.phase2_isolated_audit_write_authorization_granted": True,
        "posture.online_shadow_execution_enabled": False,
        "execution.phase2_write_pilot_executed": False,
    }
    for path, expected in required.items():
        _require_equal(f"bundle {path}", _get(bundle, path), expected)
    grant_required = {
        "phase2_write_pilot_authorized": True,
        "phase2_writes_authorized": True,
        "phase2_isolated_audit_write_authorization_granted": True,
        "online_shadow_execution_enabled": False,
        "grant_decision.review_by": "2026-08-27",
        "grant_decision.expiry_date": "2026-08-27",
    }
    for path, expected in grant_required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)


def _verified_current_artifact_record(name: str, path: Path, *, repo_root: Path) -> dict[str, Any]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
        "verification_status": "confirmed",
    }


def _assert_allowed_file_records(files_written: list[dict[str, Any]], *, pilot_dir: Path, repo_root: Path) -> None:
    names = [record.get("relative_path") for record in files_written]
    if names != list(ALLOWED_PILOT_FILES):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            f"pilot files must be exactly {list(ALLOWED_PILOT_FILES)}, got {names}"
        )
    for record in files_written:
        if record.get("write_target") != ISOLATED_AUDIT_SHADOW_ARTIFACTS:
            raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
                "pilot files must target isolated_audit_shadow_artifacts"
            )
        assert_write_path_allowed(pilot_dir / str(record["relative_path"]), repo_root)


def _runtime_sequence_passed(preflight: Mapping[str, Any], pilot: Mapping[str, Any], postflight: Mapping[str, Any]) -> bool:
    return (
        preflight.get("status") == "skipped_runtime_disabled"
        and preflight.get("shadow_row_count") == 0
        and pilot.get("status") == "succeeded_test_only"
        and pilot.get("shadow_row_count") == EXPECTED_POOL_SIZE
        and postflight.get("status") == "skipped_runtime_disabled"
        and postflight.get("shadow_row_count") == 0
    )


def _build_execution_slice(
    *,
    pilot_run_id: str,
    generated_at: str,
    repo_root: Path,
    pilot_dir: Path,
    runtime_rows: list[dict[str, Any]],
    join_summary: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    files_written: list[dict[str, Any]],
    observability: Mapping[str, Any],
    write_counts_payload: Mapping[str, Any],
    pass_fail: Mapping[str, Any],
    environment_restored: bool,
) -> dict[str, Any]:
    forbidden_nonzero = pass_fail.get("forbidden_nonzero_write_counts")
    write_count_verification = {
        **deepcopy(dict(write_counts_payload)),
        "forbidden_targets_zero": not forbidden_nonzero,
        "forbidden_nonzero_write_counts": deepcopy(forbidden_nonzero),
    }
    return {
        "phase2_write_pilot_executed": True,
        "phase2_write_pilot_passed": pass_fail.get("passed") is True,
        "phase2_write_pilot_run": {
            "pilot_run_id": pilot_run_id,
            "status": pilot.get("status"),
            "shadow_row_count": pilot.get("shadow_row_count"),
            "expected_shadow_row_count": EXPECTED_POOL_SIZE,
        },
        "pilot_run_id": pilot_run_id,
        "pilot_run_directory": {
            "root_path": PHASE2_WRITE_TARGET_ROOT,
            "relative_path": f"{PHASE2_WRITE_TARGET_ROOT}{pilot_run_id}/",
            "local_gitignored": True,
        },
        "input_join_summary": deepcopy(dict(join_summary)),
        "isolated_file_writes": {
            "files_written": deepcopy(files_written),
            "file_count": len(files_written),
            "bytes_written": sum(int(record["byte_count"]) for record in files_written),
            "write_target": ISOLATED_AUDIT_SHADOW_ARTIFACTS,
            "pilot_directory_retained_for_inspection": True,
        },
        "write_count_verification": write_count_verification,
        "observability": deepcopy(dict(observability)),
        "pilot_runtime_summary": proof_module._sanitize_runtime_result(pilot),
        "disable_drill": {
            "preflight": proof_module._sanitize_runtime_result(preflight),
            "postflight": proof_module._sanitize_runtime_result(postflight),
            "passed": preflight.get("status") == "skipped_runtime_disabled"
            and postflight.get("status") == "skipped_runtime_disabled",
            "environment_restored": environment_restored,
            "call_order": ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
        },
        "cleanup_performed": False,
        "executed_at": generated_at,
        "runtime_row_count": len(runtime_rows),
        "runtime_writes_performed": pilot.get("writes_performed") is True,
        "isolated_artifact_tree_writes_performed": bool(files_written),
        "production_default_changed": pilot.get("production_default_changed"),
        "user_visible_ranking_changed": pilot.get("user_visible_ranking_changed"),
        "api_web_changes_allowed": False,
        "labels_used_for_scoring": pilot.get("labels_used_for_scoring"),
        "pass_fail_evaluation": deepcopy(dict(pass_fail)),
    }


def run_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_pilot(
    *,
    bundle_path: Path,
    learned_probability_artifact_path: Path,
    second_surface_generalization_audit_path: Path,
    pilot_run_id: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    generated = generated_at or proof_module._now_iso_z()
    run_id = pilot_run_id or proof_module._default_pilot_run_id(generated)
    try:
        pilot_dir = resolve_pilot_directory(root, run_id)
        proof_root = phase2_proof_root(root)
        assert_write_path_allowed(pilot_dir, root)
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(str(exc)) from exc

    bundle_path = Path(bundle_path).resolve()
    bundle = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(bundle, repo_root=root, expect_pilot_executed=False)
    except MLShadowScorerPhaseBundleError as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(str(exc)) from exc

    grant_path = _resolve_legacy_artifact(bundle, "phase2_write_authorization_grant", repo_root=root)
    _resolve_legacy_artifact(bundle, "phase2_write_mode_plan", repo_root=root)
    _resolve_legacy_artifact(bundle, "phase2_write_mode_proof", repo_root=root)
    _resolve_legacy_artifact(bundle, "online_shadow_policy", repo_root=root)
    grant = _load_json_object(grant_path)
    _validate_authorization(bundle, grant)

    learned_path = Path(learned_probability_artifact_path).resolve()
    audit_path = Path(second_surface_generalization_audit_path).resolve()
    learned = _load_json_object(learned_path)
    audit = _load_json_object(audit_path)
    try:
        learned_metadata, learned_rows = proof_module._validate_learned_probability(learned)
        audit_metadata, audit_rows = proof_module._validate_generalization_audit(audit)
        runtime_rows, join_summary = proof_module._build_runtime_rows(
            audit_rows=audit_rows,
            learned_rows=learned_rows,
        )
    except MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(str(exc)) from exc

    source_artifacts = {
        "bundle": _verified_current_artifact_record("bundle", bundle_path, repo_root=root),
        "learned_probability_artifact": _verified_current_artifact_record(
            "learned_probability_artifact",
            learned_path,
            repo_root=root,
        ),
        "second_surface_generalization_audit": _verified_current_artifact_record(
            "second_surface_generalization_audit",
            audit_path,
            repo_root=root,
        ),
    }
    source_artifacts["learned_probability_artifact"]["artifact_version"] = learned_metadata.get("artifact_version")
    source_artifacts["second_surface_generalization_audit"]["artifact_version"] = audit_metadata.get("artifact_version")

    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = proof_module._runtime_call([], flag_value=None)
    pilot = proof_module._runtime_call(runtime_rows, flag_value="true")
    postflight = proof_module._runtime_call([], flag_value=None)
    environment_restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original
    try:
        proof_module._require_disabled_runtime(preflight, label="preflight")
        proof_module._require_disabled_runtime(postflight, label="postflight")
        proof_module._require_pilot_runtime(pilot)
        assert_forbidden_write_target_counts(proof_module._write_count_template(file_count=4))
    except (MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, ShadowWritePathGuardError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(str(exc)) from exc
    if not _runtime_sequence_passed(preflight, pilot, postflight):
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError("runtime drill order failed")

    try:
        files_written, observability, write_counts_payload = proof_module._write_isolated_files(
            pilot_dir=pilot_dir,
            pilot_run_id=run_id,
            generated_at=generated,
            source_artifacts=source_artifacts,
            runtime_rows=runtime_rows,
            pilot_result=pilot,
            preflight_result=preflight,
            postflight_result=postflight,
        )
        assert_forbidden_write_target_counts(write_counts_payload["write_counts_by_isolated_target"])
        _assert_allowed_file_records(files_written, pilot_dir=pilot_dir, repo_root=root)
    except (MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError, ShadowWritePathGuardError) as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(str(exc)) from exc
    if pilot_dir.parent != proof_root:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            "pilot output directory must be the direct child under phase2-proof root"
        )

    cleanup = {
        "cleanup_after_proof": False,
        "cleanup_completed": False,
        "cleanup_target": str(pilot_dir),
        "directory_absent_after_cleanup": not pilot_dir.exists(),
        "phase2_proof_root_remains": proof_root.exists(),
    }
    pass_fail = proof_module._evaluate_pass_fail(
        runtime_rows=runtime_rows,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        files_written=files_written,
        write_counts=write_counts_payload["write_counts_by_isolated_target"],
        cleanup=cleanup,
        cleanup_after_proof=False,
        environment_restored=environment_restored,
    )
    allowed_names = [record["relative_path"] for record in files_written] == list(ALLOWED_PILOT_FILES)
    if not allowed_names:
        pass_fail["checks"]["allowed_file_names"] = False
        pass_fail["failed_checks"].append("allowed_file_names")
        pass_fail["passed"] = False
    pilot_passed = pass_fail.get("passed") is True
    if not pilot_passed:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(
            "phase2 write pilot failed checks: " + ", ".join(pass_fail["failed_checks"])
        )

    execution_slice = _build_execution_slice(
        pilot_run_id=run_id,
        generated_at=generated,
        repo_root=root,
        pilot_dir=pilot_dir,
        runtime_rows=runtime_rows,
        join_summary=join_summary,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        files_written=files_written,
        observability=observability,
        write_counts_payload=write_counts_payload,
        pass_fail=pass_fail,
        environment_restored=environment_restored,
    )
    updated_bundle = apply_phase2_write_pilot_execution(bundle, execution_slice, generated_at=generated)
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(updated_bundle, repo_root=root, expect_pilot_executed=True)
    except MLShadowScorerPhaseBundleError as exc:
        raise MLShadowScorerOnlineShadowPhase2IsolatedAuditWritePilotError(str(exc)) from exc
    if update_bundle:
        bundle_path.write_text(json.dumps(updated_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        markdown_path = bundle_path.with_name("bundle.md")
        markdown_path.write_text(markdown_from_ml_shadow_scorer_phase_bundle(updated_bundle), encoding="utf-8")

    return {
        "pilot_run_id": run_id,
        "phase2_write_pilot_passed": pilot_passed,
        "pilot_run_directory": execution_slice["pilot_run_directory"],
        "execution": execution_slice,
        "bundle": updated_bundle,
        "bundle_updated": update_bundle,
        "recommended_next_stage": updated_bundle["recommended_next_stage"],
        "expected_recommended_next_stage": POST_PILOT_REVIEW_NEXT_STAGE,
    }

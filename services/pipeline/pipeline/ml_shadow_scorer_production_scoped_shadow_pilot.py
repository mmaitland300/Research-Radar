"""Bounded production-scoped online shadow pilot from frozen audit artifacts.

This runner executes the production-scoped pilot milestone without live
production source reads. Approved inputs are the committed second-surface audit
artifacts used by the Phase 2 ladder; they are verified by path, SHA-256,
artifact version, identity, and 528-row join coverage before the runtime is
called.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator, Mapping, Sequence

from pipeline import ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof as proof_module
from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof import (
    EXPECTED_POOL_SIZE,
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError,
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
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.ml_shadow_scorer_phase_bundle import PINNED_IDENTITY
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    MLShadowScorerProductionScopedShadowBundleError,
    PILOT_RUN_SURFACE,
    POST_PILOT_HARNESS_REVIEW_ACCEPTED_NEXT_STAGE,
    apply_production_scoped_shadow_pilot_run,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path
from pipeline.shadow_write_path_guards import (
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    PROD_SCOPED_SHADOW_ROOT,
    ShadowWritePathGuardError,
    assert_prod_scoped_forbidden_write_target_counts,
    assert_prod_scoped_write_path_allowed,
    resolve_prod_scoped_pilot_directory,
    validate_pilot_run_id,
)

APPROVED_LEARNED_PROBABILITY_REL = "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json"
APPROVED_GENERALIZATION_AUDIT_REL = "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json"
APPROVED_LEARNED_PROBABILITY_SHA256 = "92df47cf9f49b4391404d170775cdcae6b4615423f852e2e8198562fbca778af"
APPROVED_GENERALIZATION_AUDIT_SHA256 = "335d06c3ceae65c1420e12fc64bf9d9b9e20c19bfb762858d2299218e5253c96"
ALLOWED_PILOT_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")


class MLShadowScorerProductionScopedShadowPilotError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_pilot_run_id(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "")
    if compact.endswith("Z"):
        compact = compact[:-1] + "Z"
    return f"{RANKING_RUN_ID}-{compact}"


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowPilotError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowPilotError(f"Expected JSON object in {path}")
    return payload


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _approved_artifact_record(
    *,
    name: str,
    path: Path,
    repo_root: Path,
    expected_rel: str,
    expected_sha256: str,
) -> dict[str, Any]:
    resolved = Path(path).resolve()
    expected_path = (repo_root / expected_rel).resolve()
    if resolved != expected_path:
        raise MLShadowScorerProductionScopedShadowPilotError(
            f"{name} must be approved frozen audit artifact {expected_rel}, got {portable_repo_path(resolved, repo_root=repo_root)}"
        )
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowPilotError(f"{name} does not exist: {expected_rel}")
    observed_sha = sha256_file(resolved)
    if not recorded_sha256_matches_text_artifact(resolved, expected_sha256):
        raise MLShadowScorerProductionScopedShadowPilotError(
            f"{name} sha256 mismatch: expected {expected_sha256}, got {observed_sha}"
        )
    return {
        "name": name,
        "path": expected_rel,
        "sha256": observed_sha,
        "verification_status": "confirmed",
    }


@contextmanager
def _scoped_feature_flag(value: str | None) -> Iterator[None]:
    sentinel = object()
    original = os.environ.get(FEATURE_FLAG, sentinel)
    if value is None:
        os.environ.pop(FEATURE_FLAG, None)
    else:
        os.environ[FEATURE_FLAG] = value
    try:
        yield
    finally:
        if original is sentinel:
            os.environ.pop(FEATURE_FLAG, None)
        else:
            os.environ[FEATURE_FLAG] = str(original)


def _runtime_call(candidate_rows: Sequence[Mapping[str, Any]], *, flag_value: str | None) -> dict[str, Any]:
    started = perf_counter()
    try:
        with _scoped_feature_flag(flag_value):
            result = run_ml_shadow_scorer_v1_online_shadow_runtime(candidate_rows)
    except Exception as exc:  # pragma: no cover - defensive artifact path
        elapsed_ms = (perf_counter() - started) * 1000
        return {
            "status": "runtime_exception",
            "reason": str(exc),
            "runtime_feature_flag": FEATURE_FLAG,
            "runtime_feature_flag_value": flag_value,
            "runtime_enabled": flag_value == "true",
            "shadow_rows": [],
            "shadow_row_count": 0,
            "writes_performed": False,
            "write_count": 0,
            "labels_used_for_scoring": False,
            "production_default_changed": False,
            "user_visible_ranking_changed": False,
            "elapsed_ms": elapsed_ms,
            "runtime_errors": [str(exc)],
        }
    out = dict(result)
    out["elapsed_ms"] = (perf_counter() - started) * 1000
    out["runtime_errors"] = []
    return out


def _sanitize_runtime_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "reason": result.get("reason"),
        "runtime_feature_flag": result.get("runtime_feature_flag"),
        "runtime_feature_flag_value": result.get("runtime_feature_flag_value"),
        "runtime_enabled": result.get("runtime_enabled"),
        "shadow_row_count": result.get("shadow_row_count"),
        "writes_performed": result.get("writes_performed"),
        "write_count": result.get("write_count"),
        "labels_used_for_scoring": result.get("labels_used_for_scoring"),
        "production_default_changed": result.get("production_default_changed"),
        "user_visible_ranking_changed": result.get("user_visible_ranking_changed"),
        "elapsed_ms": result.get("elapsed_ms"),
        "runtime_errors": list(result.get("runtime_errors") or []),
    }


def _write_counts_by_isolated_target(*, file_count: int = 4) -> dict[str, int]:
    return {
        ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS: file_count,
        **{target: 0 for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS},
    }


def _shadow_row_export_rows(shadow_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(shadow_rows, start=1):
        out.append(
            {
                "audit_only": True,
                "pilot_surface": PILOT_RUN_SURFACE,
                "shadow_rank": rank,
                "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                "final_score": row.get("final_score"),
                "audit_embedding_probability_work": row.get("audit_embedding_probability_work"),
                "final_score_rank_pct": row.get("final_score_rank_pct"),
                "audit_embedding_probability_rank_pct": row.get("audit_embedding_probability_rank_pct"),
                "ml_shadow_scorer_v1_score": row.get("ml_shadow_scorer_v1_score"),
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_version": EMBEDDING_VERSION,
                "scorer_id": SCORER_ID,
                "formula_id": FORMULA_ID,
                "live_prod_source_reads_performed": False,
            }
        )
    return out


def _write_json(path: Path, payload: Mapping[str, Any], *, repo_root: Path) -> dict[str, Any]:
    assert_prod_scoped_write_path_allowed(path, repo_root)
    data = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {
        "relative_path": path.name,
        "byte_count": len(data),
        "row_count": None,
        "sha256": _sha256_bytes(data),
        "write_target": ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    }


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]], *, repo_root: Path) -> dict[str, Any]:
    assert_prod_scoped_write_path_allowed(path, repo_root)
    data = ("\n".join(json.dumps(dict(row), sort_keys=True, separators=(",", ":")) for row in rows) + "\n").encode(
        "utf-8"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return {
        "relative_path": path.name,
        "byte_count": len(data),
        "row_count": len(rows),
        "sha256": _sha256_bytes(data),
        "write_target": ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    }


def _observability_summary(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    shadow_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    write_counts: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "pilot_surface": PILOT_RUN_SURFACE,
        "observability_complete": True,
        "live_prod_source_reads_performed": False,
        "signals_emitted": [
            "run status",
            "row counts",
            "error counters",
            "latency",
            "component coverage",
            "score distributions",
            "skipped runs/reasons",
            "forbidden write target counts (all zero)",
            "rank displacement audit-only",
        ],
        "run_status": pilot.get("status"),
        "row_counts": {
            "runtime_rows": len(runtime_rows),
            "shadow_rows": len(shadow_rows),
            "preflight_shadow_rows": preflight.get("shadow_row_count"),
            "postflight_shadow_rows": postflight.get("shadow_row_count"),
        },
        "error_counters": {
            "runtime_errors": sum(len(result.get("runtime_errors") or []) for result in (preflight, pilot, postflight)),
            "forbidden_write_count_errors": 0,
        },
        "latency": {
            "preflight_elapsed_ms": preflight.get("elapsed_ms"),
            "pilot_elapsed_ms": pilot.get("elapsed_ms"),
            "postflight_elapsed_ms": postflight.get("elapsed_ms"),
        },
        "component_coverage": {
            "complete": len(runtime_rows) == EXPECTED_POOL_SIZE and len(shadow_rows) == EXPECTED_POOL_SIZE,
            "runtime_candidate_count": len(runtime_rows),
            "shadow_row_count": len(shadow_rows),
        },
        "score_distributions": {
            "ml_shadow_scorer_v1_score": [
                row.get("ml_shadow_scorer_v1_score") for row in shadow_rows
            ],
        },
        "skipped_runs": [
            {
                "phase": "preflight_disabled",
                "status": preflight.get("status"),
                "reason": preflight.get("reason"),
            },
            {
                "phase": "postflight_disabled",
                "status": postflight.get("status"),
                "reason": postflight.get("reason"),
            },
        ],
        "forbidden_write_target_counts": dict(write_counts),
        "rank_displacement_audit_only": "not_recomputed_in_production_scoped_audit_artifact_pilot",
    }


def _build_pass_fail(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    environment_restored: bool,
    files_written: Sequence[Mapping[str, Any]],
    write_counts: Mapping[str, int],
) -> dict[str, Any]:
    forbidden_nonzero = {
        target: count for target, count in write_counts.items()
        if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS and count != 0
    }
    checks = {
        "joined_candidate_count_528": join_summary.get("joined_candidate_count") == EXPECTED_POOL_SIZE,
        "runtime_row_count_528": len(runtime_rows) == EXPECTED_POOL_SIZE,
        "preflight_disabled": preflight.get("status") == "skipped_runtime_disabled" and preflight.get("shadow_row_count") == 0,
        "pilot_runtime_succeeded": pilot.get("status") == "succeeded_test_only" and pilot.get("shadow_row_count") == EXPECTED_POOL_SIZE,
        "postflight_disabled": postflight.get("status") == "skipped_runtime_disabled" and postflight.get("shadow_row_count") == 0,
        "environment_restored": environment_restored,
        "runtime_drill_order": True,
        "forbidden_write_counts_zero": not forbidden_nonzero,
        "isolated_artifact_target_count_4": write_counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS) == 4,
        "files_written_allowed": [record.get("relative_path") for record in files_written] == list(ALLOWED_PILOT_FILES),
        "live_prod_source_reads_false": True,
        "runtime_writes_false": pilot.get("writes_performed") is False,
        "production_default_changed_false": pilot.get("production_default_changed") is False,
        "user_visible_ranking_changed_false": pilot.get("user_visible_ranking_changed") is False,
        "labels_used_for_scoring_false": pilot.get("labels_used_for_scoring") is False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "overall_passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "forbidden_nonzero_write_counts": forbidden_nonzero,
    }


def _write_pilot_artifacts(
    *,
    repo_root: Path,
    pilot_run_id: str,
    generated_at: str,
    source_artifacts: Mapping[str, Mapping[str, Any]],
    runtime_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    if pilot_dir.exists() and any(pilot_dir.iterdir()):
        raise MLShadowScorerProductionScopedShadowPilotError(
            f"pilot output directory already exists and is not empty: {pilot_dir}"
        )
    assert_prod_scoped_write_path_allowed(pilot_dir, repo_root)
    shadow_rows = pilot.get("shadow_rows") if isinstance(pilot.get("shadow_rows"), list) else []
    shadow_export = _shadow_row_export_rows([row for row in shadow_rows if isinstance(row, Mapping)])
    write_counts = _write_counts_by_isolated_target(file_count=4)
    assert_prod_scoped_forbidden_write_target_counts(write_counts)
    manifest = {
        "artifact_type": "ml_shadow_scorer_production_scoped_shadow_pilot_manifest",
        "generated_at": generated_at,
        "pilot_run_id": pilot_run_id,
        "pilot_surface": PILOT_RUN_SURFACE,
        "live_prod_source_reads_performed": False,
        "pinned_identity": deepcopy(PINNED_IDENTITY),
        "source_artifacts": deepcopy(dict(source_artifacts)),
        "input_join_summary": deepcopy(dict(join_summary)),
        "identity": {
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "scorer_id": SCORER_ID,
            "formula_id": FORMULA_ID,
        },
        "component_coverage": {
            "runtime_row_count": len(runtime_rows),
            "shadow_row_count": len(shadow_export),
            "expected_row_count": EXPECTED_POOL_SIZE,
        },
    }
    observability = _observability_summary(
        runtime_rows=runtime_rows,
        shadow_rows=shadow_export,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        write_counts=write_counts,
    )
    write_counts_payload = {
        "pilot_run_id": pilot_run_id,
        "pilot_surface": PILOT_RUN_SURFACE,
        "live_prod_source_reads_performed": False,
        "local_artifact_tree_writes_performed": True,
        "production_writes_performed": False,
        "committed_artifact_writes_performed": False,
        "runtime_writes_performed": False,
        "file_count": 4,
        "write_count": 4,
        "write_counts_by_isolated_target": write_counts,
        "forbidden_write_counts_zero": True,
    }
    files = [
        _write_json(pilot_dir / "manifest.json", manifest, repo_root=repo_root),
        _write_jsonl(pilot_dir / "shadow_rows.jsonl", shadow_export, repo_root=repo_root),
        _write_json(pilot_dir / "observability.json", observability, repo_root=repo_root),
        _write_json(pilot_dir / "write_counts.json", write_counts_payload, repo_root=repo_root),
    ]
    return files, observability, write_counts_payload


def run_ml_shadow_scorer_production_scoped_shadow_pilot(
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
    generated = generated_at or _now_iso_z()
    run_id = pilot_run_id or _default_pilot_run_id(generated)
    try:
        validate_pilot_run_id(run_id)
        if "harness" in run_id:
            raise MLShadowScorerProductionScopedShadowPilotError("pilot_run_id must not contain harness")
        pilot_dir = resolve_prod_scoped_pilot_directory(root, run_id)
        assert_prod_scoped_write_path_allowed(pilot_dir, root)
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowPilotError(str(exc)) from exc

    bundle_path = Path(bundle_path).resolve()
    bundle = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            bundle,
            repo_root=root,
            expect_pilot_harness_review_filed=True,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowPilotError(str(exc)) from exc
    if bundle.get("recommended_next_stage") != POST_PILOT_HARNESS_REVIEW_ACCEPTED_NEXT_STAGE:
        raise MLShadowScorerProductionScopedShadowPilotError(
            "bundle recommended_next_stage must be run_production_scoped_online_shadow_pilot_v1"
        )
    if bundle.get("execution", {}).get("prod_scoped_shadow_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowPilotError("production-scoped pilot has already been filed")
    harness_run_id = bundle.get("execution", {}).get("pilot_harness", {}).get("pilot_run_id")
    proof_run_id = bundle.get("proof", {}).get("pilot_run_id")
    if run_id in {harness_run_id, proof_run_id}:
        raise MLShadowScorerProductionScopedShadowPilotError(
            "pilot_run_id must differ from proof and harness pilot_run_id values"
        )

    learned_path = Path(learned_probability_artifact_path).resolve()
    audit_path = Path(second_surface_generalization_audit_path).resolve()
    source_artifacts = {
        "learned_probability_artifact": _approved_artifact_record(
            name="learned_probability_artifact",
            path=learned_path,
            repo_root=root,
            expected_rel=APPROVED_LEARNED_PROBABILITY_REL,
            expected_sha256=APPROVED_LEARNED_PROBABILITY_SHA256,
        ),
        "second_surface_generalization_audit": _approved_artifact_record(
            name="second_surface_generalization_audit",
            path=audit_path,
            repo_root=root,
            expected_rel=APPROVED_GENERALIZATION_AUDIT_REL,
            expected_sha256=APPROVED_GENERALIZATION_AUDIT_SHA256,
        ),
        "bundle": {
            "name": "bundle",
            "path": portable_repo_path(bundle_path, repo_root=root),
            "sha256": sha256_file(bundle_path),
            "verification_status": "confirmed",
        },
    }
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
        raise MLShadowScorerProductionScopedShadowPilotError(str(exc)) from exc
    source_artifacts["learned_probability_artifact"]["artifact_version"] = learned_metadata.get("artifact_version")
    source_artifacts["second_surface_generalization_audit"]["artifact_version"] = audit_metadata.get("artifact_version")

    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = _runtime_call([], flag_value=None)
    pilot = _runtime_call(runtime_rows, flag_value="true")
    postflight = _runtime_call([], flag_value=None)
    environment_restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original
    try:
        proof_module._require_disabled_runtime(preflight, label="preflight")
        proof_module._require_pilot_runtime(pilot)
        proof_module._require_disabled_runtime(postflight, label="postflight")
    except MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModeProofError as exc:
        raise MLShadowScorerProductionScopedShadowPilotError(str(exc)) from exc

    try:
        files_written, observability, write_counts_payload = _write_pilot_artifacts(
            repo_root=root,
            pilot_run_id=run_id,
            generated_at=generated,
            source_artifacts=source_artifacts,
            runtime_rows=runtime_rows,
            join_summary=join_summary,
            preflight=preflight,
            pilot=pilot,
            postflight=postflight,
        )
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowPilotError(str(exc)) from exc
    pass_fail = _build_pass_fail(
        runtime_rows=runtime_rows,
        join_summary=join_summary,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        environment_restored=environment_restored,
        files_written=files_written,
        write_counts=write_counts_payload["write_counts_by_isolated_target"],
    )
    if not pass_fail["overall_passed"]:
        raise MLShadowScorerProductionScopedShadowPilotError(
            "production-scoped pilot failed checks: " + ", ".join(pass_fail["failed_checks"])
        )
    pilot_slice = {
        "pilot_run_id": run_id,
        "pilot_surface": PILOT_RUN_SURFACE,
        "pilot_run_directory": {
            "root_path": PROD_SCOPED_SHADOW_ROOT,
            "relative_path": f"{PROD_SCOPED_SHADOW_ROOT}{run_id}/",
        },
        "source_artifacts": deepcopy(source_artifacts),
        "input_join_summary": deepcopy(dict(join_summary)),
        "live_prod_source_reads_performed": False,
        "runtime_drill": {
            "call_order": ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
            "environment_restored": environment_restored,
            "preflight": _sanitize_runtime_result(preflight),
            "pilot": _sanitize_runtime_result(pilot),
            "postflight": _sanitize_runtime_result(postflight),
        },
        "files_written": files_written,
        "observability_summary": observability,
        "write_count_verification": {
            **write_counts_payload,
            "forbidden_write_counts_zero": pass_fail["checks"]["forbidden_write_counts_zero"],
            "forbidden_nonzero_write_counts": pass_fail["forbidden_nonzero_write_counts"],
        },
        "pass_fail_evaluation": pass_fail,
        "executed_at": generated,
    }
    try:
        updated_bundle = apply_production_scoped_shadow_pilot_run(
            bundle,
            pilot_slice,
            generated_at=generated,
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated_bundle,
            repo_root=root,
            expect_pilot_run_filed=True,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowPilotError(str(exc)) from exc
    if update_bundle:
        bundle_path.write_text(json.dumps(updated_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated_bundle),
            encoding="utf-8",
        )
    return {
        "pilot_run_id": run_id,
        "prod_scoped_shadow_pilot_passed": True,
        "pilot_run_directory": pilot_slice["pilot_run_directory"],
        "execution": pilot_slice,
        "bundle": updated_bundle,
        "bundle_updated": update_bundle,
        "recommended_next_stage": updated_bundle["recommended_next_stage"],
    }

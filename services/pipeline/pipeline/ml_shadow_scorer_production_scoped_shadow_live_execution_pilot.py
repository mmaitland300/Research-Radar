"""Bounded production-scoped online shadow live execution pilot.

This runner is intentionally narrow: it re-reads the approved production
sources, enables the runtime flag only inside a process-scoped drill, writes
isolated audit artifacts, and advances the bundle only after the rev15 checks
pass.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from time import perf_counter
from typing import Any, Iterator, Mapping, Sequence

from pipeline.ml_shadow_scorer_generalization_second_surface import _database_url_from_env
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
    LIVE_EXECUTION_PILOT_RUN_EXPECTED_FILES,
    LIVE_EXECUTION_PILOT_RUN_ID_PREFIX,
    LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS,
    LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION,
    LIVE_EXECUTION_PILOT_RUN_SURFACE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_EXECUTION_GRANT_NEXT_STAGE,
    apply_production_scoped_shadow_live_execution_pilot_run,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot import (
    APPROVED_SOURCE_TABLES,
    EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
    MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError,
    _assert_live_read_only_database_url,
    _build_live_source_reads,
    _build_runtime_rows_from_live_reads,
    _connect_readonly,
    _load_frozen_audit_embedding_scorer,
    _query_candidate_inputs,
    _query_ranking_run,
    _validate_ranking_run_row,
    _write_counts_by_isolated_target,
    _write_json,
    _write_jsonl,
)
from pipeline.repo_paths import default_repo_root
from pipeline.shadow_write_path_guards import (
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    PROD_SCOPED_SHADOW_ROOT,
    ShadowWritePathGuardError,
    assert_prod_scoped_forbidden_write_target_counts,
    assert_prod_scoped_write_path_allowed,
    resolve_prod_scoped_pilot_directory,
    validate_pilot_run_id,
)

EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT = EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT


class MLShadowScorerProductionScopedShadowLiveExecutionPilotError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_pilot_run_id(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "")
    if compact.endswith("Z"):
        compact = compact[:-1] + "Z"
    return f"{LIVE_EXECUTION_PILOT_RUN_ID_PREFIX}-{RANKING_RUN_ID}-{compact}"


def _sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


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
            "elapsed_ms": (perf_counter() - started) * 1000,
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


def _shadow_row_export_rows(shadow_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rank, row in enumerate(shadow_rows, start=1):
        out.append(
            {
                "audit_only": True,
                "pilot_surface": LIVE_EXECUTION_PILOT_RUN_SURFACE,
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
                "live_prod_source_reads_performed": True,
            }
        )
    return out


def _observability_summary(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    shadow_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    incomplete_coverage: Mapping[str, Any],
    write_counts: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "pilot_surface": LIVE_EXECUTION_PILOT_RUN_SURFACE,
        "observability_complete": True,
        "live_prod_source_reads_performed": True,
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
            "live source read summary",
            "incomplete coverage drill",
        ],
        "run_status": pilot.get("status"),
        "row_counts": {
            "runtime_rows": len(runtime_rows),
            "shadow_rows": len(shadow_rows),
            "preflight_shadow_rows": preflight.get("shadow_row_count"),
            "postflight_shadow_rows": postflight.get("shadow_row_count"),
            "incomplete_coverage_shadow_rows": incomplete_coverage.get("shadow_row_count"),
        },
        "error_counters": {
            "runtime_errors": sum(
                len(result.get("runtime_errors") or [])
                for result in (preflight, pilot, postflight, incomplete_coverage)
            ),
            "forbidden_write_count_errors": 0,
        },
        "latency": {
            "preflight_elapsed_ms": preflight.get("elapsed_ms"),
            "pilot_elapsed_ms": pilot.get("elapsed_ms"),
            "postflight_elapsed_ms": postflight.get("elapsed_ms"),
            "incomplete_coverage_elapsed_ms": incomplete_coverage.get("elapsed_ms"),
        },
        "component_coverage": {
            "complete": len(runtime_rows) == EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT
            and len(shadow_rows) == EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT,
            "runtime_candidate_count": len(runtime_rows),
            "shadow_row_count": len(shadow_rows),
        },
        "score_distributions": {
            "ml_shadow_scorer_v1_score": [row.get("ml_shadow_scorer_v1_score") for row in shadow_rows],
        },
        "skipped_runs": [
            {"phase": "preflight_disabled", "status": preflight.get("status"), "reason": preflight.get("reason")},
            {"phase": "postflight_disabled", "status": postflight.get("status"), "reason": postflight.get("reason")},
            {
                "phase": "incomplete_coverage_drill",
                "status": incomplete_coverage.get("status"),
                "reason": incomplete_coverage.get("reason"),
            },
        ],
        "forbidden_write_target_counts": dict(write_counts),
        "rank_displacement_audit_only": "bounded_live_execution_prod_scoped_pilot",
    }


def _incomplete_coverage_probe() -> dict[str, Any]:
    rows = [
        {
            "canonical_openalex_work_id": "W0000000001",
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "final_score": 0.42,
        }
    ]
    result = _sanitize_runtime_result(_runtime_call(rows, flag_value="true"))
    result["fixture_in_memory_rows"] = True
    result["live_prod_source_reads_performed"] = False
    return result


def _input_provenance(bundle: Mapping[str, Any], live_source_reads: Mapping[str, Any]) -> dict[str, Any]:
    previous = bundle.get("execution", {}).get("live_read_only_pilot_run", {})
    return {
        "previous_live_read_only_pilot_run_id": previous.get("pilot_run_id"),
        "previous_ranking_run_id": RANKING_RUN_ID,
        "ranking_version": live_source_reads.get("ranking_run", {}).get("ranking_version"),
        "reread_approved_production_sources": True,
        "fixture_ranking_version_used": False,
        "fixture_rows_used_for_main_join": False,
        "ranking_identity": {
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
        },
    }


def _live_execution_scope(bundle: Mapping[str, Any]) -> dict[str, Any]:
    authorization = bundle.get("authorization", {})
    return {
        "live_execution_grant_decision": deepcopy(authorization.get("live_execution_grant_decision")),
        "live_execution_granted_scope": deepcopy(authorization.get("live_execution_granted_scope")),
        "bounded_live_execution_pilot_only": True,
        "scope_statement": "bounded live execution pilot only",
        "prod_scoped_shadow_live_execution_authorized": True,
        "prod_scoped_shadow_execution_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "production_recommendation_output_produced": False,
    }


def _write_pilot_artifacts(
    *,
    repo_root: Path,
    pilot_run_id: str,
    generated_at: str,
    live_source_reads: Mapping[str, Any],
    runtime_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
    input_provenance: Mapping[str, Any],
    live_execution_scope: Mapping[str, Any],
    incomplete_coverage: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    if pilot_dir.exists() and any(pilot_dir.iterdir()):
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            f"pilot output directory already exists and is not empty: {pilot_dir}"
        )
    assert_prod_scoped_write_path_allowed(pilot_dir, repo_root)
    shadow_rows = pilot.get("shadow_rows") if isinstance(pilot.get("shadow_rows"), list) else []
    shadow_export = _shadow_row_export_rows([row for row in shadow_rows if isinstance(row, Mapping)])
    write_counts = _write_counts_by_isolated_target(file_count=4)
    assert_prod_scoped_forbidden_write_target_counts(write_counts)
    manifest = {
        "artifact_type": "ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_manifest",
        "generated_at": generated_at,
        "pilot_run_id": pilot_run_id,
        "pilot_surface": LIVE_EXECUTION_PILOT_RUN_SURFACE,
        "live_prod_source_reads_performed": True,
        "pinned_identity": deepcopy(PINNED_IDENTITY),
        "approved_source_tables": list(APPROVED_SOURCE_TABLES),
        "live_source_reads": deepcopy(dict(live_source_reads)),
        "input_join_summary": deepcopy(dict(join_summary)),
        "input_provenance": deepcopy(dict(input_provenance)),
        "live_execution_scope": deepcopy(dict(live_execution_scope)),
        "incomplete_coverage_drill": deepcopy(dict(incomplete_coverage)),
        "identity": {
            "ranking_run_id": RANKING_RUN_ID,
            "ranking_version": LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION,
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
            "expected_row_count": EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT,
        },
    }
    observability = _observability_summary(
        runtime_rows=runtime_rows,
        shadow_rows=shadow_export,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        incomplete_coverage=incomplete_coverage,
        write_counts=write_counts,
    )
    write_counts_payload = {
        "pilot_run_id": pilot_run_id,
        "pilot_surface": LIVE_EXECUTION_PILOT_RUN_SURFACE,
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


def _build_pass_fail(
    *,
    bundle: Mapping[str, Any],
    runtime_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    incomplete_coverage: Mapping[str, Any],
    environment_restored: bool,
    files_written: Sequence[Mapping[str, Any]],
    write_counts: Mapping[str, int],
    live_source_reads: Mapping[str, Any],
    live_execution_scope: Mapping[str, Any],
) -> dict[str, Any]:
    forbidden_nonzero = {
        target: count
        for target, count in write_counts.items()
        if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS and count != 0
    }
    checks = {
        "live_execution_grant_present": isinstance(
            bundle.get("authorization", {}).get("live_execution_grant_decision"),
            Mapping,
        ),
        "joined_candidate_count_528": join_summary.get("joined_candidate_count")
        == EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT,
        "runtime_row_count_528": len(runtime_rows) == EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT,
        "runtime_drill_call_order": True,
        "preflight_postflight_disabled": preflight.get("status") == "skipped_runtime_disabled"
        and preflight.get("shadow_row_count") == 0
        and postflight.get("status") == "skipped_runtime_disabled"
        and postflight.get("shadow_row_count") == 0,
        "pilot_status_succeeded_test_only": pilot.get("status") == "succeeded_test_only"
        and pilot.get("shadow_row_count") == EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT,
        "process_scoped_runtime_flag_only": True,
        "environment_restored": environment_restored,
        "incomplete_coverage_skip_verified": incomplete_coverage.get("status") == "skipped_incomplete_coverage"
        and incomplete_coverage.get("shadow_row_count") == 0
        and incomplete_coverage.get("writes_performed") is False
        and incomplete_coverage.get("live_prod_source_reads_performed") is False,
        "forbidden_write_counts_zero": not forbidden_nonzero,
        "isolated_artifact_count_4": write_counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS) == 4,
        "expected_files_recorded": [record.get("relative_path") for record in files_written]
        == list(LIVE_EXECUTION_PILOT_RUN_EXPECTED_FILES),
        "production_api_user_visible_unchanged": pilot.get("production_default_changed") is False
        and pilot.get("user_visible_ranking_changed") is False
        and live_execution_scope.get("production_default_allowed") is False
        and live_execution_scope.get("api_web_changes_allowed") is False
        and live_execution_scope.get("user_visible_ranking_changed") is False,
        "no_labels_refit_embedding_generation_or_label_ingest": pilot.get("labels_used_for_scoring") is False
        and live_source_reads.get("labels_not_used_for_scoring") is True
        and live_source_reads.get("refit_training_performed") is False
        and live_source_reads.get("embedding_generation_performed") is False
        and live_source_reads.get("label_ingest_performed") is False,
        "global_execution_authorization_false": live_execution_scope.get("prod_scoped_shadow_execution_authorized")
        is False
        and bundle.get("authorization", {}).get("prod_scoped_shadow_execution_authorized") is False,
    }
    missing = [name for name in LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS if name not in checks]
    if missing:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "missing live execution pass/fail checks: " + ", ".join(missing)
        )
    failed = [name for name in LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS if not checks.get(name)]
    return {
        "overall_passed": not failed,
        "checks": {name: checks[name] for name in LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS},
        "failed_checks": failed,
        "forbidden_nonzero_write_counts": forbidden_nonzero,
    }


def run_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot(
    *,
    bundle_path: Path,
    database_url: str | None = None,
    pilot_run_id: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
    confirm_live_execution_pilot: bool = False,
    confirm_live_read_only_prod_source_reads: bool = False,
) -> dict[str, Any]:
    if not confirm_live_execution_pilot:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "--confirm-live-execution-pilot is required before bounded live execution pilot"
        )
    if not confirm_live_read_only_prod_source_reads:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "--confirm-live-read-only-prod-source-reads is required before live production source reads"
        )

    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    generated = generated_at or _now_iso_z()
    run_id = pilot_run_id or _default_pilot_run_id(generated)
    try:
        validate_pilot_run_id(run_id)
        if not run_id.startswith(f"{LIVE_EXECUTION_PILOT_RUN_ID_PREFIX}-"):
            raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
                "pilot_run_id must use prod-live-exec prefix"
            )
        if "harness" in run_id:
            raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError("pilot_run_id must not contain harness")
        pilot_dir = resolve_prod_scoped_pilot_directory(root, run_id)
        assert_prod_scoped_write_path_allowed(pilot_dir, root)
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(str(exc)) from exc

    bundle_path = Path(bundle_path).resolve()
    try:
        bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            f"Failed to load bundle JSON {bundle_path}: {exc}"
        ) from exc
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError("bundle JSON must be an object")
    if bundle.get("execution", {}).get("prod_scoped_shadow_live_execution_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "live execution pilot run has already been filed"
        )
    try:
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            bundle,
            repo_root=root,
            expect_live_execution_grant_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(str(exc)) from exc
    if bundle.get("recommended_next_stage") != POST_LIVE_EXECUTION_GRANT_NEXT_STAGE:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "bundle recommended_next_stage must be run_production_scoped_online_shadow_live_execution_pilot_v1"
        )
    existing_ids = {
        bundle.get("proof", {}).get("pilot_run_id"),
        bundle.get("execution", {}).get("pilot_harness", {}).get("pilot_run_id"),
        bundle.get("execution", {}).get("pilot_run", {}).get("pilot_run_id"),
        bundle.get("execution", {}).get("live_read_only_pilot_run", {}).get("pilot_run_id"),
    }
    if run_id in existing_ids:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "pilot_run_id must differ from proof, harness, audit-artifact pilot, and live read-only pilot run ids"
        )

    db_url = database_url or _database_url_from_env()
    try:
        database_summary = _assert_live_read_only_database_url(db_url)
        scorer_payload, scorer_summary = _load_frozen_audit_embedding_scorer(root)
        conn = _connect_readonly(db_url)
    except MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(str(exc), code=exc.code) from exc
    except Exception as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            f"live execution database unavailable: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        try:
            ranking_row = _query_ranking_run(conn, ranking_run_id=RANKING_RUN_ID)
            _validate_ranking_run_row(ranking_row)
            raw_rows = _query_candidate_inputs(
                conn,
                ranking_run_id=RANKING_RUN_ID,
                family=FAMILY,
                corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
                embedding_version=EMBEDDING_VERSION,
            )
            runtime_rows, join_summary = _build_runtime_rows_from_live_reads(
                raw_rows,
                scorer_payload=scorer_payload,
                scorer_summary=scorer_summary,
            )
        except MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError as exc:
            raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(str(exc), code=exc.code) from exc
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    live_source_reads = _build_live_source_reads(
        database_summary=database_summary,
        scorer_summary=scorer_summary,
        ranking_row=ranking_row,
        raw_rows=raw_rows,
        join_summary=join_summary,
    )
    if live_source_reads.get("ranking_run", {}).get("ranking_version") != LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "live execution ranking_version must match approved production candidate ranking version"
        )

    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = _runtime_call([], flag_value=None)
    pilot = _runtime_call(runtime_rows, flag_value="true")
    postflight = _runtime_call([], flag_value=None)
    incomplete_coverage = _incomplete_coverage_probe()
    environment_restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original
    if preflight.get("status") != "skipped_runtime_disabled":
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError("preflight runtime must be disabled")
    if pilot.get("status") != "succeeded_test_only" or pilot.get("shadow_row_count") != EXPECTED_LIVE_EXECUTION_PILOT_ROW_COUNT:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError("pilot runtime did not succeed with 528 rows")
    if postflight.get("status") != "skipped_runtime_disabled":
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError("postflight runtime must be disabled")
    if incomplete_coverage.get("status") != "skipped_incomplete_coverage":
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "incomplete coverage drill must skip the run"
        )

    input_provenance = _input_provenance(bundle, live_source_reads)
    live_execution_scope = _live_execution_scope(bundle)
    try:
        files_written, observability, write_counts_payload = _write_pilot_artifacts(
            repo_root=root,
            pilot_run_id=run_id,
            generated_at=generated,
            live_source_reads=live_source_reads,
            runtime_rows=runtime_rows,
            join_summary=join_summary,
            input_provenance=input_provenance,
            live_execution_scope=live_execution_scope,
            incomplete_coverage=incomplete_coverage,
            preflight=preflight,
            pilot=pilot,
            postflight=postflight,
        )
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(str(exc)) from exc
    pass_fail = _build_pass_fail(
        bundle=bundle,
        runtime_rows=runtime_rows,
        join_summary=join_summary,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        incomplete_coverage=incomplete_coverage,
        environment_restored=environment_restored,
        files_written=files_written,
        write_counts=write_counts_payload["write_counts_by_isolated_target"],
        live_source_reads=live_source_reads,
        live_execution_scope=live_execution_scope,
    )
    if not pass_fail["overall_passed"]:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(
            "live execution pilot failed checks: " + ", ".join(pass_fail["failed_checks"])
        )

    pilot_slice = {
        "pilot_run_id": run_id,
        "pilot_surface": LIVE_EXECUTION_PILOT_RUN_SURFACE,
        "pilot_run_directory": {
            "root_path": PROD_SCOPED_SHADOW_ROOT,
            "relative_path": f"{PROD_SCOPED_SHADOW_ROOT}{run_id}/",
        },
        "input_join_summary": deepcopy(dict(join_summary)),
        "live_prod_source_reads_performed": True,
        "live_source_reads": deepcopy(live_source_reads),
        "input_provenance": deepcopy(input_provenance),
        "live_execution_scope": deepcopy(live_execution_scope),
        "runtime_drill": {
            "call_order": ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
            "environment_restored": environment_restored,
            "process_scoped_runtime_flag_only": True,
            "preflight": _sanitize_runtime_result(preflight),
            "pilot": _sanitize_runtime_result(pilot),
            "postflight": _sanitize_runtime_result(postflight),
        },
        "incomplete_coverage_drill": deepcopy(incomplete_coverage),
        "files_written": files_written,
        "observability_summary": observability,
        "write_count_verification": {
            **write_counts_payload,
            "forbidden_write_counts_zero": pass_fail["checks"]["forbidden_write_counts_zero"],
            "forbidden_nonzero_write_counts": pass_fail["forbidden_nonzero_write_counts"],
        },
        "pass_fail_checks": deepcopy(pass_fail["checks"]),
        "pass_fail_evaluation": pass_fail,
        "executed_at": generated,
    }
    try:
        updated_bundle = apply_production_scoped_shadow_live_execution_pilot_run(
            bundle,
            pilot_slice,
            generated_at=generated,
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated_bundle,
            repo_root=root,
            expect_live_execution_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveExecutionPilotError(str(exc)) from exc
    if update_bundle:
        bundle_path.write_text(json.dumps(updated_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated_bundle),
            encoding="utf-8",
        )
    return {
        "pilot_run_id": run_id,
        "prod_scoped_shadow_live_execution_pilot_passed": True,
        "pilot_run_directory": pilot_slice["pilot_run_directory"],
        "execution": pilot_slice,
        "bundle": updated_bundle,
        "bundle_updated": update_bundle,
        "recommended_next_stage": updated_bundle["recommended_next_stage"],
    }

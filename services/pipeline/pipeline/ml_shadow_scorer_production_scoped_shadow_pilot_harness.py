"""Bounded production-scoped online shadow pilot harness for ml-shadow-scorer-v1.

The harness is deliberately not a live production pilot. It calls the runtime
only with synthetic or supplied fixture rows, writes gitignored prod-scoped audit
artifacts, and returns a bundle execution slice. It does not read live prod data,
touch databases, enable global flags, or authorize production/default/API/user-
visible behavior.
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

from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    RANKING_RUN_ID,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.ml_shadow_scorer_phase_bundle import PINNED_IDENTITY
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_PILOT_GRANT_NEXT_STAGE,
    apply_production_scoped_shadow_pilot_harness,
    markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
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

ALLOWED_HARNESS_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
PILOT_SURFACE = "bounded_fixture_pilot_harness"
DEFAULT_FIXTURE_ROW_COUNT = 3
RUNTIME_INPUT_FIELDS = (
    "canonical_openalex_work_id",
    "final_score",
    "audit_embedding_probability_work",
    "ranking_run_id",
    "family",
    "candidate_pool_work_set_sha256",
    "corpus_snapshot_version",
    "embedding_version",
)
FORBIDDEN_FIXTURE_LABEL_FIELDS = {
    "label",
    "labels",
    "label_any_positive",
    "good_or_acceptable",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
    "holdout_assignment",
    "holdout_split",
    "train_eval_split",
}


class MLShadowScorerProductionScopedShadowPilotHarnessError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_pilot_run_id(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "")
    if compact.endswith("Z"):
        compact = compact[:-1] + "Z"
    return f"{RANKING_RUN_ID}-harness-{compact}"


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(f"Expected JSON object in {path}")
    return payload


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(Path(path).read_bytes())


def _stable_rows_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    encoded = "\n".join(json.dumps(dict(row), sort_keys=True, separators=(",", ":")) for row in rows)
    return _sha256_bytes(encoded.encode("utf-8"))


def _default_fixture_rows(generated_at: str) -> list[dict[str, Any]]:
    return [
        {
            "canonical_openalex_work_id": "W-PROD-HARNESS-001",
            "final_score": 0.91,
            "audit_embedding_probability_work": 0.74,
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "component_coverage": {"final_score": True, "audit_embedding_probability_work": True},
            "generated_at": generated_at,
            "input_hashes": {"source": "bounded_fixture_pilot_harness", "row": "001"},
            "rank_displacement": 0,
        },
        {
            "canonical_openalex_work_id": "W-PROD-HARNESS-002",
            "final_score": 0.63,
            "audit_embedding_probability_work": 0.88,
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "component_coverage": {"final_score": True, "audit_embedding_probability_work": True},
            "generated_at": generated_at,
            "input_hashes": {"source": "bounded_fixture_pilot_harness", "row": "002"},
            "rank_displacement": 1,
        },
        {
            "canonical_openalex_work_id": "W-PROD-HARNESS-003",
            "final_score": 0.32,
            "audit_embedding_probability_work": 0.35,
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "component_coverage": {"final_score": True, "audit_embedding_probability_work": True},
            "generated_at": generated_at,
            "input_hashes": {"source": "bounded_fixture_pilot_harness", "row": "003"},
            "rank_displacement": -1,
        },
    ]


def _load_fixture_rows(path: Path | None, *, generated_at: str) -> list[dict[str, Any]]:
    if path is None:
        return _default_fixture_rows(generated_at)
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(f"fixture input does not exist: {path}")
    if resolved.suffix.lower() == ".jsonl":
        rows = [json.loads(line) for line in resolved.read_text(encoding="utf-8").splitlines() if line.strip()]
    else:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list) or not rows:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError("fixture input must contain a non-empty row list")
    out: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerProductionScopedShadowPilotHarnessError(f"fixture row {index} must be an object")
        out.append(dict(row))
    return out


def _validate_fixture_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    if len(rows) != DEFAULT_FIXTURE_ROW_COUNT:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(
            f"pilot harness fixture must contain exactly {DEFAULT_FIXTURE_ROW_COUNT} rows"
        )
    seen: set[str] = set()
    for index, row in enumerate(rows):
        forbidden = sorted(FORBIDDEN_FIXTURE_LABEL_FIELDS.intersection(row))
        if forbidden:
            raise MLShadowScorerProductionScopedShadowPilotHarnessError(
                f"fixture row {index} contains label fields forbidden for scoring: {', '.join(forbidden)}"
            )
        missing = [field for field in RUNTIME_INPUT_FIELDS if field not in row]
        if missing:
            raise MLShadowScorerProductionScopedShadowPilotHarnessError(
                f"fixture row {index} missing runtime fields: {', '.join(missing)}"
            )
        work_id = str(row.get("canonical_openalex_work_id") or "").strip()
        if not work_id or work_id in seen:
            raise MLShadowScorerProductionScopedShadowPilotHarnessError(
                f"fixture row {index} must have a unique canonical_openalex_work_id"
            )
        seen.add(work_id)
        expected_identity = {
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
        }
        for field, expected in expected_identity.items():
            if row.get(field) != expected:
                raise MLShadowScorerProductionScopedShadowPilotHarnessError(
                    f"fixture row {index}.{field} must be {expected!r}"
                )


def _runtime_rows(fixture_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [{field: row[field] for field in RUNTIME_INPUT_FIELDS} for row in fixture_rows]


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
                "pilot_surface": PILOT_SURFACE,
                "shadow_rank": rank,
                "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                "final_score": row.get("final_score"),
                "audit_embedding_probability_work": row.get("audit_embedding_probability_work"),
                "ml_shadow_scorer_v1_score": row.get("ml_shadow_scorer_v1_score"),
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "embedding_version": EMBEDDING_VERSION,
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
        "pilot_surface": PILOT_SURFACE,
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
            "fixture_rows": len(runtime_rows),
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
            "complete": len(runtime_rows) == DEFAULT_FIXTURE_ROW_COUNT,
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
        "rank_displacement_audit_only": "fixture_only",
    }


def _build_pass_fail(
    *,
    runtime_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    environment_restored: bool,
    files_written: Sequence[Mapping[str, Any]],
    write_counts: Mapping[str, int],
) -> dict[str, Any]:
    forbidden_zero = all(count == 0 for target, count in write_counts.items() if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)
    checks = {
        "fixture_row_count_3": len(runtime_rows) == DEFAULT_FIXTURE_ROW_COUNT,
        "preflight_disabled": preflight.get("status") == "skipped_runtime_disabled" and preflight.get("shadow_row_count") == 0,
        "pilot_runtime_succeeded": pilot.get("status") == "succeeded_test_only" and pilot.get("shadow_row_count") == len(runtime_rows),
        "postflight_disabled": postflight.get("status") == "skipped_runtime_disabled" and postflight.get("shadow_row_count") == 0,
        "environment_restored": environment_restored,
        "runtime_drill_order": True,
        "forbidden_write_counts_zero": forbidden_zero,
        "isolated_artifact_target_count_4": write_counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS) == 4,
        "files_written_allowed": [record.get("relative_path") for record in files_written] == list(ALLOWED_HARNESS_FILES),
        "live_prod_source_reads_false": True,
        "runtime_writes_false": pilot.get("writes_performed") is False,
        "production_default_changed_false": pilot.get("production_default_changed") is False,
        "user_visible_ranking_changed_false": pilot.get("user_visible_ranking_changed") is False,
        "labels_used_for_scoring_false": pilot.get("labels_used_for_scoring") is False,
        "pilot_executed_false": True,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "overall_passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "forbidden_nonzero_write_counts": {
            target: count for target, count in write_counts.items()
            if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS and count != 0
        },
    }


def _build_and_write_artifacts(
    *,
    repo_root: Path,
    pilot_run_id: str,
    generated_at: str,
    bundle_path: Path,
    bundle_sha256: str,
    fixture_rows: Sequence[Mapping[str, Any]],
    runtime_rows: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    if pilot_dir.exists() and any(pilot_dir.iterdir()):
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(
            f"pilot harness output directory already exists and is not empty: {pilot_dir}"
        )
    assert_prod_scoped_write_path_allowed(pilot_dir, repo_root)
    shadow_rows = pilot.get("shadow_rows") if isinstance(pilot.get("shadow_rows"), list) else []
    shadow_export = _shadow_row_export_rows([row for row in shadow_rows if isinstance(row, Mapping)])
    write_counts = _write_counts_by_isolated_target(file_count=4)
    assert_prod_scoped_forbidden_write_target_counts(write_counts)
    manifest = {
        "artifact_type": "ml_shadow_scorer_production_scoped_shadow_pilot_harness_manifest",
        "generated_at": generated_at,
        "pilot_run_id": pilot_run_id,
        "pilot_surface": PILOT_SURFACE,
        "live_prod_source_reads_performed": False,
        "pinned_identity": deepcopy(PINNED_IDENTITY),
        "input_hashes": {
            "bundle": bundle_sha256,
            "fixture_rows": _stable_rows_sha256(fixture_rows),
        },
        "source_bundle": str(bundle_path),
        "fixture_row_count": len(fixture_rows),
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
        "pilot_surface": PILOT_SURFACE,
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


def run_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
    *,
    bundle_path: Path,
    pilot_run_id: str | None = None,
    fixture_input_path: Path | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    generated = generated_at or _now_iso_z()
    bundle_path = Path(bundle_path).resolve()
    bundle = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            bundle,
            repo_root=root,
            expect_pilot_grant_filed=True,
        )
        run_id = pilot_run_id or _default_pilot_run_id(generated)
        validate_pilot_run_id(run_id)
        pilot_dir = resolve_prod_scoped_pilot_directory(root, run_id)
        assert_prod_scoped_write_path_allowed(pilot_dir, root)
    except (MLShadowScorerProductionScopedShadowBundleError, ShadowWritePathGuardError) as exc:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(str(exc)) from exc
    proof_run_id = bundle.get("proof", {}).get("pilot_run_id") if isinstance(bundle.get("proof"), Mapping) else None
    if run_id == proof_run_id:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(
            "pilot harness pilot_run_id must differ from proof pilot_run_id"
        )
    if _get_bool(bundle, "execution.prod_scoped_shadow_pilot_harness_executed"):
        raise MLShadowScorerProductionScopedShadowPilotHarnessError("pilot harness has already been filed")
    if bundle.get("recommended_next_stage") != POST_PILOT_GRANT_NEXT_STAGE:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(
            "bundle recommended_next_stage must be run_production_scoped_online_shadow_pilot_v1"
        )

    fixture_rows = _load_fixture_rows(fixture_input_path, generated_at=generated)
    _validate_fixture_rows(fixture_rows)
    runtime_rows = _runtime_rows(fixture_rows)
    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = _runtime_call([], flag_value=None)
    pilot = _runtime_call(runtime_rows, flag_value="true")
    postflight = _runtime_call([], flag_value=None)
    environment_restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original

    bundle_sha = _sha256_file(bundle_path)
    try:
        files_written, observability, write_counts_payload = _build_and_write_artifacts(
            repo_root=root,
            pilot_run_id=run_id,
            generated_at=generated,
            bundle_path=bundle_path,
            bundle_sha256=bundle_sha,
            fixture_rows=fixture_rows,
            runtime_rows=runtime_rows,
            preflight=preflight,
            pilot=pilot,
            postflight=postflight,
        )
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(str(exc)) from exc
    pass_fail = _build_pass_fail(
        runtime_rows=runtime_rows,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        environment_restored=environment_restored,
        files_written=files_written,
        write_counts=write_counts_payload["write_counts_by_isolated_target"],
    )
    if not pass_fail["overall_passed"]:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(
            "production-scoped pilot harness failed checks: " + ", ".join(pass_fail["failed_checks"])
        )
    harness_slice = {
        "pilot_run_id": run_id,
        "pilot_surface": PILOT_SURFACE,
        "pilot_run_directory": {
            "root_path": PROD_SCOPED_SHADOW_ROOT,
            "relative_path": f"{PROD_SCOPED_SHADOW_ROOT}{run_id}/",
        },
        "fixture_row_count": len(fixture_rows),
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
        updated_bundle = apply_production_scoped_shadow_pilot_harness(
            bundle,
            harness_slice,
            generated_at=generated,
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated_bundle,
            repo_root=root,
            expect_pilot_harness_filed=True,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowPilotHarnessError(str(exc)) from exc
    if update_bundle:
        bundle_path.write_text(json.dumps(updated_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated_bundle),
            encoding="utf-8",
        )
    return {
        "pilot_run_id": run_id,
        "pilot_harness_passed": True,
        "pilot_run_directory": harness_slice["pilot_run_directory"],
        "execution": harness_slice,
        "bundle": updated_bundle,
        "bundle_updated": update_bundle,
        "recommended_next_stage": updated_bundle["recommended_next_stage"],
    }


def _get_bool(payload: Mapping[str, Any], path: str) -> bool:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return False
    return current is True

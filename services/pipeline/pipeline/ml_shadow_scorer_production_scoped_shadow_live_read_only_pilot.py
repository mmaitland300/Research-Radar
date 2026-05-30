"""Bounded production-scoped online shadow live read-only pilot.

This runner is the first production-scoped milestone that may read approved
production sources. It is fail-closed: without explicit confirmation it does
not open a database connection, call runtime, write artifacts, or touch the
bundle.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
from time import perf_counter
from typing import Any, Iterator, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_offline_audit_embedding_scorer_export import (
    MLOfflineAuditEmbeddingScorerExportError,
    score_audit_embedding_probability,
)
from pipeline.ml_shadow_scorer_generalization_second_surface import (
    MLShadowScorerGeneralizationSecondSurfaceError,
    _database_url_from_env,
    assert_local_database_url as _assert_generalization_local_database_url,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FORBIDDEN_LABEL_FIELDS,
    FORMULA_ID,
    RANKING_RUN_ID,
    SCORER_ID,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.ml_shadow_scorer_phase_bundle import PINNED_IDENTITY
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    FORBIDDEN_PROD_SCOPED_WRITE_TARGETS,
    LIVE_READ_ONLY_PILOT_RUN_PASS_FAIL_CHECKS,
    LIVE_READ_ONLY_PILOT_RUN_SURFACE,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_LIVE_READ_ONLY_GRANT_NEXT_STAGE,
    apply_production_scoped_shadow_live_read_only_pilot_run,
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

EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT = 528
APPROVED_SOURCE_TABLES = ("ranking_runs", "paper_scores", "works", "embeddings")
ALLOWED_PILOT_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
FROZEN_AUDIT_EMBEDDING_SCORER_PATH = Path("docs/audit/ml-offline-audit-embedding-scorer-v2.json")
FROZEN_AUDIT_EMBEDDING_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
FROZEN_AUDIT_EMBEDDING_SCORER_TYPE = "ml_offline_audit_embedding_scorer"
_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)
_WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|merge|copy|grant|revoke|vacuum|call)\b",
    re.IGNORECASE,
)
_SOURCE_TABLE_RE = re.compile(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_\.]*)", re.IGNORECASE)


class MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_pilot_run_id(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "")
    if compact.endswith("Z"):
        compact = compact[:-1] + "Z"
    return f"prod-readonly-{RANKING_RUN_ID}-{compact}"


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(f"Expected JSON object in {path}")
    return payload


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_vector(value: Any) -> list[float]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"embedding vector string is not valid JSON: {exc}"
            ) from exc
    if not isinstance(raw, (list, tuple)):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError("embedding vector must be an array")
    vector: list[float] = []
    for item in raw:
        try:
            number = float(item)
        except (TypeError, ValueError) as exc:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"embedding vector contains non-numeric value: {item!r}"
            ) from exc
        if not math.isfinite(number):
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                "embedding vector contains non-finite value"
            )
        vector.append(number)
    return vector


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _assert_live_read_only_database_url(database_url: str) -> dict[str, Any]:
    try:
        summary = dict(_assert_generalization_local_database_url(database_url))
    except MLShadowScorerGeneralizationSecondSurfaceError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(str(exc), code=exc.code) from exc
    summary["read_only_contract"] = "SELECT-only queries; no database mutations"
    summary["approved_source_tables"] = list(APPROVED_SOURCE_TABLES)
    return summary


def _connect_readonly(database_url: str) -> Any:
    return psycopg.connect(database_url, autocommit=True, options="-c default_transaction_read_only=on")


def _load_frozen_audit_embedding_scorer(repo_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    scorer_path = (repo_root / FROZEN_AUDIT_EMBEDDING_SCORER_PATH).resolve()
    payload = _load_json_object(scorer_path)
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "frozen audit embedding scorer missing metadata"
        )
    if metadata.get("artifact_type") != FROZEN_AUDIT_EMBEDDING_SCORER_TYPE:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "frozen audit embedding scorer artifact_type mismatch"
        )
    if metadata.get("scorer_version") != FROZEN_AUDIT_EMBEDDING_SCORER_VERSION:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "frozen audit embedding scorer version mismatch"
        )
    if metadata.get("fit_mode") != "holdout_bound_train_only":
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "frozen audit embedding scorer fit_mode mismatch"
        )
    if metadata.get("target") != "good_or_acceptable":
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "frozen audit embedding scorer target mismatch"
        )
    dimensions = metadata.get("embedding_dimensions")
    if not isinstance(dimensions, int) or isinstance(dimensions, bool) or dimensions <= 0:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "frozen audit embedding scorer embedding_dimensions must be positive"
        )
    return payload, {
        "path": FROZEN_AUDIT_EMBEDDING_SCORER_PATH.as_posix(),
        "sha256": sha256_file(scorer_path),
        "scorer_version": FROZEN_AUDIT_EMBEDDING_SCORER_VERSION,
        "fit_mode": metadata.get("fit_mode"),
        "target": metadata.get("target"),
        "embedding_dimensions": dimensions,
        "loaded_after_confirmation": True,
    }


def _execute_select(cur: Any, sql: str, params: Sequence[Any] = ()) -> None:
    stripped = str(sql).lstrip()
    if not stripped.lower().startswith("select") or _WRITE_SQL_RE.search(stripped):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "SQL guard allows SELECT statements only"
        )
    referenced = {match.group(1).split(".")[-1] for match in _SOURCE_TABLE_RE.finditer(stripped)}
    forbidden = sorted(referenced - set(APPROVED_SOURCE_TABLES))
    if forbidden:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "SQL source allowlist violation: " + ", ".join(forbidden)
        )
    cur.execute(stripped, tuple(params))


def _query_ranking_run(conn: Any, *, ranking_run_id: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(
            cur,
            """
            SELECT ranking_run_id, status, ranking_version, corpus_snapshot_version, embedding_version
            FROM ranking_runs
            WHERE ranking_run_id = %s
            """,
            (ranking_run_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            f"ranking_run_id not found: {ranking_run_id}"
        )
    return _json_safe(dict(row))


def _query_candidate_inputs(
    conn: Any,
    *,
    ranking_run_id: str,
    family: str,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(
            cur,
            """
            SELECT
                ps.ranking_run_id,
                ps.work_id AS internal_work_id,
                ps.recommendation_family,
                ps.final_score,
                w.openalex_id,
                w.title,
                w.year,
                w.corpus_snapshot_version,
                e.embedding_version AS observed_embedding_version,
                e.vector
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            LEFT JOIN embeddings e
              ON e.work_id = ps.work_id
             AND e.embedding_version = %s
            WHERE ps.ranking_run_id = %s
              AND ps.recommendation_family = %s
              AND w.corpus_snapshot_version = %s
            ORDER BY ps.final_score DESC NULLS LAST, ps.work_id ASC
            """,
            (embedding_version, ranking_run_id, family, corpus_snapshot_version),
        )
        return [_json_safe(dict(row)) for row in cur.fetchall()]


def _validate_ranking_run_row(row: Mapping[str, Any]) -> None:
    if row.get("ranking_run_id") != RANKING_RUN_ID:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError("ranking_run_id row mismatch")
    if row.get("corpus_snapshot_version") != CORPUS_SNAPSHOT_VERSION:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "ranking_run corpus_snapshot_version mismatch"
        )
    if row.get("embedding_version") != EMBEDDING_VERSION:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "ranking_run embedding_version mismatch"
        )


def _build_runtime_rows_from_live_reads(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    scorer_payload: Mapping[str, Any],
    scorer_summary: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if len(raw_rows) != EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            f"incomplete live read coverage: expected {EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT}, got {len(raw_rows)}"
        )
    seen: set[str] = set()
    runtime_rows: list[dict[str, Any]] = []
    final_score_coverage = 0
    learned_probability_coverage = 0
    embedding_coverage = 0
    forbidden_label_rows: list[dict[str, Any]] = []
    for index, row in enumerate(raw_rows, start=1):
        forbidden = sorted(field for field in FORBIDDEN_LABEL_FIELDS if field in row)
        if forbidden:
            forbidden_label_rows.append({"row_index": index, "forbidden_fields": forbidden})
        canonical = _canonical_from_value(row.get("openalex_id"))
        if not canonical:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {index} missing canonical OpenAlex ID"
            )
        if canonical in seen:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"duplicate canonical OpenAlex work ID: {canonical}"
            )
        seen.add(canonical)
        if row.get("ranking_run_id") != RANKING_RUN_ID:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {canonical} ranking_run_id mismatch"
            )
        if row.get("recommendation_family") != FAMILY:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {canonical} family mismatch"
            )
        if row.get("corpus_snapshot_version") != CORPUS_SNAPSHOT_VERSION:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {canonical} corpus_snapshot_version mismatch"
            )
        final_score = _float_or_none(row.get("final_score"))
        if final_score is None:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {canonical} missing final_score"
            )
        final_score_coverage += 1
        if row.get("observed_embedding_version") != EMBEDDING_VERSION or row.get("vector") is None:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {canonical} missing approved embedding coverage"
            )
        vector = _parse_vector(row.get("vector"))
        try:
            learned_probability = score_audit_embedding_probability(vector, scorer_payload)
        except MLOfflineAuditEmbeddingScorerExportError as exc:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
                f"candidate row {canonical} scorer application failed: {exc}",
                code=exc.code,
            ) from exc
        learned_probability_coverage += 1
        embedding_coverage += 1
        runtime_rows.append(
            {
                "canonical_openalex_work_id": canonical,
                "title": row.get("title"),
                "year": row.get("year"),
                "ranking_run_id": RANKING_RUN_ID,
                "family": FAMILY,
                "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
                "final_score": final_score,
                "audit_embedding_probability_work": learned_probability,
                "embedding_version": EMBEDDING_VERSION,
                "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
                "scorer_id": SCORER_ID,
                "formula_id": FORMULA_ID,
            }
        )
    if forbidden_label_rows:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "labels or holdout fields were loaded in live read rows"
        )
    observed_sha = _work_set_sha256(sorted(seen))
    if observed_sha != CANDIDATE_POOL_WORK_SET_SHA256:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            f"candidate_pool_work_set_sha256 mismatch: expected {CANDIDATE_POOL_WORK_SET_SHA256}, got {observed_sha}"
        )
    join_summary = {
        "joined_candidate_count": len(runtime_rows),
        "runtime_row_count": len(runtime_rows),
        "expected_candidate_count": EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
        "final_score_coverage_count": final_score_coverage,
        "learned_probability_coverage_count": learned_probability_coverage,
        "embedding_coverage_count": embedding_coverage,
        "candidate_pool_work_set_sha256": observed_sha,
        "labels_not_used_for_scoring": True,
        "audit_embedding_probability_source": "computed_from_live_embedding_vectors_with_frozen_scorer",
        "audit_embedding_scorer": dict(scorer_summary),
        "refit_training_performed": False,
        "embedding_generation_performed": False,
        "label_ingest_performed": False,
    }
    return runtime_rows, join_summary


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
                "pilot_surface": LIVE_READ_ONLY_PILOT_RUN_SURFACE,
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
        "pilot_surface": LIVE_READ_ONLY_PILOT_RUN_SURFACE,
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
            "complete": len(runtime_rows) == EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT
            and len(shadow_rows) == EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
            "runtime_candidate_count": len(runtime_rows),
            "shadow_row_count": len(shadow_rows),
        },
        "score_distributions": {
            "ml_shadow_scorer_v1_score": [row.get("ml_shadow_scorer_v1_score") for row in shadow_rows],
        },
        "skipped_runs": [
            {"phase": "preflight_disabled", "status": preflight.get("status"), "reason": preflight.get("reason")},
            {"phase": "postflight_disabled", "status": postflight.get("status"), "reason": postflight.get("reason")},
        ],
        "forbidden_write_target_counts": dict(write_counts),
        "rank_displacement_audit_only": "not_recomputed_in_production_scoped_live_read_only_pilot",
    }


def _build_live_source_reads(
    *,
    database_summary: Mapping[str, Any],
    scorer_summary: Mapping[str, Any],
    ranking_row: Mapping[str, Any],
    raw_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "approved_tables": list(APPROVED_SOURCE_TABLES),
        "database_url_scope": {
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "local_database_url_confirmed": database_summary.get("local_database_url_confirmed"),
            "read_only_contract": database_summary.get("read_only_contract"),
        },
        "row_counts": {
            "ranking_runs": 1,
            "paper_scores": len(raw_rows),
            "works": len(raw_rows),
            "embeddings": int(join_summary.get("embedding_coverage_count") or 0),
            "joined_candidate_count": int(join_summary.get("joined_candidate_count") or 0),
        },
        "ranking_run": dict(ranking_row),
        "audit_embedding_probability_derivation": {
            "source": join_summary.get("audit_embedding_probability_source"),
            "scorer": dict(scorer_summary),
            "live_embedding_vectors_used": True,
            "frozen_candidate_score_artifact_used_as_primary_input": False,
        },
        "input_identity_verification": {
            **deepcopy(PINNED_IDENTITY),
            "matches_pinned_identity": True,
        },
        "candidate_pool_work_set_sha256_match": True,
        "read_only_assertions": {
            "select_only_sql_enforced": True,
            "approved_source_allowlist_enforced": True,
            "default_transaction_read_only": True,
            "no_write_sql_detected": True,
        },
        "labels_not_used_for_scoring": True,
        "refit_training_performed": False,
        "embedding_generation_performed": False,
        "label_ingest_performed": False,
    }


def _write_pilot_artifacts(
    *,
    repo_root: Path,
    pilot_run_id: str,
    generated_at: str,
    live_source_reads: Mapping[str, Any],
    runtime_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    if pilot_dir.exists() and any(pilot_dir.iterdir()):
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            f"pilot output directory already exists and is not empty: {pilot_dir}"
        )
    assert_prod_scoped_write_path_allowed(pilot_dir, repo_root)
    shadow_rows = pilot.get("shadow_rows") if isinstance(pilot.get("shadow_rows"), list) else []
    shadow_export = _shadow_row_export_rows([row for row in shadow_rows if isinstance(row, Mapping)])
    write_counts = _write_counts_by_isolated_target(file_count=4)
    assert_prod_scoped_forbidden_write_target_counts(write_counts)
    manifest = {
        "artifact_type": "ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot_manifest",
        "generated_at": generated_at,
        "pilot_run_id": pilot_run_id,
        "pilot_surface": LIVE_READ_ONLY_PILOT_RUN_SURFACE,
        "live_prod_source_reads_performed": True,
        "pinned_identity": deepcopy(PINNED_IDENTITY),
        "live_source_reads": deepcopy(dict(live_source_reads)),
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
            "expected_row_count": EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
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
        "pilot_surface": LIVE_READ_ONLY_PILOT_RUN_SURFACE,
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
    runtime_rows: Sequence[Mapping[str, Any]],
    join_summary: Mapping[str, Any],
    preflight: Mapping[str, Any],
    pilot: Mapping[str, Any],
    postflight: Mapping[str, Any],
    environment_restored: bool,
    files_written: Sequence[Mapping[str, Any]],
    write_counts: Mapping[str, int],
    live_source_reads: Mapping[str, Any],
) -> dict[str, Any]:
    forbidden_nonzero = {
        target: count
        for target, count in write_counts.items()
        if target != ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS and count != 0
    }
    checks = {
        "joined_candidate_count_528": join_summary.get("joined_candidate_count")
        == EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
        "runtime_row_count_528": len(runtime_rows) == EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
        "runtime_drill_call_order": True,
        "preflight_postflight_disabled": preflight.get("status") == "skipped_runtime_disabled"
        and preflight.get("shadow_row_count") == 0
        and postflight.get("status") == "skipped_runtime_disabled"
        and postflight.get("shadow_row_count") == 0,
        "environment_restored": environment_restored,
        "forbidden_write_counts_zero": not forbidden_nonzero,
        "isolated_artifact_count_4": write_counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS) == 4,
        "expected_files_recorded": [record.get("relative_path") for record in files_written] == list(ALLOWED_PILOT_FILES),
        "live_prod_source_reads_true": True,
        "live_source_reads_documented": isinstance(live_source_reads, Mapping)
        and sorted(live_source_reads.get("approved_tables") or [])
        == ["embeddings", "paper_scores", "ranking_runs", "works"],
        "pilot_surface_bounded_live_read_only_prod_scoped": True,
        "no_labels_used_for_scoring": pilot.get("labels_used_for_scoring") is False
        and live_source_reads.get("labels_not_used_for_scoring") is True,
        "no_refit_training_embedding_generation_or_label_ingest": live_source_reads.get("refit_training_performed") is False
        and live_source_reads.get("embedding_generation_performed") is False
        and live_source_reads.get("label_ingest_performed") is False,
        "read_only_sql_and_allowlist_enforced": all(
            live_source_reads.get("read_only_assertions", {}).get(field) is True
            for field in (
                "select_only_sql_enforced",
                "approved_source_allowlist_enforced",
                "default_transaction_read_only",
                "no_write_sql_detected",
            )
        ),
        "production_api_user_visible_unchanged": pilot.get("production_default_changed") is False
        and pilot.get("user_visible_ranking_changed") is False,
        "global_live_execution_authorization_false": True,
    }
    missing = [name for name in LIVE_READ_ONLY_PILOT_RUN_PASS_FAIL_CHECKS if name not in checks]
    if missing:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "missing live read-only pass/fail checks: " + ", ".join(missing)
        )
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "overall_passed": not failed,
        "checks": checks,
        "failed_checks": failed,
        "forbidden_nonzero_write_counts": forbidden_nonzero,
    }


def run_ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot(
    *,
    bundle_path: Path,
    database_url: str | None = None,
    pilot_run_id: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
    confirm_live_read_only_prod_source_reads: bool = False,
) -> dict[str, Any]:
    if not confirm_live_read_only_prod_source_reads:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "--confirm-live-read-only-prod-source-reads is required before live production source reads"
        )

    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    generated = generated_at or _now_iso_z()
    run_id = pilot_run_id or _default_pilot_run_id(generated)
    try:
        validate_pilot_run_id(run_id)
        if "harness" in run_id:
            raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError("pilot_run_id must not contain harness")
        pilot_dir = resolve_prod_scoped_pilot_directory(root, run_id)
        assert_prod_scoped_write_path_allowed(pilot_dir, root)
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(str(exc)) from exc

    bundle_path = Path(bundle_path).resolve()
    bundle = _load_json_object(bundle_path)
    try:
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            bundle,
            repo_root=root,
            expect_live_read_only_grant_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(str(exc)) from exc
    if bundle.get("recommended_next_stage") != POST_LIVE_READ_ONLY_GRANT_NEXT_STAGE:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "bundle recommended_next_stage must be run_production_scoped_online_shadow_live_read_only_pilot_v1"
        )
    if bundle.get("execution", {}).get("prod_scoped_shadow_live_read_only_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "live read-only pilot run has already been filed"
        )
    existing_ids = {
        bundle.get("proof", {}).get("pilot_run_id"),
        bundle.get("execution", {}).get("pilot_harness", {}).get("pilot_run_id"),
        bundle.get("execution", {}).get("pilot_run", {}).get("pilot_run_id"),
    }
    if run_id in existing_ids:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "pilot_run_id must differ from proof, harness, and audit-artifact pilot run ids"
        )

    db_url = database_url or _database_url_from_env()
    database_summary = _assert_live_read_only_database_url(db_url)
    scorer_payload, scorer_summary = _load_frozen_audit_embedding_scorer(root)
    try:
        conn = _connect_readonly(db_url)
    except Exception as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            f"live read-only database unavailable: {type(exc).__name__}: {exc}"
        ) from exc
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

    original = os.environ.get(FEATURE_FLAG)
    original_present = FEATURE_FLAG in os.environ
    preflight = _runtime_call([], flag_value=None)
    pilot = _runtime_call(runtime_rows, flag_value="true")
    postflight = _runtime_call([], flag_value=None)
    environment_restored = (FEATURE_FLAG in os.environ) == original_present and os.environ.get(FEATURE_FLAG) == original
    if preflight.get("status") != "skipped_runtime_disabled":
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError("preflight runtime must be disabled")
    if pilot.get("status") != "succeeded_test_only" or pilot.get("shadow_row_count") != EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError("pilot runtime did not succeed with 528 rows")
    if postflight.get("status") != "skipped_runtime_disabled":
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError("postflight runtime must be disabled")

    try:
        files_written, observability, write_counts_payload = _write_pilot_artifacts(
            repo_root=root,
            pilot_run_id=run_id,
            generated_at=generated,
            live_source_reads=live_source_reads,
            runtime_rows=runtime_rows,
            join_summary=join_summary,
            preflight=preflight,
            pilot=pilot,
            postflight=postflight,
        )
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(str(exc)) from exc
    pass_fail = _build_pass_fail(
        runtime_rows=runtime_rows,
        join_summary=join_summary,
        preflight=preflight,
        pilot=pilot,
        postflight=postflight,
        environment_restored=environment_restored,
        files_written=files_written,
        write_counts=write_counts_payload["write_counts_by_isolated_target"],
        live_source_reads=live_source_reads,
    )
    if not pass_fail["overall_passed"]:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(
            "live read-only pilot failed checks: " + ", ".join(pass_fail["failed_checks"])
        )

    pilot_slice = {
        "pilot_run_id": run_id,
        "pilot_surface": LIVE_READ_ONLY_PILOT_RUN_SURFACE,
        "pilot_run_directory": {
            "root_path": PROD_SCOPED_SHADOW_ROOT,
            "relative_path": f"{PROD_SCOPED_SHADOW_ROOT}{run_id}/",
        },
        "input_join_summary": deepcopy(dict(join_summary)),
        "live_prod_source_reads_performed": True,
        "live_source_reads": deepcopy(live_source_reads),
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
        "pass_fail_checks": deepcopy(pass_fail["checks"]),
        "pass_fail_evaluation": pass_fail,
        "executed_at": generated,
    }
    try:
        updated_bundle = apply_production_scoped_shadow_live_read_only_pilot_run(
            bundle,
            pilot_slice,
            generated_at=generated,
        )
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            updated_bundle,
            repo_root=root,
            expect_live_read_only_pilot_run_filed=True,
            verify_local_pilot_files=False,
        )
    except MLShadowScorerProductionScopedShadowBundleError as exc:
        raise MLShadowScorerProductionScopedShadowLiveReadOnlyPilotError(str(exc)) from exc
    if update_bundle:
        bundle_path.write_text(json.dumps(updated_bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        bundle_path.with_name("bundle.md").write_text(
            markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated_bundle),
            encoding="utf-8",
        )
    return {
        "pilot_run_id": run_id,
        "prod_scoped_shadow_live_read_only_pilot_passed": True,
        "pilot_run_directory": pilot_slice["pilot_run_directory"],
        "execution": pilot_slice,
        "bundle": updated_bundle,
        "bundle_updated": update_bundle,
        "recommended_next_stage": updated_bundle["recommended_next_stage"],
    }

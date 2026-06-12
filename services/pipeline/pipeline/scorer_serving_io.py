"""Read-only database and frozen-scorer IO shared by the scorer serving paths.

These helpers back the bounded Emerging scorer serving module
(`ml_scorer_rollout_serving`). They connect read-only, load the pinned frozen
audit-embedding scorer artifact, and build runtime candidate rows from live
SELECT-only reads against the pinned ranking run.
"""

from __future__ import annotations

from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_offline_audit_embedding_scorer_export import (
    MLOfflineAuditEmbeddingScorerExportError,
    score_audit_embedding_probability,
)
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FORBIDDEN_LABEL_FIELDS,
    FORMULA_ID,
    RANKING_RUN_ID,
    SCORER_ID,
)

EXPECTED_CANDIDATE_ROW_COUNT = 528
APPROVED_SOURCE_TABLES = ("ranking_runs", "paper_scores", "works", "embeddings")
FROZEN_AUDIT_EMBEDDING_SCORER_PATH = Path("docs/audit/ml-offline-audit-embedding-scorer-v2.json")
FROZEN_AUDIT_EMBEDDING_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
FROZEN_AUDIT_EMBEDDING_SCORER_TYPE = "ml_offline_audit_embedding_scorer"
_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)
_WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|merge|copy|grant|revoke|vacuum|call)\b",
    re.IGNORECASE,
)
_SOURCE_TABLE_RE = re.compile(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_\.]*)", re.IGNORECASE)


class ScorerServingIOError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScorerServingIOError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ScorerServingIOError(f"Expected JSON object in {path}")
    return payload


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
            raise ScorerServingIOError(f"embedding vector string is not valid JSON: {exc}") from exc
    if not isinstance(raw, (list, tuple)):
        raise ScorerServingIOError("embedding vector must be an array")
    vector: list[float] = []
    for item in raw:
        try:
            number = float(item)
        except (TypeError, ValueError) as exc:
            raise ScorerServingIOError(f"embedding vector contains non-numeric value: {item!r}") from exc
        if not math.isfinite(number):
            raise ScorerServingIOError("embedding vector contains non-finite value")
        vector.append(number)
    return vector


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def connect_readonly(database_url: str) -> Any:
    return psycopg.connect(database_url, autocommit=True, options="-c default_transaction_read_only=on")


def load_frozen_audit_embedding_scorer(repo_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    scorer_path = (repo_root / FROZEN_AUDIT_EMBEDDING_SCORER_PATH).resolve()
    payload = _load_json_object(scorer_path)
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ScorerServingIOError("frozen audit embedding scorer missing metadata")
    if metadata.get("artifact_type") != FROZEN_AUDIT_EMBEDDING_SCORER_TYPE:
        raise ScorerServingIOError("frozen audit embedding scorer artifact_type mismatch")
    if metadata.get("scorer_version") != FROZEN_AUDIT_EMBEDDING_SCORER_VERSION:
        raise ScorerServingIOError("frozen audit embedding scorer version mismatch")
    if metadata.get("fit_mode") != "holdout_bound_train_only":
        raise ScorerServingIOError("frozen audit embedding scorer fit_mode mismatch")
    if metadata.get("target") != "good_or_acceptable":
        raise ScorerServingIOError("frozen audit embedding scorer target mismatch")
    dimensions = metadata.get("embedding_dimensions")
    if not isinstance(dimensions, int) or isinstance(dimensions, bool) or dimensions <= 0:
        raise ScorerServingIOError("frozen audit embedding scorer embedding_dimensions must be positive")
    return payload, {
        "path": FROZEN_AUDIT_EMBEDDING_SCORER_PATH.as_posix(),
        "sha256": sha256_file(scorer_path),
        "scorer_version": FROZEN_AUDIT_EMBEDDING_SCORER_VERSION,
        "fit_mode": metadata.get("fit_mode"),
        "target": metadata.get("target"),
        "embedding_dimensions": dimensions,
        "loaded_after_confirmation": True,
    }


def execute_select(cur: Any, sql: str, params: Sequence[Any] = ()) -> None:
    stripped = str(sql).lstrip()
    if not stripped.lower().startswith("select") or _WRITE_SQL_RE.search(stripped):
        raise ScorerServingIOError("SQL guard allows SELECT statements only")
    referenced = {match.group(1).split(".")[-1] for match in _SOURCE_TABLE_RE.finditer(stripped)}
    forbidden = sorted(referenced - set(APPROVED_SOURCE_TABLES))
    if forbidden:
        raise ScorerServingIOError("SQL source allowlist violation: " + ", ".join(forbidden))
    cur.execute(stripped, tuple(params))


def query_ranking_run(conn: Any, *, ranking_run_id: str) -> dict[str, Any]:
    with conn.cursor(row_factory=dict_row) as cur:
        execute_select(
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
        raise ScorerServingIOError(f"ranking_run_id not found: {ranking_run_id}")
    return _json_safe(dict(row))


def query_candidate_inputs(
    conn: Any,
    *,
    ranking_run_id: str,
    family: str,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        execute_select(
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


def validate_ranking_run_row(row: Mapping[str, Any]) -> None:
    if row.get("ranking_run_id") != RANKING_RUN_ID:
        raise ScorerServingIOError("ranking_run_id row mismatch")
    if row.get("corpus_snapshot_version") != CORPUS_SNAPSHOT_VERSION:
        raise ScorerServingIOError("ranking_run corpus_snapshot_version mismatch")
    if row.get("embedding_version") != EMBEDDING_VERSION:
        raise ScorerServingIOError("ranking_run embedding_version mismatch")


def build_runtime_rows_from_live_reads(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    scorer_payload: Mapping[str, Any],
    scorer_summary: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if len(raw_rows) != EXPECTED_CANDIDATE_ROW_COUNT:
        raise ScorerServingIOError(
            f"incomplete live read coverage: expected {EXPECTED_CANDIDATE_ROW_COUNT}, got {len(raw_rows)}"
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
            raise ScorerServingIOError(f"candidate row {index} missing canonical OpenAlex ID")
        if canonical in seen:
            raise ScorerServingIOError(f"duplicate canonical OpenAlex work ID: {canonical}")
        seen.add(canonical)
        if row.get("ranking_run_id") != RANKING_RUN_ID:
            raise ScorerServingIOError(f"candidate row {canonical} ranking_run_id mismatch")
        if row.get("recommendation_family") != FAMILY:
            raise ScorerServingIOError(f"candidate row {canonical} family mismatch")
        if row.get("corpus_snapshot_version") != CORPUS_SNAPSHOT_VERSION:
            raise ScorerServingIOError(f"candidate row {canonical} corpus_snapshot_version mismatch")
        final_score = _float_or_none(row.get("final_score"))
        if final_score is None:
            raise ScorerServingIOError(f"candidate row {canonical} missing final_score")
        final_score_coverage += 1
        if row.get("observed_embedding_version") != EMBEDDING_VERSION or row.get("vector") is None:
            raise ScorerServingIOError(f"candidate row {canonical} missing approved embedding coverage")
        vector = _parse_vector(row.get("vector"))
        try:
            learned_probability = score_audit_embedding_probability(vector, scorer_payload)
        except MLOfflineAuditEmbeddingScorerExportError as exc:
            raise ScorerServingIOError(
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
        raise ScorerServingIOError("labels or holdout fields were loaded in live read rows")
    observed_sha = work_set_sha256(sorted(seen))
    if observed_sha != CANDIDATE_POOL_WORK_SET_SHA256:
        raise ScorerServingIOError(
            f"candidate_pool_work_set_sha256 mismatch: expected {CANDIDATE_POOL_WORK_SET_SHA256}, got {observed_sha}"
        )
    join_summary = {
        "joined_candidate_count": len(runtime_rows),
        "runtime_row_count": len(runtime_rows),
        "expected_candidate_count": EXPECTED_CANDIDATE_ROW_COUNT,
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

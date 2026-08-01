"""Apply frozen audit embedding probabilities for the second shadow surface.

This command reads local Postgres ranking, paper_scores, works, and embeddings
rows for the selected second shadow-generalization surface, applies the frozen
``ml-offline-audit-embedding-scorer-v2`` JSON scorer to existing embeddings,
and writes an isolated audit artifact. It does not write database rows, refit
or train a scorer, generate embeddings, rerun discovery, ingest labels, or
authorize shadow/runtime/production behavior.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.error_reporting import safe_exception_summary
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
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_learned_probability"
ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-surface-learned-probability-v1"

PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_second_surface_learned_probability_coverage_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-second-surface-learned-probability-coverage-plan-v1"
PLAN_NEXT_STAGE = "apply_second_surface_learned_probability_coverage_v1"
SECOND_SURFACE_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SECOND_SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"
DISCOVERY_STATUS = "selected_needs_learned_probability_coverage"
LABEL_DATASET_VERSION = "ml-label-dataset-v11"
EMBEDDINGS_ARTIFACT_TYPE = "ml_shadow_scorer_second_snapshot_embeddings"
EMBEDDINGS_ARTIFACT_VERSION = "ml-shadow-scorer-v1-second-snapshot-embeddings-v1"
SCORER_ARTIFACT_TYPE = "ml_offline_audit_embedding_scorer"
SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
SCORER_FIT_MODE = "holdout_bound_train_only"
SCORER_TARGET = "good_or_acceptable"

DEFAULT_RANKING_RUN_ID = "rank-83787b91ef"
DEFAULT_FAMILY = "emerging"
DEFAULT_CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
DEFAULT_EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
EXPECTED_CANDIDATE_POOL_WORK_COUNT = 528
EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT = 168
EXPECTED_POSITIVE_COUNT = 94
EXPECTED_NEGATIVE_COUNT = 74
EXPECTED_CANDIDATE_SHA = "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc"
RECOMMENDED_NEXT_STAGE = "extend_second_surface_probability_probe_and_rerun_discovery_v1"

_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)
_WRITE_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|merge|copy|grant|revoke|vacuum|call)\b",
    re.IGNORECASE,
)

CAVEATS = (
    "Offline audit artifact only.",
    "Applies the frozen scorer to existing embeddings; does not refit or train.",
    "Does not generate embeddings.",
    "Does not write database rows.",
    "Does not authorize online shadow, API/web, production default, or user-visible ranking changes.",
    "Labels are provenance only and are not scoring features.",
)


class MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _assert_local_database_url(database_url: str) -> dict[str, Any]:
    try:
        return dict(_assert_generalization_local_database_url(database_url))
    except MLShadowScorerGeneralizationSecondSurfaceError as exc:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(str(exc), code=exc.code) from exc


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


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


def _execute_select(cur: Any, sql: str, params: Sequence[Any] = ()) -> None:
    stripped = str(sql).lstrip()
    if not stripped.lower().startswith("select") or _WRITE_SQL_RE.search(stripped):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("SQL guard allows SELECT statements only")
    cur.execute(sql, tuple(params))


def _parse_vector(value: Any) -> list[float]:
    raw = value
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
                f"embedding vector string is not valid JSON: {exc}"
            ) from exc
    if not isinstance(raw, (list, tuple)):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("embedding vector must be an array")
    vector: list[float] = []
    for item in raw:
        try:
            number = float(item)
        except (TypeError, ValueError) as exc:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
                f"embedding vector contains non-numeric value: {item!r}"
            ) from exc
        if not math.isfinite(number):
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("embedding vector contains non-finite value")
        vector.append(number)
    return vector


def _dataset_version(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("dataset_version") or _get(payload, "metadata.dataset_version")
    return str(value) if value else None


def _validate_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="learned-probability-coverage-plan")
    if metadata.get("artifact_type") != PLAN_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("coverage plan metadata.artifact_type mismatch")
    if metadata.get("plan_version") != PLAN_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("coverage plan metadata.plan_version mismatch")
    if payload.get("recommended_next_stage") != PLAN_NEXT_STAGE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("coverage plan recommended_next_stage mismatch")
    contract = payload.get("learned_probability_coverage_contract")
    if not isinstance(contract, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("coverage plan missing learned_probability_coverage_contract")
    checks = {
        "approved_scorer": contract.get("approved_scorer") == SCORER_VERSION,
        "approved_embedding_version": contract.get("approved_embedding_version") == DEFAULT_EMBEDDING_VERSION,
        "must_not_refit": contract.get("must_not_refit") is True,
        "must_not_regenerate_embeddings": contract.get("must_not_regenerate_embeddings") is True,
        "must_not_use_v11_labels_as_scorer_features": contract.get("must_not_use_v11_labels_as_scorer_features") is True,
        "target_coverage.covered_work_count": _get(contract, "target_coverage.covered_work_count")
        == EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "target_coverage.candidate_pool_work_count": _get(contract, "target_coverage.candidate_pool_work_count")
        == EXPECTED_CANDIDATE_POOL_WORK_COUNT,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"coverage plan contract failed: {failed}")
    return metadata


def _validate_discovery(
    payload: Mapping[str, Any],
    *,
    ranking_run_id: str,
    family: str,
    corpus_snapshot_version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="generalization-second-surface")
    if metadata.get("artifact_type") != SECOND_SURFACE_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            "generalization second-surface metadata.artifact_type mismatch"
        )
    if metadata.get("surface_version") != SECOND_SURFACE_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            "generalization second-surface metadata.surface_version mismatch"
        )
    if _get(payload, "discovery_summary.status") != DISCOVERY_STATUS:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            f"discovery_summary.status must be {DISCOVERY_STATUS}"
        )
    selected = payload.get("selected_second_surface")
    if not isinstance(selected, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("selected_second_surface must be populated")
    expected = {
        "ranking_run_id": ranking_run_id,
        "family": family,
        "corpus_snapshot_version": corpus_snapshot_version,
        "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
        "confirmatory_metric_eligible_work_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
    }
    for key, value in expected.items():
        if selected.get(key) != value:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
                f"selected_second_surface.{key} must be {value}"
            )
    if _get(payload, "learned_probability_coverage.learned_probability_coverage_count") != 0:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            "discovery learned_probability_coverage_count must be 0 before apply"
        )
    if _get(payload, "learned_probability_coverage.approved_upstream_probability_probe.probe_status") != "not_found":
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("discovery probability probe_status must be not_found")
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any], discovery_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="label-dataset")
    if _dataset_version(payload) != LABEL_DATASET_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("label dataset version must be ml-label-dataset-v11")
    ingest = _get(payload, "metadata.shadow_generalization_second_surface_v1_ingest")
    if not isinstance(ingest, Mapping):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            "label dataset metadata.shadow_generalization_second_surface_v1_ingest missing"
        )
    if ingest.get("label_thresholds_passed") is not True:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("v11 label thresholds must pass")
    expected = {
        "labeled_count": EXPECTED_CONFIRMATORY_ELIGIBLE_COUNT,
        "positive_count": EXPECTED_POSITIVE_COUNT,
        "negative_count": EXPECTED_NEGATIVE_COUNT,
        "ranking_run_id": DEFAULT_RANKING_RUN_ID,
        "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
    }
    for key, value in expected.items():
        if ingest.get(key) != value:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"v11 shadow ingest {key} mismatch")
    if _get(discovery_payload, "label_coverage.work_level.confirmatory_labeled_work_count") != ingest.get("labeled_count"):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("v11 labeled count does not match discovery")
    if _get(discovery_payload, "label_coverage.work_level.confirmatory_positive_work_count") != ingest.get("positive_count"):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("v11 positive count does not match discovery")
    if _get(discovery_payload, "label_coverage.work_level.confirmatory_negative_work_count") != ingest.get("negative_count"):
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("v11 negative count does not match discovery")
    return metadata


def _validate_embeddings(
    payload: Mapping[str, Any],
    *,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="second-snapshot-embeddings")
    if metadata.get("artifact_type") != EMBEDDINGS_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("second-snapshot-embeddings artifact_type mismatch")
    if metadata.get("artifact_version") != EMBEDDINGS_ARTIFACT_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("second-snapshot-embeddings artifact_version mismatch")
    if metadata.get("embedding_version") != embedding_version:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("second-snapshot-embeddings embedding_version mismatch")
    if metadata.get("snapshot_version") != corpus_snapshot_version:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("second-snapshot-embeddings snapshot_version mismatch")
    if _get(payload, "coverage.embedded_work_count") != EXPECTED_CANDIDATE_POOL_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("second-snapshot-embeddings embedded_work_count mismatch")
    if _get(payload, "coverage.missing_embedding_count") != 0:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("second-snapshot-embeddings missing_embedding_count must be 0")
    return metadata


def _validate_scorer(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="offline-audit-embedding-scorer")
    if metadata.get("artifact_type") != SCORER_ARTIFACT_TYPE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("offline audit scorer artifact_type mismatch")
    if metadata.get("scorer_version") != SCORER_VERSION:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("offline audit scorer scorer_version mismatch")
    if metadata.get("fit_mode") != SCORER_FIT_MODE:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("offline audit scorer fit_mode mismatch")
    if metadata.get("target") != SCORER_TARGET:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("offline audit scorer target mismatch")
    dimensions = metadata.get("embedding_dimensions")
    if not isinstance(dimensions, int) or isinstance(dimensions, bool) or dimensions <= 0:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("offline audit scorer embedding_dimensions must be positive")
    return metadata


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
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"ranking_run_id not found: {ranking_run_id}")
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


def _validate_ranking_run_row(
    row: Mapping[str, Any],
    *,
    ranking_run_id: str,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> None:
    if row.get("ranking_run_id") != ranking_run_id:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("ranking_run_id row mismatch")
    if row.get("corpus_snapshot_version") != corpus_snapshot_version:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("ranking_run corpus_snapshot_version mismatch")
    if row.get("embedding_version") != embedding_version:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError("ranking_run embedding_version mismatch")


def _score_candidate_rows(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    scorer_payload: Mapping[str, Any],
    scorer_dimensions: int,
    ranking_run_id: str,
    family: str,
    corpus_snapshot_version: str,
    embedding_version: str,
    candidate_sha: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    if len(raw_rows) != EXPECTED_CANDIDATE_POOL_WORK_COUNT:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            f"candidate pool row count must be {EXPECTED_CANDIDATE_POOL_WORK_COUNT}, got {len(raw_rows)}"
        )
    seen: set[str] = set()
    final_score_coverage = 0
    embedding_coverage = 0
    scored_rows: list[dict[str, Any]] = []
    for index, row in enumerate(raw_rows, start=1):
        canonical = _canonical_from_value(row.get("openalex_id"))
        if not canonical:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"candidate row {index} missing canonical OpenAlex ID")
        if canonical in seen:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"duplicate canonical OpenAlex work ID: {canonical}")
        seen.add(canonical)
        final_score = _float_or_none(row.get("final_score"))
        if final_score is None:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"candidate row {canonical} missing final_score")
        final_score_coverage += 1
        if row.get("vector") is None:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(f"candidate row {canonical} missing embedding vector")
        vector = _parse_vector(row.get("vector"))
        if len(vector) != scorer_dimensions:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
                f"embedding vector length for {canonical} is {len(vector)}, expected {scorer_dimensions}"
            )
        embedding_coverage += 1
        try:
            probability = score_audit_embedding_probability(vector, scorer_payload)
        except MLOfflineAuditEmbeddingScorerExportError as exc:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(str(exc), code=exc.code) from exc
        scored_rows.append(
            {
                "canonical_openalex_work_id": canonical,
                "title": row.get("title"),
                "year": row.get("year"),
                "ranking_run_id": ranking_run_id,
                "family": family,
                "corpus_snapshot_version": corpus_snapshot_version,
                "final_score": final_score,
                "audit_embedding_probability_work": probability,
                "embedding_version": embedding_version,
                "scorer_version": SCORER_VERSION,
                "candidate_pool_work_set_sha256": candidate_sha,
            }
        )
    observed_sha = _work_set_sha256(sorted(seen))
    if observed_sha != candidate_sha:
        raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
            f"candidate_pool_work_set_sha256 mismatch: expected {candidate_sha}, got {observed_sha}"
        )
    scored_rows.sort(
        key=lambda row: (
            -float(row["audit_embedding_probability_work"]),
            -float(row["final_score"]),
            str(row["canonical_openalex_work_id"]),
        )
    )
    return scored_rows, {
        "candidate_pool_work_count": len(seen),
        "final_score_coverage_count": final_score_coverage,
        "embedding_coverage_count": embedding_coverage,
    }


def _quantile(sorted_values: Sequence[float], q: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = (len(sorted_values) - 1) * q
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return float(sorted_values[lower])
    weight = position - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    ordered = sorted(float(value) for value in values)
    return {
        "min": ordered[0] if ordered else None,
        "p25": _quantile(ordered, 0.25),
        "median": _quantile(ordered, 0.5),
        "p75": _quantile(ordered, 0.75),
        "max": ordered[-1] if ordered else None,
        "mean": float(mean(ordered)) if ordered else None,
        "count": len(ordered),
    }


def _blocked_payload(
    *,
    artifact_version: str,
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    ranking_run_id: str,
    family: str,
    corpus_snapshot_version: str,
    embedding_version: str,
    scorer_metadata: Mapping[str, Any],
    database_error: str,
) -> dict[str, Any]:
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "artifact_version": artifact_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "ranking_run_id": ranking_run_id,
            "family": family,
            "corpus_snapshot_version": corpus_snapshot_version,
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "embedding_version": embedding_version,
            "scorer_version": scorer_metadata.get("scorer_version"),
            "scorer_fit_mode": scorer_metadata.get("fit_mode"),
            "scorer_target": scorer_metadata.get("target"),
            "execution_mode": "offline_audit_file_only",
            "labels_used_for_scoring": False,
            "scorer_refit_performed": False,
            "embeddings_generated": False,
            "db_writes_performed": False,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "database_unavailable_error": database_error,
        },
        "source_contract": {},
        "execution_summary": {
            "status": "blocked_database_unavailable",
            "candidate_pool_work_count": 0,
            "output_row_count": 0,
            "learned_probability_coverage_count": 0,
            "missing_learned_probability_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "full_learned_probability_coverage": False,
            "scorer_execution_mode": "not_run_database_unavailable",
            "recommended_next_stage": "retry_second_surface_learned_probability_coverage_apply_v1",
        },
        "coverage_summary": {
            "final_score_coverage_count": 0,
            "embedding_coverage_count": 0,
            "learned_probability_coverage_count": 0,
            "missing_embedding_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "missing_probability_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
        },
        "score_distribution": {"audit_embedding_probability_work": _distribution([])},
        "candidate_work_scores": [],
        "sql_read_report": {
            "reads_enabled": False,
            "source_tables": ["ranking_runs", "paper_scores", "works", "embeddings"],
            "writes_enabled": False,
            "write_tables": [],
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "embeddings_written": False,
            "production_tables_modified": False,
        },
        "blocked_actions": _blocked_actions(),
        "shadow_and_production_blockers": _blockers(full_coverage=False),
        "recommended_next_stage": "retry_second_surface_learned_probability_coverage_apply_v1",
        "caveats": list(CAVEATS),
    }


def _blocked_actions() -> list[str]:
    return [
        "database_writes",
        "openai_calls",
        "openalex_calls",
        "ranking_run_creation",
        "embedding_generation",
        "scorer_refit_or_training",
        "label_ingest",
        "discovery_rerun",
        "online_shadow_execution",
        "api_web_change",
        "production_default_change",
    ]


def _blockers(*, full_coverage: bool) -> dict[str, Any]:
    return {
        "missing_second_surface_learned_probability_coverage": not full_coverage,
        "missing_generalization_audit_on_second_surface": True,
        "missing_generalization_audit_gates": True,
        "missing_online_shadow_implementation_disabled_by_default": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }


def _artifact_payload(
    *,
    artifact_version: str,
    inputs: list[dict[str, str]],
    database_summary: Mapping[str, Any],
    ranking_run_id: str,
    family: str,
    corpus_snapshot_version: str,
    embedding_version: str,
    scorer_metadata: Mapping[str, Any],
    source_contract: Mapping[str, Any],
    scored_rows: Sequence[Mapping[str, Any]],
    coverage_counts: Mapping[str, int],
) -> dict[str, Any]:
    full_coverage = len(scored_rows) == EXPECTED_CANDIDATE_POOL_WORK_COUNT
    probabilities = [float(row["audit_embedding_probability_work"]) for row in scored_rows]
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "artifact_version": artifact_version,
            "generated_at": _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "ranking_run_id": ranking_run_id,
            "family": family,
            "corpus_snapshot_version": corpus_snapshot_version,
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "embedding_version": embedding_version,
            "scorer_version": scorer_metadata.get("scorer_version"),
            "scorer_fit_mode": scorer_metadata.get("fit_mode"),
            "scorer_target": scorer_metadata.get("target"),
            "execution_mode": "offline_audit_file_only",
            "labels_used_for_scoring": False,
            "scorer_refit_performed": False,
            "embeddings_generated": False,
            "db_writes_performed": False,
            "runtime_implementation_authorized": False,
            "online_shadow_execution_enabled": False,
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
        },
        "source_contract": dict(source_contract),
        "execution_summary": {
            "status": "succeeded",
            "candidate_pool_work_count": EXPECTED_CANDIDATE_POOL_WORK_COUNT,
            "output_row_count": len(scored_rows),
            "learned_probability_coverage_count": len(scored_rows),
            "missing_learned_probability_count": max(0, EXPECTED_CANDIDATE_POOL_WORK_COUNT - len(scored_rows)),
            "full_learned_probability_coverage": full_coverage,
            "scorer_execution_mode": "frozen_apply_only",
            "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        },
        "coverage_summary": {
            "final_score_coverage_count": int(coverage_counts["final_score_coverage_count"]),
            "embedding_coverage_count": int(coverage_counts["embedding_coverage_count"]),
            "learned_probability_coverage_count": len(scored_rows),
            "missing_embedding_count": max(0, EXPECTED_CANDIDATE_POOL_WORK_COUNT - int(coverage_counts["embedding_coverage_count"])),
            "missing_probability_count": max(0, EXPECTED_CANDIDATE_POOL_WORK_COUNT - len(scored_rows)),
        },
        "score_distribution": {
            "audit_embedding_probability_work": _distribution(probabilities),
        },
        "candidate_work_scores": [dict(row) for row in scored_rows],
        "sql_read_report": {
            "reads_enabled": True,
            "source_tables": ["ranking_runs", "paper_scores", "works", "embeddings"],
            "writes_enabled": False,
            "write_tables": [],
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "embeddings_written": False,
            "production_tables_modified": False,
        },
        "blocked_actions": _blocked_actions(),
        "shadow_and_production_blockers": _blockers(full_coverage=full_coverage),
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def build_ml_shadow_scorer_second_surface_learned_probability_apply_payload(
    conn: Any | None,
    *,
    learned_probability_coverage_plan_path: Path,
    generalization_second_surface_path: Path,
    label_dataset_path: Path,
    second_snapshot_embeddings_path: Path,
    offline_audit_embedding_scorer_path: Path,
    database_url: str | None = None,
    ranking_run_id: str = DEFAULT_RANKING_RUN_ID,
    family: str = DEFAULT_FAMILY,
    corpus_snapshot_version: str = DEFAULT_CORPUS_SNAPSHOT_VERSION,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
    database_unavailable_error: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(learned_probability_coverage_plan_path).resolve()
    discovery_path = Path(generalization_second_surface_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    embeddings_path = Path(second_snapshot_embeddings_path).resolve()
    scorer_path = Path(offline_audit_embedding_scorer_path).resolve()

    plan_payload = _load_json_object(plan_path)
    discovery_payload = _load_json_object(discovery_path)
    label_payload = _load_json_object(label_path)
    embeddings_payload = _load_json_object(embeddings_path)
    scorer_payload = _load_json_object(scorer_path)

    _validate_plan(plan_payload)
    _validate_discovery(
        discovery_payload,
        ranking_run_id=ranking_run_id,
        family=family,
        corpus_snapshot_version=corpus_snapshot_version,
    )
    _validate_label_dataset(label_payload, discovery_payload)
    _validate_embeddings(
        embeddings_payload,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
    )
    scorer_metadata = _validate_scorer(scorer_payload)
    inputs = [
        _input_record("learned_probability_coverage_plan", plan_path, repo_root=root),
        _input_record("generalization_second_surface", discovery_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("second_snapshot_embeddings", embeddings_path, repo_root=root),
        _input_record("offline_audit_embedding_scorer", scorer_path, repo_root=root),
    ]
    if database_url is None:
        database_summary = {
            "database_target_redacted": None,
            "read_only_contract": "SELECT-only queries; no database mutations",
            "local_database_url_confirmed": None,
        }
    else:
        database_summary = _assert_local_database_url(database_url)
    if conn is None:
        return _blocked_payload(
            artifact_version=artifact_version,
            inputs=inputs,
            database_summary=database_summary,
            ranking_run_id=ranking_run_id,
            family=family,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            scorer_metadata=scorer_metadata,
            database_error=database_unavailable_error or "local database unavailable",
        )

    ranking_row = _query_ranking_run(conn, ranking_run_id=ranking_run_id)
    _validate_ranking_run_row(
        ranking_row,
        ranking_run_id=ranking_run_id,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
    )
    raw_rows = _query_candidate_inputs(
        conn,
        ranking_run_id=ranking_run_id,
        family=family,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
    )
    scored_rows, coverage_counts = _score_candidate_rows(
        raw_rows,
        scorer_payload=scorer_payload,
        scorer_dimensions=int(scorer_metadata["embedding_dimensions"]),
        ranking_run_id=ranking_run_id,
        family=family,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        candidate_sha=EXPECTED_CANDIDATE_SHA,
    )
    source_contract = {
        **dict(plan_payload["learned_probability_coverage_contract"]),
        "frozen_apply_only": True,
        "labels_not_used_for_scoring": True,
        "no_db_writes": True,
    }
    return _artifact_payload(
        artifact_version=artifact_version,
        inputs=inputs,
        database_summary=database_summary,
        ranking_run_id=ranking_run_id,
        family=family,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
        scorer_metadata=scorer_metadata,
        source_contract=source_contract,
        scored_rows=scored_rows,
        coverage_counts=coverage_counts,
    )


def markdown_from_ml_shadow_scorer_second_surface_learned_probability_apply(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    summary = payload["execution_summary"]
    coverage = payload["coverage_summary"]
    distribution = payload["score_distribution"]["audit_embedding_probability_work"]
    blockers = payload["shadow_and_production_blockers"]
    rows = payload.get("candidate_work_scores") if isinstance(payload.get("candidate_work_scores"), list) else []
    lines = [
        f"# Second-Surface Learned Probability Coverage ({metadata['artifact_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact applies the frozen offline audit embedding scorer to existing second-surface embeddings and writes audit-only learned probabilities. It does not refit, train, generate embeddings, write database rows, rerun discovery, or authorize shadow/production behavior.",
        "",
        f"- Status: `{summary['status']}`",
        f"- Ranking run: `{metadata['ranking_run_id']}`",
        f"- Family: `{metadata['family']}`",
        f"- Candidate SHA: `{metadata['candidate_pool_work_set_sha256']}`",
        f"- Candidate pool: {summary['candidate_pool_work_count']}",
        f"- Learned-probability coverage: {summary['learned_probability_coverage_count']} / {summary['candidate_pool_work_count']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Evidence Chain",
        "",
        f"- Corpus snapshot: `{metadata['corpus_snapshot_version']}`",
        f"- Embedding version: `{metadata['embedding_version']}`",
        f"- Scorer version: `{metadata['scorer_version']}`",
        f"- Scorer fit mode: `{metadata['scorer_fit_mode']}`",
        f"- Scorer target: `{metadata['scorer_target']}`",
        f"- Labels used for scoring: {metadata['labels_used_for_scoring']}",
        "",
        "## Frozen Scorer Contract",
        "",
    ]
    for key, value in payload["source_contract"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## DB Read Scope",
            "",
            f"- Reads enabled: {payload['sql_read_report']['reads_enabled']}",
            f"- Source tables: {', '.join(payload['sql_read_report']['source_tables'])}",
            f"- Writes enabled: {payload['sql_read_report']['writes_enabled']}",
            f"- Write tables: {payload['sql_read_report']['write_tables']}",
            "",
            "## Coverage Counts",
            "",
            f"- Final score coverage: {coverage['final_score_coverage_count']} / {summary['candidate_pool_work_count']}",
            f"- Embedding coverage: {coverage['embedding_coverage_count']} / {summary['candidate_pool_work_count']}",
            f"- Learned probability coverage: {coverage['learned_probability_coverage_count']} / {summary['candidate_pool_work_count']}",
            f"- Missing embedding count: {coverage['missing_embedding_count']}",
            f"- Missing probability count: {coverage['missing_probability_count']}",
            "",
            "## Probability Distribution",
            "",
        ]
    )
    for key, value in distribution.items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Top 20 Probability Preview", "", "| Rank | Work | Probability | Final score | Title |", "| ---: | --- | ---: | ---: | --- |"])
    for index, row in enumerate(rows[:20], start=1):
        title = str(row.get("title") or "").replace("|", "\\|")
        lines.append(
            f"| {index} | `{row.get('canonical_openalex_work_id')}` | "
            f"{float(row.get('audit_embedding_probability_work') or 0.0):.6f} | "
            f"{float(row.get('final_score') or 0.0):.6f} | {title} |"
        )
    lines.extend(["", "## Blockers", ""])
    for key, value in blockers.items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Next Rerun Command",
            "",
            "After extending the discovery probe to include this artifact, rerun `ml-shadow-scorer-generalization-second-surface` pinned to `rank-83787b91ef` with `ml-label-dataset-v11.json`.",
            "",
            "## Caveats",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def _connect_readonly(database_url: str) -> Any:
    return psycopg.connect(database_url, autocommit=True, options="-c default_transaction_read_only=on")


def write_ml_shadow_scorer_second_surface_learned_probability_apply(
    *,
    learned_probability_coverage_plan_path: Path,
    generalization_second_surface_path: Path,
    label_dataset_path: Path,
    second_snapshot_embeddings_path: Path,
    offline_audit_embedding_scorer_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
    ranking_run_id: str = DEFAULT_RANKING_RUN_ID,
    family: str = DEFAULT_FAMILY,
    corpus_snapshot_version: str = DEFAULT_CORPUS_SNAPSHOT_VERSION,
    embedding_version: str = DEFAULT_EMBEDDING_VERSION,
    artifact_version: str = ARTIFACT_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    explicit_database_url = database_url is not None
    db_url = database_url or _database_url_from_env()
    database_summary = _assert_local_database_url(db_url)
    conn: Any | None = None
    database_unavailable_error: str | None = None
    try:
        conn = _connect_readonly(db_url)
    except Exception as exc:  # pragma: no cover - exact driver exception varies by environment
        if explicit_database_url:
            raise MLShadowScorerSecondSurfaceLearnedProbabilityApplyError(
                f"local database unavailable: {safe_exception_summary(exc)}"
            ) from exc
        database_unavailable_error = safe_exception_summary(exc)
    try:
        payload = build_ml_shadow_scorer_second_surface_learned_probability_apply_payload(
            conn,
            learned_probability_coverage_plan_path=learned_probability_coverage_plan_path,
            generalization_second_surface_path=generalization_second_surface_path,
            label_dataset_path=label_dataset_path,
            second_snapshot_embeddings_path=second_snapshot_embeddings_path,
            offline_audit_embedding_scorer_path=offline_audit_embedding_scorer_path,
            database_url=db_url,
            ranking_run_id=ranking_run_id,
            family=family,
            corpus_snapshot_version=corpus_snapshot_version,
            embedding_version=embedding_version,
            artifact_version=artifact_version,
            repo_root=repo_root,
            database_unavailable_error=database_unavailable_error,
        )
        payload["metadata"]["database_target_redacted"] = database_summary.get("database_target_redacted")
    finally:
        if conn is not None:
            conn.close()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_second_surface_learned_probability_apply(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "ARTIFACT_VERSION",
    "MLShadowScorerSecondSurfaceLearnedProbabilityApplyError",
    "build_ml_shadow_scorer_second_surface_learned_probability_apply_payload",
    "markdown_from_ml_shadow_scorer_second_surface_learned_probability_apply",
    "write_ml_shadow_scorer_second_surface_learned_probability_apply",
]

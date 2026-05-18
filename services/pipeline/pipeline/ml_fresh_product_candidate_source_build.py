"""Build a fresh product-candidate source artifact for hybrid validation.

This command is an offline eval/source builder. In v1 the expected mode is
artifact_only_freeze: it reads local Postgres with SELECT-only queries, unions
existing product-candidate pools where useful, and writes a frozen JSON source
artifact. It does not create ranking runs, write database rows, score hybrid
arms, train models, generate embeddings, import labels, or authorize
shadow/prod.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_product_candidate_source_build"
BUILD_VERSION = "ml-fresh-product-candidate-source-build-v1"
EXPANSION_PLAN_ARTIFACT_TYPE = "ml_fresh_candidate_source_expansion_plan"
EXPANSION_PLAN_VERSION = "ml-fresh-candidate-source-expansion-plan-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
TARGET = "good_or_acceptable"
DEFAULT_FAMILY = "emerging"
DEFAULT_MODE = "artifact_only_freeze"
OLD_RANKING_RUN_ID = "rank-ee2ba6c816"
OLD_EVAL_WORK_SET_SHA256 = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
UNDERPOWERED_RANKING_RUN_ID = "rank-3904fec89d"
UNDERPOWERED_WORK_SET_SHA256 = "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926"

SOURCE_FIELDS_CHECKED_IN_ORDER = ("work_id", "openalex_work_id", "paper_id")
_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

CAVEATS = (
    "Artifact-only candidate source build; no production ranking/default changes.",
    "Read-only local Postgres SELECTs in v1; no database writes are performed.",
    "No hybrid scoring, training, embeddings, label import, API/web, shadow, or production changes.",
    "Old 217-work surface overlaps are excluded from confirmatory denominators.",
    "Underpowered 44-work source overlaps are tagged and separated from incremental expansion counts.",
    "Label readiness is informational only and does not authorize hybrid validation.",
    "No shadow or production authorization.",
)


class MLFreshProductCandidateSourceBuildError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _database_url_from_env() -> str:
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    user = os.environ.get("PGUSER", "research_radar")
    password = os.environ.get("PGPASSWORD", "research_radar")
    db = os.environ.get("PGDATABASE", "research_radar")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _redacted_database_url(database_url: str) -> str:
    parsed = urlparse(str(database_url or ""))
    if not parsed.scheme:
        return "(unparseable local database target)"
    user = parsed.username or ""
    userinfo = f"{user}:***@" if user else ""
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{userinfo}{parsed.hostname or ''}{port}{parsed.path or ''}"


def assert_local_database_url(database_url: str) -> dict[str, Any]:
    text = str(database_url or "").strip()
    if not text:
        raise MLFreshProductCandidateSourceBuildError("database URL is required")
    lower = text.lower()
    forbidden_hosts = ("railway", "rlwy", "render.com", "amazonaws", "neon.tech", "supabase", "herokuapp", "azure.com")
    if any(token in lower for token in forbidden_hosts):
        raise MLFreshProductCandidateSourceBuildError(
            "database URL must target local Postgres, not hosted production infrastructure"
        )
    parsed = urlparse(text)
    host = parsed.hostname
    local_hosts = {None, "", "localhost", "127.0.0.1", "::1", "host.docker.internal"}
    if host not in local_hosts and not str(host).endswith(".local"):
        raise MLFreshProductCandidateSourceBuildError(
            f"database URL must target local Postgres; host {host!r} is not allowed"
        )
    return {
        "database_target_redacted": _redacted_database_url(text),
        "database_url_host": host or "(local socket)",
        "database_url_port": parsed.port,
        "database_name": (parsed.path or "").lstrip("/") or None,
        "local_database_url_confirmed": True,
        "read_only_contract": "SELECT-only queries; no database mutations",
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshProductCandidateSourceBuildError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshProductCandidateSourceBuildError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshProductCandidateSourceBuildError(f"{name} JSON missing metadata object")
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
        raise MLFreshProductCandidateSourceBuildError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _canonical_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    match = _WORK_ID_RE.search(text)
    return match.group(1).upper() if match else None


def _canonical_work_id_from_label(row: Mapping[str, Any]) -> str | None:
    for field in SOURCE_FIELDS_CHECKED_IN_ORDER:
        canonical = _canonical_from_value(row.get(field))
        if canonical:
            return canonical
    return None


def _work_set_sha256(work_ids: Sequence[str]) -> str:
    lines = "".join(f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()}))
    return hashlib.sha256(lines.encode("utf-8")).hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _label_row_is_explicit(row: Mapping[str, Any]) -> bool:
    return row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()})


def _policy_input_record(policy_metadata: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    inputs = policy_metadata.get("inputs")
    if not isinstance(inputs, list):
        return None
    return next((item for item in inputs if isinstance(item, Mapping) and item.get("name") == name), None)


def _resolve_policy_input(record: Mapping[str, Any], *, repo_root: Path) -> Path:
    raw_path = str(record.get("path") or "").strip()
    if not raw_path:
        raise MLFreshProductCandidateSourceBuildError("policy input record missing path")
    path = Path(raw_path)
    resolved = path if path.is_absolute() else (repo_root / path)
    resolved = resolved.resolve()
    if not resolved.exists():
        raise MLFreshProductCandidateSourceBuildError(f"policy input path does not exist: {raw_path}")
    expected_sha = str(record.get("sha256") or "").strip()
    if expected_sha and sha256_file(resolved) != expected_sha:
        raise MLFreshProductCandidateSourceBuildError(f"policy input SHA mismatch for {raw_path}")
    return resolved


def _old_eval_ids_from_scoring(scoring_payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    rows = scoring_payload.get("candidate_pool_rows")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    return out


def _old_eval_ids_from_assignment(assignment_payload: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    for section in ("work_assignments", "assignments"):
        rows = assignment_payload.get(section)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if isinstance(row, Mapping) and row.get("assignment") == "eval":
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    return out


def _old_eval_work_ids(policy_metadata: Mapping[str, Any], *, repo_root: Path) -> set[str]:
    old_ids: set[str] = set()
    scoring_record = _policy_input_record(policy_metadata, "production_candidate_scoring")
    assignment_record = _policy_input_record(policy_metadata, "holdout_assignment")
    if scoring_record is not None:
        old_ids.update(_old_eval_ids_from_scoring(_load_json_object(_resolve_policy_input(scoring_record, repo_root=repo_root))))
    if assignment_record is not None:
        old_ids.update(_old_eval_ids_from_assignment(_load_json_object(_resolve_policy_input(assignment_record, repo_root=repo_root))))
    if not old_ids:
        raise MLFreshProductCandidateSourceBuildError(
            "could not reconstruct old 217-work eval IDs from policy production_candidate_scoring/holdout_assignment inputs"
        )
    if _work_set_sha256(old_ids) != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshProductCandidateSourceBuildError("old eval work IDs from policy inputs do not match disallowed SHA")
    return old_ids


def _validate_expansion_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-candidate-source-expansion-plan")
    if metadata.get("artifact_type") != EXPANSION_PLAN_ARTIFACT_TYPE:
        raise MLFreshProductCandidateSourceBuildError(
            f"expected expansion plan metadata.artifact_type={EXPANSION_PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != EXPANSION_PLAN_VERSION:
        raise MLFreshProductCandidateSourceBuildError(
            f"expected expansion plan metadata.plan_version={EXPANSION_PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    next_stage = payload.get("recommended_next_stage")
    primary_next_stage = _get(payload, "recommended_next_stages.0.stage")
    if next_stage != "implement_or_run_fresh_product_candidate_source_build_v1" and primary_next_stage != "implement_or_run_fresh_product_candidate_source_build_v1":
        raise MLFreshProductCandidateSourceBuildError(
            "expansion plan must recommend implement_or_run_fresh_product_candidate_source_build_v1"
        )
    if _get(payload, "current_blocker_summary.candidate_gap") != 56:
        raise MLFreshProductCandidateSourceBuildError("expansion plan must document candidate_gap == 56")
    if _get(payload, "current_blocker_summary.best_source_ranking_run_id") != UNDERPOWERED_RANKING_RUN_ID:
        raise MLFreshProductCandidateSourceBuildError(
            f"expansion plan best source must be {UNDERPOWERED_RANKING_RUN_ID}"
        )
    if _get(payload, "current_blocker_summary.best_source_confirmatory_eligible_work_count") != 44:
        raise MLFreshProductCandidateSourceBuildError("expansion plan must document best source has 44 eligible works")
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, float | int]]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshProductCandidateSourceBuildError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshProductCandidateSourceBuildError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    if metadata.get("disallowed_eval_work_set_sha256") != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshProductCandidateSourceBuildError("policy disallowed old 217 eval_work_set_sha256 is missing or unexpected")
    thresholds = _get(payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLFreshProductCandidateSourceBuildError("fresh surface policy missing label thresholds")
    keys = (
        "minimum_candidate_work_count",
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_label_coverage_rate",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
    )
    out: dict[str, float | int] = {}
    for key in keys:
        value = thresholds.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise MLFreshProductCandidateSourceBuildError(f"policy threshold {key} must be numeric")
        out[key] = value
    return metadata, out


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshProductCandidateSourceBuildError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLFreshProductCandidateSourceBuildError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _label_groups(label_rows: Sequence[Mapping[str, Any]]) -> dict[str, list[bool]]:
    groups: dict[str, list[bool]] = defaultdict(list)
    for row in label_rows:
        if not _label_row_is_explicit(row):
            continue
        if not isinstance(row.get(TARGET), bool):
            continue
        canonical = _canonical_work_id_from_label(row)
        if canonical:
            groups[canonical].append(bool(row[TARGET]))
    return groups


def _query_candidate_source_rows(conn: Any, family: str) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ps.ranking_run_id,
                COUNT(*) AS paper_scores_row_count,
                rr.status,
                rr.ranking_version,
                rr.corpus_snapshot_version,
                rr.embedding_version,
                rr.started_at,
                rr.finished_at,
                rr.config_json,
                rr.counts_json
            FROM paper_scores ps
            LEFT JOIN ranking_runs rr ON rr.ranking_run_id = ps.ranking_run_id
            WHERE ps.recommendation_family = %s
            GROUP BY ps.ranking_run_id, rr.status, rr.ranking_version, rr.corpus_snapshot_version,
                     rr.embedding_version, rr.started_at, rr.finished_at, rr.config_json, rr.counts_json
            ORDER BY
                CASE WHEN rr.status = 'succeeded' THEN 0 ELSE 1 END ASC,
                rr.finished_at DESC NULLS LAST,
                rr.started_at DESC NULLS LAST,
                ps.ranking_run_id DESC
            """,
            (family,),
        )
        return [_json_safe(dict(row)) for row in cur.fetchall()]


def _query_ranking_run_metadata(conn: Any, ranking_run_id: str) -> dict[str, Any] | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version,
                   status, started_at, finished_at, config_json, counts_json, notes
            FROM ranking_runs
            WHERE ranking_run_id = %s
            """,
            (ranking_run_id,),
        )
        row = cur.fetchone()
    return _json_safe(dict(row)) if row is not None else None


def _query_candidate_pool(conn: Any, *, ranking_run_id: str, family: str) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                ps.ranking_run_id,
                ps.work_id AS internal_work_id,
                ps.recommendation_family,
                ps.semantic_score,
                ps.citation_velocity_score,
                ps.topic_growth_score,
                ps.bridge_score,
                ps.diversity_penalty,
                ps.final_score,
                ps.bridge_eligible,
                ps.reason_short,
                w.openalex_id,
                w.title,
                w.year,
                w.citation_count,
                w.inclusion_status,
                w.corpus_snapshot_version
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            WHERE ps.ranking_run_id = %s
              AND ps.recommendation_family = %s
            ORDER BY ps.final_score DESC, ps.work_id ASC
            """,
            (ranking_run_id, family),
        )
        raw_rows = [_json_safe(dict(row)) for row in cur.fetchall()]
    rows: list[dict[str, Any]] = []
    for idx, row in enumerate(raw_rows, start=1):
        canonical = _canonical_from_value(row.get("openalex_id"))
        rows.append(
            {
                "ranking_run_id": ranking_run_id,
                "family": row.get("recommendation_family"),
                "heuristic_rank": idx,
                "internal_work_id": row.get("internal_work_id"),
                "openalex_id": row.get("openalex_id"),
                "canonical_openalex_work_id": canonical,
                "title": row.get("title"),
                "year": row.get("year"),
                "citation_count": row.get("citation_count"),
                "inclusion_status": row.get("inclusion_status"),
                "corpus_snapshot_version": row.get("corpus_snapshot_version"),
                "final_score": _float_or_none(row.get("final_score")),
                "semantic_score": _float_or_none(row.get("semantic_score")),
                "citation_velocity_score": _float_or_none(row.get("citation_velocity_score")),
                "topic_growth_score": _float_or_none(row.get("topic_growth_score")),
                "bridge_score": _float_or_none(row.get("bridge_score")),
                "diversity_penalty": _float_or_none(row.get("diversity_penalty")),
                "bridge_eligible": row.get("bridge_eligible"),
                "reason_short": row.get("reason_short"),
            }
        )
    return rows


def _candidate_work_ids(candidate_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({str(row["canonical_openalex_work_id"]) for row in candidate_rows if row.get("canonical_openalex_work_id")})


def _label_coverage_summary(confirmatory_ids: set[str], label_groups: Mapping[str, Sequence[bool]]) -> dict[str, Any]:
    labeled_ids = sorted(work_id for work_id in confirmatory_ids if work_id in label_groups)
    positive_ids = sorted(work_id for work_id in labeled_ids if any(label_groups[work_id]))
    negative_ids = sorted(work_id for work_id in labeled_ids if label_groups[work_id] and all(not value for value in label_groups[work_id]))
    conflict_ids = sorted(work_id for work_id in labeled_ids if any(label_groups[work_id]) and any(not value for value in label_groups[work_id]))
    duplicate_ids = sorted(work_id for work_id in labeled_ids if len(label_groups[work_id]) > 1)
    eligible_count = len(confirmatory_ids)
    labeled_count = len(labeled_ids)
    return {
        "labeled_work_count": labeled_count,
        "unlabeled_work_count": max(0, eligible_count - labeled_count),
        "label_coverage_rate": labeled_count / eligible_count if eligible_count else 0.0,
        "positive_labeled_work_count": len(positive_ids),
        "negative_labeled_work_count": len(negative_ids),
        "distinct_negative_work_count": len(negative_ids),
        "conflicting_target_work_group_count": len(conflict_ids),
        "duplicate_labeled_work_group_count": len(duplicate_ids),
        "conflicting_work_ids_preview": conflict_ids[:25],
        "duplicate_work_ids_preview": duplicate_ids[:25],
    }


def _threshold_check(
    *,
    confirmatory_eligible_work_count: int,
    label_summary: Mapping[str, Any],
    min_confirmatory_eligible_works: int,
    policy_thresholds: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    checks = {
        "minimum_confirmatory_candidate_work_count": (
            confirmatory_eligible_work_count,
            min_confirmatory_eligible_works,
        ),
        "minimum_confirmatory_labeled_work_count": (
            label_summary.get("labeled_work_count"),
            policy_thresholds.get("minimum_confirmatory_labeled_work_count"),
        ),
        "minimum_confirmatory_label_coverage_rate": (
            label_summary.get("label_coverage_rate"),
            policy_thresholds.get("minimum_confirmatory_label_coverage_rate"),
        ),
        "minimum_confirmatory_positive_work_count": (
            label_summary.get("positive_labeled_work_count"),
            policy_thresholds.get("minimum_confirmatory_positive_work_count"),
        ),
        "minimum_confirmatory_negative_work_count": (
            label_summary.get("negative_labeled_work_count"),
            policy_thresholds.get("minimum_confirmatory_negative_work_count"),
        ),
        "minimum_distinct_negative_work_count": (
            label_summary.get("distinct_negative_work_count"),
            policy_thresholds.get("minimum_distinct_negative_work_count"),
        ),
    }
    out: dict[str, dict[str, Any]] = {}
    for key, (observed, threshold) in checks.items():
        passed = (
            isinstance(observed, (int, float))
            and not isinstance(observed, bool)
            and isinstance(threshold, (int, float))
            and not isinstance(threshold, bool)
            and float(observed) >= float(threshold)
        )
        out[key] = {"observed": observed, "threshold": threshold, "passed": passed}
    return out


def _source_recency_key(source: Mapping[str, Any]) -> tuple[str, str, str]:
    metadata = source.get("ranking_run_metadata") if isinstance(source.get("ranking_run_metadata"), Mapping) else source
    return (
        str(metadata.get("finished_at") or ""),
        str(metadata.get("started_at") or ""),
        str(source.get("ranking_run_id") or ""),
    )


def _source_summary(
    *,
    source: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any] | None,
    family: str,
    old_eval_ids: set[str],
    underpowered_ids: set[str],
    label_groups: Mapping[str, Sequence[bool]],
    min_confirmatory_eligible_works: int,
    policy_thresholds: Mapping[str, Any],
) -> dict[str, Any]:
    rid = str(source.get("ranking_run_id") or "")
    work_ids = _candidate_work_ids(candidate_rows)
    candidate_sha = _work_set_sha256(work_ids) if work_ids else None
    old_overlap_ids = sorted(set(work_ids).intersection(old_eval_ids))
    underpowered_overlap_ids = sorted(set(work_ids).intersection(underpowered_ids))
    confirmatory_ids = sorted(set(work_ids).difference(old_eval_ids))
    label_summary = _label_coverage_summary(set(confirmatory_ids), label_groups)
    corpus_versions = sorted({str(row.get("corpus_snapshot_version")) for row in candidate_rows if row.get("corpus_snapshot_version")})
    meta = dict(metadata or {})
    corpus_snapshot_version = meta.get("corpus_snapshot_version") or (corpus_versions[0] if len(corpus_versions) == 1 else None)
    summary: dict[str, Any] = {
        "ranking_run_id": rid,
        "family": family,
        "corpus_snapshot_version": corpus_snapshot_version,
        "ranking_run_metadata": _json_safe(meta) if meta else None,
        "paper_scores_row_count": len(candidate_rows),
        "candidate_work_count": len(work_ids),
        "canonical_openalex_work_count": len(work_ids),
        "candidate_work_set_sha256": candidate_sha,
        "overlap_with_old_217_count": len(old_overlap_ids),
        "underpowered_source_overlap_count": len(underpowered_overlap_ids),
        "confirmatory_eligible_work_count": len(confirmatory_ids),
        "source_is_fresh_relative_to_old_217": rid != OLD_RANKING_RUN_ID and candidate_sha != OLD_EVAL_WORK_SET_SHA256,
        "is_current_underpowered_freeze": rid == UNDERPOWERED_RANKING_RUN_ID or candidate_sha == UNDERPOWERED_WORK_SET_SHA256,
        "label_coverage_summary": label_summary,
        "threshold_check": _threshold_check(
            confirmatory_eligible_work_count=len(confirmatory_ids),
            label_summary=label_summary,
            min_confirmatory_eligible_works=min_confirmatory_eligible_works,
            policy_thresholds=policy_thresholds,
        ),
    }
    summary["selection_blockers"] = []
    if summary["source_is_fresh_relative_to_old_217"] is not True:
        summary["selection_blockers"].append("old_217_surface_not_fresh")
    if len(confirmatory_ids) < min_confirmatory_eligible_works:
        summary["selection_blockers"].append("confirmatory_eligible_work_count_below_minimum")
    return summary


def _ranked_sources_with_rows(
    conn: Any,
    *,
    family: str,
    old_eval_ids: set[str],
    underpowered_ids: set[str],
    label_groups: Mapping[str, Sequence[bool]],
    min_confirmatory_eligible_works: int,
    policy_thresholds: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    source_rows = _query_candidate_source_rows(conn, family)
    considered: list[dict[str, Any]] = []
    rows_by_source: dict[str, list[dict[str, Any]]] = {}
    for source in source_rows:
        rid = str(source.get("ranking_run_id") or "").strip()
        if not rid:
            continue
        metadata = _query_ranking_run_metadata(conn, rid) or source
        candidate_rows = _query_candidate_pool(conn, ranking_run_id=rid, family=family)
        rows_by_source[rid] = candidate_rows
        considered.append(
            _source_summary(
                source=source,
                candidate_rows=candidate_rows,
                metadata=metadata,
                family=family,
                old_eval_ids=old_eval_ids,
                underpowered_ids=underpowered_ids,
                label_groups=label_groups,
                min_confirmatory_eligible_works=min_confirmatory_eligible_works,
                policy_thresholds=policy_thresholds,
            )
        )
    considered.sort(
        key=lambda item: (
            int(item.get("confirmatory_eligible_work_count") or 0),
            _source_recency_key(item),
        ),
        reverse=True,
    )
    return considered, rows_by_source


def _union_candidate_rows(
    considered: Sequence[Mapping[str, Any]],
    rows_by_source: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    old_eval_ids: set[str],
    underpowered_ids: set[str],
    family: str,
) -> list[dict[str, Any]]:
    source_order = sorted(considered, key=_source_recency_key, reverse=True)
    by_work: dict[str, dict[str, Any]] = {}
    memberships: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in source_order:
        rid = str(source.get("ranking_run_id") or "")
        if not rid or source.get("source_is_fresh_relative_to_old_217") is not True:
            continue
        for row in rows_by_source.get(rid, []):
            canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
            if canonical is None:
                continue
            memberships[canonical].append(
                {
                    "ranking_run_id": rid,
                    "family": row.get("family") or family,
                    "heuristic_rank": row.get("heuristic_rank"),
                    "final_score": row.get("final_score"),
                    "corpus_snapshot_version": row.get("corpus_snapshot_version"),
                }
            )
            previous = by_work.get(canonical)
            candidate = dict(row)
            candidate["canonical_openalex_work_id"] = canonical
            if previous is None:
                by_work[canonical] = candidate
                continue
            prev_key = (
                str(previous.get("_source_finished_at") or ""),
                float(previous.get("final_score") or 0.0),
                str(previous.get("ranking_run_id") or ""),
            )
            candidate_key = (
                str(source.get("ranking_run_metadata", {}).get("finished_at") if isinstance(source.get("ranking_run_metadata"), Mapping) else ""),
                float(candidate.get("final_score") or 0.0),
                rid,
            )
            if candidate_key > prev_key:
                by_work[canonical] = candidate
    rows: list[dict[str, Any]] = []
    for idx, work_id in enumerate(sorted(by_work), start=1):
        row = dict(by_work[work_id])
        row.pop("_source_finished_at", None)
        row["artifact_source_rank"] = idx
        row["source_memberships"] = sorted(
            memberships[work_id],
            key=lambda item: (str(item.get("ranking_run_id") or ""), int(item.get("heuristic_rank") or 0)),
        )
        row["source_membership_count"] = len(memberships[work_id])
        row["previous_eval_overlap"] = work_id in old_eval_ids
        row["underpowered_source_overlap"] = work_id in underpowered_ids
        row["confirmatory_metric_eligible"] = work_id not in old_eval_ids
        row["incremental_expansion_work"] = work_id not in old_eval_ids and work_id not in underpowered_ids
        rows.append(row)
    return rows


def _strategy_summaries(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    considered: Sequence[Mapping[str, Any]],
    old_eval_ids: set[str],
    underpowered_ids: set[str],
    label_groups: Mapping[str, Sequence[bool]],
    min_confirmatory_eligible_works: int,
    candidate_gap: int,
    policy_thresholds: Mapping[str, Any],
    family: str,
) -> dict[str, Any]:
    work_ids = _candidate_work_ids(candidate_rows)
    old_overlap_ids = sorted(set(work_ids).intersection(old_eval_ids))
    underpowered_overlap_ids = sorted(set(work_ids).intersection(underpowered_ids))
    confirmatory_ids = sorted(set(work_ids).difference(old_eval_ids))
    incremental_ids = sorted(set(confirmatory_ids).difference(underpowered_ids))
    label_summary = _label_coverage_summary(set(confirmatory_ids), label_groups)
    threshold_check = _threshold_check(
        confirmatory_eligible_work_count=len(confirmatory_ids),
        label_summary=label_summary,
        min_confirmatory_eligible_works=min_confirmatory_eligible_works,
        policy_thresholds=policy_thresholds,
    )
    label_thresholds_currently_met = all(
        check.get("passed") is True
        for key, check in threshold_check.items()
        if key != "minimum_confirmatory_candidate_work_count"
    )
    candidate_sha = _work_set_sha256(work_ids) if work_ids else None
    candidate_threshold_met = (
        len(confirmatory_ids) >= min_confirmatory_eligible_works
        and candidate_sha not in {OLD_EVAL_WORK_SET_SHA256, UNDERPOWERED_WORK_SET_SHA256}
        and len(incremental_ids) >= candidate_gap
    )
    source_ids = [str(source.get("ranking_run_id")) for source in considered if source.get("source_is_fresh_relative_to_old_217") is True]
    return {
        "strategy_id": "union_broaden_product_candidate_pools_from_existing_runs",
        "strategy_order": [
            "union/broaden product-candidate pools from newer snapshots if present",
            "broaden existing snapshot candidate pool while preserving product-like scope",
            "blocked_needs_corpus_or_candidate_expansion if still below threshold",
        ],
        "strategy_succeeded": candidate_threshold_met,
        "family": family,
        "source_ranking_run_ids": source_ids,
        "source_count": len(source_ids),
        "candidate_work_count": len(work_ids),
        "canonical_work_count": len(work_ids),
        "candidate_work_set_sha256": candidate_sha,
        "canonical_work_ids_sha256": candidate_sha,
        "overlap_with_old_217_count": len(old_overlap_ids),
        "underpowered_source_overlap_count": len(underpowered_overlap_ids),
        "confirmatory_eligible_work_count": len(confirmatory_ids),
        "incremental_confirmatory_eligible_work_count_excluding_underpowered_source": len(incremental_ids),
        "candidate_gap_from_expansion_plan": candidate_gap,
        "underpowered_source_overlap_excluded_from_incremental_success_criteria": True,
        "candidate_threshold_met": candidate_threshold_met,
        "label_thresholds_currently_met": label_thresholds_currently_met,
        "label_coverage_summary": label_summary,
        "threshold_check": threshold_check,
        "source_provenance": {
            "query_description": "SELECT existing paper_scores pools for the requested family, join works, then freeze a deduplicated canonical-work union artifact.",
            "source_tables": ["paper_scores", "works", "ranking_runs"],
            "ranking_run_ids_considered": [str(source.get("ranking_run_id")) for source in considered],
            "old_217_overlap_excluded_from_confirmatory_denominator": True,
            "underpowered_44_overlap_tagged": True,
            "label_blind_union": True,
        },
    }


def build_ml_fresh_product_candidate_source_build_payload(
    conn: Any,
    *,
    fresh_candidate_source_expansion_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    database_url: str | None = None,
    family: str = DEFAULT_FAMILY,
    min_confirmatory_eligible_works: int | None = None,
    mode: str = DEFAULT_MODE,
    write_eval_db_source: bool = False,
    build_version: str = BUILD_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if mode not in {"artifact_only_freeze", "eval_db_source_create"}:
        raise MLFreshProductCandidateSourceBuildError("mode must be artifact_only_freeze or eval_db_source_create")
    if write_eval_db_source or mode == "eval_db_source_create":
        raise MLFreshProductCandidateSourceBuildError(
            "eval_db_source_create/write-eval-db-source is unsupported in v1; no established eval-only DB writer is reused"
        )
    if mode != "artifact_only_freeze":
        raise MLFreshProductCandidateSourceBuildError("v1 requires artifact_only_freeze mode when write-eval-db-source is false")

    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    expansion_path = Path(fresh_candidate_source_expansion_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    expansion_payload = _load_json_object(expansion_path)
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(label_path)
    expansion_metadata = _validate_expansion_plan(expansion_payload)
    policy_metadata, policy_thresholds = _validate_policy(policy_payload)
    label_rows = _validate_label_dataset(label_payload)
    if not conflict_path.exists():
        raise MLFreshProductCandidateSourceBuildError(f"conflict policy does not exist: {conflict_path}")

    inputs = [
        _input_record("fresh_candidate_source_expansion_plan", expansion_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    min_candidates = int(
        min_confirmatory_eligible_works
        if min_confirmatory_eligible_works is not None
        else _get(expansion_payload, "source_expansion_requirements.minimum_confirmatory_eligible_work_count")
        or policy_thresholds["minimum_candidate_work_count"]
    )
    if min_candidates <= 0:
        raise MLFreshProductCandidateSourceBuildError("min_confirmatory_eligible_works must be positive")
    candidate_gap = int(_get(expansion_payload, "current_blocker_summary.candidate_gap") or max(0, min_candidates - 44))

    old_eval_ids = _old_eval_work_ids(policy_metadata, repo_root=root)
    label_groups = _label_groups(label_rows)
    database_summary = assert_local_database_url(database_url) if database_url else {
        "database_target_redacted": None,
        "read_only_contract": "SELECT-only queries; no database mutations",
    }
    considered, rows_by_source = _ranked_sources_with_rows(
        conn,
        family=family,
        old_eval_ids=old_eval_ids,
        underpowered_ids=set(),
        label_groups=label_groups,
        min_confirmatory_eligible_works=min_candidates,
        policy_thresholds=policy_thresholds,
    )
    underpowered_rows = rows_by_source.get(UNDERPOWERED_RANKING_RUN_ID, [])
    underpowered_ids = set(_candidate_work_ids(underpowered_rows)).difference(old_eval_ids)
    considered, rows_by_source = _ranked_sources_with_rows(
        conn,
        family=family,
        old_eval_ids=old_eval_ids,
        underpowered_ids=underpowered_ids,
        label_groups=label_groups,
        min_confirmatory_eligible_works=min_candidates,
        policy_thresholds=policy_thresholds,
    )
    union_rows = _union_candidate_rows(
        considered,
        rows_by_source,
        old_eval_ids=old_eval_ids,
        underpowered_ids=underpowered_ids,
        family=family,
    )
    built = _strategy_summaries(
        candidate_rows=union_rows,
        considered=considered,
        old_eval_ids=old_eval_ids,
        underpowered_ids=underpowered_ids,
        label_groups=label_groups,
        min_confirmatory_eligible_works=min_candidates,
        candidate_gap=candidate_gap,
        policy_thresholds=policy_thresholds,
        family=family,
    )
    status = "source_built_artifact_only" if built["candidate_threshold_met"] is True else "blocked_needs_corpus_or_candidate_expansion"
    recommended_next_stage = (
        "extend_materializer_to_accept_candidate_source_build_artifact"
        if status == "source_built_artifact_only"
        else "blocked_expand_corpus_or_candidate_generation"
    )
    source_id = (
        f"artifact-union-{family}-{str(built.get('candidate_work_set_sha256') or 'empty')[:12]}"
        if built.get("candidate_work_set_sha256")
        else f"artifact-union-{family}-empty"
    )
    corpus_versions = sorted({str(row.get("corpus_snapshot_version")) for row in union_rows if row.get("corpus_snapshot_version")})
    candidate_rows_hash = hashlib.sha256(
        json.dumps(
            [
                {
                    "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                    "source_memberships": row.get("source_memberships"),
                }
                for row in union_rows
            ],
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    materializer_command = (
        "materializer extension required: ml-fresh-eval-surface-hybrid-materialize does not yet accept "
        "ml-fresh-product-candidate-source-build artifact inputs; do not claim materialization complete."
    )
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "build_version": build_version,
            "generated_at": generated_at or _now_iso_z(),
            "mode": mode,
            "inputs": inputs,
            "database_target_redacted": database_summary.get("database_target_redacted"),
            "database": database_summary,
            "expansion_plan_version": expansion_metadata.get("plan_version"),
            "fresh_surface_policy_version": policy_metadata.get("policy_version"),
            "label_dataset_version": label_payload.get("dataset_version"),
            "conflict_policy_sha256": inputs[-1]["sha256"],
            "target": TARGET,
            "family": family,
            "caveats": list(CAVEATS),
        },
        "build_result": {
            "status": status,
            "confirmatory_eligible_work_count": built["confirmatory_eligible_work_count"],
            "incremental_confirmatory_eligible_work_count_excluding_underpowered_source": built[
                "incremental_confirmatory_eligible_work_count_excluding_underpowered_source"
            ],
            "candidate_work_set_sha256": built["candidate_work_set_sha256"],
            "old_surface_overlap_count": built["overlap_with_old_217_count"],
            "underpowered_source_overlap_count": built["underpowered_source_overlap_count"],
            "candidate_threshold_met": built["candidate_threshold_met"],
            "candidate_threshold_basis": (
                f"confirmatory_eligible_work_count >= {min_candidates} and incremental works outside "
                f"{UNDERPOWERED_RANKING_RUN_ID} >= candidate_gap {candidate_gap}"
            ),
            "label_thresholds_currently_met": built["label_thresholds_currently_met"],
            "recommended_next_stage": recommended_next_stage,
        },
        "candidate_source": {
            "source_id": source_id,
            "family": family,
            "corpus_snapshot_version": "multi_snapshot_union" if len(corpus_versions) > 1 else (corpus_versions[0] if corpus_versions else None),
            "corpus_snapshot_versions": corpus_versions,
            "ranking_run_id": None,
            "candidate_rows": union_rows,
            "candidate_rows_summary": {
                "row_count": len(union_rows),
                "candidate_rows_hash": candidate_rows_hash,
                "canonical_work_ids_sha256": built["canonical_work_ids_sha256"],
            },
            "canonical_work_ids_sha256": built["canonical_work_ids_sha256"],
            "source_provenance": built["source_provenance"],
            "query_description": built["source_provenance"]["query_description"],
            "materializer_handoff": {
                "ranking_run_id_exists": False,
                "artifact_only": True,
                "use_with_materializer_command": materializer_command,
            },
        },
        "source_strategy": {
            "selected_strategy_id": built["strategy_id"],
            "strategy_order": built["strategy_order"],
            "strategy_succeeded": built["strategy_succeeded"],
            "candidate_sources_considered_count": len(considered),
        },
        "candidate_sources_considered": considered,
        "label_snapshot": {
            **built["label_coverage_summary"],
            "threshold_check": built["threshold_check"],
            "label_thresholds_currently_met": built["label_thresholds_currently_met"],
            "informational_only": True,
        },
        "sql_write_report": {
            "writes_enabled": False,
            "statements_allowed": [],
            "affected_row_counts": {},
            "production_tables_modified": False,
        },
        "blocked_actions": [
            "execute_hybrid_validation_on_fresh_surface_v1",
            "hybrid_validation_metric_gates_v1",
            "ml-shadow-scorer-v1",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
        "recommended_next_stage": recommended_next_stage,
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}" if abs(value) < 1 else f"{value:.2f}"
    return str(value)


def markdown_from_ml_fresh_product_candidate_source_build(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    result = payload["build_result"]
    source = payload["candidate_source"]
    label_snapshot = payload["label_snapshot"]
    lines = [
        f"# Fresh Product-Candidate Source Build ({metadata['build_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact freezes a larger product-candidate source in artifact-only mode when existing local candidate pools can be safely broadened. It does not create rankings, write databases, score hybrids, train, import labels, or authorize shadow/production.",
        "",
        f"- **Mode:** `{metadata['mode']}`",
        f"- **Status:** `{result['status']}`",
        f"- **Confirmatory eligible works:** {result['confirmatory_eligible_work_count']}",
        f"- **Candidate threshold met:** {result['candidate_threshold_met']}",
        f"- **Recommended next stage:** `{result['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_and_production_blockers']['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['shadow_and_production_blockers']['production_default_allowed']}",
        "",
        "## Candidate Count And SHA",
        "",
        f"- Candidate source ID: `{source['source_id']}`",
        f"- Candidate work-set SHA: `{result['candidate_work_set_sha256']}`",
        f"- Canonical work IDs SHA: `{source['canonical_work_ids_sha256']}`",
        f"- Candidate rows frozen: {source['candidate_rows_summary']['row_count']}",
        f"- Corpus snapshot version: `{source['corpus_snapshot_version']}`",
        "",
        "## Old 217 Exclusion",
        "",
        f"- Old surface overlap count: {result['old_surface_overlap_count']}",
        "- Old 217 overlaps are excluded from confirmatory denominators.",
        "",
        "## Underpowered 44 Overlap",
        "",
        f"- Underpowered source overlap count: {result['underpowered_source_overlap_count']}",
        f"- Incremental works outside underpowered source: {result['incremental_confirmatory_eligible_work_count_excluding_underpowered_source']}",
        f"- Threshold basis: {result['candidate_threshold_basis']}",
        "",
        "## Label Snapshot",
        "",
        f"- Labeled works: {label_snapshot['labeled_work_count']}",
        f"- Label coverage rate: {_fmt(label_snapshot['label_coverage_rate'])}",
        f"- Positive labeled works: {label_snapshot['positive_labeled_work_count']}",
        f"- Negative labeled works: {label_snapshot['negative_labeled_work_count']}",
        f"- Distinct negative works: {label_snapshot['distinct_negative_work_count']}",
        f"- Label thresholds currently met: {label_snapshot['label_thresholds_currently_met']}",
        "",
        "## Threshold Checks",
        "",
        "| Threshold | Observed | Required | Passed |",
        "| --- | ---: | ---: | --- |",
    ]
    for key, item in label_snapshot["threshold_check"].items():
        lines.append(f"| `{key}` | {_fmt(item['observed'])} | {_fmt(item['threshold'])} | {item['passed']} |")
    lines.extend(
        [
            "",
            "## Materializer Handoff",
            "",
            source["materializer_handoff"]["use_with_materializer_command"],
            "",
            "## Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_product_candidate_source_build(
    *,
    fresh_candidate_source_expansion_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
    family: str = DEFAULT_FAMILY,
    min_confirmatory_eligible_works: int | None = None,
    mode: str = DEFAULT_MODE,
    write_eval_db_source: bool = False,
    build_version: str = BUILD_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    if mode != "artifact_only_freeze" or write_eval_db_source:
        raise MLFreshProductCandidateSourceBuildError(
            "eval_db_source_create/write-eval-db-source is unsupported in v1; use --mode artifact_only_freeze"
        )
    dsn = database_url or _database_url_from_env()
    assert_local_database_url(dsn)
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        payload = build_ml_fresh_product_candidate_source_build_payload(
            conn,
            fresh_candidate_source_expansion_plan_path=fresh_candidate_source_expansion_plan_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            label_dataset_path=label_dataset_path,
            conflict_policy_path=conflict_policy_path,
            database_url=dsn,
            family=family,
            min_confirmatory_eligible_works=min_confirmatory_eligible_works,
            mode=mode,
            write_eval_db_source=write_eval_db_source,
            build_version=build_version,
            repo_root=repo_root,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_product_candidate_source_build(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "BUILD_VERSION",
    "MLFreshProductCandidateSourceBuildError",
    "assert_local_database_url",
    "build_ml_fresh_product_candidate_source_build_payload",
    "markdown_from_ml_fresh_product_candidate_source_build",
    "write_ml_fresh_product_candidate_source_build",
    "_work_set_sha256",
]

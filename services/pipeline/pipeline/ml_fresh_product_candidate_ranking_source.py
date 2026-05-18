"""Freeze a fresh product-candidate ranking source for hybrid validation.

This command is read-only. It discovers existing product-candidate style
`paper_scores` pools in local Postgres, excludes the old observed 217-work
surface from confirmatory denominators, and freezes the best sufficiently large
source for a later materialization/labeling pass. It does not create ranking
runs, score hybrid arms, train, generate embeddings, import labels, or
authorize shadow/prod.
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

ARTIFACT_TYPE = "ml_fresh_product_candidate_ranking_source"
SOURCE_VERSION = "ml-fresh-product-candidate-ranking-source-v1"
PLAN_ARTIFACT_TYPE = "ml_fresh_eval_labeling_plan_hybrid"
PLAN_VERSION = "ml-fresh-eval-labeling-plan-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
TARGET = "good_or_acceptable"
DEFAULT_FAMILY = "emerging"
OLD_EVAL_WORK_SET_SHA256 = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
OLD_RANKING_RUN_ID = "rank-ee2ba6c816"
CURRENT_UNDERPOWERED_WORK_SET_SHA256 = "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926"

SOURCE_FIELDS_CHECKED_IN_ORDER = ("work_id", "openalex_work_id", "paper_id")
_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

CAVEATS = (
    "Read-only source discovery and freeze only.",
    "No ranking run is created and no database writes are performed.",
    "No hybrid scoring, training, embeddings, or label import.",
    "Old 217-work surface overlaps are excluded from confirmatory denominators.",
    "Label readiness is a snapshot from ml-label-dataset-v8, not new labeling.",
    "No shadow or production authorization.",
)


class MLFreshProductCandidateRankingSourceError(Exception):
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
        raise MLFreshProductCandidateRankingSourceError("database URL is required")
    lower = text.lower()
    forbidden_hosts = ("railway", "rlwy", "render.com", "amazonaws", "neon.tech", "supabase", "herokuapp", "azure.com")
    if any(token in lower for token in forbidden_hosts):
        raise MLFreshProductCandidateRankingSourceError(
            "database URL must target local Postgres, not hosted production infrastructure"
        )
    parsed = urlparse(text)
    host = parsed.hostname
    local_hosts = {None, "", "localhost", "127.0.0.1", "::1", "host.docker.internal"}
    if host not in local_hosts and not str(host).endswith(".local"):
        raise MLFreshProductCandidateRankingSourceError(
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
        raise MLFreshProductCandidateRankingSourceError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshProductCandidateRankingSourceError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshProductCandidateRankingSourceError(f"{name} JSON missing metadata object")
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
        raise MLFreshProductCandidateRankingSourceError(f"Input {name} does not exist: {path}")
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


def _validate_labeling_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-eval-labeling-plan")
    if metadata.get("artifact_type") != PLAN_ARTIFACT_TYPE:
        raise MLFreshProductCandidateRankingSourceError(
            f"expected plan metadata.artifact_type={PLAN_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("plan_version") != PLAN_VERSION:
        raise MLFreshProductCandidateRankingSourceError(
            f"expected plan metadata.plan_version={PLAN_VERSION!r}, got {metadata.get('plan_version')!r}"
        )
    if payload.get("recommended_next_stage") != "create_fresh_product_candidate_ranking_source_v1":
        raise MLFreshProductCandidateRankingSourceError(
            "labeling plan recommended_next_stage must be create_fresh_product_candidate_ranking_source_v1"
        )
    if _get(payload, "blocking_diagnosis.current_surface_can_be_made_ready_by_labeling_alone") is not False:
        raise MLFreshProductCandidateRankingSourceError(
            "labeling plan must state current_surface_can_be_made_ready_by_labeling_alone is false"
        )
    return metadata


def _validate_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshProductCandidateRankingSourceError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshProductCandidateRankingSourceError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    old_sha = str(metadata.get("disallowed_eval_work_set_sha256") or "").strip()
    if old_sha != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshProductCandidateRankingSourceError(
            f"policy metadata.disallowed_eval_work_set_sha256 must be {OLD_EVAL_WORK_SET_SHA256}"
        )
    return metadata


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshProductCandidateRankingSourceError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLFreshProductCandidateRankingSourceError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _policy_input_record(policy_metadata: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    inputs = policy_metadata.get("inputs")
    if not isinstance(inputs, list):
        return None
    return next((item for item in inputs if isinstance(item, Mapping) and item.get("name") == name), None)


def _resolve_policy_input(record: Mapping[str, Any], *, repo_root: Path) -> Path:
    raw_path = str(record.get("path") or "").strip()
    if not raw_path:
        raise MLFreshProductCandidateRankingSourceError("policy input record missing path")
    path = Path(raw_path)
    resolved = path if path.is_absolute() else (repo_root / path)
    resolved = resolved.resolve()
    if not resolved.exists():
        raise MLFreshProductCandidateRankingSourceError(f"policy input path does not exist: {raw_path}")
    expected_sha = str(record.get("sha256") or "").strip()
    if expected_sha and sha256_file(resolved) != expected_sha:
        raise MLFreshProductCandidateRankingSourceError(f"policy input SHA mismatch for {raw_path}")
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
        raise MLFreshProductCandidateRankingSourceError(
            "could not reconstruct old 217-work eval IDs from policy production_candidate_scoring/holdout_assignment inputs"
        )
    if _work_set_sha256(old_ids) != OLD_EVAL_WORK_SET_SHA256:
        raise MLFreshProductCandidateRankingSourceError("old eval work IDs from policy inputs do not match disallowed SHA")
    return old_ids


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
                ps.final_score,
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
                "canonical_openalex_work_id": canonical,
                "openalex_id": row.get("openalex_id"),
                "title": row.get("title"),
                "year": row.get("year"),
                "citation_count": row.get("citation_count"),
                "inclusion_status": row.get("inclusion_status"),
                "corpus_snapshot_version": row.get("corpus_snapshot_version"),
                "final_score": _float_or_none(row.get("final_score")),
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


def _threshold_check(summary: Mapping[str, Any], *, min_confirmatory_candidate_works: int, policy_thresholds: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    checks = {
        "minimum_confirmatory_candidate_work_count": (
            summary.get("confirmatory_eligible_work_count"),
            min_confirmatory_candidate_works,
        ),
        "minimum_confirmatory_labeled_work_count": (
            _get(summary, "label_coverage_summary.labeled_work_count"),
            policy_thresholds.get("minimum_confirmatory_labeled_work_count"),
        ),
        "minimum_confirmatory_label_coverage_rate": (
            _get(summary, "label_coverage_summary.label_coverage_rate"),
            policy_thresholds.get("minimum_confirmatory_label_coverage_rate"),
        ),
        "minimum_confirmatory_positive_work_count": (
            _get(summary, "label_coverage_summary.positive_labeled_work_count"),
            policy_thresholds.get("minimum_confirmatory_positive_work_count"),
        ),
        "minimum_confirmatory_negative_work_count": (
            _get(summary, "label_coverage_summary.negative_labeled_work_count"),
            policy_thresholds.get("minimum_confirmatory_negative_work_count"),
        ),
        "minimum_distinct_negative_work_count": (
            _get(summary, "label_coverage_summary.distinct_negative_work_count"),
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
    metadata = source.get("ranking_run_metadata") if isinstance(source.get("ranking_run_metadata"), Mapping) else {}
    return (
        str(metadata.get("finished_at") or ""),
        str(metadata.get("started_at") or ""),
        str(source.get("ranking_run_id") or ""),
    )


def _build_source_summary(
    *,
    source: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any] | None,
    family: str,
    old_eval_ids: set[str],
    label_groups: Mapping[str, Sequence[bool]],
    min_confirmatory_candidate_works: int,
    policy_thresholds: Mapping[str, Any],
) -> dict[str, Any]:
    rid = str(source.get("ranking_run_id") or "")
    work_ids = _candidate_work_ids(candidate_rows)
    candidate_sha = _work_set_sha256(work_ids) if work_ids else None
    overlap_ids = sorted(set(work_ids).intersection(old_eval_ids))
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
        "overlap_with_old_217_count": len(overlap_ids),
        "overlap_work_ids_preview": overlap_ids[:25],
        "confirmatory_eligible_work_count": len(confirmatory_ids),
        "label_coverage_summary": label_summary,
        "source_is_fresh_relative_to_old_217": rid != OLD_RANKING_RUN_ID and candidate_sha != OLD_EVAL_WORK_SET_SHA256,
        "is_current_underpowered_freeze": candidate_sha == CURRENT_UNDERPOWERED_WORK_SET_SHA256,
    }
    summary["threshold_check"] = _threshold_check(
        summary,
        min_confirmatory_candidate_works=min_confirmatory_candidate_works,
        policy_thresholds=policy_thresholds,
    )
    reason: list[str] = []
    if rid == OLD_RANKING_RUN_ID:
        reason.append("ranking_run_id_is_policy_disallowed_old_surface")
    if candidate_sha == OLD_EVAL_WORK_SET_SHA256:
        reason.append("candidate_work_set_sha_matches_policy_disallowed_old_surface")
    if len(confirmatory_ids) < min_confirmatory_candidate_works:
        reason.append("confirmatory_eligible_work_count_below_minimum")
    summary["selection_eligibility"] = {
        "meets_min_confirmatory_candidate_works": len(confirmatory_ids) >= min_confirmatory_candidate_works,
        "policy_valid_source": not reason or reason == ["confirmatory_eligible_work_count_below_minimum"],
        "selection_blockers": reason,
    }
    return summary


def _select_source(
    sources: Sequence[Mapping[str, Any]],
    *,
    min_confirmatory_candidate_works: int,
) -> dict[str, Any] | None:
    valid = [
        dict(source)
        for source in sources
        if source.get("source_is_fresh_relative_to_old_217") is True
        and int(source.get("confirmatory_eligible_work_count") or 0) >= min_confirmatory_candidate_works
    ]
    if not valid:
        return None
    larger_than_current = [
        source
        for source in valid
        if source.get("candidate_work_set_sha256") != CURRENT_UNDERPOWERED_WORK_SET_SHA256
        and int(source.get("confirmatory_eligible_work_count") or 0) > 44
    ]
    candidates = larger_than_current or valid
    return max(
        candidates,
        key=lambda source: (
            int(source.get("confirmatory_eligible_work_count") or 0),
            _source_recency_key(source),
        ),
    )


def _policy_thresholds(policy_payload: Mapping[str, Any]) -> dict[str, float | int]:
    raw = _get(policy_payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(raw, Mapping):
        raise MLFreshProductCandidateRankingSourceError("fresh surface policy missing label thresholds")
    keys = (
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_label_coverage_rate",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
    )
    out: dict[str, float | int] = {}
    for key in keys:
        value = raw.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise MLFreshProductCandidateRankingSourceError(f"policy threshold {key} must be numeric")
        out[key] = value
    return out


def build_ml_fresh_product_candidate_ranking_source_payload(
    conn: Any,
    *,
    fresh_eval_labeling_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    database_url: str | None = None,
    family: str = DEFAULT_FAMILY,
    min_confirmatory_candidate_works: int | None = None,
    source_version: str = SOURCE_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(fresh_eval_labeling_plan_path).resolve()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()

    plan_payload = _load_json_object(plan_path)
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(label_path)
    plan_metadata = _validate_labeling_plan(plan_payload)
    policy_metadata = _validate_policy(policy_payload)
    label_rows = _validate_label_dataset(label_payload)
    if not conflict_path.exists():
        raise MLFreshProductCandidateRankingSourceError(f"conflict policy does not exist: {conflict_path}")
    inputs = [
        _input_record("fresh_eval_labeling_plan", plan_path, repo_root=root),
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    policy_thresholds = _policy_thresholds(policy_payload)
    min_candidates = int(
        min_confirmatory_candidate_works
        if min_confirmatory_candidate_works is not None
        else _get(policy_payload, "label_policy.minimum_confirmatory_label_thresholds.minimum_candidate_work_count")
    )
    if min_candidates <= 0:
        raise MLFreshProductCandidateRankingSourceError("min_confirmatory_candidate_works must be positive")

    old_eval_ids = _old_eval_work_ids(policy_metadata, repo_root=root)
    label_groups = _label_groups(label_rows)
    source_rows = _query_candidate_source_rows(conn, family)
    considered: list[dict[str, Any]] = []
    for source in source_rows:
        rid = str(source.get("ranking_run_id") or "").strip()
        if not rid:
            continue
        metadata = _query_ranking_run_metadata(conn, rid) or source
        candidate_rows = _query_candidate_pool(conn, ranking_run_id=rid, family=family)
        considered.append(
            _build_source_summary(
                source=source,
                candidate_rows=candidate_rows,
                metadata=metadata,
                family=family,
                old_eval_ids=old_eval_ids,
                label_groups=label_groups,
                min_confirmatory_candidate_works=min_candidates,
                policy_thresholds=policy_thresholds,
            )
        )
    considered.sort(
        key=lambda source: (
            int(source.get("confirmatory_eligible_work_count") or 0),
            _source_recency_key(source),
        ),
        reverse=True,
    )
    selected = _select_source(considered, min_confirmatory_candidate_works=min_candidates)
    status = "source_frozen_needs_materialization" if selected is not None else "blocked_no_source_meets_candidate_threshold"
    recommended_next_stage = (
        "rerun_fresh_eval_surface_materialize_with_selected_source"
        if selected is not None
        else "create_new_or_larger_candidate_snapshot"
    )
    db_summary = assert_local_database_url(database_url) if database_url else {
        "database_target_redacted": None,
        "read_only_contract": "SELECT-only queries; no database mutations",
    }
    selection_rule = (
        "largest confirmatory_eligible_work_count among policy-valid sources with count >= "
        f"{min_candidates}; tie-break by newest ranking_run metadata when available. "
        f"The current underpowered freeze SHA {CURRENT_UNDERPOWERED_WORK_SET_SHA256} is considered but not selected when a strictly larger valid source exists."
    )
    selected_freeze: dict[str, Any] | None = None
    if selected is not None:
        selected_freeze = {
            "ranking_run_id": selected["ranking_run_id"],
            "family": selected["family"],
            "corpus_snapshot_version": selected.get("corpus_snapshot_version"),
            "candidate_work_set_sha256": selected["candidate_work_set_sha256"],
            "confirmatory_eligible_work_count": selected["confirmatory_eligible_work_count"],
            "source_is_fresh_relative_to_old_217": selected["source_is_fresh_relative_to_old_217"],
            "overlap_with_old_217_count": selected["overlap_with_old_217_count"],
            "label_coverage_summary": selected["label_coverage_summary"],
            "use_with_materializer_command": (
                "py -m pipeline.cli ml-fresh-eval-surface-hybrid-materialize "
                "--fresh-surface-policy ../../docs/audit/ml-fresh-eval-surface-policy-hybrid-v1.json "
                "--label-dataset ../../docs/audit/ml-label-dataset-v8.json "
                "--conflict-policy ../../docs/audit/ml-label-conflict-policy.md "
                f"--family {selected['family']} "
                f"--ranking-run-id {selected['ranking_run_id']} "
                "--output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.json "
                "--markdown-output ../../docs/audit/ml-fresh-eval-surface-hybrid-v1.md"
            ),
        }

    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "source_version": source_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "database_target_redacted": db_summary.get("database_target_redacted"),
            "database": db_summary,
            "source_labeling_plan_version": plan_metadata.get("plan_version"),
            "fresh_surface_policy_version": policy_metadata.get("policy_version"),
            "label_dataset_version": label_payload.get("dataset_version"),
            "conflict_policy_sha256": inputs[-1]["sha256"],
            "target": TARGET,
            "caveats": list(CAVEATS),
        },
        "source_selection": {
            "status": status,
            "selected_source": selected,
            "selection_rule": selection_rule,
            "old_surface_exclusion_sha": OLD_EVAL_WORK_SET_SHA256,
            "minimum_confirmatory_candidate_works": min_candidates,
            "recommended_next_stage": recommended_next_stage,
        },
        "candidate_sources_considered": considered,
        "selected_source_freeze": selected_freeze,
        "recommended_next_stage": recommended_next_stage,
        "blocked_actions": [
            "new_ranking_run_creation",
            "database_writes",
            "hybrid_scoring",
            "model_training_or_refit",
            "embedding_generation",
            "label_import",
            "shadow_scoring",
            "production_default_change",
        ],
        "shadow_and_production_blockers": {
            "shadow_scoring_allowed": False,
            "production_default_allowed": False,
            "confirmatory_validation_complete": False,
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}" if abs(value) < 1 else f"{value:.2f}"
    return str(value)


def markdown_from_ml_fresh_product_candidate_ranking_source(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    selection = payload["source_selection"]
    selected = payload.get("selected_source_freeze")
    lines = [
        f"# Fresh Product-Candidate Ranking Source ({metadata['source_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact freezes an existing read-only product-candidate ranking source, if one is large enough after excluding the old 217-work surface. It does not create rankings, score hybrids, train, import labels, or authorize shadow/production.",
        "",
        f"- **Status:** `{selection['status']}`",
        f"- **Minimum confirmatory candidate works:** {selection['minimum_confirmatory_candidate_works']}",
        f"- **Recommended next stage:** `{selection['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_and_production_blockers']['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['shadow_and_production_blockers']['production_default_allowed']}",
        "",
        "## Why This Step Exists",
        "",
        "The current fresh surface has only 44 confirmatory-eligible works, so labeling alone cannot satisfy the 100-work policy floor. This step looks for a larger already-existing source before any new product/corpus work is attempted.",
        "",
        "## Sources Considered",
        "",
        "| Ranking run | Snapshot | Works | Old overlap | Confirmatory eligible | Labeled | Negatives | Candidate pass |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for source in payload["candidate_sources_considered"]:
        label_summary = source["label_coverage_summary"]
        candidate_pass = source["threshold_check"]["minimum_confirmatory_candidate_work_count"]["passed"]
        lines.append(
            f"| `{source['ranking_run_id']}` | `{source.get('corpus_snapshot_version')}` | "
            f"{source['candidate_work_count']} | {source['overlap_with_old_217_count']} | "
            f"{source['confirmatory_eligible_work_count']} | {label_summary['labeled_work_count']} | "
            f"{label_summary['negative_labeled_work_count']} | {candidate_pass} |"
        )
    lines.extend(["", "## Selected Source Or Blocker", ""])
    if selected is None:
        lines.extend(
            [
                "No existing source met the minimum confirmatory candidate-work threshold after old-surface exclusion.",
                "",
            ]
        )
    else:
        labels = selected["label_coverage_summary"]
        lines.extend(
            [
                f"- Ranking run: `{selected['ranking_run_id']}`",
                f"- Family: `{selected['family']}`",
                f"- Snapshot: `{selected['corpus_snapshot_version']}`",
                f"- Candidate SHA: `{selected['candidate_work_set_sha256']}`",
                f"- Confirmatory eligible works: {selected['confirmatory_eligible_work_count']}",
                f"- Old 217 overlap excluded: {selected['overlap_with_old_217_count']}",
                f"- Labeled works: {labels['labeled_work_count']}",
                f"- Label coverage rate: {_fmt(labels['label_coverage_rate'])}",
                f"- Positive labeled works: {labels['positive_labeled_work_count']}",
                f"- Negative labeled works: {labels['negative_labeled_work_count']}",
                "",
                "## Materializer Rerun Command",
                "",
                "```powershell",
                selected["use_with_materializer_command"],
                "```",
                "",
            ]
        )
    lines.extend(
        [
            "## Not Validation / Not Shadow / Not Production",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in payload["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_product_candidate_ranking_source(
    *,
    fresh_eval_labeling_plan_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    database_url: str | None = None,
    family: str = DEFAULT_FAMILY,
    min_confirmatory_candidate_works: int | None = None,
    source_version: str = SOURCE_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    dsn = database_url or _database_url_from_env()
    assert_local_database_url(dsn)
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        payload = build_ml_fresh_product_candidate_ranking_source_payload(
            conn,
            fresh_eval_labeling_plan_path=fresh_eval_labeling_plan_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            label_dataset_path=label_dataset_path,
            conflict_policy_path=conflict_policy_path,
            database_url=dsn,
            family=family,
            min_confirmatory_candidate_works=min_confirmatory_candidate_works,
            source_version=source_version,
            repo_root=repo_root,
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_product_candidate_ranking_source(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "SOURCE_VERSION",
    "MLFreshProductCandidateRankingSourceError",
    "assert_local_database_url",
    "build_ml_fresh_product_candidate_ranking_source_payload",
    "markdown_from_ml_fresh_product_candidate_ranking_source",
    "write_ml_fresh_product_candidate_ranking_source",
]

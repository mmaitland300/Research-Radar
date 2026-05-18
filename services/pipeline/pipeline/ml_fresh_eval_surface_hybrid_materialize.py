"""Materialize a fresh eval surface inventory for hybrid validation.

This command is read-only. It selects an existing product-candidate style
`paper_scores` pool, excludes the already-observed hybrid eval surface from
confirmatory denominators, joins existing v8 labels from disk, and writes a
machine-checkable inventory. It does not score hybrid arms, train, generate
embeddings, create ranking runs, import labels, or authorize shadow/prod.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row

from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_fresh_eval_surface_hybrid"
SURFACE_VERSION = "ml-fresh-eval-surface-hybrid-v1"
POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v8"
TARGET = "good_or_acceptable"
DEFAULT_FAMILY = "emerging"
OLD_RANKING_RUN_ID_FALLBACK = "rank-ee2ba6c816"
OLD_SURFACE_ID_FALLBACK = "product_candidate_eval_surface_rank-ee2ba6c816_emerging_v3"

SOURCE_FIELDS_CHECKED_IN_ORDER = ("work_id", "openalex_work_id", "paper_id")
_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

CAVEATS = (
    "Surface materialization only.",
    "Not confirmatory validation.",
    "No hybrid scoring executed.",
    "Existing labels may be insufficient; labeling plan may be required.",
    "Overlap with old 217 is smoke/regression only, not confirmatory evidence.",
    "No shadow/production authorization.",
)


class MLFreshEvalSurfaceHybridMaterializeError(Exception):
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


def assert_local_database_url(database_url: str) -> dict[str, Any]:
    text = str(database_url or "").strip()
    if not text:
        raise MLFreshEvalSurfaceHybridMaterializeError("database URL is required")
    lower = text.lower()
    forbidden_hosts = ("railway", "rlwy", "render.com", "amazonaws", "neon.tech", "supabase", "herokuapp", "azure.com")
    if any(token in lower for token in forbidden_hosts):
        raise MLFreshEvalSurfaceHybridMaterializeError(
            "database URL must target local Docker Postgres, not hosted production infrastructure"
        )
    parsed = urlparse(text)
    host = parsed.hostname
    local_hosts = {None, "", "localhost", "127.0.0.1", "::1", "host.docker.internal"}
    if host not in local_hosts and not str(host).endswith(".local"):
        raise MLFreshEvalSurfaceHybridMaterializeError(
            f"database URL must target local Docker Postgres; host {host!r} is not allowed"
        )
    return {
        "local_database_url_confirmed": True,
        "database_url_host": host or "(local socket)",
        "database_url_port": parsed.port,
        "database_name": (parsed.path or "").lstrip("/") or None,
        "read_only_contract": "SELECT-only queries; no database mutations",
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLFreshEvalSurfaceHybridMaterializeError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLFreshEvalSurfaceHybridMaterializeError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLFreshEvalSurfaceHybridMaterializeError(f"{name} JSON missing metadata object")
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
        raise MLFreshEvalSurfaceHybridMaterializeError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _policy_input_record(policy_metadata: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    inputs = policy_metadata.get("inputs")
    if not isinstance(inputs, list):
        return None
    return next((item for item in inputs if isinstance(item, Mapping) and item.get("name") == name), None)


def _resolve_policy_input(record: Mapping[str, Any], *, repo_root: Path) -> Path:
    raw_path = str(record.get("path") or "").strip()
    if not raw_path:
        raise MLFreshEvalSurfaceHybridMaterializeError("policy input record missing path")
    path = Path(raw_path)
    resolved = path if path.is_absolute() else (repo_root / path)
    resolved = resolved.resolve()
    if not resolved.exists():
        raise MLFreshEvalSurfaceHybridMaterializeError(f"policy input path does not exist: {raw_path}")
    expected_sha = str(record.get("sha256") or "").strip()
    if expected_sha and sha256_file(resolved) != expected_sha:
        raise MLFreshEvalSurfaceHybridMaterializeError(f"policy input SHA mismatch for {raw_path}")
    return resolved


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


def _validate_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, float | int], set[str]]:
    metadata = _metadata(payload, name="fresh-surface-policy")
    if metadata.get("artifact_type") != POLICY_ARTIFACT_TYPE:
        raise MLFreshEvalSurfaceHybridMaterializeError(
            f"expected policy metadata.artifact_type={POLICY_ARTIFACT_TYPE!r}, got {metadata.get('artifact_type')!r}"
        )
    if metadata.get("policy_version") != POLICY_VERSION:
        raise MLFreshEvalSurfaceHybridMaterializeError(
            f"expected policy metadata.policy_version={POLICY_VERSION!r}, got {metadata.get('policy_version')!r}"
        )
    if metadata.get("status") != "blocked_for_confirmatory_until_surface_materialized":
        raise MLFreshEvalSurfaceHybridMaterializeError("policy status must be blocked_for_confirmatory_until_surface_materialized")
    assertions = payload.get("policy_assertions")
    if not isinstance(assertions, Mapping):
        raise MLFreshEvalSurfaceHybridMaterializeError("policy missing policy_assertions object")
    expected_assertions = {
        "old_217_surface_confirmatory_reuse_allowed": False,
        "frozen_primary_hybrid_arm": "hybrid_rank_mean_50_50",
        "shadow_allowed_by_this_policy": False,
    }
    for key, expected in expected_assertions.items():
        if assertions.get(key) != expected:
            raise MLFreshEvalSurfaceHybridMaterializeError(f"policy policy_assertions.{key} must be {expected!r}")
    thresholds = _get(payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLFreshEvalSurfaceHybridMaterializeError("policy missing label thresholds")
    normalized_thresholds: dict[str, float | int] = {}
    for key in (
        "minimum_candidate_work_count",
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_confirmatory_label_coverage_rate",
        "minimum_distinct_negative_work_count",
    ):
        value = thresholds.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise MLFreshEvalSurfaceHybridMaterializeError(f"policy threshold {key} must be numeric")
        normalized_thresholds[key] = value
    disallowed_surfaces = payload.get("disallowed_surfaces")
    old_ids: set[str] = set()
    if isinstance(disallowed_surfaces, list):
        for surface in disallowed_surfaces:
            if isinstance(surface, Mapping):
                for item in surface.get("canonical_openalex_work_ids", []) if isinstance(surface.get("canonical_openalex_work_ids"), list) else []:
                    canonical = _canonical_from_value(item)
                    if canonical:
                        old_ids.add(canonical)
    return metadata, normalized_thresholds, old_ids


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    if payload.get("dataset_version") != LABEL_DATASET_VERSION:
        raise MLFreshEvalSurfaceHybridMaterializeError(
            f"expected label dataset_version={LABEL_DATASET_VERSION!r}, got {payload.get('dataset_version')!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLFreshEvalSurfaceHybridMaterializeError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _old_eval_work_ids(policy_payload: Mapping[str, Any], assignment_payload: Mapping[str, Any]) -> set[str]:
    old_ids: set[str] = set()
    for row in policy_payload.get("candidate_pool_rows", []) if isinstance(policy_payload.get("candidate_pool_rows"), list) else []:
        if isinstance(row, Mapping):
            canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
            if canonical:
                old_ids.add(canonical)
    work_assignments = assignment_payload.get("work_assignments")
    if isinstance(work_assignments, list):
        for row in work_assignments:
            if isinstance(row, Mapping) and row.get("assignment") == "eval":
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    old_ids.add(canonical)
    assignments = assignment_payload.get("assignments")
    if isinstance(assignments, list):
        for row in assignments:
            if isinstance(row, Mapping) and row.get("assignment") == "eval":
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    old_ids.add(canonical)
    return old_ids


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


def _validate_assignment(payload: Mapping[str, Any], *, expected_eval_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="holdout-assignment")
    if metadata.get("assignment_version") != "ml-learned-scorer-holdout-assignment-v1":
        raise MLFreshEvalSurfaceHybridMaterializeError(
            "holdout assignment metadata.assignment_version must be ml-learned-scorer-holdout-assignment-v1"
        )
    if metadata.get("eval_work_set_sha256") != expected_eval_sha:
        raise MLFreshEvalSurfaceHybridMaterializeError("holdout assignment eval_work_set_sha256 must match policy old eval SHA")
    return metadata


def _label_row_is_explicit(row: Mapping[str, Any]) -> bool:
    return row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()})


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


def _corpus_snapshot_matches(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    ranking_metadata: Mapping[str, Any] | None,
    requested: str | None,
) -> tuple[bool, str | None, dict[str, Any]]:
    versions = sorted({str(row.get("corpus_snapshot_version")) for row in candidate_rows if row.get("corpus_snapshot_version")})
    metadata_version = str(ranking_metadata.get("corpus_snapshot_version")) if isinstance(ranking_metadata, Mapping) and ranking_metadata.get("corpus_snapshot_version") else None
    observed = metadata_version or (versions[0] if len(versions) == 1 else None)
    detail = {
        "requested_corpus_snapshot_version": requested,
        "ranking_run_corpus_snapshot_version": metadata_version,
        "candidate_row_corpus_snapshot_versions": versions,
    }
    if requested is None:
        return True, observed, detail
    if metadata_version is not None and metadata_version != requested:
        return False, observed, detail
    if versions and (len(versions) != 1 or versions[0] != requested):
        return False, observed, detail
    return True, observed or requested, detail


def _select_source(
    conn: Any,
    *,
    ranking_run_id: str | None,
    family: str,
    old_ranking_run_id: str | None,
    old_eval_sha: str,
    corpus_snapshot_version: str | None,
) -> dict[str, Any]:
    attempted: list[dict[str, Any]] = []
    query_intent = {
        "source_tables": ["paper_scores", "works", "ranking_runs"],
        "contract": "SELECT-only candidate inventory; no database mutations",
        "candidate_query": "paper_scores joined to works by ranking_run_id and recommendation_family",
        "discovery_query": "paper_scores grouped by ranking_run_id with ranking_runs metadata",
    }
    if ranking_run_id:
        rid = str(ranking_run_id).strip()
        metadata = _query_ranking_run_metadata(conn, rid)
        candidate_rows = _query_candidate_pool(conn, ranking_run_id=rid, family=family)
        work_ids = _candidate_work_ids(candidate_rows)
        candidate_sha = _work_set_sha256(work_ids) if work_ids else None
        corpus_ok, observed_corpus, corpus_detail = _corpus_snapshot_matches(
            candidate_rows=candidate_rows,
            ranking_metadata=metadata,
            requested=corpus_snapshot_version,
        )
        reason = None
        if not candidate_rows:
            reason = "no_paper_scores_rows_for_ranking_run_and_family"
        elif old_ranking_run_id and rid == old_ranking_run_id:
            reason = "ranking_run_id_matches_disallowed_old_surface"
        elif candidate_sha == old_eval_sha:
            reason = "candidate_work_set_sha_matches_disallowed_old_surface"
        elif not corpus_ok:
            reason = "corpus_snapshot_version_mismatch"
        if reason:
            attempted.append(
                {
                    "ranking_run_id": rid,
                    "family": family,
                    "candidate_work_count": len(work_ids),
                    "candidate_work_set_sha256": candidate_sha,
                    "reason": reason,
                    "corpus_snapshot_check": corpus_detail,
                }
            )
            return {
                "source_selection_mode": "explicit",
                "status": "blocked_source_not_fresh",
                "ranking_run_id": rid,
                "family": family,
                "corpus_snapshot_version": observed_corpus,
                "ranking_run_metadata": metadata,
                "candidate_rows": candidate_rows,
                "selected_source_rationale": reason,
                "attempted_sources": attempted,
                "query_intent": query_intent,
            }
        return {
            "source_selection_mode": "explicit",
            "status": "selected",
            "ranking_run_id": rid,
            "family": family,
            "corpus_snapshot_version": observed_corpus,
            "ranking_run_metadata": metadata,
            "candidate_rows": candidate_rows,
            "selected_source_rationale": "explicit ranking_run_id is fresh relative to policy disallowed surface",
            "attempted_sources": attempted,
            "query_intent": query_intent,
        }

    source_rows = _query_candidate_source_rows(conn, family)
    for source in source_rows:
        rid = str(source.get("ranking_run_id") or "").strip()
        if not rid:
            continue
        metadata = _query_ranking_run_metadata(conn, rid) or source
        candidate_rows = _query_candidate_pool(conn, ranking_run_id=rid, family=family)
        work_ids = _candidate_work_ids(candidate_rows)
        candidate_sha = _work_set_sha256(work_ids) if work_ids else None
        corpus_ok, observed_corpus, corpus_detail = _corpus_snapshot_matches(
            candidate_rows=candidate_rows,
            ranking_metadata=metadata,
            requested=corpus_snapshot_version,
        )
        reason = "selected"
        if old_ranking_run_id and rid == old_ranking_run_id:
            reason = "ranking_run_id_matches_disallowed_old_surface"
        elif not candidate_rows:
            reason = "no_paper_scores_rows_for_family"
        elif candidate_sha == old_eval_sha:
            reason = "candidate_work_set_sha_matches_disallowed_old_surface"
        elif not corpus_ok:
            reason = "corpus_snapshot_version_mismatch"
        attempted.append(
            {
                "ranking_run_id": rid,
                "family": family,
                "status": source.get("status"),
                "candidate_work_count": len(work_ids),
                "candidate_work_set_sha256": candidate_sha,
                "reason": reason,
                "corpus_snapshot_check": corpus_detail,
            }
        )
        if reason == "selected":
            return {
                "source_selection_mode": "discovered",
                "status": "selected",
                "ranking_run_id": rid,
                "family": family,
                "corpus_snapshot_version": observed_corpus,
                "ranking_run_metadata": metadata,
                "candidate_rows": candidate_rows,
                "selected_source_rationale": "first successful deterministic discovery candidate with fresh work-set SHA",
                "attempted_sources": attempted,
                "query_intent": query_intent,
            }
    return {
        "source_selection_mode": "blocked",
        "status": "blocked_no_fresh_candidate_source",
        "ranking_run_id": None,
        "family": family,
        "corpus_snapshot_version": corpus_snapshot_version,
        "ranking_run_metadata": None,
        "candidate_rows": [],
        "selected_source_rationale": "no qualifying paper_scores source found with a fresh work-set SHA",
        "attempted_sources": attempted,
        "query_intent": query_intent,
    }


def _candidate_pool_section(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    old_eval_ids: set[str],
    old_eval_sha: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    work_ids = _candidate_work_ids(candidate_rows)
    candidate_sha = _work_set_sha256(work_ids) if work_ids else None
    missing_canonical = sum(1 for row in candidate_rows if not row.get("canonical_openalex_work_id"))
    overlap_ids = sorted(set(work_ids).intersection(old_eval_ids))
    overlap_sha = _work_set_sha256(overlap_ids) if overlap_ids else None
    enriched_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in candidate_rows:
        item = dict(row)
        canonical = str(item.get("canonical_openalex_work_id") or "")
        overlap = canonical in old_eval_ids if canonical else False
        confirmatory = bool(canonical and not overlap)
        item["previous_eval_overlap"] = overlap
        item["confirmatory_metric_eligible"] = confirmatory
        if canonical in seen:
            item["duplicate_candidate_canonical_work"] = True
        else:
            item["duplicate_candidate_canonical_work"] = False
        if canonical:
            seen.add(canonical)
        enriched_rows.append(item)
    confirmatory_ids = sorted(set(work_ids).difference(old_eval_ids))
    candidate_pool = {
        "candidate_work_count": len(work_ids),
        "candidate_work_set_sha256": candidate_sha,
        "candidate_rows": enriched_rows,
        "old_eval_work_set_sha256": old_eval_sha,
        "work_set_sha_differs_from_old_eval": candidate_sha is not None and candidate_sha != old_eval_sha,
    }
    overlap_report = {
        "overlap_work_count": len(overlap_ids),
        "excluded_previous_eval_overlap_count": len(overlap_ids),
        "overlap_work_ids_preview": overlap_ids[:50],
        "overlap_work_set_sha256": overlap_sha,
        "old_surface_id": OLD_SURFACE_ID_FALLBACK,
        "confirmatory_metric_denominator_excludes_overlap": True,
    }
    eligibility = {
        "confirmatory_metric_eligible_work_count": len(confirmatory_ids),
        "excluded_previous_eval_overlap_count": len(overlap_ids),
        "excluded_missing_canonical_work_id_count": missing_canonical,
        "eligibility_notes": [
            "Confirmatory metric denominator excludes old eval overlaps.",
            "Candidate rows without canonical work IDs are excluded from confirmatory metrics.",
        ],
    }
    return candidate_pool, overlap_report, eligibility


def _label_coverage(
    label_rows: Sequence[Mapping[str, Any]],
    *,
    confirmatory_work_ids: set[str],
    old_eval_ids: set[str],
) -> dict[str, Any]:
    explicit_rows = [row for row in label_rows if _label_row_is_explicit(row)]
    boolean_rows: list[dict[str, Any]] = []
    missing_canonical = 0
    for row in explicit_rows:
        if not isinstance(row.get(TARGET), bool):
            continue
        canonical = _canonical_work_id_from_label(row)
        if canonical is None:
            missing_canonical += 1
            continue
        item = dict(row)
        item["_canonical_openalex_work_id"] = canonical
        item["_target_value"] = bool(row[TARGET])
        boolean_rows.append(item)

    confirmatory_observations = [row for row in boolean_rows if row["_canonical_openalex_work_id"] in confirmatory_work_ids]
    overlap_observations = [row for row in boolean_rows if row["_canonical_openalex_work_id"] in old_eval_ids]
    groups: dict[str, list[bool]] = defaultdict(list)
    row_ids_by_work: dict[str, list[str]] = defaultdict(list)
    for row in confirmatory_observations:
        canonical = str(row["_canonical_openalex_work_id"])
        groups[canonical].append(bool(row["_target_value"]))
        row_ids_by_work[canonical].append(str(row.get("row_id") or ""))
    positive_works = sorted(work_id for work_id, values in groups.items() if any(values))
    negative_works = sorted(work_id for work_id, values in groups.items() if values and all(not value for value in values))
    conflict_works = sorted(work_id for work_id, values in groups.items() if any(values) and any(not value for value in values))
    duplicate_works = sorted(work_id for work_id, values in groups.items() if len(values) > 1)
    positive_obs = sum(1 for row in confirmatory_observations if row["_target_value"] is True)
    negative_obs = sum(1 for row in confirmatory_observations if row["_target_value"] is False)
    labeled_work_count = len(groups)
    candidate_count = len(confirmatory_work_ids)
    coverage_rate = labeled_work_count / candidate_count if candidate_count else 0.0
    return {
        "observation_level": {
            "total_label_dataset_rows": len(label_rows),
            "explicit_manual_label_rows": len(explicit_rows),
            "explicit_boolean_target_observation_rows": len(boolean_rows),
            "confirmatory_labeled_observation_count": len(confirmatory_observations),
            "confirmatory_positive_observation_count": positive_obs,
            "confirmatory_negative_observation_count": negative_obs,
            "overlap_labeled_observation_count_smoke_only": len(overlap_observations),
            "label_rows_missing_canonical_work_id": missing_canonical,
        },
        "work_level": {
            "confirmatory_candidate_work_count": candidate_count,
            "confirmatory_labeled_work_count": labeled_work_count,
            "confirmatory_unlabeled_work_count": max(0, candidate_count - labeled_work_count),
            "confirmatory_positive_work_count": len(positive_works),
            "confirmatory_negative_work_count": len(negative_works),
            "conflicting_target_work_group_count": len(conflict_works),
            "distinct_negative_work_count": len(negative_works),
            "duplicate_labeled_work_group_count": len(duplicate_works),
            "label_coverage_rate": coverage_rate,
            "conflicting_work_ids_preview": conflict_works[:25],
            "duplicate_work_ids_preview": duplicate_works[:25],
        },
        "label_coverage_rate": coverage_rate,
        "positive_negative_conflict_counts": {
            "positive_work_count_any_positive": len(positive_works),
            "negative_work_count_all_negative": len(negative_works),
            "conflicting_target_work_group_count": len(conflict_works),
        },
        "unlabeled_counts": {
            "confirmatory_unlabeled_work_count": max(0, candidate_count - labeled_work_count),
        },
        "observation_level_labels_preserved": True,
        "silent_conflict_merge_used": False,
    }


def _threshold_check(label_coverage: Mapping[str, Any], thresholds: Mapping[str, float | int]) -> dict[str, dict[str, Any]]:
    work = label_coverage.get("work_level") if isinstance(label_coverage.get("work_level"), Mapping) else {}
    values = {
        "minimum_candidate_work_count": work.get("confirmatory_candidate_work_count"),
        "minimum_confirmatory_labeled_work_count": work.get("confirmatory_labeled_work_count"),
        "minimum_confirmatory_positive_work_count": work.get("confirmatory_positive_work_count"),
        "minimum_confirmatory_negative_work_count": work.get("confirmatory_negative_work_count"),
        "minimum_confirmatory_label_coverage_rate": label_coverage.get("label_coverage_rate"),
        "minimum_distinct_negative_work_count": work.get("distinct_negative_work_count"),
    }
    out: dict[str, dict[str, Any]] = {}
    for key, threshold in thresholds.items():
        observed = values.get(key)
        passed = isinstance(observed, (int, float)) and not isinstance(observed, bool) and float(observed) >= float(threshold)
        out[key] = {"observed": observed, "threshold": threshold, "passed": passed}
    return out


def _all_thresholds_pass(threshold_check: Mapping[str, Mapping[str, Any]]) -> bool:
    return all(item.get("passed") is True for item in threshold_check.values())


def _status_and_next_stage(
    *,
    source_status: str,
    explicit_ranking_run_id: bool,
    fresh_source_exists: bool,
    ready: bool,
    thresholds_pass: bool,
) -> tuple[str, str]:
    if source_status == "blocked_no_fresh_candidate_source":
        return "blocked_no_fresh_candidate_source", "create_new_product_candidate_ranking_run_or_snapshot"
    if source_status == "blocked_source_not_fresh":
        return "blocked_source_not_fresh", "blocked_fix_source_selection" if explicit_ranking_run_id else "create_new_product_candidate_ranking_run_or_snapshot"
    if fresh_source_exists and ready:
        return "materialized_ready", "execute_hybrid_validation_on_fresh_surface_v1"
    if fresh_source_exists and not thresholds_pass:
        return "materialized_needs_labels", "create_fresh_eval_labeling_plan_hybrid_v1"
    return "blocked_no_fresh_candidate_source", "create_new_product_candidate_ranking_run_or_snapshot"


def build_ml_fresh_eval_surface_hybrid_materialize_payload(
    conn: Any,
    *,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    ranking_run_id: str | None = None,
    family: str = DEFAULT_FAMILY,
    corpus_snapshot_version: str | None = None,
    database_url: str | None = None,
    surface_version: str = SURFACE_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()
    policy_payload = _load_json_object(policy_path)
    label_payload = _load_json_object(label_path)
    policy_metadata, thresholds, _policy_old_ids = _validate_policy(policy_payload)
    label_rows = _validate_label_dataset(label_payload)

    inputs = [
        _input_record("fresh_surface_policy", policy_path, repo_root=root),
        _input_record("label_dataset", label_path, repo_root=root),
        _input_record("conflict_policy", conflict_path, repo_root=root),
    ]
    conflict_sha = inputs[-1]["sha256"]
    old_eval_sha = str(policy_metadata.get("disallowed_eval_work_set_sha256") or "")
    if not old_eval_sha:
        raise MLFreshEvalSurfaceHybridMaterializeError("policy metadata.disallowed_eval_work_set_sha256 must be present")

    old_ids: set[str] = set()
    scoring_record = _policy_input_record(policy_metadata, "production_candidate_scoring")
    assignment_record = _policy_input_record(policy_metadata, "holdout_assignment")
    if scoring_record is not None:
        scoring_path = _resolve_policy_input(scoring_record, repo_root=root)
        old_ids.update(_old_eval_ids_from_scoring(_load_json_object(scoring_path)))
    if assignment_record is not None:
        assignment_path = _resolve_policy_input(assignment_record, repo_root=root)
        assignment_payload = _load_json_object(assignment_path)
        _validate_assignment(assignment_payload, expected_eval_sha=old_eval_sha)
        old_ids.update(_old_eval_work_ids(policy_payload, assignment_payload))
    if not old_ids:
        raise MLFreshEvalSurfaceHybridMaterializeError(
            "could not reconstruct disallowed old eval work IDs from policy production_candidate_scoring/holdout_assignment inputs"
        )
    if old_ids and _work_set_sha256(old_ids) != old_eval_sha:
        raise MLFreshEvalSurfaceHybridMaterializeError("old eval work IDs from policy inputs do not match disallowed SHA")

    old_ranking_run_id = str(_get(policy_payload, "disallowed_surfaces.0.ranking_run_id") or OLD_RANKING_RUN_ID_FALLBACK)
    selected = _select_source(
        conn,
        ranking_run_id=ranking_run_id,
        family=str(family or DEFAULT_FAMILY),
        old_ranking_run_id=old_ranking_run_id,
        old_eval_sha=old_eval_sha,
        corpus_snapshot_version=corpus_snapshot_version,
    )
    candidate_rows = selected["candidate_rows"]
    candidate_pool, overlap_report, eligibility = _candidate_pool_section(
        candidate_rows,
        old_eval_ids=old_ids,
        old_eval_sha=old_eval_sha,
    )
    confirmatory_work_ids = {
        str(row["canonical_openalex_work_id"])
        for row in candidate_pool["candidate_rows"]
        if row.get("confirmatory_metric_eligible") is True and row.get("canonical_openalex_work_id")
    }
    label_coverage = _label_coverage(label_rows, confirmatory_work_ids=confirmatory_work_ids, old_eval_ids=old_ids)
    threshold_check = _threshold_check(label_coverage, thresholds)
    thresholds_pass = _all_thresholds_pass(threshold_check)
    fresh_source_exists = selected["status"] == "selected" and candidate_pool["work_set_sha_differs_from_old_eval"] is True
    ready = bool(fresh_source_exists and thresholds_pass)
    status, recommended = _status_and_next_stage(
        source_status=str(selected["status"]),
        explicit_ranking_run_id=bool(ranking_run_id),
        fresh_source_exists=fresh_source_exists,
        ready=ready,
        thresholds_pass=thresholds_pass,
    )
    database_summary = assert_local_database_url(database_url) if database_url is not None else {
        "local_database_url_confirmed": None,
        "read_only_contract": "SELECT-only queries; no database mutations",
    }
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "surface_version": surface_version,
        "generated_at": generated_at or _now_iso_z(),
        "status": status,
        "inputs": inputs,
        "policy_version": policy_metadata.get("policy_version"),
        "label_dataset_version": label_payload.get("dataset_version"),
        "conflict_policy_sha256": conflict_sha,
        "database": database_summary,
        "caveats": list(CAVEATS),
    }
    candidate_source = {
        "source_selection_mode": selected["source_selection_mode"],
        "ranking_run_id": selected["ranking_run_id"],
        "family": selected["family"],
        "corpus_snapshot_version": selected["corpus_snapshot_version"],
        "ranking_run_metadata": selected["ranking_run_metadata"],
        "selected_source_rationale": selected["selected_source_rationale"],
        "attempted_sources": selected["attempted_sources"],
        "query_intent": selected["query_intent"],
    }
    return {
        "metadata": metadata,
        "candidate_source": candidate_source,
        "candidate_pool": candidate_pool,
        "disallowed_overlap_report": {
            **overlap_report,
            "old_surface_id": _get(policy_payload, "disallowed_surfaces.0.surface_id") or OLD_SURFACE_ID_FALLBACK,
        },
        "confirmatory_eligibility": eligibility,
        "label_coverage": label_coverage,
        "threshold_check": threshold_check,
        "ready_for_hybrid_validation_scoring": ready,
        "confirmatory_validation_complete": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": recommended,
        "blocked_actions": [
            "shadow_scoring",
            "production_default_change",
            "bridge/default ranking changes",
            "model/scorer deployment",
            "public production-readiness claims",
        ],
        "policy_assertions": {
            "old_217_surface_confirmatory_reuse_allowed": False,
            "overlaps_excluded_from_confirmatory_metrics": True,
            "frozen_primary_hybrid_arm": "hybrid_rank_mean_50_50",
            "retuning_on_fresh_eval_labels_allowed": False,
            "confirmatory_validation_complete": False,
            "shadow_allowed": False,
            "production_default_allowed": False,
        },
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def markdown_from_ml_fresh_eval_surface_hybrid_materialize(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    source = payload["candidate_source"]
    pool = payload["candidate_pool"]
    overlap = payload["disallowed_overlap_report"]
    eligibility = payload["confirmatory_eligibility"]
    labels = payload["label_coverage"]
    work = labels["work_level"]
    lines = [
        f"# Fresh Eval Surface Hybrid Materialization ({metadata['surface_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact inventories a concrete existing product-candidate pool for fresh hybrid validation. It does not run hybrid scoring, train, label, create rankings, or authorize shadow/production.",
        "",
        f"- **Status:** `{metadata['status']}`",
        f"- **Ready for hybrid validation scoring:** {payload['ready_for_hybrid_validation_scoring']}",
        f"- **Recommended next stage:** `{payload['recommended_next_stage']}`",
        f"- **Shadow scoring allowed:** {payload['shadow_scoring_allowed']}",
        f"- **Production default allowed:** {payload['production_default_allowed']}",
        "",
        "## Candidate Source",
        "",
        f"- **Mode:** `{source['source_selection_mode']}`",
        f"- **Ranking run:** `{source['ranking_run_id']}`",
        f"- **Family:** `{source['family']}`",
        f"- **Corpus snapshot:** `{source['corpus_snapshot_version']}`",
        f"- **Rationale/block reason:** {source['selected_source_rationale']}",
        "",
        "## Candidate Count And SHA",
        "",
        f"- Candidate works: {pool['candidate_work_count']}",
        f"- Candidate work-set SHA: `{pool['candidate_work_set_sha256']}`",
        f"- Differs from old eval SHA: {pool['work_set_sha_differs_from_old_eval']}",
        "",
        "## Overlap With Old 217",
        "",
        f"- Overlap work count: {overlap['overlap_work_count']}",
        f"- Excluded previous eval overlap count: {overlap['excluded_previous_eval_overlap_count']}",
        f"- Confirmatory denominator excludes overlap: {overlap['confirmatory_metric_denominator_excludes_overlap']}",
        "",
        "## Confirmatory Eligible Counts",
        "",
        f"- Confirmatory metric eligible works: {eligibility['confirmatory_metric_eligible_work_count']}",
        f"- Missing canonical work exclusions: {eligibility['excluded_missing_canonical_work_id_count']}",
        "",
        "## Label Coverage And Thresholds",
        "",
        f"- Labeled works: {work['confirmatory_labeled_work_count']}",
        f"- Unlabeled works: {work['confirmatory_unlabeled_work_count']}",
        f"- Positive works: {work['confirmatory_positive_work_count']}",
        f"- Negative works: {work['confirmatory_negative_work_count']}",
        f"- Conflicting target work groups: {work['conflicting_target_work_group_count']}",
        f"- Label coverage rate: {_fmt(labels['label_coverage_rate'])}",
        "",
        "| Threshold | Observed | Required | Passed |",
        "| --- | ---: | ---: | --- |",
    ]
    for key, item in payload["threshold_check"].items():
        lines.append(f"| `{key}` | {_fmt(item['observed'])} | {_fmt(item['threshold'])} | {item['passed']} |")
    lines.extend(
        [
            "",
            "## Blocked Actions",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in payload["blocked_actions"])
    lines.extend(
        [
            "",
            "## Not Shadow / Not Production Caveats",
            "",
        ]
    )
    lines.extend(f"- {caveat}" for caveat in metadata["caveats"])
    lines.append("")
    return "\n".join(lines)


def write_ml_fresh_eval_surface_hybrid_materialize(
    *,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    ranking_run_id: str | None = None,
    family: str = DEFAULT_FAMILY,
    corpus_snapshot_version: str | None = None,
    database_url: str | None = None,
    surface_version: str = SURFACE_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    dsn = database_url or _database_url_from_env()
    assert_local_database_url(dsn)
    try:
        conn_context = psycopg.connect(dsn, connect_timeout=10)
    except psycopg.Error as exc:
        class _BlockedCursor:
            def execute(self, query: str, params: tuple | None = None) -> "_BlockedCursor":
                return self

            def fetchall(self) -> list[dict[str, Any]]:
                return []

            def fetchone(self) -> None:
                return None

            def __enter__(self) -> "_BlockedCursor":
                return self

            def __exit__(self, *args: object) -> None:
                return None

        class _BlockedConnection:
            def cursor(self, row_factory: object | None = None) -> _BlockedCursor:
                return _BlockedCursor()

        payload = build_ml_fresh_eval_surface_hybrid_materialize_payload(
            _BlockedConnection(),
            fresh_surface_policy_path=fresh_surface_policy_path,
            label_dataset_path=label_dataset_path,
            conflict_policy_path=conflict_policy_path,
            ranking_run_id=ranking_run_id,
            family=family,
            corpus_snapshot_version=corpus_snapshot_version,
            database_url=dsn,
            surface_version=surface_version,
            repo_root=repo_root,
        )
        payload["candidate_source"]["selected_source_rationale"] = (
            f"database connection failed before candidate discovery: {type(exc).__name__}"
        )
        payload["candidate_source"]["connection_error"] = str(exc)
    else:
        with conn_context as conn:
            payload = build_ml_fresh_eval_surface_hybrid_materialize_payload(
                conn,
                fresh_surface_policy_path=fresh_surface_policy_path,
                label_dataset_path=label_dataset_path,
                conflict_policy_path=conflict_policy_path,
                ranking_run_id=ranking_run_id,
                family=family,
                corpus_snapshot_version=corpus_snapshot_version,
                database_url=dsn,
                surface_version=surface_version,
                repo_root=repo_root,
            )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_fresh_eval_surface_hybrid_materialize(payload),
        encoding="utf-8",
        newline="\n",
    )
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "SURFACE_VERSION",
    "MLFreshEvalSurfaceHybridMaterializeError",
    "assert_local_database_url",
    "build_ml_fresh_eval_surface_hybrid_materialize_payload",
    "markdown_from_ml_fresh_eval_surface_hybrid_materialize",
    "write_ml_fresh_eval_surface_hybrid_materialize",
]

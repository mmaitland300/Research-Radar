"""Select a second fresh surface for ml-shadow-scorer-v1 generalization.

This command performs read-only local Postgres discovery of existing
product-candidate ranking runs. It freezes counts, overlap, labels, and
coverage needed to decide whether an existing distinct second surface can be
used for a future generalization audit. It does not write to the database,
create ranking runs, execute scoring, generate embeddings, ingest labels,
implement online shadowing, or authorize production behavior.
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

from pipeline.ml_fresh_eval_surface_hybrid_materialize import (
    MLFreshEvalSurfaceHybridMaterializeError,
    assert_local_database_url as _assert_materializer_local_database_url,
)
from pipeline.ml_label_dataset import row_has_explicit_label, sha256_file
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_generalization_second_surface"
SURFACE_VERSION = "ml-shadow-scorer-v1-generalization-second-surface-v1"

PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_plan"
PLAN_VERSION = "ml-shadow-scorer-v1-generalization-audit-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"
FRESH_SURFACE_POLICY_ARTIFACT_TYPE = "ml_fresh_eval_surface_policy_hybrid"
FRESH_SURFACE_POLICY_VERSION = "ml-fresh-eval-surface-policy-hybrid-v1"
LABEL_DATASET_VERSION = "ml-label-dataset-v11"
OFFLINE_SCORING_ARTIFACT_TYPE = "ml_offline_production_candidate_scoring"
OFFLINE_SCORING_VERSION = "ml-offline-production-candidate-scoring-v3"

DISALLOWED_RANKING_RUN_ID = "rank-9f4b2a2084"
DISALLOWED_CANDIDATE_SHA = "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6"
OLD_217_EVAL_SHA = "213986401c1c9ba354b8356a73e8b70777d7061072fe5645fc248f2ac9fe8f8a"
TARGET = "good_or_acceptable"
DEFAULT_FAMILY = "emerging"
EXPECTED_NEXT_STAGE = "materialize_or_select_second_fresh_surface_for_shadow_generalization_v1"
DEFAULT_FIRST_VALIDATED_SURFACE = Path("docs/audit/ml-fresh-eval-surface-hybrid-v1.json")

SOURCE_FIELDS_CHECKED_IN_ORDER = ("work_id", "openalex_work_id", "paper_id", "canonical_openalex_work_id")
_WORK_ID_RE = re.compile(r"(?:openalex\.org/)?(W\d+)\s*$", re.IGNORECASE)

STATUS_TO_NEXT_STAGE = {
    "selected_ready_for_generalization_audit": "audit_ml_shadow_scorer_v1_on_second_fresh_surface",
    "selected_needs_labels": "create_second_surface_labeling_plan_for_shadow_generalization_v1",
    "selected_needs_learned_probability_coverage": "create_second_surface_learned_probability_coverage_plan_v1",
    "blocked_no_distinct_second_surface": "create_or_expand_second_fresh_candidate_source_for_shadow_generalization_v1",
    "blocked_no_candidate_source_meets_minimum": "create_or_expand_second_fresh_candidate_source_for_shadow_generalization_v1",
    "blocked_database_unavailable": "retry_local_database_discovery_for_shadow_generalization_v1",
}

CAVEATS = (
    "Selection/inventory artifact only; does not execute generalization audit or shadow scorer.",
    "Does not materialize full hybrid surface rows; records metadata for a future surface/audit pass.",
    "Blocked outcomes are expected and must not invent ranking runs or probabilities.",
    "No database writes, ranking creation, scorer execution, embedding generation, label ingest, online shadow, API/web, or production changes.",
)


class MLShadowScorerGeneralizationSecondSurfaceError(Exception):
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
    try:
        db = _assert_materializer_local_database_url(database_url)
    except MLFreshEvalSurfaceHybridMaterializeError as exc:
        raise MLShadowScorerGeneralizationSecondSurfaceError(str(exc)) from exc
    return {
        **db,
        "database_target_redacted": _redacted_database_url(database_url),
    }


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"{name} JSON missing metadata object")
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
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    if metadata.get("artifact_type") != artifact_type:
        raise MLShadowScorerGeneralizationSecondSurfaceError(
            f"{name} metadata.artifact_type must be {artifact_type}"
        )
    if metadata.get(version_field) != version:
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"{name} metadata.{version_field} must be {version}")
    return metadata


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
    lines = "".join(
        f"{work_id}\n" for work_id in sorted({str(work_id).strip() for work_id in work_ids if str(work_id).strip()})
    )
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


def _execute_select(cur: Any, sql: str, params: Sequence[Any] = ()) -> None:
    if not str(sql).lstrip().lower().startswith("select"):
        raise MLShadowScorerGeneralizationSecondSurfaceError("SQL guard allows SELECT statements only")
    cur.execute(sql, tuple(params))


def _label_row_is_explicit(row: Mapping[str, Any]) -> bool:
    return row_has_explicit_label({str(k): "" if v is None else str(v) for k, v in row.items()})


def _validate_plan(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-plan",
        artifact_type=PLAN_ARTIFACT_TYPE,
        version_field="plan_version",
        version=PLAN_VERSION,
    )
    checks = {
        "generalization_audit_plan_defined": payload.get("generalization_audit_plan_defined") is True,
        "generalization_audit_executed": payload.get("generalization_audit_executed") is False,
        "runtime_implementation_authorized": payload.get("runtime_implementation_authorized") is False,
        "recommended_next_stage": payload.get("recommended_next_stage") == EXPECTED_NEXT_STAGE,
        "missing_generalization_audit_plan_v1": _get(
            payload, "shadow_and_production_blockers.missing_generalization_audit_plan_v1"
        )
        is False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"generalization audit plan pre-checks failed: {failed}")
    return metadata


def _validate_online_policy(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    if payload.get("runtime_implementation_authorized") is not False:
        raise MLShadowScorerGeneralizationSecondSurfaceError("online policy runtime_implementation_authorized must be false")
    return metadata


def _validate_fresh_surface_policy(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], dict[str, Any], dict[str, Any]]:
    metadata = _validate_identity(
        payload,
        name="fresh-surface-policy",
        artifact_type=FRESH_SURFACE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=FRESH_SURFACE_POLICY_VERSION,
    )
    lift = _get(payload, "gate_linkage.material_lift_thresholds")
    if not isinstance(lift, Mapping):
        raise MLShadowScorerGeneralizationSecondSurfaceError("fresh surface policy missing material_lift_thresholds")
    if lift.get("delta_roc_auc_gte") != 0.03 and lift.get("or_delta_average_precision_gte") != 0.02:
        raise MLShadowScorerGeneralizationSecondSurfaceError("fresh surface policy material lift thresholds must be present")
    thresholds = _get(payload, "label_policy.minimum_confirmatory_label_thresholds")
    if not isinstance(thresholds, Mapping):
        raise MLShadowScorerGeneralizationSecondSurfaceError("fresh surface policy missing label thresholds")
    required_keys = (
        "minimum_candidate_work_count",
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
        "minimum_confirmatory_label_coverage_rate",
    )
    normalized: dict[str, Any] = {}
    for key in required_keys:
        value = thresholds.get(key)
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise MLShadowScorerGeneralizationSecondSurfaceError(f"fresh surface policy threshold {key} must be numeric")
        normalized[key] = value
    return metadata, dict(lift), normalized


def _validate_label_dataset(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    version = payload.get("dataset_version") or _get(payload, "metadata.dataset_version")
    if version != LABEL_DATASET_VERSION:
        raise MLShadowScorerGeneralizationSecondSurfaceError(
            f"label dataset version must be {LABEL_DATASET_VERSION}, got {version!r}"
        )
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise MLShadowScorerGeneralizationSecondSurfaceError("label dataset missing rows array")
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _validate_offline_scoring_v3(payload: Mapping[str, Any], *, policy_old_sha: str) -> Mapping[str, Any]:
    metadata = _metadata(payload, name="offline-production-candidate-scoring-v3")
    if metadata.get("artifact_type") != OFFLINE_SCORING_ARTIFACT_TYPE:
        raise MLShadowScorerGeneralizationSecondSurfaceError(
            f"offline scoring metadata.artifact_type must be {OFFLINE_SCORING_ARTIFACT_TYPE}"
        )
    if metadata.get("experiment_version") != OFFLINE_SCORING_VERSION:
        raise MLShadowScorerGeneralizationSecondSurfaceError(
            f"offline scoring metadata.experiment_version must be {OFFLINE_SCORING_VERSION}"
        )
    if metadata.get("eval_work_set_sha256") != OLD_217_EVAL_SHA:
        raise MLShadowScorerGeneralizationSecondSurfaceError(
            f"offline scoring metadata.eval_work_set_sha256 must be {OLD_217_EVAL_SHA}"
        )
    if policy_old_sha and policy_old_sha != OLD_217_EVAL_SHA:
        raise MLShadowScorerGeneralizationSecondSurfaceError("policy disallowed old eval SHA mismatch")
    return metadata


def _old_eval_ids_from_v3(payload: Mapping[str, Any]) -> set[str]:
    rows = payload.get("candidate_pool_rows")
    out: set[str] = set()
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    if _work_set_sha256(sorted(out)) != OLD_217_EVAL_SHA:
        raise MLShadowScorerGeneralizationSecondSurfaceError("old 217 IDs from v3 scoring artifact do not match eval_work_set_sha256")
    return out


def _first_surface_ids(payload: Mapping[str, Any] | None) -> set[str]:
    if not isinstance(payload, Mapping):
        return set()
    candidate_pool = payload.get("candidate_pool")
    if not isinstance(candidate_pool, Mapping):
        return set()
    expected_sha = candidate_pool.get("candidate_work_set_sha256")
    if expected_sha and expected_sha != DISALLOWED_CANDIDATE_SHA:
        raise MLShadowScorerGeneralizationSecondSurfaceError("first validated surface candidate SHA mismatch")
    rows = candidate_pool.get("candidate_rows")
    out: set[str] = set()
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                canonical = _canonical_from_value(row.get("canonical_openalex_work_id"))
                if canonical:
                    out.add(canonical)
    return out


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


def _label_coverage_summary(confirmatory_ids: set[str], label_groups: Mapping[str, Sequence[bool]]) -> dict[str, Any]:
    labeled_ids = sorted(work_id for work_id in confirmatory_ids if work_id in label_groups)
    positive_ids = sorted(work_id for work_id in labeled_ids if any(label_groups[work_id]))
    negative_ids = sorted(work_id for work_id in labeled_ids if label_groups[work_id] and all(not value for value in label_groups[work_id]))
    conflict_ids = sorted(work_id for work_id in labeled_ids if any(label_groups[work_id]) and any(not value for value in label_groups[work_id]))
    duplicate_ids = sorted(work_id for work_id in labeled_ids if len(label_groups[work_id]) > 1)
    eligible_count = len(confirmatory_ids)
    labeled_count = len(labeled_ids)
    return {
        "work_level": {
            "confirmatory_candidate_work_count": eligible_count,
            "confirmatory_labeled_work_count": labeled_count,
            "confirmatory_unlabeled_work_count": max(0, eligible_count - labeled_count),
            "confirmatory_positive_work_count": len(positive_ids),
            "confirmatory_negative_work_count": len(negative_ids),
            "distinct_negative_work_count": len(negative_ids),
            "conflicting_target_work_group_count": len(conflict_ids),
            "duplicate_labeled_work_group_count": len(duplicate_ids),
            "label_coverage_rate": labeled_count / eligible_count if eligible_count else 0.0,
            "conflicting_work_ids_preview": conflict_ids[:25],
            "duplicate_work_ids_preview": duplicate_ids[:25],
        },
        "label_coverage_rate": labeled_count / eligible_count if eligible_count else 0.0,
        "silent_conflict_merge_used": False,
    }


def _query_candidate_source_rows(conn: Any, family: str, ranking_run_id: str | None = None) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        if ranking_run_id:
            _execute_select(
                cur,
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
                WHERE ps.recommendation_family = %s AND ps.ranking_run_id = %s
                GROUP BY ps.ranking_run_id, rr.status, rr.ranking_version, rr.corpus_snapshot_version,
                         rr.embedding_version, rr.started_at, rr.finished_at, rr.config_json, rr.counts_json
                """,
                (family, ranking_run_id),
            )
        else:
            _execute_select(
                cur,
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
                ORDER BY rr.finished_at DESC NULLS LAST, rr.started_at DESC NULLS LAST, ps.ranking_run_id DESC
                """,
                (family,),
            )
        return [_json_safe(dict(row)) for row in cur.fetchall()]


def _query_candidate_pool(conn: Any, *, ranking_run_id: str, family: str) -> list[dict[str, Any]]:
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
                w.citation_count,
                w.inclusion_status,
                w.corpus_snapshot_version
            FROM paper_scores ps
            JOIN works w ON w.id = ps.work_id
            WHERE ps.ranking_run_id = %s
              AND ps.recommendation_family = %s
            ORDER BY ps.final_score DESC NULLS LAST, ps.work_id ASC
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
                "family": row.get("recommendation_family") or family,
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


def _query_embedding_coverage_count(conn: Any, *, internal_work_ids: Sequence[Any], embedding_version: str | None) -> int:
    ids = [item for item in internal_work_ids if item is not None]
    if not ids or not embedding_version:
        return 0
    with conn.cursor(row_factory=dict_row) as cur:
        _execute_select(
            cur,
            """
            SELECT COUNT(DISTINCT e.work_id) AS embedding_coverage_count
            FROM embeddings e
            WHERE e.embedding_version = %s
              AND e.work_id = ANY(%s)
            """,
            (embedding_version, ids),
        )
        row = cur.fetchone()
    if not row:
        return 0
    return int(dict(row).get("embedding_coverage_count") or 0)


def _candidate_work_ids(candidate_rows: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted({str(row["canonical_openalex_work_id"]) for row in candidate_rows if row.get("canonical_openalex_work_id")})


def _approved_probability_probe(
    *,
    repo_root: Path,
    ranking_run_id: str,
    candidate_sha: str | None,
    candidate_work_count: int,
) -> dict[str, Any]:
    probes = [
        repo_root / "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
        repo_root / "docs/audit/ml-shadow-scorer-v1-audit-output.json",
        repo_root / "docs/audit/ml-hybrid-validation-on-fresh-surface-v1.json",
    ]
    for path in probes:
        if not path.exists():
            continue
        try:
            payload = _load_json_object(path)
        except MLShadowScorerGeneralizationSecondSurfaceError:
            continue
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
        payload_sha = metadata.get("candidate_pool_work_set_sha256") or _get(payload, "execution_verification.candidate_pool_work_set_sha256")
        payload_run = metadata.get("ranking_run_id")
        rows = payload.get("shadow_output_rows") or payload.get("candidate_work_scores")
        if payload_sha == candidate_sha and payload_run == ranking_run_id and isinstance(rows, list):
            count = sum(
                1
                for row in rows
                if isinstance(row, Mapping) and _float_or_none(row.get("audit_embedding_probability_work")) is not None
            )
            return {
                "probe_status": "found",
                "source_artifact_path": portable_repo_path(path.resolve(), repo_root=repo_root),
                "learned_probability_coverage_count": count,
                "full_coverage": count == candidate_work_count,
            }
    return {
        "probe_status": "not_found",
        "source_artifact_path": None,
        "learned_probability_coverage_count": 0,
        "full_coverage": False,
    }


def _threshold_check(
    *,
    confirmatory_eligible_work_count: int,
    final_score_coverage_count: int,
    candidate_pool_work_count: int,
    learned_probability_coverage_count: int,
    label_coverage: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    work = label_coverage.get("work_level") if isinstance(label_coverage.get("work_level"), Mapping) else {}
    checks = {
        "minimum_confirmatory_candidate_work_count": (
            confirmatory_eligible_work_count,
            thresholds["minimum_candidate_work_count"],
        ),
        "minimum_confirmatory_labeled_work_count": (
            work.get("confirmatory_labeled_work_count"),
            thresholds["minimum_confirmatory_labeled_work_count"],
        ),
        "minimum_confirmatory_positive_work_count": (
            work.get("confirmatory_positive_work_count"),
            thresholds["minimum_confirmatory_positive_work_count"],
        ),
        "minimum_confirmatory_negative_work_count": (
            work.get("confirmatory_negative_work_count"),
            thresholds["minimum_confirmatory_negative_work_count"],
        ),
        "minimum_distinct_negative_work_count": (
            work.get("distinct_negative_work_count"),
            thresholds["minimum_distinct_negative_work_count"],
        ),
        "minimum_confirmatory_label_coverage_rate": (
            label_coverage.get("label_coverage_rate"),
            thresholds["minimum_confirmatory_label_coverage_rate"],
        ),
        "unresolved_label_conflicts": (
            work.get("conflicting_target_work_group_count"),
            0,
        ),
        "final_score_coverage": (
            final_score_coverage_count,
            candidate_pool_work_count,
        ),
        "learned_probability_coverage": (
            learned_probability_coverage_count,
            candidate_pool_work_count,
        ),
    }
    out: dict[str, dict[str, Any]] = {}
    for key, (observed, threshold) in checks.items():
        if key == "unresolved_label_conflicts":
            passed = observed == 0
        else:
            passed = (
                isinstance(observed, (int, float))
                and not isinstance(observed, bool)
                and isinstance(threshold, (int, float))
                and not isinstance(threshold, bool)
                and float(observed) >= float(threshold)
            )
        out[key] = {"observed": observed, "threshold": threshold, "passed": passed}
    return out


def _all_pass(keys: Sequence[str], threshold_check: Mapping[str, Mapping[str, Any]]) -> bool:
    return all(threshold_check.get(key, {}).get("passed") is True for key in keys)


def _recency_key(source: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(source.get("finished_at") or ""),
        str(source.get("started_at") or ""),
        str(source.get("ranking_run_id") or ""),
    )


def _source_summary(
    *,
    source: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    family: str,
    old_217_ids: set[str],
    first_surface_ids: set[str],
    label_groups: Mapping[str, Sequence[bool]],
    thresholds: Mapping[str, Any],
    repo_root: Path,
    conn: Any,
) -> dict[str, Any]:
    ranking_run_id = str(source.get("ranking_run_id") or "")
    work_ids = _candidate_work_ids(candidate_rows)
    work_set = set(work_ids)
    candidate_sha = _work_set_sha256(work_ids) if work_ids else None
    old_overlap = work_set.intersection(old_217_ids)
    first_overlap = work_set.intersection(first_surface_ids)
    combined_overlap = old_overlap.union(first_overlap)
    confirmatory_ids = work_set.difference(combined_overlap)
    final_score_coverage_count = sum(1 for row in candidate_rows if row.get("canonical_openalex_work_id") and row.get("final_score") is not None)
    missing_final_score_count = max(0, len(work_ids) - final_score_coverage_count)
    label_coverage = _label_coverage_summary(confirmatory_ids, label_groups)
    corpus_versions = sorted({str(row.get("corpus_snapshot_version")) for row in candidate_rows if row.get("corpus_snapshot_version")})
    corpus_snapshot_version = source.get("corpus_snapshot_version") or (corpus_versions[0] if len(corpus_versions) == 1 else None)
    embedding_version = source.get("embedding_version")
    embedding_coverage_count = _query_embedding_coverage_count(
        conn,
        internal_work_ids=[row.get("internal_work_id") for row in candidate_rows],
        embedding_version=str(embedding_version or ""),
    )
    probability_probe = _approved_probability_probe(
        repo_root=repo_root,
        ranking_run_id=ranking_run_id,
        candidate_sha=candidate_sha,
        candidate_work_count=len(work_ids),
    )
    learned_probability_coverage_count = int(probability_probe.get("learned_probability_coverage_count") or 0)
    threshold_check = _threshold_check(
        confirmatory_eligible_work_count=len(confirmatory_ids),
        final_score_coverage_count=final_score_coverage_count,
        candidate_pool_work_count=len(work_ids),
        learned_probability_coverage_count=learned_probability_coverage_count,
        label_coverage=label_coverage,
        thresholds=thresholds,
    )
    disallowed_reasons: list[str] = []
    if ranking_run_id == DISALLOWED_RANKING_RUN_ID:
        disallowed_reasons.append("ranking_run_id_matches_first_validated_surface")
    if candidate_sha == DISALLOWED_CANDIDATE_SHA:
        disallowed_reasons.append("candidate_sha_matches_first_validated_surface")
    candidate_minimum_pass = threshold_check["minimum_confirmatory_candidate_work_count"]["passed"] is True
    return {
        "ranking_run_id": ranking_run_id,
        "family": family,
        "corpus_snapshot_version": corpus_snapshot_version,
        "embedding_version": embedding_version,
        "status": source.get("status"),
        "ranking_version": source.get("ranking_version"),
        "started_at": source.get("started_at"),
        "finished_at": source.get("finished_at"),
        "candidate_pool_work_count": len(work_ids),
        "candidate_pool_work_set_sha256": candidate_sha,
        "final_score_coverage_count": final_score_coverage_count,
        "missing_final_score_count": missing_final_score_count,
        "confirmatory_metric_eligible_work_count": len(confirmatory_ids),
        "old_217_overlap_count": len(old_overlap),
        "rank_9f4b2a2084_overlap_count": len(first_overlap),
        "combined_prior_surface_overlap_count": len(combined_overlap),
        "overlap_work_ids_preview": sorted(combined_overlap)[:25],
        "label_coverage": label_coverage,
        "embedding_coverage_probe": {
            "embedding_version": embedding_version,
            "embedding_coverage_count": embedding_coverage_count,
            "candidate_pool_work_count": len(work_ids),
            "full_embedding_coverage": embedding_coverage_count == len(work_ids),
        },
        "approved_upstream_probability_probe": probability_probe,
        "learned_probability_coverage_count": learned_probability_coverage_count,
        "missing_learned_probability_count": max(0, len(work_ids) - learned_probability_coverage_count),
        "threshold_check": threshold_check,
        "distinct_from_first_validated_surface": not disallowed_reasons,
        "disallowed_reasons": disallowed_reasons,
        "candidate_minimum_pass": candidate_minimum_pass,
        "candidate_row_preview": [
            {
                "canonical_openalex_work_id": row.get("canonical_openalex_work_id"),
                "title": row.get("title"),
                "final_score": row.get("final_score"),
                "heuristic_rank": row.get("heuristic_rank"),
            }
            for row in list(candidate_rows)[:5]
        ],
    }


def _select_source(candidates: Sequence[Mapping[str, Any]], thresholds: Mapping[str, Any]) -> tuple[str, dict[str, Any] | None]:
    distinct = [dict(source) for source in candidates if source.get("distinct_from_first_validated_surface") is True]
    if not distinct:
        return "blocked_no_distinct_second_surface", None
    eligible = [
        source
        for source in distinct
        if int(source.get("confirmatory_metric_eligible_work_count") or 0) >= int(thresholds["minimum_candidate_work_count"])
    ]
    if not eligible:
        return "blocked_no_candidate_source_meets_minimum", None
    selected = max(
        eligible,
        key=lambda source: (
            int(source.get("confirmatory_metric_eligible_work_count") or 0),
            _recency_key(source),
        ),
    )
    threshold_check = selected.get("threshold_check") if isinstance(selected.get("threshold_check"), Mapping) else {}
    label_keys = [
        "minimum_confirmatory_candidate_work_count",
        "minimum_confirmatory_labeled_work_count",
        "minimum_confirmatory_positive_work_count",
        "minimum_confirmatory_negative_work_count",
        "minimum_distinct_negative_work_count",
        "minimum_confirmatory_label_coverage_rate",
        "unresolved_label_conflicts",
    ]
    if not _all_pass(label_keys, threshold_check):
        return "selected_needs_labels", selected
    if not _all_pass(["final_score_coverage", "learned_probability_coverage"], threshold_check):
        return "selected_needs_learned_probability_coverage", selected
    return "selected_ready_for_generalization_audit", selected


def _query_intent() -> dict[str, Any]:
    return {
        "source_tables": ["ranking_runs", "paper_scores", "works", "embeddings"],
        "contract": "SELECT-only discovery and coverage probes; no database mutations",
        "candidate_query": "paper_scores joined to works by ranking_run_id and recommendation_family",
        "embedding_coverage_probe": "count existing embeddings rows for selected source embedding_version",
        "learned_probability_probe": "read-only committed audit artifact search; no scorer execution",
    }


def _base_inputs(
    *,
    repo_root: Path,
    plan_path: Path,
    policy_path: Path,
    fresh_policy_path: Path,
    label_path: Path,
    conflict_path: Path,
    scoring_path: Path,
    first_surface_path: Path | None,
) -> list[dict[str, str]]:
    inputs = [
        _input_record("generalization_audit_plan", plan_path, repo_root=repo_root),
        _input_record("online_shadow_policy", policy_path, repo_root=repo_root),
        _input_record("fresh_surface_policy", fresh_policy_path, repo_root=repo_root),
        _input_record("label_dataset", label_path, repo_root=repo_root),
        _input_record("conflict_policy", conflict_path, repo_root=repo_root),
        _input_record("offline_production_candidate_scoring_v3", scoring_path, repo_root=repo_root),
    ]
    if first_surface_path is not None and first_surface_path.exists():
        inputs.append(_input_record("first_validated_surface", first_surface_path, repo_root=repo_root))
    return inputs


def build_ml_shadow_scorer_generalization_second_surface_payload(
    conn: Any | None,
    *,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    offline_production_candidate_scoring_v3_path: Path,
    first_validated_surface_path: Path | None = None,
    database_url: str | None = None,
    family: str = DEFAULT_FAMILY,
    ranking_run_id: str | None = None,
    surface_version: str = SURFACE_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
    database_unavailable_error: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    plan_path = Path(generalization_audit_plan_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    fresh_policy_path = Path(fresh_surface_policy_path).resolve()
    label_path = Path(label_dataset_path).resolve()
    conflict_path = Path(conflict_policy_path).resolve()
    scoring_path = Path(offline_production_candidate_scoring_v3_path).resolve()
    first_surface_path = (
        Path(first_validated_surface_path).resolve()
        if first_validated_surface_path is not None
        else (root / DEFAULT_FIRST_VALIDATED_SURFACE).resolve()
    )

    plan_payload = _load_json_object(plan_path)
    policy_payload = _load_json_object(policy_path)
    fresh_policy_payload = _load_json_object(fresh_policy_path)
    label_payload = _load_json_object(label_path)
    scoring_payload = _load_json_object(scoring_path)
    first_surface_payload = _load_json_object(first_surface_path) if first_surface_path.exists() else None

    plan_metadata = _validate_plan(plan_payload)
    policy_metadata = _validate_online_policy(policy_payload)
    fresh_policy_metadata, _lift_thresholds, thresholds = _validate_fresh_surface_policy(fresh_policy_payload)
    label_rows = _validate_label_dataset(label_payload)
    if not conflict_path.exists():
        raise MLShadowScorerGeneralizationSecondSurfaceError(f"conflict policy does not exist: {conflict_path}")
    policy_old_sha = str(fresh_policy_metadata.get("disallowed_eval_work_set_sha256") or "")
    scoring_metadata = _validate_offline_scoring_v3(scoring_payload, policy_old_sha=policy_old_sha)
    old_217_ids = _old_eval_ids_from_v3(scoring_payload)
    first_surface_ids = _first_surface_ids(first_surface_payload)
    label_groups = _label_groups(label_rows)
    inputs = _base_inputs(
        repo_root=root,
        plan_path=plan_path,
        policy_path=policy_path,
        fresh_policy_path=fresh_policy_path,
        label_path=label_path,
        conflict_path=conflict_path,
        scoring_path=scoring_path,
        first_surface_path=first_surface_path if first_surface_path.exists() else None,
    )
    db_summary = assert_local_database_url(database_url) if database_url else {
        "database_target_redacted": None,
        "read_only_contract": "SELECT-only queries; no database mutations",
        "local_database_url_confirmed": None,
    }

    if conn is None:
        status = "blocked_database_unavailable"
        selected = None
        considered: list[dict[str, Any]] = []
    else:
        source_rows = _query_candidate_source_rows(conn, str(family or DEFAULT_FAMILY), ranking_run_id=ranking_run_id)
        considered = []
        for source in source_rows:
            rid = str(source.get("ranking_run_id") or "").strip()
            if not rid:
                continue
            candidate_rows = _query_candidate_pool(conn, ranking_run_id=rid, family=str(family or DEFAULT_FAMILY))
            considered.append(
                _source_summary(
                    source=source,
                    candidate_rows=candidate_rows,
                    family=str(family or DEFAULT_FAMILY),
                    old_217_ids=old_217_ids,
                    first_surface_ids=first_surface_ids,
                    label_groups=label_groups,
                    thresholds=thresholds,
                    repo_root=root,
                    conn=conn,
                )
            )
        considered.sort(
            key=lambda source: (
                int(source.get("confirmatory_metric_eligible_work_count") or 0),
                _recency_key(source),
            ),
            reverse=True,
        )
        status, selected = _select_source(considered, thresholds)

    recommended_next_stage = STATUS_TO_NEXT_STAGE[status]
    selected_summary = None
    if selected is not None:
        selected_summary = {
            "ranking_run_id": selected["ranking_run_id"],
            "family": selected["family"],
            "corpus_snapshot_version": selected.get("corpus_snapshot_version"),
            "embedding_version": selected.get("embedding_version"),
            "candidate_pool_work_count": selected["candidate_pool_work_count"],
            "candidate_pool_work_set_sha256": selected["candidate_pool_work_set_sha256"],
            "confirmatory_metric_eligible_work_count": selected["confirmatory_metric_eligible_work_count"],
            "final_score_coverage_count": selected["final_score_coverage_count"],
            "missing_final_score_count": selected["missing_final_score_count"],
            "learned_probability_coverage_count": selected["learned_probability_coverage_count"],
            "missing_learned_probability_count": selected["missing_learned_probability_count"],
            "distinct_from_first_validated_surface": selected["distinct_from_first_validated_surface"],
        }
    label_coverage = selected.get("label_coverage") if isinstance(selected, Mapping) else None
    learned_probability_coverage = {
        "learned_probability_coverage_count": selected.get("learned_probability_coverage_count") if isinstance(selected, Mapping) else 0,
        "missing_learned_probability_count": selected.get("missing_learned_probability_count") if isinstance(selected, Mapping) else 0,
        "approved_upstream_probability_probe": selected.get("approved_upstream_probability_probe") if isinstance(selected, Mapping) else None,
        "embedding_coverage_probe": selected.get("embedding_coverage_probe") if isinstance(selected, Mapping) else None,
        "scorer_execution_used": False,
    }
    threshold_check = selected.get("threshold_check") if isinstance(selected, Mapping) else {}
    overlap_report = {
        "old_217_eval_work_set_sha256": OLD_217_EVAL_SHA,
        "first_validated_candidate_work_set_sha256": DISALLOWED_CANDIDATE_SHA,
        "old_217_overlap_count": selected.get("old_217_overlap_count") if isinstance(selected, Mapping) else 0,
        "rank_9f4b2a2084_overlap_count": selected.get("rank_9f4b2a2084_overlap_count") if isinstance(selected, Mapping) else 0,
        "combined_prior_surface_overlap_count": selected.get("combined_prior_surface_overlap_count") if isinstance(selected, Mapping) else 0,
        "confirmatory_denominator_excludes_prior_overlaps": True,
    }
    readiness = {
        "ready_for_generalization_audit_execution": status == "selected_ready_for_generalization_audit",
        "status": status,
        "candidate_source_selected": selected is not None,
        "generalization_audit_executed": False,
    }
    blockers = {
        "missing_generalization_audit_plan_v1": False,
        "missing_generalization_second_surface_selected": status != "selected_ready_for_generalization_audit",
        "missing_generalization_audit_on_second_surface": True,
        "missing_generalization_audit_gates": True,
        "missing_online_shadow_implementation_disabled_by_default": True,
        "missing_shadow_runtime_isolation_verification": True,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "runtime_implementation_authorized": False,
    }
    metadata = {
        "artifact_type": ARTIFACT_TYPE,
        "surface_version": surface_version,
        "generated_at": generated_at or _now_iso_z(),
        "inputs": inputs,
        "database_target_redacted": db_summary.get("database_target_redacted"),
        "database": db_summary,
        "query_intent": _query_intent(),
        "source_plan_version": plan_metadata.get("plan_version"),
        "source_online_shadow_policy_version": policy_metadata.get("policy_version"),
        "source_policy_version": fresh_policy_metadata.get("policy_version"),
        "source_label_dataset_version": label_payload.get("dataset_version") or _get(label_payload, "metadata.dataset_version"),
        "source_offline_scoring_version": scoring_metadata.get("experiment_version"),
        "generalization_audit_executed": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "database_unavailable_error": database_unavailable_error,
        "caveats": list(CAVEATS),
    }
    return {
        "metadata": metadata,
        "discovery_summary": {
            "status": status,
            "family": str(family or DEFAULT_FAMILY),
            "explicit_ranking_run_id": ranking_run_id,
            "candidate_sources_considered_count": len(considered),
            "selected_ranking_run_id": selected_summary.get("ranking_run_id") if selected_summary else None,
            "recommended_next_stage": recommended_next_stage,
        },
        "selected_second_surface": selected_summary,
        "candidate_sources_considered": considered,
        "overlap_report": overlap_report,
        "label_coverage": label_coverage,
        "learned_probability_coverage": learned_probability_coverage,
        "threshold_check": threshold_check,
        "readiness_for_generalization_audit": readiness,
        "blocked_actions": [
            "database_writes",
            "ranking_run_creation",
            "scorer_execution",
            "embedding_generation",
            "learned_scorer_application",
            "label_ingest",
            "online_shadow_execution",
            "api_web_change",
            "production_default_change",
        ],
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": recommended_next_stage,
        "caveats": list(CAVEATS),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4f}" if abs(value) < 1 else f"{value:.2f}"
    return str(value)


def markdown_from_ml_shadow_scorer_generalization_second_surface(payload: Mapping[str, Any]) -> str:
    discovery = payload["discovery_summary"]
    selected = payload.get("selected_second_surface")
    readiness = payload["readiness_for_generalization_audit"]
    lines = [
        f"# ML Shadow Scorer v1 Generalization Second Surface ({payload['metadata']['surface_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact performs read-only source discovery for a distinct second fresh surface. It does not execute the generalization audit or shadow scorer and does not authorize runtime or production behavior.",
        "",
        f"- Status: `{discovery['status']}`",
        f"- Sources considered: {discovery['candidate_sources_considered_count']}",
        f"- Ready for generalization audit execution: {readiness['ready_for_generalization_audit_execution']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Selected Second Surface",
        "",
    ]
    if selected:
        for key, value in selected.items():
            lines.append(f"- `{key}`: {_fmt(value)}")
    else:
        lines.append("- No qualifying second surface was selected.")
    lines.extend(["", "## Sources Considered", "", "| Ranking run | Candidate count | Confirmatory eligible | SHA | Status |", "| --- | ---: | ---: | --- | --- |"])
    for source in payload.get("candidate_sources_considered", []):
        if isinstance(source, Mapping):
            lines.append(
                "| "
                f"`{source.get('ranking_run_id')}` | "
                f"{_fmt(source.get('candidate_pool_work_count'))} | "
                f"{_fmt(source.get('confirmatory_metric_eligible_work_count'))} | "
                f"`{source.get('candidate_pool_work_set_sha256')}` | "
                f"{'distinct' if source.get('distinct_from_first_validated_surface') else ', '.join(source.get('disallowed_reasons') or [])} |"
            )
    lines.extend(["", "## Overlap Report", ""])
    for key, value in payload["overlap_report"].items():
        lines.append(f"- `{key}`: {_fmt(value)}")
    lines.extend(["", "## Threshold Check", ""])
    for key, item in payload.get("threshold_check", {}).items():
        if isinstance(item, Mapping):
            lines.append(
                f"- `{key}`: observed `{_fmt(item.get('observed'))}` / threshold `{_fmt(item.get('threshold'))}` / passed `{item.get('passed')}`"
            )
    lines.extend(["", "## Learned Probability Coverage", ""])
    for key, value in payload["learned_probability_coverage"].items():
        lines.append(f"- `{key}`: {_fmt(value)}")
    lines.extend(["", "## Blockers", ""])
    for key, value in payload["shadow_and_production_blockers"].items():
        lines.append(f"- `{key}`: {value}")
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def _connect_readonly(database_url: str) -> Any:
    return psycopg.connect(database_url, autocommit=True, options="-c default_transaction_read_only=on")


def write_ml_shadow_scorer_generalization_second_surface(
    *,
    generalization_audit_plan_path: Path,
    online_shadow_policy_path: Path,
    fresh_surface_policy_path: Path,
    label_dataset_path: Path,
    conflict_policy_path: Path,
    offline_production_candidate_scoring_v3_path: Path,
    first_validated_surface_path: Path | None = None,
    database_url: str | None = None,
    ranking_run_id: str | None = None,
    family: str = DEFAULT_FAMILY,
    output_path: Path,
    markdown_output_path: Path,
    surface_version: str = SURFACE_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    explicit_database_url = database_url is not None
    db_url = database_url or _database_url_from_env()
    db_summary = assert_local_database_url(db_url)
    conn: Any | None = None
    database_unavailable_error: str | None = None
    try:
        conn = _connect_readonly(db_url)
    except Exception as exc:  # pragma: no cover - exact driver exception varies by environment
        if explicit_database_url:
            raise MLShadowScorerGeneralizationSecondSurfaceError(f"local database unavailable: {exc}") from exc
        database_unavailable_error = f"{type(exc).__name__}: {exc}"
    try:
        payload = build_ml_shadow_scorer_generalization_second_surface_payload(
            conn,
            generalization_audit_plan_path=generalization_audit_plan_path,
            online_shadow_policy_path=online_shadow_policy_path,
            fresh_surface_policy_path=fresh_surface_policy_path,
            label_dataset_path=label_dataset_path,
            conflict_policy_path=conflict_policy_path,
            offline_production_candidate_scoring_v3_path=offline_production_candidate_scoring_v3_path,
            first_validated_surface_path=first_validated_surface_path,
            database_url=db_url,
            family=family,
            ranking_run_id=ranking_run_id,
            surface_version=surface_version,
            repo_root=repo_root,
            database_unavailable_error=database_unavailable_error,
        )
        payload["metadata"]["database_target_redacted"] = db_summary.get("database_target_redacted")
        payload["metadata"]["database"] = db_summary
    finally:
        if conn is not None:
            conn.close()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_generalization_second_surface(payload), encoding="utf-8")
    return payload


__all__ = [
    "ARTIFACT_TYPE",
    "SURFACE_VERSION",
    "MLShadowScorerGeneralizationSecondSurfaceError",
    "assert_local_database_url",
    "build_ml_shadow_scorer_generalization_second_surface_payload",
    "markdown_from_ml_shadow_scorer_generalization_second_surface",
    "write_ml_shadow_scorer_generalization_second_surface",
]

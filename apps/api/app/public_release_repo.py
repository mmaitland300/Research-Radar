"""Read-only serving context for the promoted public release."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import database_url_from_env

PUBLIC_RELEASE_SCORER_KIND = "materialized_paper_scores"
EXPECTED_FAMILIES: tuple[str, ...] = ("emerging", "bridge", "undercited")
NON_SERVING_EMBEDDING_VERSIONS = frozenset({"none", "none-v0"})


@dataclass(frozen=True)
class MaterializedRunContext:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    status: str
    finished_at: datetime | None
    error_message: str | None
    config_json: dict[str, Any]
    counts_json: dict[str, Any]


@dataclass(frozen=True)
class PublicReleasePromotion:
    promotion_id: int
    promoted_at: datetime
    promoted_by: str
    note: str | None
    run: MaterializedRunContext
    scorer_kind: str = PUBLIC_RELEASE_SCORER_KIND


@dataclass(frozen=True)
class PublicReleaseDiagnostics:
    serveable: bool
    membership_count: int
    embedding_count: int
    missing_embedding_count: int
    family_score_counts: dict[str, int]
    expected_family_score_counts: dict[str, int]
    out_of_membership_score_count: int
    cluster_version: str | None
    cluster_assignment_count: int | None
    missing_cluster_assignment_count: int | None
    out_of_membership_cluster_count: int | None
    failures: tuple[str, ...]


def _json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _run_from_row(row: Mapping[str, Any]) -> MaterializedRunContext:
    return MaterializedRunContext(
        ranking_run_id=str(row["ranking_run_id"]),
        ranking_version=str(row["ranking_version"]),
        corpus_snapshot_version=str(row["corpus_snapshot_version"]),
        embedding_version=str(row["embedding_version"]),
        status=str(row["status"]),
        finished_at=row.get("finished_at"),
        error_message=(
            str(row["error_message"]) if row.get("error_message") is not None else None
        ),
        config_json=_json_object(row.get("config_json")),
        counts_json=_json_object(row.get("counts_json")),
    )


def fetch_latest_public_release_promotion(
    conn: psycopg.Connection,
) -> PublicReleasePromotion | None:
    """Return the newest promotion even when its referenced run is not serveable."""
    row = conn.execute("""
        SELECT
            pr.promotion_id,
            pr.promoted_at,
            pr.promoted_by,
            pr.note,
            rr.ranking_run_id,
            rr.ranking_version,
            rr.corpus_snapshot_version,
            rr.embedding_version,
            rr.status,
            rr.finished_at,
            rr.error_message,
            rr.config_json,
            rr.counts_json
        FROM public_release_promotions pr
        JOIN ranking_runs rr ON rr.ranking_run_id = pr.ranking_run_id
        ORDER BY pr.promotion_id DESC
        LIMIT 1
        """).fetchone()
    if row is None:
        return None
    return PublicReleasePromotion(
        promotion_id=int(row["promotion_id"]),
        promoted_at=row["promoted_at"],
        promoted_by=str(row["promoted_by"]),
        note=str(row["note"]) if row.get("note") is not None else None,
        run=_run_from_row(row),
    )


def fetch_succeeded_materialized_run(conn: psycopg.Connection, *, ranking_run_id: str) -> MaterializedRunContext | None:
    """Resolve one exact historical run without any latest-run fallback."""
    row = conn.execute(
        """
        SELECT
            ranking_run_id,
            ranking_version,
            corpus_snapshot_version,
            embedding_version,
            status,
            finished_at,
            error_message,
            config_json,
            counts_json
        FROM ranking_runs
        WHERE ranking_run_id = %s
          AND status = 'succeeded'
        """,
        (ranking_run_id,),
    ).fetchone()
    return None if row is None else _run_from_row(row)


def load_latest_public_release_promotion() -> PublicReleasePromotion | None:
    with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
        return fetch_latest_public_release_promotion(conn)


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _expected_family_counts(counts_json: Mapping[str, Any]) -> dict[str, int]:
    raw = counts_json.get("rows_by_family")
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, int] = {}
    for family in EXPECTED_FAMILIES:
        count = _positive_int(raw.get(family))
        if count is not None:
            out[family] = count
    return out


def _validate_materialized_config(
    config_json: Mapping[str, Any],
    *,
    corpus_snapshot_version: str,
) -> list[str]:
    failures: list[str] = []
    raw_families = config_json.get("families_written")
    if (
        not isinstance(raw_families, list)
        or not all(isinstance(family, str) for family in raw_families)
        or len(raw_families) != len(EXPECTED_FAMILIES)
        or set(raw_families) != set(EXPECTED_FAMILIES)
    ):
        failures.append("materialized_families_invalid")

    selection_scope = config_json.get("selection_scope")
    if not isinstance(selection_scope, Mapping) or selection_scope.get("type") != "included_works":
        failures.append("materialized_selection_scope_invalid")
    elif selection_scope.get("corpus_snapshot_version") != corpus_snapshot_version:
        failures.append("materialized_selection_scope_snapshot_mismatch")
    return failures


def inspect_public_release_serveability(
    conn: psycopg.Connection,
    promotion: PublicReleasePromotion,
) -> PublicReleaseDiagnostics:
    """Check that every artifact needed by the promoted materialized run is readable."""
    run = promotion.run
    failures: list[str] = []
    if run.status != "succeeded":
        failures.append("ranking_run_not_succeeded")
    if run.finished_at is None:
        failures.append("ranking_run_not_finished")
    if run.error_message is not None:
        failures.append("ranking_run_has_error")
    embedding_version = run.embedding_version.strip()
    if not embedding_version or embedding_version.lower() in NON_SERVING_EMBEDDING_VERSIONS:
        failures.append("embedding_version_not_serveable")

    coverage = conn.execute(
        """
        SELECT
            COUNT(*) AS membership_count,
            COUNT(e.work_id) AS embedding_count
        FROM work_source_snapshot_memberships wssm
        LEFT JOIN embeddings e
          ON e.work_id = wssm.work_id
         AND e.embedding_version = %s
        WHERE wssm.source_snapshot_version = %s
          AND wssm.inclusion_status = 'included'
        """,
        (run.embedding_version, run.corpus_snapshot_version),
    ).fetchone()
    membership_count = int(coverage["membership_count"] or 0)
    embedding_count = int(coverage["embedding_count"] or 0)
    missing_embedding_count = max(0, membership_count - embedding_count)
    if membership_count <= 0:
        failures.append("snapshot_membership_empty")
    if missing_embedding_count:
        failures.append("embedding_coverage_incomplete")

    score_rows = conn.execute(
        """
        SELECT
            ps.recommendation_family,
            COUNT(*) FILTER (WHERE wssm.work_id IS NOT NULL) AS member_score_count,
            COUNT(*) FILTER (WHERE wssm.work_id IS NULL) AS out_of_membership_count
        FROM paper_scores ps
        LEFT JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = ps.work_id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        WHERE ps.ranking_run_id = %s
        GROUP BY ps.recommendation_family
        """,
        (run.corpus_snapshot_version, run.ranking_run_id),
    ).fetchall()
    family_score_counts = {family: 0 for family in EXPECTED_FAMILIES}
    out_of_membership_score_count = 0
    for row in score_rows:
        family = str(row["recommendation_family"])
        if family in family_score_counts:
            family_score_counts[family] = int(row["member_score_count"] or 0)
        out_of_membership_score_count += int(row["out_of_membership_count"] or 0)

    expected_family_score_counts = _expected_family_counts(run.counts_json)
    raw_expected_family_counts = run.counts_json.get("rows_by_family")
    if (
        not isinstance(raw_expected_family_counts, Mapping)
        or set(raw_expected_family_counts) != set(EXPECTED_FAMILIES)
        or set(expected_family_score_counts) != set(EXPECTED_FAMILIES)
    ):
        failures.append("expected_family_score_counts_missing")
    for family in EXPECTED_FAMILIES:
        actual = family_score_counts[family]
        expected = expected_family_score_counts.get(family)
        if actual <= 0:
            failures.append(f"family_score_rows_missing:{family}")
        if expected is not None and actual != expected:
            failures.append(f"family_score_count_mismatch:{family}")
    for family in ("emerging", "bridge"):
        if family_score_counts[family] != membership_count:
            failures.append(f"family_snapshot_coverage_incomplete:{family}")
    if not 1 <= family_score_counts["undercited"] <= membership_count:
        failures.append("family_snapshot_subset_invalid:undercited")
    if out_of_membership_score_count:
        failures.append("ranking_rows_outside_snapshot_membership")

    expected_candidates = _positive_int(run.counts_json.get("total_candidate_works"))
    if expected_candidates is None:
        failures.append("expected_candidate_count_missing")
    elif expected_candidates != membership_count:
        failures.append("candidate_membership_count_mismatch")
    expected_total_rows = _positive_int(run.counts_json.get("total_rows_written"))
    actual_total_rows = sum(family_score_counts.values())
    if expected_total_rows is None:
        failures.append("expected_total_score_count_missing")
    elif expected_total_rows != actual_total_rows:
        failures.append("total_score_count_mismatch")

    if not run.config_json:
        failures.append("materialized_scorer_config_missing")
    else:
        failures.extend(
            _validate_materialized_config(
                run.config_json,
                corpus_snapshot_version=run.corpus_snapshot_version,
            )
        )

    cluster_version: str | None = None
    cluster_assignment_count: int | None = None
    missing_cluster_assignment_count: int | None = None
    out_of_membership_cluster_count: int | None = None
    clustering_artifact = run.config_json.get("clustering_artifact")
    if clustering_artifact is not None:
        if not isinstance(clustering_artifact, Mapping):
            failures.append("clustering_artifact_invalid")
        else:
            raw_cluster_version = clustering_artifact.get("cluster_version")
            cluster_version = str(raw_cluster_version or "").strip() or None
            artifact_snapshot = str(clustering_artifact.get("corpus_snapshot_version") or "").strip()
            artifact_embedding = str(clustering_artifact.get("embedding_version") or "").strip()
            if artifact_snapshot != run.corpus_snapshot_version:
                failures.append("clustering_artifact_snapshot_mismatch")
            if artifact_embedding != run.embedding_version:
                failures.append("clustering_artifact_embedding_mismatch")
            if cluster_version is None:
                failures.append("cluster_version_missing")
            else:
                cluster_row = conn.execute(
                    """
                    SELECT
                        cr.status,
                        cr.corpus_snapshot_version,
                        cr.embedding_version,
                        COUNT(c.work_id) FILTER (WHERE wssm.work_id IS NOT NULL)
                            AS member_assignment_count,
                        COUNT(c.work_id) FILTER (WHERE wssm.work_id IS NULL)
                            AS out_of_membership_count
                    FROM clustering_runs cr
                    LEFT JOIN clusters c ON c.cluster_version = cr.cluster_version
                    LEFT JOIN work_source_snapshot_memberships wssm
                      ON wssm.work_id = c.work_id
                     AND wssm.source_snapshot_version = %s
                     AND wssm.inclusion_status = 'included'
                    WHERE cr.cluster_version = %s
                    GROUP BY cr.status, cr.corpus_snapshot_version, cr.embedding_version
                    """,
                    (run.corpus_snapshot_version, cluster_version),
                ).fetchone()
                if cluster_row is None:
                    failures.append("clustering_run_missing")
                else:
                    cluster_assignment_count = int(cluster_row["member_assignment_count"] or 0)
                    missing_cluster_assignment_count = max(0, membership_count - cluster_assignment_count)
                    out_of_membership_cluster_count = int(cluster_row["out_of_membership_count"] or 0)
                    if str(cluster_row["status"]) != "succeeded":
                        failures.append("clustering_run_not_succeeded")
                    if str(cluster_row["corpus_snapshot_version"]) != run.corpus_snapshot_version:
                        failures.append("clustering_snapshot_mismatch")
                    if str(cluster_row["embedding_version"]) != run.embedding_version:
                        failures.append("clustering_embedding_mismatch")
                    if missing_cluster_assignment_count:
                        failures.append("cluster_assignment_coverage_incomplete")
                    if out_of_membership_cluster_count:
                        failures.append("cluster_assignments_outside_snapshot_membership")

    unique_failures = tuple(dict.fromkeys(failures))
    return PublicReleaseDiagnostics(
        serveable=not unique_failures,
        membership_count=membership_count,
        embedding_count=embedding_count,
        missing_embedding_count=missing_embedding_count,
        family_score_counts=family_score_counts,
        expected_family_score_counts=expected_family_score_counts,
        out_of_membership_score_count=out_of_membership_score_count,
        cluster_version=cluster_version,
        cluster_assignment_count=cluster_assignment_count,
        missing_cluster_assignment_count=missing_cluster_assignment_count,
        out_of_membership_cluster_count=out_of_membership_cluster_count,
        failures=unique_failures,
    )


__all__ = [
    "EXPECTED_FAMILIES",
    "NON_SERVING_EMBEDDING_VERSIONS",
    "PUBLIC_RELEASE_SCORER_KIND",
    "MaterializedRunContext",
    "PublicReleaseDiagnostics",
    "PublicReleasePromotion",
    "fetch_latest_public_release_promotion",
    "fetch_succeeded_materialized_run",
    "inspect_public_release_serveability",
    "load_latest_public_release_promotion",
]

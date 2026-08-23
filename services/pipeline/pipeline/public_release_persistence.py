"""Persistence operations for the append-only public release pointer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Sequence

import psycopg


# Stable, repository-specific transaction lock key (ASCII ``RRPUBLIC``).
PUBLIC_RELEASE_ADVISORY_LOCK_ID = 0x52525055424C4943


@dataclass(frozen=True)
class RankingRunForPromotion:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    status: str
    finished_at: datetime | None
    config_json: Any
    counts_json: Any
    error_message: str | None


@dataclass(frozen=True)
class PublicReleasePromotionRow:
    promotion_id: int
    ranking_run_id: str
    promoted_at: datetime
    promoted_by: str
    note: str | None


@dataclass(frozen=True)
class ScoreCoverage:
    emerging_count: int
    bridge_count: int
    undercited_count: int
    total_count: int
    outside_membership_count: int

    @property
    def rows_by_family(self) -> dict[str, int]:
        return {
            "emerging": self.emerging_count,
            "bridge": self.bridge_count,
            "undercited": self.undercited_count,
        }


def acquire_public_release_advisory_lock(conn: psycopg.Connection) -> None:
    """Serialize validation and promotion for the one public serving pointer."""
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLIC_RELEASE_ADVISORY_LOCK_ID,))


def _row_value(row: Mapping[str, Any] | Sequence[Any], key: str, index: int) -> Any:
    return row.get(key) if isinstance(row, Mapping) else row[index]


def _ranking_run_from_row(
    row: Mapping[str, Any] | Sequence[Any],
) -> RankingRunForPromotion:
    return RankingRunForPromotion(
        ranking_run_id=str(_row_value(row, "ranking_run_id", 0)),
        ranking_version=str(_row_value(row, "ranking_version", 1)),
        corpus_snapshot_version=str(_row_value(row, "corpus_snapshot_version", 2)),
        embedding_version=str(_row_value(row, "embedding_version", 3)),
        status=str(_row_value(row, "status", 4)),
        finished_at=_row_value(row, "finished_at", 5),
        config_json=_row_value(row, "config_json", 6),
        counts_json=_row_value(row, "counts_json", 7),
        error_message=(
            str(_row_value(row, "error_message", 8))
            if _row_value(row, "error_message", 8) is not None
            else None
        ),
    )


def fetch_ranking_run_for_promotion(
    conn: psycopg.Connection, *, ranking_run_id: str
) -> RankingRunForPromotion | None:
    row = conn.execute(
        """
        SELECT
            ranking_run_id,
            ranking_version,
            corpus_snapshot_version,
            embedding_version,
            status,
            finished_at,
            config_json,
            counts_json,
            error_message
        FROM ranking_runs
        WHERE ranking_run_id = %s
        FOR SHARE
        """,
        (ranking_run_id,),
    ).fetchone()
    return None if row is None else _ranking_run_from_row(row)


def _promotion_from_row(
    row: Mapping[str, Any] | Sequence[Any],
) -> PublicReleasePromotionRow:
    return PublicReleasePromotionRow(
        promotion_id=int(_row_value(row, "promotion_id", 0)),
        ranking_run_id=str(_row_value(row, "ranking_run_id", 1)),
        promoted_at=_row_value(row, "promoted_at", 2),
        promoted_by=str(_row_value(row, "promoted_by", 3)),
        note=(
            str(_row_value(row, "note", 4))
            if _row_value(row, "note", 4) is not None
            else None
        ),
    )


def fetch_active_public_release_promotion(
    conn: psycopg.Connection,
) -> PublicReleasePromotionRow | None:
    row = conn.execute(
        """
        SELECT promotion_id, ranking_run_id, promoted_at, promoted_by, note
        FROM public_release_promotions
        ORDER BY promotion_id DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else _promotion_from_row(row)


def fetch_score_coverage(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    corpus_snapshot_version: str,
) -> ScoreCoverage:
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE ps.recommendation_family = 'emerging'
                  AND wssm.work_id IS NOT NULL
            ) AS emerging_count,
            COUNT(*) FILTER (
                WHERE ps.recommendation_family = 'bridge'
                  AND wssm.work_id IS NOT NULL
            ) AS bridge_count,
            COUNT(*) FILTER (
                WHERE ps.recommendation_family = 'undercited'
                  AND wssm.work_id IS NOT NULL
            ) AS undercited_count,
            COUNT(*) AS total_count,
            COUNT(*) FILTER (WHERE wssm.work_id IS NULL) AS outside_membership_count
        FROM paper_scores ps
        LEFT JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = ps.work_id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        WHERE ps.ranking_run_id = %s
        """,
        (corpus_snapshot_version, ranking_run_id),
    ).fetchone()
    if row is None:
        return ScoreCoverage(0, 0, 0, 0, 0)
    return ScoreCoverage(
        emerging_count=int(_row_value(row, "emerging_count", 0) or 0),
        bridge_count=int(_row_value(row, "bridge_count", 1) or 0),
        undercited_count=int(_row_value(row, "undercited_count", 2) or 0),
        total_count=int(_row_value(row, "total_count", 3) or 0),
        outside_membership_count=int(
            _row_value(row, "outside_membership_count", 4) or 0
        ),
    )


def count_cluster_assignments_outside_membership(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    cluster_version: str,
) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM clusters c
        LEFT JOIN work_source_snapshot_memberships wssm
          ON wssm.work_id = c.work_id
         AND wssm.source_snapshot_version = %s
         AND wssm.inclusion_status = 'included'
        WHERE c.cluster_version = %s
          AND wssm.work_id IS NULL
        """,
        (corpus_snapshot_version, cluster_version),
    ).fetchone()
    return int(_row_value(row, "count", 0) or 0) if row is not None else 0


def append_public_release_promotion(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    promoted_by: str,
    note: str | None = None,
) -> PublicReleasePromotionRow:
    row = conn.execute(
        """
        INSERT INTO public_release_promotions (ranking_run_id, promoted_by, note)
        VALUES (%s, %s, %s)
        RETURNING promotion_id, ranking_run_id, promoted_at, promoted_by, note
        """,
        (ranking_run_id, promoted_by, note),
    ).fetchone()
    if row is None:  # pragma: no cover - PostgreSQL INSERT ... RETURNING always returns a row.
        raise RuntimeError("public release promotion insert returned no row")
    return _promotion_from_row(row)


__all__ = [
    "PUBLIC_RELEASE_ADVISORY_LOCK_ID",
    "PublicReleasePromotionRow",
    "RankingRunForPromotion",
    "ScoreCoverage",
    "acquire_public_release_advisory_lock",
    "append_public_release_promotion",
    "count_cluster_assignments_outside_membership",
    "fetch_active_public_release_promotion",
    "fetch_ranking_run_for_promotion",
    "fetch_score_coverage",
]

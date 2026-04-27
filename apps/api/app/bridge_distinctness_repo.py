"""Read-only bridge distinctness report for one pinned succeeded ranking_run_id (no latest fallback)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row

SuggestedNextStep = Literal[
    "inspect_cluster_quality_first",
    "eligible_filter_not_distinct_enough",
    "candidate_for_small_weight_experiment",
    "insufficient_bridge_signal_coverage",
]


def cluster_version_from_config(config: dict[str, Any]) -> str | None:
    art = config.get("clustering_artifact")
    if not isinstance(art, dict):
        return None
    cv = art.get("cluster_version")
    return str(cv) if isinstance(cv, str) and cv.strip() else None


def overlap_count_and_jaccard(ids_a: list[str], ids_b: list[str]) -> tuple[int, float]:
    """Set overlap and Jaccard on paper ids; deterministic for fixed inputs."""
    set_a = set(ids_a)
    set_b = set(ids_b)
    inter = set_a & set_b
    union = set_a | set_b
    overlap = len(inter)
    if not union:
        jaccard = 1.0
    else:
        jaccard = round(len(inter) / len(union), 6)
    return overlap, float(jaccard)


def compute_decision_support(
    *,
    full_bridge_top_k_ids: list[str],
    eligible_bridge_top_k_ids: list[str],
    emerging_top_k_ids: list[str],
    cluster_version: str | None,
    bridge_family_row_count: int,
    bridge_signal_json_present_count: int,
    full_vs_eligible_jaccard: float,
) -> tuple[bool, bool, SuggestedNextStep]:
    """
    Conservative, non-validating hints only. Does not assert bridge quality or product readiness.
    """
    eligible_head_differs_from_full = full_bridge_top_k_ids != eligible_bridge_top_k_ids
    o_fe = overlap_count_and_jaccard(eligible_bridge_top_k_ids, emerging_top_k_ids)
    o_fem = overlap_count_and_jaccard(full_bridge_top_k_ids, emerging_top_k_ids)
    eligible_head_less_emerging_like_than_full = o_fe[1] < o_fem[1]

    insufficient = bridge_family_row_count == 0 or (
        bridge_family_row_count > 0 and bridge_signal_json_present_count == 0
    )

    if insufficient:
        return (
            eligible_head_differs_from_full,
            eligible_head_less_emerging_like_than_full,
            "insufficient_bridge_signal_coverage",
        )
    if not eligible_head_differs_from_full or full_vs_eligible_jaccard >= 1.0:
        return (
            eligible_head_differs_from_full,
            eligible_head_less_emerging_like_than_full,
            "eligible_filter_not_distinct_enough",
        )
    if cluster_version is None:
        return (
            eligible_head_differs_from_full,
            eligible_head_less_emerging_like_than_full,
            "inspect_cluster_quality_first",
        )
    if eligible_head_less_emerging_like_than_full:
        return (
            eligible_head_differs_from_full,
            eligible_head_less_emerging_like_than_full,
            "candidate_for_small_weight_experiment",
        )
    return (
        eligible_head_differs_from_full,
        eligible_head_less_emerging_like_than_full,
        "inspect_cluster_quality_first",
    )


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _top_family_ids(
    conn: psycopg.Connection, *, ranking_run_id: str, family: str, k: int, bridge_eligible_true_only: bool
) -> list[str]:
    if bridge_eligible_true_only and family != "bridge":
        raise ValueError(
            "bridge_eligible_true_only applies only to recommendation_family='bridge'; "
            f"got {family!r}"
        )
    elig = "          AND ps.bridge_eligible IS TRUE\n" if bridge_eligible_true_only else ""
    sql = f"""
        SELECT w.openalex_id
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = %s
{elig}        ORDER BY ps.final_score DESC, ps.work_id ASC
        LIMIT %s
    """
    rows = conn.execute(sql, (ranking_run_id, family, k)).fetchall()
    return [str(r["openalex_id"]) for r in rows]


def _bridge_coverage_row(conn: psycopg.Connection, *, ranking_run_id: str) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*)::bigint AS bridge_family_row_count,
            COUNT(*) FILTER (WHERE ps.bridge_score IS NOT NULL)::bigint AS bridge_score_nonnull_count,
            COUNT(*) FILTER (WHERE ps.bridge_score IS NULL)::bigint AS bridge_score_null_count,
            COUNT(*) FILTER (WHERE ps.bridge_eligible IS TRUE)::bigint AS bridge_eligible_true_count,
            COUNT(*) FILTER (WHERE ps.bridge_eligible IS FALSE)::bigint AS bridge_eligible_false_count,
            COUNT(*) FILTER (WHERE ps.bridge_eligible IS NULL)::bigint AS bridge_eligible_null_count,
            COUNT(*) FILTER (WHERE ps.bridge_signal_json IS NOT NULL)::bigint
                AS bridge_signal_json_present_count,
            COUNT(*) FILTER (WHERE ps.bridge_signal_json IS NULL)::bigint
                AS bridge_signal_json_missing_count
        FROM paper_scores ps
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = 'bridge'
        """,
        (ranking_run_id,),
    ).fetchone()
    assert row is not None
    return dict(row)


@dataclass(frozen=True)
class BridgeDistinctnessPayload:
    ranking_run_id: str
    ranking_version: str
    corpus_snapshot_version: str
    embedding_version: str
    cluster_version: str | None
    k: int
    full_bridge_top_k_ids: list[str]
    eligible_bridge_top_k_ids: list[str]
    emerging_top_k_ids: list[str]
    full_bridge_vs_eligible_bridge_overlap_count: int
    full_bridge_vs_eligible_bridge_jaccard: float
    full_bridge_vs_emerging_overlap_count: int
    full_bridge_vs_emerging_jaccard: float
    eligible_bridge_vs_emerging_overlap_count: int
    eligible_bridge_vs_emerging_jaccard: float
    bridge_family_row_count: int
    bridge_score_nonnull_count: int
    bridge_score_null_count: int
    bridge_eligible_true_count: int
    bridge_eligible_false_count: int
    bridge_eligible_null_count: int
    bridge_signal_json_present_count: int
    bridge_signal_json_missing_count: int
    eligible_head_differs_from_full: bool
    eligible_head_less_emerging_like_than_full: bool
    suggested_next_step: SuggestedNextStep


def load_bridge_distinctness_report(
    *,
    database_url: str,
    ranking_run_id: str,
    k: int,
) -> BridgeDistinctnessPayload | None:
    """
    Load heads and coverage for one explicit succeeded run. Never resolves latest or ranking_version.
    """
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        run = conn.execute(
            """
            SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version,
                   config_json, status
            FROM ranking_runs
            WHERE ranking_run_id = %s
            """,
            (ranking_run_id,),
        ).fetchone()
        if run is None or str(run["status"]) != "succeeded":
            return None

        cfg = _parse_config_json(run.get("config_json"))
        cluster_ver = cluster_version_from_config(cfg)

        full_ids = _top_family_ids(
            conn, ranking_run_id=str(run["ranking_run_id"]), family="bridge", k=k, bridge_eligible_true_only=False
        )
        eligible_ids = _top_family_ids(
            conn, ranking_run_id=str(run["ranking_run_id"]), family="bridge", k=k, bridge_eligible_true_only=True
        )
        emerging_ids = _top_family_ids(
            conn, ranking_run_id=str(run["ranking_run_id"]), family="emerging", k=k, bridge_eligible_true_only=False
        )

        cov = _bridge_coverage_row(conn, ranking_run_id=str(run["ranking_run_id"]))

        o_fe = overlap_count_and_jaccard(full_ids, eligible_ids)
        o_fem = overlap_count_and_jaccard(full_ids, emerging_ids)
        o_eem = overlap_count_and_jaccard(eligible_ids, emerging_ids)

        ediff, eless, step = compute_decision_support(
            full_bridge_top_k_ids=full_ids,
            eligible_bridge_top_k_ids=eligible_ids,
            emerging_top_k_ids=emerging_ids,
            cluster_version=cluster_ver,
            bridge_family_row_count=int(cov["bridge_family_row_count"]),
            bridge_signal_json_present_count=int(cov["bridge_signal_json_present_count"]),
            full_vs_eligible_jaccard=o_fe[1],
        )

        return BridgeDistinctnessPayload(
            ranking_run_id=str(run["ranking_run_id"]),
            ranking_version=str(run["ranking_version"]),
            corpus_snapshot_version=str(run["corpus_snapshot_version"]),
            embedding_version=str(run["embedding_version"]),
            cluster_version=cluster_ver,
            k=k,
            full_bridge_top_k_ids=full_ids,
            eligible_bridge_top_k_ids=eligible_ids,
            emerging_top_k_ids=emerging_ids,
            full_bridge_vs_eligible_bridge_overlap_count=o_fe[0],
            full_bridge_vs_eligible_bridge_jaccard=o_fe[1],
            full_bridge_vs_emerging_overlap_count=o_fem[0],
            full_bridge_vs_emerging_jaccard=o_fem[1],
            eligible_bridge_vs_emerging_overlap_count=o_eem[0],
            eligible_bridge_vs_emerging_jaccard=o_eem[1],
            bridge_family_row_count=int(cov["bridge_family_row_count"]),
            bridge_score_nonnull_count=int(cov["bridge_score_nonnull_count"]),
            bridge_score_null_count=int(cov["bridge_score_null_count"]),
            bridge_eligible_true_count=int(cov["bridge_eligible_true_count"]),
            bridge_eligible_false_count=int(cov["bridge_eligible_false_count"]),
            bridge_eligible_null_count=int(cov["bridge_eligible_null_count"]),
            bridge_signal_json_present_count=int(cov["bridge_signal_json_present_count"]),
            bridge_signal_json_missing_count=int(cov["bridge_signal_json_missing_count"]),
            eligible_head_differs_from_full=ediff,
            eligible_head_less_emerging_like_than_full=eless,
            suggested_next_step=step,
        )

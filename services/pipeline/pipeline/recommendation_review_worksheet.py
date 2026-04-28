"""Generate a CSV worksheet for human judgment of materialized recommendations (one pinned run)."""

from __future__ import annotations

import csv
import json
import io
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row

from pipeline.bootstrap_loader import database_url_from_env

VALID_FAMILIES: frozenset[str] = frozenset({"emerging", "bridge", "undercited"})

# CSV representation for bridge_eligible: see docs/recommendation-review-rubric.md
TRUE = "true"
FALSE = "false"
# SQL NULL => empty field in CSV (not the literal "null")
NULL_ELIGIBLE = ""


def cluster_version_from_config(config: dict[str, Any]) -> str | None:
    art = config.get("clustering_artifact")
    if not isinstance(art, dict):
        return None
    cv = art.get("cluster_version")
    return str(cv) if isinstance(cv, str) and cv.strip() else None


def _parse_config_json(raw: Any) -> dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    if isinstance(raw, dict):
        return dict(raw)
    return {}


def format_bridge_eligible_for_csv(value: bool | None) -> str:
    if value is True:
        return TRUE
    if value is False:
        return FALSE
    return NULL_ELIGIBLE


def _topic_names_from_json(topics_json: Any) -> list[str]:
    if topics_json is None:
        return []
    if isinstance(topics_json, str):
        try:
            topics_json = json.loads(topics_json)
        except json.JSONDecodeError:
            return []
    if not isinstance(topics_json, list):
        return []
    out: list[str] = []
    for t in topics_json:
        if isinstance(t, str):
            out.append(t)
    return out


def _fmt_float(v: float | None) -> str:
    if v is None:
        return ""
    return f"{v:.9g}"


def _assert_succeeded_run(
    conn: psycopg.Connection, *, ranking_run_id: str
) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT ranking_run_id, ranking_version, corpus_snapshot_version, embedding_version, config_json, status
        FROM ranking_runs
        WHERE ranking_run_id = %s
        """,
        (ranking_run_id,),
    ).fetchone()
    if row is None:
        msg = f"ranking_run_id not found: {ranking_run_id!r}"
        raise WorksheetError(msg, code=2)
    if str(row["status"]) != "succeeded":
        msg = (
            f"ranking run {ranking_run_id!r} is not succeeded (status={row['status']!r}). "
            "Only succeeded runs can generate review worksheets."
        )
        raise WorksheetError(msg, code=2)
    return dict(row)


def _fetch_scored_rows(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    family: str,
    limit: int,
    bridge_eligible_only: bool,
) -> list[dict[str, Any]]:
    # Raw per-row signal JSON is not selected (not exported in this worksheet)
    eligible_clause = ""
    if bridge_eligible_only:
        eligible_clause = "AND ps.bridge_eligible IS TRUE"
    query = f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY ps.final_score DESC, ps.work_id ASC) AS rank,
            w.openalex_id AS paper_id,
            w.title,
            w.year,
            w.citation_count,
            w.source_slug,
            COALESCE(topic_agg.topics, '[]'::json) AS topics,
            ps.final_score,
            ps.reason_short,
            ps.semantic_score,
            ps.citation_velocity_score,
            ps.topic_growth_score,
            ps.bridge_score,
            ps.diversity_penalty,
            ps.bridge_eligible
        FROM paper_scores ps
        JOIN works w ON w.id = ps.work_id
        LEFT JOIN LATERAL (
            SELECT json_agg(sub.topic_name ORDER BY sub.score DESC, sub.topic_name ASC) AS topics
            FROM (
                SELECT t.name AS topic_name, wt.score AS score
                FROM work_topics wt
                JOIN topics t ON t.id = wt.topic_id
                WHERE wt.work_id = w.id
                ORDER BY wt.score DESC, t.name ASC
                LIMIT 3
            ) sub
        ) topic_agg ON TRUE
        WHERE ps.ranking_run_id = %s
          AND ps.recommendation_family = %s
          {eligible_clause}
        ORDER BY ps.final_score DESC, ps.work_id ASC
        LIMIT %s
    """
    return list(conn.execute(query, (ranking_run_id, family, limit)).fetchall())


WORKSHEET_COLUMNS: tuple[str, ...] = (
    "ranking_run_id",
    "ranking_version",
    "corpus_snapshot_version",
    "embedding_version",
    "cluster_version",
    "review_pool_variant",
    "family",
    "rank",
    "paper_id",
    "title",
    "year",
    "citation_count",
    "source_slug",
    "topics",
    "final_score",
    "reason_short",
    "semantic_score",
    "citation_velocity_score",
    "topic_growth_score",
    "bridge_score",
    "diversity_penalty",
    "bridge_eligible",
    "relevance_label",
    "novelty_label",
    "bridge_like_label",
    "reviewer_notes",
)


class WorksheetError(Exception):
    def __init__(self, message: str, *, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


def build_worksheet_rows(
    conn: psycopg.Connection,
    *,
    ranking_run_id: str,
    family: str,
    limit: int,
    bridge_eligible_only: bool = False,
) -> list[dict[str, str]]:
    """
    Provenance is repeated on every data row. Reviewer columns are empty strings.
    """
    if not ranking_run_id or not str(ranking_run_id).strip():
        raise WorksheetError("--ranking-run-id is required and must not be blank", code=2)
    rid = str(ranking_run_id).strip()
    if family not in VALID_FAMILIES:
        raise WorksheetError(
            f"Invalid --family {family!r}. Expected one of: {', '.join(sorted(VALID_FAMILIES))}.",
            code=2,
        )
    if limit < 1 or limit > 200:
        raise WorksheetError("--limit must be between 1 and 200", code=2)
    if bridge_eligible_only and family != "bridge":
        raise WorksheetError("--bridge-eligible-only is only valid with --family bridge", code=2)

    run = _assert_succeeded_run(conn, ranking_run_id=rid)
    cfg = _parse_config_json(run.get("config_json"))
    cluster_ver = cluster_version_from_config(cfg) or ""

    review_pool_variant = "bridge_eligible_only" if bridge_eligible_only else "full_family_top_k"
    prov = {
        "ranking_run_id": str(run["ranking_run_id"]),
        "ranking_version": str(run["ranking_version"]),
        "corpus_snapshot_version": str(run["corpus_snapshot_version"]),
        "embedding_version": str(run["embedding_version"]),
        "cluster_version": cluster_ver,
        "review_pool_variant": review_pool_variant,
        "family": family,
    }

    raw_rows = _fetch_scored_rows(
        conn,
        ranking_run_id=rid,
        family=family,
        limit=limit,
        bridge_eligible_only=bridge_eligible_only,
    )
    out: list[dict[str, str]] = []
    for row in raw_rows:
        topics_list = _topic_names_from_json(row.get("topics"))
        topics_str = ";".join(topics_list) if topics_list else ""
        be = row.get("bridge_eligible")
        be_out = format_bridge_eligible_for_csv(None if be is None else bool(be))
        out.append(
            {
                **prov,
                "rank": str(int(row["rank"])),
                "paper_id": str(row["paper_id"]),
                "title": str(row["title"] or ""),
                "year": str(int(row["year"]) if row["year"] is not None else ""),
                "citation_count": str(int(row["citation_count"] or 0)),
                "source_slug": str(row["source_slug"]) if row["source_slug"] is not None else "",
                "topics": topics_str,
                "final_score": _fmt_float(float(row["final_score"]) if row["final_score"] is not None else None),
                "reason_short": str(row["reason_short"] or ""),
                "semantic_score": _fmt_float(
                    float(row["semantic_score"]) if row["semantic_score"] is not None else None
                ),
                "citation_velocity_score": _fmt_float(
                    float(row["citation_velocity_score"])
                    if row["citation_velocity_score"] is not None
                    else None
                ),
                "topic_growth_score": _fmt_float(
                    float(row["topic_growth_score"]) if row["topic_growth_score"] is not None else None
                ),
                "bridge_score": _fmt_float(
                    float(row["bridge_score"]) if row["bridge_score"] is not None else None
                ),
                "diversity_penalty": _fmt_float(
                    float(row["diversity_penalty"]) if row["diversity_penalty"] is not None else None
                ),
                "bridge_eligible": be_out,
                "relevance_label": "",
                "novelty_label": "",
                "bridge_like_label": "",
                "reviewer_notes": "",
            }
        )
    return out


def render_worksheet_csv(rows: Sequence[dict[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=WORKSHEET_COLUMNS,
        lineterminator="\n",
        quoting=csv.QUOTE_MINIMAL,
    )
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in WORKSHEET_COLUMNS})
    return buf.getvalue()


def write_recommendation_review_worksheet(
    *,
    output_path: Path,
    database_url: str | None,
    ranking_run_id: str,
    family: str,
    limit: int,
    bridge_eligible_only: bool = False,
) -> None:
    dsn = database_url or database_url_from_env()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        data_rows = build_worksheet_rows(
            conn,
            ranking_run_id=ranking_run_id,
            family=family,
            limit=limit,
            bridge_eligible_only=bridge_eligible_only,
        )
    text = render_worksheet_csv(data_rows)
    output_path.write_text(text, encoding="utf-8", newline="")


__all__ = [
    "VALID_FAMILIES",
    "WORKSHEET_COLUMNS",
    "WorksheetError",
    "build_worksheet_rows",
    "cluster_version_from_config",
    "format_bridge_eligible_for_csv",
    "render_worksheet_csv",
    "write_recommendation_review_worksheet",
]

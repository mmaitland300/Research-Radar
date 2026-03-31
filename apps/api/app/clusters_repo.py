from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.papers_repo import database_url_from_env


@dataclass(frozen=True)
class ClusterSamplePaper:
    paper_id: str
    title: str


@dataclass(frozen=True)
class ClusterGroupRow:
    cluster_id: str
    work_count: int
    sample_papers: list[ClusterSamplePaper]


@dataclass(frozen=True)
class ClusterInspectionPayload:
    cluster_version: str
    embedding_version: str
    corpus_snapshot_version: str
    algorithm: str
    status: str
    config_json: dict[str, Any]
    groups: list[ClusterGroupRow]


def load_cluster_inspection(
    *,
    cluster_version: str,
    sample_per_cluster: int = 5,
    database_url: str | None = None,
) -> ClusterInspectionPayload | None:
    if sample_per_cluster < 1:
        raise ValueError("sample_per_cluster must be at least 1.")
    dsn = database_url or database_url_from_env()
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        run = conn.execute(
            """
            SELECT embedding_version, corpus_snapshot_version, algorithm, status, config_json
            FROM clustering_runs
            WHERE cluster_version = %s
            """,
            (cluster_version,),
        ).fetchone()
        if run is None:
            return None

        size_rows = conn.execute(
            """
            SELECT cluster_id, COUNT(*)::bigint AS work_count
            FROM clusters
            WHERE cluster_version = %s
            GROUP BY cluster_id
            ORDER BY cluster_id
            """,
            (cluster_version,),
        ).fetchall()

        sample_rows = conn.execute(
            """
            SELECT cluster_id, paper_id, title
            FROM (
                SELECT
                    c.cluster_id,
                    w.openalex_id AS paper_id,
                    w.title,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.cluster_id ORDER BY w.id
                    ) AS rn
                FROM clusters c
                JOIN works w ON w.id = c.work_id
                WHERE c.cluster_version = %s
            ) sub
            WHERE rn <= %s
            ORDER BY cluster_id, rn
            """,
            (cluster_version, sample_per_cluster),
        ).fetchall()

    sizes: dict[str, int] = {str(r["cluster_id"]): int(r["work_count"] or 0) for r in size_rows}
    samples_by_cluster: dict[str, list[ClusterSamplePaper]] = {}
    for row in sample_rows:
        cid = str(row["cluster_id"])
        samples_by_cluster.setdefault(cid, []).append(
            ClusterSamplePaper(paper_id=str(row["paper_id"]), title=str(row["title"]))
        )

    groups: list[ClusterGroupRow] = []
    for cid in sorted(sizes.keys()):
        groups.append(
            ClusterGroupRow(
                cluster_id=cid,
                work_count=sizes[cid],
                sample_papers=samples_by_cluster.get(cid, []),
            )
        )

    cfg = run["config_json"]
    if not isinstance(cfg, dict):
        cfg = {}

    return ClusterInspectionPayload(
        cluster_version=cluster_version,
        embedding_version=str(run["embedding_version"]),
        corpus_snapshot_version=str(run["corpus_snapshot_version"]),
        algorithm=str(run["algorithm"]),
        status=str(run["status"]),
        config_json=cfg,
        groups=groups,
    )

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

import psycopg

from pipeline.clustering import ClusterAssignment, ClusteringInput
from pipeline.config import ClusteringCounts, ClusteringRun
from pipeline.embedding_persistence import latest_corpus_snapshot_version_with_works


@dataclass(frozen=True)
class ClusterInputSummary:
    corpus_snapshot_version: str
    embedding_version: str
    total_input_works: int
    rows: list[ClusteringInput]


def _parse_vector(raw: Any) -> tuple[float, ...]:
    if isinstance(raw, str):
        parsed = json.loads(raw)
        return tuple(float(x) for x in parsed)
    if isinstance(raw, (list, tuple)):
        return tuple(float(x) for x in raw)
    raise ValueError(f"Unsupported embedding vector type: {type(raw).__name__}")


def list_clustering_inputs(
    conn: psycopg.Connection,
    *,
    embedding_version: str,
    corpus_snapshot_version: str | None = None,
) -> ClusterInputSummary:
    snapshot = corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
    if snapshot is None:
        raise RuntimeError(
            "No corpus_snapshot_version with included works found. "
            "Pass --corpus-snapshot-version or ingest data first."
        )
    rows = conn.execute(
        """
        SELECT w.id, e.vector
        FROM works w
        JOIN embeddings e
          ON e.work_id = w.id
         AND e.embedding_version = %s
        WHERE w.inclusion_status = 'included'
          AND w.corpus_snapshot_version = %s
        ORDER BY w.id ASC
        """,
        (embedding_version, snapshot),
    ).fetchall()

    parsed = [ClusteringInput(work_id=int(row[0]), vector=_parse_vector(row[1])) for row in rows]
    return ClusterInputSummary(
        corpus_snapshot_version=snapshot,
        embedding_version=embedding_version,
        total_input_works=len(parsed),
        rows=parsed,
    )


def insert_clustering_run_started(conn: psycopg.Connection, run: ClusteringRun) -> None:
    conn.execute(
        """
        INSERT INTO clustering_runs (
            cluster_version,
            embedding_version,
            corpus_snapshot_version,
            status,
            algorithm,
            started_at,
            config_json,
            notes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        ON CONFLICT (cluster_version) DO UPDATE SET
            embedding_version = EXCLUDED.embedding_version,
            corpus_snapshot_version = EXCLUDED.corpus_snapshot_version,
            status = EXCLUDED.status,
            algorithm = EXCLUDED.algorithm,
            started_at = EXCLUDED.started_at,
            finished_at = NULL,
            config_json = EXCLUDED.config_json,
            counts_json = NULL,
            error_message = NULL,
            notes = EXCLUDED.notes
        """,
        (
            run.cluster_version,
            run.embedding_version,
            run.corpus_snapshot_version,
            run.status,
            run.algorithm,
            run.started_at,
            json.dumps(run.config),
            run.notes,
        ),
    )


def replace_cluster_assignments(
    conn: psycopg.Connection,
    *,
    cluster_version: str,
    assignments: list[ClusterAssignment],
) -> None:
    # Idempotency rule: delete all rows for cluster_version, then insert fresh assignments.
    conn.execute("DELETE FROM clusters WHERE cluster_version = %s", (cluster_version,))
    for row in assignments:
        conn.execute(
            """
            INSERT INTO clusters (work_id, cluster_id, cluster_version)
            VALUES (%s, %s, %s)
            """,
            (row.work_id, row.cluster_id, cluster_version),
        )


def update_clustering_run_final(
    conn: psycopg.Connection,
    *,
    cluster_version: str,
    status: str,
    counts: ClusteringCounts | None,
    error_message: str | None,
) -> None:
    conn.execute(
        """
        UPDATE clustering_runs
        SET status = %s,
            finished_at = %s,
            counts_json = %s::jsonb,
            error_message = %s
        WHERE cluster_version = %s
        """,
        (
            status,
            datetime.now(UTC),
            (json.dumps(asdict(counts)) if counts is not None else None),
            error_message,
            cluster_version,
        ),
    )


def load_cluster_assignments(
    conn: psycopg.Connection, *, cluster_version: str
) -> dict[int, str]:
    rows = conn.execute(
        """
        SELECT work_id, cluster_id
        FROM clusters
        WHERE cluster_version = %s
        ORDER BY work_id ASC
        """,
        (cluster_version,),
    ).fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


def require_successful_clustering_run(
    conn: psycopg.Connection,
    *,
    cluster_version: str,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> None:
    row = conn.execute(
        """
        SELECT 1
        FROM clustering_runs
        WHERE cluster_version = %s
          AND corpus_snapshot_version = %s
          AND embedding_version = %s
          AND status = 'succeeded'
        """,
        (cluster_version, corpus_snapshot_version, embedding_version),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            "No succeeded clustering_runs row matches cluster_version="
            f"{cluster_version!r}, corpus_snapshot_version={corpus_snapshot_version!r}, "
            f"embedding_version={embedding_version!r}."
        )


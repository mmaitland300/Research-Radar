from __future__ import annotations

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.clustering import cluster_inputs_kmeans
from pipeline.clustering_persistence import (
    insert_clustering_run_started,
    list_clustering_inputs,
    replace_cluster_assignments,
    update_clustering_run_final,
)
from pipeline.config import ClusteringCounts, ClusteringRun

CLUSTERING_ALGORITHM = "kmeans-l2-v0"


def _build_clustering_config(
    *,
    embedding_version: str,
    corpus_snapshot_version: str,
    cluster_count: int,
    max_iterations: int,
) -> dict[str, object]:
    return {
        "algorithm": CLUSTERING_ALGORITHM,
        "identity": {
            "cluster_version": "user_supplied",
            "embedding_version": embedding_version,
            "corpus_snapshot_version": corpus_snapshot_version,
        },
        "cluster_count": cluster_count,
        "max_iterations": max_iterations,
        "idempotency": "delete_rows_for_cluster_version_then_insert_cleanly",
    }


def execute_clustering_run(
    *,
    cluster_version: str,
    embedding_version: str,
    corpus_snapshot_version: str | None = None,
    cluster_count: int = 12,
    max_iterations: int = 20,
    database_url: str | None = None,
    note: str | None = None,
) -> ClusteringRun:
    if cluster_count <= 0:
        raise ValueError("cluster_count must be positive.")
    if max_iterations <= 0:
        raise ValueError("max_iterations must be positive.")
    if not cluster_version.strip():
        raise ValueError("cluster_version must be non-empty.")
    if not embedding_version.strip():
        raise ValueError("embedding_version must be non-empty.")

    dsn = database_url or database_url_from_env()

    with psycopg.connect(dsn, autocommit=False) as conn:
        inputs = list_clustering_inputs(
            conn,
            embedding_version=embedding_version,
            corpus_snapshot_version=corpus_snapshot_version,
        )
        if inputs.total_input_works <= 0:
            raise RuntimeError(
                "No included works with embeddings found for the resolved snapshot + embedding_version."
            )
        config = _build_clustering_config(
            embedding_version=embedding_version,
            corpus_snapshot_version=inputs.corpus_snapshot_version,
            cluster_count=cluster_count,
            max_iterations=max_iterations,
        )
        run = ClusteringRun.start(
            cluster_version=cluster_version,
            embedding_version=embedding_version,
            corpus_snapshot_version=inputs.corpus_snapshot_version,
            algorithm=CLUSTERING_ALGORITHM,
            config=config,
            notes=note,
        )
        insert_clustering_run_started(conn, run)
        conn.commit()

    try:
        with psycopg.connect(dsn, autocommit=False) as conn:
            inputs = list_clustering_inputs(
                conn,
                embedding_version=embedding_version,
                corpus_snapshot_version=run.corpus_snapshot_version,
            )
            assignments = cluster_inputs_kmeans(
                inputs.rows, cluster_count=cluster_count, max_iterations=max_iterations
            )
            replace_cluster_assignments(
                conn, cluster_version=run.cluster_version, assignments=assignments
            )
            unique_clusters = len({row.cluster_id for row in assignments})
            counts = ClusteringCounts(
                total_input_works=inputs.total_input_works,
                clustered_works=len(assignments),
                cluster_count=unique_clusters,
            )
            update_clustering_run_final(
                conn,
                cluster_version=run.cluster_version,
                status="succeeded",
                counts=counts,
                error_message=None,
            )
            conn.commit()
        return run.complete(counts)
    except Exception as exc:
        with psycopg.connect(dsn, autocommit=False) as conn2:
            update_clustering_run_final(
                conn2,
                cluster_version=cluster_version,
                status="failed",
                counts=None,
                error_message=str(exc),
            )
            conn2.commit()
        raise


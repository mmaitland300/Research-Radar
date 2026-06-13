from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.clusters_repo import load_cluster_inspection
from app.contracts import (
    ClusterGroupItem,
    ClusterInspectionResponse,
    ClusterSamplePaperItem,
    utc_now,
)
from app.demo_fixtures import fixture_cluster_inspection, fixture_mode_enabled

router = APIRouter()


@router.get("/api/v1/clusters/{cluster_version}/inspect", response_model=ClusterInspectionResponse)
def get_cluster_inspection(
    cluster_version: str,
    sample_per_cluster: int = Query(default=5, ge=1, le=50),
) -> ClusterInspectionResponse:
    """
    Inspect cluster assignments for a clustering run: per-cluster size and sample paper titles.
    Clustering uses kmeans-l2 on stored vectors; similar-papers uses cosine distance (see metric_note).
    """
    if fixture_mode_enabled():
        return fixture_cluster_inspection(sample_per_cluster=sample_per_cluster)
    try:
        payload = load_cluster_inspection(
            cluster_version=cluster_version,
            sample_per_cluster=sample_per_cluster,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and clustering data exists.",
        ) from exc

    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"No clustering run found for cluster_version={cluster_version!r}.",
        )

    cfg = payload.config_json
    clustering_metric = cfg.get("clustering_metric") if isinstance(cfg, dict) else None
    metric_note = cfg.get("note") if isinstance(cfg, dict) else None

    return ClusterInspectionResponse(
        cluster_version=payload.cluster_version,
        embedding_version=payload.embedding_version,
        corpus_snapshot_version=payload.corpus_snapshot_version,
        algorithm=payload.algorithm,
        status=payload.status,
        clustering_metric=str(clustering_metric) if clustering_metric is not None else None,
        metric_note=str(metric_note) if metric_note is not None else None,
        groups=[
            ClusterGroupItem(
                cluster_id=g.cluster_id,
                work_count=g.work_count,
                sample_papers=[
                    ClusterSamplePaperItem(paper_id=p.paper_id, title=p.title) for p in g.sample_papers
                ],
            )
            for g in payload.groups
        ],
        generated_at=utc_now(),
    )

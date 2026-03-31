import logging
from typing import Literal

import psycopg
from fastapi import FastAPI, HTTPException, Query

from app.config import PRODUCT_RANKING_METADATA_NOTE, settings
from app.contracts import (
    ClusterGroupItem,
    ClusterInspectionResponse,
    ClusterSamplePaperItem,
    EvaluationCitationProxy,
    EvaluationCompareResponse,
    EvaluationDisclaimer,
    EvaluationListArmResponse,
    EvaluationPaperItem,
    EvaluationRecencyProxy,
    EvaluationSummary,
    EvaluationTopicMixProxy,
    EvaluationTopicOverlap,
    HealthResponse,
    MaterializedRankingMeta,
    PaperDetail,
    PaperListItem,
    PaperListResponse,
    ProductSummary,
    RankedRecommendationItem,
    RankedRecommendationsResponse,
    RankedSignals,
    RankingFamily,
    ReadinessResponse,
    SimilarPaperItem,
    SimilarPapersResponse,
    TopicTrendItem,
    TopicTrendsResponse,
    UndercitedRecommendationItem,
    UndercitedRecommendationsResponse,
    utc_now,
)
from app.clusters_repo import load_cluster_inspection
from app.evaluation_repo import EvalListArm, load_evaluation_compare
from app.papers_repo import database_url_from_env
from app.papers_repo import get_paper_detail as get_paper_detail_row
from app.papers_repo import list_papers
from app.papers_repo import list_undercited_heuristic_v0
from app.scores_repo import fetch_latest_materialized_ranking_for_meta, list_ranked_recommendations
from app.similarity_repo import list_similar_papers
from app.trends_repo import list_topic_trends

logger = logging.getLogger(__name__)

EVALUATION_V0_DISCLAIMER = EvaluationDisclaimer(
    headline="These outputs are comparison aids for engineering and transparency, not human relevance judgments.",
    bullets=[
        "Side-by-side lists share the same candidate pool for the selected recommendation family and corpus snapshot.",
        "Recency, citation, and topic summaries are coarse proxies over the short lists shown — they do not measure usefulness to a researcher.",
        "Topic overlap uses Jaccard similarity on topic labels attached to papers in this corpus, not semantic similarity of full text.",
        "Use ranked outputs for product behavior; use this endpoint to sanity-check drift against naive orderings.",
    ],
)

TOPIC_OVERLAP_NOTE = (
    "Jaccard index on the set of OpenAlex topic labels appearing in the top tags of each paper in the list. "
    "High overlap means similar topic mix, not similar intellectual content."
)


def _evaluation_arm_response(arm: EvalListArm) -> EvaluationListArmResponse:
    return EvaluationListArmResponse(
        arm_label=arm.arm_label,
        arm_description=arm.arm_description,
        ordering_description=arm.ordering_description,
        items=[
            EvaluationPaperItem(
                paper_id=i.paper_id,
                title=i.title,
                year=i.year,
                citation_count=i.citation_count,
                source_slug=i.source_slug,
                topics=list(i.topics),
                final_score=i.final_score,
            )
            for i in arm.items
        ],
        recency=EvaluationRecencyProxy(
            mean_year=arm.recency.mean_year,
            min_year=arm.recency.min_year,
            max_year=arm.recency.max_year,
            share_in_latest_two_years=arm.recency.share_in_latest_two_years,
        ),
        citations=EvaluationCitationProxy(
            mean=arm.citations.mean,
            median=arm.citations.median,
            min_val=arm.citations.min_val,
            max_val=arm.citations.max_val,
        ),
        topics=EvaluationTopicMixProxy(
            unique_topic_labels=arm.topics.unique_topic_labels,
            top_topics=list(arm.topics.top_topics),
        ),
    )

app = FastAPI(
    title="Research Radar API",
    version="0.1.0",
    description="API surface for ranking, explainability, and evaluation in the Research Radar project.",
)


@app.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", timestamp=utc_now())


@app.get("/readyz", response_model=ReadinessResponse)
def readiness() -> ReadinessResponse:
    try:
        with psycopg.connect(database_url_from_env()) as conn:
            conn.execute("SELECT 1").fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database unreachable.") from exc
    return ReadinessResponse(status="ok", database="connected", timestamp=utc_now())


@app.get("/api/v1/meta/product", response_model=ProductSummary)
def get_product_summary() -> ProductSummary:
    materialized = None
    try:
        row = fetch_latest_materialized_ranking_for_meta()
        if row is not None:
            materialized = MaterializedRankingMeta(
                ranking_run_id=row.ranking_run_id,
                ranking_version=row.ranking_version,
                corpus_snapshot_version=row.corpus_snapshot_version,
                embedding_version=row.embedding_version,
                config_json=row.config_json,
            )
    except Exception:
        logger.exception("Failed to load materialized ranking metadata for product summary")
        materialized = None

    return ProductSummary(
        name=settings.name,
        thesis=settings.thesis,
        core_slice=list(settings.core_slice),
        edge_slice=list(settings.edge_slice),
        pages=list(settings.v1_pages),
        evaluation_checks=list(settings.evaluation_checks),
        ranking_weights={
            "semantic": settings.weights.semantic,
            "citation_velocity": settings.weights.citation_velocity,
            "topic_growth": settings.weights.topic_growth,
            "bridge": settings.weights.bridge,
            "diversity_penalty": settings.weights.diversity_penalty,
        },
        ranking_metadata_note=PRODUCT_RANKING_METADATA_NOTE,
        materialized_ranking=materialized,
    )


@app.get(
    "/api/v1/recommendations/undercited",
    response_model=UndercitedRecommendationsResponse,
)
def get_recommendations_undercited(
    limit: int = Query(default=15, ge=1, le=100),
    min_year: int = Query(default=2019, ge=1990, le=2100),
    max_citations: int = Query(default=30, ge=0, le=10_000),
) -> UndercitedRecommendationsResponse:
    """
    Heuristic v0 baseline: frozen low-cite candidate pool (docs/candidate-pool-low-cite.md v0) —
    included core papers, recency and citation ceiling, non-empty title and abstract.
    Global query (not corpus-snapshot scoped). For snapshot-scoped comparisons, use
    GET /api/v1/evaluation/compare?family=undercited. Not a trained ranking model.
    """
    try:
        rows = list_undercited_heuristic_v0(
            limit=limit,
            min_year=min_year,
            max_citations=max_citations,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and seeded.",
        ) from exc

    return UndercitedRecommendationsResponse(
        heuristic_label="undercited-core-recent-v0",
        heuristic_version="v0",
        description=(
            "Frozen low-cite candidate pool (docs/candidate-pool-low-cite.md v0): included core papers, "
            "recency and citation ceiling, non-empty title and abstract. Global listing (not snapshot-scoped). "
            "Order: year DESC, citation_count ASC, openalex_id ASC."
        ),
        total=len(rows),
        items=[
            UndercitedRecommendationItem(
                paper_id=r.paper_id,
                title=r.title,
                year=r.year,
                citation_count=r.citation_count,
                source_slug=r.source_slug,
                reason=r.reason,
                signal_breakdown=r.signal_breakdown,
            )
            for r in rows
        ],
    )


@app.get(
    "/api/v1/recommendations/ranked",
    response_model=RankedRecommendationsResponse,
)
def get_recommendations_ranked(
    family: Literal["emerging", "bridge", "undercited"] = Query(...),
    limit: int = Query(default=20, ge=1, le=100),
    corpus_snapshot_version: str | None = Query(default=None),
    ranking_run_id: str | None = Query(default=None),
    ranking_version: str | None = Query(default=None),
) -> RankedRecommendationsResponse:
    """
    Read persisted paper_scores for a succeeded ranking run (latest for snapshot unless
    ranking_run_id or ranking_version narrows the choice).
    """
    try:
        resolved = list_ranked_recommendations(
            family=family,
            limit=limit,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and ranking data exists.",
        ) from exc

    if resolved is None:
        raise HTTPException(
            status_code=404,
            detail="No succeeded ranking run found for the given filters.",
        )

    ctx, rows = resolved
    return RankedRecommendationsResponse(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
        family=family,
        total=len(rows),
        items=[
            RankedRecommendationItem(
                paper_id=r.paper_id,
                title=r.title,
                year=r.year,
                citation_count=r.citation_count,
                source_slug=r.source_slug,
                topics=r.topics,
                signals=RankedSignals(
                    semantic=r.semantic_score,
                    citation_velocity=r.citation_velocity_score,
                    topic_growth=r.topic_growth_score,
                    bridge=r.bridge_score,
                    diversity_penalty=r.diversity_penalty,
                ),
                final_score=r.final_score,
                reason_short=r.reason_short,
            )
            for r in rows
        ],
    )


@app.get("/api/v1/recommendations/families", response_model=list[RankingFamily])
def get_recommendation_families() -> list[RankingFamily]:
    descriptions = {
        "emerging": "High-growth, semantically relevant work before it becomes consensus canon.",
        "bridge": "Work connecting nearby but distinct local clusters.",
        "undercited": "Relevant work that beats popularity-only ranking baselines.",
    }
    return [
        RankingFamily(key=family, description=descriptions[family])
        for family in settings.recommendation_families
    ]


@app.get("/api/v1/evaluation/summary", response_model=EvaluationSummary)
def get_evaluation_summary() -> EvaluationSummary:
    return EvaluationSummary(
        benchmark_target_size="100-200 papers",
        primary_metrics=["precision@10", "precision@20"],
        checks=list(settings.evaluation_checks),
        generated_at=utc_now(),
    )


@app.get("/api/v1/evaluation/compare", response_model=EvaluationCompareResponse)
def get_evaluation_compare(
    family: Literal["emerging", "bridge", "undercited"] = Query(...),
    limit: int = Query(default=15, ge=1, le=50),
    corpus_snapshot_version: str | None = Query(default=None),
    ranking_run_id: str | None = Query(default=None),
    ranking_version: str | None = Query(default=None),
) -> EvaluationCompareResponse:
    """
    Evaluation v0: ranked family vs citation-ordered and date-ordered baselines on the same pool.
    Proxy stats only — see response disclaimer.
    """
    try:
        payload = load_evaluation_compare(
            database_url=database_url_from_env(),
            family=family,
            limit=limit,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and ranking data exists.",
        ) from exc

    if payload is None:
        raise HTTPException(
            status_code=404,
            detail="No succeeded ranking run found for the given filters.",
        )

    return EvaluationCompareResponse(
        disclaimer=EVALUATION_V0_DISCLAIMER,
        ranking_run_id=payload.ranking_run_id,
        ranking_version=payload.ranking_version,
        corpus_snapshot_version=payload.corpus_snapshot_version,
        embedding_version=payload.embedding_version,
        family=payload.family,
        pool_definition=payload.pool_definition,
        pool_size=payload.pool_size,
        low_cite_min_year=payload.low_cite_min_year,
        low_cite_max_citations=payload.low_cite_max_citations,
        candidate_pool_doc_revision=payload.candidate_pool_doc_revision,
        topic_overlap_note=TOPIC_OVERLAP_NOTE,
        ranked=_evaluation_arm_response(payload.ranked),
        citation_baseline=_evaluation_arm_response(payload.citation_baseline),
        date_baseline=_evaluation_arm_response(payload.date_baseline),
        topic_overlap=EvaluationTopicOverlap(
            jaccard_ranked_vs_citation_baseline=payload.topic_overlap.jaccard_ranked_vs_citation_baseline,
            jaccard_ranked_vs_date_baseline=payload.topic_overlap.jaccard_ranked_vs_date_baseline,
            jaccard_citation_vs_date_baseline=payload.topic_overlap.jaccard_citation_vs_date_baseline,
        ),
        generated_at=utc_now(),
    )


@app.get("/api/v1/trends/topics", response_model=TopicTrendsResponse)
def get_topic_trends(
    limit: int = Query(default=20, ge=1, le=100),
    since_year: int = Query(default=utc_now().year - 1, ge=1990, le=2100),
    min_works: int = Query(default=2, ge=1, le=10_000),
    corpus_snapshot_version: str | None = Query(default=None),
) -> TopicTrendsResponse:
    try:
        result = list_topic_trends(
            limit=limit,
            since_year=since_year,
            min_works=min_works,
            corpus_snapshot_version=corpus_snapshot_version,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and topic data exists.",
        ) from exc

    return TopicTrendsResponse(
        corpus_snapshot_version=result.corpus_snapshot_version,
        since_year=since_year,
        min_works=min_works,
        total=len(result.rows),
        items=[
            TopicTrendItem(
                topic_id=r.topic_id,
                topic_name=r.topic_name,
                total_works=r.total_works,
                recent_works=r.recent_works,
                prior_works=r.prior_works,
                delta=r.delta,
                growth_label=r.growth_label,
            )
            for r in result.rows
        ],
        generated_at=utc_now(),
    )


@app.get("/api/v1/clusters/{cluster_version}/inspect", response_model=ClusterInspectionResponse)
def get_cluster_inspection(
    cluster_version: str,
    sample_per_cluster: int = Query(default=5, ge=1, le=50),
) -> ClusterInspectionResponse:
    """
    Inspect cluster assignments for a clustering run: per-cluster size and sample paper titles.
    Clustering uses kmeans-l2 on stored vectors; similar-papers uses cosine distance (see metric_note).
    """
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


@app.get(
    "/api/v1/papers/{paper_id:path}/similar",
    response_model=SimilarPapersResponse,
)
def get_paper_similar(
    paper_id: str,
    embedding_version: str = Query(..., min_length=1),
    limit: int = Query(default=10, ge=1, le=100),
) -> SimilarPapersResponse:
    """
    Nearest included neighbors by cosine similarity on persisted vectors for embedding_version.
    """
    try:
        result = list_similar_papers(
            paper_id=paper_id,
            embedding_version=embedding_version,
            limit=limit,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and embeddings exist.",
        ) from exc

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Paper not found or no embedding for this embedding_version.",
        )

    return SimilarPapersResponse(
        paper_id=result.paper_id,
        embedding_version=result.embedding_version,
        total=len(result.items),
        items=[
            SimilarPaperItem(
                paper_id=r.paper_id,
                title=r.title,
                year=r.year,
                citation_count=r.citation_count,
                source_slug=r.source_slug,
                topics=r.topics,
                similarity=r.similarity,
            )
            for r in result.items
        ],
    )


@app.get("/api/v1/papers/{paper_id:path}", response_model=PaperDetail)
def get_paper_detail(paper_id: str) -> PaperDetail:
    try:
        paper = get_paper_detail_row(paper_id)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database query failed. Confirm Postgres is running and seeded.") from exc

    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found.")

    return PaperDetail(
        paper_id=paper.paper_id,
        title=paper.title,
        abstract=paper.abstract,
        venue=paper.venue,
        year=paper.year,
        citation_count=paper.citation_count,
        source_slug=paper.source_slug,
        is_core_corpus=paper.is_core_corpus,
        authors=paper.authors,
        topics=paper.topics,
    )


@app.get("/api/v1/papers", response_model=PaperListResponse)
def get_papers(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> PaperListResponse:
    try:
        papers = list_papers(limit=limit, q=q)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database query failed. Confirm Postgres is running and seeded.") from exc

    return PaperListResponse(
        total=len(papers),
        items=[
            PaperListItem(
                paper_id=paper.paper_id,
                title=paper.title,
                year=paper.year,
                citation_count=paper.citation_count,
                source_slug=paper.source_slug,
                source_label=paper.source_label,
                is_core_corpus=paper.is_core_corpus,
                topics=paper.topics,
            )
            for paper in papers
        ],
    )

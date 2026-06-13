from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.contracts import (
    PaperDetail,
    PaperListItem,
    PaperListResponse,
    PaperRankingFamilyItem,
    PaperRankingResponse,
    RankedSignalExplanation,
    RankedSignals,
    SimilarPaperItem,
    SimilarPapersResponse,
)
from app.demo_fixtures import (
    fixture_mode_enabled,
    fixture_paper_detail,
    fixture_paper_ranking,
    fixture_papers,
    fixture_similar_papers,
)
from app.papers_repo import get_paper_detail as get_paper_detail_row
from app.papers_repo import list_papers
from app.ranked_explanations import build_signal_explanations, family_weights_from_config
from app.scores_repo import get_paper_family_rankings
from app.similarity_repo import list_similar_papers

router = APIRouter()


@router.get(
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
    if fixture_mode_enabled():
        fixture_result = fixture_similar_papers(
            paper_id=paper_id,
            embedding_version=embedding_version,
            limit=limit,
        )
        if fixture_result is None:
            raise HTTPException(status_code=404, detail="Paper not found in fixture corpus.")
        return fixture_result
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


@router.get(
    "/api/v1/papers/{paper_id:path}/ranking",
    response_model=PaperRankingResponse,
    response_model_exclude_none=False,
)
def get_paper_ranking(
    paper_id: str,
    top_n: int = Query(default=50, ge=1, le=500),
    corpus_snapshot_version: str | None = Query(default=None),
    ranking_run_id: str | None = Query(default=None),
    ranking_version: str | None = Query(default=None),
) -> PaperRankingResponse:
    if fixture_mode_enabled():
        fixture_result = fixture_paper_ranking(paper_id=paper_id, top_n=top_n)
        if fixture_result is None:
            raise HTTPException(status_code=404, detail="Paper not found in fixture corpus.")
        return fixture_result
    try:
        paper = get_paper_detail_row(paper_id)
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and seeded.",
        ) from exc

    if paper is None:
        raise HTTPException(status_code=404, detail="Paper not found.")

    try:
        resolved = get_paper_family_rankings(
            paper_id=paper_id,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
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

    ctx, rows, run_config = resolved
    families: list[PaperRankingFamilyItem] = []
    for row in rows:
        present = row.final_score is not None
        rank = row.rank if row.rank is not None and row.rank <= top_n else None
        weights = family_weights_from_config(run_config, row.family)
        explanations = (
            [
                RankedSignalExplanation(**x)
                for x in build_signal_explanations(
                    family=row.family,
                    semantic=row.semantic_score,
                    citation_velocity=row.citation_velocity_score,
                    topic_growth=row.topic_growth_score,
                    bridge=row.bridge_score,
                    diversity_penalty=row.diversity_penalty,
                    weights=weights,
                )
            ]
            if present
            else []
        )
        families.append(
            PaperRankingFamilyItem(
                family=row.family,
                present=present,
                in_top_n=rank is not None,
                rank=rank,
                final_score=row.final_score,
                reason_short=row.reason_short,
                signals=(
                    RankedSignals(
                        semantic=row.semantic_score,
                        citation_velocity=row.citation_velocity_score,
                        topic_growth=row.topic_growth_score,
                        bridge=row.bridge_score,
                        diversity_penalty=row.diversity_penalty,
                    )
                    if present
                    else None
                ),
                signal_explanations=explanations,
                bridge_eligible=row.bridge_eligible,
            )
        )

    return PaperRankingResponse(
        paper_id=paper.paper_id,
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
        top_n=top_n,
        rank_scope="family_global",
        families=families,
    )


@router.get("/api/v1/papers/{paper_id:path}", response_model=PaperDetail)
def get_paper_detail(paper_id: str) -> PaperDetail:
    if fixture_mode_enabled():
        fixture_result = fixture_paper_detail(paper_id)
        if fixture_result is None:
            raise HTTPException(status_code=404, detail="Paper not found in fixture corpus.")
        return fixture_result
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


@router.get("/api/v1/papers", response_model=PaperListResponse)
def get_papers(
    q: str | None = Query(default=None, min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
) -> PaperListResponse:
    if fixture_mode_enabled():
        return fixture_papers(q=q, limit=limit)
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

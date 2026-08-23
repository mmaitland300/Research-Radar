from __future__ import annotations

import logging

import psycopg
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row

from app.contracts import (
    ActivePublicReleaseMeta,
    HealthResponse,
    PublicReleaseReadinessDiagnostics,
    PublicReleaseScorerMeta,
    ReadinessResponse,
    utc_now,
)
from app.demo_fixtures import fixture_mode_enabled, fixture_readiness
from app.papers_repo import database_url_from_env
from app.public_release_repo import (
    PublicReleaseDiagnostics,
    PublicReleasePromotion,
    fetch_latest_public_release_promotion,
    inspect_public_release_serveability,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _release_meta(promotion: PublicReleasePromotion) -> ActivePublicReleaseMeta:
    return ActivePublicReleaseMeta(
        promotion_id=promotion.promotion_id,
        ranking_run_id=promotion.run.ranking_run_id,
        ranking_version=promotion.run.ranking_version,
        corpus_snapshot_version=promotion.run.corpus_snapshot_version,
        embedding_version=promotion.run.embedding_version,
        scorer=PublicReleaseScorerMeta(
            kind="materialized_paper_scores",
            version=promotion.run.ranking_run_id,
        ),
        promoted_at=promotion.promoted_at,
    )


def _release_diagnostics(
    diagnostics: PublicReleaseDiagnostics,
) -> PublicReleaseReadinessDiagnostics:
    return PublicReleaseReadinessDiagnostics(
        serveable=diagnostics.serveable,
        membership_count=diagnostics.membership_count,
        embedding_count=diagnostics.embedding_count,
        missing_embedding_count=diagnostics.missing_embedding_count,
        family_score_counts=diagnostics.family_score_counts,
        expected_family_score_counts=diagnostics.expected_family_score_counts,
        out_of_membership_score_count=diagnostics.out_of_membership_score_count,
        cluster_version=diagnostics.cluster_version,
        cluster_assignment_count=diagnostics.cluster_assignment_count,
        missing_cluster_assignment_count=diagnostics.missing_cluster_assignment_count,
        out_of_membership_cluster_count=diagnostics.out_of_membership_cluster_count,
        failures=list(diagnostics.failures),
    )


@router.get("/health", response_model=HealthResponse)
def health_check() -> HealthResponse:
    return HealthResponse(status="ok", timestamp=utc_now())


@router.get("/readyz", response_model=ReadinessResponse)
def readiness() -> ReadinessResponse | JSONResponse:
    if fixture_mode_enabled():
        return fixture_readiness()
    try:
        with psycopg.connect(database_url_from_env(), row_factory=dict_row) as conn:
            conn.execute("SELECT 1").fetchone()
            promotion = fetch_latest_public_release_promotion(conn)
            diagnostics = inspect_public_release_serveability(conn, promotion) if promotion is not None else None
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Database unreachable.") from exc

    if promotion is None:
        return ReadinessResponse(
            status="ok",
            database="connected",
            timestamp=utc_now(),
        )

    response = ReadinessResponse(
        status="ok" if diagnostics and diagnostics.serveable else "not_ready",
        database="connected",
        timestamp=utc_now(),
        active_release=_release_meta(promotion),
        release_diagnostics=(_release_diagnostics(diagnostics) if diagnostics is not None else None),
    )
    if diagnostics is not None and not diagnostics.serveable:
        logger.error(
            "Active public release is not serveable promotion_id=%s failures=%s",
            promotion.promotion_id,
            ",".join(diagnostics.failures),
            extra={
                "promotion_id": promotion.promotion_id,
                "failures": diagnostics.failures,
            },
        )
        return JSONResponse(status_code=503, content=response.model_dump(mode="json"))
    return response

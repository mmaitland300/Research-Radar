from __future__ import annotations

import logging

from fastapi import APIRouter

from app.config import PRODUCT_RANKING_METADATA_NOTE, settings
from app.contracts import (
    ActivePublicReleaseMeta,
    MaterializedRankingMeta,
    ProductSummary,
    PublicReleaseScorerMeta,
)
from app.demo_fixtures import fixture_mode_enabled, fixture_product_summary
from app.public_release_repo import load_latest_public_release_promotion
from app.scores_repo import fetch_latest_materialized_ranking_for_meta

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/v1/meta/product", response_model=ProductSummary)
def get_product_summary() -> ProductSummary:
    if fixture_mode_enabled():
        return fixture_product_summary()
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
    except Exception as exc:
        exception_type = type(exc).__name__
        logger.error(
            "Failed to load materialized ranking metadata for product summary exception_type=%s",
            exception_type,
            extra={"exception_type": exception_type},
        )
        materialized = None

    active_release = None
    try:
        promotion = load_latest_public_release_promotion()
        if promotion is not None:
            active_release = ActivePublicReleaseMeta(
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
    except Exception as exc:
        exception_type = type(exc).__name__
        logger.error(
            "Failed to load active release metadata for product summary exception_type=%s",
            exception_type,
            extra={"exception_type": exception_type},
        )
        active_release = None

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
        active_release=active_release,
    )

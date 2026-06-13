from __future__ import annotations

import logging

from fastapi import APIRouter

from app.config import PRODUCT_RANKING_METADATA_NOTE, settings
from app.contracts import MaterializedRankingMeta, ProductSummary
from app.demo_fixtures import fixture_mode_enabled, fixture_product_summary
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

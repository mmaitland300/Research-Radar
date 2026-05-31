"""Fail-closed API integration for the bounded ML scorer rollout."""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any

from fastapi import Request

from app.contracts import (
    RankedListExplanation,
    RankedRecommendationItem,
    RankedRecommendationsResponse,
    RankedRankingMode,
    RankedSignalExplanation,
    RankedSignals,
)
from app.ml_scorer_rollout_gate import (
    ROLLOUT_ROUTE,
    build_gate_from_env,
    get_rollout_served_count,
    release_rollout_slot_for_failure,
    try_reserve_rollout_slot,
)
from app.ranked_explanations import (
    build_list_ranking_explanation,
    build_signal_explanations,
    family_weights_from_config,
)
from app.scores_repo import (
    RankedRecommendationRow,
    RankedRunContext,
    hydrate_ranked_recommendation_rows_for_paper_ids,
    list_ranked_recommendations,
)

logger = logging.getLogger(__name__)

_CANARY_SUBJECT_HEADER = "X-Research-Radar-Canary-Subject"
_FEATURE_FLAG = "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"
_SCORER_RANKING_MODE_DETAIL = (
    "Order selected by bounded ML scorer; "
    "displayed scores/signals are materialized ranking metadata."
)


def get_canary_subject(request: Request) -> str | None:
    subject = (request.headers.get(_CANARY_SUBJECT_HEADER) or "").strip()
    return subject or None


def build_ranked_recommendations_response(
    ctx: RankedRunContext,
    rows: list[RankedRecommendationRow],
    run_config: dict[str, Any],
    family: str,
    *,
    ranking_mode: RankedRankingMode = "materialized_heuristic",
    ranking_mode_detail: str | None = None,
) -> RankedRecommendationsResponse:
    weights = family_weights_from_config(run_config, family)
    list_payload = build_list_ranking_explanation(family=family, weights=weights)
    list_explanation = RankedListExplanation(**list_payload)

    items_out: list[RankedRecommendationItem] = []
    for row in rows:
        expl = build_signal_explanations(
            family=family,
            semantic=row.semantic_score,
            citation_velocity=row.citation_velocity_score,
            topic_growth=row.topic_growth_score,
            bridge=row.bridge_score,
            diversity_penalty=row.diversity_penalty,
            weights=weights,
        )
        items_out.append(
            RankedRecommendationItem(
                paper_id=row.paper_id,
                title=row.title,
                year=row.year,
                citation_count=row.citation_count,
                source_slug=row.source_slug,
                topics=row.topics,
                signals=RankedSignals(
                    semantic=row.semantic_score,
                    citation_velocity=row.citation_velocity_score,
                    topic_growth=row.topic_growth_score,
                    bridge=row.bridge_score,
                    diversity_penalty=row.diversity_penalty,
                ),
                final_score=row.final_score,
                reason_short=row.reason_short,
                signal_explanations=[RankedSignalExplanation(**item) for item in expl],
                bridge_eligible=row.bridge_eligible,
            )
        )

    return RankedRecommendationsResponse(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
        family=family,
        ranking_mode=ranking_mode,
        ranking_mode_detail=ranking_mode_detail,
        total=len(rows),
        list_explanation=list_explanation,
        items=items_out,
    )


def _load_pipeline_serving_module() -> Any:
    return importlib.import_module("pipeline.ml_scorer_rollout_serving")


def _log_gate_closed(reason: str, *, exception_type: str | None = None) -> None:
    extra = {
        "reason": reason,
        "exception_type": exception_type,
    }
    logger.info(
        "ml_scorer_rollout gate_closed reason=%s exception_type=%s",
        reason,
        exception_type,
        extra=extra,
    )


def maybe_build_scorer_ranked_response(
    *,
    route: str = ROLLOUT_ROUTE,
    family: str,
    limit: int,
    corpus_snapshot_version: str | None,
    ranking_run_id: str | None,
    ranking_version: str | None,
    bridge_eligible_only: bool,
    subject: str | None,
) -> RankedRecommendationsResponse | None:
    subject_present = bool(subject)
    try:
        gate = build_gate_from_env()
    except Exception as exc:
        _log_gate_closed(
            "gate_config_failed",
            exception_type=type(exc).__name__,
        )
        return None

    current_served = get_rollout_served_count()
    should_attempt, reason = gate.should_attempt_scorer_path(
        route,
        family,
        limit,
        bridge_eligible_only,
        subject,
        current_served,
        gate.exposure_cap,
    )
    if not should_attempt:
        _log_gate_closed(reason or "unknown")
        return None

    try:
        resolved = list_ranked_recommendations(
            family=family,
            limit=limit,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
            bridge_eligible_only=bridge_eligible_only,
        )
    except Exception as exc:
        _log_gate_closed(
            "db_read_failed",
            exception_type=type(exc).__name__,
        )
        return None
    if resolved is None:
        _log_gate_closed("ranking_context_missing")
        return None

    ctx, _baseline_rows, run_config = resolved
    if not gate.is_pinned_run_context(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        family=family,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
    ):
        _log_gate_closed("identity_mismatch")
        return None

    if not try_reserve_rollout_slot(gate.exposure_cap):
        _log_gate_closed("cap_exhausted")
        return None

    try:
        try:
            serving = _load_pipeline_serving_module()
        except (ImportError, ModuleNotFoundError) as exc:
            release_rollout_slot_for_failure()
            _log_gate_closed(
                "pipeline_import_failed",
                exception_type=type(exc).__name__,
            )
            return None

        shadow_rows, _metadata = serving.rank_emerging_recommendations_with_scorer(
            env={**os.environ, _FEATURE_FLAG: "true"}
        )
        ordered_paper_ids = serving.map_shadow_rows_to_paper_ids(
            shadow_rows, limit=limit
        )
        hydrated_rows = hydrate_ranked_recommendation_rows_for_paper_ids(
            ctx=ctx,
            family=family,
            ordered_openalex_ids=ordered_paper_ids,
        )
        if hydrated_rows is None or len(hydrated_rows) != limit:
            release_rollout_slot_for_failure()
            _log_gate_closed("hydration_incomplete")
            return None

        response = build_ranked_recommendations_response(
            ctx,
            hydrated_rows,
            run_config,
            family,
            ranking_mode="bounded_ml_scorer",
            ranking_mode_detail=_SCORER_RANKING_MODE_DETAIL,
        )
        logger.info(
            (
                "ml_scorer_rollout gate_open served=true subject_present=%s "
                "public_rollout_enabled=%s public_rollout_percent=%s cap=%s current_served=%s"
            ),
            subject_present,
            gate.public_rollout_enabled,
            gate.public_rollout_percent,
            gate.exposure_cap,
            current_served,
            extra={
                "served": True,
                "subject_present": subject_present,
                "public_rollout_enabled": gate.public_rollout_enabled,
                "public_rollout_percent": gate.public_rollout_percent,
                "cap": gate.exposure_cap,
                "current_served": current_served,
            },
        )
        logger.debug("ml_scorer_rollout item_count=%s", len(hydrated_rows))
        return response
    except Exception as exc:
        release_rollout_slot_for_failure()
        _log_gate_closed(
            "scorer_failed",
            exception_type=type(exc).__name__,
        )
        return None

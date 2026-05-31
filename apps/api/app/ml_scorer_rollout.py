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


def get_canary_subject(request: Request) -> str | None:
    subject = (request.headers.get(_CANARY_SUBJECT_HEADER) or "").strip()
    return subject or None


def build_ranked_recommendations_response(
    ctx: RankedRunContext,
    rows: list[RankedRecommendationRow],
    run_config: dict[str, Any],
    family: str,
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
        total=len(rows),
        list_explanation=list_explanation,
        items=items_out,
    )


def _load_pipeline_serving_module() -> Any:
    return importlib.import_module("pipeline.ml_scorer_rollout_serving")


def _log_gate_closed(reason: str, *, subject_present: bool, exception_type: str | None = None) -> None:
    extra = {
        "reason": reason,
        "subject_present": subject_present,
        "exception_type": exception_type,
    }
    logger.info(
        "ml_scorer_rollout gate_closed reason=%s subject_present=%s exception_type=%s",
        reason,
        subject_present,
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
            subject_present=subject_present,
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
        _log_gate_closed(reason or "unknown", subject_present=subject_present)
        return None

    resolved = list_ranked_recommendations(
        family=family,
        limit=limit,
        corpus_snapshot_version=corpus_snapshot_version,
        ranking_run_id=ranking_run_id,
        ranking_version=ranking_version,
        bridge_eligible_only=bridge_eligible_only,
    )
    if resolved is None:
        return None

    ctx, _baseline_rows, run_config = resolved
    if not gate.is_pinned_run_context(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        family=family,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
    ):
        _log_gate_closed("identity_mismatch", subject_present=subject_present)
        return None

    if not try_reserve_rollout_slot(gate.exposure_cap):
        _log_gate_closed("cap_exhausted", subject_present=subject_present)
        return None

    try:
        try:
            serving = _load_pipeline_serving_module()
        except (ImportError, ModuleNotFoundError) as exc:
            release_rollout_slot_for_failure()
            _log_gate_closed(
                "pipeline_import_failed",
                subject_present=subject_present,
                exception_type=type(exc).__name__,
            )
            return None

        shadow_rows, _metadata = serving.rank_emerging_recommendations_with_scorer(
            env={**os.environ, _FEATURE_FLAG: "true"}
        )
        ordered_paper_ids = serving.map_shadow_rows_to_paper_ids(shadow_rows, limit=limit)
        hydrated_rows = hydrate_ranked_recommendation_rows_for_paper_ids(
            ctx=ctx,
            family=family,
            ordered_openalex_ids=ordered_paper_ids,
        )
        if hydrated_rows is None or len(hydrated_rows) != limit:
            release_rollout_slot_for_failure()
            _log_gate_closed("hydration_incomplete", subject_present=subject_present)
            return None

        response = build_ranked_recommendations_response(ctx, hydrated_rows, run_config, family)
        logger.info(
            "ml_scorer_rollout gate_open served=true subject_present=%s",
            subject_present,
            extra={"served": True, "subject_present": subject_present},
        )
        logger.debug("ml_scorer_rollout item_count=%s", len(hydrated_rows))
        return response
    except Exception as exc:
        release_rollout_slot_for_failure()
        _log_gate_closed(
            "scorer_failed",
            subject_present=subject_present,
            exception_type=type(exc).__name__,
        )
        return None

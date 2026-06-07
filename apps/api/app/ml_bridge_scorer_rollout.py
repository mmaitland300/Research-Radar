"""Fail-closed API integration for bounded Bridge rank-pct hybrid scorer serving."""

from __future__ import annotations

import importlib
import logging
from typing import Any

from app.contracts import (
    RankedListExplanation,
    RankedRecommendationItem,
    RankedRecommendationsResponse,
    RankedRankingMode,
    RankedSignalExplanation,
    RankedSignals,
)
from app.ml_bridge_scorer_rollout_gate import (
    BRIDGE_ROLLOUT_ROUTE,
)
from app.ml_bridge_scorer_rollout_gate import (
    build_bridge_gate_from_env,
    get_bridge_rollout_served_count,
    release_bridge_rollout_slot_for_failure,
    try_reserve_bridge_rollout_slot,
)
from app.ranked_explanations import (
    build_list_ranking_explanation,
    build_signal_explanations,
    family_weights_from_config,
)
from app.scores_repo import (
    RankedRecommendationRow,
    RankedRunContext,
    hydrate_ranked_bridge_recommendation_rows_for_paper_ids,
    list_ranked_recommendations,
)

logger = logging.getLogger(__name__)

_SCORER_RANKING_MODE_DETAIL = (
    "Bridge order selected by bounded ML scorer rollout; blends bridge_score with "
    "a frozen Bridge ML scorer over full-pool rank percentiles. Displayed "
    "scores/signals remain materialized ranking metadata."
)
_PRIMARY_ALPHA = 0.5
_RANK_PCT_SCOPE = "full_bridge_candidate_pool"
_SCORER_PROBABILITY_SOURCE = "full_pool_frozen_inference_not_oof"


def build_bridge_ranked_recommendations_response(
    ctx: RankedRunContext,
    rows: list[RankedRecommendationRow],
    run_config: dict[str, Any],
    *,
    ranking_mode: RankedRankingMode = "materialized_heuristic",
    ranking_mode_detail: str | None = None,
    bridge_recommendations_ml_served: bool | None = None,
    emitted_to_public_users: bool | None = None,
) -> RankedRecommendationsResponse:
    family = "bridge"
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

    served = bridge_recommendations_ml_served is True
    return RankedRecommendationsResponse(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
        family=family,
        ranking_mode=ranking_mode,
        ranking_mode_detail=ranking_mode_detail,
        scorer_surface="bridge" if served else None,
        bridge_recommendations_ml_served=bridge_recommendations_ml_served,
        bridge_rank_pct_hybrid_alpha=_PRIMARY_ALPHA if served else None,
        bridge_rank_pct_scope=_RANK_PCT_SCOPE if served else None,
        emitted_to_public_users=emitted_to_public_users if served else None,
        total=len(rows),
        list_explanation=list_explanation,
        items=items_out,
    )


def _load_pipeline_serving_module() -> Any:
    return importlib.import_module("pipeline.ml_bridge_scorer_rollout_serving")


def _metadata_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _log_gate_closed(
    reason: str,
    *,
    route: str,
    family: str,
    current_served: int | None,
    cap: int | None,
    ranking_run_id: str | None,
    public_rollout_enabled: bool | None,
    public_rollout_percent: int | None,
    exception_type: str | None = None,
) -> None:
    extra = {
        "ranking_mode": "materialized_heuristic",
        "family": family,
        "route": route,
        "gate_decision": "closed",
        "reason_closed": reason,
        "current_served": current_served,
        "cap": cap,
        "ranking_run_id": ranking_run_id,
        "public_rollout_enabled": public_rollout_enabled,
        "public_rollout_percent": public_rollout_percent,
        "exception_type": exception_type,
    }
    logger.info(
        (
            "bridge_scorer_rollout gate_closed family=%s route=%s reason_closed=%s "
            "current_served=%s cap=%s ranking_run_id=%s public_rollout_enabled=%s "
            "public_rollout_percent=%s exception_type=%s"
        ),
        family,
        route,
        reason,
        current_served,
        cap,
        ranking_run_id,
        public_rollout_enabled,
        public_rollout_percent,
        exception_type,
        extra=extra,
    )


def maybe_build_bridge_scorer_ranked_response(
    *,
    route: str = BRIDGE_ROLLOUT_ROUTE,
    family: str,
    limit: int,
    corpus_snapshot_version: str | None,
    ranking_run_id: str | None,
    ranking_version: str | None,
    bridge_eligible_only: bool,
    subject: str | None,
) -> RankedRecommendationsResponse | None:
    try:
        gate = build_bridge_gate_from_env()
    except Exception as exc:
        _log_gate_closed(
            "gate_config_failed",
            route=route,
            family=family,
            current_served=None,
            cap=None,
            ranking_run_id=ranking_run_id,
            public_rollout_enabled=None,
            public_rollout_percent=None,
            exception_type=type(exc).__name__,
        )
        return None

    current_served = get_bridge_rollout_served_count()
    decision = gate.should_attempt_scorer_path(
        route,
        family,
        limit,
        bridge_eligible_only,
        subject,
        current_served,
        gate.exposure_cap,
        ranking_run_id,
    )
    if not decision.should_attempt:
        _log_gate_closed(
            decision.reason or "unknown",
            route=route,
            family=family,
            current_served=current_served,
            cap=gate.exposure_cap,
            ranking_run_id=ranking_run_id,
            public_rollout_enabled=gate.public_rollout_enabled,
            public_rollout_percent=gate.public_rollout_percent,
        )
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
            route=route,
            family=family,
            current_served=current_served,
            cap=gate.exposure_cap,
            ranking_run_id=ranking_run_id,
            public_rollout_enabled=gate.public_rollout_enabled,
            public_rollout_percent=gate.public_rollout_percent,
            exception_type=type(exc).__name__,
        )
        return None
    if resolved is None:
        _log_gate_closed(
            "ranking_context_missing",
            route=route,
            family=family,
            current_served=current_served,
            cap=gate.exposure_cap,
            ranking_run_id=ranking_run_id,
            public_rollout_enabled=gate.public_rollout_enabled,
            public_rollout_percent=gate.public_rollout_percent,
        )
        return None

    ctx, _baseline_rows, run_config = resolved
    pinned_ok, pinned_reason = gate.is_pinned_run_context(
        ranking_run_id=ctx.ranking_run_id,
        ranking_version=ctx.ranking_version,
        requested_ranking_version=ranking_version,
        corpus_snapshot_version=ctx.corpus_snapshot_version,
        requested_corpus_snapshot_version=corpus_snapshot_version,
    )
    if not pinned_ok:
        _log_gate_closed(
            pinned_reason or "identity_mismatch",
            route=route,
            family=family,
            current_served=current_served,
            cap=gate.exposure_cap,
            ranking_run_id=ctx.ranking_run_id,
            public_rollout_enabled=gate.public_rollout_enabled,
            public_rollout_percent=gate.public_rollout_percent,
        )
        return None

    if not try_reserve_bridge_rollout_slot(gate.exposure_cap):
        _log_gate_closed(
            "cap_exhausted",
            route=route,
            family=family,
            current_served=current_served,
            cap=gate.exposure_cap,
            ranking_run_id=ctx.ranking_run_id,
            public_rollout_enabled=gate.public_rollout_enabled,
            public_rollout_percent=gate.public_rollout_percent,
        )
        return None

    try:
        try:
            serving = _load_pipeline_serving_module()
        except (ImportError, ModuleNotFoundError) as exc:
            release_bridge_rollout_slot_for_failure()
            _log_gate_closed(
                "pipeline_import_failed",
                route=route,
                family=family,
                current_served=current_served,
                cap=gate.exposure_cap,
                ranking_run_id=ctx.ranking_run_id,
                public_rollout_enabled=gate.public_rollout_enabled,
                public_rollout_percent=gate.public_rollout_percent,
                exception_type=type(exc).__name__,
            )
            return None

        scored_rows, metadata = serving.rank_bridge_recommendations_with_scorer(limit=limit)
        if metadata.get("ranking_run_id") != ctx.ranking_run_id or metadata.get("family") != family:
            release_bridge_rollout_slot_for_failure()
            _log_gate_closed(
                "scorer_context_mismatch",
                route=route,
                family=family,
                current_served=current_served,
                cap=gate.exposure_cap,
                ranking_run_id=ctx.ranking_run_id,
                public_rollout_enabled=gate.public_rollout_enabled,
                public_rollout_percent=gate.public_rollout_percent,
            )
            return None
        if (
            _metadata_float(metadata.get("primary_alpha")) != _PRIMARY_ALPHA
            or metadata.get("rank_pct_scope") != _RANK_PCT_SCOPE
            or metadata.get("scorer_probability_source") != _SCORER_PROBABILITY_SOURCE
        ):
            release_bridge_rollout_slot_for_failure()
            _log_gate_closed(
                "scorer_contract_mismatch",
                route=route,
                family=family,
                current_served=current_served,
                cap=gate.exposure_cap,
                ranking_run_id=ctx.ranking_run_id,
                public_rollout_enabled=gate.public_rollout_enabled,
                public_rollout_percent=gate.public_rollout_percent,
            )
            return None
        if metadata.get("writes_performed") is not False:
            release_bridge_rollout_slot_for_failure()
            _log_gate_closed(
                "scorer_write_contract_missing",
                route=route,
                family=family,
                current_served=current_served,
                cap=gate.exposure_cap,
                ranking_run_id=ctx.ranking_run_id,
                public_rollout_enabled=gate.public_rollout_enabled,
                public_rollout_percent=gate.public_rollout_percent,
            )
            return None
        ordered_paper_ids = serving.map_bridge_scorer_rows_to_paper_ids(scored_rows, limit=limit)
        if len(ordered_paper_ids) != limit:
            release_bridge_rollout_slot_for_failure()
            _log_gate_closed(
                "scorer_order_incomplete",
                route=route,
                family=family,
                current_served=current_served,
                cap=gate.exposure_cap,
                ranking_run_id=ctx.ranking_run_id,
                public_rollout_enabled=gate.public_rollout_enabled,
                public_rollout_percent=gate.public_rollout_percent,
            )
            return None
        hydrated_rows = hydrate_ranked_bridge_recommendation_rows_for_paper_ids(
            ctx=ctx,
            ordered_openalex_ids=ordered_paper_ids,
        )
        if hydrated_rows is None or len(hydrated_rows) != limit:
            release_bridge_rollout_slot_for_failure()
            _log_gate_closed(
                "hydration_incomplete",
                route=route,
                family=family,
                current_served=current_served,
                cap=gate.exposure_cap,
                ranking_run_id=ctx.ranking_run_id,
                public_rollout_enabled=gate.public_rollout_enabled,
                public_rollout_percent=gate.public_rollout_percent,
            )
            return None

        response = build_bridge_ranked_recommendations_response(
            ctx,
            hydrated_rows,
            run_config,
            ranking_mode="bounded_bridge_ml_scorer",
            ranking_mode_detail=_SCORER_RANKING_MODE_DETAIL,
            bridge_recommendations_ml_served=True,
            emitted_to_public_users=decision.emitted_to_public_users,
        )
        logger.info(
            (
                "bridge_scorer_rollout gate_open family=%s route=%s ranking_mode=%s "
                "current_served=%s cap=%s ranking_run_id=%s public_rollout_enabled=%s "
                "public_rollout_percent=%s"
            ),
            family,
            route,
            "bounded_bridge_ml_scorer",
            current_served,
            gate.exposure_cap,
            ctx.ranking_run_id,
            gate.public_rollout_enabled,
            gate.public_rollout_percent,
            extra={
                "ranking_mode": "bounded_bridge_ml_scorer",
                "family": family,
                "route": route,
                "gate_decision": "open",
                "reason_closed": None,
                "current_served": current_served,
                "cap": gate.exposure_cap,
                "ranking_run_id": ctx.ranking_run_id,
                "public_rollout_enabled": gate.public_rollout_enabled,
                "public_rollout_percent": gate.public_rollout_percent,
            },
        )
        return response
    except Exception as exc:
        release_bridge_rollout_slot_for_failure()
        _log_gate_closed(
            "scorer_failed",
            route=route,
            family=family,
            current_served=current_served,
            cap=gate.exposure_cap,
            ranking_run_id=ctx.ranking_run_id,
            public_rollout_enabled=gate.public_rollout_enabled,
            public_rollout_percent=gate.public_rollout_percent,
            exception_type=type(exc).__name__,
        )
        return None

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.bridge_distinctness_repo import load_bridge_distinctness_report
from app.config import settings
from app.contracts import (
    BridgeDistinctnessDecisionSupport,
    BridgeDistinctnessOverlapMetrics,
    BridgeDistinctnessResponse,
    EvaluationCitationProxy,
    EvaluationCompareResponse,
    EvaluationDisclaimer,
    EvaluationListArmResponse,
    EvaluationPaperItem,
    EvaluationRecencyProxy,
    EvaluationSummary,
    EvaluationTopicMixProxy,
    EvaluationTopicOverlap,
    utc_now,
)
from app.demo_fixtures import (
    fixture_bridge_distinctness,
    fixture_evaluation_compare,
    fixture_mode_enabled,
)
from app.evaluation_repo import EvalListArm, load_evaluation_compare
from app.papers_repo import database_url_from_env
from app.serving_context import ServingContextNotFoundError, ServingContextUnavailableError

router = APIRouter()


EVALUATION_V0_DISCLAIMER = EvaluationDisclaimer(
    headline=(
        "These outputs help compare ranking behavior and expose drift; "
        "they are not expert-reviewed evidence that papers are useful to researchers."
    ),
    bullets=[
        "Side-by-side lists share the same candidate pool for the selected recommendation family and corpus snapshot.",
        "Recency, citation, and topic summaries are coarse proxies over the short lists shown; they do not measure whether a researcher would find a paper useful.",
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



@router.get("/api/v1/evaluation/summary", response_model=EvaluationSummary)
def get_evaluation_summary() -> EvaluationSummary:
    planned = {
        "corpus": "100-200 papers",
        "metrics": ["precision@10", "precision@20"],
    }
    return EvaluationSummary(
        current_evaluation_type="proxy_ranked_vs_citation_and_date_baselines",
        is_human_labeled_benchmark_current=False,
        planned_labeled_benchmark=planned,
        # Legacy keys: string values are explicit in raw JSON for clients that skip schema/notes.
        benchmark_target_size="100-200 papers (roadmap; not a current human benchmark result)",
        primary_metrics=[
            "precision@10 (roadmap; not a current P@k score)",
            "precision@20 (roadmap; not a current P@k score)",
        ],
        legacy_note=(
            "benchmark_target_size and primary_metrics are roadmap-only compatibility fields; "
            "see is_human_labeled_benchmark_current and planned_labeled_benchmark for meaning."
        ),
        checks=list(settings.evaluation_checks),
        generated_at=utc_now(),
    )


@router.get("/api/v1/evaluation/compare", response_model=EvaluationCompareResponse)
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
    if fixture_mode_enabled():
        return fixture_evaluation_compare(family=family, limit=limit)
    try:
        payload = load_evaluation_compare(
            database_url=database_url_from_env(),
            family=family,
            limit=limit,
            corpus_snapshot_version=corpus_snapshot_version,
            ranking_run_id=ranking_run_id,
            ranking_version=ranking_version,
        )
    except ServingContextNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ServingContextUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail="Database query failed. Confirm Postgres is running and ranking data exists.",
        ) from exc

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


@router.get("/api/v1/evaluation/bridge-distinctness", response_model=BridgeDistinctnessResponse)
def get_bridge_distinctness(
    ranking_run_id: str = Query(
        ...,
        min_length=1,
        description="Succeeded materialized run id (required). No latest or ranking_version fallback.",
    ),
    k: int = Query(default=10, ge=1, le=50),
) -> BridgeDistinctnessResponse:
    """
    Read-only comparison of full bridge, eligible-only bridge, and emerging top-k for one pinned run.
    Decision fields are engineering hints only, not validation of bridge quality.
    """
    if not ranking_run_id.strip():
        raise HTTPException(
            status_code=422,
            detail="ranking_run_id is required and must not be blank.",
        )
    rid = ranking_run_id.strip()
    if fixture_mode_enabled():
        return fixture_bridge_distinctness(k=k)
    try:
        payload = load_bridge_distinctness_report(
            database_url=database_url_from_env(),
            ranking_run_id=rid,
            k=k,
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
            detail="Ranking run not found or not in succeeded status.",
        )

    return BridgeDistinctnessResponse(
        ranking_run_id=payload.ranking_run_id,
        ranking_version=payload.ranking_version,
        corpus_snapshot_version=payload.corpus_snapshot_version,
        embedding_version=payload.embedding_version,
        cluster_version=payload.cluster_version,
        k=payload.k,
        full_bridge_top_k_ids=payload.full_bridge_top_k_ids,
        eligible_bridge_top_k_ids=payload.eligible_bridge_top_k_ids,
        emerging_top_k_ids=payload.emerging_top_k_ids,
        full_bridge_vs_eligible_bridge=BridgeDistinctnessOverlapMetrics(
            overlap_count=payload.full_bridge_vs_eligible_bridge_overlap_count,
            jaccard=payload.full_bridge_vs_eligible_bridge_jaccard,
        ),
        full_bridge_vs_emerging=BridgeDistinctnessOverlapMetrics(
            overlap_count=payload.full_bridge_vs_emerging_overlap_count,
            jaccard=payload.full_bridge_vs_emerging_jaccard,
        ),
        eligible_bridge_vs_emerging=BridgeDistinctnessOverlapMetrics(
            overlap_count=payload.eligible_bridge_vs_emerging_overlap_count,
            jaccard=payload.eligible_bridge_vs_emerging_jaccard,
        ),
        bridge_family_row_count=payload.bridge_family_row_count,
        bridge_score_nonnull_count=payload.bridge_score_nonnull_count,
        bridge_score_null_count=payload.bridge_score_null_count,
        bridge_eligible_true_count=payload.bridge_eligible_true_count,
        bridge_eligible_false_count=payload.bridge_eligible_false_count,
        bridge_eligible_null_count=payload.bridge_eligible_null_count,
        bridge_signal_json_present_count=payload.bridge_signal_json_present_count,
        bridge_signal_json_missing_count=payload.bridge_signal_json_missing_count,
        decision_support=BridgeDistinctnessDecisionSupport(
            eligible_head_differs_from_full=payload.eligible_head_differs_from_full,
            eligible_head_less_emerging_like_than_full=payload.eligible_head_less_emerging_like_than_full,
            suggested_next_step=payload.suggested_next_step,
        ),
        generated_at=utc_now(),
    )

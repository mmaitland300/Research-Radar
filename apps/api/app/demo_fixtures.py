"""Fixture-backed API responses for no-key local reviewer demos.

The default API remains Postgres-backed. This module is only used when
RESEARCH_RADAR_DATA_MODE=fixture so reviewers can inspect the product surfaces
without Docker, OpenAlex, pgvector, or OpenAI credentials.
"""

from __future__ import annotations

import os
from statistics import median
from typing import Any, Literal
from urllib.parse import unquote

from app.config import PRODUCT_RANKING_METADATA_NOTE, settings
from app.contracts import (
    ActivePublicReleaseMeta,
    BridgeDistinctnessDecisionSupport,
    BridgeDistinctnessOverlapMetrics,
    BridgeDistinctnessResponse,
    ClusterGroupItem,
    ClusterInspectionResponse,
    ClusterSamplePaperItem,
    EvaluationCitationProxy,
    EvaluationCompareResponse,
    EvaluationDisclaimer,
    EvaluationListArmResponse,
    EvaluationPaperItem,
    EvaluationRecencyProxy,
    EvaluationTopicMixProxy,
    EvaluationTopicOverlap,
    MaterializedRankingMeta,
    PaperDetail,
    PaperListItem,
    PaperListResponse,
    PaperRankingFamilyItem,
    PaperRankingResponse,
    ProductSummary,
    PublicReleaseReadinessDiagnostics,
    PublicReleaseScorerMeta,
    RankedListExplanation,
    RankedRecommendationItem,
    RankedRecommendationsResponse,
    RankedSignalExplanation,
    RankedSignals,
    ReadinessResponse,
    SearchMatchMetadata,
    SearchResolvedFilters,
    SearchResponse,
    SearchResultItem,
    SimilarPaperItem,
    SimilarPapersResponse,
    TopicTrendItem,
    TopicTrendsResponse,
    UndercitedRecommendationItem,
    UndercitedRecommendationsResponse,
    utc_now,
)
from app.ranked_explanations import (
    build_list_ranking_explanation,
    build_signal_explanations,
    family_weights_from_config,
)

DATA_MODE_ENV = "RESEARCH_RADAR_DATA_MODE"
CORPUS_SNAPSHOT_VERSION = "fixture-snapshot-mir-audio-2026"
RANKING_RUN_ID = "fixture-rank-demo-001"
RANKING_VERSION = "fixture-demo-v0-no-db"
EMBEDDING_VERSION = "fixture-title-abstract-v0"
CLUSTER_VERSION = "fixture-kmeans-v0"

RUN_CONFIG: dict[str, Any] = {
    "data_mode": "fixture",
    "claim_boundary": (
        "Local fixture demo for UI/API walkthroughs only; not live ranking data "
        "or a validation benchmark."
    ),
    "family_weights": {
        "emerging": {
            "semantic": 0.25,
            "citation_velocity": 0.45,
            "topic_growth": 0.30,
            "bridge": 0.0,
            "diversity_penalty": 0.05,
        },
        "bridge": {
            "semantic": 0.0,
            "citation_velocity": 0.25,
            "topic_growth": 0.35,
            "bridge": 0.40,
            "diversity_penalty": 0.10,
        },
        "undercited": {
            "semantic": 0.0,
            "citation_velocity": 0.35,
            "topic_growth": 0.55,
            "bridge": 0.0,
            "diversity_penalty": 0.20,
        },
    },
}

FIXTURE_PAPERS: list[dict[str, Any]] = [
    {
        "paper_id": "https://openalex.org/WF001",
        "title": "Contrastive Audio Embeddings for Music Retrieval",
        "abstract": (
            "A fixture paper about contrastive learning, audio embeddings, and "
            "music information retrieval over a curated corpus."
        ),
        "venue": "TISMIR",
        "year": 2026,
        "citation_count": 2,
        "source_slug": "tismir",
        "source_label": "TISMIR",
        "is_core_corpus": True,
        "authors": ["A. Rivera", "M. Chen"],
        "topics": ["Music Information Retrieval", "Audio Embeddings", "Contrastive Learning"],
        "cluster": "audio-retrieval",
    },
    {
        "paper_id": "https://openalex.org/WF002",
        "title": "Similarity-Based Conditioning for Controllable Sound Effects",
        "abstract": (
            "A fixture paper on sound-effect synthesis, similarity conditioning, "
            "and retrieval-like controls for generative audio."
        ),
        "venue": "JAES",
        "year": 2025,
        "citation_count": 1,
        "source_slug": "jaes",
        "source_label": "JAES",
        "is_core_corpus": True,
        "authors": ["L. Patel", "S. Gomez"],
        "topics": ["Generative Audio", "Sound Design", "Audio Embeddings"],
        "cluster": "audio-generation",
    },
    {
        "paper_id": "https://openalex.org/WF003",
        "title": "Auditory-Visual Representation Learning with Music Actions",
        "abstract": (
            "A fixture multimodal paper linking visual action cues, audio features, "
            "and music-related representation learning."
        ),
        "venue": "TISMIR",
        "year": 2026,
        "citation_count": 0,
        "source_slug": "tismir",
        "source_label": "TISMIR",
        "is_core_corpus": True,
        "authors": ["J. Okafor", "N. Singh"],
        "topics": ["Multimodal Learning", "Audio Embeddings", "Music Technology"],
        "cluster": "multimodal-audio",
    },
    {
        "paper_id": "https://openalex.org/WF004",
        "title": "Benchmarking MIR Datasets for Under-Cited Evaluation Work",
        "abstract": (
            "A fixture evaluation paper focused on dataset coverage, reproducible "
            "MIR baselines, and low-citation candidate discovery."
        ),
        "venue": "TISMIR",
        "year": 2024,
        "citation_count": 4,
        "source_slug": "tismir",
        "source_label": "TISMIR",
        "is_core_corpus": True,
        "authors": ["R. Novak"],
        "topics": ["Evaluation", "Datasets", "Music Information Retrieval"],
        "cluster": "evaluation",
    },
    {
        "paper_id": "https://openalex.org/WF005",
        "title": "Bridge Signals Between Timbre Models and Generative Audio",
        "abstract": (
            "A fixture bridge candidate connecting timbre representation learning "
            "to controllable neural audio generation."
        ),
        "venue": "JAES",
        "year": 2025,
        "citation_count": 3,
        "source_slug": "jaes",
        "source_label": "JAES",
        "is_core_corpus": True,
        "authors": ["E. Brooks", "K. Tan"],
        "topics": ["Timbre", "Generative Audio", "Representation Learning"],
        "cluster": "audio-generation",
    },
    {
        "paper_id": "https://openalex.org/WF006",
        "title": "Topic Growth Signals for Audio Research Discovery",
        "abstract": (
            "A fixture methods paper that illustrates citation velocity, topic "
            "growth, and explainable ranking diagnostics for audio research discovery."
        ),
        "venue": "TISMIR",
        "year": 2023,
        "citation_count": 6,
        "source_slug": "tismir",
        "source_label": "TISMIR",
        "is_core_corpus": True,
        "authors": ["H. Ito", "D. Morgan"],
        "topics": ["Topic Modeling", "Research Discovery", "Evaluation"],
        "cluster": "evaluation",
    },
]

RANKINGS: dict[str, list[dict[str, Any]]] = {
    "emerging": [
        {
            "paper_id": "https://openalex.org/WF001",
            "semantic": 0.84,
            "citation_velocity": 0.91,
            "topic_growth": 0.82,
            "bridge": 0.28,
            "diversity_penalty": 0.04,
            "final_score": 0.872,
            "reason_short": "Fixture emerging: strong audio-embedding fit and recent momentum.",
            "bridge_eligible": None,
        },
        {
            "paper_id": "https://openalex.org/WF003",
            "semantic": 0.76,
            "citation_velocity": 0.88,
            "topic_growth": 0.79,
            "bridge": 0.36,
            "diversity_penalty": 0.02,
            "final_score": 0.822,
            "reason_short": "Fixture emerging: recent multimodal audio work with topic growth.",
            "bridge_eligible": None,
        },
        {
            "paper_id": "https://openalex.org/WF002",
            "semantic": 0.68,
            "citation_velocity": 0.74,
            "topic_growth": 0.86,
            "bridge": 0.47,
            "diversity_penalty": 0.07,
            "final_score": 0.769,
            "reason_short": "Fixture emerging: sound-design generation topic is moving quickly.",
            "bridge_eligible": None,
        },
    ],
    "bridge": [
        {
            "paper_id": "https://openalex.org/WF005",
            "semantic": 0.62,
            "citation_velocity": 0.72,
            "topic_growth": 0.81,
            "bridge": 0.89,
            "diversity_penalty": 0.03,
            "final_score": 0.825,
            "reason_short": "Fixture bridge: links timbre representation and generative audio.",
            "bridge_eligible": True,
        },
        {
            "paper_id": "https://openalex.org/WF002",
            "semantic": 0.68,
            "citation_velocity": 0.74,
            "topic_growth": 0.86,
            "bridge": 0.72,
            "diversity_penalty": 0.06,
            "final_score": 0.756,
            "reason_short": "Fixture bridge: generation paper shares retrieval-style controls.",
            "bridge_eligible": True,
        },
        {
            "paper_id": "https://openalex.org/WF006",
            "semantic": 0.55,
            "citation_velocity": 0.51,
            "topic_growth": 0.65,
            "bridge": 0.38,
            "diversity_penalty": 0.12,
            "final_score": 0.531,
            "reason_short": "Fixture bridge: methods paper is diagnostic, not a strong bridge.",
            "bridge_eligible": False,
        },
    ],
    "undercited": [
        {
            "paper_id": "https://openalex.org/WF003",
            "semantic": 0.76,
            "citation_velocity": 0.88,
            "topic_growth": 0.79,
            "bridge": 0.36,
            "diversity_penalty": 0.01,
            "final_score": 0.811,
            "reason_short": "Fixture under-cited: new, relevant, and still uncited.",
            "bridge_eligible": None,
        },
        {
            "paper_id": "https://openalex.org/WF002",
            "semantic": 0.68,
            "citation_velocity": 0.74,
            "topic_growth": 0.86,
            "bridge": 0.47,
            "diversity_penalty": 0.03,
            "final_score": 0.796,
            "reason_short": "Fixture under-cited: low citation count in a growing topic.",
            "bridge_eligible": None,
        },
        {
            "paper_id": "https://openalex.org/WF004",
            "semantic": 0.58,
            "citation_velocity": 0.52,
            "topic_growth": 0.61,
            "bridge": 0.14,
            "diversity_penalty": 0.08,
            "final_score": 0.555,
            "reason_short": "Fixture under-cited: useful evaluation work with modest attention.",
            "bridge_eligible": None,
        },
    ],
}


def fixture_mode_enabled() -> bool:
    return os.environ.get(DATA_MODE_ENV, "").strip().lower() in {"fixture", "fixtures", "demo"}


def fixture_readiness() -> ReadinessResponse:
    family_counts = {family: len(rows) for family, rows in RANKINGS.items()}
    return ReadinessResponse(
        status="ok",
        database="fixture-data",
        timestamp=utc_now(),
        active_release=ActivePublicReleaseMeta(
            promotion_id=0,
            ranking_run_id=RANKING_RUN_ID,
            ranking_version=RANKING_VERSION,
            corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
            embedding_version=EMBEDDING_VERSION,
            scorer=PublicReleaseScorerMeta(
                kind="materialized_paper_scores",
                version=RANKING_RUN_ID,
            ),
            promoted_at=utc_now(),
        ),
        release_diagnostics=PublicReleaseReadinessDiagnostics(
            serveable=True,
            membership_count=len(FIXTURE_PAPERS),
            embedding_count=len(FIXTURE_PAPERS),
            missing_embedding_count=0,
            family_score_counts=family_counts,
            expected_family_score_counts=family_counts,
            out_of_membership_score_count=0,
            cluster_version=CLUSTER_VERSION,
            cluster_assignment_count=len(FIXTURE_PAPERS),
            missing_cluster_assignment_count=0,
            out_of_membership_cluster_count=0,
            failures=[],
        ),
    )


def fixture_product_summary() -> ProductSummary:
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
        ranking_metadata_note=(
            f"{PRODUCT_RANKING_METADATA_NOTE} Fixture mode is active: responses are "
            "curated demo data, not live Postgres ranking data."
        ),
        materialized_ranking=MaterializedRankingMeta(
            ranking_run_id=RANKING_RUN_ID,
            ranking_version=RANKING_VERSION,
            corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
            embedding_version=EMBEDDING_VERSION,
            config_json=RUN_CONFIG,
        ),
        active_release=ActivePublicReleaseMeta(
            promotion_id=0,
            ranking_run_id=RANKING_RUN_ID,
            ranking_version=RANKING_VERSION,
            corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
            embedding_version=EMBEDDING_VERSION,
            scorer=PublicReleaseScorerMeta(
                kind="materialized_paper_scores",
                version=RANKING_RUN_ID,
            ),
            promoted_at=utc_now(),
        ),
    )


def _normalize_paper_id(paper_id: str) -> str:
    return unquote(paper_id)


def _paper_by_id(paper_id: str) -> dict[str, Any] | None:
    normalized = _normalize_paper_id(paper_id)
    for paper in FIXTURE_PAPERS:
        if paper["paper_id"] == normalized or paper["paper_id"].endswith(f"/{normalized}"):
            return paper
    return None


def _matches_query(paper: dict[str, Any], q: str | None) -> bool:
    if not q:
        return True
    terms = [term.casefold() for term in q.split() if term.strip()]
    haystack = " ".join(
        [
            paper["title"],
            paper["abstract"],
            " ".join(paper["topics"]),
            paper["source_slug"],
        ]
    ).casefold()
    return all(term in haystack for term in terms)


def _paper_list_item(paper: dict[str, Any]) -> PaperListItem:
    return PaperListItem(
        paper_id=paper["paper_id"],
        title=paper["title"],
        year=paper["year"],
        citation_count=paper["citation_count"],
        source_slug=paper["source_slug"],
        source_label=paper["source_label"],
        is_core_corpus=paper["is_core_corpus"],
        topics=list(paper["topics"]),
    )


def fixture_papers(*, q: str | None, limit: int) -> PaperListResponse:
    papers = [paper for paper in FIXTURE_PAPERS if _matches_query(paper, q)]
    papers.sort(key=lambda p: (-int(p["year"]), int(p["citation_count"]), p["paper_id"]))
    return PaperListResponse(total=len(papers[:limit]), items=[_paper_list_item(p) for p in papers[:limit]])


def fixture_paper_detail(paper_id: str) -> PaperDetail | None:
    paper = _paper_by_id(paper_id)
    if paper is None:
        return None
    return PaperDetail(
        paper_id=paper["paper_id"],
        title=paper["title"],
        abstract=paper["abstract"],
        venue=paper["venue"],
        year=paper["year"],
        citation_count=paper["citation_count"],
        source_slug=paper["source_slug"],
        is_core_corpus=paper["is_core_corpus"],
        authors=list(paper["authors"]),
        topics=list(paper["topics"]),
    )


def _search_rank(paper: dict[str, Any], q: str) -> float:
    terms = [term.casefold() for term in q.split() if term.strip()]
    fields = {
        "title": paper["title"],
        "abstract": paper["abstract"],
        "topics": " ".join(paper["topics"]),
    }
    score = 0.0
    for term in terms:
        if term in fields["title"].casefold():
            score += 0.55
        if term in fields["abstract"].casefold():
            score += 0.30
        if term in fields["topics"].casefold():
            score += 0.15
    return round(min(score, 1.0), 6)


def fixture_search(
    *,
    q: str,
    limit: int,
    offset: int,
    year_from: int | None,
    year_to: int | None,
    included_scope: Literal["core", "all_included"],
    source_slug: str | None,
    topic: str | None,
    family_hint: Literal["emerging", "bridge", "undercited"] | None,
    ranking_run_id: str | None,
    ranking_version: str | None,
) -> SearchResponse:
    papers = [p for p in FIXTURE_PAPERS if _matches_query(p, q)]
    if year_from is not None:
        papers = [p for p in papers if int(p["year"]) >= year_from]
    if year_to is not None:
        papers = [p for p in papers if int(p["year"]) <= year_to]
    if included_scope == "core":
        papers = [p for p in papers if bool(p["is_core_corpus"])]
    if source_slug is not None:
        papers = [p for p in papers if p["source_slug"] == source_slug]
    if topic is not None:
        topic_folded = topic.casefold()
        papers = [p for p in papers if any(topic_folded in t.casefold() for t in p["topics"])]

    papers.sort(key=lambda p: (-_search_rank(p, q), -int(p["year"]), -int(p["citation_count"]), p["paper_id"]))
    selected = papers[offset : offset + limit]
    use_ranking_context = family_hint is not None

    return SearchResponse(
        total=len(papers),
        ordering="fixture lexical score desc, year desc, citation_count desc, paper_id asc",
        resolved_filters=SearchResolvedFilters(
            q=q,
            limit=limit,
            offset=offset,
            year_from=year_from,
            year_to=year_to,
            included_scope=included_scope,
            source_slug=source_slug,
            topic=topic,
            family_hint=family_hint,
            ranking_run_id=ranking_run_id if use_ranking_context else None,
            ranking_version=ranking_version if use_ranking_context else None,
        ),
        items=[
            SearchResultItem(
                paper_id=p["paper_id"],
                title=p["title"],
                year=p["year"],
                citation_count=p["citation_count"],
                source_slug=p["source_slug"],
                source_label=p["source_label"],
                is_core_corpus=p["is_core_corpus"],
                topics=list(p["topics"]),
                preview=p["abstract"],
                match=SearchMatchMetadata(
                    matched_fields=["title", "abstract", "topics"],
                    highlight_fragments=[p["title"]],
                    lexical_rank=_search_rank(p, q),
                ),
            )
            for p in selected
        ],
        resolved_ranking_run_id=RANKING_RUN_ID if use_ranking_context else None,
        resolved_ranking_version=RANKING_VERSION if use_ranking_context else None,
        resolved_corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION if use_ranking_context else None,
    )


def _ranked_item(family: str, row: dict[str, Any]) -> RankedRecommendationItem:
    paper = _paper_by_id(row["paper_id"])
    if paper is None:
        raise ValueError(f"Fixture ranking references missing paper: {row['paper_id']}")
    weights = family_weights_from_config(RUN_CONFIG, family)
    explanations = build_signal_explanations(
        family=family,
        semantic=row["semantic"],
        citation_velocity=row["citation_velocity"],
        topic_growth=row["topic_growth"],
        bridge=row["bridge"],
        diversity_penalty=row["diversity_penalty"],
        weights=weights,
    )
    return RankedRecommendationItem(
        paper_id=paper["paper_id"],
        title=paper["title"],
        year=paper["year"],
        citation_count=paper["citation_count"],
        source_slug=paper["source_slug"],
        topics=list(paper["topics"]),
        signals=RankedSignals(
            semantic=row["semantic"],
            citation_velocity=row["citation_velocity"],
            topic_growth=row["topic_growth"],
            bridge=row["bridge"],
            diversity_penalty=row["diversity_penalty"],
        ),
        final_score=row["final_score"],
        reason_short=row["reason_short"],
        signal_explanations=[RankedSignalExplanation(**x) for x in explanations],
        bridge_eligible=row["bridge_eligible"],
    )


def fixture_ranked_recommendations(
    *,
    family: Literal["emerging", "bridge", "undercited"],
    limit: int,
    bridge_eligible_only: bool,
) -> RankedRecommendationsResponse:
    rows = list(RANKINGS[family])
    if family == "bridge" and bridge_eligible_only:
        rows = [row for row in rows if row["bridge_eligible"] is True]
    rows = rows[:limit]
    weights = family_weights_from_config(RUN_CONFIG, family)
    return RankedRecommendationsResponse(
        ranking_run_id=RANKING_RUN_ID,
        ranking_version=RANKING_VERSION,
        corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
        family=family,
        total=len(rows),
        list_explanation=RankedListExplanation(**build_list_ranking_explanation(family=family, weights=weights)),
        items=[_ranked_item(family, row) for row in rows],
    )


def fixture_undercited_recommendations(
    *,
    limit: int,
    min_year: int,
    max_citations: int,
) -> UndercitedRecommendationsResponse:
    rows = []
    for row in RANKINGS["undercited"]:
        paper = _paper_by_id(row["paper_id"])
        if paper and int(paper["year"]) >= min_year and int(paper["citation_count"]) <= max_citations:
            rows.append(
                UndercitedRecommendationItem(
                    paper_id=paper["paper_id"],
                    title=paper["title"],
                    year=paper["year"],
                    citation_count=paper["citation_count"],
                    source_slug=paper["source_slug"],
                    reason=row["reason_short"],
                    signal_breakdown={
                        "citation_velocity": row["citation_velocity"],
                        "topic_growth": row["topic_growth"],
                    },
                )
            )
    return UndercitedRecommendationsResponse(
        heuristic_label="fixture-undercited-core-recent-v0",
        heuristic_version="fixture-v0",
        description=(
            "Fixture low-cite listing for no-key local demos. This is not live ranking data "
            "or a reviewer-labeled benchmark."
        ),
        total=len(rows[:limit]),
        items=rows[:limit],
    )


def _evaluation_item_from_ranked(item: RankedRecommendationItem) -> EvaluationPaperItem:
    return EvaluationPaperItem(
        paper_id=item.paper_id,
        title=item.title,
        year=item.year,
        citation_count=item.citation_count,
        source_slug=item.source_slug,
        topics=item.topics,
        final_score=item.final_score,
    )


def _arm_stats(
    items: list[EvaluationPaperItem],
    *,
    label: str,
    description: str,
    ordering: str,
) -> EvaluationListArmResponse:
    years = [item.year for item in items] or [0]
    citations = [item.citation_count for item in items] or [0]
    topic_counts: dict[str, int] = {}
    for item in items:
        for topic in item.topics:
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
    top_topics = [
        topic for topic, _count in sorted(topic_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    ]
    return EvaluationListArmResponse(
        arm_label=label,
        arm_description=description,
        ordering_description=ordering,
        items=items,
        recency=EvaluationRecencyProxy(
            mean_year=round(sum(years) / len(years), 2),
            min_year=min(years),
            max_year=max(years),
            share_in_latest_two_years=round(sum(1 for y in years if y >= 2025) / len(years), 4),
        ),
        citations=EvaluationCitationProxy(
            mean=round(sum(citations) / len(citations), 2),
            median=float(median(citations)),
            min_val=min(citations),
            max_val=max(citations),
        ),
        topics=EvaluationTopicMixProxy(unique_topic_labels=len(topic_counts), top_topics=top_topics),
    )


def _topic_set(items: list[EvaluationPaperItem]) -> set[str]:
    return {topic for item in items for topic in item.topics}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    return round(len(a & b) / len(a | b), 4)


def fixture_evaluation_compare(
    *,
    family: Literal["emerging", "bridge", "undercited"],
    limit: int,
) -> EvaluationCompareResponse:
    ranked_items = [_evaluation_item_from_ranked(item) for item in fixture_ranked_recommendations(
        family=family,
        limit=limit,
        bridge_eligible_only=False,
    ).items]
    pool = list(ranked_items)
    for paper in FIXTURE_PAPERS:
        if paper["paper_id"] not in {item.paper_id for item in pool}:
            pool.append(
                EvaluationPaperItem(
                    paper_id=paper["paper_id"],
                    title=paper["title"],
                    year=paper["year"],
                    citation_count=paper["citation_count"],
                    source_slug=paper["source_slug"],
                    topics=list(paper["topics"]),
                    final_score=None,
                )
            )
    citation_items = sorted(pool, key=lambda p: (-p.citation_count, -p.year, p.paper_id))[:limit]
    date_items = sorted(pool, key=lambda p: (-p.year, p.citation_count, p.paper_id))[:limit]

    ranked_arm = _arm_stats(
        ranked_items,
        label="ranked_family",
        description="Fixture ranked list using the same response shape as the materialized run API.",
        ordering="fixture final_score DESC",
    )
    citation_arm = _arm_stats(
        citation_items,
        label="citation_baseline",
        description="Fixture citation-count baseline over the same tiny demo pool.",
        ordering="citation_count DESC, year DESC",
    )
    date_arm = _arm_stats(
        date_items,
        label="date_baseline",
        description="Fixture recency baseline over the same tiny demo pool.",
        ordering="year DESC, citation_count ASC",
    )
    return EvaluationCompareResponse(
        disclaimer=EvaluationDisclaimer(
            headline=(
                "Fixture outputs are local demo aids, not evidence of paper usefulness "
                "or live Research Radar ranking results."
            ),
            bullets=[
                "Use fixture mode to inspect API and UI behavior without Postgres, pgvector, OpenAlex, or OpenAI.",
                "The same endpoint contracts are exercised, but values come from a tiny curated demo corpus.",
                "For real rankings, run the default Postgres-backed path documented in the README.",
            ],
        ),
        ranking_run_id=RANKING_RUN_ID,
        ranking_version=RANKING_VERSION,
        corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
        embedding_version=EMBEDDING_VERSION,
        family=family,
        pool_definition="Tiny checked-in fixture corpus for no-key local demos.",
        pool_size=len(FIXTURE_PAPERS),
        low_cite_min_year=2019 if family == "undercited" else None,
        low_cite_max_citations=30 if family == "undercited" else None,
        candidate_pool_doc_revision="fixture-demo-v0" if family == "undercited" else None,
        topic_overlap_note=(
            "Fixture Jaccard overlap on topic labels; useful for UI inspection only."
        ),
        ranked=ranked_arm,
        citation_baseline=citation_arm,
        date_baseline=date_arm,
        topic_overlap=EvaluationTopicOverlap(
            jaccard_ranked_vs_citation_baseline=_jaccard(_topic_set(ranked_items), _topic_set(citation_items)),
            jaccard_ranked_vs_date_baseline=_jaccard(_topic_set(ranked_items), _topic_set(date_items)),
            jaccard_citation_vs_date_baseline=_jaccard(_topic_set(citation_items), _topic_set(date_items)),
        ),
        generated_at=utc_now(),
    )


def fixture_bridge_distinctness(*, k: int) -> BridgeDistinctnessResponse:
    full_bridge = [row["paper_id"] for row in RANKINGS["bridge"]][:k]
    eligible_bridge = [row["paper_id"] for row in RANKINGS["bridge"] if row["bridge_eligible"] is True][:k]
    emerging = [row["paper_id"] for row in RANKINGS["emerging"]][:k]

    def overlap(a: list[str], b: list[str]) -> BridgeDistinctnessOverlapMetrics:
        sa, sb = set(a), set(b)
        return BridgeDistinctnessOverlapMetrics(
            overlap_count=len(sa & sb),
            jaccard=_jaccard(sa, sb),
        )

    return BridgeDistinctnessResponse(
        ranking_run_id=RANKING_RUN_ID,
        ranking_version=RANKING_VERSION,
        corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
        embedding_version=EMBEDDING_VERSION,
        cluster_version=CLUSTER_VERSION,
        k=k,
        full_bridge_top_k_ids=full_bridge,
        eligible_bridge_top_k_ids=eligible_bridge,
        emerging_top_k_ids=emerging,
        full_bridge_vs_eligible_bridge=overlap(full_bridge, eligible_bridge),
        full_bridge_vs_emerging=overlap(full_bridge, emerging),
        eligible_bridge_vs_emerging=overlap(eligible_bridge, emerging),
        bridge_family_row_count=len(RANKINGS["bridge"]),
        bridge_score_nonnull_count=len(RANKINGS["bridge"]),
        bridge_score_null_count=0,
        bridge_eligible_true_count=sum(1 for row in RANKINGS["bridge"] if row["bridge_eligible"] is True),
        bridge_eligible_false_count=sum(1 for row in RANKINGS["bridge"] if row["bridge_eligible"] is False),
        bridge_eligible_null_count=sum(1 for row in RANKINGS["bridge"] if row["bridge_eligible"] is None),
        bridge_signal_json_present_count=len(RANKINGS["bridge"]),
        bridge_signal_json_missing_count=0,
        decision_support=BridgeDistinctnessDecisionSupport(
            eligible_head_differs_from_full=full_bridge[: len(eligible_bridge)] != eligible_bridge,
            eligible_head_less_emerging_like_than_full=False,
            suggested_next_step="candidate_for_small_weight_experiment",
        ),
        generated_at=utc_now(),
    )


def fixture_topic_trends(
    *,
    limit: int,
    since_year: int,
    min_works: int,
) -> TopicTrendsResponse:
    counts: dict[str, dict[str, int]] = {}
    for paper in FIXTURE_PAPERS:
        for topic in paper["topics"]:
            bucket = counts.setdefault(topic, {"total": 0, "recent": 0, "prior": 0})
            bucket["total"] += 1
            if int(paper["year"]) >= since_year:
                bucket["recent"] += 1
            else:
                bucket["prior"] += 1
    rows = [
        (topic, data)
        for topic, data in counts.items()
        if data["total"] >= min_works
    ]
    rows.sort(key=lambda kv: (-(kv[1]["recent"] - kv[1]["prior"]), -kv[1]["total"], kv[0]))
    return TopicTrendsResponse(
        corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
        since_year=since_year,
        min_works=min_works,
        total=len(rows[:limit]),
        items=[
            TopicTrendItem(
                topic_id=index + 1,
                topic_name=topic,
                total_works=data["total"],
                recent_works=data["recent"],
                prior_works=data["prior"],
                delta=data["recent"] - data["prior"],
                growth_label="up" if data["recent"] > data["prior"] else "flat",
            )
            for index, (topic, data) in enumerate(rows[:limit])
        ],
        generated_at=utc_now(),
    )


def fixture_cluster_inspection(*, sample_per_cluster: int) -> ClusterInspectionResponse:
    groups: dict[str, list[dict[str, Any]]] = {}
    for paper in FIXTURE_PAPERS:
        groups.setdefault(paper["cluster"], []).append(paper)
    return ClusterInspectionResponse(
        cluster_version=CLUSTER_VERSION,
        embedding_version=EMBEDDING_VERSION,
        corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
        algorithm="fixture-topic-groups",
        status="succeeded",
        clustering_metric="fixture topic grouping",
        metric_note="Fixture groups are hand-curated for local demos; no vectors are loaded.",
        groups=[
            ClusterGroupItem(
                cluster_id=cluster_id,
                work_count=len(papers),
                sample_papers=[
                    ClusterSamplePaperItem(paper_id=p["paper_id"], title=p["title"])
                    for p in papers[:sample_per_cluster]
                ],
            )
            for cluster_id, papers in sorted(groups.items())
        ],
        generated_at=utc_now(),
    )


def fixture_similar_papers(
    *,
    paper_id: str,
    embedding_version: str,
    limit: int,
) -> SimilarPapersResponse | None:
    paper = _paper_by_id(paper_id)
    if paper is None:
        return None
    source_topics = set(paper["topics"])
    candidates: list[SimilarPaperItem] = []
    for other in FIXTURE_PAPERS:
        if other["paper_id"] == paper["paper_id"]:
            continue
        other_topics = set(other["topics"])
        similarity = 0.35 + 0.55 * _jaccard(source_topics, other_topics)
        candidates.append(
            SimilarPaperItem(
                paper_id=other["paper_id"],
                title=other["title"],
                year=other["year"],
                citation_count=other["citation_count"],
                source_slug=other["source_slug"],
                topics=list(other["topics"]),
                similarity=round(similarity, 4),
            )
        )
    candidates.sort(key=lambda item: (-item.similarity, -item.year, item.paper_id))
    return SimilarPapersResponse(
        paper_id=paper["paper_id"],
        embedding_version=embedding_version or EMBEDDING_VERSION,
        total=len(candidates[:limit]),
        items=candidates[:limit],
    )


def fixture_paper_ranking(
    *,
    paper_id: str,
    top_n: int,
) -> PaperRankingResponse | None:
    paper = _paper_by_id(paper_id)
    if paper is None:
        return None
    families: list[PaperRankingFamilyItem] = []
    for family in ("emerging", "bridge", "undercited"):
        rows = RANKINGS[family]
        match = next((row for row in rows if row["paper_id"] == paper["paper_id"]), None)
        if match is None:
            families.append(PaperRankingFamilyItem(family=family, present=False, in_top_n=False))
            continue
        rank = rows.index(match) + 1
        item = _ranked_item(family, match)
        families.append(
            PaperRankingFamilyItem(
                family=family,
                present=True,
                in_top_n=rank <= top_n,
                rank=rank if rank <= top_n else None,
                final_score=item.final_score,
                reason_short=item.reason_short,
                signals=item.signals,
                signal_explanations=item.signal_explanations,
                bridge_eligible=item.bridge_eligible,
            )
        )
    return PaperRankingResponse(
        paper_id=paper["paper_id"],
        ranking_run_id=RANKING_RUN_ID,
        ranking_version=RANKING_VERSION,
        corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
        top_n=top_n,
        rank_scope="fixture_family_global",
        families=families,
    )

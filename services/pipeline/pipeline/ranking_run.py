from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from math import log1p
from typing import Any

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import RankingCandidate, PaperScoreRow, ScoreWeights, final_score_partial
from pipeline.ranking_persistence import (
    insert_ranking_run_started,
    latest_corpus_snapshot_version_with_works,
    list_ranking_candidates,
    update_ranking_run_final,
    upsert_paper_scores,
)

RECOMMENDATION_FAMILIES: tuple[str, ...] = ("emerging", "bridge", "undercited")

EMERGING_REASON = (
    "Recent paper with citation momentum in active topics; semantic and bridge not yet modeled."
)
BRIDGE_REASON = (
    "Multi-topic paper in active topics; explicit bridge score not yet modeled."
)
UNDERCITED_REASON = (
    "Recent paper in active topics with a popularity penalty; semantic and bridge not yet modeled."
)

FAMILY_WEIGHTS: dict[str, ScoreWeights] = {
    "emerging": ScoreWeights(
        semantic=0.0,
        citation_velocity=0.60,
        topic_growth=0.40,
        bridge=0.0,
        diversity_penalty=0.05,
    ),
    "bridge": ScoreWeights(
        semantic=0.0,
        citation_velocity=0.35,
        topic_growth=0.65,
        bridge=0.0,
        diversity_penalty=0.20,
    ),
    "undercited": ScoreWeights(
        semantic=0.0,
        citation_velocity=0.30,
        topic_growth=0.70,
        bridge=0.0,
        diversity_penalty=0.25,
    ),
}


def _build_ranking_config(
    *,
    corpus_snapshot_version: str,
    placeholder_policy: str,
) -> dict[str, Any]:
    return {
        "default_weights": asdict(ScoreWeights()),
        "family_weights": {family: asdict(weights) for family, weights in FAMILY_WEIGHTS.items()},
        "families_written": list(RECOMMENDATION_FAMILIES),
        "placeholder_policy": placeholder_policy,
        "selection_scope": {
            "type": "included_works",
            "corpus_snapshot_version": corpus_snapshot_version,
        },
        "signal_policies": {
            "semantic_score": "null_until_embeddings",
            "citation_velocity_score": "citations_per_year_normalized_within_run",
            "topic_growth_score": "mean_recent_topic_share_within_snapshot",
            "bridge_score": "null_until_clusters_or_neighbor_features",
            "diversity_penalty": {
                "emerging": "0.0_for_step3_v0",
                "bridge": "lack_of_topic_breadth_penalty",
                "undercited": "citation_popularity_penalty",
            },
        },
        "thresholds": {
            "recent_topic_year_window": 2,
        },
    }


def _clamp_01(value: float) -> float:
    if value <= 0:
        return 0.0
    if value >= 1:
        return 1.0
    return value


def _round_score(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _citation_velocity_scores(candidates: list[RankingCandidate]) -> dict[int, float]:
    if not candidates:
        return {}
    current_year = max(candidate.year for candidate in candidates)
    raw_by_work = {
        candidate.work_id: float(candidate.citation_count) / float(max(1, current_year - candidate.year + 1))
        for candidate in candidates
    }
    max_raw = max(raw_by_work.values(), default=0.0)
    if max_raw <= 0:
        return {candidate.work_id: 0.0 for candidate in candidates}
    return {work_id: _round_score(raw / max_raw) or 0.0 for work_id, raw in raw_by_work.items()}


def _topic_growth_scores(candidates: list[RankingCandidate]) -> dict[int, float]:
    if not candidates:
        return {}
    current_year = max(candidate.year for candidate in candidates)
    recent_cutoff = max(current_year - 1, min(candidate.year for candidate in candidates))
    total_counts: Counter[int] = Counter()
    recent_counts: Counter[int] = Counter()

    for candidate in candidates:
        unique_topic_ids = tuple(dict.fromkeys(candidate.topic_ids))
        for topic_id in unique_topic_ids:
            total_counts[topic_id] += 1
            if candidate.year >= recent_cutoff:
                recent_counts[topic_id] += 1

    topic_growth_by_work: dict[int, float] = {}
    for candidate in candidates:
        unique_topic_ids = tuple(dict.fromkeys(candidate.topic_ids))
        if not unique_topic_ids:
            topic_growth_by_work[candidate.work_id] = 0.0
            continue
        ratios = [recent_counts[topic_id] / total_counts[topic_id] for topic_id in unique_topic_ids if total_counts[topic_id] > 0]
        topic_growth_by_work[candidate.work_id] = _round_score(sum(ratios) / len(ratios)) or 0.0
    return topic_growth_by_work


def _topic_breadth_penalties(candidates: list[RankingCandidate]) -> dict[int, float]:
    if not candidates:
        return {}
    max_topic_count = max((len(set(candidate.topic_ids)) for candidate in candidates), default=0)
    if max_topic_count <= 0:
        return {candidate.work_id: 1.0 for candidate in candidates}
    penalties: dict[int, float] = {}
    for candidate in candidates:
        breadth = len(set(candidate.topic_ids)) / float(max_topic_count)
        penalties[candidate.work_id] = _round_score(1.0 - _clamp_01(breadth)) or 0.0
    return penalties


def _citation_popularity_penalties(candidates: list[RankingCandidate]) -> dict[int, float]:
    if not candidates:
        return {}
    max_raw = max((log1p(candidate.citation_count) for candidate in candidates), default=0.0)
    if max_raw <= 0:
        return {candidate.work_id: 0.0 for candidate in candidates}
    return {
        candidate.work_id: _round_score(log1p(candidate.citation_count) / max_raw) or 0.0
        for candidate in candidates
    }


def _make_score_row(
    *,
    work_id: int,
    family: str,
    citation_velocity: float,
    topic_growth: float,
    diversity_penalty: float,
    reason_short: str,
) -> PaperScoreRow:
    final = final_score_partial(
        semantic=None,
        citation_velocity=citation_velocity,
        topic_growth=topic_growth,
        bridge=None,
        diversity_penalty=diversity_penalty,
        weights=FAMILY_WEIGHTS[family],
    )
    return PaperScoreRow(
        work_id=work_id,
        recommendation_family=family,
        semantic_score=None,
        citation_velocity_score=_round_score(citation_velocity),
        topic_growth_score=_round_score(topic_growth),
        bridge_score=None,
        diversity_penalty=_round_score(diversity_penalty),
        final_score=_round_score(final) or 0.0,
        reason_short=reason_short,
    )


def build_step3_heuristic_score_rows(candidates: list[RankingCandidate]) -> list[PaperScoreRow]:
    """
    Build one heuristic row per (work, family) using available metadata only.
    semantic_score and bridge_score remain null until embeddings/clusters exist.
    """
    citation_velocity_by_work = _citation_velocity_scores(candidates)
    topic_growth_by_work = _topic_growth_scores(candidates)
    topic_breadth_penalty_by_work = _topic_breadth_penalties(candidates)
    citation_popularity_penalty_by_work = _citation_popularity_penalties(candidates)

    rows: list[PaperScoreRow] = []
    for candidate in candidates:
        citation_velocity = citation_velocity_by_work[candidate.work_id]
        topic_growth = topic_growth_by_work[candidate.work_id]

        rows.append(
            _make_score_row(
                work_id=candidate.work_id,
                family="emerging",
                citation_velocity=citation_velocity,
                topic_growth=topic_growth,
                diversity_penalty=0.0,
                reason_short=EMERGING_REASON,
            )
        )
        rows.append(
            _make_score_row(
                work_id=candidate.work_id,
                family="bridge",
                citation_velocity=citation_velocity,
                topic_growth=topic_growth,
                diversity_penalty=topic_breadth_penalty_by_work[candidate.work_id],
                reason_short=BRIDGE_REASON,
            )
        )
        rows.append(
            _make_score_row(
                work_id=candidate.work_id,
                family="undercited",
                citation_velocity=citation_velocity,
                topic_growth=topic_growth,
                diversity_penalty=citation_popularity_penalty_by_work[candidate.work_id],
                reason_short=UNDERCITED_REASON,
            )
        )
    return rows


def _ranking_counts_from_rows(candidate_count: int, rows: list[PaperScoreRow]) -> RankingCounts:
    by_family: dict[str, int] = {f: 0 for f in RECOMMENDATION_FAMILIES}
    null_sem = 0
    null_bridge = 0
    for r in rows:
        by_family[r.recommendation_family] = by_family.get(r.recommendation_family, 0) + 1
        if r.semantic_score is None:
            null_sem += 1
        if r.bridge_score is None:
            null_bridge += 1
    return RankingCounts(
        total_candidate_works=candidate_count,
        total_rows_written=len(rows),
        rows_by_family=by_family,
        rows_null_semantic=null_sem,
        rows_null_bridge=null_bridge,
    )


def execute_ranking_run(
    *,
    database_url: str | None = None,
    ranking_version: str,
    corpus_snapshot_version: str | None = None,
    embedding_version: str = "none-v0",
    note: str | None = None,
    placeholder_policy: str = "semantic_and_bridge_null_until_embeddings_and_clusters",
) -> RankingRun:
    """
    Resolve snapshot, persist a running row (committed), write scores, finalize succeeded.
    On failure after the run row exists, set status failed and re-raise.
    """
    dsn = database_url or database_url_from_env()

    with psycopg.connect(dsn, autocommit=False) as conn:
        snapshot = corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
        if snapshot is None:
            raise RuntimeError(
                "No corpus_snapshot_version with included works found. "
                "Pass --corpus-snapshot-version or ingest data first."
            )

        config = _build_ranking_config(
            corpus_snapshot_version=snapshot,
            placeholder_policy=placeholder_policy,
        )
        run = RankingRun.start(
            ranking_version=ranking_version,
            corpus_snapshot_version=snapshot,
            embedding_version=embedding_version,
            config=config,
            notes=note,
        )
        insert_ranking_run_started(conn, run)
        conn.commit()

    try:
        with psycopg.connect(dsn, autocommit=False) as conn:
            candidates = list_ranking_candidates(conn, snapshot)
            if not candidates:
                raise RuntimeError(
                    f"No included works for corpus_snapshot_version={snapshot!r}; nothing to rank."
                )
            score_rows = build_step3_heuristic_score_rows(candidates)
            upsert_paper_scores(conn, run.ranking_run_id, score_rows)
            counts = _ranking_counts_from_rows(len(candidates), score_rows)
            update_ranking_run_final(conn, run.ranking_run_id, "succeeded", counts, None)
            conn.commit()
        return run.complete(counts)
    except Exception as exc:
        with psycopg.connect(dsn, autocommit=False) as conn2:
            update_ranking_run_final(conn2, run.ranking_run_id, "failed", None, str(exc))
            conn2.commit()
        raise
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from math import log1p
from typing import Any

import sys

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.embedding_persistence import count_missing_embedding_candidates
from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import (
    DEFAULT_LOW_CITE_MAX_CITATIONS,
    DEFAULT_LOW_CITE_MIN_YEAR,
    LOW_CITE_CANDIDATE_POOL_DOC,
    LOW_CITE_CANDIDATE_POOL_REVISION,
    RankingCandidate,
    PaperScoreRow,
    ScoreWeights,
    final_score_partial,
    in_low_cite_candidate_pool,
)
from pipeline.bridge_neighbor_mix import (
    NEIGHBOR_MIX_V1_DEFAULT_K,
    NeighborMixV1Result,
    compute_neighbor_mix_v1_by_work,
    neighbor_mix_v1_json_payload,
)
from pipeline.clustering import compute_bridge_boundary_scores
from pipeline.clustering_persistence import (
    list_clustering_inputs,
    load_cluster_assignments,
    require_successful_clustering_run,
)
from pipeline.ranking_persistence import (
    insert_ranking_run_started,
    latest_corpus_snapshot_version_with_works,
    list_ranking_candidates,
    update_ranking_run_final,
    upsert_paper_scores,
)

RECOMMENDATION_FAMILIES: tuple[str, ...] = ("emerging", "bridge", "undercited")

# Upper bound for --bridge-weight-for-family-bridge (ML2-5b experiments); keeps fat-finger runs bounded.
MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY = 0.25


def warn_embedding_gaps_if_any(
    conn: psycopg.Connection,
    *,
    corpus_snapshot_version: str,
    embedding_version: str,
) -> None:
    """Stderr notice when a clustered ranking run may leave bridge rows without cluster-backed scores."""
    n_missing = count_missing_embedding_candidates(
        conn,
        corpus_snapshot_version=corpus_snapshot_version,
        embedding_version=embedding_version,
    )
    if n_missing > 0:
        print(
            "ranking-run: warning: "
            f"{n_missing} included work(s) have no embedding for {embedding_version!r} "
            "in this snapshot; cluster-backed bridge scores cannot be computed for those rows. "
            "Run `embed-works` without --limit for full coverage, then re-run `cluster-works`. "
            "Audit with `embedding-coverage --embedding-version ... --corpus-snapshot-version ... "
            "--fail-on-gaps`.",
            file=sys.stderr,
        )

EMERGING_REASON = (
    "Recent paper with citation momentum in active topics; semantic and bridge not yet modeled."
)
BRIDGE_REASON_LEGACY = (
    "Multi-topic paper in active topics; no cluster_version on this run so bridge_score was not computed."
)
BRIDGE_REASON_STRUCTURAL_ZERO_WEIGHT = (
    "Near a boundary between k-means cluster centroids in embedding space (squared L2 vs other centroids); "
    "bridge family weight is zero so final_score does not use this signal. Semantic score not used."
)
BRIDGE_REASON_STRUCTURAL_WEIGHTED = (
    "Near a boundary between k-means cluster centroids in embedding space (squared L2 vs other centroids); "
    "bridge signal contributes to final_score for the bridge recommendation family in this run. Semantic score not used."
)
BRIDGE_REASON_STRUCTURAL = BRIDGE_REASON_STRUCTURAL_ZERO_WEIGHT
BRIDGE_REASON_NO_CLUSTER = "No cluster-backed bridge signal available for this run."
UNDERCITED_REASON = (
    "Low-cite candidate pool (see docs/candidate-pool-low-cite.md v0): core corpus, recency floor, "
    "citation ceiling, title+abstract gate; popularity penalty among pool members only. "
    "Semantic and bridge not yet modeled."
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


def validate_bridge_weight_for_bridge_family(value: float) -> float:
    w = float(value)
    if w < 0.0 or w > MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY:
        raise ValueError(
            "bridge_weight_for_bridge_family must be in "
            f"[0.0, {MAX_BRIDGE_WEIGHT_FOR_BRIDGE_FAMILY}], got {w!r}"
        )
    return w


def resolved_family_weights(bridge_weight_for_bridge_family: float) -> dict[str, ScoreWeights]:
    """
    Per-run weight map: emerging and undercited match checked-in defaults; bridge family uses the
    resolved bridge weight (do not mutate FAMILY_WEIGHTS).
    """
    bw = float(bridge_weight_for_bridge_family)
    base_b = FAMILY_WEIGHTS["bridge"]
    bridge_row = ScoreWeights(
        semantic=base_b.semantic,
        citation_velocity=base_b.citation_velocity,
        topic_growth=base_b.topic_growth,
        bridge=bw,
        diversity_penalty=base_b.diversity_penalty,
    )
    return {
        "emerging": FAMILY_WEIGHTS["emerging"],
        "bridge": bridge_row,
        "undercited": FAMILY_WEIGHTS["undercited"],
    }


def _build_ranking_config(
    *,
    corpus_snapshot_version: str,
    placeholder_policy: str,
    low_cite_min_year: int,
    low_cite_max_citations: int,
    cluster_version: str | None,
    embedding_version: str,
    bridge_weight_for_bridge_family: float,
    family_weights_resolved: dict[str, ScoreWeights],
    neighbor_mix_k: int = NEIGHBOR_MIX_V1_DEFAULT_K,
) -> dict[str, Any]:
    bridge_policy = (
        "null_until_clusters_or_neighbor_features"
        if cluster_version is None
        else "cluster_boundary_ratio_v0"
    )
    bw = float(bridge_weight_for_bridge_family)
    bridge_reason_mode: str | None = None
    if cluster_version is not None:
        bridge_reason_mode = "structural_zero_weight" if bw <= 0.0 else "structural_weighted"
    return {
        "default_weights": asdict(ScoreWeights()),
        "family_weights": {family: asdict(weights) for family, weights in family_weights_resolved.items()},
        "families_written": list(RECOMMENDATION_FAMILIES),
        "placeholder_policy": placeholder_policy,
        "clustering_artifact": (
            None
            if cluster_version is None
            else {
                "cluster_version": cluster_version,
                "embedding_version": embedding_version,
                "corpus_snapshot_version": corpus_snapshot_version,
                "bridge_score_mode": "cluster_boundary_ratio_v0",
                "bridge_weight_in_final_score": bw,
                "bridge_reason_mode": bridge_reason_mode,
                "neighbor_mix_v1": {
                    "signal_version": "neighbor_mix_v1",
                    "k": int(neighbor_mix_k),
                },
            }
        ),
        "selection_scope": {
            "type": "included_works",
            "corpus_snapshot_version": corpus_snapshot_version,
            "emerging_and_bridge": "all_included_works_in_snapshot",
            "undercited": {
                "low_cite_candidate_pool_revision": LOW_CITE_CANDIDATE_POOL_REVISION,
                "doc": LOW_CITE_CANDIDATE_POOL_DOC,
                "min_year": low_cite_min_year,
                "max_citations": low_cite_max_citations,
            },
        },
        "signal_policies": {
            "semantic_score": "null_until_embeddings",
            "citation_velocity_score": "citations_per_year_normalized_within_run",
            "topic_growth_score": "mean_recent_topic_share_within_snapshot",
            "bridge_score": bridge_policy,
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
    weights: ScoreWeights,
    bridge_score: float | None = None,
    bridge_eligible: bool | None = None,
    bridge_signal_json: dict[str, Any] | None = None,
) -> PaperScoreRow:
    final = final_score_partial(
        semantic=None,
        citation_velocity=citation_velocity,
        topic_growth=topic_growth,
        bridge=bridge_score,
        diversity_penalty=diversity_penalty,
        weights=weights,
    )
    return PaperScoreRow(
        work_id=work_id,
        recommendation_family=family,
        semantic_score=None,
        citation_velocity_score=_round_score(citation_velocity),
        topic_growth_score=_round_score(topic_growth),
        bridge_score=_round_score(bridge_score),
        diversity_penalty=_round_score(diversity_penalty),
        final_score=_round_score(final) or 0.0,
        reason_short=reason_short,
        bridge_eligible=bridge_eligible,
        bridge_signal_json=bridge_signal_json,
    )


def _bridge_fields_for_work(
    *,
    work_id: int,
    cluster_version: str | None,
    bridge_boundary_by_work: dict[int, float | None] | None,
    structural_bridge_weight: float,
) -> tuple[float | None, str]:
    if cluster_version is None:
        return None, BRIDGE_REASON_LEGACY
    if bridge_boundary_by_work is None:
        return None, BRIDGE_REASON_NO_CLUSTER
    if work_id not in bridge_boundary_by_work:
        return None, BRIDGE_REASON_NO_CLUSTER
    score = bridge_boundary_by_work[work_id]
    if score is None:
        return None, BRIDGE_REASON_NO_CLUSTER
    if structural_bridge_weight <= 0.0:
        return score, BRIDGE_REASON_STRUCTURAL_ZERO_WEIGHT
    return score, BRIDGE_REASON_STRUCTURAL_WEIGHTED


def build_step3_heuristic_score_rows(
    candidates: list[RankingCandidate],
    *,
    low_cite_min_year: int = DEFAULT_LOW_CITE_MIN_YEAR,
    low_cite_max_citations: int = DEFAULT_LOW_CITE_MAX_CITATIONS,
    cluster_version: str | None = None,
    bridge_boundary_by_work: dict[int, float | None] | None = None,
    bridge_weight_for_bridge_family: float = 0.0,
    neighbor_mix_by_work: dict[int, NeighborMixV1Result] | None = None,
    neighbor_mix_k: int = NEIGHBOR_MIX_V1_DEFAULT_K,
) -> list[PaperScoreRow]:
    """
    Build heuristic rows using available metadata only.
    semantic_score stays null. bridge_score for the bridge family may be set when cluster_version
    and clustering assignments/embeddings support the cluster-boundary prototype (ML2-5a).
    bridge_weight_for_bridge_family defaults to 0 (ML2-5a: bridge signal persisted but not blended
    into final_score); set positive for isolated ML2-5b experiments via CLI override only.
    Emerging and bridge: one row per included work. Undercited: only works in the frozen
    low-cite pool (docs/candidate-pool-low-cite.md), so signals stay comparable to that definition.
    """
    bw = validate_bridge_weight_for_bridge_family(bridge_weight_for_bridge_family)
    family_w = resolved_family_weights(bw)
    citation_velocity_by_work = _citation_velocity_scores(candidates)
    topic_growth_by_work = _topic_growth_scores(candidates)
    topic_breadth_penalty_by_work = _topic_breadth_penalties(candidates)
    pool_members = [c for c in candidates if in_low_cite_candidate_pool(c, min_year=low_cite_min_year, max_citations=low_cite_max_citations)]
    citation_popularity_penalty_by_work = _citation_popularity_penalties(pool_members)

    rows: list[PaperScoreRow] = []
    for candidate in candidates:
        citation_velocity = citation_velocity_by_work[candidate.work_id]
        topic_growth = topic_growth_by_work[candidate.work_id]
        bridge_score, bridge_reason = _bridge_fields_for_work(
            work_id=candidate.work_id,
            cluster_version=cluster_version,
            bridge_boundary_by_work=bridge_boundary_by_work,
            structural_bridge_weight=bw,
        )
        mix = (
            neighbor_mix_by_work.get(candidate.work_id)
            if neighbor_mix_by_work is not None
            else None
        )
        bridge_eligible: bool | None = None
        bridge_signal_json: dict[str, Any] | None = None
        if mix is not None:
            bridge_eligible = mix.eligible
            bridge_signal_json = neighbor_mix_v1_json_payload(mix, k=neighbor_mix_k)

        rows.append(
            _make_score_row(
                work_id=candidate.work_id,
                family="emerging",
                citation_velocity=citation_velocity,
                topic_growth=topic_growth,
                diversity_penalty=0.0,
                reason_short=EMERGING_REASON,
                weights=family_w["emerging"],
                bridge_eligible=bridge_eligible,
                bridge_signal_json=bridge_signal_json,
            )
        )
        rows.append(
            _make_score_row(
                work_id=candidate.work_id,
                family="bridge",
                citation_velocity=citation_velocity,
                topic_growth=topic_growth,
                diversity_penalty=topic_breadth_penalty_by_work[candidate.work_id],
                reason_short=bridge_reason,
                bridge_score=bridge_score,
                weights=family_w["bridge"],
                bridge_eligible=bridge_eligible,
                bridge_signal_json=bridge_signal_json,
            )
        )
        if in_low_cite_candidate_pool(
            candidate, min_year=low_cite_min_year, max_citations=low_cite_max_citations
        ):
            rows.append(
                _make_score_row(
                    work_id=candidate.work_id,
                    family="undercited",
                    citation_velocity=citation_velocity,
                    topic_growth=topic_growth,
                    diversity_penalty=citation_popularity_penalty_by_work[candidate.work_id],
                    reason_short=UNDERCITED_REASON,
                    weights=family_w["undercited"],
                    bridge_eligible=bridge_eligible,
                    bridge_signal_json=bridge_signal_json,
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
    cluster_version: str | None = None,
    bridge_weight_for_bridge_family: float = 0.0,
    note: str | None = None,
    placeholder_policy: str = "semantic_and_bridge_null_until_embeddings_and_clusters",
    low_cite_min_year: int = DEFAULT_LOW_CITE_MIN_YEAR,
    low_cite_max_citations: int = DEFAULT_LOW_CITE_MAX_CITATIONS,
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

        bw = validate_bridge_weight_for_bridge_family(bridge_weight_for_bridge_family)
        family_resolved = resolved_family_weights(bw)
        nm_k = NEIGHBOR_MIX_V1_DEFAULT_K
        config = _build_ranking_config(
            corpus_snapshot_version=snapshot,
            placeholder_policy=placeholder_policy,
            low_cite_min_year=low_cite_min_year,
            low_cite_max_citations=low_cite_max_citations,
            cluster_version=cluster_version.strip() if cluster_version and cluster_version.strip() else None,
            embedding_version=embedding_version,
            bridge_weight_for_bridge_family=bw,
            family_weights_resolved=family_resolved,
            neighbor_mix_k=nm_k,
        )
        cluster_v = config.get("clustering_artifact")
        cluster_key = (
            cluster_v.get("cluster_version")
            if isinstance(cluster_v, dict) and isinstance(cluster_v.get("cluster_version"), str)
            else None
        )
        bridge_boundary_by_work: dict[int, float | None] | None = None
        neighbor_mix_by_work: dict[int, NeighborMixV1Result] | None = None
        if cluster_key:
            require_successful_clustering_run(
                conn,
                cluster_version=cluster_key,
                corpus_snapshot_version=snapshot,
                embedding_version=embedding_version,
            )
            assignments = load_cluster_assignments(conn, cluster_version=cluster_key)
            summary = list_clustering_inputs(
                conn, embedding_version=embedding_version, corpus_snapshot_version=snapshot
            )
            bridge_boundary_by_work = compute_bridge_boundary_scores(summary.rows, assignments)
            neighbor_mix_by_work = compute_neighbor_mix_v1_by_work(
                summary.rows, assignments, nm_k
            )
            warn_embedding_gaps_if_any(
                conn,
                corpus_snapshot_version=snapshot,
                embedding_version=embedding_version,
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
            score_rows = build_step3_heuristic_score_rows(
                candidates,
                low_cite_min_year=low_cite_min_year,
                low_cite_max_citations=low_cite_max_citations,
                cluster_version=cluster_key,
                bridge_boundary_by_work=bridge_boundary_by_work,
                bridge_weight_for_bridge_family=bw,
                neighbor_mix_by_work=neighbor_mix_by_work,
                neighbor_mix_k=nm_k,
            )
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
from __future__ import annotations

from dataclasses import asdict
from typing import Any

import psycopg

from pipeline.bootstrap_loader import database_url_from_env
from pipeline.config import RankingCounts, RankingRun
from pipeline.ranking import PaperScoreRow, ScoreWeights, final_score_partial
from pipeline.ranking_persistence import (
    insert_ranking_run_started,
    latest_corpus_snapshot_version_with_works,
    list_ranking_candidate_work_ids,
    update_ranking_run_final,
    upsert_paper_scores,
)

RECOMMENDATION_FAMILIES: tuple[str, ...] = ("emerging", "bridge", "undercited")

STEP2_STUB_REASON = (
    "Step-2 plumbing stub; semantic and bridge not yet modeled per ranking_version policy."
)


def _build_ranking_config(
    *,
    weights: ScoreWeights,
    corpus_snapshot_version: str,
    placeholder_policy: str,
) -> dict[str, Any]:
    return {
        "weights": asdict(weights),
        "families_written": list(RECOMMENDATION_FAMILIES),
        "placeholder_policy": placeholder_policy,
        "selection_scope": {
            "type": "included_works",
            "corpus_snapshot_version": corpus_snapshot_version,
        },
        "thresholds": {},
    }


def build_step2_stub_score_rows(work_ids: list[int]) -> list[PaperScoreRow]:
    """
    One row per (work, family) for plumbing tests. Null semantic/bridge; neutral
    numeric placeholders for other signals; final_score from partial weighting.
    """
    weights = ScoreWeights()
    rows: list[PaperScoreRow] = []
    for work_id in work_ids:
        for family in RECOMMENDATION_FAMILIES:
            sem: float | None = None
            bridge: float | None = None
            cv = 0.0
            tg = 0.0
            dp = 0.0
            fs = final_score_partial(
                semantic=sem,
                citation_velocity=cv,
                topic_growth=tg,
                bridge=bridge,
                diversity_penalty=dp,
                weights=weights,
            )
            rows.append(
                PaperScoreRow(
                    work_id=work_id,
                    recommendation_family=family,
                    semantic_score=sem,
                    citation_velocity_score=cv,
                    topic_growth_score=tg,
                    bridge_score=bridge,
                    diversity_penalty=dp,
                    final_score=fs,
                    reason_short=STEP2_STUB_REASON,
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
    weights = ScoreWeights()

    with psycopg.connect(dsn, autocommit=False) as conn:
        snapshot = corpus_snapshot_version or latest_corpus_snapshot_version_with_works(conn)
        if snapshot is None:
            raise RuntimeError(
                "No corpus_snapshot_version with included works found. "
                "Pass --corpus-snapshot-version or ingest data first."
            )

        config = _build_ranking_config(
            weights=weights,
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
            work_ids = list_ranking_candidate_work_ids(conn, snapshot)
            if not work_ids:
                raise RuntimeError(
                    f"No included works for corpus_snapshot_version={snapshot!r}; nothing to rank."
                )
            score_rows = build_step2_stub_score_rows(work_ids)
            upsert_paper_scores(conn, run.ranking_run_id, score_rows)
            counts = _ranking_counts_from_rows(len(work_ids), score_rows)
            update_ranking_run_final(conn, run.ranking_run_id, "succeeded", counts, None)
            conn.commit()
        return run.complete(counts)
    except Exception as exc:
        with psycopg.connect(dsn, autocommit=False) as conn2:
            update_ranking_run_final(conn2, run.ranking_run_id, "failed", None, str(exc))
            conn2.commit()
        raise

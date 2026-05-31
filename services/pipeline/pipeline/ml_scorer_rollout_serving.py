"""Pure serving helper for the bounded ML scorer rollout."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_shadow_scorer_generalization_second_surface import _database_url_from_env
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    RANKING_RUN_ID,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot import (
    EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT,
    _build_runtime_rows_from_live_reads,
    _connect_readonly,
    _load_frozen_audit_embedding_scorer,
    _query_candidate_inputs,
    _query_ranking_run,
    _validate_ranking_run_row,
)
from pipeline.repo_paths import default_repo_root

PINNED_RANKING_VERSION = "shadow-generalization-product-candidate-ranking-v1"


class MLScorerRolloutServingError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _runtime_env(env: Mapping[str, str] | None) -> dict[str, str]:
    out = dict(os.environ if env is None else env)
    out[FEATURE_FLAG] = "true"
    return out


def _validate_pinned_ranking_version(row: Mapping[str, Any]) -> None:
    if row.get("ranking_version") != PINNED_RANKING_VERSION:
        raise MLScorerRolloutServingError("ranking_version row mismatch")


def rank_emerging_recommendations_with_scorer(
    database_url: str | None = None,
    env: Mapping[str, str] | None = None,
    repo_root: str | Path | None = None,
) -> tuple[list[Mapping[str, Any]], dict[str, Any]]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    db_url = database_url or _database_url_from_env()
    scorer_payload, scorer_summary = _load_frozen_audit_embedding_scorer(root)

    conn = _connect_readonly(db_url)
    try:
        ranking_row = _query_ranking_run(conn, ranking_run_id=RANKING_RUN_ID)
        _validate_ranking_run_row(ranking_row)
        _validate_pinned_ranking_version(ranking_row)
        raw_rows = _query_candidate_inputs(
            conn,
            ranking_run_id=RANKING_RUN_ID,
            family=FAMILY,
            corpus_snapshot_version=CORPUS_SNAPSHOT_VERSION,
            embedding_version=EMBEDDING_VERSION,
        )
        runtime_rows, join_summary = _build_runtime_rows_from_live_reads(
            raw_rows,
            scorer_payload=scorer_payload,
            scorer_summary=scorer_summary,
        )
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()

    result = run_ml_shadow_scorer_v1_online_shadow_runtime(
        runtime_rows,
        env=_runtime_env(env),
    )
    if result.get("status") != "succeeded_test_only":
        raise MLScorerRolloutServingError("scorer runtime did not succeed")
    if result.get("shadow_row_count") != EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT:
        raise MLScorerRolloutServingError("scorer runtime did not return the expected 528 rows")
    if result.get("writes_performed") is not False or result.get("write_count") != 0:
        raise MLScorerRolloutServingError("scorer runtime reported writes")

    shadow_rows = [
        row for row in (result.get("shadow_rows") or []) if isinstance(row, Mapping)
    ]
    if len(shadow_rows) != EXPECTED_LIVE_READ_ONLY_PILOT_ROW_COUNT:
        raise MLScorerRolloutServingError("scorer shadow rows are incomplete")

    metadata = {
        "ranking_run_id": RANKING_RUN_ID,
        "ranking_version": PINNED_RANKING_VERSION,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "runtime_status": result.get("status"),
        "shadow_row_count": result.get("shadow_row_count"),
        "writes_performed": result.get("writes_performed"),
        "input_join_summary": dict(join_summary),
    }
    return shadow_rows, metadata


def map_shadow_rows_to_paper_ids(
    shadow_rows: Sequence[Mapping[str, Any]],
    limit: int = 20,
) -> list[str]:
    out: list[str] = []
    for row in shadow_rows[:limit]:
        paper_id = str(row.get("canonical_openalex_work_id") or "").strip()
        if paper_id:
            out.append(paper_id)
    return out

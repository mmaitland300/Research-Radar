import os

import pytest

from pipeline import ml_scorer_rollout_serving as serving
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    RANKING_RUN_ID,
)


def _ranking_row() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "status": "succeeded",
        "ranking_version": serving.PINNED_RANKING_VERSION,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
    }


def _runtime_rows(count: int = 528) -> list[dict[str, object]]:
    return [
        {
            "canonical_openalex_work_id": f"W{i:010d}",
            "ranking_run_id": RANKING_RUN_ID,
            "family": FAMILY,
            "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
            "embedding_version": EMBEDDING_VERSION,
            "final_score": 1.0 - (i / 1000),
            "audit_embedding_probability_work": i / 1000,
        }
        for i in range(count)
    ]


def _shadow_rows(count: int = 528) -> list[dict[str, object]]:
    return [
        {
            "canonical_openalex_work_id": f"W{count - i:010d}",
            "ml_shadow_scorer_v1_score": 1.0 - (i / 1000),
        }
        for i in range(count)
    ]


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _patch_live_read_helpers(
    monkeypatch: pytest.MonkeyPatch,
    runtime_rows: list[dict[str, object]],
) -> _FakeConn:
    conn = _FakeConn()
    monkeypatch.setattr(serving, "_load_frozen_audit_embedding_scorer", lambda _root: ({}, {}))
    monkeypatch.setattr(serving, "_connect_readonly", lambda _database_url: conn)
    monkeypatch.setattr(serving, "_query_ranking_run", lambda _conn, *, ranking_run_id: _ranking_row())
    monkeypatch.setattr(serving, "_query_candidate_inputs", lambda _conn, **_kwargs: [{"raw": True}])
    monkeypatch.setattr(
        serving,
        "_build_runtime_rows_from_live_reads",
        lambda _raw_rows, *, scorer_payload, scorer_summary: (
            runtime_rows,
            {"runtime_row_count": len(runtime_rows)},
        ),
    )
    return conn


def test_monkeypatched_db_helpers_call_runtime_with_call_scoped_flag_on_and_return_ordered_shadow_rows(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(FEATURE_FLAG, "off")
    rows_for_runtime = _runtime_rows()
    rows_from_scorer = _shadow_rows()
    conn = _patch_live_read_helpers(monkeypatch, rows_for_runtime)
    seen = {}

    def fake_runtime(candidate_rows, *, env):
        seen["candidate_rows"] = candidate_rows
        seen["env_flag"] = env[FEATURE_FLAG]
        assert os.environ[FEATURE_FLAG] == "off"
        return {
            "status": "succeeded_test_only",
            "shadow_rows": rows_from_scorer,
            "shadow_row_count": 528,
            "writes_performed": False,
            "write_count": 0,
        }

    monkeypatch.setattr(serving, "run_ml_shadow_scorer_v1_online_shadow_runtime", fake_runtime)

    shadow_rows, metadata = serving.rank_emerging_recommendations_with_scorer(
        database_url="postgresql://example",
        env={FEATURE_FLAG: "off"},
        repo_root=tmp_path,
    )

    assert seen["candidate_rows"] == rows_for_runtime
    assert seen["env_flag"] == "true"
    assert os.environ[FEATURE_FLAG] == "off"
    assert conn.closed is True
    assert shadow_rows[:3] == rows_from_scorer[:3]
    assert metadata["runtime_status"] == "succeeded_test_only"
    assert metadata["shadow_row_count"] == 528


@pytest.mark.parametrize("status", ["skipped_runtime_disabled", "failed_runtime"])
def test_runtime_disabled_or_non_succeeded_result_raises(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    _patch_live_read_helpers(monkeypatch, _runtime_rows())

    monkeypatch.setattr(
        serving,
        "run_ml_shadow_scorer_v1_online_shadow_runtime",
        lambda _rows, *, env: {
            "status": status,
            "shadow_rows": [],
            "shadow_row_count": 0,
            "writes_performed": False,
            "write_count": 0,
        },
    )

    with pytest.raises(serving.MLScorerRolloutServingError, match="did not succeed"):
        serving.rank_emerging_recommendations_with_scorer(
            database_url="postgresql://example",
            env={FEATURE_FLAG: "off"},
            repo_root=tmp_path,
        )


def test_shadow_row_count_must_be_528_on_happy_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_live_read_helpers(monkeypatch, _runtime_rows())

    monkeypatch.setattr(
        serving,
        "run_ml_shadow_scorer_v1_online_shadow_runtime",
        lambda _rows, *, env: {
            "status": "succeeded_test_only",
            "shadow_rows": _shadow_rows(20),
            "shadow_row_count": 20,
            "writes_performed": False,
            "write_count": 0,
        },
    )

    with pytest.raises(serving.MLScorerRolloutServingError, match="expected 528"):
        serving.rank_emerging_recommendations_with_scorer(
            database_url="postgresql://example",
            env={FEATURE_FLAG: "true"},
            repo_root=tmp_path,
        )


def test_map_shadow_rows_to_paper_ids_preserves_order_and_limit() -> None:
    rows = [
        {"canonical_openalex_work_id": "W3"},
        {"canonical_openalex_work_id": "W1"},
        {"canonical_openalex_work_id": "W2"},
    ]

    assert serving.map_shadow_rows_to_paper_ids(rows, limit=2) == ["W3", "W1"]

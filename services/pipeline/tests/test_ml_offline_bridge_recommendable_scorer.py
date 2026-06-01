"""Tests for offline bridge_recommendable scorer diagnostic."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_recommendable_scorer import (
    MLOfflineBridgeRecommendableScorerError,
    _execute_select,
    build_ml_offline_bridge_recommendable_scorer_payload,
    markdown_from_ml_offline_bridge_recommendable_scorer,
)


RANKING_RUN_ID = "rank-83787b91ef"
REVIEW_POOL = "ml_bridge_negative_mining_audit"
EMBEDDING_VERSION = "shadow-generalization-text-embedding-v1"
SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"


class _FakeCursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()) -> "_FakeCursor":
        self.executed.append((sql, params))
        return self

    def fetchall(self) -> list[dict]:
        return list(self.rows)


class _FakeCursorContext:
    def __init__(self, rows: list[dict]) -> None:
        self.cursor = _FakeCursor(rows)

    def __enter__(self) -> _FakeCursor:
        return self.cursor

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.cursor_contexts: list[_FakeCursorContext] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCursorContext:
        ctx = _FakeCursorContext(self.rows)
        self.cursor_contexts.append(ctx)
        return ctx


def _row(index: int, *, pool: str = REVIEW_POOL) -> dict:
    work_token = f"W990{index:04d}"
    relevance = "good" if index <= 33 else ("acceptable" if index <= 60 else ("miss" if index <= 66 else "irrelevant"))
    bridge_like = "yes" if index <= 13 else ("partial" if index <= 38 else "no")
    recommendable = index <= 38
    return {
        "dataset_version": "ml-label-dataset-v12",
        "row_id": f"row-{index:02d}",
        "paper_id": f"https://openalex.org/{work_token}",
        "work_id": work_token,
        "split": "audit_only",
        "ranking_run_id": RANKING_RUN_ID,
        "family": "bridge",
        "review_pool_variant": pool,
        "relevance_label": relevance,
        "novelty_label": "useful" if index <= 60 else "not_useful",
        "bridge_like_label": bridge_like,
        "good_or_acceptable": index <= 60,
        "bridge_like_yes_or_partial": index <= 38,
        "bridge_recommendable": recommendable,
        "final_score": 1.0 - index / 100.0,
        "bridge_score": None,
        "semantic_score": 0.2 + index / 1000.0,
        "bridge_negative_mining_context": {
            "row_id": f"row-{index:02d}",
            "internal_work_id": index,
            "ranking_run_id": RANKING_RUN_ID,
            "family": "bridge",
            "final_score": 1.0 - index / 100.0,
            "bridge_score": None,
            "semantic_score": 0.2 + index / 1000.0,
        },
    }


def _label_payload(rows: list[dict]) -> dict:
    return {
        "dataset_version": "ml-label-dataset-v12",
        "rows": rows,
        "metadata": {
            "bridge_negative_mining_v1_ingest": {
                "labeled_worksheet_path": "docs/audit/manual-review/bridge_labeled.csv",
                "context_sidecar_path": "docs/audit/manual-review/bridge_context.json",
            }
        },
    }


def _readiness_payload(label_sha: str, **overrides: object) -> dict:
    group = {
        "ranking_run_id": RANKING_RUN_ID,
        "family": "bridge",
        "target": "bridge_recommendable",
        "total_labeled_rows": 70,
        "positive_count": 38,
        "negative_count": 32,
        "paper_scores_joinable_count": 70,
        "missing_score_count": 0,
        "derived_target_conflict_count": 0,
        "readiness": {
            "has_both_classes": True,
            "enough_for_diagnostic_auc": True,
            "enough_for_tiny_baseline": True,
        },
    }
    group.update(overrides)
    return {
        "artifact_type": "ml_label_readiness_matrix",
        "provenance": {
            "label_dataset_version": "ml-label-dataset-v12",
            "label_dataset_sha256": label_sha,
        },
        "groups": [group],
    }


def _embeddings_provenance(dim: int = 4) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_embeddings",
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "embedding_version": EMBEDDING_VERSION,
            "snapshot_version": SNAPSHOT_VERSION,
        },
        "embedding_result": {
            "status": "succeeded",
            "full_snapshot_embedding_coverage": True,
            "embedding_dimensions": dim,
        },
        "coverage": {
            "embedded_work_count": 528,
            "missing_embedding_count": 0,
        },
    }


def _embedding_rows(rows: list[dict], *, dim: int = 4, missing_work_id: int | None = None) -> list[dict]:
    out = []
    for row in rows:
        work_id = row["bridge_negative_mining_context"]["internal_work_id"]
        if work_id == missing_work_id:
            continue
        label_value = 1.0 if row["bridge_recommendable"] else -1.0
        vector = [label_value, work_id / 100.0, (work_id % 7) / 10.0, 0.5]
        out.append({"work_id": work_id, "vector": json.dumps(vector[:dim])})
    return out


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _fixture(tmp_path: Path, *, rows: list[dict] | None = None, readiness_overrides: dict | None = None) -> dict:
    data_rows = rows or [_row(i) for i in range(1, 71)]
    label_path = tmp_path / "docs/audit/ml-label-dataset-v12.json"
    readiness_path = tmp_path / "docs/audit/ml-label-readiness-matrix-v9.json"
    embeddings_path = tmp_path / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
    sidecar_path = tmp_path / "docs/audit/manual-review/bridge_context.json"
    labeled_path = tmp_path / "docs/audit/manual-review/bridge_labeled.csv"
    _write_json(label_path, _label_payload(data_rows))
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.write_text("{}\n", encoding="utf-8")
    labeled_path.write_text("row_id\n", encoding="utf-8")
    label_sha = sha256_file(label_path)
    _write_json(readiness_path, _readiness_payload(label_sha, **(readiness_overrides or {})))
    _write_json(embeddings_path, _embeddings_provenance())
    return {
        "rows": data_rows,
        "label_path": label_path,
        "readiness_path": readiness_path,
        "embeddings_path": embeddings_path,
        "conn": _FakeConn(_embedding_rows(data_rows)),
    }


def test_happy_path_with_tiny_vectors_and_all_null_bridge_score_arm(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )

    meta = payload["metadata"]
    assert payload["artifact_type"] == "ml_offline_bridge_recommendable_scorer"
    assert meta["slice_counts"]["row_count"] == 70
    assert meta["slice_counts"]["positive_count"] == 38
    assert meta["slice_counts"]["negative_count"] == 32
    assert meta["hard_negative_count"] == 22
    assert meta["bridge_like_positive_relevance_leak_count"] == 0
    assert meta["embedding_coverage"]["loaded_vector_count"] == 70
    assert payload["evaluation"]["learned_cv"]["aggregate_oof"]["roc_auc"] is not None
    assert payload["evaluation"]["heuristic_arms"]["bridge_score"]["status"] == "not_applicable"
    assert payload["frozen_scorer"]["embedding_dimensions"] == 4
    executed_sql = fx["conn"].cursor_contexts[0].cursor.executed[0][0].strip().lower()
    assert executed_sql.startswith("select")


def test_readiness_validation_rejects_stale_counts(tmp_path: Path) -> None:
    fx = _fixture(tmp_path, readiness_overrides={"positive_count": 37})

    with pytest.raises(MLOfflineBridgeRecommendableScorerError, match="positive_count"):
        build_ml_offline_bridge_recommendable_scorer_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


def test_slice_filter_rejects_wrong_pool_or_wrong_count(tmp_path: Path) -> None:
    rows = [_row(i) for i in range(1, 71)]
    rows[0] = _row(1, pool="wrong_pool")
    fx = _fixture(tmp_path, rows=rows)

    with pytest.raises(MLOfflineBridgeRecommendableScorerError, match="mandatory filter"):
        build_ml_offline_bridge_recommendable_scorer_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


def test_missing_embeddings_rejects(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    fx["conn"] = _FakeConn(_embedding_rows(fx["rows"], missing_work_id=70))

    with pytest.raises(MLOfflineBridgeRecommendableScorerError, match="embedding coverage mismatch"):
        build_ml_offline_bridge_recommendable_scorer_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


def test_execute_select_rejects_writes() -> None:
    cur = _FakeCursor([])
    for sql in ("INSERT INTO embeddings VALUES (1)", "UPDATE embeddings SET vector = '[]'", "DELETE FROM embeddings"):
        with pytest.raises(MLOfflineBridgeRecommendableScorerError, match="DB safety"):
            _execute_select(cur, sql, ())


def test_markdown_contains_offline_diagnostic_not_validation_and_pool(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )
    md = markdown_from_ml_offline_bridge_recommendable_scorer(payload)

    assert "Offline diagnostic" in md
    assert "not validation" in md
    assert "ml_bridge_negative_mining_audit" in md

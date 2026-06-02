"""Tests for offline bridge_recommendable scorer v2 (combined slice diagnostic)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_recommendable_scorer_v2 import (
    MLOfflineBridgeRecommendableScorerV2Error,
    _execute_select,
    _top_20_comparison,
    build_ml_offline_bridge_recommendable_scorer_v2_payload,
    markdown_from_ml_offline_bridge_recommendable_scorer_v2,
)


RANKING_RUN_ID = "rank-83787b91ef"
POOL_NEG_MINING = "ml_bridge_negative_mining_audit"
POOL_TOP_RANKED = "ml_bridge_top_ranked_validation_audit"
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


def _neg_mining_row(index: int, *, pool: str = POOL_NEG_MINING) -> dict:
    """Construct a negative-mining slice row (index 1-70).

    Distribution mirrors the real labeled worksheet:
      1-13:  yes (positive)
      14-38: partial (positive)
      39-70: no (negative)
    Relevance:
      1-33:  good
      34-60: acceptable
      61-66: miss
      67-70: irrelevant
    Hard negatives (good/acceptable AND no): rows 39-60 = 22.
    """
    work_token = f"W990{index:04d}"
    relevance = (
        "good" if index <= 33
        else "acceptable" if index <= 60
        else "miss" if index <= 66
        else "irrelevant"
    )
    bridge_like = "yes" if index <= 13 else ("partial" if index <= 38 else "no")
    recommendable = index <= 38
    return {
        "dataset_version": "ml-label-dataset-v13",
        "row_id": f"neg-{index:03d}",
        "paper_id": f"https://openalex.org/{work_token}",
        "work_id": work_token,
        "split": "audit_only",
        "ranking_run_id": RANKING_RUN_ID,
        "family": "bridge",
        "review_pool_variant": pool,
        "relevance_label": relevance,
        "novelty_label": "useful",
        "bridge_like_label": bridge_like,
        "good_or_acceptable": index <= 60,
        "bridge_recommendable": recommendable,
        "final_score": 1.0 - index / 100.0,
        "bridge_score": None,
        "semantic_score": 0.2 + index / 1000.0,
        "sample_reason": "bridge_suppressed_final",
        "family_rank": 200 + index,
        "internal_work_id": index,
    }


def _top_ranked_row(index: int, *, pool: str = POOL_TOP_RANKED) -> dict:
    """Construct a top-ranked validation row (index 71-100).

    Distribution for 30 rows (internal index = index - 70):
      71-74:  yes, bridge_top_ranked (4 positive, top-20)
      75-78:  partial, bridge_top_ranked (4 positive, top-20)
      79-90:  no, good relevance, bridge_top_ranked (12 negative, top-20)
      91-95:  yes, bridge_borderline_contrastive (1 yes more from overall)
      96-100: partial, bridge_borderline_contrastive (last 6 partial overall)

    Wait -- we need yes=5, partial=10, no=15 total for 30 rows.
    And top-20 bridge_top_ranked: 8 positive (4 yes + 4 partial) + 12 negative.
    Borderline-contrastive (10): 1 yes + 6 partial + 3 no.
    Combined: yes=5, partial=10, no=15 ✓  hard_negatives=15 (all no rows are good) ✓
    """
    local = index - 70  # 1-30
    work_token = f"W991{index:04d}"
    if local <= 4:
        bridge_like = "yes"
        sample_reason = "bridge_top_ranked"
    elif local <= 8:
        bridge_like = "partial"
        sample_reason = "bridge_top_ranked"
    elif local <= 20:
        bridge_like = "no"
        sample_reason = "bridge_top_ranked"
    elif local == 21:
        bridge_like = "yes"
        sample_reason = "bridge_borderline_contrastive"
    elif local <= 27:
        bridge_like = "partial"
        sample_reason = "bridge_borderline_contrastive"
    else:
        bridge_like = "no"
        sample_reason = "bridge_borderline_contrastive"
    recommendable = bridge_like in {"yes", "partial"}
    family_rank = local  # 1-30 within the bridge family
    return {
        "dataset_version": "ml-label-dataset-v13",
        "row_id": f"top-{index:03d}",
        "paper_id": f"https://openalex.org/{work_token}",
        "work_id": work_token,
        "split": "audit_only",
        "ranking_run_id": RANKING_RUN_ID,
        "family": "bridge",
        "review_pool_variant": pool,
        "relevance_label": "good",
        "novelty_label": "useful",
        "bridge_like_label": bridge_like,
        "good_or_acceptable": True,
        "bridge_recommendable": recommendable,
        "final_score": 1.0 - local / 100.0,
        "bridge_score": None,
        "semantic_score": 0.5 + local / 1000.0,
        "sample_reason": sample_reason,
        "family_rank": family_rank,
        "internal_work_id": index,
    }


def _all_rows() -> list[dict]:
    return [_neg_mining_row(i) for i in range(1, 71)] + [_top_ranked_row(i) for i in range(71, 101)]


def _label_payload(rows: list[dict]) -> dict:
    return {
        "dataset_version": "ml-label-dataset-v13",
        "rows": rows,
        "metadata": {
            "bridge_negative_mining_v1_ingest": {
                "labeled_worksheet_path": "docs/audit/manual-review/bridge_neg_labeled.csv",
                "context_sidecar_path": "docs/audit/manual-review/bridge_neg_context.json",
            },
            "bridge_top_ranked_v1_ingest": {
                "labeled_worksheet_path": "docs/audit/manual-review/bridge_top_labeled.csv",
                "context_sidecar_path": "docs/audit/manual-review/bridge_top_context.json",
            },
        },
    }


def _readiness_payload(label_sha: str, **overrides: object) -> dict:
    group: dict = {
        "ranking_run_id": RANKING_RUN_ID,
        "family": "bridge",
        "target": "bridge_recommendable",
        "total_labeled_rows": 100,
        "positive_count": 53,
        "negative_count": 47,
        "paper_scores_joinable_count": 100,
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
            "label_dataset_version": "ml-label-dataset-v13",
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


def _embedding_rows_for(rows: list[dict], *, dim: int = 4, missing_work_id: int | None = None) -> list[dict]:
    out = []
    for row in rows:
        work_id = row["internal_work_id"]
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
    data_rows = rows if rows is not None else _all_rows()
    label_path = tmp_path / "docs/audit/ml-label-dataset-v13.json"
    readiness_path = tmp_path / "docs/audit/ml-label-readiness-matrix-v10.json"
    embeddings_path = tmp_path / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
    _write_json(label_path, _label_payload(data_rows))
    label_sha = sha256_file(label_path)
    _write_json(readiness_path, _readiness_payload(label_sha, **(readiness_overrides or {})))
    _write_json(embeddings_path, _embeddings_provenance())
    return {
        "rows": data_rows,
        "label_path": label_path,
        "readiness_path": readiness_path,
        "embeddings_path": embeddings_path,
        "conn": _FakeConn(_embedding_rows_for(data_rows)),
    }


def test_execute_select_rejects_writes() -> None:
    cur = _FakeCursor([])
    for sql in (
        "INSERT INTO embeddings VALUES (1)",
        "UPDATE embeddings SET vector = '[]'",
        "DELETE FROM embeddings",
        "DROP TABLE embeddings",
        "TRUNCATE embeddings",
    ):
        with pytest.raises(MLOfflineBridgeRecommendableScorerV2Error, match="DB safety"):
            _execute_select(cur, sql, ())


def test_execute_select_allows_select() -> None:
    cur = _FakeCursor([])
    _execute_select(cur, "SELECT work_id FROM embeddings WHERE work_id = ANY(%s)", ([], ))
    assert cur.executed


def test_happy_path_slice_counts_and_stratification(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_v2_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )

    meta = payload["metadata"]
    assert payload["artifact_type"] == "ml_offline_bridge_recommendable_scorer_v2"
    assert meta["slice_counts"]["row_count"] == 100
    assert meta["slice_counts"]["positive_count"] == 53
    assert meta["slice_counts"]["negative_count"] == 47
    assert meta["hard_negative_count"] == 37
    assert meta["bridge_like_positive_relevance_leak_count"] == 0
    assert meta["embedding_coverage"]["loaded_vector_count"] == 100

    # Stratified metrics are present
    strat = payload["evaluation"]["learned_cv"]["stratified_oof_metrics"]
    strat_labels = {s["stratum"] for s in strat}
    assert "all_100_rows" in strat_labels
    assert "negative_mining_slice_70_rows" in strat_labels
    assert "top_ranked_validation_slice_30_rows" in strat_labels
    assert "top_20_live_bridge_rows" in strat_labels

    neg_mining_strat = next(s for s in strat if s["stratum"] == "negative_mining_slice_70_rows")
    assert neg_mining_strat["n"] == 70

    top_ranked_strat = next(s for s in strat if s["stratum"] == "top_ranked_validation_slice_30_rows")
    assert top_ranked_strat["n"] == 30


def test_top_20_comparison_is_in_output(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_v2_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )

    top20 = payload["top_20_live_bridge_comparison"]
    assert top20["status"] == "ok"
    assert top20["n_top20"] == 20
    assert top20["positive_count"] == 8
    assert top20["negative_count"] == 12
    assert "verdict" in top20
    assert "pairwise_correct_ordering_fraction" in top20
    assert len(top20["per_row"]) == 20


def test_slice_filter_accepts_both_pool_variants(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_v2_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )
    pool_counts = payload["metadata"]["slice_counts"]["review_pool_variant_counts"]
    assert pool_counts.get(POOL_NEG_MINING) == 70
    assert pool_counts.get(POOL_TOP_RANKED) == 30


def test_slice_filter_rejects_wrong_pool(tmp_path: Path) -> None:
    rows = _all_rows()
    rows[0] = {**rows[0], "review_pool_variant": "wrong_pool"}
    fx = _fixture(tmp_path, rows=rows)

    with pytest.raises(MLOfflineBridgeRecommendableScorerV2Error, match="mandatory filter"):
        build_ml_offline_bridge_recommendable_scorer_v2_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


def test_readiness_validation_rejects_wrong_positive_count(tmp_path: Path) -> None:
    fx = _fixture(tmp_path, readiness_overrides={"positive_count": 38})

    with pytest.raises(MLOfflineBridgeRecommendableScorerV2Error, match="positive_count"):
        build_ml_offline_bridge_recommendable_scorer_v2_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


def test_readiness_validation_rejects_stale_dataset_version(tmp_path: Path) -> None:
    rows = _all_rows()
    label_path = tmp_path / "docs/audit/ml-label-dataset-v13.json"
    readiness_path = tmp_path / "docs/audit/ml-label-readiness-matrix-v10.json"
    embeddings_path = tmp_path / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
    _write_json(label_path, _label_payload(rows))
    label_sha = sha256_file(label_path)
    stale_readiness = _readiness_payload(label_sha)
    stale_readiness["provenance"]["label_dataset_version"] = "ml-label-dataset-v12"
    _write_json(readiness_path, stale_readiness)
    _write_json(embeddings_path, _embeddings_provenance())
    conn = _FakeConn(_embedding_rows_for(rows))

    with pytest.raises(MLOfflineBridgeRecommendableScorerV2Error, match="ml-label-dataset-v13"):
        build_ml_offline_bridge_recommendable_scorer_v2_payload(
            conn,
            label_dataset_path=label_path,
            readiness_matrix_path=readiness_path,
            embeddings_provenance_path=embeddings_path,
        )


def test_internal_work_id_read_from_top_level_field(tmp_path: Path) -> None:
    """v2 reads internal_work_id from top-level row field; no context dict required."""
    fx = _fixture(tmp_path)
    # Verify rows have no bridge_negative_mining_context but do have top-level internal_work_id
    row_with_top_pool = next(r for r in fx["rows"] if r["review_pool_variant"] == POOL_TOP_RANKED)
    assert "bridge_negative_mining_context" not in row_with_top_pool
    assert isinstance(row_with_top_pool.get("internal_work_id"), int)

    # Full scorer must still succeed
    payload = build_ml_offline_bridge_recommendable_scorer_v2_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )
    assert payload["metadata"]["slice_counts"]["row_count"] == 100


def test_missing_embeddings_rejects(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    fx["conn"] = _FakeConn(_embedding_rows_for(fx["rows"], missing_work_id=100))

    with pytest.raises(MLOfflineBridgeRecommendableScorerV2Error, match="embedding coverage mismatch"):
        build_ml_offline_bridge_recommendable_scorer_v2_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


def test_top_20_comparison_not_applicable_when_count_wrong() -> None:
    rows: list[dict] = []
    oof_prob: list[float] = []
    result = _top_20_comparison(rows, oof_prob)
    assert result["status"] == "unexpected_count"
    assert result["found"] == 0


def test_markdown_contains_key_sections(tmp_path: Path) -> None:
    fx = _fixture(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_v2_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
    )
    md = markdown_from_ml_offline_bridge_recommendable_scorer_v2(payload)

    assert "combined slice diagnostic" in md
    assert "Stratified OOF Metrics" in md
    assert "Top-20 Live Bridge Comparison" in md
    assert "Decision Signal" in md
    assert "not validation" in md
    assert "not a serving change" in md

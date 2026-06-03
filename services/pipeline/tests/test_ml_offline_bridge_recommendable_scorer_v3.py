"""Tests for offline bridge_recommendable scorer v3 (three-pool shadow audit diagnostic)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_recommendable_scorer_v3 import (
    DEDUPED_POOL_COUNTS,
    DEDUPED_ROWS,
    EXPECTED_CONFLICT_WORK_ID,
    MLOfflineBridgeRecommendableScorerV3Error,
    POOL_NEG,
    POOL_SHADOW,
    POOL_TOP,
    _dedupe_by_work_id,
    _execute_select,
    _slice_row_level_rows,
    build_ml_offline_bridge_recommendable_scorer_v3_payload,
    markdown_from_ml_offline_bridge_recommendable_scorer_v3,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

LABEL_V14 = REPO_ROOT / "docs/audit/ml-label-dataset-v14.json"
READINESS_V11 = REPO_ROOT / "docs/audit/ml-label-readiness-matrix-v11.json"
EMBEDDINGS_PROV = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
V2_BASELINE = REPO_ROOT / "docs/audit/ml-offline-bridge-recommendable-scorer-v2.json"


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

    def cursor(self, row_factory: object | None = None) -> _FakeCursorContext:
        return _FakeCursorContext(self.rows)


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _deduped_rows_from_v14() -> tuple[list[dict], list[dict]]:
    if not LABEL_V14.is_file():
        pytest.skip("ml-label-dataset-v14.json not present")
    row_level = _slice_row_level_rows(_load(LABEL_V14))
    deduped, _ = _dedupe_by_work_id(row_level)
    return row_level, deduped


def _embedding_rows_for(deduped: list[dict], *, dim: int = 4) -> list[dict]:
    out = []
    for row in deduped:
        work_id = int(row["internal_work_id"])
        label_value = 1.0 if row["bridge_recommendable"] else -1.0
        vector = [label_value, work_id / 1000.0, (work_id % 7) / 10.0, 0.5]
        out.append({"work_id": work_id, "vector": json.dumps(vector[:dim])})
    return out


def _embeddings_provenance(dim: int = 4) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_snapshot_embeddings",
            "artifact_version": "ml-shadow-scorer-v1-second-snapshot-embeddings-v1",
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
        },
        "embedding_result": {
            "status": "succeeded",
            "full_snapshot_embedding_coverage": True,
            "embedding_dimensions": dim,
        },
        "coverage": {"embedded_work_count": 528, "missing_embedding_count": 0},
    }


def _write_audit_inputs(
    tmp_path: Path,
    *,
    label_payload: dict | None = None,
) -> dict:
    if label_payload is None:
        label_payload = _load(LABEL_V14)
    label_path = tmp_path / "docs/audit/ml-label-dataset-v14.json"
    readiness_path = tmp_path / "docs/audit/ml-label-readiness-matrix-v11.json"
    embeddings_path = tmp_path / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
    label_path.parent.mkdir(parents=True, exist_ok=True)
    label_path.write_text(json.dumps(label_payload), encoding="utf-8")
    label_sha = sha256_file(label_path)
    readiness = _load(READINESS_V11)
    readiness["provenance"]["label_dataset_sha256"] = label_sha
    readiness_path.write_text(json.dumps(readiness), encoding="utf-8")
    embeddings_path.write_text(json.dumps(_embeddings_provenance(dim=4)), encoding="utf-8")
    deduped, _ = _dedupe_by_work_id(_slice_row_level_rows(label_payload))
    return {
        "label_path": label_path,
        "readiness_path": readiness_path,
        "embeddings_path": embeddings_path,
        "conn": _FakeConn(_embedding_rows_for(deduped, dim=4)),
        "deduped": deduped,
    }


def _fixture_conn(tmp_path: Path) -> _FakeConn:
    return _write_audit_inputs(tmp_path)["conn"]


@pytest.mark.skipif(not LABEL_V14.is_file(), reason="v14 label dataset missing")
def test_execute_select_rejects_writes() -> None:
    cur = _FakeCursor([])
    for sql in (
        "INSERT INTO embeddings VALUES (1)",
        "UPDATE embeddings SET vector = '[]'",
        "DELETE FROM embeddings",
    ):
        with pytest.raises(MLOfflineBridgeRecommendableScorerV3Error, match="DB safety"):
            _execute_select(cur, sql, ())


def test_dedupe_policy_prefers_shadow_over_top_ranked_over_negative_mining() -> None:
    row_level, deduped = _deduped_rows_from_v14()
    conflict = next(r for r in deduped if r["work_id"] == EXPECTED_CONFLICT_WORK_ID)
    assert conflict["review_pool_variant"] == POOL_SHADOW
    assert conflict["bridge_recommendable"] is False
    neg_row = next(
        r
        for r in row_level
        if r["work_id"] == EXPECTED_CONFLICT_WORK_ID and r["review_pool_variant"] == POOL_NEG
    )
    assert neg_row["bridge_recommendable"] is True


@pytest.mark.skipif(
    not all(p.is_file() for p in (LABEL_V14, READINESS_V11, EMBEDDINGS_PROV, V2_BASELINE)),
    reason="v14/v11/embeddings/v2 audit inputs missing",
)
def test_happy_path_real_v14_slice_counts_and_strata(tmp_path: Path) -> None:
    fx = _write_audit_inputs(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_v3_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
        v2_baseline_path=V2_BASELINE,
        random_seed=20260602,
    )
    assert payload["artifact_type"] == "ml_offline_bridge_recommendable_scorer_v3"
    assert payload["metadata"]["row_level_160"]["row_count"] == 160
    assert payload["metadata"]["deduped_130"]["row_count"] == DEDUPED_ROWS
    assert payload["metadata"]["deduped_130"]["positive_count"] == 75
    assert payload["metadata"]["deduped_130"]["negative_count"] == 55
    for pool, count in DEDUPED_POOL_COUNTS.items():
        assert payload["metadata"]["deduped_130"]["review_pool_variant_counts"][pool] == count
    assert payload["metadata"]["deduped_130"]["derived_target_conflict_count"] == 1

    deduped_strata = {
        s["stratum"]
        for s in payload["evaluation"]["learned_cv_primary_deduped"]["stratified_oof_metrics_deduped"]
    }
    assert "all_deduped_130_rows" in deduped_strata
    assert "shadow_pilot_60_rows" in deduped_strata
    assert "negative_mining_selected_62_rows" in deduped_strata
    assert "top_ranked_selected_8_rows" in deduped_strata
    assert "rank-83787b91ef_deduped_70_rows" in deduped_strata
    assert "rank-5a7efa5ca3_deduped_60_rows" in deduped_strata

    row_audit = payload["evaluation"]["stratified_oof_metrics_row_level_audit"]
    assert row_audit[0]["duplicate_sensitive"] is True
    assert row_audit[0]["excluded_from_recommended_next_stage_decision"] is True
    assert any(s["stratum"] == "all_row_level_160_rows" for s in row_audit)

    targeted = payload["evaluation"]["targeted_decision_readouts"]
    assert "verdict" in targeted["high_ml_low_bridge_score"]
    assert "verdict" in targeted["high_bridge_score_low_ml"]

    v2_delta = payload["evaluation"]["v2_baseline_delta"]
    assert v2_delta["v2_work_id_count"] == 100
    assert v2_delta["comparison_scope"] == "v3_deduped_oof_on_v2_work_id_set"
    assert "rank-83787b91ef_deduped_70_rows" not in str(v2_delta.get("comparison_scope", ""))


@pytest.mark.skipif(
    not all(p.is_file() for p in (LABEL_V14, READINESS_V11, EMBEDDINGS_PROV)),
    reason="v14 audit inputs missing",
)
def test_slice_filter_rejects_wrong_pool(tmp_path: Path) -> None:
    label_payload = _load(LABEL_V14)
    rows = _slice_row_level_rows(label_payload)
    rows[0] = {**rows[0], "review_pool_variant": "wrong_pool"}
    label_payload = {**label_payload, "rows": rows}
    label_path = tmp_path / "docs/audit/ml-label-dataset-v14.json"
    readiness_path = tmp_path / "docs/audit/ml-label-readiness-matrix-v11.json"
    embeddings_path = tmp_path / "docs/audit/ml-shadow-scorer-v1-second-snapshot-embeddings-v1.json"
    label_path.parent.mkdir(parents=True, exist_ok=True)
    label_path.write_text(json.dumps(label_payload), encoding="utf-8")
    label_sha = sha256_file(label_path)
    readiness = _load(READINESS_V11)
    readiness["provenance"]["label_dataset_sha256"] = label_sha
    readiness_path.write_text(json.dumps(readiness), encoding="utf-8")
    embeddings_path.write_text(json.dumps(_embeddings_provenance(dim=4)), encoding="utf-8")
    with pytest.raises(MLOfflineBridgeRecommendableScorerV3Error, match="row-level training slice"):
        build_ml_offline_bridge_recommendable_scorer_v3_payload(
            _FakeConn([]),
            label_dataset_path=label_path,
            readiness_matrix_path=readiness_path,
            embeddings_provenance_path=embeddings_path,
        )


@pytest.mark.skipif(
    not all(p.is_file() for p in (LABEL_V14, READINESS_V11, EMBEDDINGS_PROV)),
    reason="v14 audit inputs missing",
)
def test_missing_embeddings_rejects(tmp_path: Path) -> None:
    fx = _write_audit_inputs(tmp_path)
    missing_id = int(fx["deduped"][0]["internal_work_id"])
    rows = [r for r in _embedding_rows_for(fx["deduped"], dim=4) if r["work_id"] != missing_id]
    with pytest.raises(MLOfflineBridgeRecommendableScorerV3Error, match="embedding coverage mismatch"):
        build_ml_offline_bridge_recommendable_scorer_v3_payload(
            _FakeConn(rows),
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


@pytest.mark.skipif(
    not all(p.is_file() for p in (LABEL_V14, READINESS_V11, EMBEDDINGS_PROV)),
    reason="v14 audit inputs missing",
)
def test_readiness_validation_rejects_wrong_v14_reference(tmp_path: Path) -> None:
    fx = _write_audit_inputs(tmp_path)
    readiness = _load(fx["readiness_path"])
    readiness["provenance"]["label_dataset_version"] = "ml-label-dataset-v13"
    fx["readiness_path"].write_text(json.dumps(readiness), encoding="utf-8")
    with pytest.raises(MLOfflineBridgeRecommendableScorerV3Error, match="ml-label-dataset-v14"):
        build_ml_offline_bridge_recommendable_scorer_v3_payload(
            fx["conn"],
            label_dataset_path=fx["label_path"],
            readiness_matrix_path=fx["readiness_path"],
            embeddings_provenance_path=fx["embeddings_path"],
        )


@pytest.mark.skipif(
    not all(p.is_file() for p in (LABEL_V14, READINESS_V11, EMBEDDINGS_PROV, V2_BASELINE)),
    reason="v14 audit inputs missing",
)
def test_markdown_contains_key_sections(tmp_path: Path) -> None:
    fx = _write_audit_inputs(tmp_path)
    payload = build_ml_offline_bridge_recommendable_scorer_v3_payload(
        fx["conn"],
        label_dataset_path=fx["label_path"],
        readiness_matrix_path=fx["readiness_path"],
        embeddings_provenance_path=fx["embeddings_path"],
        v2_baseline_path=V2_BASELINE,
    )
    md = markdown_from_ml_offline_bridge_recommendable_scorer_v3(payload)
    assert "three-pool shadow audit" in md
    assert "deduped 130" in md.lower() or "Deduped primary rows: 130" in md
    assert "not validation" in md.lower() or "not a serving" in md.lower()

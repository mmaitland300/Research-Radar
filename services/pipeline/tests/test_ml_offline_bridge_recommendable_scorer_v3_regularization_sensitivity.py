"""Tests for offline v3 bridge scorer regularization sensitivity."""

from __future__ import annotations

import json
from pathlib import Path

from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity import (
    SWEEP_C_VALUES,
    _load_baseline_reference,
    _load_slice_embeddings_select_only,
    _mark_too_strong_regularization,
    _selection_from_sweep,
    build_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_payload_from_slice,
)


def _tiny_rows() -> list[dict]:
    buckets = [
        "high_ml_low_bridge_score",
        "high_bridge_score_low_ml",
        "promoted_by_hybrid",
        "demoted_by_hybrid",
    ]
    rows = []
    for i in range(20):
        label = i < 10
        rows.append(
            {
                "row_id": f"row-{i}",
                "work_id": f"W{i + 1}",
                "internal_work_id": i + 1,
                "bridge_recommendable": label,
                "review_pool_variant": "ml_bridge_shadow_pilot_audit",
                "ranking_run_id": "rank-5a7efa5ca3",
                "sample_reason": buckets[i % len(buckets)],
                "ml_probability": 0.8 if label else 0.2,
            }
        )
    return rows


def _tiny_vectors(rows: list[dict]) -> dict[int, list[float]]:
    vectors = {}
    for i, row in enumerate(rows):
        signal = 2.0 if row["bridge_recommendable"] else -2.0
        vectors[row["internal_work_id"]] = [signal, i / 10.0, (i % 3) / 3.0]
    return vectors


def _tiny_v2_payload(rows: list[dict]) -> dict:
    return {
        "scorer_version": "ml-offline-bridge-recommendable-scorer-v2",
        "evaluation": {
            "learned_cv": {
                "aggregate_oof": {"roc_auc": 0.5},
                "oof_predictions": [
                    {
                        "work_id": row["work_id"],
                        "label": row["bridge_recommendable"],
                        "probability": 0.5,
                    }
                    for row in rows
                ],
            }
        },
    }


def _baseline_reference() -> dict:
    return {
        "name": "v3_baseline_scorer",
        "path": "docs/audit/ml-offline-bridge-recommendable-scorer-v3.json",
        "sha256": "baseline-sha",
        "sha256_source": "test",
        "artifact_type": "ml_offline_bridge_recommendable_scorer_v3",
        "scorer_version": "ml-offline-bridge-recommendable-scorer-v3",
        "baseline_C": 1.0,
        "read_only_reference": True,
    }


def test_tiny_sweep_runs_and_reports_per_c_fields() -> None:
    rows = _tiny_rows()
    payload = build_ml_offline_bridge_recommendable_scorer_v3_regularization_sensitivity_payload_from_slice(
        deduped_rows=rows,
        vectors_by_work=_tiny_vectors(rows),
        baseline_reference=_baseline_reference(),
        v2_payload=_tiny_v2_payload(rows),
        expected_v2_work_id_count=len(rows),
        generated_at="2026-06-02T00:00:00Z",
    )

    assert [item["C"] for item in payload["regularization_sweep"]] == list(SWEEP_C_VALUES)
    assert "serving_ready_for_hybrid_eval" not in json.dumps(payload)
    for item in payload["regularization_sweep"]:
        assert item["oof_roc_auc"] is not None
        assert item["oof_average_precision"] is not None
        assert item["oof_precision_at_20"] is not None
        assert item["in_sample_roc_auc"] is not None
        assert item["in_sample_auc_minus_oof_auc"] is not None
        assert item["v3_on_v2_work_id_set"]["status"] == "ok"
        assert item["v3_on_v2_work_id_set_roc_auc"] is not None
        frozen = item["in_sample_full_fit_only_not_validation"]["frozen_scorer"]
        assert frozen["in_sample_full_fit_only_not_validation"] is True
        assert frozen["C"] == item["C"]
        targeted = item["targeted_decision_readouts"]
        for key in (
            "high_ml_low_bridge_score",
            "high_bridge_score_low_ml",
            "promoted_by_hybrid",
            "demoted_by_hybrid",
        ):
            assert "verdict" in targeted[key]


def test_selection_logic_marks_ready_false_when_no_c_passes_gates() -> None:
    results = [
        {"C": 1.0, "oof_roc_auc": 0.69, "in_sample_auc_minus_oof_auc": 0.1},
        {"C": 0.1, "oof_roc_auc": 0.69, "in_sample_auc_minus_oof_auc": 0.2},
        {"C": 0.01, "oof_roc_auc": 0.65, "in_sample_auc_minus_oof_auc": 0.05},
        {"C": 0.001, "oof_roc_auc": 0.64, "in_sample_auc_minus_oof_auc": 0.04},
        {"C": 0.0001, "oof_roc_auc": 0.63, "in_sample_auc_minus_oof_auc": 0.03},
    ]
    _mark_too_strong_regularization(results)
    selection = _selection_from_sweep(results)

    assert selection["ready_for_offline_hybrid_eval"] is False
    assert selection["selected_frozen_coefficient_C"] is None
    assert all(item["acceptable_for_offline_hybrid_eval"] is False for item in results)


def test_selection_logic_selects_acceptable_c_with_best_oof_auc() -> None:
    results = [
        {"C": 1.0, "oof_roc_auc": 0.72, "in_sample_auc_minus_oof_auc": 0.28},
        {"C": 0.1, "oof_roc_auc": 0.73, "in_sample_auc_minus_oof_auc": 0.27},
        {"C": 0.01, "oof_roc_auc": 0.74, "in_sample_auc_minus_oof_auc": 0.26},
        {"C": 0.001, "oof_roc_auc": 0.75, "in_sample_auc_minus_oof_auc": 0.25},
        {"C": 0.0001, "oof_roc_auc": 0.70, "in_sample_auc_minus_oof_auc": 0.3},
    ]
    _mark_too_strong_regularization(results)
    selection = _selection_from_sweep(results)

    assert selection["ready_for_offline_hybrid_eval"] is True
    assert selection["selected_frozen_coefficient_C"] == 0.001


def test_baseline_reference_records_v3_path_and_sha256(tmp_path: Path) -> None:
    baseline_path = tmp_path / "docs/audit/ml-offline-bridge-recommendable-scorer-v3.json"
    baseline_path.parent.mkdir(parents=True)
    baseline_path.write_text(
        json.dumps(
            {
                "artifact_type": "ml_offline_bridge_recommendable_scorer_v3",
                "scorer_version": "ml-offline-bridge-recommendable-scorer-v3",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    reference = _load_baseline_reference(baseline_path)

    assert reference["name"] == "v3_baseline_scorer"
    assert reference["path"].endswith("docs/audit/ml-offline-bridge-recommendable-scorer-v3.json")
    assert reference["sha256"] == sha256_file(baseline_path)
    assert reference["baseline_C"] == 1.0
    assert reference["read_only_reference"] is True


class _FakeCursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()) -> "_FakeCursor":
        lowered = sql.strip().lower()
        assert lowered.startswith("select")
        for write_word in ("insert", "update", "delete", "drop", "alter", "truncate", "merge"):
            assert write_word not in lowered
        self.executed.append((sql, params))
        return self

    def fetchall(self) -> list[dict]:
        return list(self.rows)


class _FakeCursorContext:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor = cursor

    def __enter__(self) -> _FakeCursor:
        return self.cursor

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self.cursor_obj = cursor

    def cursor(self, row_factory: object | None = None) -> _FakeCursorContext:
        return _FakeCursorContext(self.cursor_obj)


def test_embedding_load_is_select_only_for_tiny_fixture() -> None:
    cursor = _FakeCursor(
        [
            {"work_id": 1, "vector": json.dumps([1.0, 0.0])},
            {"work_id": 2, "vector": json.dumps([-1.0, 0.0])},
        ]
    )
    vectors = _load_slice_embeddings_select_only(
        _FakeConn(cursor),
        internal_work_ids=[1, 2],
        expected_dimensions=2,
    )

    assert vectors == {1: [1.0, 0.0], 2: [-1.0, 0.0]}
    assert len(cursor.executed) == 1

"""Tests for bridge-objective-experiment-compare."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.bridge_objective_experiment_compare import (
    BridgeObjectiveExperimentCompareError,
    build_bridge_objective_experiment_compare_payload,
)
from pipeline.bridge_weight_experiment_compare import _RunProvenance


def _run(
    rid: str,
    *,
    mode: str,
    weight: float,
) -> _RunProvenance:
    return _RunProvenance(
        ranking_run_id=rid,
        ranking_version="v",
        corpus_snapshot_version="snap",
        embedding_version="ev",
        cluster_version="cv",
        bridge_eligibility_mode=mode,
        bridge_weight_for_family_bridge=weight,
        status="succeeded",
    )


@patch("pipeline.bridge_objective_experiment_compare._fetch_top_k_rows", return_value=[])
@patch("pipeline.bridge_objective_experiment_compare._load_labeled_baseline_paper_ids", return_value=set())
@patch("pipeline.bridge_objective_experiment_compare._load_run")
def test_payload_rejects_same_eligibility_mode(mock_load: MagicMock, *_: MagicMock) -> None:
    mock_load.side_effect = [
        _run("a", mode="current", weight=0.0),
        _run("b", mode="current", weight=0.0),
    ]
    with pytest.raises(BridgeObjectiveExperimentCompareError, match="must differ"):
        build_bridge_objective_experiment_compare_payload(
            MagicMock(),
            baseline_ranking_run_id="a",
            experiment_ranking_run_id="b",
            k=20,
            baseline_bridge_worksheet_path=Path("x.csv"),
        )


@patch("pipeline.bridge_objective_experiment_compare._fetch_top_k_rows", return_value=[])
@patch("pipeline.bridge_objective_experiment_compare._load_labeled_baseline_paper_ids", return_value=set())
@patch("pipeline.bridge_objective_experiment_compare._load_run")
def test_payload_rejects_different_bridge_weight(mock_load: MagicMock, *_: MagicMock) -> None:
    mock_load.side_effect = [
        _run("a", mode="top50_cross_cluster_gte_0_40", weight=0.0),
        _run("b", mode="current", weight=0.05),
    ]
    with pytest.raises(BridgeObjectiveExperimentCompareError, match="same-weight"):
        build_bridge_objective_experiment_compare_payload(
            MagicMock(),
            baseline_ranking_run_id="a",
            experiment_ranking_run_id="b",
            k=20,
            baseline_bridge_worksheet_path=Path("x.csv"),
        )


@patch("pipeline.bridge_objective_experiment_compare._fetch_top_k_rows", return_value=[])
@patch("pipeline.bridge_objective_experiment_compare._load_labeled_baseline_paper_ids", return_value=set())
@patch("pipeline.bridge_objective_experiment_compare._load_run")
def test_payload_includes_review_kind(mock_load: MagicMock, *_: MagicMock) -> None:
    mock_load.side_effect = [
        _run("a", mode="top50_cross_cluster_gte_0_40", weight=0.0),
        _run("b", mode="top50_cross040_exclude_persistent_shared_v1", weight=0.0),
    ]
    p = build_bridge_objective_experiment_compare_payload(
        MagicMock(),
        baseline_ranking_run_id="a",
        experiment_ranking_run_id="b",
        k=20,
        baseline_bridge_worksheet_path=Path("x.csv"),
    )
    assert p["review_kind"] == "bridge_objective_experiment_compare"
    assert p["same_stack_check"]["bridge_eligibility_modes_differ"] is True
    assert p["same_stack_check"]["same_bridge_weight_for_family_bridge"] is True
    assert p["decision"]["ready_for_default"] is False

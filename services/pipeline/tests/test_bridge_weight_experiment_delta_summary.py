"""Tests for bridge-weight-experiment-delta-summary."""

from __future__ import annotations

import csv
import io
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main

from pipeline.bridge_weight_experiment_delta_summary import (
    BridgeWeightExperimentDeltaSummaryError,
    EXPECTED_BASELINE_RANKING_RUN_ID,
    EXPECTED_EXPERIMENT_RANKING_RUN_ID,
    build_bridge_weight_experiment_delta_summary_payload,
    markdown_from_bridge_weight_experiment_delta_summary,
)
from pipeline.bridge_weight_experiment_delta_worksheet import DELTA_WORKSHEET_COLUMNS


def _row(
    *,
    paper_id: str = "https://openalex.org/W1",
    rank: int = 1,
    rel: str = "good",
    nov: str = "useful",
    bridge: str = "yes",
    baseline: str = EXPECTED_BASELINE_RANKING_RUN_ID,
    experiment: str = EXPECTED_EXPERIMENT_RANKING_RUN_ID,
) -> dict[str, str]:
    return {
        "baseline_ranking_run_id": baseline,
        "experiment_ranking_run_id": experiment,
        "experiment_rank": str(rank),
        "paper_id": paper_id,
        "title": "t",
        "year": "2025",
        "citation_count": "0",
        "source_slug": "jaes",
        "topics": "x",
        "final_score": "0.5",
        "bridge_score": "0.9",
        "reason_short": "r",
        "relevance_label": rel,
        "novelty_label": nov,
        "bridge_like_label": bridge,
        "reviewer_notes": "",
    }


def test_incomplete_label_fails() -> None:
    rows = [
        _row(),
        _row(rank=2, paper_id="https://openalex.org/W2"),
        _row(rank=3, paper_id="https://openalex.org/W3"),
        _row(rank=4, paper_id="https://openalex.org/W4", rel=""),
    ]
    with pytest.raises(BridgeWeightExperimentDeltaSummaryError, match="blank"):
        build_bridge_weight_experiment_delta_summary_payload(rows, input_path=Path("x.csv"))


def test_wrong_baseline_run_id_fails() -> None:
    rows = [
        _row(baseline="wrong"),
        _row(rank=2, paper_id="https://openalex.org/W2"),
        _row(rank=3, paper_id="https://openalex.org/W3"),
        _row(rank=4, paper_id="https://openalex.org/W4"),
    ]
    with pytest.raises(BridgeWeightExperimentDeltaSummaryError, match="baseline_ranking_run_id"):
        build_bridge_weight_experiment_delta_summary_payload(rows, input_path=Path("x.csv"))


def test_wrong_experiment_run_id_fails() -> None:
    rows = [
        _row(experiment="wrong"),
        _row(rank=2, paper_id="https://openalex.org/W2"),
        _row(rank=3, paper_id="https://openalex.org/W3"),
        _row(rank=4, paper_id="https://openalex.org/W4"),
    ]
    with pytest.raises(BridgeWeightExperimentDeltaSummaryError, match="experiment_ranking_run_id"):
        build_bridge_weight_experiment_delta_summary_payload(rows, input_path=Path("x.csv"))


def test_row_count_not_four_fails() -> None:
    with pytest.raises(BridgeWeightExperimentDeltaSummaryError, match="exactly 4 data rows"):
        build_bridge_weight_experiment_delta_summary_payload([_row()], input_path=Path("x.csv"))


def test_metrics_and_pass_gates() -> None:
    rows = [
        _row(paper_id="https://openalex.org/A", rel="acceptable", nov="surprising", bridge="yes"),
        _row(rank=2, paper_id="https://openalex.org/B", rel="good", nov="surprising", bridge="yes"),
        _row(rank=3, paper_id="https://openalex.org/C", rel="acceptable", nov="useful", bridge="partial"),
        _row(rank=4, paper_id="https://openalex.org/D", rel="good", nov="useful", bridge="partial"),
    ]
    p = build_bridge_weight_experiment_delta_summary_payload(rows, input_path=Path("delta.csv"))
    assert p["row_count"] == 4
    m = p["metrics"]
    assert m["good_count"] == 2
    assert m["acceptable_count"] == 2
    assert m["good_or_acceptable_count"] == 4
    assert m["useful_or_surprising_count"] == 4
    assert m["bridge_like_yes_or_partial_count"] == 4
    assert m["miss_or_irrelevant_count"] == 0
    assert m["bridge_like_no_count"] == 0
    assert m["good_or_acceptable_share"] == 1.0
    assert m["useful_or_surprising_share"] == 1.0
    assert m["bridge_like_yes_or_partial_share"] == 1.0
    assert p["gates"]["experiment_quality_gate_pass"] is True
    assert "preserved quality" in p["decision"]["text"]
    assert p["decision"]["ready_for_default"] is False


def test_gates_fail_when_relevance_weak() -> None:
    rows = [
        _row(paper_id="https://openalex.org/a", rel="miss"),
        _row(rank=2, paper_id="https://openalex.org/b", rel="miss"),
        _row(rank=3, paper_id="https://openalex.org/c", rel="good"),
        _row(rank=4, paper_id="https://openalex.org/d", rel="good"),
    ]
    p = build_bridge_weight_experiment_delta_summary_payload(rows, input_path=Path("delta.csv"))
    assert p["metrics"]["good_or_acceptable_share"] == 0.5
    assert p["gates"]["delta_quality_pass"] is False
    assert p["gates"]["experiment_quality_gate_pass"] is False
    assert "did not both pass" in p["decision"]["text"]


def test_markdown_includes_not_validation_and_default_caveat() -> None:
    rows = [
        _row(),
        _row(rank=2, paper_id="https://openalex.org/W2"),
        _row(rank=3, paper_id="https://openalex.org/W3"),
        _row(rank=4, paper_id="https://openalex.org/W4"),
    ]
    p = build_bridge_weight_experiment_delta_summary_payload(rows, input_path=Path("d.csv"))
    md = markdown_from_bridge_weight_experiment_delta_summary(p)
    low = md.lower()
    assert "does **not** validate" in md or "not validation" in low
    assert "default" in low
    assert "ready_for_default" in low


def test_cli_writes_json_and_md(tmp_path: Path) -> None:
    pth = tmp_path / "delta.csv"
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(DELTA_WORKSHEET_COLUMNS))
    w.writeheader()
    for i, (rel, nov, bl) in enumerate(
        [
            ("acceptable", "surprising", "yes"),
            ("good", "surprising", "yes"),
            ("acceptable", "useful", "partial"),
            ("good", "useful", "partial"),
        ],
        start=1,
    ):
        w.writerow(
            {
                "baseline_ranking_run_id": EXPECTED_BASELINE_RANKING_RUN_ID,
                "experiment_ranking_run_id": EXPECTED_EXPERIMENT_RANKING_RUN_ID,
                "experiment_rank": str(10 + i),
                "paper_id": f"https://openalex.org/W{i}",
                "title": "t",
                "year": "2025",
                "citation_count": "0",
                "source_slug": "jaes",
                "topics": "x",
                "final_score": "0.5",
                "bridge_score": "0.9",
                "reason_short": "r",
                "relevance_label": rel,
                "novelty_label": nov,
                "bridge_like_label": bl,
                "reviewer_notes": "",
            }
        )
    pth.write_text(buf.getvalue(), encoding="utf-8")
    out = tmp_path / "out.json"
    md = tmp_path / "out.md"
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "bridge-weight-experiment-delta-summary",
            "--input",
            str(pth),
            "--output",
            str(out),
            "--markdown-output",
            str(md),
        ],
    ):
        cli_main.main()
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["row_count"] == 4
    assert md.is_file()

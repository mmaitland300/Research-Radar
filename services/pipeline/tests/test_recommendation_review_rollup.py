"""Tests for recommendation review rollup artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
from pipeline.recommendation_review_rollup import (
    ReviewRollupError,
    build_recommendation_review_rollup,
    run_recommendation_review_rollup,
)


def _summary(*, family: str, run_id: str = "rank-1", complete: bool = True) -> dict:
    bridge_share = None
    if family == "bridge":
        bridge_share = 0.75
    return {
        "input_path": f"/tmp/{family}.csv",
        "generated_at": "2026-01-01T00:00:00+00:00",
        "row_count": 20,
        "is_complete": complete,
        "ranking_run_id": [run_id],
        "ranking_version": ["rv-1"],
        "corpus_snapshot_version": ["snap-1"],
        "embedding_version": ["emb-1"],
        "cluster_version": ["cl-1"],
        "family": [family],
        "label_counts": {
            "relevance_label": {"good": 10, "acceptable": 10, "miss": 0, "irrelevant": 0},
            "novelty_label": {"surprising": 5, "useful": 15, "obvious": 0, "not_useful": 0},
            "bridge_like_label": {"yes": 0, "partial": 0, "no": 0, "not_applicable": 20},
        },
        "metrics": {
            "precision_at_k_good_only": 0.5 if family == "undercited" else 0.9,
            "precision_at_k_good_or_acceptable": 1.0,
            "bridge_like_yes_or_partial_share": bridge_share,
            "surprising_or_useful_share": 1.0,
        },
        "warnings": [],
    }


def test_successful_rollup_with_three_families() -> None:
    rollup = build_recommendation_review_rollup(
        [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited")],
        source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
    )
    assert rollup["family_count"] == 3
    assert set(rollup["families_present"]) == {"emerging", "bridge", "undercited"}
    assert rollup["provenance"]["ranking_run_id"] == "rank-1"
    assert rollup["bridge_specific"]["bridge_like_yes_or_partial_share"] == 0.75
    assert rollup["readiness"]["ready_for_distinctness_analysis"] is True
    assert rollup["readiness"]["ready_for_weight_experiment"] is True
    assert "candidate signal only" in rollup["readiness"]["suggested_next_step"]


def test_mismatched_ranking_run_id_fails() -> None:
    with pytest.raises(ReviewRollupError):
        build_recommendation_review_rollup(
            [_summary(family="emerging", run_id="rank-1"), _summary(family="bridge", run_id="rank-2")],
            source_paths=[Path("a.json"), Path("b.json")],
        )


def test_incomplete_summary_fails() -> None:
    with pytest.raises(ReviewRollupError):
        build_recommendation_review_rollup(
            [_summary(family="emerging", complete=False), _summary(family="bridge")],
            source_paths=[Path("a.json"), Path("b.json")],
        )


def test_duplicate_family_fails() -> None:
    with pytest.raises(ReviewRollupError):
        build_recommendation_review_rollup(
            [_summary(family="bridge"), _summary(family="bridge")],
            source_paths=[Path("a.json"), Path("b.json")],
        )


def test_missing_bridge_summary_warns_and_limits_readiness() -> None:
    rollup = build_recommendation_review_rollup(
        [_summary(family="emerging"), _summary(family="undercited")],
        source_paths=[Path("e.json"), Path("u.json")],
    )
    assert any("bridge family summary missing" in x for x in rollup["warnings"])
    assert rollup["readiness"]["ready_for_distinctness_analysis"] is False
    assert rollup["readiness"]["ready_for_weight_experiment"] is False


def test_readiness_logic_false_when_bridge_threshold_not_met() -> None:
    bridge = _summary(family="bridge")
    bridge["metrics"]["precision_at_k_good_or_acceptable"] = 0.7
    rollup = build_recommendation_review_rollup(
        [_summary(family="emerging"), bridge],
        source_paths=[Path("e.json"), Path("b.json")],
    )
    assert rollup["readiness"]["ready_for_distinctness_analysis"] is True
    assert rollup["readiness"]["ready_for_weight_experiment"] is False


def test_markdown_output_written(tmp_path: Path) -> None:
    e = tmp_path / "e.json"
    b = tmp_path / "b.json"
    out = tmp_path / "rollup.json"
    md = tmp_path / "rollup.md"
    e.write_text(json.dumps(_summary(family="emerging")), encoding="utf-8")
    b.write_text(json.dumps(_summary(family="bridge")), encoding="utf-8")
    run_recommendation_review_rollup(
        summary_paths=[e, b],
        output_path=out,
        markdown_path=md,
    )
    assert out.is_file()
    assert md.is_file()
    assert "# Recommendation review rollup" in md.read_text(encoding="utf-8")


def test_cli_rollup_writes_outputs(tmp_path: Path) -> None:
    e = tmp_path / "e.json"
    b = tmp_path / "b.json"
    u = tmp_path / "u.json"
    out = tmp_path / "rollup.json"
    md = tmp_path / "rollup.md"
    e.write_text(json.dumps(_summary(family="emerging")), encoding="utf-8")
    b.write_text(json.dumps(_summary(family="bridge")), encoding="utf-8")
    u.write_text(json.dumps(_summary(family="undercited")), encoding="utf-8")
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-rollup",
            "--summary",
            str(e),
            "--summary",
            str(b),
            "--summary",
            str(u),
            "--output",
            str(out),
            "--markdown-output",
            str(md),
        ],
    ):
        cli_main.main()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["family_count"] == 3
    assert md.is_file()


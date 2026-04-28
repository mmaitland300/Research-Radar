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
    markdown_from_rollup,
    run_recommendation_review_rollup,
)


def _summary(*, family: str, run_id: str = "rank-1", complete: bool = True, cluster_version: str = "cl-1") -> dict:
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
        "cluster_version": [cluster_version],
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


def _diagnostics() -> dict:
    return {
        "overlap_detail": {
            "bridge_vs_emerging_jaccard": 0.73913,
            "eligible_bridge_vs_emerging_jaccard": 0.212121,
            "emerging_overlap_delta_from_full_to_eligible": 0.527009,
        },
        "diagnosis": {
            "eligible_head_differs_from_full": True,
            "eligible_head_less_emerging_like_than_full": True,
            "eligible_distinctness_improves_by_threshold": True,
        },
    }


def _bridge_rows(*, run_id: str = "rank-1", eligible: str = "true", variant: str = "bridge_eligible_only") -> list[dict[str, str]]:
    return [
        {
            "ranking_run_id": run_id,
            "family": "bridge",
            "review_pool_variant": variant,
            "bridge_eligible": eligible,
        },
        {
            "ranking_run_id": run_id,
            "family": "bridge",
            "review_pool_variant": variant,
            "bridge_eligible": eligible,
        },
    ]


def test_successful_rollup_with_three_families() -> None:
    rollup = build_recommendation_review_rollup(
        [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited")],
        source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
        bridge_diagnostics=_diagnostics(),
        bridge_worksheet_rows=_bridge_rows(),
    )
    assert rollup["family_count"] == 3
    assert set(rollup["families_present"]) == {"emerging", "bridge", "undercited"}
    assert rollup["provenance"]["ranking_run_id"] == "rank-1"
    assert rollup["bridge_specific"]["bridge_like_yes_or_partial_share"] == 0.75
    assert rollup["readiness"]["label_quality_ready"] is True
    assert rollup["readiness"]["bridge_like_ready"] is True
    assert rollup["readiness"]["distinctness_ready"] is True
    assert rollup["readiness"]["family_quality_context_ready"] is True
    assert rollup["readiness"]["ready_for_small_bridge_weight_experiment"] is True
    assert "small gated bridge-weight experiment; not validation" in rollup["readiness"]["suggested_next_step"]
    assert rollup["bridge_review_pool_validation"]["review_pool_variant"] == "bridge_eligible_only"


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


def test_expected_family_set_required() -> None:
    with pytest.raises(ReviewRollupError, match="Expected exactly families"):
        build_recommendation_review_rollup(
            [_summary(family="emerging"), _summary(family="bridge")],
            source_paths=[Path("e.json"), Path("b.json")],
        )


def test_readiness_logic_false_when_bridge_threshold_not_met() -> None:
    bridge = _summary(family="bridge")
    bridge["metrics"]["precision_at_k_good_or_acceptable"] = 0.7
    rollup = build_recommendation_review_rollup(
        [_summary(family="emerging"), bridge, _summary(family="undercited")],
        source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
        bridge_diagnostics=_diagnostics(),
        bridge_worksheet_rows=_bridge_rows(),
    )
    assert rollup["readiness"]["ready_for_small_bridge_weight_experiment"] is False
    assert "label_quality_ready" in rollup["readiness"]["failed_gates"]


def test_mismatched_cluster_version_fails() -> None:
    with pytest.raises(ReviewRollupError, match="Provenance mismatch"):
        build_recommendation_review_rollup(
            [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited", cluster_version="other")],
            source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
        )


def test_bridge_worksheet_variant_must_be_eligible_only() -> None:
    with pytest.raises(ReviewRollupError, match="review_pool_variant"):
        build_recommendation_review_rollup(
            [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited")],
            source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
            bridge_diagnostics=_diagnostics(),
            bridge_worksheet_rows=_bridge_rows(variant="full_family_top_k"),
        )


def test_bridge_worksheet_requires_true_bridge_eligible() -> None:
    with pytest.raises(ReviewRollupError, match="bridge_eligible"):
        build_recommendation_review_rollup(
            [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited")],
            source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
            bridge_diagnostics=_diagnostics(),
            bridge_worksheet_rows=_bridge_rows(eligible=""),
        )


def test_missing_distinctness_fields_fail() -> None:
    bad = {"overlap_detail": {}, "diagnosis": {}}
    with pytest.raises(ReviewRollupError, match="missing overlap_detail"):
        build_recommendation_review_rollup(
            [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited")],
            source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
            bridge_diagnostics=bad,
            bridge_worksheet_rows=_bridge_rows(),
        )


def test_markdown_includes_not_validation_and_single_reviewer() -> None:
    rollup = build_recommendation_review_rollup(
        [_summary(family="emerging"), _summary(family="bridge"), _summary(family="undercited")],
        source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
        bridge_diagnostics=_diagnostics(),
        bridge_worksheet_rows=_bridge_rows(),
    )
    md = markdown_from_rollup(rollup)
    assert "Single-reviewer, top-20, offline evidence." in md
    assert "not validation" in md


def test_old_run_id_rejected_for_this_rollup() -> None:
    with pytest.raises(ReviewRollupError, match="Provenance mismatch"):
        build_recommendation_review_rollup(
            [_summary(family="emerging", run_id="rank-3904fec89d"), _summary(family="bridge"), _summary(family="undercited")],
            source_paths=[Path("e.json"), Path("b.json"), Path("u.json")],
        )


def test_markdown_output_written(tmp_path: Path) -> None:
    e = tmp_path / "e.json"
    b = tmp_path / "b.json"
    out = tmp_path / "rollup.json"
    md = tmp_path / "rollup.md"
    e.write_text(json.dumps(_summary(family="emerging")), encoding="utf-8")
    b.write_text(json.dumps(_summary(family="bridge")), encoding="utf-8")
    u = tmp_path / "u.json"
    d = tmp_path / "diag.json"
    w = tmp_path / "bridge.csv"
    u.write_text(json.dumps(_summary(family="undercited")), encoding="utf-8")
    d.write_text(json.dumps(_diagnostics()), encoding="utf-8")
    w.write_text(
        "ranking_run_id,family,review_pool_variant,bridge_eligible\n"
        "rank-1,bridge,bridge_eligible_only,true\n",
        encoding="utf-8",
    )
    run_recommendation_review_rollup(
        summary_paths=[e, b, u],
        output_path=out,
        markdown_path=md,
        bridge_diagnostics_path=d,
        bridge_worksheet_path=w,
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
    d = tmp_path / "diag.json"
    w = tmp_path / "bridge.csv"
    e.write_text(json.dumps(_summary(family="emerging")), encoding="utf-8")
    b.write_text(json.dumps(_summary(family="bridge")), encoding="utf-8")
    u.write_text(json.dumps(_summary(family="undercited")), encoding="utf-8")
    d.write_text(json.dumps(_diagnostics()), encoding="utf-8")
    w.write_text(
        "ranking_run_id,family,review_pool_variant,bridge_eligible\n"
        "rank-1,bridge,bridge_eligible_only,true\n",
        encoding="utf-8",
    )
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "recommendation-review-rollup",
            "--bridge-summary",
            str(b),
            "--emerging-summary",
            str(e),
            "--undercited-summary",
            str(u),
            "--bridge-diagnostics",
            str(d),
            "--bridge-worksheet",
            str(w),
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


"""Tests for bridge-weight response rollup (zero / w005 / w010 synthesis)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.bridge_weight_response_rollup import (
    BridgeWeightResponseRollupError,
    build_bridge_weight_response_rollup_payload,
    markdown_from_bridge_weight_response_rollup,
    run_bridge_weight_response_rollup,
)

STACK = {
    "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
    "embedding_version": "v2-title-abstract-1536-cleantext-r1",
    "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
    "bridge_eligibility_mode": "top50_cross_cluster_gte_0_40",
}

RANK_ZERO = "rank-ee2ba6c816"
RANK_W005 = "rank-bc1123e00c"
RANK_W010 = "rank-9a02c81d40"


def _overlap(sb: set[int], se: set[int]) -> dict[str, float | int]:
    inter = sb & se
    uni = sb | se
    return {
        "jaccard": len(inter) / len(uni) if uni else 1.0,
        "overlap_count": len(inter),
        "union_count": len(uni),
    }


def _prov(wb: float, we: float, rid_b: str, rid_e: str, *, stack: dict[str, str] | None = None) -> dict:
    s = stack or STACK
    return {
        "baseline": {
            **s,
            "ranking_run_id": rid_b,
            "bridge_weight_for_family_bridge": wb,
            "ranking_version": f"rv-{rid_b}",
            "status": "succeeded",
        },
        "experiment": {
            **s,
            "ranking_run_id": rid_e,
            "bridge_weight_for_family_bridge": we,
            "ranking_version": f"rv-{rid_e}",
            "status": "succeeded",
        },
        "k": 20,
    }


def _cmp(
    *,
    wb: float,
    we: float,
    rid_b: str,
    rid_e: str,
    base_elig: list[int],
    exp_elig: list[int],
    dz_b: float,
    dz_e: float,
    em_ch: bool = False,
    uc_ch: bool = False,
    quality_risk: dict | None = None,
    stack: dict[str, str] | None = None,
) -> dict:
    sb = set(base_elig)
    se = set(exp_elig)
    return {
        "provenance": _prov(wb, we, rid_b, rid_e, stack=stack),
        "bridge_top_k_comparison": {
            "baseline_eligible_bridge_top_k_ids": base_elig,
            "experiment_eligible_bridge_top_k_ids": exp_elig,
            "eligible_bridge_overlap": _overlap(sb, se),
        },
        "emerging_comparison": {"changed": em_ch},
        "undercited_comparison": {"changed": uc_ch},
        "distinctness": {
            "baseline_eligible_bridge_vs_emerging_jaccard": dz_b,
            "experiment_eligible_bridge_vs_emerging_jaccard": dz_e,
        },
        "quality_risk": quality_risk or {},
    }


def _rollup() -> dict:
    return {
        "provenance": {
            "ranking_run_id": RANK_ZERO,
            "corpus_snapshot_version": STACK["corpus_snapshot_version"],
            "embedding_version": STACK["embedding_version"],
            "cluster_version": STACK["cluster_version"],
        },
        "per_family": {
            "bridge": {
                "metrics": {"precision_at_k_good_only": 0.85},
            },
        },
    }


def _delta_summary() -> dict:
    return {"metrics": {"rows_reviewed": 4}, "gates": {"acceptable": True}}


@pytest.fixture
def label_csvs(tmp_path: Path) -> tuple[Path, Path]:
    base = tmp_path / "base.csv"
    delta = tmp_path / "delta.csv"
    base.write_text("paper_id\nW1\nW2\n", encoding="utf-8")
    delta.write_text("paper_id\nW3\n", encoding="utf-8")
    return base, delta


def test_rejects_mismatched_stack(label_csvs: tuple[Path, Path]) -> None:
    base_csv, delta_csv = label_csvs
    bad_stack = {**STACK, "cluster_version": "other-cluster"}
    c01 = _cmp(
        wb=0.0,
        we=0.05,
        rid_b=RANK_ZERO,
        rid_e=RANK_W005,
        base_elig=[1, 2],
        exp_elig=[1, 2],
        dz_b=0.15,
        dz_e=0.14,
    )
    c12 = _cmp(
        wb=0.05,
        we=0.10,
        rid_b=RANK_W005,
        rid_e=RANK_W010,
        base_elig=[1, 2],
        exp_elig=[1, 2],
        dz_b=0.14,
        dz_e=0.13,
        stack=bad_stack,
    )
    c02 = _cmp(
        wb=0.0,
        we=0.10,
        rid_b=RANK_ZERO,
        rid_e=RANK_W010,
        base_elig=[1, 2],
        exp_elig=[1, 2],
        dz_b=0.15,
        dz_e=0.13,
    )
    with pytest.raises(BridgeWeightResponseRollupError, match="stack mismatch"):
        build_bridge_weight_response_rollup_payload(
            baseline_review_rollup=_rollup(),
            compare_zero_vs_w005=c01,
            delta_review_summary=_delta_summary(),
            compare_w005_vs_w010=c12,
            compare_zero_vs_w010=c02,
            labeled_baseline_bridge_worksheet=base_csv,
            delta_review_csv=delta_csv,
            conn=None,
        )


def test_saturated_when_w005_vs_w010_jaccard_one(label_csvs: tuple[Path, Path]) -> None:
    base_csv, delta_csv = label_csvs
    elig = [1, 2, 3, 4]
    c01 = _cmp(
        wb=0.0,
        we=0.05,
        rid_b=RANK_ZERO,
        rid_e=RANK_W005,
        base_elig=[1, 2],
        exp_elig=[2, 3],
        dz_b=0.15,
        dz_e=0.14,
    )
    c12 = _cmp(
        wb=0.05,
        we=0.10,
        rid_b=RANK_W005,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.14,
        dz_e=0.13,
        quality_risk={"unlabeled_new_experiment_eligible_top_k_count": 0},
    )
    c02 = _cmp(
        wb=0.0,
        we=0.10,
        rid_b=RANK_ZERO,
        rid_e=RANK_W010,
        base_elig=[1, 2],
        exp_elig=[2, 3],
        dz_b=0.15,
        dz_e=0.13,
    )
    payload = build_bridge_weight_response_rollup_payload(
        baseline_review_rollup=_rollup(),
        compare_zero_vs_w005=c01,
        delta_review_summary=_delta_summary(),
        compare_w005_vs_w010=c12,
        compare_zero_vs_w010=c02,
        labeled_baseline_bridge_worksheet=base_csv,
        delta_review_csv=delta_csv,
        conn=None,
    )
    assert payload["decision"]["weight_response_saturated"] is True
    assert payload["decision"]["recommend_next_weight_increase"] is False
    assert "0.10 did not improve eligible top-20 membership" in payload["decision"]["recommendation_text"]
    assert payload["movement"]["eligible_bridge_jaccard"]["w005_vs_w010"] == 1.0


def test_controlled_movement_from_zero_to_w005(label_csvs: tuple[Path, Path]) -> None:
    base_csv, delta_csv = label_csvs
    c01 = _cmp(
        wb=0.0,
        we=0.05,
        rid_b=RANK_ZERO,
        rid_e=RANK_W005,
        base_elig=[10, 20],
        exp_elig=[20, 30],
        dz_b=0.15,
        dz_e=0.14,
    )
    c12 = _cmp(
        wb=0.05,
        we=0.10,
        rid_b=RANK_W005,
        rid_e=RANK_W010,
        base_elig=[20, 30],
        exp_elig=[20, 31],
        dz_b=0.14,
        dz_e=0.13,
        quality_risk={"unlabeled_new_experiment_eligible_top_k_count": 0},
    )
    c02 = _cmp(
        wb=0.0,
        we=0.10,
        rid_b=RANK_ZERO,
        rid_e=RANK_W010,
        base_elig=[10, 20],
        exp_elig=[20, 31],
        dz_b=0.15,
        dz_e=0.13,
    )
    payload = build_bridge_weight_response_rollup_payload(
        baseline_review_rollup=_rollup(),
        compare_zero_vs_w005=c01,
        delta_review_summary=_delta_summary(),
        compare_w005_vs_w010=c12,
        compare_zero_vs_w010=c02,
        labeled_baseline_bridge_worksheet=base_csv,
        delta_review_csv=delta_csv,
        conn=None,
    )
    assert payload["decision"]["weight_response_controlled"] is True
    assert payload["movement"]["eligible_bridge_sets"]["zero_vs_w005"]["new_eligible_work_ids"] == [30]


def test_ready_for_default_false(label_csvs: tuple[Path, Path]) -> None:
    base_csv, delta_csv = label_csvs
    elig = [1, 2]
    c01 = _cmp(
        wb=0.0,
        we=0.05,
        rid_b=RANK_ZERO,
        rid_e=RANK_W005,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.15,
        dz_e=0.14,
    )
    c12 = _cmp(
        wb=0.05,
        we=0.10,
        rid_b=RANK_W005,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.14,
        dz_e=0.13,
        quality_risk={"unlabeled_new_experiment_eligible_top_k_count": 0},
    )
    c02 = _cmp(
        wb=0.0,
        we=0.10,
        rid_b=RANK_ZERO,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.15,
        dz_e=0.13,
    )
    payload = build_bridge_weight_response_rollup_payload(
        baseline_review_rollup=_rollup(),
        compare_zero_vs_w005=c01,
        delta_review_summary=_delta_summary(),
        compare_w005_vs_w010=c12,
        compare_zero_vs_w010=c02,
        labeled_baseline_bridge_worksheet=base_csv,
        delta_review_csv=delta_csv,
        conn=None,
    )
    assert payload["decision"]["ready_for_default"] is False


def test_markdown_includes_not_validation_caveat(
    label_csvs: tuple[Path, Path],
) -> None:
    base_csv, delta_csv = label_csvs
    elig = [1, 2]
    c01 = _cmp(
        wb=0.0,
        we=0.05,
        rid_b=RANK_ZERO,
        rid_e=RANK_W005,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.15,
        dz_e=0.14,
    )
    c12 = _cmp(
        wb=0.05,
        we=0.10,
        rid_b=RANK_W005,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.14,
        dz_e=0.13,
        quality_risk={"unlabeled_new_experiment_eligible_top_k_count": 0},
    )
    c02 = _cmp(
        wb=0.0,
        we=0.10,
        rid_b=RANK_ZERO,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.15,
        dz_e=0.13,
    )
    payload = build_bridge_weight_response_rollup_payload(
        baseline_review_rollup=_rollup(),
        compare_zero_vs_w005=c01,
        delta_review_summary=_delta_summary(),
        compare_w005_vs_w010=c12,
        compare_zero_vs_w010=c02,
        labeled_baseline_bridge_worksheet=base_csv,
        delta_review_csv=delta_csv,
        conn=None,
    )
    md = markdown_from_bridge_weight_response_rollup(payload)
    lower = md.lower()
    assert "not" in lower and "validation" in lower


def test_run_bridge_weight_response_rollup_writes_json_only(tmp_path: Path, label_csvs: tuple[Path, Path]) -> None:
    base_csv, delta_csv = label_csvs
    elig = [1, 2]
    c01 = _cmp(
        wb=0.0,
        we=0.05,
        rid_b=RANK_ZERO,
        rid_e=RANK_W005,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.15,
        dz_e=0.14,
    )
    c12 = _cmp(
        wb=0.05,
        we=0.10,
        rid_b=RANK_W005,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.14,
        dz_e=0.13,
        quality_risk={"unlabeled_new_experiment_eligible_top_k_count": 0},
    )
    c02 = _cmp(
        wb=0.0,
        we=0.10,
        rid_b=RANK_ZERO,
        rid_e=RANK_W010,
        base_elig=elig,
        exp_elig=elig,
        dz_b=0.15,
        dz_e=0.13,
    )
    br = tmp_path / "rollup.json"
    dr = tmp_path / "rev.json"
    dz = tmp_path / "dz.json"
    c01p = tmp_path / "c01.json"
    c12p = tmp_path / "c12.json"
    c02p = tmp_path / "c02.json"
    br.write_text(json.dumps(_rollup()), encoding="utf-8")
    dr.write_text(json.dumps(_delta_summary()), encoding="utf-8")
    c01p.write_text(json.dumps(c01), encoding="utf-8")
    c12p.write_text(json.dumps(c12), encoding="utf-8")
    c02p.write_text(json.dumps(c02), encoding="utf-8")
    out = tmp_path / "out.json"
    md_out = tmp_path / "out.md"
    run_bridge_weight_response_rollup(
        baseline_review_rollup_path=br,
        compare_zero_vs_w005_path=c01p,
        delta_review_summary_path=dr,
        compare_w005_vs_w010_path=c12p,
        compare_zero_vs_w010_path=c02p,
        labeled_baseline_bridge_worksheet_path=base_csv,
        delta_review_csv_path=delta_csv,
        output_path=out,
        markdown_path=md_out,
        database_url="postgresql://invalid.invalid:1/db",
    )
    assert out.is_file()
    assert md_out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["decision"]["ready_for_default"] is False

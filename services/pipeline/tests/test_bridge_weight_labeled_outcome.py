"""Tests for bridge-weight-labeled-outcome rollup."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest

from pipeline.bridge_weight_labeled_outcome import (
    BridgeWeightLabeledOutcomeError,
    build_bridge_weight_labeled_outcome_payload,
    markdown_from_bridge_weight_labeled_outcome,
    run_bridge_weight_labeled_outcome,
)

STACK = {
    "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
    "embedding_version": "v2-title-abstract-1536-cleantext-r1",
    "cluster_version": "kmeans-l2-v2-cleantext-r1-k12",
    "bridge_eligibility_mode": "top50_cross_cluster_gte_0_40",
}

R0 = "rank-ee2ba6c816"
R5 = "rank-bc1123e00c"
R10 = "rank-9a02c81d40"


def _prov_pair(wb: float, we: float, rb: str, re: str) -> dict:
    return {
        "baseline": {
            **STACK,
            "ranking_run_id": rb,
            "bridge_weight_for_family_bridge": wb,
            "ranking_version": f"v-{rb}",
            "status": "succeeded",
        },
        "experiment": {
            **STACK,
            "ranking_run_id": re,
            "bridge_weight_for_family_bridge": we,
            "ranking_version": f"v-{re}",
            "status": "succeeded",
        },
        "k": 20,
    }


def _compare_json() -> tuple[dict, dict, dict]:
    c01 = {"provenance": _prov_pair(0.0, 0.05, R0, R5)}
    c12 = {"provenance": _prov_pair(0.05, 0.10, R5, R10)}
    c02 = {"provenance": _prov_pair(0.0, 0.10, R0, R10)}
    return c01, c12, c02


def _response_rollup(*, sat: bool) -> dict:
    j_w = 1.0 if sat else 0.5
    return {
        "stack": STACK,
        "movement": {
            "eligible_bridge_jaccard": {
                "zero_vs_w005": 0.5,
                "w005_vs_w010": j_w,
                "zero_vs_w010": 0.5,
            },
            "eligible_bridge_sets": {
                "zero_vs_w005": {"new_eligible_work_ids": [99], "dropped_eligible_work_ids": []},
                "w005_vs_w010": {"new_eligible_work_ids": [], "dropped_eligible_work_ids": []},
                "zero_vs_w010": {"new_eligible_work_ids": [99], "dropped_eligible_work_ids": []},
            },
        },
    }


def _diag(rid: str, *, eligible_ids: list[int], elig_j: float = 0.15, full_j: float = 0.4) -> dict:
    rows = []
    for i, wid in enumerate(eligible_ids, start=1):
        rows.append(
            {
                "rank": i,
                "work_id": wid,
                "paper_id": f"https://openalex.org/W{wid}",
                "bridge_eligible": True,
            }
        )
    return {
        "provenance": {
            "ranking_run_id": rid,
            "corpus_snapshot_version": STACK["corpus_snapshot_version"],
            "embedding_version": STACK["embedding_version"],
            "cluster_version": STACK["cluster_version"],
            "k": 20,
        },
        "bridge_top_k_rows": rows,
        "overlap_detail": {
            "eligible_bridge_top_k_ids": eligible_ids,
            "eligible_bridge_vs_emerging_jaccard": elig_j,
            "bridge_vs_emerging_jaccard": full_j,
        },
    }


def _write_baseline_csv(path: Path, *, n: int = 20, start_wid: int = 1) -> None:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "ranking_run_id",
            "ranking_version",
            "corpus_snapshot_version",
            "embedding_version",
            "cluster_version",
            "review_pool_variant",
            "family",
            "rank",
            "paper_id",
            "title",
            "year",
            "citation_count",
            "source_slug",
            "topics",
            "final_score",
            "reason_short",
            "semantic_score",
            "citation_velocity_score",
            "topic_growth_score",
            "bridge_score",
            "diversity_penalty",
            "bridge_eligible",
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
            "reviewer_notes",
        ]
    )
    for rank in range(1, n + 1):
        wid = start_wid + rank - 1
        pid = f"https://openalex.org/W{wid}"
        w.writerow(
            [
                R0,
                "rv",
                STACK["corpus_snapshot_version"],
                STACK["embedding_version"],
                STACK["cluster_version"],
                "bridge_eligible_only",
                "bridge",
                str(rank),
                pid,
                f"t{wid}",
                "2025",
                "0",
                "s",
                "t",
                "0.5",
                "r",
                "",
                "0",
                "1",
                "0.5",
                "0",
                "true",
                "good",
                "useful",
                "yes",
                "",
            ]
        )
    path.write_text(buf.getvalue(), encoding="utf-8")


def _write_delta_csv(path: Path, rows: list[tuple[str, str, str]]) -> None:
    """rows: list of (paper_id, relevance, bridge_like) — novelty fixed useful."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
            "baseline_ranking_run_id",
            "experiment_ranking_run_id",
            "experiment_rank",
            "paper_id",
            "title",
            "year",
            "citation_count",
            "source_slug",
            "topics",
            "final_score",
            "bridge_score",
            "reason_short",
            "relevance_label",
            "novelty_label",
            "bridge_like_label",
            "reviewer_notes",
        ]
    )
    for i, (pid, rel, bl) in enumerate(rows, start=1):
        w.writerow(
            [
                R0,
                R5,
                str(10 + i),
                pid,
                "t",
                "2025",
                "0",
                "s",
                "t",
                "0.5",
                "0.5",
                "r",
                rel,
                "useful",
                bl,
                "",
            ]
        )
    path.write_text(buf.getvalue(), encoding="utf-8")


def _paths(tmp: Path, **kwargs: dict) -> dict[str, Path]:
    out = {}
    for k, v in kwargs.items():
        p = tmp / f"{k}.json"
        p.write_text(json.dumps(v), encoding="utf-8")
        out[k] = p
    return out


def test_combines_baseline_and_delta_maps(tmp_path: Path) -> None:
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W999", "good", "yes")])
    c01, c12, c02 = _compare_json()
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=True))
    paths["dz0"] = tmp_path / "dz0.json"
    paths["dz0"].write_text(json.dumps(_diag(R0, eligible_ids=list(range(1, 21)))), encoding="utf-8")
    paths["dz5"] = tmp_path / "dz5.json"
    paths["dz5"].write_text(json.dumps(_diag(R5, eligible_ids=list(range(1, 21)))), encoding="utf-8")
    paths["dz10"] = tmp_path / "dz10.json"
    paths["dz10"].write_text(json.dumps(_diag(R10, eligible_ids=list(range(1, 21)))), encoding="utf-8")
    p = build_bridge_weight_labeled_outcome_payload(
        baseline_worksheet_path=tmp_path / "b.csv",
        delta_worksheet_path=tmp_path / "d.csv",
        response_rollup_path=paths["rr"],
        compare_zero_vs_w005_path=paths["c01"],
        compare_w005_vs_w010_path=paths["c12"],
        compare_zero_vs_w010_path=paths["c02"],
        diagnostics_paths={R0: paths["dz0"], R5: paths["dz5"], R10: paths["dz10"]},
        conn=None,
    )
    assert p["label_sources"]["label_map_paper_id_count"] == 21


def test_fails_on_conflicting_duplicate_labels(tmp_path: Path) -> None:
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W1", "acceptable", "yes")])
    c01, c12, c02 = _compare_json()
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=True))
    for name, rid in (("dz0", R0), ("dz5", R5), ("dz10", R10)):
        pth = tmp_path / f"{name}.json"
        pth.write_text(json.dumps(_diag(rid, eligible_ids=list(range(1, 21)))), encoding="utf-8")
        paths[name] = pth
    with pytest.raises(BridgeWeightLabeledOutcomeError, match="conflicting labels"):
        build_bridge_weight_labeled_outcome_payload(
            baseline_worksheet_path=tmp_path / "b.csv",
            delta_worksheet_path=tmp_path / "d.csv",
            response_rollup_path=paths["rr"],
            compare_zero_vs_w005_path=paths["c01"],
            compare_w005_vs_w010_path=paths["c12"],
            compare_zero_vs_w010_path=paths["c02"],
            diagnostics_paths={R0: paths["dz0"], R5: paths["dz5"], R10: paths["dz10"]},
            conn=None,
        )


def test_incomplete_coverage_lists_missing_without_pass_decisions(tmp_path: Path) -> None:
    """w005 head includes a paper_id with no label in baseline ∪ delta union."""
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W999", "good", "yes")])
    c01, c12, c02 = _compare_json()
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=True))
    elig = list(range(1, 21))
    rows0 = []
    for i, wid in enumerate(elig, start=1):
        rows0.append(
            {
                "rank": i,
                "work_id": wid,
                "paper_id": f"https://openalex.org/W{wid}",
                "bridge_eligible": True,
            }
        )
    dz0 = {
        "provenance": _diag(R0, eligible_ids=elig)["provenance"],
        "bridge_top_k_rows": rows0,
        "overlap_detail": _diag(R0, eligible_ids=elig)["overlap_detail"],
    }
    rows5 = []
    for i, wid in enumerate(elig, start=1):
        pid = f"https://openalex.org/W{wid}" if i < 20 else "https://openalex.org/W_NO_LABEL"
        rows5.append({"rank": i, "work_id": wid, "paper_id": pid, "bridge_eligible": True})
    od5 = {
        "eligible_bridge_top_k_ids": elig,
        "eligible_bridge_vs_emerging_jaccard": 0.15,
        "bridge_vs_emerging_jaccard": 0.4,
    }
    dz5 = {
        "provenance": _diag(R5, eligible_ids=elig)["provenance"],
        "bridge_top_k_rows": rows5,
        "overlap_detail": od5,
    }
    dz10 = json.loads(json.dumps(dz0))
    dz10["provenance"]["ranking_run_id"] = R10
    paths["dz0"] = tmp_path / "dz0.json"
    paths["dz0"].write_text(json.dumps(dz0), encoding="utf-8")
    paths["dz5"] = tmp_path / "dz5.json"
    paths["dz5"].write_text(json.dumps(dz5), encoding="utf-8")
    paths["dz10"] = tmp_path / "dz10.json"
    paths["dz10"].write_text(json.dumps(dz10), encoding="utf-8")
    p = build_bridge_weight_labeled_outcome_payload(
        baseline_worksheet_path=tmp_path / "b.csv",
        delta_worksheet_path=tmp_path / "d.csv",
        response_rollup_path=paths["rr"],
        compare_zero_vs_w005_path=paths["c01"],
        compare_w005_vs_w010_path=paths["c12"],
        compare_zero_vs_w010_path=paths["c02"],
        diagnostics_paths={R0: paths["dz0"], R5: paths["dz5"], R10: paths["dz10"]},
        conn=None,
    )
    assert p["coverage"]["all_runs_complete"] is False
    assert p["per_run"]["w005"]["coverage_complete"] is False
    assert any("W_NO_LABEL" in x for x in (p["per_run"]["w005"]["missing_paper_ids"] or []))
    assert p["per_run"]["w005"]["metrics"] is None
    assert p["decision"]["w005_quality_preserved"] is False
    assert p["decision"]["zero_quality_baseline_ready"] is True


def test_per_run_quality_metrics_and_saturation(tmp_path: Path) -> None:
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W999", "good", "yes")])
    c01, c12, c02 = _compare_json()
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=True))
    for name, rid in (("dz0", R0), ("dz5", R5), ("dz10", R10)):
        pth = tmp_path / f"{name}.json"
        pth.write_text(json.dumps(_diag(rid, eligible_ids=list(range(1, 21)))), encoding="utf-8")
        paths[name] = pth
    p = build_bridge_weight_labeled_outcome_payload(
        baseline_worksheet_path=tmp_path / "b.csv",
        delta_worksheet_path=tmp_path / "d.csv",
        response_rollup_path=paths["rr"],
        compare_zero_vs_w005_path=paths["c01"],
        compare_w005_vs_w010_path=paths["c12"],
        compare_zero_vs_w010_path=paths["c02"],
        diagnostics_paths={R0: paths["dz0"], R5: paths["dz5"], R10: paths["dz10"]},
        conn=None,
    )
    for k in ("zero", "w005", "w010"):
        m = p["per_run"][k]["metrics"]
        assert m["row_count"] == 20
        assert m["good_count"] == 20
        assert m["good_or_acceptable_share"] == 1.0
    assert p["decision"]["response_saturated"] is True
    assert p["decision"]["ready_for_default"] is False
    assert p["decision"]["recommend_next_weight_increase"] is False


def test_rejects_mismatched_stack(tmp_path: Path) -> None:
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W999", "good", "yes")])
    c01 = {"provenance": _prov_pair(0.0, 0.05, R0, R5)}
    c12 = {"provenance": _prov_pair(0.05, 0.10, R5, R10)}
    c02 = {"provenance": _prov_pair(0.0, 0.10, R0, R10)}
    c12["provenance"]["baseline"]["embedding_version"] = "other"
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=False))
    for name, rid in (("dz0", R0), ("dz5", R5), ("dz10", R10)):
        pth = tmp_path / f"{name}.json"
        pth.write_text(json.dumps(_diag(rid, eligible_ids=list(range(1, 21)))), encoding="utf-8")
        paths[name] = pth
    with pytest.raises(BridgeWeightLabeledOutcomeError, match="stack mismatch"):
        build_bridge_weight_labeled_outcome_payload(
            baseline_worksheet_path=tmp_path / "b.csv",
            delta_worksheet_path=tmp_path / "d.csv",
            response_rollup_path=paths["rr"],
            compare_zero_vs_w005_path=paths["c01"],
            compare_w005_vs_w010_path=paths["c12"],
            compare_zero_vs_w010_path=paths["c02"],
            diagnostics_paths={R0: paths["dz0"], R5: paths["dz5"], R10: paths["dz10"]},
            conn=None,
        )


def test_markdown_includes_not_validation(tmp_path: Path) -> None:
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W999", "good", "yes")])
    c01, c12, c02 = _compare_json()
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=True))
    for name, rid in (("dz0", R0), ("dz5", R5), ("dz10", R10)):
        pth = tmp_path / f"{name}.json"
        pth.write_text(json.dumps(_diag(rid, eligible_ids=list(range(1, 21)))), encoding="utf-8")
        paths[name] = pth
    p = build_bridge_weight_labeled_outcome_payload(
        baseline_worksheet_path=tmp_path / "b.csv",
        delta_worksheet_path=tmp_path / "d.csv",
        response_rollup_path=paths["rr"],
        compare_zero_vs_w005_path=paths["c01"],
        compare_w005_vs_w010_path=paths["c12"],
        compare_zero_vs_w010_path=paths["c02"],
        diagnostics_paths={R0: paths["dz0"], R5: paths["dz5"], R10: paths["dz10"]},
        conn=None,
    )
    md = markdown_from_bridge_weight_labeled_outcome(p)
    low = md.lower()
    assert "not" in low and "validation" in low


def test_run_writes_json_without_db(tmp_path: Path) -> None:
    _write_baseline_csv(tmp_path / "b.csv", n=20)
    _write_delta_csv(tmp_path / "d.csv", [("https://openalex.org/W999", "good", "yes")])
    c01, c12, c02 = _compare_json()
    paths = _paths(tmp_path, c01=c01, c12=c12, c02=c02, rr=_response_rollup(sat=True))
    for name, rid in (("dz0", R0), ("dz5", R5), ("dz10", R10)):
        pth = tmp_path / f"{name}.json"
        pth.write_text(json.dumps(_diag(rid, eligible_ids=list(range(1, 21)))), encoding="utf-8")
        paths[name] = pth
    out = tmp_path / "out.json"
    md = tmp_path / "out.md"
    run_bridge_weight_labeled_outcome(
        baseline_worksheet_path=tmp_path / "b.csv",
        delta_worksheet_path=tmp_path / "d.csv",
        response_rollup_path=paths["rr"],
        compare_zero_vs_w005_path=paths["c01"],
        compare_w005_vs_w010_path=paths["c12"],
        compare_zero_vs_w010_path=paths["c02"],
        diagnostics_rank_zero_path=paths["dz0"],
        diagnostics_rank_w005_path=paths["dz5"],
        diagnostics_rank_w010_path=paths["dz10"],
        output_path=out,
        markdown_path=md,
        database_url="postgresql://invalid.invalid:1/db",
    )
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["decision"]["ready_for_default"] is False

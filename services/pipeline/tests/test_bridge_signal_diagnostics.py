"""Tests for bridge-signal-diagnostics (explicit ranking_run_id, read-only)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main
from pipeline.bridge_signal_diagnostics import (
    BridgeSignalDiagnosticsError,
    build_bridge_signal_diagnostics_payload,
    markdown_from_diagnostics,
    summarize_bridge_signal_json,
)


class _FakeResult:
    def __init__(self, parent: "_FakeConn", query: str, params: tuple | None) -> None:
        self._p = parent
        self._query = query
        self._params = params

    def fetchone(self) -> dict | None:
        if "FROM ranking_runs" in self._query:
            return self._p.run_row
        if "bridge_family_row_count" in self._query:
            return self._p.coverage_row
        return None

    def fetchall(self) -> list[dict]:
        if "SELECT ps.bridge_score" in self._query and "recommendation_family = 'bridge'" in self._query:
            return [{"bridge_score": s} for s in self._p.all_bridge_scores]
        if "SELECT ps.work_id" in self._query and "FROM paper_scores ps" in self._query and "LIMIT" in self._query:
            if self._params is None:
                return []
            _rid, family, k = self._params
            key = (family, "bridge_eligible IS TRUE" in self._query)
            ids = self._p.topk_map.get(key, [])
            return [{"work_id": i} for i in ids[: int(k)]]
        if "JOIN works w" in self._query and "recommendation_family = 'bridge'" in self._query:
            return self._p.bridge_topk_detail_rows
        return []


class _FakeConn:
    def __init__(
        self,
        *,
        run_row: dict,
        coverage_row: dict,
        all_bridge_scores: list[float | None],
        topk_map: dict[tuple[str, bool], list[int]],
        bridge_topk_detail_rows: list[dict],
    ) -> None:
        self.run_row = run_row
        self.coverage_row = coverage_row
        self.all_bridge_scores = all_bridge_scores
        self.topk_map = topk_map
        self.bridge_topk_detail_rows = bridge_topk_detail_rows
        self.queries: list[tuple[str, tuple | None]] = []

    def execute(self, query: str, params: tuple | None = None) -> _FakeResult:
        self.queries.append((query, params))
        return _FakeResult(self, query, params)


def _nm1_json(
    *,
    eligible: bool,
    anchor: str,
    neighbors: list[int] | None,
    mix: float | None = None,
) -> dict:
    out: dict = {
        "signal_version": "neighbor_mix_v1",
        "k": 15,
        "eligible": eligible,
        "anchor_cluster_id": anchor,
    }
    if eligible and neighbors is not None:
        out["neighbor_work_ids"] = neighbors
        out["mix_score"] = mix
        out["foreign_neighbor_count"] = int(mix * 15) if mix is not None else 0
    return out


def test_summarize_neighbor_mix_stable_keys() -> None:
    s = summarize_bridge_signal_json(_nm1_json(eligible=True, anchor="c0", neighbors=[1, 2, 3], mix=0.4))
    assert s is not None
    assert s["signal_version"] == "neighbor_mix_v1"
    assert s["neighbor_work_id_count"] == 3
    assert "neighbor_work_ids" not in s


def test_build_payload_flags_and_overlap_booleans(monkeypatch: pytest.MonkeyPatch) -> None:
    run_row = {
        "ranking_run_id": "rank-1",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
        "status": "succeeded",
    }
    coverage = {
        "bridge_family_row_count": 50,
        "bridge_score_nonnull_count": 50,
        "bridge_score_null_count": 0,
        "bridge_eligible_true_count": 50,
        "bridge_eligible_false_count": 0,
        "bridge_eligible_null_count": 0,
        "bridge_signal_json_present_count": 50,
        "bridge_signal_json_missing_count": 0,
    }
    # full bridge top-k same as eligible -> not selective
    same_ids = [10, 11, 12]
    topk_map = {
        ("bridge", False): same_ids,
        ("bridge", True): same_ids,
        ("emerging", False): [10, 11, 99],  # high overlap with bridge
        ("undercited", False): [200, 201, 202],
    }
    cluster_assign = {10: "a", 11: "a", 12: "b", 99: "c", 1: "b", 2: "b", 3: "c"}
    detail = [
        {
            "work_id": 10,
            "paper_id": "W10",
            "title": "t10",
            "final_score": 1.0,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": 0.1,
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": _nm1_json(eligible=True, anchor="a", neighbors=[1, 2, 3], mix=0.4),
        },
        {
            "work_id": 11,
            "paper_id": "W11",
            "title": "t11",
            "final_score": 0.9,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": 0.1,
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": _nm1_json(eligible=True, anchor="a", neighbors=[1, 2, 3], mix=0.4),
        },
        {
            "work_id": 12,
            "paper_id": "W12",
            "title": "t12",
            "final_score": 0.8,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": 0.1,
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": _nm1_json(eligible=True, anchor="b", neighbors=[1, 2, 3], mix=0.2),
        },
    ]

    def fake_load_cluster_assignments(_conn: object, *, cluster_version: str) -> dict[int, str]:
        assert cluster_version == "cv1"
        return cluster_assign

    monkeypatch.setattr(
        "pipeline.bridge_signal_diagnostics.load_cluster_assignments",
        fake_load_cluster_assignments,
    )

    conn = _FakeConn(
        run_row=run_row,
        coverage_row=coverage,
        all_bridge_scores=[0.1] * 50,
        topk_map=topk_map,
        bridge_topk_detail_rows=detail,
    )
    p = build_bridge_signal_diagnostics_payload(conn, ranking_run_id="rank-1", k=3)

    assert p["head_eligibility"]["full_bridge_equals_eligible_only_bridge_top_k"] is True
    assert p["diagnosis"]["eligibility_filter_not_selective_at_head"] is True
    assert p["overlap_detail"]["bridge_vs_emerging_jaccard"] >= 0.50
    assert p["overlap_detail"]["eligible_bridge_vs_emerging_jaccard"] >= 0.50
    assert p["overlap_detail"]["emerging_overlap_delta_from_full_to_eligible"] == 0.0
    assert p["diagnosis"]["bridge_head_emerging_overlap_high"] is True
    assert p["diagnosis"]["bridge_score_has_low_variance"] is True  # unique 1 value in top-k
    assert p["diagnosis"]["eligible_head_differs_from_full"] is False
    assert p["diagnosis"]["eligible_head_less_emerging_like_than_full"] is False
    assert p["diagnosis"]["eligible_distinctness_improves_by_threshold"] is False

    rows = p["bridge_top_k_rows"]
    assert rows[0]["in_emerging_top_k"] is True
    assert rows[0]["in_undercited_top_k"] is False

    for q, params in conn.queries:
        if params and isinstance(params, tuple) and len(params) >= 1 and "ranking_run_id = %s" in q:
            assert params[0] == "rank-1"

    assert any("ORDER BY ps.final_score DESC, ps.work_id ASC" in q for q, _ in conn.queries)
    assert p["suggested_next_step"] == "tighten_bridge_eligibility_thresholds"


def test_missing_bridge_signal_json_triggers_warning_and_sparse_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    run_row = {
        "ranking_run_id": "rank-1",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
        "status": "succeeded",
    }
    coverage = {
        "bridge_family_row_count": 5,
        "bridge_score_nonnull_count": 5,
        "bridge_score_null_count": 0,
        "bridge_eligible_true_count": 5,
        "bridge_eligible_false_count": 0,
        "bridge_eligible_null_count": 0,
        "bridge_signal_json_present_count": 3,
        "bridge_signal_json_missing_count": 2,
    }
    b_ids = [1, 2, 3]
    topk_map = {
        ("bridge", False): b_ids,
        ("bridge", True): [1, 2, 4],
        ("emerging", False): [99, 98, 97],
        ("undercited", False): [200, 201, 202],
    }
    detail = [
        {
            "work_id": wid,
            "paper_id": f"W{wid}",
            "title": f"t{wid}",
            "final_score": 1.0 - i * 0.1,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": float(i),
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": None if i == 0 else _nm1_json(eligible=True, anchor="a", neighbors=[10], mix=0.5),
        }
        for i, wid in enumerate(b_ids)
    ]

    monkeypatch.setattr(
        "pipeline.bridge_signal_diagnostics.load_cluster_assignments",
        lambda _conn, *, cluster_version: {1: "a", 10: "b", 99: "c"},
    )

    conn = _FakeConn(
        run_row=run_row,
        coverage_row=coverage,
        all_bridge_scores=[0.0, 1.0, 2.0, 3.0, 4.0],
        topk_map=topk_map,
        bridge_topk_detail_rows=detail,
    )
    p = build_bridge_signal_diagnostics_payload(conn, ranking_run_id="rank-1", k=3)
    assert p["diagnosis"]["bridge_signal_details_missing_or_sparse"] is True
    assert any("NULL bridge_signal_json" in w for w in p["warnings"])
    assert p["suggested_next_step"] == "repair_bridge_signal_generation"
    assert not any(
        ("INSERT " in q.upper()) or ("UPDATE " in q.upper()) or ("DELETE " in q.upper())
        for q, _ in conn.queries
    )


def test_eligible_only_overlap_metrics_and_threshold_delta() -> None:
    run_row = {
        "ranking_run_id": "rank-1",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {},
        "status": "succeeded",
    }
    coverage = {
        "bridge_family_row_count": 10,
        "bridge_score_nonnull_count": 10,
        "bridge_score_null_count": 0,
        "bridge_eligible_true_count": 5,
        "bridge_eligible_false_count": 5,
        "bridge_eligible_null_count": 0,
        "bridge_signal_json_present_count": 10,
        "bridge_signal_json_missing_count": 0,
    }
    # full vs emerging: 5/15 => 0.333333 ; eligible vs emerging: 2/18 => 0.111111 ; delta 0.222222
    topk_map = {
        ("bridge", False): [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ("bridge", True): [1, 2, 11, 12, 13, 14, 15, 16, 17, 18],
        ("emerging", False): [1, 2, 3, 4, 5, 101, 102, 103, 104, 105],
        ("undercited", False): [200, 201, 202],
    }
    detail = [
        {
            "work_id": wid,
            "paper_id": f"W{wid}",
            "title": f"t{wid}",
            "final_score": 1.0 - (i * 0.01),
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": float(i),
            "diversity_penalty": 0.0,
            "bridge_eligible": wid in {1, 2, 11, 12, 13, 14, 15, 16, 17, 18},
            "bridge_signal_json": _nm1_json(eligible=True, anchor="a", neighbors=[10], mix=0.5),
        }
        for i, wid in enumerate(topk_map[("bridge", False)])
    ]
    conn = _FakeConn(
        run_row=run_row,
        coverage_row=coverage,
        all_bridge_scores=[float(i) for i in range(10)],
        topk_map=topk_map,
        bridge_topk_detail_rows=detail,
    )
    p = build_bridge_signal_diagnostics_payload(conn, ranking_run_id="rank-1", k=10)

    assert p["overlap_detail"]["eligible_bridge_top_k_ids"] == topk_map[("bridge", True)]
    assert p["overlap_detail"]["bridge_vs_emerging_jaccard"] == pytest.approx(0.333333, abs=1e-6)
    assert p["overlap_detail"]["eligible_bridge_vs_emerging_jaccard"] == pytest.approx(0.111111, abs=1e-6)
    assert p["overlap_detail"]["eligible_bridge_vs_emerging_overlap_count"] == 2
    assert p["overlap_detail"]["full_bridge_vs_eligible_bridge_overlap_count"] == 2
    assert p["overlap_detail"]["full_bridge_vs_eligible_bridge_jaccard"] == pytest.approx(0.111111, abs=1e-6)
    assert p["overlap_detail"]["emerging_overlap_delta_from_full_to_eligible"] == pytest.approx(0.222222, abs=1e-6)
    assert p["diagnosis"]["eligible_head_differs_from_full"] is True
    assert p["diagnosis"]["eligible_head_less_emerging_like_than_full"] is True
    assert p["diagnosis"]["eligible_distinctness_improves_by_threshold"] is True


def test_markdown_no_neighbor_id_lists_and_no_validation_claims() -> None:
    run_row = {
        "ranking_run_id": "rank-1",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
        "status": "succeeded",
    }
    coverage = {
        "bridge_family_row_count": 3,
        "bridge_score_nonnull_count": 3,
        "bridge_score_null_count": 0,
        "bridge_eligible_true_count": 3,
        "bridge_eligible_false_count": 0,
        "bridge_eligible_null_count": 0,
        "bridge_signal_json_present_count": 3,
        "bridge_signal_json_missing_count": 0,
    }
    b_ids = [1, 2, 3]
    topk_map = {
        ("bridge", False): b_ids,
        ("bridge", True): [4, 5, 6],
        ("emerging", False): [99],
        ("undercited", False): [200],
    }
    detail = [
        {
            "work_id": wid,
            "paper_id": f"W{wid}",
            "title": "t",
            "final_score": 1.0,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": float(wid),
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": _nm1_json(eligible=True, anchor="a", neighbors=[10], mix=0.5),
        }
        for wid in b_ids
    ]
    conn = _FakeConn(
        run_row=run_row,
        coverage_row=coverage,
        all_bridge_scores=[1.0, 2.0, 3.0],
        topk_map=topk_map,
        bridge_topk_detail_rows=detail,
    )

    with patch("pipeline.bridge_signal_diagnostics.load_cluster_assignments", return_value={1: "a", 10: "b"}):
        p = build_bridge_signal_diagnostics_payload(conn, ranking_run_id="rank-1", k=3)
    md = markdown_from_diagnostics(p)
    assert "neighbor_work_ids" not in md
    low = md.lower()
    assert "is validated" not in low
    assert "does **not** validate" in md or "diagnostic only" in low
    assert "Eligible-only bridge vs emerging Jaccard" in md


def test_cli_writes_json_and_md(tmp_path: Path) -> None:
    run_row = {
        "ranking_run_id": "rank-x",
        "ranking_version": "rv",
        "corpus_snapshot_version": "snap",
        "embedding_version": "emb",
        "config_json": {},
        "status": "succeeded",
    }
    coverage = {
        "bridge_family_row_count": 2,
        "bridge_score_nonnull_count": 2,
        "bridge_score_null_count": 0,
        "bridge_eligible_true_count": 2,
        "bridge_eligible_false_count": 0,
        "bridge_eligible_null_count": 0,
        "bridge_signal_json_present_count": 2,
        "bridge_signal_json_missing_count": 0,
    }
    topk_map = {
        ("bridge", False): [1, 2],
        ("bridge", True): [1, 2],
        ("emerging", False): [9, 8],
        ("undercited", False): [200, 201],
    }
    detail = [
        {
            "work_id": 1,
            "paper_id": "W1",
            "title": "a",
            "final_score": 1.0,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": 0.5,
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": _nm1_json(eligible=True, anchor="a", neighbors=[10], mix=0.5),
        },
        {
            "work_id": 2,
            "paper_id": "W2",
            "title": "b",
            "final_score": 0.9,
            "semantic_score": 0.5,
            "citation_velocity_score": 0.5,
            "topic_growth_score": 0.5,
            "bridge_score": 0.6,
            "diversity_penalty": 0.0,
            "bridge_eligible": True,
            "bridge_signal_json": _nm1_json(eligible=True, anchor="b", neighbors=[11], mix=0.5),
        },
    ]
    fake_conn = _FakeConn(
        run_row=run_row,
        coverage_row=coverage,
        all_bridge_scores=[0.5, 0.6],
        topk_map=topk_map,
        bridge_topk_detail_rows=detail,
    )
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = fake_conn
    mock_cm.__exit__.return_value = None

    out = tmp_path / "d.json"
    md = tmp_path / "d.md"
    with patch("pipeline.bridge_signal_diagnostics.psycopg.connect", return_value=mock_cm):
        with patch.object(
            sys,
            "argv",
            [
                "pipeline.cli",
                "bridge-signal-diagnostics",
                "--ranking-run-id",
                "rank-x",
                "--k",
                "2",
                "--output",
                str(out),
                "--markdown-output",
                str(md),
            ],
        ):
            cli_main.main()
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["provenance"]["ranking_run_id"] == "rank-x"
    assert md.is_file()


def test_ranking_run_not_found_raises() -> None:
    class _ConnNoRun:
        queries: list = []

        def execute(self, query: str, params: tuple | None = None) -> object:
            self.queries.append((query, params))

            class _R:
                def fetchone(self_inner) -> None:
                    if "FROM ranking_runs" in query:
                        return None
                    return None

                def fetchall(self_inner) -> list:
                    return []

            return _R()

    with pytest.raises(BridgeSignalDiagnosticsError, match="not found"):
        build_bridge_signal_diagnostics_payload(_ConnNoRun(), ranking_run_id="missing", k=5)

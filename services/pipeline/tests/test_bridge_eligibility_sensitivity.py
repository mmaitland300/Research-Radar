"""Tests for bridge-eligibility-sensitivity (read-only threshold sweep)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.cli as cli_main
from pipeline.bridge_eligibility_sensitivity import (
    BridgeEligibilitySensitivityError,
    build_bridge_eligibility_sensitivity_payload,
    markdown_from_sensitivity,
)


class _FakeResult:
    def __init__(self, parent: "_FakeConn", query: str) -> None:
        self._p = parent
        self._q = query

    def fetchone(self):
        if "FROM ranking_runs" in self._q:
            return self._p.run_row
        return None

    def fetchall(self):
        if "FROM paper_scores" in self._q and "recommendation_family = 'bridge'" in self._q:
            return self._p.bridge_rows
        if "SELECT ps.work_id" in self._q and "recommendation_family = %s" in self._q:
            return self._p.topk_rows
        return []


class _FakeConn:
    def __init__(self, *, run_status: str = "succeeded", include_signal_fields: bool = True) -> None:
        self.queries: list[str] = []
        self.run_row = {
            "ranking_run_id": "rank-1",
            "ranking_version": "rv",
            "corpus_snapshot_version": "snap",
            "embedding_version": "emb",
            "config_json": {"clustering_artifact": {"cluster_version": "cv1"}},
            "status": run_status,
        }
        self.topk_rows = [{"work_id": i} for i in [1, 2, 3, 4, 5]]
        sig = (
            {
                "signal_version": "neighbor_mix_v1",
                "eligible": True,
                "k": 10,
                "mix_score": 0.5,
                "foreign_neighbor_count": 5,
                "anchor_cluster_id": "c1",
                "neighbor_work_ids": [10, 11, 12, 13, 14],
            }
            if include_signal_fields
            else {"signal_version": "neighbor_mix_v1"}
        )
        self.bridge_rows = [
            {
                "work_id": 1,
                "final_score": 0.9,
                "bridge_score": 0.9,
                "bridge_eligible": True,
                "bridge_signal_json": sig,
            },
            {
                "work_id": 2,
                "final_score": 0.9,
                "bridge_score": 0.8,
                "bridge_eligible": False,
                "bridge_signal_json": sig,
            },
            {
                "work_id": 3,
                "final_score": 0.8,
                "bridge_score": 0.7,
                "bridge_eligible": True,
                "bridge_signal_json": sig,
            },
            {
                "work_id": 4,
                "final_score": 0.8,
                "bridge_score": 0.6,
                "bridge_eligible": True,
                "bridge_signal_json": sig,
            },
            {
                "work_id": 5,
                "final_score": 0.7,
                "bridge_score": 0.5,
                "bridge_eligible": True,
                "bridge_signal_json": sig,
            },
        ]

    def execute(self, query: str, params=None):
        self.queries.append(" ".join(query.split()))
        if params is not None and len(params) >= 2 and params[1] == "emerging":
            self.topk_rows = [{"work_id": i} for i in [1, 2, 90, 91, 92]]
        return _FakeResult(self, query)


def test_explicit_ranking_run_id_required() -> None:
    conn = _FakeConn()
    with pytest.raises(BridgeEligibilitySensitivityError, match="--ranking-run-id"):
        build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="", k=20)


def test_run_must_be_succeeded() -> None:
    conn = _FakeConn(run_status="failed")
    with pytest.raises(BridgeEligibilitySensitivityError, match="not succeeded"):
        build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=20)


def test_read_only_queries_no_mutations() -> None:
    conn = _FakeConn()
    with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={10: "c1", 11: "c2"}):
        build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=3)
    blob = "\n".join(conn.queries).lower()
    assert "insert into" not in blob
    assert "update " not in blob
    assert "delete from" not in blob


def test_deterministic_ordering_by_final_score_then_work_id() -> None:
    conn = _FakeConn()
    with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={10: "c1", 11: "c2"}):
        payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=5)
    assert payload["baseline"]["full_bridge_top_k_ids"][:3] == [1, 2, 3]
    assert any("ORDER BY final_score DESC, work_id ASC" in q for q in conn.queries)


def test_missing_signal_fields_yield_warnings_not_crash() -> None:
    conn = _FakeConn(include_signal_fields=False)
    with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={}):
        payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=5)
    assert payload["recommended_next_step"] in {"inspect_bridge_signal_schema", "keep_current_eligibility_and_label"}
    assert len(payload["warnings"]) >= 1


def test_jaccard_overlap_and_distinctness_threshold() -> None:
    conn = _FakeConn()
    with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={10: "c1", 11: "c2"}):
        payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=5)
    baseline = payload["baseline"]["full_bridge_vs_emerging_jaccard"]
    variant = next(v for v in payload["variants"] if v["variant_id"] == "existing_bridge_eligible")
    assert baseline >= 0.0
    assert "variant_vs_emerging_jaccard" in variant
    assert variant["distinctness_improves"] == (variant["emerging_overlap_delta_vs_full_bridge"] >= 0.10)


def test_warning_when_eligible_top_k_below_k() -> None:
    conn = _FakeConn()
    conn.bridge_rows = conn.bridge_rows[:2]
    with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={}):
        payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=5)
    sparse = [v for v in payload["variants"] if v["warning"]]
    assert sparse
    assert any("below k" in str(v["warning"]) for v in sparse)


def test_recommended_next_step_logic_for_candidate() -> None:
    conn = _FakeConn()
    # Larger sample to satisfy candidate minimum counts.
    conn.bridge_rows = []
    for i in range(1, 61):
        conn.bridge_rows.append(
            {
                "work_id": i,
                "final_score": float(100 - i),
                "bridge_score": float(100 - i),
                "bridge_eligible": True,
                "bridge_signal_json": {
                    "signal_version": "neighbor_mix_v1",
                    "eligible": True,
                    "k": 10,
                    "mix_score": 0.9,
                    "foreign_neighbor_count": 9,
                    "anchor_cluster_id": "c1",
                    "neighbor_work_ids": [1000 + i, 2000 + i],
                },
            }
        )
    with patch(
        "pipeline.bridge_eligibility_sensitivity.fetch_top_k_work_ids",
        side_effect=lambda _c, ranking_run_id, family, k, bridge_eligible_true_only: (
            list(range(1, k + 1)) if family == "bridge" else list(range(41, 41 + k))
        ),
    ):
        with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={}):
            payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=20)
    assert payload["recommended_next_step"] in {
        "rerun_zero_bridge_with_candidate_threshold",
        "tighten_bridge_eligibility",
        "keep_current_eligibility_and_label",
    }


def test_markdown_caveat_no_validation_claim() -> None:
    conn = _FakeConn()
    with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={}):
        payload = build_bridge_eligibility_sensitivity_payload(conn, ranking_run_id="rank-1", k=5)
    md = markdown_from_sensitivity(payload).lower()
    assert "not ranking validation" in md
    assert "not bridge validation" in md
    assert "no bridge weights were changed" in md


def test_cli_writes_json_and_md(tmp_path: Path) -> None:
    fake_conn = _FakeConn()
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = fake_conn
    mock_cm.__exit__.return_value = None
    out = tmp_path / "s.json"
    md = tmp_path / "s.md"
    with patch("pipeline.bridge_eligibility_sensitivity.psycopg.connect", return_value=mock_cm):
        with patch("pipeline.bridge_eligibility_sensitivity.load_cluster_assignments", return_value={}):
            with patch.object(
                sys,
                "argv",
                [
                    "pipeline.cli",
                    "bridge-eligibility-sensitivity",
                    "--ranking-run-id",
                    "rank-1",
                    "--k",
                    "5",
                    "--output",
                    str(out),
                    "--markdown-output",
                    str(md),
                ],
            ):
                cli_main.main()
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["provenance"]["ranking_run_id"] == "rank-1"
    assert md.is_file()

"""Tests for bridge-weight-experiment-compare (read-only baseline vs experiment)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pipeline.cli as cli_main
import pytest

from pipeline.bridge_weight_experiment_compare import (
    BridgeWeightExperimentCompareError,
    build_bridge_weight_experiment_compare_payload,
    markdown_from_bridge_weight_experiment_compare,
)


class _FakeResult:
    def __init__(self, parent: "_FakeConn", query: str, params: tuple | None) -> None:
        self._p = parent
        self._q = query
        self._params = params

    def fetchone(self) -> dict | None:
        if "FROM ranking_runs" not in self._q or self._params is None:
            return None
        run_id = self._params[0]
        return self._p.runs.get(str(run_id))

    def fetchall(self) -> list[dict]:
        if "FROM paper_scores ps" not in self._q or self._params is None:
            return []
        run_id, family, k = self._params
        elig = "bridge_eligible IS TRUE" in self._q
        rows = self._p.topk_rows.get((str(run_id), str(family), bool(elig)), [])
        return rows[: int(k)]


class _FakeConn:
    def __init__(
        self,
        *,
        runs: dict[str, dict],
        topk_rows: dict[tuple[str, str, bool], list[dict]],
    ) -> None:
        self.runs = runs
        self.topk_rows = topk_rows
        self.queries: list[str] = []

    def execute(self, query: str, params: tuple | None = None) -> _FakeResult:
        self.queries.append(query)
        return _FakeResult(self, query, params)


def _run_row(run_id: str, *, bridge_weight: float, mode: str = "top50_cross_cluster_gte_0_40") -> dict:
    return {
        "ranking_run_id": run_id,
        "ranking_version": f"rv-{run_id}",
        "corpus_snapshot_version": "snap-1",
        "embedding_version": "emb-1",
        "config_json": {
            "bridge_eligibility_mode": mode,
            "clustering_artifact": {"cluster_version": "cl-1", "bridge_weight_in_final_score": bridge_weight},
            "family_weights": {"bridge": {"bridge": bridge_weight}},
        },
        "status": "succeeded",
    }


def _rows(ids: list[int], *, prefix: str = "W") -> list[dict]:
    out: list[dict] = []
    for i, wid in enumerate(ids):
        out.append(
            {
                "work_id": wid,
                "paper_id": f"{prefix}{wid}",
                "title": f"title-{wid}",
                "final_score": 1.0 - i * 0.01,
            }
        )
    return out


def test_compare_rejects_mismatched_stack(tmp_path: Path) -> None:
    runs = {
        "rank-base": _run_row("rank-base", bridge_weight=0.0),
        "rank-exp": {**_run_row("rank-exp", bridge_weight=0.05), "embedding_version": "emb-2"},
    }
    conn = _FakeConn(runs=runs, topk_rows={})
    ws = tmp_path / "baseline.csv"
    ws.write_text("paper_id\nW1\n", encoding="utf-8")
    with pytest.raises(BridgeWeightExperimentCompareError, match="embedding versions differ"):
        build_bridge_weight_experiment_compare_payload(
            conn,
            baseline_ranking_run_id="rank-base",
            experiment_ranking_run_id="rank-exp",
            k=20,
            baseline_bridge_worksheet_path=ws,
        )


def test_compare_happy_path_and_unlabeled_flags(tmp_path: Path) -> None:
    runs = {
        "rank-base": _run_row("rank-base", bridge_weight=0.0),
        "rank-exp": _run_row("rank-exp", bridge_weight=0.05),
    }
    topk = {
        ("rank-base", "bridge", False): _rows([1, 2, 3]),
        ("rank-exp", "bridge", False): _rows([2, 3, 4]),
        ("rank-base", "bridge", True): _rows([11, 12, 13]),
        ("rank-exp", "bridge", True): _rows([11, 13, 14]),
        ("rank-base", "emerging", False): _rows([11, 50, 51]),
        ("rank-exp", "emerging", False): _rows([11, 50, 51]),
        ("rank-base", "undercited", False): _rows([70, 71, 72]),
        ("rank-exp", "undercited", False): _rows([70, 71, 72]),
    }
    conn = _FakeConn(runs=runs, topk_rows=topk)
    ws = tmp_path / "baseline.csv"
    ws.write_text("paper_id\nW11\nW12\nW13\n", encoding="utf-8")
    payload = build_bridge_weight_experiment_compare_payload(
        conn,
        baseline_ranking_run_id="rank-base",
        experiment_ranking_run_id="rank-exp",
        k=3,
        baseline_bridge_worksheet_path=ws,
    )
    assert payload["same_stack_check"]["only_bridge_weight_differs"] is True
    assert payload["bridge_top_k_comparison"]["full_bridge_overlap"]["jaccard"] == pytest.approx(0.5, abs=1e-6)
    assert payload["bridge_top_k_comparison"]["new_full_bridge_work_ids"] == [4]
    assert payload["bridge_top_k_comparison"]["dropped_full_bridge_work_ids"] == [1]
    assert payload["decision"]["candidate_for_labeling"] is True
    assert payload["decision"]["candidate_for_weight_increase"] is False
    assert payload["decision"]["ready_for_default"] is False
    assert payload["quality_risk"]["unlabeled_experiment_eligible_top_k_count"] == 1
    assert payload["quality_risk"]["experiment_eligible_top_k_not_in_labeled_baseline_rows"][0]["work_id"] == 14
    assert not any(
        ("INSERT " in q.upper()) or ("UPDATE " in q.upper()) or ("DELETE " in q.upper()) for q in conn.queries
    )


def test_unlabeled_risk_only_flags_new_eligible_entrants(tmp_path: Path) -> None:
    runs = {
        "rank-base": _run_row("rank-base", bridge_weight=0.05),
        "rank-exp": _run_row("rank-exp", bridge_weight=0.10),
    }
    topk = {
        ("rank-base", "bridge", False): _rows([1, 2, 3]),
        ("rank-exp", "bridge", False): _rows([1, 2, 3]),
        ("rank-base", "bridge", True): _rows([11, 12, 13]),
        ("rank-exp", "bridge", True): _rows([11, 12, 14]),
        ("rank-base", "emerging", False): _rows([50, 51, 52]),
        ("rank-exp", "emerging", False): _rows([50, 51, 52]),
        ("rank-base", "undercited", False): _rows([70, 71, 72]),
        ("rank-exp", "undercited", False): _rows([70, 71, 72]),
    }
    conn = _FakeConn(runs=runs, topk_rows=topk)
    ws = tmp_path / "delta.csv"
    ws.write_text("paper_id\nW99\n", encoding="utf-8")
    payload = build_bridge_weight_experiment_compare_payload(
        conn,
        baseline_ranking_run_id="rank-base",
        experiment_ranking_run_id="rank-exp",
        k=3,
        baseline_bridge_worksheet_path=ws,
    )
    assert payload["quality_risk"]["unlabeled_new_experiment_eligible_top_k_count"] == 1
    assert payload["quality_risk"]["unlabeled_new_experiment_eligible_top_k_rows"][0]["work_id"] == 14
    assert payload["decision"]["candidate_for_labeling"] is True


def test_markdown_includes_required_caveat_text(tmp_path: Path) -> None:
    runs = {
        "rank-base": _run_row("rank-base", bridge_weight=0.0),
        "rank-exp": _run_row("rank-exp", bridge_weight=0.05),
    }
    topk = {
        ("rank-base", "bridge", False): _rows([1]),
        ("rank-exp", "bridge", False): _rows([1]),
        ("rank-base", "bridge", True): _rows([1]),
        ("rank-exp", "bridge", True): _rows([1]),
        ("rank-base", "emerging", False): _rows([2]),
        ("rank-exp", "emerging", False): _rows([2]),
        ("rank-base", "undercited", False): _rows([3]),
        ("rank-exp", "undercited", False): _rows([3]),
    }
    conn = _FakeConn(runs=runs, topk_rows=topk)
    ws = tmp_path / "baseline.csv"
    ws.write_text("paper_id\nW1\n", encoding="utf-8")
    payload = build_bridge_weight_experiment_compare_payload(
        conn,
        baseline_ranking_run_id="rank-base",
        experiment_ranking_run_id="rank-exp",
        k=1,
        baseline_bridge_worksheet_path=ws,
    )
    md = markdown_from_bridge_weight_experiment_compare(payload)
    assert "ranking movement experiment, not validation" in md
    assert "Do not make positive bridge weight the default" in md


def test_cli_compare_writes_outputs(tmp_path: Path) -> None:
    runs = {
        "rank-base": _run_row("rank-base", bridge_weight=0.0),
        "rank-exp": _run_row("rank-exp", bridge_weight=0.05),
    }
    topk = {
        ("rank-base", "bridge", False): _rows([1, 2]),
        ("rank-exp", "bridge", False): _rows([2, 3]),
        ("rank-base", "bridge", True): _rows([10, 11]),
        ("rank-exp", "bridge", True): _rows([10, 12]),
        ("rank-base", "emerging", False): _rows([10, 20]),
        ("rank-exp", "emerging", False): _rows([10, 20]),
        ("rank-base", "undercited", False): _rows([30, 31]),
        ("rank-exp", "undercited", False): _rows([30, 31]),
    }
    fake_conn = _FakeConn(runs=runs, topk_rows=topk)
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = fake_conn
    mock_cm.__exit__.return_value = None

    worksheet = tmp_path / "baseline.csv"
    worksheet.write_text("paper_id\nW10\nW11\n", encoding="utf-8")
    out = tmp_path / "cmp.json"
    md = tmp_path / "cmp.md"

    with patch("pipeline.bridge_weight_experiment_compare.psycopg.connect", return_value=mock_cm):
        with patch.object(
            sys,
            "argv",
            [
                "pipeline.cli",
                "bridge-weight-experiment-compare",
                "--baseline-ranking-run-id",
                "rank-base",
                "--experiment-ranking-run-id",
                "rank-exp",
                "--k",
                "2",
                "--baseline-bridge-worksheet",
                str(worksheet),
                "--output",
                str(out),
                "--markdown-output",
                str(md),
            ],
        ):
            cli_main.main()
    assert out.is_file()
    data = json.loads(out.read_text(encoding="utf-8"))
    assert data["provenance"]["baseline"]["ranking_run_id"] == "rank-base"
    assert data["provenance"]["experiment"]["ranking_run_id"] == "rank-exp"
    assert md.is_file()


def test_cli_accepts_labeled_bridge_worksheet_alias(tmp_path: Path) -> None:
    runs = {
        "rank-base": _run_row("rank-base", bridge_weight=0.0),
        "rank-exp": _run_row("rank-exp", bridge_weight=0.05),
    }
    topk = {
        ("rank-base", "bridge", False): _rows([1]),
        ("rank-exp", "bridge", False): _rows([1]),
        ("rank-base", "bridge", True): _rows([10]),
        ("rank-exp", "bridge", True): _rows([10]),
        ("rank-base", "emerging", False): _rows([20]),
        ("rank-exp", "emerging", False): _rows([20]),
        ("rank-base", "undercited", False): _rows([30]),
        ("rank-exp", "undercited", False): _rows([30]),
    }
    fake_conn = _FakeConn(runs=runs, topk_rows=topk)
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = fake_conn
    mock_cm.__exit__.return_value = None
    worksheet = tmp_path / "labeled.csv"
    worksheet.write_text("paper_id\nW10\n", encoding="utf-8")
    out = tmp_path / "cmp.json"

    with patch("pipeline.bridge_weight_experiment_compare.psycopg.connect", return_value=mock_cm):
        with patch.object(
            sys,
            "argv",
            [
                "pipeline.cli",
                "bridge-weight-experiment-compare",
                "--baseline-ranking-run-id",
                "rank-base",
                "--experiment-ranking-run-id",
                "rank-exp",
                "--k",
                "1",
                "--labeled-bridge-worksheet",
                str(worksheet),
                "--output",
                str(out),
            ],
        ):
            cli_main.main()
    assert out.is_file()

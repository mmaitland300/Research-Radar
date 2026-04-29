"""Tests for bridge-objective-redesign-simulation (read-only logic)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.bridge_objective_redesign_simulation import (
    BridgeSimRow,
    _compute_variant,
    _residual_key,
    _take_eligible_top_20,
    build_simulation_payload,
    load_labeled_paper_ids_from_worksheet_csv,
    load_persistent_ids_from_failure_json,
    markdown_from_simulation,
    run_bridge_objective_redesign_simulation,
)


def _rows(n: int = 60) -> list[BridgeSimRow]:
    """Descending final_score; work_id 1..n; eligible on even ids; emerging finals correlate."""
    out: list[BridgeSimRow] = []
    for i in range(1, n + 1):
        fs = float(n - i + 1)
        elig = i % 2 == 0
        out.append(
            BridgeSimRow(
                work_id=i,
                paper_id=f"https://openalex.org/W{i}",
                final_score=fs,
                bridge_score=0.5 + 0.01 * i,
                bridge_eligible=elig,
                emerging_final_score=0.1 * i,
            )
        )
    return out


def test_take_eligible_top_20_preserves_global_order_without_sort() -> None:
    rows = _rows(40)
    top = _take_eligible_top_20(rows, pool_pred=lambda r: r.bridge_eligible, sort_key=None)
    assert len(top) == 20
    assert all(r.bridge_eligible for r in top)
    assert top[0].work_id == 2


def test_take_eligible_top_20_residual_reorders() -> None:
    rows = _rows(40)
    top = _take_eligible_top_20(
        rows,
        pool_pred=lambda r: r.bridge_eligible,
        sort_key=_residual_key(1.0),
    )
    assert len(top) == 20


def test_exclusion_drops_emerging_heads() -> None:
    rows = _rows(40)
    emerging = list(range(1, 21))
    em_set = set(range(1, 51))
    baseline_top = {r.work_id for r in _take_eligible_top_20(rows, pool_pred=lambda r: r.bridge_eligible, sort_key=None)}
    labeled = {f"https://openalex.org/W{i}" for i in range(2, 42, 2)}
    out = _compute_variant(
        variant_id="test_exclude_20",
        variant_type="hard_exclusion",
        rows_global=rows,
        emerging_top_k=emerging,
        emerging_top_50=em_set,
        persistent_ids={2, 4},
        baseline_eligible_top20_ids=baseline_top,
        labeled_paper_ids=labeled,
        current_jaccard=0.5,
        pool_pred=lambda r: r.bridge_eligible and r.work_id not in set(emerging),
        sort_key=None,
        hard_exclusion=True,
    )
    assert 1 not in out["eligible_top_20_ids"]
    assert out["overlap_ids_with_emerging_top_20"] == [] or min(out["overlap_ids_with_emerging_top_20"]) > 20


def test_candidate_gate_true_when_all_pass() -> None:
    rows = _rows(120)
    emerging = list(range(1000, 1020))
    em50 = set(range(900, 960))
    baseline_top = {r.work_id for r in _take_eligible_top_20(rows, pool_pred=lambda r: r.bridge_eligible, sort_key=None)}
    labeled = {r.paper_id for r in rows if r.bridge_eligible}
    out = _compute_variant(
        variant_id="residual",
        variant_type="residual_penalty",
        rows_global=rows,
        emerging_top_k=emerging,
        emerging_top_50=em50,
        persistent_ids=set(),
        baseline_eligible_top20_ids=baseline_top,
        labeled_paper_ids=labeled,
        current_jaccard=0.99,
        pool_pred=lambda r: r.bridge_eligible,
        sort_key=_residual_key(0.5),
        hard_exclusion=False,
    )
    assert out["eligible_top_20_count"] == 20
    assert out["new_unlabeled_top20_count"] == 0
    assert out["eligible_bridge_vs_emerging_jaccard"] < 0.99
    assert out["candidate_for_zero_weight_rerun"] is True


def test_load_persistent_ids(tmp_path: Path) -> None:
    p = tmp_path / "f.json"
    p.write_text(json.dumps({"persistent_shared_with_emerging_ids": [10, 14]}), encoding="utf-8")
    assert load_persistent_ids_from_failure_json(p) == [10, 14]


def test_load_labeled_paper_ids_from_csv(tmp_path: Path) -> None:
    p = tmp_path / "w.csv"
    p.write_text(
        "paper_id,relevance_label,bridge_like_label\n"
        "https://openalex.org/W1,good,yes\n"
        "https://openalex.org/W2,,,\n",
        encoding="utf-8",
    )
    s = load_labeled_paper_ids_from_worksheet_csv(p)
    assert "https://openalex.org/W1" in s
    assert "https://openalex.org/W2" not in s


def test_markdown_has_required_caveats() -> None:
    md = markdown_from_simulation(
        {
            "ranking_run_id": "x",
            "k": 20,
            "inputs": {"reference_eligible_vs_emerging_jaccard": 0.2},
            "variants": [],
            "summary": {},
        }
    )
    assert "Diagnostic only" in md
    assert "No DB writes" in md
    assert "follow-up zero-weight" in md.lower()


def test_simulation_sql_is_select_only() -> None:
    src = (Path(__file__).resolve().parents[1] / "pipeline" / "bridge_objective_redesign_simulation.py").read_text(
        encoding="utf-8"
    )
    assert "INSERT " not in src
    assert "UPDATE " not in src
    assert "DELETE " not in src
    assert "conn.execute" in src


@patch("pipeline.bridge_objective_redesign_simulation.fetch_top_k_work_ids", return_value=list(range(1, 51)))
@patch("pipeline.bridge_objective_redesign_simulation.psycopg.connect")
def test_build_simulation_payload_uses_select_only(mock_connect: MagicMock, _ft: MagicMock) -> None:
    emerging_scores = {i: 0.01 * i for i in range(1, 222)}
    bridge_rows = []
    for i in range(1, 218):
        bridge_rows.append(
            {
                "work_id": i,
                "paper_id": f"https://openalex.org/W{i}",
                "final_score": float(300 - i),
                "bridge_score": 0.8,
                "bridge_eligible": i % 3 != 0,
            }
        )

    def exec_side_effect(sql: str, params: object | None = None) -> MagicMock:
        m = MagicMock()

        def fetchall() -> list:
            if "recommendation_family = 'emerging'" in sql:
                return [{"work_id": w, "final_score": emerging_scores.get(w, 0.0)} for w in emerging_scores]
            if "recommendation_family = 'bridge'" in sql:
                return bridge_rows
            return []

        m.fetchall.side_effect = fetchall
        return m

    conn = MagicMock()
    conn.execute.side_effect = exec_side_effect
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = conn
    mock_cm.__exit__.return_value = None
    mock_connect.return_value = mock_cm

    tmp = Path(__file__).resolve().parent / "_sim_artifacts"
    tmp.mkdir(exist_ok=True)
    sens = tmp / "sens.json"
    sens.write_text(
        json.dumps(
            {
                "baseline": {"emerging_top_k_ids": list(range(1, 21))},
                "variants": [
                    {
                        "variant_id": "existing_bridge_eligible",
                        "eligible_top_k_ids": list(range(1, 21)),
                        "variant_vs_emerging_jaccard": 0.5,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    fail = tmp / "fail.json"
    fail.write_text(json.dumps({"persistent_shared_with_emerging_ids": [2, 4]}), encoding="utf-8")
    csvp = tmp / "labels.csv"
    csvp.write_text(
        "paper_id,relevance_label,bridge_like_label\n" + "\n".join(f"https://openalex.org/W{i},good,yes" for i in range(1, 22)),
        encoding="utf-8",
    )

    payload = build_simulation_payload(
        conn,
        ranking_run_id="rank-test",
        k=20,
        sensitivity_json_path=sens,
        failure_analysis_json_path=fail,
        bridge_worksheet_csv_path=csvp,
    )
    assert len(payload["variants"]) >= 10
    calls = [str(c[0][0]) for c in conn.execute.call_args_list]
    assert all("SELECT" in s.upper() for s in calls)
    for s in calls:
        assert "INSERT" not in s.upper()
        assert "UPDATE" not in s.upper()
        assert "DELETE" not in s.upper()


@patch("pipeline.bridge_objective_redesign_simulation.build_simulation_payload")
def test_run_writes_files(mock_build: MagicMock, tmp_path: Path) -> None:
    mock_build.return_value = {"ranking_run_id": "r", "k": 20, "inputs": {"reference_eligible_vs_emerging_jaccard": 0.2}, "variants": [], "summary": {}}
    outj = tmp_path / "o.json"
    outm = tmp_path / "o.md"
    with patch("pipeline.bridge_objective_redesign_simulation.psycopg.connect") as mock_connect:
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = object()
        mock_cm.__exit__.return_value = None
        mock_connect.return_value = mock_cm
        run_bridge_objective_redesign_simulation(
            ranking_run_id="r",
            k=20,
            sensitivity_json_path=tmp_path / "s.json",
            failure_analysis_json_path=tmp_path / "f.json",
            bridge_worksheet_csv_path=tmp_path / "c.csv",
            output_json_path=outj,
            markdown_path=outm,
            database_url="postgresql://invalid",
        )
    assert outj.is_file() and outm.is_file()

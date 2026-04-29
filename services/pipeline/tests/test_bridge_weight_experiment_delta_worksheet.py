"""Tests for bridge-weight experiment delta review worksheet generation."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pipeline.cli as cli_main
import pytest

from pipeline.bridge_weight_experiment_delta_worksheet import (
    BridgeWeightExperimentDeltaWorksheetError,
    _validate_comparison_artifact,
    build_bridge_weight_experiment_delta_rows,
    render_delta_worksheet_csv,
)


class _FakeResult:
    def __init__(self, parent: "_FakeConn", query: str, params: tuple | None) -> None:
        self._parent = parent
        self._query = query
        self._params = params

    def fetchall(self) -> list[dict]:
        if "FROM paper_scores ps" not in self._query or self._params is None:
            return []
        _run_id, work_ids = self._params
        wanted = {int(x) for x in work_ids}
        return [r for r in self._parent.rows if int(r["work_id"]) in wanted]


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def execute(self, query: str, params: tuple | None = None) -> _FakeResult:
        self.queries.append(query)
        return _FakeResult(self, query, params)


def _comparison(rows: list[dict] | None = None, *, same_stack: bool = True) -> dict:
    delta_rows = rows or [
        {"rank": 11, "work_id": 124, "paper_id": "https://openalex.org/W4411141538", "title": "paper 124"},
        {"rank": 13, "work_id": 127, "paper_id": "https://openalex.org/W4411141958", "title": "paper 127"},
        {"rank": 15, "work_id": 117, "paper_id": "https://openalex.org/W4412072221", "title": "paper 117"},
        {"rank": 17, "work_id": 1281, "paper_id": "https://openalex.org/W7128600794", "title": "paper 1281"},
    ]
    return {
        "provenance": {
            "baseline": {"ranking_run_id": "rank-base"},
            "experiment": {"ranking_run_id": "rank-exp"},
            "k": 20,
        },
        "same_stack_check": {
            "same_corpus_snapshot_version": same_stack,
            "same_embedding_version": True,
            "same_cluster_version": True,
            "same_bridge_eligibility_mode": True,
            "only_bridge_weight_differs": True,
        },
        "quality_risk": {
            "experiment_eligible_top_k_not_in_labeled_baseline_rows": delta_rows,
            "unlabeled_experiment_eligible_top_k_count": len(delta_rows),
        },
    }


def _diagnostics(work_ids: list[int] | None = None, *, run_id: str = "rank-exp") -> dict:
    return {
        "provenance": {"ranking_run_id": run_id, "k": 20},
        "overlap_detail": {"eligible_bridge_top_k_ids": work_ids or [1, 124, 2, 127, 3, 117, 4, 1281]},
    }


def _db_rows() -> list[dict]:
    return [
        {
            "experiment_rank": 11,
            "work_id": 124,
            "paper_id": "https://openalex.org/W4411141538",
            "title": "Generating Music Reactive Videos",
            "year": 2025,
            "citation_count": 1,
            "source_slug": "jaes",
            "topics": ["Music Technology", "Generative Models"],
            "final_score": 0.65,
            "bridge_score": 0.91,
            "reason_short": "bridge reason",
            "bridge_eligible": True,
        },
        {
            "experiment_rank": 13,
            "work_id": 127,
            "paper_id": "https://openalex.org/W4411141958",
            "title": "Reverse Engineering of Music Mixing Graphs",
            "year": 2025,
            "citation_count": 1,
            "source_slug": "jaes",
            "topics": ["Audio Processing"],
            "final_score": 0.64,
            "bridge_score": 0.89,
            "reason_short": "bridge reason",
            "bridge_eligible": True,
        },
        {
            "experiment_rank": 15,
            "work_id": 117,
            "paper_id": "https://openalex.org/W4412072221",
            "title": "Copy-Move Audio Forgery Detection",
            "year": 2025,
            "citation_count": 0,
            "source_slug": "jaes",
            "topics": ["Forensics"],
            "final_score": 0.63,
            "bridge_score": 0.88,
            "reason_short": "bridge reason",
            "bridge_eligible": True,
        },
        {
            "experiment_rank": 17,
            "work_id": 1281,
            "paper_id": "https://openalex.org/W7128600794",
            "title": "Objective Analysis and Perceptual Evaluation of LA-2A Compressors",
            "year": 2024,
            "citation_count": 0,
            "source_slug": "jaes",
            "topics": ["Audio Effects"],
            "final_score": 0.62,
            "bridge_score": 0.87,
            "reason_short": "bridge reason",
            "bridge_eligible": True,
        },
    ]


def _write_inputs(tmp_path: Path, *, comparison: dict | None = None, diagnostics: dict | None = None) -> tuple[Path, Path, Path]:
    comparison_path = tmp_path / "comparison.json"
    diagnostics_path = tmp_path / "diagnostics.json"
    baseline_path = tmp_path / "baseline.csv"
    comparison_path.write_text(json.dumps(comparison or _comparison()), encoding="utf-8")
    diagnostics_path.write_text(json.dumps(diagnostics or _diagnostics()), encoding="utf-8")
    baseline_path.write_text(
        "ranking_run_id,paper_id,relevance_label,novelty_label,bridge_like_label\n"
        "rank-base,https://openalex.org/W1,good,useful,yes\n"
        "rank-base,https://openalex.org/W2,good,useful,yes\n",
        encoding="utf-8",
        newline="",
    )
    return comparison_path, baseline_path, diagnostics_path


def test_detects_unlabeled_experiment_entrants_preserves_order_and_blank_labels(tmp_path: Path) -> None:
    comparison_path, baseline_path, diagnostics_path = _write_inputs(tmp_path)
    conn = _FakeConn(rows=_db_rows())

    rows = build_bridge_weight_experiment_delta_rows(
        conn,
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_path,
        diagnostics_path=diagnostics_path,
    )

    assert [r["paper_id"] for r in rows] == [
        "https://openalex.org/W4411141538",
        "https://openalex.org/W4411141958",
        "https://openalex.org/W4412072221",
        "https://openalex.org/W7128600794",
    ]
    assert [r["experiment_rank"] for r in rows] == ["11", "13", "15", "17"]
    assert all(r["relevance_label"] == "" for r in rows)
    assert all(r["novelty_label"] == "" for r in rows)
    assert all(r["bridge_like_label"] == "" for r in rows)
    assert all(r["reviewer_notes"] == "" for r in rows)
    assert not any(("INSERT " in q.upper()) or ("UPDATE " in q.upper()) or ("DELETE " in q.upper()) for q in conn.queries)


def test_excludes_rows_already_labeled_in_baseline_worksheet(tmp_path: Path) -> None:
    comparison_path, baseline_path, diagnostics_path = _write_inputs(tmp_path)
    baseline_path.write_text(
        "ranking_run_id,paper_id,relevance_label,novelty_label,bridge_like_label\n"
        "rank-base,https://openalex.org/W4411141538,good,useful,yes\n",
        encoding="utf-8",
        newline="",
    )
    conn = _FakeConn(rows=_db_rows())

    rows = build_bridge_weight_experiment_delta_rows(
        conn,
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_path,
        diagnostics_path=diagnostics_path,
    )

    assert [r["paper_id"] for r in rows] == [
        "https://openalex.org/W4411141958",
        "https://openalex.org/W4412072221",
        "https://openalex.org/W7128600794",
    ]


def test_rejects_mismatched_baseline_and_experiment_artifact_inputs(tmp_path: Path) -> None:
    comparison_path, baseline_path, diagnostics_path = _write_inputs(tmp_path, diagnostics=_diagnostics(run_id="rank-other"))
    conn = _FakeConn(rows=_db_rows())

    with pytest.raises(BridgeWeightExperimentDeltaWorksheetError, match="diagnostics artifact ranking_run_id"):
        build_bridge_weight_experiment_delta_rows(
            conn,
            comparison_path=comparison_path,
            baseline_worksheet_path=baseline_path,
            diagnostics_path=diagnostics_path,
        )

    with pytest.raises(BridgeWeightExperimentDeltaWorksheetError, match="baseline ranking_run_id argument"):
        build_bridge_weight_experiment_delta_rows(
            conn,
            comparison_path=comparison_path,
            baseline_worksheet_path=baseline_path,
            diagnostics_path=diagnostics_path,
            baseline_ranking_run_id="rank-wrong",
        )


def test_rejects_failed_same_stack_comparison(tmp_path: Path) -> None:
    comparison_path, baseline_path, diagnostics_path = _write_inputs(tmp_path, comparison=_comparison(same_stack=False))
    conn = _FakeConn(rows=_db_rows())

    with pytest.raises(BridgeWeightExperimentDeltaWorksheetError, match="same-stack"):
        build_bridge_weight_experiment_delta_rows(
            conn,
            comparison_path=comparison_path,
            baseline_worksheet_path=baseline_path,
            diagnostics_path=diagnostics_path,
        )


def test_render_delta_worksheet_csv_has_blank_label_fields(tmp_path: Path) -> None:
    comparison_path, baseline_path, diagnostics_path = _write_inputs(tmp_path)
    rows = build_bridge_weight_experiment_delta_rows(
        _FakeConn(rows=_db_rows()),
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_path,
        diagnostics_path=diagnostics_path,
    )

    parsed = list(csv.DictReader(render_delta_worksheet_csv(rows).splitlines()))

    assert len(parsed) == 4
    assert parsed[0]["experiment_rank"] == "11"
    assert parsed[0]["relevance_label"] == ""
    assert parsed[0]["novelty_label"] == ""
    assert parsed[0]["bridge_like_label"] == ""
    assert parsed[0]["reviewer_notes"] == ""


def test_cli_delta_worksheet_writes_output(tmp_path: Path) -> None:
    comparison_path, baseline_path, diagnostics_path = _write_inputs(tmp_path)
    fake_conn = _FakeConn(rows=_db_rows())
    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = fake_conn
    mock_cm.__exit__.return_value = None
    out = tmp_path / "delta.csv"

    with patch("pipeline.bridge_weight_experiment_delta_worksheet.psycopg.connect", return_value=mock_cm):
        with patch.object(
            sys,
            "argv",
            [
                "pipeline.cli",
                "bridge-weight-experiment-delta-worksheet",
                "--comparison",
                str(comparison_path),
                "--baseline-bridge-worksheet",
                str(baseline_path),
                "--experiment-diagnostics",
                str(diagnostics_path),
                "--output",
                str(out),
                "--baseline-ranking-run-id",
                "rank-base",
                "--experiment-ranking-run-id",
                "rank-exp",
            ],
        ):
            cli_main.main()

    parsed = list(csv.DictReader(out.read_text(encoding="utf-8").splitlines()))
    assert len(parsed) == 4
    assert parsed[0]["paper_id"] == "https://openalex.org/W4411141538"


def test_validate_comparison_artifact_accepts_objective_experiment_compare() -> None:
    comp = {
        "review_kind": "bridge_objective_experiment_compare",
        "provenance": {
            "baseline": {"ranking_run_id": "rank-a"},
            "experiment": {"ranking_run_id": "rank-b"},
        },
        "same_stack_check": {
            "same_corpus_snapshot_version": True,
            "same_embedding_version": True,
            "same_cluster_version": True,
            "same_bridge_weight_for_family_bridge": True,
            "bridge_eligibility_modes_differ": True,
        },
        "quality_risk": {
            "experiment_eligible_top_k_not_in_labeled_baseline_rows": [
                {"rank": 1, "work_id": 99, "paper_id": "https://openalex.org/W99", "title": "t"},
            ],
            "unlabeled_experiment_eligible_top_k_count": 1,
        },
    }
    base, exp, rows = _validate_comparison_artifact(comp, baseline_ranking_run_id=None, experiment_ranking_run_id=None)
    assert base == "rank-a"
    assert exp == "rank-b"
    assert len(rows) == 1

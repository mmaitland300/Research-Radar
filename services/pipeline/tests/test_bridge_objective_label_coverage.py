"""Tests for objective-experiment label coverage correction."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pipeline.cli as cli_main

from pipeline.bridge_objective_label_coverage import (
    build_objective_label_coverage_payload,
    render_one_row_review_csv,
)


def _comparison() -> dict:
    return {
        "provenance": {
            "baseline": {"ranking_run_id": "rank-ee2ba6c816"},
            "experiment": {"ranking_run_id": "rank-60910a47b4"},
        },
        "bridge_top_k_comparison": {
            "baseline_eligible_bridge_top_k_ids": [1, 2, 3, 4, 5],
            "experiment_eligible_bridge_top_k_ids": [1, 6, 7, 8, 9],
        },
        "quality_risk": {
            "unlabeled_new_experiment_eligible_top_k_rows": [
                {"rank": 16, "work_id": 6, "paper_id": "https://openalex.org/W4412072221", "title": "p6"},
                {"rank": 17, "work_id": 7, "paper_id": "https://openalex.org/W4411141538", "title": "p7"},
                {"rank": 18, "work_id": 8, "paper_id": "https://openalex.org/W4411141958", "title": "p8"},
                {"rank": 19, "work_id": 9, "paper_id": "https://openalex.org/W7128600794", "title": "p9"},
                {"rank": 20, "work_id": 10, "paper_id": "https://openalex.org/W4412072230", "title": "p10"},
            ],
        },
    }


def _write_inputs(tmp_path: Path, *, baseline_csv: str, prior_csv: str) -> tuple[Path, Path, Path]:
    comparison_path = tmp_path / "comparison.json"
    baseline_path = tmp_path / "baseline.csv"
    prior_path = tmp_path / "prior.csv"
    comparison_path.write_text(json.dumps(_comparison()), encoding="utf-8")
    baseline_path.write_text(baseline_csv, encoding="utf-8", newline="")
    prior_path.write_text(prior_csv, encoding="utf-8", newline="")
    return comparison_path, baseline_path, prior_path


def test_label_source_union_and_stable_paper_id_matching(tmp_path: Path) -> None:
    comparison_path, baseline_path, prior_path = _write_inputs(
        tmp_path,
        baseline_csv=(
            "ranking_run_id,paper_id,relevance_label\n"
            "rank-ee2ba6c816,https://openalex.org/W4412072221,good\n"
        ),
        prior_csv=(
            "paper_id,relevance_label,novelty_label,bridge_like_label\n"
            "https://openalex.org/W4411141538,acceptable,surprising,yes\n"
            "https://openalex.org/W4411141958,good,surprising,yes\n"
            "https://openalex.org/W7128600794,good,useful,partial\n"
        ),
    )

    payload, review_rows = build_objective_label_coverage_payload(
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_path,
        prior_delta_worksheet_path=prior_path,
    )

    assert payload["summary"]["moved_in_count_relative_to_baseline"] == 5
    assert payload["summary"]["moved_in_already_labeled_count"] == 4
    assert payload["summary"]["truly_unlabeled_moved_in_count"] == 1
    assert payload["moved_in"]["truly_unlabeled_moved_in_ids"] == ["https://openalex.org/W4412072230"]
    assert review_rows[0]["paper_id"] == "https://openalex.org/W4412072230"


def test_internal_work_id_used_as_secondary_match(tmp_path: Path) -> None:
    comparison = _comparison()
    comparison["quality_risk"]["unlabeled_new_experiment_eligible_top_k_rows"][0]["paper_id"] = ""
    comparison_path = tmp_path / "comparison.json"
    comparison_path.write_text(json.dumps(comparison), encoding="utf-8")
    baseline_path = tmp_path / "baseline.csv"
    baseline_path.write_text(
        "ranking_run_id,paper_id,work_id,relevance_label\nrank-ee2ba6c816,,6,good\n",
        encoding="utf-8",
        newline="",
    )
    prior_path = tmp_path / "prior.csv"
    prior_path.write_text("paper_id,relevance_label\nhttps://openalex.org/W4411141538,acceptable\n", encoding="utf-8")

    payload, _ = build_objective_label_coverage_payload(
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_path,
        prior_delta_worksheet_path=prior_path,
    )

    assert "work_id:6" in payload["moved_in"]["already_labeled_moved_in_ids"]


def test_one_row_review_csv_has_blank_reviewer_columns(tmp_path: Path) -> None:
    comparison_path, baseline_path, prior_path = _write_inputs(
        tmp_path,
        baseline_csv="ranking_run_id,paper_id\nrank-ee2ba6c816,https://openalex.org/W4412072221\n",
        prior_csv=(
            "paper_id,relevance_label\n"
            "https://openalex.org/W4411141538,ok\n"
            "https://openalex.org/W4411141958,ok\n"
            "https://openalex.org/W7128600794,ok\n"
        ),
    )
    _, rows = build_objective_label_coverage_payload(
        comparison_path=comparison_path,
        baseline_worksheet_path=baseline_path,
        prior_delta_worksheet_path=prior_path,
    )
    parsed = list(csv.DictReader(render_one_row_review_csv(rows).splitlines()))
    assert len(parsed) == 1
    assert parsed[0]["paper_id"] == "https://openalex.org/W4412072230"
    assert parsed[0]["relevance_label"] == ""
    assert parsed[0]["novelty_label"] == ""
    assert parsed[0]["bridge_like_label"] == ""
    assert parsed[0]["reviewer_notes"] == ""


def test_cli_runs_without_db_access(tmp_path: Path) -> None:
    comparison_path, baseline_path, prior_path = _write_inputs(
        tmp_path,
        baseline_csv="ranking_run_id,paper_id\nrank-ee2ba6c816,https://openalex.org/W4412072221\n",
        prior_csv=(
            "paper_id,relevance_label\n"
            "https://openalex.org/W4411141538,ok\n"
            "https://openalex.org/W4411141958,ok\n"
            "https://openalex.org/W7128600794,ok\n"
        ),
    )
    out_json = tmp_path / "coverage.json"
    out_md = tmp_path / "coverage.md"
    out_csv = tmp_path / "one-row.csv"
    old_argv = sys.argv[:]
    sys.argv = [
        "cli.py",
        "bridge-objective-label-coverage",
        "--comparison",
        str(comparison_path),
        "--baseline-bridge-worksheet",
        str(baseline_path),
        "--prior-delta-worksheet",
        str(prior_path),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--review-output",
        str(out_csv),
    ]
    try:
        with patch("pipeline.cli.psycopg.connect", side_effect=AssertionError("DB should not be touched")):
            cli_main.main()
    finally:
        sys.argv = old_argv
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["summary"]["truly_unlabeled_moved_in_count"] == 1

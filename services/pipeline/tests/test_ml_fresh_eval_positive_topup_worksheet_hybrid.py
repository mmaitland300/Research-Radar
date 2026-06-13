"""Tests for fresh hybrid positive top-up worksheet v1."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_fresh_eval_positive_topup_worksheet_hybrid import (
    MLFreshEvalPositiveTopupWorksheetHybridError,
    WORKSHEET_VERSION,
    build_ml_fresh_eval_positive_topup_worksheet_hybrid_payloads,
    stable_row_id,
    write_ml_fresh_eval_positive_topup_worksheet_hybrid,
)


def _work_id(index: int) -> str:
    return f"W{900000 + index}"


def _candidate(
    index: int,
    *,
    final_score: float | None = None,
    confirmatory: bool = True,
    previous_eval_overlap: bool = False,
) -> dict:
    work_id = _work_id(index)
    return {
        "canonical_openalex_work_id": work_id,
        "openalex_id": f"https://openalex.org/{work_id}",
        "internal_work_id": index,
        "title": f"Fresh Positive Candidate {index}",
        "year": 2026,
        "citation_count": index,
        "source_slug": "fixture-source",
        "topics": "music; audio",
        "abstract": f"Fixture abstract {index}",
        "final_score": round((final_score if final_score is not None else index / 1000), 6),
        "heuristic_rank": index,
        "ranking_run_id": "rank-9f4b2a2084",
        "family": "emerging",
        "semantic_score": 0.5,
        "citation_velocity_score": 0.1,
        "topic_growth_score": 0.2,
        "bridge_score": None,
        "bridge_eligible": None,
        "confirmatory_metric_eligible": confirmatory,
        "previous_eval_overlap": previous_eval_overlap,
    }


def _surface_payload(
    *,
    label_dataset_version: str = "ml-label-dataset-v9",
    expected_label_dataset_version: str | None = "ml-label-dataset-v9",
    positive_passed: bool = False,
    other_threshold_fails: bool = False,
    candidate_rows: list[dict] | None = None,
) -> dict:
    rows = [_candidate(i) for i in range(1, 144)] if candidate_rows is None else candidate_rows
    rows.extend(
        [
            _candidate(9001, final_score=9.9, confirmatory=False, previous_eval_overlap=True),
            _candidate(9002, final_score=9.8, confirmatory=False, previous_eval_overlap=False),
        ]
    )
    negative_passed = not other_threshold_fails
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "label_dataset_version": label_dataset_version,
            "expected_label_dataset_version": expected_label_dataset_version,
            "status": "materialized_needs_labels",
        },
        "candidate_source": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        },
        "candidate_pool": {
            "candidate_work_count": 358,
            "candidate_work_set_sha256": "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6",
            "candidate_rows": rows,
        },
        "disallowed_overlap_report": {
            "excluded_previous_eval_overlap_count": 215,
            "overlap_work_count": 215,
        },
        "confirmatory_eligibility": {
            "confirmatory_metric_eligible_work_count": 143,
        },
        "label_coverage": {
            "work_level": {
                "confirmatory_candidate_work_count": 143,
                "confirmatory_labeled_work_count": 121,
                "confirmatory_unlabeled_work_count": 22,
                "confirmatory_positive_work_count": 50 if positive_passed else 39,
                "confirmatory_negative_work_count": 82,
                "distinct_negative_work_count": 82,
                "label_coverage_rate": 121 / 143,
            }
        },
        "threshold_check": {
            "minimum_candidate_work_count": {"threshold": 100, "observed": 143, "passed": True},
            "minimum_confirmatory_labeled_work_count": {"threshold": 100, "observed": 121, "passed": True},
            "minimum_confirmatory_label_coverage_rate": {"threshold": 0.60, "observed": 121 / 143, "passed": True},
            "minimum_confirmatory_positive_work_count": {
                "threshold": 50,
                "observed": 50 if positive_passed else 39,
                "passed": positive_passed,
            },
            "minimum_confirmatory_negative_work_count": {
                "threshold": 20,
                "observed": 82,
                "passed": negative_passed,
            },
            "minimum_distinct_negative_work_count": {"threshold": 20, "observed": 82, "passed": True},
        },
        "ready_for_hybrid_validation_scoring": False,
        "recommended_next_stage": "create_fresh_eval_labeling_worksheet_hybrid_v1",
    }


def _label_dataset_payload(*, version: str = "ml-label-dataset-v9", labeled_count: int = 121) -> dict:
    rows: list[dict] = []
    for index in range(1, labeled_count + 1):
        positive = index <= 39
        rows.append(
            {
                "dataset_version": version,
                "row_id": f"label-{index}",
                "paper_id": f"https://openalex.org/{_work_id(index)}",
                "work_id": _work_id(index),
                "openalex_work_id": _work_id(index),
                "review_pool_variant": "ml_fresh_hybrid_eval_v1",
                "relevance_label": "good" if positive else "irrelevant",
                "novelty_label": "useful" if positive else "neither",
                "bridge_like_label": "yes" if positive else "not_applicable",
                "good_or_acceptable": positive,
            }
        )
    return {"dataset_version": version, "rows": rows}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    surface: dict | None = None,
    label_dataset: dict | None = None,
) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    conflict_path = tmp_path / "ml-label-conflict-policy.md"
    conflict_path.write_text("# Conflict Policy\n\nPreserve conflicts.\n", encoding="utf-8")
    return {
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", label_dataset or _label_dataset_payload()),
        "conflict_policy_path": conflict_path,
    }


def _build(tmp_path: Path, **kwargs: object) -> tuple[list[dict[str, str]], dict, str]:
    return build_ml_fresh_eval_positive_topup_worksheet_hybrid_payloads(
        **_paths(tmp_path, **kwargs),
        requested_rows=0,
        seed=20260519,
        repo_root=tmp_path,
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_happy_path_writes_csv_context_and_markdown(tmp_path: Path) -> None:
    out_csv = tmp_path / "fresh_hybrid_positive_topup_v1.csv"
    out_context = tmp_path / "fresh_hybrid_positive_topup_v1_context.json"
    out_md = tmp_path / "fresh_hybrid_positive_topup_v1.md"

    payload = write_ml_fresh_eval_positive_topup_worksheet_hybrid(
        **_paths(tmp_path),
        output_path=out_csv,
        context_output_path=out_context,
        markdown_output_path=out_md,
        requested_rows=0,
        seed=20260519,
        repo_root=tmp_path,
    )

    csv_rows = _read_csv(out_csv)
    context = json.loads(out_context.read_text(encoding="utf-8"))
    assert payload["metadata"]["artifact_type"] == "ml_fresh_eval_positive_topup_worksheet_hybrid"
    assert len(csv_rows) == 22
    assert context["metadata"]["unlabeled_confirmatory_universe_size"] == 22
    assert context["metadata"]["positive_deficit_before_labeling"] == 11
    assert "only positive threshold short by" in out_md.read_text(encoding="utf-8").lower()


def test_row_id_parity_labels_blank_and_ordering(tmp_path: Path) -> None:
    csv_rows, context, _markdown = _build(tmp_path)

    csv_ids = {row["row_id"] for row in csv_rows}
    sidecar_ids = {row["row_id"] for row in context["rows"]}
    assert csv_ids == sidecar_ids
    assert all(row[column] == "" for row in csv_rows for column in ("relevance_label", "novelty_label", "bridge_like_label", "reviewer_notes"))
    assert all(row["sample_reason"] == "fresh_hybrid_positive_topup" for row in csv_rows)
    assert [row["work_id"] for row in csv_rows] == [_work_id(i) for i in range(143, 121, -1)]
    first = csv_rows[0]
    assert first["row_id"] == stable_row_id(
        worksheet_version=WORKSHEET_VERSION,
        seed=20260519,
        canonical_openalex_work_id=first["work_id"],
    )


def test_excludes_v9_labeled_overlap_and_non_confirmatory_rows(tmp_path: Path) -> None:
    csv_rows, context, _markdown = _build(tmp_path)
    selected = {row["work_id"] for row in csv_rows}

    assert _work_id(1) not in selected
    assert _work_id(9001) not in selected
    assert _work_id(9002) not in selected
    assert all(row["confirmatory_metric_eligible"] is True for row in context["rows"])
    assert all(row["previous_eval_overlap"] is not True for row in context["rows"])
    assert all(not row["existing_v9_labels_for_same_work"] for row in context["rows"])


def test_rejects_if_any_threshold_other_than_positive_fails(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalPositiveTopupWorksheetHybridError, match="only positive threshold"):
        _build(tmp_path, surface=_surface_payload(other_threshold_fails=True))


def test_rejects_wrong_label_dataset_version_or_surface_version(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalPositiveTopupWorksheetHybridError, match="label dataset_version"):
        _build(tmp_path, label_dataset=_label_dataset_payload(version="ml-label-dataset-v8"))

    with pytest.raises(MLFreshEvalPositiveTopupWorksheetHybridError, match="label_dataset_version"):
        _build(tmp_path, surface=_surface_payload(label_dataset_version="ml-label-dataset-v8"))


def test_rejects_positive_threshold_already_passing(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalPositiveTopupWorksheetHybridError, match="positive threshold"):
        _build(tmp_path, surface=_surface_payload(positive_passed=True))


def test_rejects_empty_unlabeled_universe(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalPositiveTopupWorksheetHybridError, match="unlabeled confirmatory universe is empty"):
        _build(tmp_path, label_dataset=_label_dataset_payload(labeled_count=143))


def test_requested_rows_can_cap_unlabeled_universe(tmp_path: Path) -> None:
    csv_rows, context, _markdown = build_ml_fresh_eval_positive_topup_worksheet_hybrid_payloads(
        **_paths(tmp_path),
        requested_rows=5,
        seed=20260519,
        repo_root=tmp_path,
    )

    assert len(csv_rows) == 5
    assert context["metadata"]["selected_rows"] == 5
    assert context["metadata"]["unlabeled_confirmatory_universe_size"] == 22


def test_cli_writes_all_outputs(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_csv = tmp_path / "worksheet.csv"
    out_context = tmp_path / "context.json"
    out_md = tmp_path / "worksheet.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-eval-positive-topup-worksheet-hybrid",
        "--fresh-eval-surface",
        str(paths["fresh_eval_surface_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--requested-rows",
        "0",
        "--seed",
        "20260519",
        "--output",
        str(out_csv),
        "--context-output",
        str(out_context),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert len(_read_csv(out_csv)) == 22
    assert json.loads(out_context.read_text(encoding="utf-8"))["metadata"]["achieved_rows"] == 22
    assert "not validation" in out_md.read_text(encoding="utf-8").lower()


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_eval_positive_topup_worksheet_hybrid.py"
    ).read_text(encoding="utf-8").lower()
    for forbidden in (
        "import psycopg",
        "from psycopg",
        "import postgres",
        "from postgres",
        "import openai",
        "from openai",
        "import sklearn",
        "from sklearn",
    ):
        assert forbidden not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-fresh-eval-positive-topup-worksheet-hybrid"')
    end = cli_source.index("ml_fresh_candidate_source_expansion_plan_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block
    assert "--scoring-mode" not in parser_block
    assert "--train" not in parser_block

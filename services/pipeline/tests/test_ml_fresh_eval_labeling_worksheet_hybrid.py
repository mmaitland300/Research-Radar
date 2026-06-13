"""Tests for fresh hybrid eval labeling worksheet v1."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
import pipeline.ml_fresh_eval_labeling_worksheet_hybrid as worksheet_mod
from pipeline.ml_fresh_eval_labeling_worksheet_hybrid import (
    MLFreshEvalLabelingWorksheetHybridError,
    WORKSHEET_VERSION,
    build_ml_fresh_eval_labeling_worksheet_hybrid_payloads,
    stable_row_id,
    write_ml_fresh_eval_labeling_worksheet_hybrid,
)


def _candidate(work_index: int, *, previous_eval_overlap: bool = False) -> dict:
    work_id = f"W{500000 + work_index}"
    return {
        "bridge_eligible": None,
        "bridge_score": None,
        "canonical_openalex_work_id": work_id,
        "citation_count": work_index,
        "confirmatory_metric_eligible": not previous_eval_overlap,
        "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        "family": "emerging",
        "final_score": round(0.9 - (work_index / 1000), 6),
        "heuristic_rank": work_index,
        "internal_work_id": work_index,
        "openalex_id": f"https://openalex.org/{work_id}",
        "previous_eval_overlap": previous_eval_overlap,
        "ranking_run_id": "rank-9f4b2a2084",
        "semantic_score": round(0.8 - (work_index / 2000), 6),
        "title": f"Fresh Work {work_index}",
        "topic_growth_score": 0.0,
        "year": 2026,
    }


def _surface_payload(
    *,
    status: str = "materialized_needs_labels",
    recommended_next_stage: str = "create_fresh_eval_labeling_worksheet_hybrid_v1",
    ready: bool = False,
    eligible: int = 143,
    labeled: int = 1,
    positive: int = 1,
    negative: int = 0,
    distinct_negative: int = 0,
    overlap: int = 3,
    candidate_rows: list[dict] | None = None,
) -> dict:
    rows = candidate_rows
    if rows is None:
        rows = [_candidate(i) for i in range(1, eligible + 1)]
        rows.extend(_candidate(9000 + i, previous_eval_overlap=True) for i in range(1, overlap + 1))
    coverage = labeled / eligible if eligible else 0.0
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "status": status,
        },
        "candidate_source": {
            "ranking_run_id": "rank-9f4b2a2084",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
        },
        "candidate_pool": {
            "candidate_work_count": eligible + overlap,
            "candidate_work_set_sha256": "fresh-sha",
            "candidate_rows": rows,
        },
        "disallowed_overlap_report": {
            "excluded_previous_eval_overlap_count": overlap,
            "overlap_work_count": overlap,
        },
        "confirmatory_eligibility": {
            "confirmatory_metric_eligible_work_count": eligible,
        },
        "label_coverage": {
            "work_level": {
                "confirmatory_candidate_work_count": eligible,
                "confirmatory_labeled_work_count": labeled,
                "confirmatory_unlabeled_work_count": max(0, eligible - labeled),
                "confirmatory_positive_work_count": positive,
                "confirmatory_negative_work_count": negative,
                "distinct_negative_work_count": distinct_negative,
                "label_coverage_rate": coverage,
            }
        },
        "threshold_check": {
            "minimum_candidate_work_count": {"threshold": 100, "observed": eligible, "passed": eligible >= 100},
            "minimum_confirmatory_labeled_work_count": {"threshold": 100, "observed": labeled, "passed": False},
            "minimum_confirmatory_label_coverage_rate": {"threshold": 0.60, "observed": coverage, "passed": False},
            "minimum_confirmatory_positive_work_count": {"threshold": 50, "observed": positive, "passed": False},
            "minimum_confirmatory_negative_work_count": {"threshold": 20, "observed": negative, "passed": False},
            "minimum_distinct_negative_work_count": {"threshold": 20, "observed": distinct_negative, "passed": False},
        },
        "ready_for_hybrid_validation_scoring": ready,
        "recommended_next_stage": recommended_next_stage,
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "frozen_hybrid_arms": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
        },
    }


def _label_dataset_payload(*, labeled_work_ids: list[str] | None = None) -> dict:
    rows = []
    work_ids = ["W500001"] if labeled_work_ids is None else labeled_work_ids
    for idx, work_id in enumerate(work_ids, start=1):
        rows.append(
            {
                "row_id": f"label-{idx}",
                "paper_id": f"https://openalex.org/{work_id}",
                "work_id": work_id,
                "openalex_work_id": work_id,
                "relevance_label": "good",
                "novelty_label": "useful",
                "bridge_like_label": "yes",
                "good_or_acceptable": True,
            }
        )
    return {"dataset_version": "ml-label-dataset-v8", "rows": rows}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    surface: dict | None = None,
    policy: dict | None = None,
    label_dataset: dict | None = None,
) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    conflict_path = tmp_path / "conflict-policy.md"
    conflict_path.write_text("# Conflict Policy\n\nPreserve conflicts.\n", encoding="utf-8")
    return {
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", label_dataset or _label_dataset_payload()),
        "conflict_policy_path": conflict_path,
    }


def _build(tmp_path: Path, **kwargs: object) -> tuple[list[dict[str, str]], dict, str]:
    return build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
        **_paths(tmp_path, **kwargs),
        rows=12,
        seed=20260519,
        repo_root=tmp_path,
    )


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_happy_path_writes_csv_context_and_markdown(tmp_path: Path) -> None:
    out_csv = tmp_path / "worksheet.csv"
    out_context = tmp_path / "context.json"
    out_md = tmp_path / "worksheet.md"

    payload = write_ml_fresh_eval_labeling_worksheet_hybrid(
        **_paths(tmp_path),
        output_path=out_csv,
        context_output_path=out_context,
        markdown_output_path=out_md,
        rows=12,
        seed=20260519,
        repo_root=tmp_path,
    )

    csv_rows = _read_csv(out_csv)
    context = json.loads(out_context.read_text(encoding="utf-8"))
    assert payload["metadata"]["artifact_type"] == "ml_fresh_eval_labeling_worksheet_hybrid"
    assert payload["metadata"]["worksheet_version"] == WORKSHEET_VERSION
    assert len(csv_rows) == 12
    assert context["metadata"]["achieved_rows"] == 12
    assert "Worksheet Only / Not Validation" in out_md.read_text(encoding="utf-8")


def test_row_id_parity_labels_blank_and_existing_labels_excluded(tmp_path: Path) -> None:
    csv_rows, context, _markdown = _build(tmp_path)

    csv_ids = {row["row_id"] for row in csv_rows}
    sidecar_ids = {row["row_id"] for row in context["rows"]}
    assert csv_ids == sidecar_ids
    assert all(row["relevance_label"] == "" for row in csv_rows)
    assert all(row["novelty_label"] == "" for row in csv_rows)
    assert all(row["bridge_like_label"] == "" for row in csv_rows)
    assert all(row["reviewer_notes"] == "" for row in csv_rows)
    assert "W500001" not in {row["work_id"] for row in csv_rows}
    assert all(not row["existing_v8_labels_for_same_work"] for row in context["rows"])


def test_old_217_overlap_excluded_and_no_duplicate_canonical_works(tmp_path: Path) -> None:
    csv_rows, _context, _markdown = _build(tmp_path)

    work_ids = [row["work_id"] for row in csv_rows]
    assert len(work_ids) == len(set(work_ids))
    assert all(not work_id.startswith("W509") for work_id in work_ids)


def test_deterministic_by_seed_and_uses_documented_row_id_formula(tmp_path: Path) -> None:
    first_rows, _first_context, _ = build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
        **_paths(tmp_path / "a"),
        rows=12,
        seed=123,
        repo_root=tmp_path,
    )
    second_rows, _second_context, _ = build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
        **_paths(tmp_path / "b"),
        rows=12,
        seed=123,
        repo_root=tmp_path,
    )

    assert [row["row_id"] for row in first_rows] == [row["row_id"] for row in second_rows]
    first = first_rows[0]
    assert first["row_id"] == stable_row_id(
        worksheet_version=WORKSHEET_VERSION,
        seed=123,
        canonical_openalex_work_id=first["work_id"],
    )


def test_shortfall_when_rows_exceed_available(tmp_path: Path) -> None:
    rows = [_candidate(i) for i in range(1, 6)]
    surface = _surface_payload(eligible=100, labeled=0, candidate_rows=rows, overlap=0)
    csv_rows, context, _markdown = build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
        **_paths(tmp_path, surface=surface, label_dataset=_label_dataset_payload(labeled_work_ids=[])),
        rows=20,
        seed=20260519,
        repo_root=tmp_path,
    )

    assert len(csv_rows) == 5
    assert context["metadata"]["achieved_rows"] == 5
    assert context["metadata"]["shortfall_count"] == 15


def test_threshold_gaps_in_sidecar(tmp_path: Path) -> None:
    _csv_rows, context, _markdown = _build(tmp_path)
    gaps = context["metadata"]["threshold_gap_before_labeling"]

    assert gaps["minimum_confirmatory_labeled_work_count"]["deficit"] == 99
    assert gaps["minimum_confirmatory_label_coverage_rate"]["deficit"] == 85
    assert gaps["minimum_confirmatory_positive_work_count"]["deficit"] == 49
    assert gaps["minimum_confirmatory_negative_work_count"]["deficit"] == 20
    assert gaps["minimum_distinct_negative_work_count"]["deficit"] == 20


def test_rejects_wrong_status_or_ready_surface(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalLabelingWorksheetHybridError, match="status"):
        _build(tmp_path, surface=_surface_payload(status="materialized_ready"))

    with pytest.raises(MLFreshEvalLabelingWorksheetHybridError, match="ready_for_hybrid_validation_scoring"):
        _build(tmp_path, surface=_surface_payload(ready=True))


def test_accepts_labeling_plan_equivalent_recommended_next_stage(tmp_path: Path) -> None:
    _csv_rows, context, _markdown = _build(
        tmp_path,
        surface=_surface_payload(recommended_next_stage="create_fresh_eval_labeling_plan_hybrid_v1"),
    )

    assert context["metadata"]["achieved_rows"] == 12


class _FakeResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows


class _FakeConn:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> _FakeResult:
        self.sql.append(sql)
        ids = list((params or ([],))[0])
        return _FakeResult(
            [
                (
                    work_id,
                    f"https://openalex.org/W{work_id}",
                    f"DB title {work_id}",
                    f"DB abstract {work_id}",
                    "db-source",
                    2025,
                    0,
                )
                for work_id in ids
            ]
        )


def test_optional_db_enrichment_is_select_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeConn()
    monkeypatch.setattr(worksheet_mod.psycopg, "connect", lambda *args, **kwargs: fake_conn)

    _csv_rows, context, _markdown = build_ml_fresh_eval_labeling_worksheet_hybrid_payloads(
        **_paths(tmp_path),
        rows=3,
        seed=20260519,
        database_url="postgresql://localhost/research_radar",
        repo_root=tmp_path,
    )

    assert context["metadata"]["database_access"] == "read_only_select"
    assert fake_conn.sql
    assert all("select" in sql.lower() for sql in fake_conn.sql)
    assert not any(word in " ".join(fake_conn.sql).lower() for word in ("insert", "update", "delete"))


def test_shadow_and_production_are_blocked(tmp_path: Path) -> None:
    _csv_rows, context, _markdown = _build(tmp_path)

    blockers = context["shadow_and_production_blockers"]
    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["confirmatory_validation_complete"] is False
    assert "shadow" in context["blocked_actions"]
    assert "production_default" in context["blocked_actions"]


def test_cli_writes_all_outputs(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_csv = tmp_path / "fresh_hybrid_eval_v1.csv"
    out_context = tmp_path / "fresh_hybrid_eval_v1_context.json"
    out_md = tmp_path / "fresh_hybrid_eval_v1.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-eval-labeling-worksheet-hybrid",
        "--fresh-eval-surface",
        str(paths["fresh_eval_surface_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--rows",
        "7",
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

    assert len(_read_csv(out_csv)) == 7
    assert json.loads(out_context.read_text(encoding="utf-8"))["metadata"]["achieved_rows"] == 7
    assert "human reviewer fills the csv" in out_md.read_text(encoding="utf-8").lower()


def test_no_forbidden_imports_and_cli_has_no_scoring_training_label_import_flags() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_eval_labeling_worksheet_hybrid.py"
    ).read_text(encoding="utf-8").lower()
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-fresh-eval-labeling-worksheet-hybrid"')
    end = cli_source.index("ml_fresh_candidate_source_expansion_plan_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" in parser_block
    assert "--scoring-mode" not in parser_block
    assert "--label-import" not in parser_block
    assert "--train" not in parser_block

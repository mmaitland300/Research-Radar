"""Tests for second-surface labeling worksheet for shadow generalization v1."""

from __future__ import annotations

import csv
import json
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
import pipeline.ml_shadow_scorer_second_surface_labeling_worksheet as worksheet_mod
from pipeline.ml_shadow_scorer_second_surface_labeling_worksheet import (
    DEFAULT_REVIEW_POOL_VARIANT,
    EXPECTED_CANDIDATE_SHA,
    EXPECTED_RANKING_RUN_ID,
    MLShadowScorerSecondSurfaceLabelingWorksheetError,
    WORKSHEET_VERSION,
    build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads,
    stable_row_id,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]


class _Cursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.sql: list[str] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple = ()) -> None:
        compact = " ".join(sql.split())
        if not compact.lower().startswith("select"):
            raise AssertionError(f"Unexpected write SQL: {compact}")
        self.sql.append(compact)

    def fetchall(self) -> list[dict]:
        return self.rows


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def cursor(self, *args: object, **kwargs: object) -> _Cursor:
        return _Cursor(self.rows)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False


def _work_id(index: int) -> str:
    return f"W{index:06d}"


def _candidate_rows(count: int = 528) -> list[dict]:
    rows: list[dict] = []
    for index in range(1, count + 1):
        rows.append(
            {
                "ranking_run_id": EXPECTED_RANKING_RUN_ID,
                "internal_work_id": index,
                "recommendation_family": "emerging",
                "final_score": float(count - index),
                "openalex_id": _work_id(index),
                "title": f"Work {index}",
                "year": 2020 + (index % 5),
                "citation_count": index,
                "source_slug": "fixture",
                "abstract": f"Abstract for work {index}. " * 4,
                "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
                "topics": ["Music Information Retrieval", "Evaluation"],
            }
        )
    return rows


def _old_ids() -> set[str]:
    return {_work_id(index) for index in range(1, 216)} | {_work_id(359), _work_id(360)}


def _first_ids() -> set[str]:
    return {_work_id(index) for index in range(1, 359)}


def _discovery_payload(**overrides: object) -> dict:
    payload = {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_second_surface",
            "surface_version": "ml-shadow-scorer-v1-generalization-second-surface-v1",
        },
        "discovery_summary": {
            "status": "selected_needs_labels",
            "recommended_next_stage": "create_second_surface_labeling_plan_for_shadow_generalization_v1",
        },
        "selected_second_surface": {
            "ranking_run_id": EXPECTED_RANKING_RUN_ID,
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
            "embedding_version": "shadow-generalization-text-embedding-v1",
            "candidate_pool_work_count": 528,
            "candidate_pool_work_set_sha256": EXPECTED_CANDIDATE_SHA,
            "confirmatory_metric_eligible_work_count": 168,
        },
        "overlap_report": {
            "old_217_overlap_count": 217,
            "rank_9f4b2a2084_overlap_count": 358,
            "combined_prior_surface_overlap_count": 360,
        },
        "threshold_check": {
            "minimum_confirmatory_labeled_work_count": {"observed": 0, "threshold": 100, "passed": False},
            "minimum_confirmatory_positive_work_count": {"observed": 0, "threshold": 50, "passed": False},
            "minimum_confirmatory_negative_work_count": {"observed": 0, "threshold": 20, "passed": False},
            "minimum_distinct_negative_work_count": {"observed": 0, "threshold": 20, "passed": False},
            "minimum_confirmatory_label_coverage_rate": {"observed": 0.0, "threshold": 0.6, "passed": False},
            "final_score_coverage": {"observed": 528, "threshold": 528, "passed": True},
            "learned_probability_coverage": {"observed": 0, "threshold": 528, "passed": False},
        },
    }
    for key, value in overrides.items():
        if key == "status":
            payload["discovery_summary"]["status"] = value
        elif key == "next_stage":
            payload["discovery_summary"]["recommended_next_stage"] = value
        elif key == "ranking_run_id":
            payload["selected_second_surface"]["ranking_run_id"] = value
        else:
            payload[key] = value
    return payload


def _label_dataset_payload(labeled_ids: set[str] | None = None) -> dict:
    rows = []
    for work_id in sorted(labeled_ids or set()):
        rows.append(
            {
                "row_id": f"label-{work_id}",
                "work_id": work_id,
                "openalex_work_id": work_id,
                "relevance_label": "good",
                "novelty_label": "useful",
                "bridge_like_label": "partial",
                "good_or_acceptable": True,
            }
        )
    return {"dataset_version": "ml-label-dataset-v10", "rows": rows}


def _scoring_payload() -> dict:
    return {"metadata": {"eval_work_set_sha256": worksheet_mod.OLD_217_EVAL_SHA}, "candidate_pool_rows": []}


def _first_surface_payload() -> dict:
    return {
        "candidate_pool": {
            "candidate_work_set_sha256": "927df6837513753bcb025a5443adf35993ea323cfc0b11cac1395b0839f3f3a6",
            "candidate_rows": [{"canonical_openalex_work_id": work_id} for work_id in sorted(_first_ids())],
        }
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        },
        "label_policy": {
            "minimum_confirmatory_label_thresholds": {
                "minimum_confirmatory_labeled_work_count": 100,
                "minimum_confirmatory_positive_work_count": 50,
                "minimum_confirmatory_negative_work_count": 20,
                "minimum_distinct_negative_work_count": 20,
                "minimum_confirmatory_label_coverage_rate": 0.6,
            }
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, *, discovery: dict | None = None, labels: dict | None = None) -> dict[str, Path]:
    conflict = tmp_path / "conflict.md"
    conflict.write_text("# Conflict policy\n", encoding="utf-8")
    return {
        "generalization_second_surface_path": _write_json(tmp_path, "discovery.json", discovery or _discovery_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", labels or _label_dataset_payload()),
        "conflict_policy_path": conflict,
        "offline_production_candidate_scoring_v3_path": _write_json(tmp_path, "scoring.json", _scoring_payload()),
        "first_validated_surface_path": _write_json(tmp_path, "first-surface.json", _first_surface_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn | None = None, **kwargs: object) -> tuple[list[dict], dict, str]:
    return build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
        conn or _FakeConn(_candidate_rows()),
        **_paths(tmp_path),
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        repo_root=tmp_path,
        **kwargs,
    )


@pytest.fixture(autouse=True)
def _patch_overlap_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worksheet_mod, "_old_eval_ids_from_v3", lambda payload: _old_ids())
    monkeypatch.setattr(worksheet_mod, "_first_surface_ids", lambda payload: _first_ids())


def test_builds_168_rows_from_fixture_db_matching_discovery_counts(tmp_path: Path) -> None:
    rows, context, _markdown = _build(tmp_path)

    assert len(rows) == 168
    assert context["selection_summary"]["selected_row_count"] == 168
    assert context["selection_summary"]["excluded_old_217_count"] == 217
    assert context["selection_summary"]["excluded_first_surface_count"] == 358
    assert context["selection_summary"]["excluded_prior_overlap_union_count"] == 360
    assert rows[0]["work_id"] == _work_id(361)
    assert rows[-1]["work_id"] == _work_id(528)
    assert all(row[column] == "" for row in rows for column in worksheet_mod.BLANK_LABEL_COLUMNS)


def test_excludes_old_217_and_first_surface_overlap_rows(tmp_path: Path) -> None:
    rows, _context, _markdown = _build(tmp_path)
    exported_ids = {row["work_id"] for row in rows}

    assert not exported_ids.intersection(_old_ids())
    assert not exported_ids.intersection(_first_ids())
    assert _work_id(361) in exported_ids


def test_excludes_existing_v10_labeled_works(tmp_path: Path) -> None:
    labeled = {_work_id(361), _work_id(362)}
    paths = _paths(tmp_path, labels=_label_dataset_payload(labeled))

    rows, context, _markdown = build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
        _FakeConn(_candidate_rows()),
        **paths,
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        repo_root=tmp_path,
    )

    assert len(rows) == 166
    assert context["selection_summary"]["excluded_existing_v10_label_count"] == 2
    assert _work_id(361) not in {row["work_id"] for row in rows}


def test_row_id_stable_and_matches_sidecar(tmp_path: Path) -> None:
    rows, context, _markdown = _build(tmp_path)
    first = rows[0]
    expected = stable_row_id(
        worksheet_version=WORKSHEET_VERSION,
        seed=20260522,
        canonical_openalex_work_id=first["work_id"],
    )

    assert first["row_id"] == expected
    assert {row["row_id"] for row in rows} == {row["row_id"] for row in context["rows"]}
    assert context["row_id_policy"]["csv_row_id_set_equals_sidecar_row_id_set"] is True


def test_requested_rows_zero_includes_all_not_120(tmp_path: Path) -> None:
    rows, context, _markdown = _build(tmp_path, requested_rows=0)

    assert len(rows) == 168
    assert context["selection_summary"]["requested_rows"] == 0


def test_rejects_wrong_discovery_status_stage_or_run_id(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLabelingWorksheetError, match="status"):
        build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
            _FakeConn(_candidate_rows()),
            **_paths(tmp_path, discovery=_discovery_payload(status="selected_ready_for_generalization_audit")),
            repo_root=tmp_path,
        )
    with pytest.raises(MLShadowScorerSecondSurfaceLabelingWorksheetError, match="recommended_next_stage"):
        build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
            _FakeConn(_candidate_rows()),
            **_paths(tmp_path, discovery=_discovery_payload(next_stage="other")),
            repo_root=tmp_path,
        )
    with pytest.raises(MLShadowScorerSecondSurfaceLabelingWorksheetError, match="ranking_run_id"):
        build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
            _FakeConn(_candidate_rows()),
            **_paths(tmp_path, discovery=_discovery_payload(ranking_run_id="rank-other")),
            repo_root=tmp_path,
        )


def test_rejects_hosted_prod_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSurfaceLabelingWorksheetError, match="host"):
        build_ml_shadow_scorer_second_surface_labeling_worksheet_payloads(
            _FakeConn(_candidate_rows()),
            **_paths(tmp_path),
            database_url="postgresql://user:pass@prod.neon.tech/db",
            repo_root=tmp_path,
        )


def test_sort_order_deterministic(tmp_path: Path) -> None:
    shuffled = list(reversed(_candidate_rows()))
    rows, _context, _markdown = _build(tmp_path, conn=_FakeConn(shuffled))
    scores = [float(row["final_score"]) for row in rows]

    assert scores == sorted(scores, reverse=True)
    assert [row["rank_in_family"] for row in rows[:3]] == ["168", "167", "166"]


def test_cli_writes_csv_context_and_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worksheet_mod, "_connect_readonly", lambda database_url: _FakeConn(_candidate_rows()))
    paths = _paths(tmp_path)
    out_csv = tmp_path / "worksheet.csv"
    out_json = tmp_path / "context.json"
    out_md = tmp_path / "worksheet.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-surface-labeling-worksheet",
        "--generalization-second-surface",
        str(paths["generalization_second_surface_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--offline-production-candidate-scoring-v3",
        str(paths["offline_production_candidate_scoring_v3_path"]),
        "--first-validated-surface",
        str(paths["first_validated_surface_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--database-url",
        "postgresql://research_radar:research_radar@localhost:5432/research_radar",
        "--output",
        str(out_csv),
        "--context-output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    csv_rows = list(csv.DictReader(StringIO(out_csv.read_text(encoding="utf-8"))))
    context = json.loads(out_json.read_text(encoding="utf-8"))
    assert len(csv_rows) == 168
    assert context["metadata"]["artifact_type"] == "ml_shadow_scorer_second_surface_labeling_worksheet"
    assert "not ingest labels" in out_md.read_text(encoding="utf-8").lower()


def test_no_forbidden_imports_or_db_writes() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_surface_labeling_worksheet.py"
    ).read_text(encoding="utf-8")
    lower_source = module_source.lower()

    assert "from pipeline.ranking_run" not in lower_source
    assert "score_audit_embedding_probability" not in lower_source
    assert "from sklearn" not in lower_source
    assert "import sklearn" not in lower_source
    assert "insert into" not in lower_source
    assert "update " not in lower_source
    assert "delete from" not in lower_source
    assert "--database-url" in read_cli_parser_source(PACKAGE_ROOT)

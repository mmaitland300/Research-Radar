"""Tests for fresh eval labeling plan hybrid v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_fresh_eval_labeling_plan_hybrid import (
    MLFreshEvalLabelingPlanHybridError,
    PLAN_VERSION,
    build_ml_fresh_eval_labeling_plan_hybrid_payload,
    markdown_from_ml_fresh_eval_labeling_plan_hybrid,
)


def _surface_payload(
    *,
    status: str = "materialized_needs_labels",
    ready: bool = False,
    recommended_next_stage: str = "create_fresh_eval_labeling_plan_hybrid_v1",
    eligible: int = 44,
    labeled: int = 20,
    positive: int = 20,
    negative: int = 0,
    distinct_negative: int = 0,
    coverage: float = 20 / 44,
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_hybrid",
            "surface_version": "ml-fresh-eval-surface-hybrid-v1",
            "status": status,
        },
        "candidate_source": {
            "ranking_run_id": "rank-3904fec89d",
            "family": "emerging",
            "corpus_snapshot_version": "source-snapshot-20260425-044015",
        },
        "candidate_pool": {
            "candidate_work_count": 59,
            "candidate_work_set_sha256": "fresh-sha",
            "old_eval_work_set_sha256": "old-sha",
        },
        "disallowed_overlap_report": {
            "excluded_previous_eval_overlap_count": 15,
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
            "minimum_candidate_work_count": {"threshold": 100, "observed": eligible, "passed": False},
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
        "label_policy": {
            "minimum_confirmatory_label_thresholds": {
                "minimum_candidate_work_count": 100,
                "minimum_confirmatory_labeled_work_count": 100,
                "minimum_confirmatory_label_coverage_rate": 0.60,
                "minimum_confirmatory_positive_work_count": 50,
                "minimum_confirmatory_negative_work_count": 20,
                "minimum_distinct_negative_work_count": 20,
            }
        },
        "frozen_hybrid_arms": {
            "primary_confirmatory_arm": "hybrid_rank_mean_50_50",
            "secondary_reporting_arm": "hybrid_rank_mean_25_75_heuristic",
            "baselines_for_future_comparison": [
                "heuristic_final_score_baseline",
                "holdout_embedding_probability_baseline",
            ],
        },
    }


def _label_dataset_payload() -> dict:
    return {"dataset_version": "ml-label-dataset-v8", "rows": []}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
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
    conflict = tmp_path / "conflict-policy.md"
    conflict.write_text("# Conflict Policy\n\nNo silent conflict merge.\n", encoding="utf-8")
    return {
        "fresh_eval_surface_path": _write_json(tmp_path, "surface.json", surface or _surface_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", label_dataset or _label_dataset_payload()),
        "conflict_policy_path": conflict,
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_fresh_eval_labeling_plan_hybrid_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-18T00:00:00Z",
    )


def _gap(payload: dict, threshold: str) -> dict:
    return next(row for row in payload["threshold_gap_analysis"] if row["threshold"] == threshold)


def test_happy_path_with_materialized_needs_labels_fixture(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_eval_labeling_plan_hybrid"
    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["surface_summary"]["confirmatory_eligible_work_count"] == 44
    assert payload["recommended_collection_plan"]["primary_next_action"] == "create_larger_fresh_product_candidate_source"
    assert payload["recommended_next_stage"] == "create_fresh_product_candidate_ranking_source_v1"
    assert "create_fresh_product_candidate_ranking_source_v1" in payload["allowed_next_stages"]


def test_rejects_wrong_status_or_recommended_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLFreshEvalLabelingPlanHybridError, match="status"):
        _build(tmp_path, surface=_surface_payload(status="materialized_ready"))

    with pytest.raises(MLFreshEvalLabelingPlanHybridError, match="recommended_next_stage"):
        _build(tmp_path, surface=_surface_payload(recommended_next_stage="execute_hybrid_validation_on_fresh_surface_v1"))


def test_deficits_are_absolute_80_and_coverage_only_7(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["labeled_work_absolute_deficit"] == 80
    assert payload["labeled_work_absolute_deficit_formula"] == (
        "policy_minimum_confirmatory_labeled_work_count - observed_labeled_works = 100 - 20"
    )
    assert payload["coverage_only_deficit_at_current_eligible_count"] == 7
    assert payload["coverage_only_deficit_formula"] == (
        "ceil(0.60 * confirmatory_eligible_work_count) - observed_labeled_works = ceil(0.60 * 44) - 20 = 27 - 20"
    )
    assert _gap(payload, "minimum_confirmatory_labeled_work_count")["deficit"] == 80
    assert _gap(payload, "minimum_confirmatory_label_coverage_rate")["deficit"] == "7 at current 44"


def test_current_surface_cannot_be_made_ready_by_labeling_alone(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    diagnosis = payload["blocking_diagnosis"]

    assert diagnosis["current_surface_can_be_made_ready_by_labeling_alone"] is False
    assert diagnosis["candidate_source_under_minimum"] is True
    assert "confirmatory_eligible_work_count 44 < policy minimum 100" in diagnosis["reason"]
    assert "labeling all 24 current unlabeled eligible works cannot satisfy" in diagnosis["reason"]


def test_shadow_and_production_are_blocked(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["confirmatory_validation_complete"] is False
    assert blockers["missing_sufficient_fresh_candidate_surface"] is True
    assert blockers["missing_negative_labels"] is True


def test_markdown_distinguishes_absolute_80_from_coverage_only_7(tmp_path: Path) -> None:
    markdown = markdown_from_ml_fresh_eval_labeling_plan_hybrid(_build(tmp_path))

    assert "Absolute labeled-work deficit" in markdown
    assert "80 additional labeled works" in markdown
    assert "Coverage-only deficit on current 44-work surface" in markdown
    assert "7 additional labeled works" in markdown
    assert "does not override the absolute 100 labeled-work minimum" in markdown


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "plan.json"
    out_md = tmp_path / "plan.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-eval-labeling-plan-hybrid",
        "--fresh-eval-surface",
        str(paths["fresh_eval_surface_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["labeled_work_absolute_deficit"] == 80
    assert "Two Label Deficits" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_eval_labeling_plan_hybrid.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-fresh-eval-labeling-plan-hybrid"')
    end = cli_source.index("ml_fresh_eval_labeling_worksheet_hybrid_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

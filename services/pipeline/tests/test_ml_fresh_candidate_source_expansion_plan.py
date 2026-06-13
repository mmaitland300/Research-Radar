"""Tests for fresh candidate source expansion plan v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_fresh_candidate_source_expansion_plan import (
    MLFreshCandidateSourceExpansionPlanError,
    PLAN_VERSION,
    build_ml_fresh_candidate_source_expansion_plan_payload,
    markdown_from_ml_fresh_candidate_source_expansion_plan,
)


def _ranking_source_payload(
    *,
    status: str = "blocked_no_source_meets_candidate_threshold",
    selected_source: dict | None = None,
    recommended_next_stage: str = "create_new_or_larger_candidate_snapshot",
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_product_candidate_ranking_source",
            "source_version": "ml-fresh-product-candidate-ranking-source-v1",
        },
        "source_selection": {
            "status": status,
            "selected_source": selected_source,
            "recommended_next_stage": recommended_next_stage,
            "minimum_confirmatory_candidate_works": 100,
        },
        "recommended_next_stage": recommended_next_stage,
        "candidate_sources_considered": [
            {
                "ranking_run_id": "rank-3904fec89d",
                "candidate_work_set_sha256": "1a62e9802e8562854e9b4fd2c44ad72a183d81fe8dbb86b50b09d94fb496e926",
                "confirmatory_eligible_work_count": 44,
                "overlap_with_old_217_count": 15,
                "label_coverage_summary": {
                    "labeled_work_count": 20,
                    "positive_labeled_work_count": 20,
                    "negative_labeled_work_count": 0,
                    "distinct_negative_work_count": 0,
                    "label_coverage_rate": 20 / 44,
                },
            }
        ],
    }


def _labeling_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_labeling_plan_hybrid",
            "plan_version": "ml-fresh-eval-labeling-plan-hybrid-v1",
        },
        "recommended_next_stage": "create_fresh_product_candidate_ranking_source_v1",
        "blocking_diagnosis": {"current_surface_can_be_made_ready_by_labeling_alone": False},
        "labeled_work_absolute_deficit": 80,
        "coverage_only_deficit_at_current_eligible_count": 7,
        "recommended_collection_plan": {
            "minimum_negative_works_needed": 20,
            "minimum_distinct_negative_works_needed": 20,
        },
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
    ranking_source: dict | None = None,
    labeling_plan: dict | None = None,
    policy: dict | None = None,
    label_dataset: dict | None = None,
) -> dict[str, Path]:
    conflict = tmp_path / "conflict-policy.md"
    conflict.write_text("# Conflict Policy\n\nNo silent merge.\n", encoding="utf-8")
    return {
        "fresh_product_candidate_ranking_source_path": _write_json(
            tmp_path, "ranking-source.json", ranking_source or _ranking_source_payload()
        ),
        "fresh_eval_labeling_plan_path": _write_json(tmp_path, "labeling-plan.json", labeling_plan or _labeling_plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
        "label_dataset_path": _write_json(tmp_path, "labels.json", label_dataset or _label_dataset_payload()),
        "conflict_policy_path": conflict,
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_fresh_candidate_source_expansion_plan_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-18T00:00:00Z",
    )


def test_happy_path_from_blocked_ranking_source_artifact(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_candidate_source_expansion_plan"
    assert payload["metadata"]["plan_version"] == PLAN_VERSION
    assert payload["current_blocker_summary"]["sources_considered_count"] == 1
    assert payload["current_blocker_summary"]["best_source_ranking_run_id"] == "rank-3904fec89d"
    assert payload["recommended_next_stage"] == "implement_or_run_fresh_product_candidate_source_build_v1"


def test_rejects_non_blocked_status_or_selected_source(tmp_path: Path) -> None:
    with pytest.raises(MLFreshCandidateSourceExpansionPlanError, match="status"):
        _build(tmp_path, ranking_source=_ranking_source_payload(status="source_frozen_needs_materialization"))

    with pytest.raises(MLFreshCandidateSourceExpansionPlanError, match="selected_source"):
        _build(tmp_path, ranking_source=_ranking_source_payload(selected_source={"ranking_run_id": "rank-ok"}))


def test_candidate_gap_is_56(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["current_blocker_summary"]["candidate_gap"] == 56
    assert payload["current_blocker_summary"]["minimum_confirmatory_candidate_works"] == 100
    assert payload["current_blocker_summary"]["best_source_confirmatory_eligible_work_count"] == 44


def test_rejects_wrong_ranking_source_recommended_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLFreshCandidateSourceExpansionPlanError, match="recommended_next_stage"):
        _build(
            tmp_path,
            ranking_source=_ranking_source_payload(recommended_next_stage="rerun_fresh_eval_surface_materialize_with_selected_source"),
        )


def test_strategies_and_forbidden_shortcuts_are_present(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["allowed_expansion_strategies"][0]["strategy_id"] == "create_newer_corpus_snapshot_and_candidate_run"
    forbidden_blob = "\n".join(payload["forbidden_expansion_strategies"]).lower()
    assert "old 217 eval surface" in forbidden_blob
    assert "lowering policy thresholds" in forbidden_blob
    assert "rank-3904fec89d" in forbidden_blob


def test_shadow_and_production_are_blocked(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["confirmatory_validation_complete"] is False
    assert payload["policy_assertions"]["hybrid_validation_on_44_work_surface_allowed"] is False


def test_markdown_contains_label_gaps_and_future_command(tmp_path: Path) -> None:
    markdown = markdown_from_ml_fresh_candidate_source_expansion_plan(_build(tmp_path))

    assert "Candidate gap" in markdown
    assert "Future command name: `ml-fresh-product-candidate-source-build`" in markdown
    assert "Absolute labeled-work deficit after expansion: 80" in markdown
    assert "Coverage-only deficit on current 44: 7" in markdown
    assert "Negative work deficit: 20" in markdown


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "expansion-plan.json"
    out_md = tmp_path / "expansion-plan.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-candidate-source-expansion-plan",
        "--fresh-product-candidate-ranking-source",
        str(paths["fresh_product_candidate_ranking_source_path"]),
        "--fresh-eval-labeling-plan",
        str(paths["fresh_eval_labeling_plan_path"]),
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

    assert json.loads(out_json.read_text(encoding="utf-8"))["current_blocker_summary"]["candidate_gap"] == 56
    assert "Allowed Strategies" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_candidate_source_expansion_plan.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-fresh-candidate-source-expansion-plan"')
    end = cli_source.index("ml_source_split_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

"""Tests for hybrid scorer offline experiment v1."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_hybrid_scorer_offline_experiment import (
    EXPERIMENT_VERSION,
    MLHybridScorerOfflineExperimentError,
    build_ml_hybrid_scorer_offline_experiment_payload,
    write_ml_hybrid_scorer_offline_experiment,
)


EXPECTED_ARMS = {
    "heuristic_final_score_baseline": "final_score",
    "holdout_embedding_probability_baseline": "audit_embedding_probability_work",
    "hybrid_rank_mean_50_50": "0.5 * rank_pct(final_score) + 0.5 * rank_pct(audit_embedding_probability_work)",
    "hybrid_rank_mean_75_25_heuristic": "0.75 * rank_pct(final_score) + 0.25 * rank_pct(audit_embedding_probability_work)",
    "hybrid_rank_mean_25_75_heuristic": "0.25 * rank_pct(final_score) + 0.75 * rank_pct(audit_embedding_probability_work)",
}


def _precision_entry(total: int, positives: int, k: int) -> dict:
    return {
        "precision": None,
        "recall": None,
        "reason": f"requires at least {k} labeled candidate works",
        "labeled_work_count": total,
        "positive_count": positives,
        "negative_count": total - positives,
    }


def _scoring_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v3",
            "scoring_mode": "heuristic_and_holdout_embedding_scorer",
            "target": "good_or_acceptable",
            "ranking_run_id": "rank-fixture",
            "family": "emerging",
            "eval_work_set_sha256": "eval-sha",
        },
        "candidate_pool_summary": {
            "candidate_unique_canonical_work_count": 4,
            "candidate_pool_work_set_sha256": "eval-sha",
        },
        "leakage_report": {
            "train_rows_used_in_metrics": 0,
            "train_works_used_in_metrics": 0,
            "candidate_pool_work_set_matches_eval_set": True,
        },
        "scoring_mode_details": {"eval_only": True},
        "candidate_pool_rows": [
            {"canonical_openalex_work_id": "W1", "title": "one", "year": 2024, "heuristic_rank": 1, "final_score": 0.9},
            {"canonical_openalex_work_id": "W2", "title": "two", "year": 2024, "heuristic_rank": 2, "final_score": 0.8},
            {"canonical_openalex_work_id": "W3", "title": "three", "year": 2024, "heuristic_rank": 3, "final_score": 0.8},
            {"canonical_openalex_work_id": "W4", "title": "four", "year": 2024, "heuristic_rank": 4, "final_score": 0.1},
        ],
        "labeled_eval_subset": [
            {
                "canonical_openalex_work_id": "W1",
                "final_score": 0.9,
                "label_any_positive": True,
                "observation_count": 1,
                "positive_observation_count": 1,
                "negative_observation_count": 0,
                "conflicting_target_observations": False,
                "row_ids": ["r1"],
                "audit_embedding_probability_work": 0.2,
            },
            {
                "canonical_openalex_work_id": "W2",
                "final_score": 0.8,
                "label_any_positive": False,
                "observation_count": 1,
                "positive_observation_count": 0,
                "negative_observation_count": 1,
                "conflicting_target_observations": False,
                "row_ids": ["r2"],
                "audit_embedding_probability_work": 0.8,
            },
            {
                "canonical_openalex_work_id": "W3",
                "final_score": 0.8,
                "label_any_positive": True,
                "observation_count": 1,
                "positive_observation_count": 1,
                "negative_observation_count": 0,
                "conflicting_target_observations": False,
                "row_ids": ["r3"],
                "audit_embedding_probability_work": 0.8,
            },
            {
                "canonical_openalex_work_id": "W4",
                "final_score": 0.1,
                "label_any_positive": None,
                "observation_count": 0,
                "positive_observation_count": 0,
                "negative_observation_count": 0,
                "conflicting_target_observations": False,
                "row_ids": [],
                "audit_embedding_probability_work": 1.0,
            },
        ],
        "heuristic_metrics": {
            "roc_auc_mann_whitney": 0.75,
            "average_precision": 0.8333333333333333,
            "precision_recall_at_k": {
                str(k): _precision_entry(3, 2, k)
                for k in (5, 10, 20)
            },
        },
        "learned_or_embedding_metrics": {
            "eval_only": True,
            "metrics": {
                "roc_auc_mann_whitney": 0.25,
                "average_precision": 0.5833333333333333,
                "precision_recall_at_k": {
                    str(k): _precision_entry(3, 2, k)
                    for k in (5, 10, 20)
                },
            },
        },
    }


def _gates_payload(*, material_lift_passed: bool = False) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v3",
        },
        "independent_learned_validation_passed": True,
        "material_lift_passed": material_lift_passed,
        "recommended_next_stage": "create_hybrid_scorer_offline_experiment_v1",
    }


def _spec_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_hybrid_scorer_offline_experiment_spec",
            "spec_version": "ml-hybrid-scorer-offline-experiment-v1-spec",
            "eval_work_set_sha256": "eval-sha",
        },
        "future_gate_contract": {"best_arm_on_seen_eval_is_exploratory_only": True},
        "pre_registered_hybrid_arms": [
            {"arm_id": arm_id, "score_formula": formula}
            for arm_id, formula in EXPECTED_ARMS.items()
        ],
    }


def _assignment_payload(*, eval_sha: str = "eval-sha") -> dict:
    return {
        "metadata": {
            "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
            "eval_work_set_sha256": eval_sha,
        },
        "leakage_report": {"global_zero_assertion": True},
        "assignments": [
            {"canonical_openalex_work_id": "W1", "assignment": "eval"},
            {"canonical_openalex_work_id": "W2", "assignment": "eval"},
            {"canonical_openalex_work_id": "W3", "assignment": "eval"},
            {"canonical_openalex_work_id": "W4", "assignment": "eval"},
        ],
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    scoring: dict | None = None,
    gates: dict | None = None,
    spec: dict | None = None,
    assignment: dict | None = None,
) -> dict[str, Path]:
    return {
        "production_candidate_scoring_path": _write_json(tmp_path, "scoring.json", scoring or _scoring_payload()),
        "production_candidate_metric_gates_path": _write_json(tmp_path, "gates.json", gates or _gates_payload()),
        "experiment_spec_path": _write_json(tmp_path, "spec.json", spec or _spec_payload()),
        "holdout_assignment_path": _write_json(tmp_path, "assignment.json", assignment or _assignment_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_hybrid_scorer_offline_experiment_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def test_happy_path_fixture_with_scoring_v3_shape(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["artifact_type"] == "ml_hybrid_scorer_offline_experiment"
    assert payload["metadata"]["experiment_version"] == EXPERIMENT_VERSION
    assert payload["candidate_eval_coverage"]["candidate_pool_work_count"] == 4
    assert payload["candidate_eval_coverage"]["labeled_eval_metric_work_count"] == 3
    assert set(payload["arm_metrics"]) == set(EXPECTED_ARMS)
    assert payload["summary"]["best_arm_selection_is_exploratory_only"] is True
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False


def test_arm_formulas_exactly_match_spec(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    observed = {item["arm_id"]: item["score_formula"] for item in payload["pre_registered_arms_executed"]}
    assert observed == EXPECTED_ARMS


def test_rank_pct_computed_over_full_pool_not_eval_subset(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    by_work = {row["canonical_openalex_work_id"]: row for row in payload["candidate_work_scores"]}

    assert by_work["W2"]["final_score_rank_pct"] == pytest.approx(0.5)
    assert by_work["W2"]["audit_embedding_probability_rank_pct"] == pytest.approx(0.5)
    assert by_work["W4"]["audit_embedding_probability_rank_pct"] == pytest.approx(1.0)


def test_average_rank_tie_policy(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    by_work = {row["canonical_openalex_work_id"]: row for row in payload["candidate_work_scores"]}

    assert by_work["W2"]["final_score_rank_pct"] == pytest.approx(by_work["W3"]["final_score_rank_pct"])
    assert by_work["W2"]["audit_embedding_probability_rank_pct"] == pytest.approx(
        by_work["W3"]["audit_embedding_probability_rank_pct"]
    )


def test_rejects_gates_material_lift_passed_true(tmp_path: Path) -> None:
    with pytest.raises(MLHybridScorerOfflineExperimentError, match="material_lift_passed"):
        _build(tmp_path, gates=_gates_payload(material_lift_passed=True))


def test_rejects_missing_learned_probability_on_pool_work(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["labeled_eval_subset"][3]["audit_embedding_probability_work"] = None

    with pytest.raises(MLHybridScorerOfflineExperimentError, match="audit_embedding_probability_work is missing"):
        _build(tmp_path, scoring=scoring)


def test_rejects_duplicate_candidate_canonical_ids(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["candidate_pool_rows"][1]["canonical_openalex_work_id"] = "W1"

    with pytest.raises(MLHybridScorerOfflineExperimentError, match="duplicate canonical"):
        _build(tmp_path, scoring=scoring)


def test_rejects_duplicate_labeled_eval_subset_canonical_ids(tmp_path: Path) -> None:
    scoring = _scoring_payload()
    scoring["labeled_eval_subset"][1]["canonical_openalex_work_id"] = "W1"

    with pytest.raises(MLHybridScorerOfflineExperimentError, match="duplicate canonical"):
        _build(tmp_path, scoring=scoring)


def test_rejects_eval_sha_mismatch(tmp_path: Path) -> None:
    with pytest.raises(MLHybridScorerOfflineExperimentError, match="eval_work_set_sha256"):
        _build(tmp_path, assignment=_assignment_payload(eval_sha="different-sha"))


def test_holdout_embedding_baseline_matches_scoring_v3_learned_metrics(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    metrics = payload["arm_metrics"]["holdout_embedding_probability_baseline"]

    assert metrics["roc_auc_mann_whitney"] == pytest.approx(
        _scoring_payload()["learned_or_embedding_metrics"]["metrics"]["roc_auc_mann_whitney"]
    )
    assert metrics["average_precision"] == pytest.approx(
        _scoring_payload()["learned_or_embedding_metrics"]["metrics"]["average_precision"]
    )


def _scoring_payload_with_lifted_baseline_only() -> dict:
    labels = [True, True, True, False, False, False]
    final_scores = [
        0.21392441974876908,
        0.055544671860936146,
        0.70397554245907,
        0.13957093537602938,
        0.9334445443216315,
        0.4869251046960519,
    ]
    learned_scores = [
        0.12563430215861582,
        0.8829883502268094,
        0.07382927252295557,
        0.8064623720525329,
        0.3767064397211578,
        0.8782375714938467,
    ]
    scoring = _scoring_payload()
    scoring["candidate_pool_rows"] = []
    scoring["labeled_eval_subset"] = []
    for idx, (label, final_score, learned_score) in enumerate(zip(labels, final_scores, learned_scores), start=1):
        work_id = f"W{idx}"
        scoring["candidate_pool_rows"].append(
            {
                "canonical_openalex_work_id": work_id,
                "title": str(idx),
                "heuristic_rank": idx,
                "final_score": final_score,
            }
        )
        scoring["labeled_eval_subset"].append(
            {
                "canonical_openalex_work_id": work_id,
                "final_score": final_score,
                "label_any_positive": label,
                "observation_count": 1,
                "row_ids": [f"r{idx}"],
                "audit_embedding_probability_work": learned_score,
            }
        )
    scoring["candidate_pool_summary"]["candidate_unique_canonical_work_count"] = 6
    scoring["heuristic_metrics"]["roc_auc_mann_whitney"] = 0.3333333333333333
    scoring["heuristic_metrics"]["average_precision"] = 0.5
    scoring["learned_or_embedding_metrics"]["metrics"]["roc_auc_mann_whitney"] = 0.3333333333333333
    scoring["learned_or_embedding_metrics"]["metrics"]["average_precision"] = 0.6333333333333333
    scoring["learned_or_embedding_metrics"]["metrics"]["precision_recall_at_k"]["5"] = {
        "precision": 0.4,
        "recall": 2 / 3,
        "reason": None,
        "labeled_work_count": 6,
        "positive_count": 3,
        "negative_count": 3,
        "top_k_labeled_positive_count": 2,
        "top_k_labeled_negative_count": 3,
    }
    return scoring


def test_hybrid_material_lift_only_counts_hybrid_arms_not_baselines(tmp_path: Path) -> None:
    scoring = _scoring_payload_with_lifted_baseline_only()
    assignment = _assignment_payload()
    assignment["assignments"] = [
        {"canonical_openalex_work_id": f"W{idx}", "assignment": "eval"}
        for idx in range(1, 7)
    ]

    payload = _build(tmp_path, scoring=scoring, assignment=assignment)

    assert payload["comparisons_vs_heuristic"]["holdout_embedding_probability_baseline"][
        "material_lift_passed_against_heuristic"
    ] is True
    assert payload["summary"]["hybrid_material_lift_passed"] is False


def test_best_arm_selection_is_exploratory_only_true(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["summary"]["best_arm_selection_is_exploratory_only"] is True


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "hybrid.json"
    out_md = tmp_path / "hybrid.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-hybrid-scorer-offline-experiment",
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--production-candidate-metric-gates",
        str(paths["production_candidate_metric_gates_path"]),
        "--experiment-spec",
        str(paths["experiment_spec_path"]),
        "--holdout-assignment",
        str(paths["holdout_assignment_path"]),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["metadata"]["experiment_version"] == EXPERIMENT_VERSION
    assert "Arm Metrics" in out_md.read_text(encoding="utf-8")


def test_module_imports_no_db_network_or_ml_clients_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_hybrid_scorer_offline_experiment.py"
    ).read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-hybrid-scorer-offline-experiment"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

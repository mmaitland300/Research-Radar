"""Tests for learned scorer holdout policy v1."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_learned_scorer_holdout_policy import (
    MLLearnedScorerHoldoutPolicyError,
    POLICY_VERSION,
    build_ml_learned_scorer_holdout_policy_payload,
    markdown_from_ml_learned_scorer_holdout_policy,
)


def _row(row_id: str, work_id: str, good: bool | None, *, split: str = "audit_only") -> dict:
    relevance = "good" if good is True else "miss" if good is False else ""
    return {
        "dataset_version": "ml-label-dataset-v8",
        "row_id": row_id,
        "split": split,
        "paper_id": f"https://openalex.org/{work_id}" if work_id else "",
        "work_id": work_id,
        "openalex_work_id": work_id,
        "review_pool_variant": "fixture_pool",
        "family": "emerging",
        "relevance_label": relevance,
        "novelty_label": "useful" if good is not None else "",
        "bridge_like_label": "no" if good is not None else "",
        "good_or_acceptable": good,
        "surprising_or_useful": good,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v8",
        "rows": [
            _row("r1", "W1", True),
            _row("r2", "W1", False),
            _row("r3", "W2", True),
            _row("r4", "W4", False),
            _row("r5", "W5", True, split="train_only"),
        ],
        "metadata": {
            "duplicate_paper_id_report": {"duplicate_paper_id_count": 1},
            "conflicting_label_report": {"conflicting_label_count": 2},
            "derived_target_conflict_report": {"derived_target_conflict_count": 1},
        },
    }


def _split_policy_payload(*, grouped: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_label_split_policy",
            "policy_version": "ml-label-split-policy-v1",
        },
        "allowed_targets_for_v1_split": ["good_or_acceptable"],
        "forbidden_targets": ["surprising_or_useful"],
        "policy_assertions": {
            "requires_grouped_split_by_work": grouped,
            "permits_row_level_random_split": False,
            "permits_silent_conflict_resolution": False,
            "production_default_change_allowed": False,
            "surprising_or_useful_allowed_for_v1_split": False,
        },
        "randomness_policy": {"recommended_default_seed": 20260515},
    }


def _embeddings_payload(label_sha: str, *, missing_r4: bool = False) -> dict:
    rows = []
    for row_id in ["r1", "r2", "r3", "r4"]:
        if row_id == "r4" and missing_r4:
            continue
        rows.append(
            {
                "row_id": row_id,
                "embedding_status": "ok",
                "embedding": [1.0, 0.0],
                "paper_id": f"https://openalex.org/W{row_id[-1]}",
            }
        )
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v3",
            "source_label_dataset_sha256": label_sha,
            "source_label_dataset_version": "ml-label-dataset-v8",
            "embedding_dimensions": 2,
        },
        "rows": rows,
    }


def _scoring_payload(*, include_candidate_rows: bool = True) -> dict:
    payload = {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v2",
            "scoring_mode": "heuristic_and_audit_embedding_scorer",
            "ranking_run_id": "rank-fixture",
            "family": "emerging",
            "target": "good_or_acceptable",
        },
        "candidate_pool_definition": {"ranking_run_id": "rank-fixture", "family": "emerging"},
        "candidate_pool_summary": {"candidate_unique_canonical_work_count": 3},
        "label_join_summary": {
            "labeled_eval_subset_work_count": 2,
            "labeled_eval_subset_positive_work_count": 2,
            "labeled_eval_subset_negative_work_count": 1,
        },
    }
    if include_candidate_rows:
        payload["candidate_pool_rows"] = [
            {"canonical_openalex_work_id": "W1", "title": "one"},
            {"canonical_openalex_work_id": "W2", "title": "two"},
            {"canonical_openalex_work_id": "W3", "title": "three-unlabeled"},
        ]
    return payload


def _gates_payload(
    *,
    next_stage: str = "create_learned_scorer_holdout_policy_v1",
    independent: bool | None = False,
) -> dict:
    payload = {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v2",
        },
        "learned_scorer_application_gates_passed": True,
        "recommended_next_stage": next_stage,
    }
    if independent is not None:
        payload["independent_learned_validation_passed"] = independent
    return payload


def _production_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_production_readiness_plan",
            "plan_version": "ml-production-readiness-plan-v1",
            "overall_status": "research_only",
        },
        "targets": {"good_or_acceptable": {"production_eligible": False}},
        "production_default_authorized": False,
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    *,
    split_policy: dict | None = None,
    scoring: dict | None = None,
    gates: dict | None = None,
    embeddings_missing_r4: bool = False,
) -> dict[str, Path]:
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    label_sha = sha256_file(label_path)
    return {
        "label_dataset_path": label_path,
        "split_policy_path": _write_json(tmp_path, "split-policy.json", split_policy or _split_policy_payload()),
        "embeddings_path": _write_json(
            tmp_path,
            "embeddings.json",
            _embeddings_payload(label_sha, missing_r4=embeddings_missing_r4),
        ),
        "production_candidate_scoring_path": _write_json(
            tmp_path,
            "scoring-v2.json",
            scoring or _scoring_payload(),
        ),
        "production_candidate_metric_gates_path": _write_json(
            tmp_path,
            "metric-gates-v2.json",
            gates or _gates_payload(),
        ),
        "production_readiness_plan_path": _write_json(tmp_path, "plan.json", _production_plan_payload()),
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_learned_scorer_holdout_policy_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def test_happy_path_writes_policy_inventory_and_boundary(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    inventory = payload["dataset_inventory"]
    expected_sha = hashlib.sha256("W1\nW2\nW3\n".encode("utf-8")).hexdigest()

    assert payload["metadata"]["artifact_type"] == "ml_learned_scorer_holdout_policy"
    assert payload["metadata"]["policy_version"] == POLICY_VERSION
    assert payload["target_policy"]["eligible_target"] == "good_or_acceptable"
    assert payload["primary_holdout_strategy"]["strategy_id"] == "product_candidate_snapshot_holdout"
    assert payload["primary_holdout_strategy"]["eval_work_set_definition"]["eval_work_set_count"] == 3
    assert inventory["product_candidate_pool_work_count"] == 3
    assert inventory["product_candidate_labeled_eval_work_count"] == 2
    assert inventory["product_candidate_unlabeled_eval_work_count"] == 1
    assert inventory["product_candidate_eval_work_set_sha256"] == expected_sha
    assert inventory["train_work_count_estimate"] == 1
    assert inventory["train_observation_count_estimate"] == 1
    assert inventory["eval_observation_count_estimate"] == 3
    assert inventory["eval_labeled_observation_count_estimate"] == 3
    assert inventory["eval_positive_observation_count"] == 2
    assert inventory["eval_negative_observation_count"] == 1
    assert inventory["overlap_work_count_between_full_fit_training_universe_and_eval_set"] == 2
    assert payload["shadow_and_production_blockers"]["shadow_scoring_authorized"] is False
    assert payload["shadow_and_production_blockers"]["production_default_authorized"] is False


def test_rejects_gates_v2_wrong_recommended_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutPolicyError, match="recommended_next_stage"):
        _build(tmp_path, gates=_gates_payload(next_stage="draft_ml_shadow_scorer_v1_spec"))


@pytest.mark.parametrize("independent", [True, None])
def test_rejects_gates_v2_independent_validation_true_or_missing(
    tmp_path: Path,
    independent: bool | None,
) -> None:
    with pytest.raises(MLLearnedScorerHoldoutPolicyError, match="independent_learned_validation_passed"):
        _build(tmp_path, gates=_gates_payload(independent=independent))


def test_rejects_split_policy_without_grouped_by_work(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutPolicyError, match="requires_grouped_split_by_work"):
        _build(tmp_path, split_policy=_split_policy_payload(grouped=False))


def test_rejects_scoring_v2_without_candidate_pool_rows_canonical_ids(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutPolicyError, match="candidate_pool_rows"):
        _build(tmp_path, scoring=_scoring_payload(include_candidate_rows=False))

    scoring = _scoring_payload()
    scoring["candidate_pool_rows"][1].pop("canonical_openalex_work_id")
    with pytest.raises(MLLearnedScorerHoldoutPolicyError, match="canonical_openalex_work_id"):
        _build(tmp_path, scoring=scoring)


def test_eval_work_set_uses_all_candidate_rows_not_only_labeled_subset(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    inventory = payload["dataset_inventory"]

    assert inventory["product_candidate_pool_work_count"] == 3
    assert inventory["product_candidate_labeled_eval_work_count"] == 2
    assert inventory["product_candidate_unlabeled_eval_work_count"] == 1


def test_train_estimates_exclude_eval_work_ids(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    inventory = payload["dataset_inventory"]

    assert inventory["train_work_count_estimate"] == 1
    assert inventory["train_observation_count_estimate"] == 1
    assert inventory["eval_observation_count_estimate"] == 3


def test_duplicate_conflict_counts_are_preserved_and_reported(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    inventory = payload["dataset_inventory"]

    assert inventory["duplicate_work_group_count"] == 1
    assert inventory["duplicate_observation_pressure"] == 1
    assert inventory["conflicting_target_work_group_count"] == 1
    assert inventory["duplicate_conflict_rollups"]["duplicate_paper_id_count"] == 1
    assert payload["conflict_and_duplicate_policy"]["silent_label_merge_allowed"] is False


def test_embeddings_artifact_is_validated_and_used_for_eligibility_counts(tmp_path: Path) -> None:
    payload = _build(tmp_path, embeddings_missing_r4=True)
    inventory = payload["dataset_inventory"]

    assert inventory["audit_eligible_observation_count"] == 4
    assert inventory["audit_eligible_observations_with_embedding_count"] == 3
    assert inventory["train_work_count_estimate"] == 0
    assert inventory["excluded_rows_by_reason"]["missing_or_bad_embedding"] == 1


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "holdout-policy.json"
    out_md = tmp_path / "holdout-policy.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-learned-scorer-holdout-policy",
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--embeddings",
        str(paths["embeddings_path"]),
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--production-candidate-metric-gates",
        str(paths["production_candidate_metric_gates_path"]),
        "--production-readiness-plan",
        str(paths["production_readiness_plan_path"]),
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
    assert data["metadata"]["artifact_type"] == "ml_learned_scorer_holdout_policy"
    assert "What Still Blocks Shadow" in out_md.read_text(encoding="utf-8")


def test_markdown_contains_not_validation_and_shadow_blocked_sections(tmp_path: Path) -> None:
    md = markdown_from_ml_learned_scorer_holdout_policy(_build(tmp_path))

    assert "Executive Summary" in md
    assert "Why V2 Gates Require This Policy" in md
    assert "Primary Holdout Strategy" in md
    assert "Train Vs Eval Definitions" in md
    assert "Product-Candidate Eval Work-Set Source" in md
    assert "Leakage Rules" in md
    assert "Conflict/Duplicate Handling" in md
    assert "Dataset Inventory Summary" in md
    assert "Future Command Chain" in md
    assert "What Still Blocks Shadow" in md
    assert "Not validation." in md
    assert "Shadow scoring authorized: False" in md


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_learned_scorer_holdout_policy.py").read_text(
        encoding="utf-8"
    ).lower()
    assert "psycopg" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "openai" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-learned-scorer-holdout-policy"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

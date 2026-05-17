"""Tests for learned scorer holdout assignment v1."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_learned_scorer_holdout_assignment import (
    ASSIGNMENT_VERSION,
    MLLearnedScorerHoldoutAssignmentError,
    build_ml_learned_scorer_holdout_assignment_payload,
)


def _row(row_id: str, work_id: str, good: bool | None, *, split: str = "audit_only") -> dict:
    return {
        "dataset_version": "ml-label-dataset-v8",
        "row_id": row_id,
        "split": split,
        "paper_id": f"https://openalex.org/{work_id}",
        "work_id": work_id,
        "openalex_work_id": work_id,
        "review_pool_variant": "fixture_pool",
        "family": "emerging",
        "ranking_run_id": "rank-fixture",
        "relevance_label": "good" if good is True else "miss" if good is False else "",
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


def _split_policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_label_split_policy",
            "policy_version": "ml-label-split-policy-v1",
        },
        "policy_assertions": {
            "requires_grouped_split_by_work": True,
            "permits_row_level_random_split": False,
        },
        "randomness_policy": {"recommended_default_seed": 20260515},
    }


def _embeddings_payload(label_sha: str, *, duplicate_row_id: bool = False) -> dict:
    rows = [
        {"row_id": "r1", "embedding_status": "ok", "embedding": [1.0, 0.0]},
        {"row_id": "r2", "embedding_status": "ok", "embedding": [0.5, 0.0]},
        {"row_id": "r3", "embedding_status": "ok", "embedding": [0.0, 1.0]},
        {"row_id": "r4", "embedding_status": "ok", "embedding": [-1.0, 0.0]},
    ]
    if duplicate_row_id:
        rows.append({"row_id": "r4", "embedding_status": "ok", "embedding": [-1.0, 0.0]})
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v3",
            "source_label_dataset_sha256": label_sha,
            "embedding_dimensions": 2,
        },
        "rows": rows,
    }


def _scoring_payload(*, missing_canonical: bool = False) -> dict:
    rows = [
        {"canonical_openalex_work_id": "W1"},
        {"canonical_openalex_work_id": "W2"},
        {"canonical_openalex_work_id": "W3"},
    ]
    if missing_canonical:
        rows[1] = {"title": "missing"}
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_scoring",
            "experiment_version": "ml-offline-production-candidate-scoring-v2",
            "scoring_mode": "heuristic_and_audit_embedding_scorer",
            "ranking_run_id": "rank-fixture",
            "family": "emerging",
        },
        "candidate_pool_definition": {"ranking_run_id": "rank-fixture", "family": "emerging"},
        "candidate_pool_summary": {"candidate_unique_canonical_work_count": 3},
        "candidate_pool_rows": rows,
    }


def _gates_payload(*, learned_passed: bool = True) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v2",
        },
        "learned_scorer_application_gates_passed": learned_passed,
        "independent_learned_validation_passed": False,
    }


def _eval_sha() -> str:
    return hashlib.sha256("W1\nW2\nW3\n".encode("utf-8")).hexdigest()


def _holdout_policy_payload(
    *,
    input_shas: dict[str, str],
    eval_sha: str | None = None,
    strategy_id: str = "product_candidate_snapshot_holdout",
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_policy",
            "policy_version": "ml-learned-scorer-holdout-policy-v1",
            "inputs": [
                {"name": name, "path": f"{name}.json", "sha256": sha}
                for name, sha in input_shas.items()
            ],
        },
        "primary_holdout_strategy": {
            "strategy_id": strategy_id,
            "eval_work_set_definition": {
                "eval_work_set_sha256": eval_sha or _eval_sha(),
            },
        },
        "dataset_inventory": {
            "product_candidate_pool_work_count": 3,
            "product_candidate_eval_work_set_sha256": eval_sha or _eval_sha(),
        },
        "randomness_policy": {"recommended_default_seed": 20260515},
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
    policy_eval_sha: str | None = None,
    policy_strategy_id: str = "product_candidate_snapshot_holdout",
    policy_sha_mismatch: bool = False,
    duplicate_embedding_row_id: bool = False,
) -> dict[str, Path]:
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    label_sha = sha256_file(label_path)
    split_path = _write_json(tmp_path, "split-policy.json", _split_policy_payload())
    embeddings_path = _write_json(
        tmp_path,
        "embeddings.json",
        _embeddings_payload(label_sha, duplicate_row_id=duplicate_embedding_row_id),
    )
    scoring_path = _write_json(tmp_path, "scoring.json", scoring or _scoring_payload())
    gates_path = _write_json(tmp_path, "gates.json", gates or _gates_payload())
    input_shas = {
        "label_dataset": sha256_file(label_path),
        "split_policy": sha256_file(split_path),
        "embeddings": sha256_file(embeddings_path),
        "production_candidate_scoring": sha256_file(scoring_path),
        "production_candidate_metric_gates": sha256_file(gates_path),
    }
    if policy_sha_mismatch:
        input_shas["label_dataset"] = "wrong-sha"
    policy_path = _write_json(
        tmp_path,
        "holdout-policy.json",
        _holdout_policy_payload(
            input_shas=input_shas,
            eval_sha=policy_eval_sha,
            strategy_id=policy_strategy_id,
        ),
    )
    return {
        "label_dataset_path": label_path,
        "split_policy_path": split_path,
        "embeddings_path": embeddings_path,
        "production_candidate_scoring_path": scoring_path,
        "holdout_policy_path": policy_path,
        "production_candidate_metric_gates_path": gates_path,
    }


def _build(tmp_path: Path, **kwargs: object) -> dict:
    return build_ml_learned_scorer_holdout_assignment_payload(
        **_paths(tmp_path, **kwargs),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def test_happy_path_materializes_assignments_and_zero_leakage(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["assignment_version"] == ASSIGNMENT_VERSION
    assert payload["metadata"]["eval_work_count"] == 3
    assert payload["metadata"]["eval_work_set_sha256"] == _eval_sha()
    assert len(payload["assignments"]) == 4
    assert payload["leakage_report"]["train_eval_work_overlap_count"] == 0
    assert payload["leakage_report"]["train_eval_row_id_overlap_count"] == 0
    assert payload["leakage_report"]["global_zero_assertion"] is True
    assert payload["dataset_inventory"]["assigned_train_observation_count"] == 1
    assert payload["dataset_inventory"]["assigned_eval_observation_count"] == 3
    assert payload["dataset_inventory"]["overlap_work_count_between_full_fit_training_universe_and_eval_set"] == 0
    assert payload["interpretation"]["next_authorized_step"] == "ml-offline-audit-embedding-scorer-export-v2"


def test_eval_work_set_uses_all_candidate_rows_not_only_labeled_subset(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["leakage_report"]["eval_unique_work_count"] == 3
    assert payload["leakage_report"]["assigned_eval_unique_work_count"] == 2
    assert payload["dataset_inventory"]["product_candidate_unlabeled_eval_work_count"] == 1


def test_rejects_eval_work_set_sha_mismatch_vs_policy(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutAssignmentError, match="eval_work_set_sha256"):
        _build(tmp_path, policy_eval_sha="bad-sha")


def test_rejects_missing_canonical_openalex_work_id_in_candidate_rows(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutAssignmentError, match="canonical_openalex_work_id"):
        _build(tmp_path, scoring=_scoring_payload(missing_canonical=True))


def test_rejects_holdout_policy_strategy_mismatch(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutAssignmentError, match="strategy_id"):
        _build(tmp_path, policy_strategy_id="wrong_strategy")


def test_rejects_metric_gates_when_learned_application_failed(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutAssignmentError, match="learned_scorer_application_gates_passed"):
        _build(tmp_path, gates=_gates_payload(learned_passed=False))


def test_rejects_duplicate_embedding_row_id(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutAssignmentError, match="duplicate row_id"):
        _build(tmp_path, duplicate_embedding_row_id=True)


def test_rejects_policy_input_sha_mismatch_when_policy_lists_input(tmp_path: Path) -> None:
    with pytest.raises(MLLearnedScorerHoldoutAssignmentError, match="sha256 mismatch"):
        _build(tmp_path, policy_sha_mismatch=True)


def test_train_eval_class_balance_fields_are_populated(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    obs = payload["class_balance"]["observation_level"]
    work = payload["class_balance"]["work_level_any_positive"]

    assert obs["train"] == {"count": 1, "positive": 0, "negative": 1, "positive_rate": 0.0}
    assert obs["eval"]["count"] == 3
    assert obs["eval"]["positive"] == 2
    assert obs["eval"]["negative"] == 1
    assert work["train"]["negative_works"] == 1
    assert work["eval"]["positive_works"] == 2
    assert payload["class_balance"]["train_negative_works_below_threshold"] is True


def test_duplicate_and_conflict_counts_present(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    report = payload["duplicate_and_conflict_report"]

    assert report["global"]["duplicate_work_group_count"] == 1
    assert report["global"]["duplicate_observation_pressure"] == 1
    assert report["global"]["conflicting_target_work_group_count"] == 1
    assert report["eval"]["conflicting_target_work_group_count"] == 1
    assert report["duplicate_conflict_rollups_from_label_dataset_metadata"]["derived_target_conflict_count"] == 1


def test_work_level_consistency_all_rows_for_same_work_share_assignment(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    by_work = {}
    for row in payload["assignments"]:
        by_work.setdefault(row["canonical_openalex_work_id"], set()).add(row["assignment"])
    assert all(len(values) == 1 for values in by_work.values())
    w1 = next(row for row in payload["work_assignments"] if row["canonical_openalex_work_id"] == "W1")
    assert w1["assignment"] == "eval"
    assert w1["row_ids"] == ["r1", "r2"]


def test_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "assignment.json"
    out_md = tmp_path / "assignment.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-learned-scorer-holdout-assignment",
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--embeddings",
        str(paths["embeddings_path"]),
        "--production-candidate-scoring",
        str(paths["production_candidate_scoring_path"]),
        "--holdout-policy",
        str(paths["holdout_policy_path"]),
        "--production-candidate-metric-gates",
        str(paths["production_candidate_metric_gates_path"]),
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
    assert data["metadata"]["artifact_type"] == "ml_learned_scorer_holdout_assignment"
    assert "Not Validation / Not Shadow / Not Production" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_learned_scorer_holdout_assignment.py").read_text(
        encoding="utf-8"
    ).lower()
    assert "psycopg" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "openai" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-learned-scorer-holdout-assignment"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

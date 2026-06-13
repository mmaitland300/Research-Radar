"""Tests for the offline grouped-CV ranker experiment."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_offline_ranker_experiment import (
    EXPERIMENT_VERSION,
    MLOfflineRankerExperimentError,
    build_ml_offline_ranker_experiment_payload,
    markdown_from_ml_offline_ranker_experiment,
)


def _label_row(row_id: str, work_id: str | None, target: bool, *, variant: str = "ml_transfer_gap_audit") -> dict:
    return {
        "dataset_version": "ml-label-dataset-v8",
        "row_id": row_id,
        "split": "audit_only",
        "review_pool_variant": variant,
        "family": None,
        "paper_id": work_id or "",
        "work_id": work_id or "",
        "openalex_work_id": work_id or "",
        "relevance_label": "good" if target else "miss",
        "novelty_label": "useful",
        "bridge_like_label": "no",
        "reviewer_notes": "notes",
        "good_or_acceptable": target,
        "surprising_or_useful": True,
    }


def _label_payload(*, include_missing_id: bool = True) -> dict:
    rows = [
        _label_row("r1", "https://openalex.org/W1", True, variant="ml_blind_snapshot_audit"),
        _label_row("r2", "w1", True, variant="ml_external_near_miss_audit"),
        _label_row("r3", "W2", False),
        _label_row("r4", "W3", True),
        _label_row("r5", "W4", False),
    ]
    if include_missing_id:
        rows.append(_label_row("r6", None, True))
    return {
        "dataset_version": "ml-label-dataset-v8",
        "rows": rows,
        "metadata": {
            "duplicate_paper_id_report": {"duplicate_paper_id_count": 1},
            "conflicting_label_report": {"conflicting_label_count": 0},
            "derived_target_conflict_report": {"derived_target_conflict_count": 0},
        },
    }


def _split_policy_payload(*, unsafe: str | None = None) -> dict:
    assertions = {
        "surprising_or_useful_allowed_for_v1_split": False,
        "requires_grouped_split_by_work": True,
        "permits_row_level_random_split": False,
        "permits_silent_conflict_resolution": False,
        "production_default_change_allowed": False,
    }
    if unsafe:
        assertions[unsafe] = not assertions[unsafe]
    return {
        "metadata": {"artifact_type": "ml_label_split_policy", "policy_version": "ml-label-split-policy-v1"},
        "allowed_targets_for_v1_split": ["good_or_acceptable"],
        "forbidden_targets": ["surprising_or_useful"],
        "target_policy": {
            "good_or_acceptable": {"status": "eligible_for_offline_ranker_research"},
            "surprising_or_useful": {"status": "excluded_from_v1_split"},
        },
        "eligibility_rules": {
            "review_pool_variant_handling": {
                "silent_pooling_allowed": False,
                "policy_citation_required_for_pooling": True,
            }
        },
        "randomness_policy": {"recommended_default_seed": 17},
        "policy_assertions": assertions,
    }


def _embedding_payload(label_sha: str, *, omit_row_id: str | None = None) -> dict:
    vectors = {
        "r1": [2.0, 2.0],
        "r2": [2.0, 1.8],
        "r3": [-2.0, -2.0],
        "r4": [1.8, 2.0],
        "r5": [-1.8, -2.0],
        "r6": [0.0, 0.0],
    }
    rows = []
    for row_id, vector in vectors.items():
        if row_id == omit_row_id:
            continue
        rows.append({"row_id": row_id, "embedding": vector, "embedding_status": "ok", "paper_id": row_id})
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v3",
            "embedding_dimensions": 2,
            "source_label_dataset_version": "ml-label-dataset-v8",
            "source_label_dataset_sha256": label_sha,
        },
        "rows": rows,
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _fixture_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    labels = _write_json(tmp_path, "labels.json", _label_payload())
    policy = _write_json(tmp_path, "policy.json", _split_policy_payload())
    embeddings = _write_json(tmp_path, "embeddings.json", _embedding_payload(sha256_file(labels)))
    return labels, policy, embeddings


def test_happy_path_schema_grouped_cv_and_leakage(tmp_path: Path) -> None:
    labels, policy, embeddings = _fixture_paths(tmp_path)
    payload = build_ml_offline_ranker_experiment_payload(
        label_dataset_path=labels,
        split_policy_path=policy,
        embeddings_path=embeddings,
        cv_folds=2,
        repo_root=tmp_path,
        generated_at="2026-05-15T00:00:00Z",
    )

    assert payload["metadata"]["experiment_version"] == EXPERIMENT_VERSION
    assert payload["metadata"]["target"] == "good_or_acceptable"
    assert payload["metadata"]["random_seed"] == 17
    assert payload["policy_compliance"]["grouped_split_used"] is True
    assert payload["policy_compliance"]["row_level_random_split_used"] is False
    assert payload["policy_compliance"]["production_artifact_written"] is False
    assert payload["leakage_report"]["global_zero_assertion"] is True
    assert all(fold["leakage_work_overlap_count"] == 0 for fold in payload["leakage_report"]["per_fold"])

    inventory = payload["dataset_inventory"]
    assert inventory["eligible_observations"] == 5
    assert inventory["unique_eligible_canonical_work_count"] == 4
    assert inventory["duplicate_observation_pressure"] == 1
    assert inventory["excluded_rows_by_reason"] == {"missing_canonical_work_id": 1}
    assert inventory["target_class_counts"]["observation_level"] == {"positive": 3, "negative": 2}
    assert payload["models"]["embedding_logistic"]["pipeline_steps"] == ["scaler", "classifier"]
    assert payload["models"]["embedding_logistic"]["aggregate"]["folds_evaluated"] == 2
    assert "coefficients_standardized_space" in payload["models"]["embedding_logistic"]["per_fold"][0]["model_details"]


def test_wrong_target_and_unsafe_policy_fail(tmp_path: Path) -> None:
    labels, policy, embeddings = _fixture_paths(tmp_path)
    with pytest.raises(MLOfflineRankerExperimentError, match="only good_or_acceptable"):
        build_ml_offline_ranker_experiment_payload(
            label_dataset_path=labels,
            split_policy_path=policy,
            embeddings_path=embeddings,
            target="surprising_or_useful",
        )

    unsafe_policy = _write_json(tmp_path, "unsafe-policy.json", _split_policy_payload(unsafe="permits_row_level_random_split"))
    with pytest.raises(MLOfflineRankerExperimentError, match="permits_row_level_random_split"):
        build_ml_offline_ranker_experiment_payload(
            label_dataset_path=labels,
            split_policy_path=unsafe_policy,
            embeddings_path=embeddings,
            cv_folds=2,
        )


def test_missing_embedding_for_eligible_row_fails(tmp_path: Path) -> None:
    labels = _write_json(tmp_path, "labels.json", _label_payload())
    policy = _write_json(tmp_path, "policy.json", _split_policy_payload())
    embeddings = _write_json(tmp_path, "embeddings.json", _embedding_payload(sha256_file(labels), omit_row_id="r5"))
    with pytest.raises(MLOfflineRankerExperimentError, match="missing embeddings"):
        build_ml_offline_ranker_experiment_payload(
            label_dataset_path=labels,
            split_policy_path=policy,
            embeddings_path=embeddings,
            cv_folds=2,
        )


def test_embedding_label_sha_mismatch_fails(tmp_path: Path) -> None:
    labels, policy, _ = _fixture_paths(tmp_path)
    bad_embeddings = _embedding_payload("not-the-label-sha")
    embeddings = _write_json(tmp_path, "bad-embeddings.json", bad_embeddings)
    with pytest.raises(MLOfflineRankerExperimentError, match="source_label_dataset_sha256"):
        build_ml_offline_ranker_experiment_payload(
            label_dataset_path=labels,
            split_policy_path=policy,
            embeddings_path=embeddings,
            cv_folds=2,
        )


def test_cli_writes_outputs(tmp_path: Path) -> None:
    labels, policy, embeddings = _fixture_paths(tmp_path)
    out_json = tmp_path / "experiment.json"
    out_md = tmp_path / "experiment.md"
    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-ranker-experiment",
        "--label-dataset",
        str(labels),
        "--split-policy",
        str(policy),
        "--embeddings",
        str(embeddings),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--cv-folds",
        "2",
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert data["metadata"]["artifact_type"] == "ml_offline_ranker_experiment"
    assert "Offline Ranker Experiment" in out_md.read_text(encoding="utf-8")


def test_markdown_not_production(tmp_path: Path) -> None:
    labels, policy, embeddings = _fixture_paths(tmp_path)
    payload = build_ml_offline_ranker_experiment_payload(
        label_dataset_path=labels,
        split_policy_path=policy,
        embeddings_path=embeddings,
        cv_folds=2,
        generated_at="2026-05-15T00:00:00Z",
    )
    md = markdown_from_ml_offline_ranker_experiment(payload)
    assert "Not Production" in md
    assert "not validation" in md.lower()
    assert "No model file" in md


def test_duplicate_embedding_row_id_fails(tmp_path: Path) -> None:
    labels, policy, embeddings = _fixture_paths(tmp_path)
    payload = json.loads(embeddings.read_text(encoding="utf-8"))
    payload["rows"].append(copy.deepcopy(payload["rows"][0]))
    dup_embeddings = _write_json(tmp_path, "dup-embeddings.json", payload)
    with pytest.raises(MLOfflineRankerExperimentError, match="duplicate row_id"):
        build_ml_offline_ranker_experiment_payload(
            label_dataset_path=labels,
            split_policy_path=policy,
            embeddings_path=dup_embeddings,
            cv_folds=2,
        )


def test_no_database_openai_openalex_client_imports_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_offline_ranker_experiment.py").read_text(encoding="utf-8").lower()
    assert "psycopg" not in module_source
    assert "openai_embedding" not in module_source
    assert "embedding_provider" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-offline-ranker-experiment"')
    end = cli_source.index("ml_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

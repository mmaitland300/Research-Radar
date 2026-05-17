"""Tests for offline audit embedding scorer export v1."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
import warnings
from pathlib import Path
from unittest.mock import patch

import pytest
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_offline_audit_embedding_scorer_export import (
    FIT_MODE_HOLDOUT,
    MLOfflineAuditEmbeddingScorerExportError,
    SCORER_VERSION,
    SCORER_VERSION_V2,
    build_ml_offline_audit_embedding_scorer_export_payload,
    score_audit_embedding_probability,
)


def _label_row(
    row_id: str,
    work_id: str | None,
    target: bool | None,
    *,
    split: str = "audit_only",
    variant: str = "ml_transfer_gap_audit",
) -> dict:
    return {
        "row_id": row_id,
        "split": split,
        "review_pool_variant": variant,
        "family": "emerging",
        "paper_id": f"https://openalex.org/{work_id}" if work_id else "",
        "work_id": work_id or "",
        "openalex_work_id": work_id or "",
        "relevance_label": "good" if target is True else "miss" if target is False else "good",
        "novelty_label": "useful",
        "bridge_like_label": "no",
        "good_or_acceptable": target,
        "surprising_or_useful": False,
    }


def _label_payload() -> dict:
    return {
        "dataset_version": "ml-label-dataset-v8",
        "metadata": {
            "duplicate_paper_id_report": {"duplicate_paper_id_count": 1},
            "conflicting_label_report": {"conflicting_label_count": 1},
            "derived_target_conflict_report": {"derived_target_conflict_count": 1},
        },
        "rows": [
            _label_row("r1", "W1", True, variant="ml_blind_snapshot_audit"),
            _label_row("r2", "W1", False, variant="ml_external_near_miss_audit"),
            _label_row("r3", "W2", False),
            _label_row("r4", "W3", True),
            _label_row("r5", "W4", False),
            _label_row("r6", None, True),
            _label_row("r7", "W5", True, split="holdout"),
            _label_row("r8", "W6", None),
        ],
    }


def _split_policy_payload() -> dict:
    return {
        "metadata": {"artifact_type": "ml_label_split_policy", "policy_version": "ml-label-split-policy-v1"},
        "allowed_targets_for_v1_split": ["good_or_acceptable"],
        "forbidden_targets": ["surprising_or_useful"],
        "randomness_policy": {"recommended_default_seed": 20260515},
        "policy_assertions": {
            "permits_row_level_random_split": False,
            "permits_silent_conflict_resolution": False,
            "production_default_change_allowed": False,
            "requires_grouped_split_by_work": True,
            "surprising_or_useful_allowed_for_v1_split": False,
        },
    }


def _embedding_payload(label_sha: str) -> dict:
    vectors = {
        "r1": [2.0, 2.0],
        "r2": [-1.0, -1.4],
        "r3": [-2.0, -2.0],
        "r4": [1.8, 1.4],
        "r5": [-1.6, -1.8],
        "r6": [0.0, 0.0],
        "r7": [2.2, 2.0],
        "r8": [1.0, 1.0],
    }
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v3",
            "source_label_dataset_sha256": label_sha,
            "source_label_dataset_version": "ml-label-dataset-v8",
            "embedding_model": "fixture-model",
            "embedding_provider": "fixture-provider",
            "embedding_dimensions": 2,
        },
        "rows": [
            {"row_id": row_id, "embedding_status": "ok", "embedding": vector}
            for row_id, vector in vectors.items()
        ],
    }


def _product_candidate_gates_payload(*, passed: bool = True, next_stage: str | None = None) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v1",
        },
        "product_candidate_heuristic_gates_passed": passed,
        "recommended_next_stage": next_stage or "create_frozen_audit_embedding_scorer_export_v1",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }


def _work_set_sha(work_ids: list[str]) -> str:
    return hashlib.sha256("".join(f"{work_id}\n" for work_id in sorted(work_ids)).encode("utf-8")).hexdigest()


def _holdout_assignment_payload(*, overlap: bool = False, one_class_train: bool = False) -> dict:
    train_assignment = "eval" if one_class_train else "train"
    leakage_overlap = 1 if overlap else 0
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_assignment",
            "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
            "strategy_id": "product_candidate_snapshot_holdout",
            "seed": 20260515,
            "target": "good_or_acceptable",
            "production_candidate_metric_gates_version": "ml-offline-production-candidate-metric-gates-v2",
            "eval_work_set_sha256": _work_set_sha(["W1"]),
            "eval_work_count": 1,
            "train_work_count": 3,
            "train_observation_count": 3,
        },
        "leakage_report": {
            "train_eval_work_overlap_count": leakage_overlap,
            "global_zero_assertion": not overlap,
        },
        "duplicate_and_conflict_report": {
            "train": {
                "duplicate_work_group_count": 0,
                "duplicate_observation_pressure": 0,
                "conflicting_target_work_group_count": 0,
            },
            "eval": {
                "duplicate_work_group_count": 1,
                "duplicate_observation_pressure": 1,
                "conflicting_target_work_group_count": 1,
            },
        },
        "assignments": [
            {
                "row_id": "r1",
                "canonical_openalex_work_id": "W1",
                "assignment": "eval",
                "good_or_acceptable": True,
                "embedding_status": "ok",
            },
            {
                "row_id": "r2",
                "canonical_openalex_work_id": "W1",
                "assignment": "eval",
                "good_or_acceptable": False,
                "embedding_status": "ok",
            },
            {
                "row_id": "r3",
                "canonical_openalex_work_id": "W2",
                "assignment": train_assignment,
                "good_or_acceptable": False,
                "embedding_status": "ok",
            },
            {
                "row_id": "r4",
                "canonical_openalex_work_id": "W3",
                "assignment": "train",
                "good_or_acceptable": True,
                "embedding_status": "ok",
            },
            {
                "row_id": "r5",
                "canonical_openalex_work_id": "W4",
                "assignment": train_assignment,
                "good_or_acceptable": False,
                "embedding_status": "ok",
            },
        ],
    }


def _holdout_policy_payload(*, eval_sha: str | None = None) -> dict:
    sha = eval_sha or _work_set_sha(["W1"])
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_policy",
            "policy_version": "ml-learned-scorer-holdout-policy-v1",
        },
        "dataset_inventory": {
            "product_candidate_eval_work_set_sha256": sha,
            "product_candidate_pool_work_count": 1,
        },
        "primary_holdout_strategy": {
            "strategy_id": "product_candidate_snapshot_holdout",
            "eval_work_set_definition": {
                "eval_work_set_sha256": sha,
            },
        },
    }


def _ranker_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_ranker_experiment",
            "experiment_version": "ml-offline-ranker-experiment-v1",
            "target": "good_or_acceptable",
        },
        "models": {
            "embedding_logistic": {
                "aggregate": {
                    "folds_evaluated": 2,
                    "folds_skipped": 0,
                    "skipped_reasons": {},
                    "observation_metrics_mean_std": {
                        "balanced_accuracy": {"mean": 0.75, "std": 0.05, "n": 2},
                        "roc_auc": {"mean": 0.88, "std": 0.02, "n": 2},
                        "average_precision": {"mean": 0.91, "std": 0.03, "n": 2},
                        "accuracy": {"mean": 0.80, "std": 0.01, "n": 2},
                        "macro_f1": {"mean": 0.78, "std": 0.04, "n": 2},
                    },
                    "summed_confusion": {"tn": 4, "fp": 1, "fn": 1, "tp": 4},
                },
                "per_fold": [{"model_details": {"coefficients_standardized_space": [99.0, 99.0]}}],
            }
        },
    }


def _v1_scorer_reference_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_audit_embedding_scorer",
            "scorer_version": SCORER_VERSION,
            "fit_mode": "full_fit_audit_corpus",
            "target": "good_or_acceptable",
        },
        "training_summary": {
            "in_sample_training_metrics": {
                "label": "IN-SAMPLE FULL-FIT ONLY - NOT VALIDATION",
                "roc_auc": 0.9,
                "average_precision": 0.92,
            }
        },
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _fixture_paths(tmp_path: Path) -> dict[str, Path]:
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    policy_path = _write_json(tmp_path, "policy.json", _split_policy_payload())
    embeddings_path = _write_json(tmp_path, "embeddings.json", _embedding_payload(sha256_file(label_path)))
    gates_path = _write_json(tmp_path, "candidate-gates.json", _product_candidate_gates_payload())
    ranker_path = _write_json(tmp_path, "ranker.json", _ranker_payload())
    return {
        "label_dataset_path": label_path,
        "split_policy_path": policy_path,
        "embeddings_path": embeddings_path,
        "production_candidate_metric_gates_path": gates_path,
        "ranker_experiment_path": ranker_path,
    }


def _holdout_fixture_paths(
    tmp_path: Path,
    *,
    assignment_payload: dict | None = None,
    policy_payload: dict | None = None,
) -> dict[str, Path]:
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    policy_path = _write_json(tmp_path, "policy.json", _split_policy_payload())
    embeddings_path = _write_json(tmp_path, "embeddings.json", _embedding_payload(sha256_file(label_path)))
    assignment_path = _write_json(tmp_path, "holdout-assignment.json", assignment_payload or _holdout_assignment_payload())
    holdout_policy_path = _write_json(tmp_path, "holdout-policy.json", policy_payload or _holdout_policy_payload())
    ranker_path = _write_json(tmp_path, "ranker.json", _ranker_payload())
    v1_path = _write_json(tmp_path, "scorer-v1.json", _v1_scorer_reference_payload())
    return {
        "label_dataset_path": label_path,
        "split_policy_path": policy_path,
        "embeddings_path": embeddings_path,
        "holdout_assignment_path": assignment_path,
        "holdout_policy_path": holdout_policy_path,
        "ranker_experiment_path": ranker_path,
        "audit_embedding_scorer_export_v1_path": v1_path,
    }


def _build(tmp_path: Path, *, paths: dict[str, Path] | None = None) -> dict:
    return build_ml_offline_audit_embedding_scorer_export_payload(
        **(paths or _fixture_paths(tmp_path)),
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def _build_holdout(tmp_path: Path, *, paths: dict[str, Path] | None = None) -> dict:
    return build_ml_offline_audit_embedding_scorer_export_payload(
        **(paths or _holdout_fixture_paths(tmp_path)),
        fit_mode=FIT_MODE_HOLDOUT,
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )


def test_happy_path_cli_writes_json_and_markdown_with_correct_schema(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    out_json = tmp_path / "scorer.json"
    out_md = tmp_path / "scorer.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-audit-embedding-scorer-export",
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--embeddings",
        str(paths["embeddings_path"]),
        "--production-candidate-metric-gates",
        str(paths["production_candidate_metric_gates_path"]),
        "--ranker-experiment",
        str(paths["ranker_experiment_path"]),
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
    assert data["metadata"]["artifact_type"] == "ml_offline_audit_embedding_scorer"
    assert data["metadata"]["scorer_version"] == SCORER_VERSION
    assert data["metadata"]["target"] == "good_or_acceptable"
    assert data["metadata"]["random_seed"] == 20260515
    assert data["scorer"]["scaler"]["feature_count"] == 2
    assert len(data["scorer"]["classifier"]["coefficients_standardized_space"]) == 2
    assert "Offline Audit Embedding Scorer Export" in out_md.read_text(encoding="utf-8")


def test_holdout_happy_path_writes_json_markdown_coefficients_and_leakage_asserts(tmp_path: Path) -> None:
    paths = _holdout_fixture_paths(tmp_path)
    out_json = tmp_path / "scorer-v2.json"
    out_md = tmp_path / "scorer-v2.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-audit-embedding-scorer-export",
        "--fit-mode",
        "holdout_bound_train_only",
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--embeddings",
        str(paths["embeddings_path"]),
        "--holdout-assignment",
        str(paths["holdout_assignment_path"]),
        "--holdout-policy",
        str(paths["holdout_policy_path"]),
        "--ranker-experiment",
        str(paths["ranker_experiment_path"]),
        "--audit-embedding-scorer-export-v1",
        str(paths["audit_embedding_scorer_export_v1_path"]),
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
    assert data["metadata"]["scorer_version"] == SCORER_VERSION_V2
    assert data["metadata"]["fit_mode"] == "holdout_bound_train_only"
    assert data["metadata"]["train_observation_count"] == 3
    assert data["metadata"]["train_work_count"] == 3
    assert data["metadata"]["eval_work_count"] == 1
    assert data["scorer"]["scaler"]["feature_count"] == 2
    assert len(data["scorer"]["classifier"]["coefficients_standardized_space"]) == 2
    assert data["policy_compliance"]["holdout_assignment_honored"] is True
    assert data["policy_compliance"]["eval_works_excluded_from_fit"] is True
    assert data["policy_compliance"]["train_eval_work_overlap_count"] == 0
    assert data["policy_compliance"]["shadow_scoring_authorized"] is False
    assert data["policy_compliance"]["production_artifact_written"] is False
    markdown = out_md.read_text(encoding="utf-8")
    assert "holdout assignment train arm" in markdown
    assert "not validation" in markdown.lower()
    assert "not shadow" in markdown.lower()


def test_holdout_rejects_overlap_sha_mismatch_and_one_class_train(tmp_path: Path) -> None:
    overlap_paths = _holdout_fixture_paths(tmp_path, assignment_payload=_holdout_assignment_payload(overlap=True))
    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="train_eval_work_overlap_count"):
        _build_holdout(tmp_path, paths=overlap_paths)

    mismatch_paths = _holdout_fixture_paths(
        tmp_path,
        policy_payload=_holdout_policy_payload(eval_sha="not-the-same-sha"),
    )
    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="eval_work_set_sha256"):
        _build_holdout(tmp_path, paths=mismatch_paths)

    one_class_paths = _holdout_fixture_paths(
        tmp_path,
        assignment_payload=_holdout_assignment_payload(one_class_train=True),
    )
    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="both positive and negative"):
        _build_holdout(tmp_path, paths=one_class_paths)


def test_holdout_exported_score_function_reproduces_sklearn_on_train_arm(tmp_path: Path) -> None:
    payload = build_ml_offline_audit_embedding_scorer_export_payload(
        **_holdout_fixture_paths(tmp_path),
        fit_mode=FIT_MODE_HOLDOUT,
        random_seed=17,
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )
    rows = [
        ("r3", False, [-2.0, -2.0]),
        ("r4", True, [1.8, 1.4]),
        ("r5", False, [-1.6, -1.8]),
    ]
    x = [row[2] for row in rows]
    y = [row[1] for row in rows]
    model = Pipeline(
        [
            ("scaler", StandardScaler(with_mean=True)),
            (
                "classifier",
                LogisticRegression(solver="lbfgs", penalty="l2", max_iter=5000, random_state=17),
            ),
        ]
    )
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="'penalty' was deprecated", category=FutureWarning)
        model.fit(x, y)
    true_index = list(model.named_steps["classifier"].classes_).index(True)
    expected = [float(row[true_index]) for row in model.predict_proba(x)]

    actual = [score_audit_embedding_probability(vector, payload) for _, _, vector in rows]
    assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12)


def test_v1_path_unchanged_without_fit_mode(tmp_path: Path) -> None:
    payload = _build(tmp_path)

    assert payload["metadata"]["scorer_version"] == SCORER_VERSION
    assert payload["metadata"]["fit_mode"] == "full_fit_audit_corpus"
    assert "reference_cv_baseline" in payload["training_summary"]
    assert "reference_baselines" not in payload["training_summary"]


def test_cli_conditional_required_flags_for_fit_modes(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    import pipeline.cli as cli_main

    full_missing_gates_argv = [
        "pipeline.cli",
        "ml-offline-audit-embedding-scorer-export",
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--embeddings",
        str(paths["embeddings_path"]),
        "--output",
        str(tmp_path / "out.json"),
        "--markdown-output",
        str(tmp_path / "out.md"),
    ]
    with patch.object(sys, "argv", full_missing_gates_argv), pytest.raises(SystemExit):
        cli_main.main()

    holdout_paths = _holdout_fixture_paths(tmp_path)
    holdout_missing_assignment_argv = [
        "pipeline.cli",
        "ml-offline-audit-embedding-scorer-export",
        "--fit-mode",
        "holdout_bound_train_only",
        "--label-dataset",
        str(holdout_paths["label_dataset_path"]),
        "--split-policy",
        str(holdout_paths["split_policy_path"]),
        "--embeddings",
        str(holdout_paths["embeddings_path"]),
        "--output",
        str(tmp_path / "holdout-out.json"),
        "--markdown-output",
        str(tmp_path / "holdout-out.md"),
    ]
    with patch.object(sys, "argv", holdout_missing_assignment_argv), pytest.raises(SystemExit):
        cli_main.main()


def test_rejects_wrong_label_dataset_version(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    labels = _label_payload()
    labels["dataset_version"] = "ml-label-dataset-v7"
    paths["label_dataset_path"] = _write_json(tmp_path, "bad-labels.json", labels)

    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="dataset_version"):
        _build(tmp_path, paths=paths)


def test_rejects_embeddings_sha_mismatch(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    paths["embeddings_path"] = _write_json(tmp_path, "bad-embeddings.json", _embedding_payload("not-the-sha"))

    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="source_label_dataset_sha256"):
        _build(tmp_path, paths=paths)


def test_rejects_product_candidate_gates_when_heuristic_gates_false(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    paths["production_candidate_metric_gates_path"] = _write_json(
        tmp_path,
        "bad-candidate-gates.json",
        _product_candidate_gates_payload(passed=False),
    )

    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="product_candidate_heuristic_gates_passed"):
        _build(tmp_path, paths=paths)


def test_rejects_product_candidate_gates_when_next_stage_is_wrong(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    paths["production_candidate_metric_gates_path"] = _write_json(
        tmp_path,
        "bad-candidate-gates-next.json",
        _product_candidate_gates_payload(next_stage="draft_ml_shadow_scorer_v1_spec"),
    )

    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="recommended_next_stage"):
        _build(tmp_path, paths=paths)


def test_rejects_target_surprising_or_useful(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="only good_or_acceptable"):
        build_ml_offline_audit_embedding_scorer_export_payload(
            **paths,
            target="surprising_or_useful",
            repo_root=tmp_path,
        )


def test_exported_score_function_reproduces_sklearn_predict_proba(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    payload = build_ml_offline_audit_embedding_scorer_export_payload(
        **paths,
        random_seed=17,
        repo_root=tmp_path,
        generated_at="2026-05-17T00:00:00Z",
    )
    rows = [
        ("r1", True, [2.0, 2.0]),
        ("r2", False, [-1.0, -1.4]),
        ("r3", False, [-2.0, -2.0]),
        ("r4", True, [1.8, 1.4]),
        ("r5", False, [-1.6, -1.8]),
    ]
    x = [row[2] for row in rows]
    y = [row[1] for row in rows]
    model = Pipeline(
        [
            ("scaler", StandardScaler(with_mean=True)),
            (
                "classifier",
                LogisticRegression(solver="lbfgs", penalty="l2", max_iter=5000, random_state=17),
            ),
        ]
    )
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="'penalty' was deprecated", category=FutureWarning)
        model.fit(x, y)
    true_index = list(model.named_steps["classifier"].classes_).index(True)
    expected = [float(row[true_index]) for row in model.predict_proba(x)]

    actual = [score_audit_embedding_probability(vector, payload) for _, _, vector in rows]
    assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12)
    with pytest.raises(MLOfflineAuditEmbeddingScorerExportError, match="length"):
        score_audit_embedding_probability([1.0], payload)


def test_training_inventory_preserves_observation_rows_and_duplicate_pressure(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    summary = payload["training_summary"]

    assert summary["eligible_observation_count"] == 5
    assert summary["unique_canonical_work_count"] == 4
    assert summary["duplicate_observation_pressure"] == 1
    assert summary["positive_observation_count"] == 2
    assert summary["negative_observation_count"] == 3
    assert summary["positive_work_count_any_positive"] == 2
    assert summary["negative_work_count_any_positive"] == 2
    assert summary["conflicting_target_work_group_count"] == 1
    assert summary["excluded_rows_by_reason"] == {
        "missing_canonical_work_id": 1,
        "split_not_audit_only": 1,
        "target_not_boolean": 1,
    }
    assert summary["in_sample_training_metrics"]["label"] == "IN-SAMPLE FULL-FIT ONLY — NOT VALIDATION"


def test_reference_cv_baseline_copied_when_ranker_experiment_supplied(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    reference = payload["training_summary"]["reference_cv_baseline"]

    assert reference["source_input"]["name"] == "ranker_experiment"
    assert reference["embedding_logistic_aggregate"]["folds_evaluated"] == 2
    assert reference["embedding_logistic_aggregate"]["observation_metrics_mean_std"]["roc_auc"]["mean"] == 0.88
    assert "per_fold" not in reference["embedding_logistic_aggregate"]
    assert "not reused or averaged" in reference["coefficient_reuse"]


def test_module_may_import_sklearn_but_not_db_or_external_clients_and_cli_has_no_database_url() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_offline_audit_embedding_scorer_export.py").read_text(
        encoding="utf-8"
    ).lower()
    assert "import sklearn" in module_source
    assert "psycopg" not in module_source
    assert "openai" not in module_source
    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-offline-audit-embedding-scorer-export"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" not in parser_block

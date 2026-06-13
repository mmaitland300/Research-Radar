"""Tests for production-candidate offline scoring v1."""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_offline_baseline_eval import sha256_file
from pipeline.ml_offline_audit_embedding_scorer_export import score_audit_embedding_probability
from pipeline.ml_offline_production_candidate_scoring import (
    MLOfflineProductionCandidateScoringError,
    assert_local_database_url,
    build_ml_offline_production_candidate_scoring_payload,
    markdown_from_ml_offline_production_candidate_scoring,
)


class _FakeCur:
    def __init__(self, parent: "_FakeConn") -> None:
        self._p = parent
        self._sql = ""
        self._params: tuple | None = None

    def execute(self, query: str, params: tuple | None = None) -> "_FakeCur":
        self._sql = query
        self._params = params
        self._p.executed_sql.append(query)
        return self

    def fetchone(self) -> dict | None:
        if "FROM ranking_runs" in self._sql:
            return self._p.run_row
        return None

    def fetchall(self) -> list[dict]:
        if "FROM paper_scores" not in self._sql:
            return []
        ranking_run_id, family = self._params or (None, None)
        rows = [
            row
            for row in self._p.candidate_rows
            if row.get("ranking_run_id") == ranking_run_id and row.get("recommendation_family") == family
        ]
        return sorted(rows, key=lambda row: (-float(row["final_score"]), int(row["internal_work_id"])))


class _FakeCurCtx:
    def __init__(self, parent: "_FakeConn") -> None:
        self._cur = _FakeCur(parent)

    def __enter__(self) -> _FakeCur:
        return self._cur

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(self, *, candidate_rows: list[dict] | None = None, run_row: dict | None = None) -> None:
        self.run_row = run_row or {
            "ranking_run_id": "rank-a",
            "ranking_version": "rv",
            "corpus_snapshot_version": "snap",
            "embedding_version": "emb",
            "config_json": {"clustering_artifact": {"cluster_version": "cluster-v1"}},
            "status": "succeeded",
        }
        self.candidate_rows = candidate_rows if candidate_rows is not None else _candidate_rows()
        self.executed_sql: list[str] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _label_rows() -> list[dict]:
    return [
        {
            "row_id": "r1a",
            "paper_id": "https://openalex.org/W1",
            "work_id": "W1",
            "split": "audit_only",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "review_pool_variant": "full_family_top_k",
            "good_or_acceptable": True,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
        {
            "row_id": "r1b",
            "paper_id": "https://openalex.org/W1",
            "work_id": "W1",
            "split": "audit_only",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "review_pool_variant": "ml_contrastive_offline_audit",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
        {
            "row_id": "r2",
            "paper_id": "",
            "work_id": "w2",
            "split": "audit_only",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "review_pool_variant": "full_family_top_k",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
        {
            "row_id": "r3",
            "paper_id": "https://openalex.org/W3",
            "work_id": "W3",
            "split": "audit_only",
            "ranking_run_id": "rank-b",
            "family": "bridge",
            "review_pool_variant": "ml_blind_snapshot_audit",
            "good_or_acceptable": True,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
        {
            "row_id": "r4",
            "paper_id": "https://openalex.org/W4",
            "work_id": "W4",
            "split": "audit_only",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "review_pool_variant": "ml_emerging_target_gap_audit:good_or_acceptable",
            "good_or_acceptable": False,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
        {
            "row_id": "r5",
            "paper_id": "https://openalex.org/W5",
            "work_id": "W5",
            "split": "audit_only",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "review_pool_variant": "ml_hard_negative_audit",
            "good_or_acceptable": True,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
        {
            "row_id": "r9",
            "paper_id": "https://openalex.org/W9",
            "work_id": "W9",
            "split": "audit_only",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "review_pool_variant": "full_family_top_k",
            "good_or_acceptable": True,
            "surprising_or_useful": False,
            "source_worksheet_path": "labels.csv",
        },
    ]


def _candidate_rows() -> list[dict]:
    base = []
    for internal_id, work, score in [
        (11, "W1", 0.95),
        (1, "W1", 0.40),
        (2, "W2", 0.85),
        (3, "W3", 0.75),
        (4, "W4", 0.65),
        (5, "W5", 0.55),
        (6, "W6", 0.45),
    ]:
        base.append(
            {
                "ranking_run_id": "rank-a",
                "internal_work_id": internal_id,
                "recommendation_family": "emerging",
                "semantic_score": 0.1,
                "citation_velocity_score": 0.2,
                "topic_growth_score": 0.3,
                "bridge_score": None,
                "diversity_penalty": 0.0,
                "final_score": score,
                "bridge_eligible": None,
                "reason_short": "reason",
                "openalex_id": f"https://openalex.org/{work}",
                "title": f"Title {work}",
                "year": 2026,
                "citation_count": 10,
            }
        )
    base.append({**base[0], "ranking_run_id": "rank-b", "internal_work_id": 99, "openalex_id": "https://openalex.org/W99"})
    base.append({**base[0], "recommendation_family": "bridge", "internal_work_id": 88, "openalex_id": "https://openalex.org/W88"})
    return base


def _label_payload() -> dict:
    return {"dataset_version": "ml-label-dataset-v8", "rows": _label_rows()}


def _policy_payload() -> dict:
    return {
        "allowed_targets_for_v1_split": ["good_or_acceptable"],
        "forbidden_targets": ["surprising_or_useful"],
        "metadata": {"artifact_type": "ml_label_split_policy", "policy_version": "ml-label-split-policy-v1"},
    }


def _ranker_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_ranker_experiment",
            "experiment_version": "ml-offline-ranker-experiment-v1",
            "target": "good_or_acceptable",
        },
        "models": {"embedding_logistic": {"per_fold": [{"coefficients_standardized_space": [0.1]}]}},
    }


def _embeddings_payload(*, label_sha: str, row_ids: list[str]) -> dict:
    vectors = {
        "r1a": [0.1, 0.2],
        "r1b": [0.8, 0.9],
        "r2": [-2.0, -2.0],
        "r3": [1.5, 1.5],
        "r4": [-1.0, -1.0],
        "r5": [2.0, 2.0],
        "r9": [1.7, 1.7],
    }
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v3",
            "source_label_dataset_sha256": label_sha,
            "source_label_dataset_version": "ml-label-dataset-v8",
            "embedding_dimensions": 2,
        },
        "rows": [
            {"row_id": row_id, "embedding_status": "ok", "embedding": vectors.get(row_id, [0.0, 0.1])}
            for row_id in row_ids
        ],
    }


def _gates_payload(*, ranker_path: Path, ranker_sha: str) -> dict:
    return {
        "audit_ranker_gates_passed": True,
        "recommended_next_stage": "proceed_to_production_candidate_offline_scoring",
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "metadata": {
            "artifact_type": "ml_offline_metric_gates",
            "gates_version": "ml-offline-metric-gates-v1",
            "inputs": [{"name": "ranker_experiment", "path": ranker_path.name, "sha256": ranker_sha}],
        },
    }


def _work_set_sha(work_ids: list[str]) -> str:
    return hashlib.sha256("".join(f"{work_id}\n" for work_id in sorted(set(work_ids))).encode("utf-8")).hexdigest()


def _audit_scorer_payload(*, label_sha: str, embeddings_sha: str, dimensions: int = 2, target: str = "good_or_acceptable") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_audit_embedding_scorer",
            "scorer_version": "ml-offline-audit-embedding-scorer-v1",
            "target": target,
            "fit_mode": "full_fit_audit_corpus",
            "label_dataset_sha256": label_sha,
            "embedding_artifact_sha256": embeddings_sha,
            "embedding_dimensions": dimensions,
        },
        "policy_compliance": {
            "shadow_scoring_authorized": False,
            "product_candidate_pool_used_for_training": False,
            "production_artifact_written": False,
        },
        "scorer": {
            "pipeline_steps": ["scaler", "classifier"],
            "scaler": {
                "with_mean": True,
                "feature_count": dimensions,
                "mean": [0.0] * dimensions,
                "scale": [1.0] * dimensions,
            },
            "classifier": {
                "solver": "lbfgs",
                "penalty": "l2",
                "max_iter": 5000,
                "classes": [False, True],
                "coefficients_standardized_space": [1.0] + [0.0] * (dimensions - 1),
                "intercept_standardized_space": 0.0,
            },
        },
    }


def _holdout_assignment_payload(*, eval_sha: str, bad_eval_sha: bool = False) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_assignment",
            "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
            "strategy_id": "product_candidate_snapshot_holdout",
            "ranking_run_id": "rank-a",
            "family": "emerging",
            "target": "good_or_acceptable",
            "eval_work_count": 6,
            "eval_work_set_sha256": "bad-sha" if bad_eval_sha else eval_sha,
        },
        "leakage_report": {
            "global_zero_assertion": True,
            "train_eval_work_overlap_count": 0,
        },
        "assignments": [
            {"row_id": "r1a", "canonical_openalex_work_id": "W1", "assignment": "eval"},
            {"row_id": "r1b", "canonical_openalex_work_id": "W1", "assignment": "eval"},
            {"row_id": "r2", "canonical_openalex_work_id": "W2", "assignment": "eval"},
            {"row_id": "r3", "canonical_openalex_work_id": "W3", "assignment": "eval"},
            {"row_id": "r4", "canonical_openalex_work_id": "W4", "assignment": "train"},
            {"row_id": "r5", "canonical_openalex_work_id": "W5", "assignment": "eval"},
            {"row_id": "r9", "canonical_openalex_work_id": "W9", "assignment": "train"},
        ],
    }


def _holdout_policy_payload(*, eval_sha: str) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_learned_scorer_holdout_policy",
            "policy_version": "ml-learned-scorer-holdout-policy-v1",
        },
        "dataset_inventory": {"product_candidate_eval_work_set_sha256": eval_sha},
        "primary_holdout_strategy": {"eval_work_set_definition": {"eval_work_set_sha256": eval_sha}},
    }


def _candidate_gates_v2_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_production_candidate_metric_gates",
            "gates_version": "ml-offline-production-candidate-metric-gates-v2",
        }
    }


def _holdout_scorer_payload(
    *,
    assignment_sha: str,
    eval_sha: str,
    dimensions: int = 2,
    scorer_version: str = "ml-offline-audit-embedding-scorer-v2",
    fit_mode: str = "holdout_bound_train_only",
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_offline_audit_embedding_scorer",
            "scorer_version": scorer_version,
            "target": "good_or_acceptable",
            "fit_mode": fit_mode,
            "embedding_dimensions": dimensions,
            "eval_work_set_sha256": eval_sha,
            "holdout_assignment_sha256": assignment_sha,
        },
        "policy_compliance": {
            "eval_works_excluded_from_fit": True,
            "product_candidate_pool_used_for_training": False,
            "production_artifact_written": False,
            "shadow_scoring_authorized": False,
        },
        "scorer": {
            "pipeline_steps": ["scaler", "classifier"],
            "scaler": {
                "with_mean": True,
                "feature_count": dimensions,
                "mean": [0.0] * dimensions,
                "scale": [1.0] * dimensions,
            },
            "classifier": {
                "solver": "lbfgs",
                "penalty": "l2",
                "max_iter": 5000,
                "classes": [False, True],
                "coefficients_standardized_space": [1.0] + [0.0] * (dimensions - 1),
                "intercept_standardized_space": 0.0,
            },
        },
    }


def _fixture_paths(tmp_path: Path, *, include_r5_embedding: bool = False, with_audit_scorer: bool = False) -> dict[str, Path]:
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    label_sha = sha256_file(label_path)
    policy_path = _write_json(tmp_path, "policy.json", _policy_payload())
    ranker_path = _write_json(tmp_path, "ranker.json", _ranker_payload())
    ranker_sha = sha256_file(ranker_path)
    gates_path = _write_json(tmp_path, "gates.json", _gates_payload(ranker_path=ranker_path, ranker_sha=ranker_sha))
    row_ids = ["r1a", "r1b", "r2", "r3", "r4"]
    if include_r5_embedding:
        row_ids.append("r5")
    embeddings_path = _write_json(
        tmp_path,
        "embeddings.json",
        _embeddings_payload(label_sha=label_sha, row_ids=row_ids),
    )
    paths = {
        "label_dataset_path": label_path,
        "split_policy_path": policy_path,
        "metric_gates_path": gates_path,
        "audit_ranker_experiment_path": ranker_path,
        "embeddings_path": embeddings_path,
    }
    if with_audit_scorer:
        paths["audit_embedding_scorer_export_path"] = _write_json(
            tmp_path,
            "audit-scorer.json",
            _audit_scorer_payload(label_sha=label_sha, embeddings_sha=sha256_file(embeddings_path)),
        )
    return paths


def _holdout_fixture_paths(
    tmp_path: Path,
    *,
    scorer_version: str = "ml-offline-audit-embedding-scorer-v2",
    scorer_fit_mode: str = "holdout_bound_train_only",
    bad_assignment_eval_sha: bool = False,
) -> dict[str, Path]:
    paths = _fixture_paths(tmp_path, include_r5_embedding=True)
    eval_sha = _work_set_sha(["W1", "W2", "W3", "W4", "W5", "W6"])
    assignment_path = _write_json(
        tmp_path,
        "holdout-assignment.json",
        _holdout_assignment_payload(eval_sha=eval_sha, bad_eval_sha=bad_assignment_eval_sha),
    )
    scorer_path = _write_json(
        tmp_path,
        "holdout-scorer.json",
        _holdout_scorer_payload(
            assignment_sha=sha256_file(assignment_path),
            eval_sha=eval_sha,
            scorer_version=scorer_version,
            fit_mode=scorer_fit_mode,
        ),
    )
    paths["audit_embedding_scorer_export_path"] = scorer_path
    paths["holdout_assignment_path"] = assignment_path
    paths["holdout_policy_path"] = _write_json(tmp_path, "holdout-policy.json", _holdout_policy_payload(eval_sha=eval_sha))
    paths["production_candidate_metric_gates_v2_path"] = _write_json(
        tmp_path,
        "candidate-gates-v2.json",
        _candidate_gates_v2_payload(),
    )
    return paths


def _build(
    tmp_path: Path,
    *,
    conn: _FakeConn | None = None,
    paths: dict[str, Path] | None = None,
    **kwargs: object,
) -> tuple[dict, _FakeConn]:
    fc = conn or _FakeConn()
    artifact_paths = paths or _fixture_paths(tmp_path)
    payload = build_ml_offline_production_candidate_scoring_payload(
        fc,
        **artifact_paths,
        ranking_run_id="rank-a",
        family="emerging",
        target="good_or_acceptable",
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        repo_root=tmp_path,
        generated_at="2026-05-16T00:00:00Z",
        **kwargs,
    )
    return payload, fc


def test_input_version_validation(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    bad = copy.deepcopy(_label_payload())
    bad["dataset_version"] = "ml-label-dataset-v7"
    paths["label_dataset_path"] = _write_json(tmp_path, "bad-labels.json", bad)

    with pytest.raises(MLOfflineProductionCandidateScoringError, match="dataset_version"):
        _build(tmp_path, paths=paths)

    paths = _fixture_paths(tmp_path)
    policy = _policy_payload()
    policy["metadata"]["policy_version"] = "older"
    paths["split_policy_path"] = _write_json(tmp_path, "bad-policy.json", policy)
    with pytest.raises(MLOfflineProductionCandidateScoringError, match="policy_version"):
        _build(tmp_path, paths=paths)


def test_metric_gates_must_pass_and_keep_shadow_prod_false(tmp_path: Path) -> None:
    for field, value, expected in [
        ("audit_ranker_gates_passed", False, "audit_ranker_gates_passed"),
        ("shadow_scoring_allowed", True, "shadow_scoring_allowed"),
        ("production_default_allowed", True, "production_default_allowed"),
    ]:
        paths = _fixture_paths(tmp_path)
        gates = json.loads(paths["metric_gates_path"].read_text(encoding="utf-8"))
        gates[field] = value
        paths["metric_gates_path"] = _write_json(tmp_path, f"bad-gates-{field}.json", gates)
        with pytest.raises(MLOfflineProductionCandidateScoringError, match=expected):
            _build(tmp_path, paths=paths)


def test_metric_gates_ranker_input_sha_must_match(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    gates = json.loads(paths["metric_gates_path"].read_text(encoding="utf-8"))
    gates["metadata"]["inputs"][0]["sha256"] = "not-the-ranker-sha"
    gates["metadata"]["inputs"][0]["path"] = "not-the-ranker.json"
    paths["metric_gates_path"] = _write_json(tmp_path, "bad-gates-sha.json", gates)

    with pytest.raises(MLOfflineProductionCandidateScoringError, match="ranker experiment input SHA/path"):
        _build(tmp_path, paths=paths)


def test_hard_fail_target_surprising_or_useful(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    with pytest.raises(MLOfflineProductionCandidateScoringError, match="supports only good_or_acceptable"):
        build_ml_offline_production_candidate_scoring_payload(
            _FakeConn(),
            **paths,
            ranking_run_id="rank-a",
            family="emerging",
            target="surprising_or_useful",
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
        )


def test_candidate_pool_join_metrics_conflicts_embeddings_and_sql_readonly(tmp_path: Path) -> None:
    payload, fc = _build(tmp_path)

    assert payload["candidate_pool_summary"]["paper_scores_row_count"] == 7
    assert payload["candidate_pool_summary"]["candidate_unique_canonical_work_count"] == 6
    assert payload["label_join_summary"]["joined_labeled_observation_count"] == 6
    assert payload["label_join_summary"]["labeled_eval_subset_work_count"] == 5
    assert payload["label_join_summary"]["candidate_work_labeled_coverage_rate"] == pytest.approx(5 / 6)
    assert payload["label_join_summary"]["candidate_work_unlabeled_count"] == 1
    assert payload["embedding_join_summary"]["missing_embedding_count"] == 1
    assert payload["embedding_join_summary"]["missing_embedding_row_ids"] == ["r5"]

    observations_for_w1 = [
        row for row in payload["labeled_candidate_observations"] if row["canonical_openalex_work_id"] == "W1"
    ]
    assert len(observations_for_w1) == 2

    w1_eval = next(row for row in payload["labeled_eval_subset"] if row["canonical_openalex_work_id"] == "W1")
    assert w1_eval["final_score"] == pytest.approx(0.95)
    assert w1_eval["label_any_positive"] is True
    assert w1_eval["positive_observation_count"] == 1
    assert w1_eval["negative_observation_count"] == 1

    w2_eval = next(row for row in payload["labeled_eval_subset"] if row["canonical_openalex_work_id"] == "W2")
    assert w2_eval["label_any_positive"] is False

    conflicts = payload["duplicate_conflict_diagnostics"]
    assert conflicts["observation_level_labels_preserved"] is True
    assert conflicts["conflicting_target_work_count"] == 1
    assert conflicts["majority_vote_label_counts"] == {"positive": 2, "negative": 2, "tie": 1}

    metrics = payload["heuristic_metrics"]
    assert metrics["roc_auc_mann_whitney"] == pytest.approx(0.5)
    assert metrics["average_precision"] == pytest.approx((1.0 + 2 / 3 + 3 / 5) / 3)
    assert metrics["precision_recall_at_k"]["5"]["precision"] == pytest.approx(0.6)
    assert metrics["precision_recall_at_k"]["5"]["recall"] == pytest.approx(1.0)
    assert metrics["precision_recall_at_k"]["10"]["precision"] is None
    assert "requires at least 10" in metrics["precision_recall_at_k"]["10"]["reason"]

    top5 = payload["top_k_tables"]["5"]
    assert top5["candidate_work_count"] == 5
    assert top5["labeled_work_count"] == 5
    assert top5["labeled_positive_work_count"] == 3
    assert top5["labeled_negative_work_count"] == 2

    executed = "\n".join(fc.executed_sql).upper()
    for bad in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"):
        assert bad not in executed


def test_scoring_mode_defaults_to_heuristic_and_no_learned_scores(tmp_path: Path) -> None:
    payload, _fc = _build(tmp_path)

    assert payload["metadata"]["scoring_mode"] == "heuristic_and_coverage_only"
    assert payload["metadata"]["experiment_version"] == "ml-offline-production-candidate-scoring-v1"
    assert payload["scoring_mode_details"]["learned_product_scores_produced"] is False
    assert payload["learned_or_embedding_metrics"]["metrics"] is None
    assert payload["learned_or_embedding_metrics"]["learned_product_scores_produced"] is False
    assert "per-fold coefficients only" in payload["learned_or_embedding_metrics"]["reason"]


def test_learned_mode_scores_labeled_eval_subset_and_compares_to_heuristic(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, include_r5_embedding=True, with_audit_scorer=True)
    payload, fc = _build(tmp_path, paths=paths, scoring_mode="heuristic_and_audit_embedding_scorer")

    assert payload["metadata"]["experiment_version"] == "ml-offline-production-candidate-scoring-v2"
    assert payload["metadata"]["scoring_mode"] == "heuristic_and_audit_embedding_scorer"
    assert any(item["name"] == "audit_embedding_scorer_export" for item in payload["metadata"]["inputs"])

    details = payload["scoring_mode_details"]
    assert details["learned_product_scores_produced"] is True
    assert details["audit_embedding_scorer_export_present"] is True
    assert details["product_candidate_rows_used_for_training"] == 0
    assert details["learned_score_aggregation_policy"] == "max_probability"
    assert details["learned_metric_thresholds"]["minimum_learned_roc_auc"] == 0.70

    learned = payload["learned_or_embedding_metrics"]
    assert learned["learned_product_scores_produced"] is True
    assert learned["metrics"]["score_name"] == "audit_embedding_probability_work"
    assert learned["metrics"]["labeled_eval_subset_work_count"] == payload["label_join_summary"]["labeled_eval_subset_work_count"]
    assert learned["metrics"]["scored_labeled_work_count"] == payload["label_join_summary"]["labeled_eval_subset_work_count"]
    assert "delta_roc_auc" in learned["comparison_to_heuristic"]
    assert "delta_average_precision" in learned["comparison_to_heuristic"]
    assert "delta_precision_at_10" in learned["comparison_to_heuristic"]

    scorer = json.loads(paths["audit_embedding_scorer_export_path"].read_text(encoding="utf-8"))
    w1 = next(row for row in payload["labeled_eval_subset"] if row["canonical_openalex_work_id"] == "W1")
    expected_r1a = score_audit_embedding_probability([0.1, 0.2], scorer)
    expected_r1b = score_audit_embedding_probability([0.8, 0.9], scorer)
    assert w1["audit_embedding_probability_work"] == pytest.approx(max(expected_r1a, expected_r1b))
    assert w1["observation_level_score_count"] == 2
    observations_for_w1 = [
        row for row in payload["labeled_candidate_observations"] if row["canonical_openalex_work_id"] == "W1"
    ]
    assert {row["row_id"]: row["audit_embedding_probability_observation"] for row in observations_for_w1} == pytest.approx(
        {"r1a": expected_r1a, "r1b": expected_r1b}
    )

    executed = "\n".join(fc.executed_sql).upper()
    for bad in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"):
        assert bad not in executed


def test_holdout_mode_scores_eval_assignment_only_and_compares_to_heuristic(tmp_path: Path) -> None:
    paths = _holdout_fixture_paths(tmp_path)
    payload, fc = _build(tmp_path, paths=paths, scoring_mode="heuristic_and_holdout_embedding_scorer")

    assert payload["metadata"]["experiment_version"] == "ml-offline-production-candidate-scoring-v3"
    assert payload["metadata"]["scoring_mode"] == "heuristic_and_holdout_embedding_scorer"
    assert payload["metadata"]["eval_work_set_sha256"] == _work_set_sha(["W1", "W2", "W3", "W4", "W5", "W6"])
    assert any(item["name"] == "holdout_assignment" for item in payload["metadata"]["inputs"])

    holdout = payload["holdout_assignment_summary"]
    assert holdout["pool_matches_eval_set"] is True
    assert holdout["eval_assignment_row_count"] == 5
    assert holdout["train_assignment_rows_in_join_count"] == 1
    assert holdout["unlabeled_candidate_work_count"] == 1

    leakage = payload["leakage_report"]
    assert leakage["train_rows_used_in_metrics"] == 0
    assert leakage["train_works_used_in_metrics"] == 0
    assert leakage["eval_work_set_matches_assignment"] is True
    assert leakage["candidate_pool_work_set_matches_eval_set"] is True

    eval_work_ids = {row["canonical_openalex_work_id"] for row in payload["labeled_eval_subset"]}
    assert eval_work_ids == {"W1", "W2", "W3", "W5"}
    assert "W4" not in eval_work_ids
    assert payload["label_join_summary"]["labeled_eval_subset_work_count"] == 4

    learned = payload["learned_or_embedding_metrics"]
    assert learned["learned_product_scores_produced"] is True
    assert learned["eval_only"] is True
    assert learned["product_candidate_rows_used_for_training"] == 0
    assert learned["scorer_version"] == "ml-offline-audit-embedding-scorer-v2"
    assert learned["scorer_fit_mode"] == "holdout_bound_train_only"
    assert learned["metrics"]["labeled_eval_subset_work_count"] == payload["heuristic_metrics"]["labeled_eval_subset_work_count"]
    assert learned["metrics"]["score_name"] == "audit_embedding_probability_work"
    comparison = learned["comparison_to_heuristic"]
    assert "delta_precision_at_5" in comparison
    assert "delta_precision_at_10" in comparison
    assert "delta_precision_at_20" in comparison

    scorer = json.loads(paths["audit_embedding_scorer_export_path"].read_text(encoding="utf-8"))
    w1 = next(row for row in payload["labeled_eval_subset"] if row["canonical_openalex_work_id"] == "W1")
    expected = max(
        score_audit_embedding_probability([0.1, 0.2], scorer),
        score_audit_embedding_probability([0.8, 0.9], scorer),
    )
    assert w1["audit_embedding_probability_work"] == pytest.approx(expected)

    md = markdown_from_ml_offline_production_candidate_scoring(payload)
    assert "Holdout Learned Scorer Metrics" in md
    assert "Leakage Checks" in md
    assert "Product-candidate metric gates v3" in md
    assert "This is not shadow scoring" in md
    assert "This is not production scoring" in md

    executed = "\n".join(fc.executed_sql).upper()
    for bad in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE"):
        assert bad not in executed


@pytest.mark.parametrize(
    ("paths_kwargs", "message"),
    [
        ({"scorer_version": "ml-offline-audit-embedding-scorer-v1"}, "scorer_version"),
        ({"scorer_fit_mode": "full_fit_audit_corpus"}, "fit_mode"),
        ({"bad_assignment_eval_sha": True}, "eval_work_set_sha256"),
    ],
)
def test_holdout_mode_rejects_bad_scorer_or_assignment_provenance(
    tmp_path: Path,
    paths_kwargs: dict,
    message: str,
) -> None:
    paths = _holdout_fixture_paths(tmp_path, **paths_kwargs)
    with pytest.raises(MLOfflineProductionCandidateScoringError, match=message):
        _build(tmp_path, paths=paths, scoring_mode="heuristic_and_holdout_embedding_scorer")


def test_holdout_mode_rejects_candidate_pool_sha_mismatch(tmp_path: Path) -> None:
    paths = _holdout_fixture_paths(tmp_path)
    conn = _FakeConn(candidate_rows=[row for row in _candidate_rows() if row.get("openalex_id") != "https://openalex.org/W6"])

    with pytest.raises(MLOfflineProductionCandidateScoringError, match="candidate pool work-set SHA"):
        _build(tmp_path, conn=conn, paths=paths, scoring_mode="heuristic_and_holdout_embedding_scorer")


def test_learned_mode_requires_audit_embedding_scorer_export(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, include_r5_embedding=True)
    with pytest.raises(MLOfflineProductionCandidateScoringError, match="audit-embedding-scorer-export"):
        _build(tmp_path, paths=paths, scoring_mode="heuristic_and_audit_embedding_scorer")

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-offline-production-candidate-scoring",
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--split-policy",
        str(paths["split_policy_path"]),
        "--metric-gates",
        str(paths["metric_gates_path"]),
        "--audit-ranker-experiment",
        str(paths["audit_ranker_experiment_path"]),
        "--embeddings",
        str(paths["embeddings_path"]),
        "--ranking-run-id",
        "rank-a",
        "--scoring-mode",
        "heuristic_and_audit_embedding_scorer",
        "--output",
        str(tmp_path / "out.json"),
        "--markdown-output",
        str(tmp_path / "out.md"),
        "--database-url",
        "postgresql://research_radar:research_radar@localhost:5432/research_radar",
    ]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda scorer: scorer["metadata"].__setitem__("scorer_version", "older"), "scorer_version"),
        (lambda scorer: scorer["metadata"].__setitem__("target", "surprising_or_useful"), "metadata.target"),
        (lambda scorer: scorer["metadata"].__setitem__("embedding_dimensions", 3), "embedding dimensions"),
    ],
)
def test_learned_mode_rejects_bad_scorer_version_target_or_dimension(
    tmp_path: Path,
    mutator: object,
    message: str,
) -> None:
    paths = _fixture_paths(tmp_path, include_r5_embedding=True, with_audit_scorer=True)
    scorer = json.loads(paths["audit_embedding_scorer_export_path"].read_text(encoding="utf-8"))
    mutator(scorer)
    paths["audit_embedding_scorer_export_path"] = _write_json(tmp_path, f"bad-scorer-{message}.json", scorer)
    with pytest.raises(MLOfflineProductionCandidateScoringError, match=message):
        _build(tmp_path, paths=paths, scoring_mode="heuristic_and_audit_embedding_scorer")


def test_learned_mode_rejects_scorer_provenance_mismatch(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, include_r5_embedding=True, with_audit_scorer=True)
    scorer = json.loads(paths["audit_embedding_scorer_export_path"].read_text(encoding="utf-8"))
    scorer["metadata"]["label_dataset_sha256"] = "not-the-label-sha"
    paths["audit_embedding_scorer_export_path"] = _write_json(tmp_path, "bad-scorer-provenance.json", scorer)

    with pytest.raises(MLOfflineProductionCandidateScoringError, match="label_dataset_sha256"):
        _build(tmp_path, paths=paths, scoring_mode="heuristic_and_audit_embedding_scorer")

    paths = _fixture_paths(tmp_path, include_r5_embedding=True, with_audit_scorer=True)
    scorer = json.loads(paths["audit_embedding_scorer_export_path"].read_text(encoding="utf-8"))
    scorer["metadata"]["embedding_artifact_sha256"] = "not-the-embedding-sha"
    paths["audit_embedding_scorer_export_path"] = _write_json(tmp_path, "bad-scorer-embedding-provenance.json", scorer)

    with pytest.raises(MLOfflineProductionCandidateScoringError, match="embedding_artifact_sha256"):
        _build(tmp_path, paths=paths, scoring_mode="heuristic_and_audit_embedding_scorer")


def test_markdown_includes_not_shadow_not_production_caveats(tmp_path: Path) -> None:
    payload, _fc = _build(tmp_path)
    md = markdown_from_ml_offline_production_candidate_scoring(payload)

    assert "Not Shadow / Not Production" in md
    assert "This is not shadow scoring" in md
    assert "This is not production scoring" in md
    assert "No production model artifact" in md
    assert "Product-candidate metric gates v1" in md

    learned_payload, _fc = _build(
        tmp_path,
        paths=_fixture_paths(tmp_path, include_r5_embedding=True, with_audit_scorer=True),
        scoring_mode="heuristic_and_audit_embedding_scorer",
    )
    learned_md = markdown_from_ml_offline_production_candidate_scoring(learned_payload)
    assert "Learned Audit Scorer Metrics" in learned_md
    assert "Heuristic vs Learned Comparison" in learned_md
    assert "This is not shadow scoring" in learned_md
    assert "This is not production scoring" in learned_md
    assert "Production defaults remain blocked" in learned_md
    assert "Product-candidate metric gates v2" in learned_md


def test_database_url_must_be_local() -> None:
    assert assert_local_database_url("postgresql://research_radar:research_radar@127.0.0.1:5432/research_radar")[
        "local_database_url_confirmed"
    ]
    with pytest.raises(MLOfflineProductionCandidateScoringError, match="local Docker Postgres"):
        assert_local_database_url("postgresql://user:pass@containers-us-west-1.railway.app:5432/railway")


def test_cli_parser_includes_database_url_and_command_is_read_only(tmp_path: Path) -> None:
    package_root = Path(__file__).resolve().parents[1]
    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-offline-production-candidate-scoring"')
    end = cli_source.index("ml_transfer_gap_review_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" in parser_block

    module_source = (package_root / "pipeline" / "ml_offline_production_candidate_scoring.py").read_text(encoding="utf-8")
    assert "run_ml_offline_production_candidate_scoring_cli" in module_source

    argv = [
        "pipeline.cli",
        "ml-offline-production-candidate-scoring",
        "--label-dataset",
        str(tmp_path / "labels.json"),
        "--split-policy",
        str(tmp_path / "policy.json"),
        "--metric-gates",
        str(tmp_path / "gates.json"),
        "--audit-ranker-experiment",
        str(tmp_path / "ranker.json"),
        "--embeddings",
        str(tmp_path / "embeddings.json"),
        "--ranking-run-id",
        "   ",
        "--output",
        str(tmp_path / "out.json"),
        "--markdown-output",
        str(tmp_path / "out.md"),
        "--database-url",
        "postgresql://research_radar:research_radar@localhost:5432/research_radar",
    ]
    import pipeline.cli as cli_main

    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit):
            cli_main.main()

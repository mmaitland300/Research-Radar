"""Tests for production-candidate offline scoring v1."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.ml_offline_baseline_eval import sha256_file
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
    return {
        "metadata": {
            "artifact_type": "ml_labeled_text_embeddings",
            "embedding_artifact_version": "ml-labeled-text-embeddings-v3",
            "source_label_dataset_sha256": label_sha,
            "source_label_dataset_version": "ml-label-dataset-v8",
        },
        "rows": [{"row_id": row_id, "embedding_status": "ok", "embedding": [0.0, 0.1]} for row_id in row_ids],
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


def _fixture_paths(tmp_path: Path) -> dict[str, Path]:
    label_path = _write_json(tmp_path, "labels.json", _label_payload())
    label_sha = sha256_file(label_path)
    policy_path = _write_json(tmp_path, "policy.json", _policy_payload())
    ranker_path = _write_json(tmp_path, "ranker.json", _ranker_payload())
    ranker_sha = sha256_file(ranker_path)
    gates_path = _write_json(tmp_path, "gates.json", _gates_payload(ranker_path=ranker_path, ranker_sha=ranker_sha))
    embeddings_path = _write_json(
        tmp_path,
        "embeddings.json",
        _embeddings_payload(label_sha=label_sha, row_ids=["r1a", "r1b", "r2", "r3", "r4"]),
    )
    return {
        "label_dataset_path": label_path,
        "split_policy_path": policy_path,
        "metric_gates_path": gates_path,
        "audit_ranker_experiment_path": ranker_path,
        "embeddings_path": embeddings_path,
    }


def _build(tmp_path: Path, *, conn: _FakeConn | None = None, paths: dict[str, Path] | None = None) -> tuple[dict, _FakeConn]:
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
    assert payload["scoring_mode_details"]["learned_product_scores_produced"] is False
    assert payload["learned_or_embedding_metrics"]["metrics"] is None
    assert payload["learned_or_embedding_metrics"]["learned_product_scores_produced"] is False
    assert "per-fold coefficients only" in payload["learned_or_embedding_metrics"]["reason"]


def test_markdown_includes_not_shadow_not_production_caveats(tmp_path: Path) -> None:
    payload, _fc = _build(tmp_path)
    md = markdown_from_ml_offline_production_candidate_scoring(payload)

    assert "Not Shadow / Not Production" in md
    assert "This is not shadow scoring" in md
    assert "This is not production scoring" in md
    assert "No production model artifact" in md
    assert "Product-candidate metric gates v1" in md


def test_database_url_must_be_local() -> None:
    assert assert_local_database_url("postgresql://research_radar:research_radar@127.0.0.1:5432/research_radar")[
        "local_database_url_confirmed"
    ]
    with pytest.raises(MLOfflineProductionCandidateScoringError, match="local Docker Postgres"):
        assert_local_database_url("postgresql://user:pass@containers-us-west-1.railway.app:5432/railway")


def test_cli_parser_includes_database_url_and_command_is_read_only(tmp_path: Path) -> None:
    package_root = Path(__file__).resolve().parents[1]
    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
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

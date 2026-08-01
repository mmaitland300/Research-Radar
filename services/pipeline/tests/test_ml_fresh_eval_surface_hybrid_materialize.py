"""Tests for fresh eval surface hybrid materialization v1."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from unittest.mock import patch

import psycopg
import pytest

from cli_parser_source import read_cli_parser_source
from pipeline.ml_fresh_eval_surface_hybrid_materialize import (
    MLFreshEvalSurfaceHybridMaterializeError,
    SURFACE_VERSION,
    _work_set_sha256,
    build_ml_fresh_eval_surface_hybrid_materialize_payload,
    write_ml_fresh_eval_surface_hybrid_materialize,
)
from pipeline.ml_label_dataset import sha256_file


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
            rid = (self._params or ("",))[0]
            return self._p.run_metadata_by_id.get(rid)
        return None

    def fetchall(self) -> list[dict]:
        if "GROUP BY ps.ranking_run_id" in self._sql:
            return list(self._p.source_rows)
        if "FROM paper_scores ps" in self._sql and "JOIN works w" in self._sql:
            rid, family = self._params or (None, None)
            rows = self._p.candidate_rows_by_source.get((rid, family), [])
            return sorted(rows, key=lambda row: (-float(row.get("final_score") or 0.0), int(row["internal_work_id"])))
        return []


class _FakeCurCtx:
    def __init__(self, parent: "_FakeConn") -> None:
        self._cur = _FakeCur(parent)

    def __enter__(self) -> _FakeCur:
        return self._cur

    def __exit__(self, *args: object) -> None:
        return None


class _FakeConn:
    def __init__(
        self,
        *,
        candidate_rows_by_source: dict[tuple[str, str], list[dict]] | None = None,
        source_rows: list[dict] | None = None,
        run_metadata_by_id: dict[str, dict] | None = None,
    ) -> None:
        self.candidate_rows_by_source = candidate_rows_by_source if candidate_rows_by_source is not None else {
            ("rank-fresh", "emerging"): _fresh_candidate_rows(),
        }
        self.source_rows = source_rows if source_rows is not None else [
            {
                "ranking_run_id": "rank-fresh",
                "paper_scores_row_count": 5,
                "status": "succeeded",
                "ranking_version": "rv2",
                "corpus_snapshot_version": "snap2",
                "embedding_version": "emb",
                "started_at": None,
                "finished_at": None,
                "config_json": {},
                "counts_json": {},
            }
        ]
        self.run_metadata_by_id = run_metadata_by_id if run_metadata_by_id is not None else {
            "rank-fresh": {
                "ranking_run_id": "rank-fresh",
                "ranking_version": "rv2",
                "corpus_snapshot_version": "snap2",
                "embedding_version": "emb",
                "status": "succeeded",
                "started_at": None,
                "finished_at": None,
                "config_json": {},
                "counts_json": {},
                "notes": None,
            },
            "rank-ee2ba6c816": {
                "ranking_run_id": "rank-ee2ba6c816",
                "ranking_version": "rv1",
                "corpus_snapshot_version": "snap1",
                "embedding_version": "emb",
                "status": "succeeded",
                "started_at": None,
                "finished_at": None,
                "config_json": {},
                "counts_json": {},
                "notes": None,
            },
            "rank-old-sha": {
                "ranking_run_id": "rank-old-sha",
                "ranking_version": "rv1",
                "corpus_snapshot_version": "snap1",
                "embedding_version": "emb",
                "status": "succeeded",
                "started_at": None,
                "finished_at": None,
                "config_json": {},
                "counts_json": {},
                "notes": None,
            },
        }
        self.executed_sql: list[str] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None


def _candidate_row(idx: int, work_id: str, score: float, *, run_id: str = "rank-fresh") -> dict:
    return {
        "ranking_run_id": run_id,
        "internal_work_id": idx,
        "recommendation_family": "emerging",
        "semantic_score": score,
        "citation_velocity_score": score / 2,
        "topic_growth_score": score / 3,
        "bridge_score": score / 4,
        "diversity_penalty": 0.0,
        "final_score": score,
        "bridge_eligible": True,
        "reason_short": "fixture",
        "openalex_id": f"https://openalex.org/{work_id}",
        "title": f"Work {work_id}",
        "year": 2026,
        "citation_count": idx,
        "inclusion_status": "included",
        "corpus_snapshot_version": "snap2",
    }


def _fresh_candidate_rows() -> list[dict]:
    return [
        _candidate_row(1, "W1", 0.95),
        _candidate_row(2, "W2", 0.80),
        _candidate_row(3, "W3", 0.70),
        _candidate_row(4, "W4", 0.60),
        _candidate_row(5, "W900", 0.50),
    ]


def _old_candidate_rows(*, run_id: str = "rank-ee2ba6c816") -> list[dict]:
    return [
        _candidate_row(101, "W900", 0.90, run_id=run_id),
        _candidate_row(102, "W901", 0.70, run_id=run_id),
    ]


def _label_row(row_id: str, work_id: str, target: bool) -> dict:
    return {
        "row_id": row_id,
        "paper_id": f"https://openalex.org/{work_id}",
        "work_id": work_id,
        "openalex_work_id": work_id,
        "split": "audit_only",
        "relevance_label": "good" if target else "irrelevant",
        "novelty_label": "useful" if target else "obvious",
        "good_or_acceptable": target,
    }


def _label_dataset_payload(*, sparse: bool = False, dataset_version: str = "ml-label-dataset-v8") -> dict:
    rows = [
        _label_row("r1", "W1", True),
        _label_row("r2", "W2", False),
    ]
    if not sparse:
        rows.extend(
            [
                _label_row("r3", "W3", True),
                _label_row("r4a", "W4", True),
                _label_row("r4b", "W4", False),
                _label_row("r-old", "W900", True),
            ]
        )
    return {"dataset_version": dataset_version, "rows": rows}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _fixture_paths(
    tmp_path: Path,
    *,
    sparse_labels: bool = False,
    label_dataset_version: str = "ml-label-dataset-v8",
) -> dict[str, Path]:
    old_ids = ["W900", "W901"]
    old_sha = _work_set_sha256(old_ids)
    scoring_path = _write_json(
        tmp_path,
        "old-scoring.json",
        {
            "metadata": {
                "artifact_type": "ml_offline_production_candidate_scoring",
                "experiment_version": "ml-offline-production-candidate-scoring-v3",
                "eval_work_set_sha256": old_sha,
            },
            "candidate_pool_rows": [{"canonical_openalex_work_id": work_id} for work_id in old_ids],
        },
    )
    assignment_path = _write_json(
        tmp_path,
        "assignment.json",
        {
            "metadata": {
                "assignment_version": "ml-learned-scorer-holdout-assignment-v1",
                "eval_work_set_sha256": old_sha,
            },
            "work_assignments": [
                {"canonical_openalex_work_id": work_id, "assignment": "eval"} for work_id in old_ids
            ],
        },
    )
    policy_path = _write_json(
        tmp_path,
        "fresh-policy.json",
        {
            "metadata": {
                "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
                "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
                "status": "blocked_for_confirmatory_until_surface_materialized",
                "disallowed_eval_work_set_sha256": old_sha,
                "inputs": [
                    {"name": "production_candidate_scoring", "path": scoring_path.name, "sha256": sha256_file(scoring_path)},
                    {"name": "holdout_assignment", "path": assignment_path.name, "sha256": sha256_file(assignment_path)},
                ],
            },
            "disallowed_surfaces": [
                {
                    "surface_id": "product_candidate_eval_surface_rank-ee2ba6c816_emerging_v3",
                    "eval_work_set_sha256": old_sha,
                    "candidate_work_count": 2,
                    "ranking_run_id": "rank-ee2ba6c816",
                    "family": "emerging",
                }
            ],
            "label_policy": {
                "minimum_confirmatory_label_thresholds": {
                    "minimum_candidate_work_count": 4,
                    "minimum_confirmatory_labeled_work_count": 3,
                    "minimum_confirmatory_positive_work_count": 2,
                    "minimum_confirmatory_negative_work_count": 1,
                    "minimum_confirmatory_label_coverage_rate": 0.75,
                    "minimum_distinct_negative_work_count": 1,
                }
            },
            "policy_assertions": {
                "old_217_surface_confirmatory_reuse_allowed": False,
                "frozen_primary_hybrid_arm": "hybrid_rank_mean_50_50",
                "shadow_allowed_by_this_policy": False,
            },
        },
    )
    label_path = _write_json(
        tmp_path,
        "labels.json",
        _label_dataset_payload(sparse=sparse_labels, dataset_version=label_dataset_version),
    )
    conflict_path = tmp_path / "conflict-policy.md"
    conflict_path.write_text("# Conflict Policy\n\nNo silent conflict merge.\n", encoding="utf-8")
    return {
        "fresh_surface_policy_path": policy_path,
        "label_dataset_path": label_path,
        "conflict_policy_path": conflict_path,
    }


def _build(tmp_path: Path, conn: _FakeConn | None = None, **kwargs: object) -> dict:
    return build_ml_fresh_eval_surface_hybrid_materialize_payload(
        conn or _FakeConn(),
        **_fixture_paths(tmp_path),
        repo_root=tmp_path,
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        generated_at="2026-05-17T00:00:00Z",
        **kwargs,
    )


def test_happy_path_materializes_fresh_pool_and_thresholds_pass(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn=conn)

    assert payload["metadata"]["status"] == "materialized_ready"
    assert payload["metadata"]["surface_version"] == SURFACE_VERSION
    assert payload["metadata"]["expected_label_dataset_version"] == "ml-label-dataset-v8"
    assert payload["metadata"]["label_dataset_version"] == "ml-label-dataset-v8"
    assert payload["ready_for_hybrid_validation_scoring"] is True
    assert payload["recommended_next_stage"] == "execute_hybrid_validation_on_fresh_surface_v1"
    assert payload["candidate_source"]["ranking_run_id"] == "rank-fresh"
    assert payload["candidate_pool"]["candidate_work_count"] == 5
    assert payload["disallowed_overlap_report"]["overlap_work_count"] == 1
    assert payload["confirmatory_eligibility"]["confirmatory_metric_eligible_work_count"] == 4
    assert all(item["passed"] is True for item in payload["threshold_check"].values())
    assert not re.search(r"\b(insert|update|delete|drop|alter|create)\b", "\n".join(conn.executed_sql).lower())


def test_blocked_no_fresh_candidate_source_when_discovery_finds_no_qualifying_run(tmp_path: Path) -> None:
    payload = _build(tmp_path, conn=_FakeConn(candidate_rows_by_source={}, source_rows=[]))

    assert payload["metadata"]["status"] == "blocked_no_fresh_candidate_source"
    assert payload["candidate_source"]["source_selection_mode"] == "blocked"
    assert payload["recommended_next_stage"] == "create_new_product_candidate_ranking_run_or_snapshot"
    assert payload["ready_for_hybrid_validation_scoring"] is False


def test_explicit_old_ranking_run_is_blocked_source_not_fresh(tmp_path: Path) -> None:
    conn = _FakeConn(
        candidate_rows_by_source={("rank-ee2ba6c816", "emerging"): _old_candidate_rows()},
        source_rows=[],
    )

    payload = _build(tmp_path, conn=conn, ranking_run_id="rank-ee2ba6c816")

    assert payload["metadata"]["status"] == "blocked_source_not_fresh"
    assert payload["candidate_source"]["selected_source_rationale"] == "ranking_run_id_matches_disallowed_old_surface"
    assert payload["recommended_next_stage"] == "blocked_fix_source_selection"


def test_candidate_work_set_sha_equal_to_old_sha_is_blocked(tmp_path: Path) -> None:
    conn = _FakeConn(
        candidate_rows_by_source={("rank-old-sha", "emerging"): _old_candidate_rows(run_id="rank-old-sha")},
        source_rows=[],
    )

    payload = _build(tmp_path, conn=conn, ranking_run_id="rank-old-sha")

    assert payload["metadata"]["status"] == "blocked_source_not_fresh"
    assert payload["candidate_source"]["selected_source_rationale"] == "candidate_work_set_sha_matches_disallowed_old_surface"
    assert payload["candidate_pool"]["work_set_sha_differs_from_old_eval"] is False


def test_overlap_works_are_tagged_and_excluded_from_confirmatory_metrics(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    rows = {row["canonical_openalex_work_id"]: row for row in payload["candidate_pool"]["candidate_rows"]}

    assert rows["W900"]["previous_eval_overlap"] is True
    assert rows["W900"]["confirmatory_metric_eligible"] is False
    assert payload["label_coverage"]["work_level"]["confirmatory_candidate_work_count"] == 4
    assert payload["label_coverage"]["observation_level"]["overlap_labeled_observation_count_smoke_only"] == 1


def test_label_threshold_failures_route_to_labeling_plan(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, sparse_labels=True)
    payload = build_ml_fresh_eval_surface_hybrid_materialize_payload(
        _FakeConn(),
        **paths,
        repo_root=tmp_path,
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        generated_at="2026-05-17T00:00:00Z",
    )

    assert payload["metadata"]["status"] == "materialized_needs_labels"
    assert payload["ready_for_hybrid_validation_scoring"] is False
    assert payload["recommended_next_stage"] == "create_fresh_eval_labeling_plan_hybrid_v1"
    assert payload["threshold_check"]["minimum_confirmatory_labeled_work_count"]["passed"] is False


def test_v9_label_dataset_succeeds_when_expected_version_is_v9(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, label_dataset_version="ml-label-dataset-v9")
    payload = build_ml_fresh_eval_surface_hybrid_materialize_payload(
        _FakeConn(),
        **paths,
        repo_root=tmp_path,
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        expected_label_dataset_version="ml-label-dataset-v9",
        generated_at="2026-05-17T00:00:00Z",
    )

    assert payload["metadata"]["expected_label_dataset_version"] == "ml-label-dataset-v9"
    assert payload["metadata"]["label_dataset_version"] == "ml-label-dataset-v9"
    assert payload["metadata"]["status"] == "materialized_ready"


def test_v9_label_dataset_fails_when_default_expected_version_remains_v8(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, label_dataset_version="ml-label-dataset-v9")

    with pytest.raises(MLFreshEvalSurfaceHybridMaterializeError, match="expected label dataset_version='ml-label-dataset-v8'"):
        build_ml_fresh_eval_surface_hybrid_materialize_payload(
            _FakeConn(),
            **paths,
            repo_root=tmp_path,
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            generated_at="2026-05-17T00:00:00Z",
        )


def test_conflict_and_duplicate_work_labels_are_counted_without_silent_merge(tmp_path: Path) -> None:
    payload = _build(tmp_path)
    work = payload["label_coverage"]["work_level"]

    assert work["duplicate_labeled_work_group_count"] == 1
    assert work["conflicting_target_work_group_count"] == 1
    assert payload["label_coverage"]["observation_level_labels_preserved"] is True
    assert payload["label_coverage"]["silent_conflict_merge_used"] is False


def test_cli_writes_json_and_markdown_with_mocked_db(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path, label_dataset_version="ml-label-dataset-v9")
    out_json = tmp_path / "surface.json"
    out_md = tmp_path / "surface.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-eval-surface-hybrid-materialize",
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--expected-label-dataset-version",
        "ml-label-dataset-v9",
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--family",
        "emerging",
        "--database-url",
        "postgresql://research_radar:research_radar@localhost:5432/research_radar",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv), patch(
        "pipeline.ml_fresh_eval_surface_hybrid_materialize.psycopg.connect",
        return_value=_FakeConn(),
    ):
        cli_main.main()

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["metadata"]["status"] == "materialized_ready"
    assert payload["metadata"]["expected_label_dataset_version"] == "ml-label-dataset-v9"
    assert payload["metadata"]["label_dataset_version"] == "ml-label-dataset-v9"
    assert "Ready for hybrid validation scoring" in out_md.read_text(encoding="utf-8")


def test_connection_failure_artifacts_omit_exception_details_and_database_dsn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _fixture_paths(tmp_path)
    out_json = tmp_path / "blocked.json"
    out_md = tmp_path / "blocked.md"
    secret_dsn = "postgresql://admin:super-secret@localhost:5432/research"

    def fail_connect(*args: object, **kwargs: object) -> None:
        raise psycopg.OperationalError(f"connection refused for {secret_dsn}")

    monkeypatch.setattr(
        "pipeline.ml_fresh_eval_surface_hybrid_materialize.psycopg.connect",
        fail_connect,
    )

    payload = write_ml_fresh_eval_surface_hybrid_materialize(
        **paths,
        output_path=out_json,
        markdown_output_path=out_md,
        database_url=secret_dsn,
        repo_root=tmp_path,
    )

    assert payload["metadata"]["status"] == "blocked_no_fresh_candidate_source"
    assert payload["candidate_source"]["connection_error"] == (
        "OperationalError: details redacted"
    )
    assert secret_dsn not in json.dumps(payload)
    assert secret_dsn not in out_json.read_text(encoding="utf-8")
    assert secret_dsn not in out_md.read_text(encoding="utf-8")


def test_module_imports_no_openai_openalex_or_sklearn_and_cli_flags_are_scoped() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_eval_surface_hybrid_materialize.py"
    ).read_text(encoding="utf-8").lower()
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "import sklearn" not in module_source
    assert "from sklearn" not in module_source

    cli_source = read_cli_parser_source(package_root)
    start = cli_source.index('"ml-fresh-eval-surface-hybrid-materialize"')
    end = cli_source.index("ml_source_split_tiny_baseline_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" in parser_block
    assert "--expected-label-dataset-version" in parser_block
    assert "--openai" not in parser_block.lower()
    assert "--openalex" not in parser_block.lower()

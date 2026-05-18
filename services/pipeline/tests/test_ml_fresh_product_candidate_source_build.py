"""Tests for fresh product-candidate source build v1."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_fresh_product_candidate_source_build as build_mod
from pipeline.ml_fresh_product_candidate_source_build import (
    BUILD_VERSION,
    MLFreshProductCandidateSourceBuildError,
    _work_set_sha256,
    assert_local_database_url,
    build_ml_fresh_product_candidate_source_build_payload,
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
            return list(self._p.candidate_rows_by_source.get((rid, family), []))
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
        source_rows: list[dict] | None = None,
        candidate_rows_by_source: dict[tuple[str, str], list[dict]] | None = None,
        run_metadata_by_id: dict[str, dict] | None = None,
    ) -> None:
        self.source_rows = source_rows if source_rows is not None else _source_rows()
        self.candidate_rows_by_source = candidate_rows_by_source if candidate_rows_by_source is not None else _candidate_rows_by_source()
        self.run_metadata_by_id = run_metadata_by_id if run_metadata_by_id is not None else _run_metadata()
        self.executed_sql: list[str] = []

    def cursor(self, row_factory: object | None = None) -> _FakeCurCtx:
        return _FakeCurCtx(self)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: object) -> None:
        return None


def _candidate_row(idx: int, work_id: str, *, run_id: str, score: float | None = None) -> dict:
    return {
        "ranking_run_id": run_id,
        "internal_work_id": idx,
        "recommendation_family": "emerging",
        "semantic_score": 0.5,
        "citation_velocity_score": 0.4,
        "topic_growth_score": 0.3,
        "bridge_score": 0.2,
        "diversity_penalty": 0.0,
        "final_score": score if score is not None else float(1000 - idx) / 1000.0,
        "bridge_eligible": True,
        "reason_short": "fixture",
        "openalex_id": f"https://openalex.org/{work_id}",
        "title": f"Work {work_id}",
        "year": 2026,
        "citation_count": idx,
        "inclusion_status": "included",
        "corpus_snapshot_version": f"snapshot-{run_id}",
    }


def _rows_for(run_id: str, work_ids: list[str]) -> list[dict]:
    return [_candidate_row(idx + 1, work_id, run_id=run_id) for idx, work_id in enumerate(work_ids)]


def _old_ids() -> list[str]:
    return [f"W90{i:02d}" for i in range(1, 16)]


def _underpowered_ids() -> list[str]:
    return [f"W2{i:02d}" for i in range(0, 44)]


def _source_rows() -> list[dict]:
    return [
        {"ranking_run_id": "rank-new", "paper_scores_row_count": 85, "status": "succeeded", "corpus_snapshot_version": "snapshot-new", "started_at": "2026-05-18T02:00:00Z", "finished_at": "2026-05-18T03:00:00Z"},
        {"ranking_run_id": "rank-3904fec89d", "paper_scores_row_count": 59, "status": "succeeded", "corpus_snapshot_version": "snapshot-small", "started_at": "2026-05-18T00:00:00Z", "finished_at": "2026-05-18T01:00:00Z"},
    ]


def _run_metadata() -> dict[str, dict]:
    return {
        "rank-new": {"ranking_run_id": "rank-new", "status": "succeeded", "corpus_snapshot_version": "snapshot-new", "started_at": "2026-05-18T02:00:00Z", "finished_at": "2026-05-18T03:00:00Z"},
        "rank-3904fec89d": {"ranking_run_id": "rank-3904fec89d", "status": "succeeded", "corpus_snapshot_version": "snapshot-small", "started_at": "2026-05-18T00:00:00Z", "finished_at": "2026-05-18T01:00:00Z"},
    }


def _candidate_rows_by_source(new_count: int = 70) -> dict[tuple[str, str], list[dict]]:
    old_ids = _old_ids()
    underpowered_ids = _underpowered_ids()
    new_ids = [f"W{i}" for i in range(1, new_count + 1)] + old_ids
    small_ids = underpowered_ids + old_ids
    return {
        ("rank-new", "emerging"): _rows_for("rank-new", new_ids),
        ("rank-3904fec89d", "emerging"): _rows_for("rank-3904fec89d", small_ids),
    }


def _label_row(row_id: str, work_id: str, target: bool) -> dict:
    return {
        "row_id": row_id,
        "paper_id": f"https://openalex.org/{work_id}",
        "work_id": work_id,
        "openalex_work_id": work_id,
        "relevance_label": "good" if target else "irrelevant",
        "novelty_label": "useful" if target else "obvious",
        "good_or_acceptable": target,
    }


def _label_dataset_payload() -> dict:
    rows = []
    for idx, work_id in enumerate(_underpowered_ids()[:20], start=1):
        rows.append(_label_row(f"pos-u-{idx}", work_id, True))
    for idx in range(1, 16):
        rows.append(_label_row(f"pos-n-{idx}", f"W{idx}", True))
    for idx in range(16, 31):
        rows.append(_label_row(f"neg-n-{idx}", f"W{idx}", False))
    return {"dataset_version": "ml-label-dataset-v8", "rows": rows}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    expansion_plan: dict | None = None,
    policy: dict | None = None,
) -> dict[str, Path]:
    old_ids = _old_ids()
    underpowered_ids = _underpowered_ids()
    old_sha = _work_set_sha256(old_ids)
    underpowered_sha = _work_set_sha256(underpowered_ids + old_ids)
    monkeypatch.setattr(build_mod, "OLD_EVAL_WORK_SET_SHA256", old_sha)
    monkeypatch.setattr(build_mod, "UNDERPOWERED_WORK_SET_SHA256", underpowered_sha)

    scoring_path = _write_json(
        tmp_path,
        "old-scoring.json",
        {"candidate_pool_rows": [{"canonical_openalex_work_id": work_id} for work_id in old_ids]},
    )
    assignment_path = _write_json(
        tmp_path,
        "assignment.json",
        {"work_assignments": [{"canonical_openalex_work_id": work_id, "assignment": "eval"} for work_id in old_ids]},
    )
    policy_payload = policy or {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": old_sha,
            "inputs": [
                {"name": "production_candidate_scoring", "path": scoring_path.name, "sha256": sha256_file(scoring_path)},
                {"name": "holdout_assignment", "path": assignment_path.name, "sha256": sha256_file(assignment_path)},
            ],
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
    }
    expansion_payload = expansion_plan or {
        "metadata": {
            "artifact_type": "ml_fresh_candidate_source_expansion_plan",
            "plan_version": "ml-fresh-candidate-source-expansion-plan-v1",
        },
        "recommended_next_stage": "implement_or_run_fresh_product_candidate_source_build_v1",
        "current_blocker_summary": {
            "candidate_gap": 56,
            "best_source_ranking_run_id": "rank-3904fec89d",
            "best_source_confirmatory_eligible_work_count": 44,
        },
        "source_expansion_requirements": {"minimum_confirmatory_eligible_work_count": 100},
    }
    conflict_path = tmp_path / "conflict-policy.md"
    conflict_path.write_text("# Conflict Policy\n\nNo silent merge.\n", encoding="utf-8")
    return {
        "fresh_candidate_source_expansion_plan_path": _write_json(tmp_path, "expansion-plan.json", expansion_payload),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy_payload),
        "label_dataset_path": _write_json(tmp_path, "labels.json", _label_dataset_payload()),
        "conflict_policy_path": conflict_path,
    }


def _build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    conn: _FakeConn | None = None,
    **kwargs: object,
) -> dict:
    return build_ml_fresh_product_candidate_source_build_payload(
        conn or _FakeConn(),
        **_paths(tmp_path, monkeypatch),
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        family="emerging",
        repo_root=tmp_path,
        generated_at="2026-05-18T00:00:00Z",
        **kwargs,
    )


def test_artifact_only_freeze_happy_path_builds_large_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, monkeypatch, conn=conn)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_product_candidate_source_build"
    assert payload["metadata"]["build_version"] == BUILD_VERSION
    assert payload["metadata"]["mode"] == "artifact_only_freeze"
    assert payload["build_result"]["status"] == "source_built_artifact_only"
    assert payload["build_result"]["confirmatory_eligible_work_count"] == 114
    assert payload["build_result"]["candidate_threshold_met"] is True
    assert payload["recommended_next_stage"] == "extend_materializer_to_accept_candidate_source_build_artifact"
    assert "materializer extension required" in payload["candidate_source"]["materializer_handoff"]["use_with_materializer_command"]
    assert not re.search(r"\b(insert|update|delete|drop|alter|create)\b", "\n".join(conn.executed_sql).lower())


def test_blocked_when_union_cannot_reach_threshold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn(candidate_rows_by_source=_candidate_rows_by_source(new_count=30))
    payload = _build(tmp_path, monkeypatch, conn=conn)

    assert payload["build_result"]["status"] == "blocked_needs_corpus_or_candidate_expansion"
    assert payload["build_result"]["confirmatory_eligible_work_count"] == 74
    assert payload["build_result"]["candidate_threshold_met"] is False
    assert payload["recommended_next_stage"] == "blocked_expand_corpus_or_candidate_generation"


def test_old_217_excluded_from_confirmatory_denominator(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["candidate_source"]["candidate_rows_summary"]["row_count"] == 129
    assert payload["build_result"]["old_surface_overlap_count"] == 15
    assert payload["build_result"]["confirmatory_eligible_work_count"] == 114


def test_underpowered_44_overlap_tagged_and_separated_from_incremental_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)

    assert payload["build_result"]["underpowered_source_overlap_count"] == 44
    assert payload["build_result"]["incremental_confirmatory_eligible_work_count_excluding_underpowered_source"] == 70
    assert "incremental works outside rank-3904fec89d" in payload["build_result"]["candidate_threshold_basis"]
    tagged = [row for row in payload["candidate_source"]["candidate_rows"] if row["underpowered_source_overlap"] is True]
    assert len(tagged) == 44


def test_rejects_hosted_database_urls() -> None:
    with pytest.raises(MLFreshProductCandidateSourceBuildError, match="hosted production"):
        assert_local_database_url("postgresql://user:pass@project.neon.tech/db")


def test_write_mode_without_safe_writer_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(MLFreshProductCandidateSourceBuildError, match="unsupported in v1"):
        _build(tmp_path, monkeypatch, mode="eval_db_source_create")

    with pytest.raises(MLFreshProductCandidateSourceBuildError, match="unsupported in v1"):
        _build(tmp_path, monkeypatch, write_eval_db_source=True)


def test_expansion_plan_validation_failures(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bad_paths = _paths(
        tmp_path,
        monkeypatch,
        expansion_plan={
            "metadata": {
                "artifact_type": "ml_fresh_candidate_source_expansion_plan",
                "plan_version": "ml-fresh-candidate-source-expansion-plan-v1",
            },
            "recommended_next_stage": "not_this",
            "current_blocker_summary": {
                "candidate_gap": 56,
                "best_source_ranking_run_id": "rank-3904fec89d",
                "best_source_confirmatory_eligible_work_count": 44,
            },
            "source_expansion_requirements": {"minimum_confirmatory_eligible_work_count": 100},
        },
    )
    with pytest.raises(MLFreshProductCandidateSourceBuildError, match="recommend"):
        build_ml_fresh_product_candidate_source_build_payload(
            _FakeConn(),
            **bad_paths,
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
        )


def test_shadow_and_production_are_always_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["confirmatory_validation_complete"] is False
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["production_tables_modified"] is False


def test_cli_writes_json_and_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path, monkeypatch)
    out_json = tmp_path / "source-build.json"
    out_md = tmp_path / "source-build.md"
    fake_conn = _FakeConn()
    monkeypatch.setattr(build_mod.psycopg, "connect", lambda *args, **kwargs: fake_conn)

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-product-candidate-source-build",
        "--fresh-candidate-source-expansion-plan",
        str(paths["fresh_candidate_source_expansion_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--label-dataset",
        str(paths["label_dataset_path"]),
        "--conflict-policy",
        str(paths["conflict_policy_path"]),
        "--mode",
        "artifact_only_freeze",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["build_result"]["status"] == "source_built_artifact_only"
    assert "Underpowered 44 Overlap" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_and_cli_flags_are_scoped() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_product_candidate_source_build.py"
    ).read_text(encoding="utf-8").lower()
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-fresh-product-candidate-source-build"')
    end = cli_source.index("ml_tiny_baseline_rollup_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" in parser_block
    assert "--scoring-mode" not in parser_block
    assert "--audit-embedding-scorer-export" not in parser_block
    assert "--label-import" not in parser_block
    assert "--train" not in parser_block

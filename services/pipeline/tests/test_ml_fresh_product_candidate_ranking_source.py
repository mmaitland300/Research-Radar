"""Tests for fresh product-candidate ranking source freeze v1."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

import pipeline.ml_fresh_product_candidate_ranking_source as source_mod
from pipeline.ml_fresh_product_candidate_ranking_source import (
    MLFreshProductCandidateRankingSourceError,
    SOURCE_VERSION,
    _work_set_sha256,
    assert_local_database_url,
    build_ml_fresh_product_candidate_ranking_source_payload,
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


def _candidate_row(idx: int, work_id: str, *, run_id: str, score: float | None = None) -> dict:
    return {
        "ranking_run_id": run_id,
        "internal_work_id": idx,
        "recommendation_family": "emerging",
        "final_score": score if score is not None else float(1000 - idx) / 1000.0,
        "openalex_id": f"https://openalex.org/{work_id}",
        "title": f"Work {work_id}",
        "year": 2026,
        "citation_count": idx,
        "inclusion_status": "included",
        "corpus_snapshot_version": f"snapshot-{run_id}",
    }


def _rows_for(run_id: str, work_ids: list[str]) -> list[dict]:
    return [_candidate_row(idx + 1, work_id, run_id=run_id) for idx, work_id in enumerate(work_ids)]


def _source_rows() -> list[dict]:
    return [
        {"ranking_run_id": "rank-big", "paper_scores_row_count": 110, "status": "succeeded", "corpus_snapshot_version": "snapshot-big", "started_at": "2026-05-18T02:00:00Z", "finished_at": "2026-05-18T03:00:00Z"},
        {"ranking_run_id": "rank-3904fec89d", "paper_scores_row_count": 59, "status": "succeeded", "corpus_snapshot_version": "snapshot-small", "started_at": "2026-05-18T00:00:00Z", "finished_at": "2026-05-18T01:00:00Z"},
    ]


def _run_metadata() -> dict[str, dict]:
    return {
        "rank-big": {"ranking_run_id": "rank-big", "status": "succeeded", "corpus_snapshot_version": "snapshot-big", "started_at": "2026-05-18T02:00:00Z", "finished_at": "2026-05-18T03:00:00Z"},
        "rank-3904fec89d": {"ranking_run_id": "rank-3904fec89d", "status": "succeeded", "corpus_snapshot_version": "snapshot-small", "started_at": "2026-05-18T00:00:00Z", "finished_at": "2026-05-18T01:00:00Z"},
    }


def _candidate_rows_by_source(old_ids: list[str] | None = None) -> dict[tuple[str, str], list[dict]]:
    old_ids = old_ids or _old_ids()
    big_ids = [f"W{i}" for i in range(1, 106)] + old_ids
    small_ids = [f"W{i}" for i in range(200, 244)] + old_ids[:15]
    return {
        ("rank-big", "emerging"): _rows_for("rank-big", big_ids),
        ("rank-3904fec89d", "emerging"): _rows_for("rank-3904fec89d", small_ids),
    }


def _old_ids() -> list[str]:
    return [f"W90{i:02d}" for i in range(1, 16)]


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
    for idx in range(1, 31):
        rows.append(_label_row(f"pos-{idx}", f"W{idx}", True))
    for idx in range(31, 41):
        rows.append(_label_row(f"neg-{idx}", f"W{idx}", False))
    rows.append(_label_row("conflict-a", "W41", True))
    rows.append(_label_row("conflict-b", "W41", False))
    return {"dataset_version": "ml-label-dataset-v8", "rows": rows}


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    old_ids = _old_ids()
    old_sha = _work_set_sha256(old_ids)
    monkeypatch.setattr(source_mod, "OLD_EVAL_WORK_SET_SHA256", old_sha)
    monkeypatch.setattr(source_mod, "CURRENT_UNDERPOWERED_WORK_SET_SHA256", _work_set_sha256([f"W{i}" for i in range(200, 244)] + old_ids[:15]))

    scoring_path = _write_json(
        tmp_path,
        "old-scoring.json",
        {"candidate_pool_rows": [{"canonical_openalex_work_id": work_id} for work_id in old_ids]},
    )
    assignment_path = _write_json(
        tmp_path,
        "assignment.json",
        {
            "work_assignments": [
                {"canonical_openalex_work_id": work_id, "assignment": "eval"} for work_id in old_ids
            ]
        },
    )
    policy_path = _write_json(
        tmp_path,
        "policy.json",
        {
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
        },
    )
    plan_path = _write_json(
        tmp_path,
        "plan.json",
        {
            "metadata": {
                "artifact_type": "ml_fresh_eval_labeling_plan_hybrid",
                "plan_version": "ml-fresh-eval-labeling-plan-hybrid-v1",
            },
            "recommended_next_stage": "create_fresh_product_candidate_ranking_source_v1",
            "blocking_diagnosis": {"current_surface_can_be_made_ready_by_labeling_alone": False},
        },
    )
    labels_path = _write_json(tmp_path, "labels.json", _label_dataset_payload())
    conflict_path = tmp_path / "conflict-policy.md"
    conflict_path.write_text("# Conflict Policy\n\nNo silent merge.\n", encoding="utf-8")
    return {
        "fresh_eval_labeling_plan_path": plan_path,
        "fresh_surface_policy_path": policy_path,
        "label_dataset_path": labels_path,
        "conflict_policy_path": conflict_path,
    }


def _build(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conn: _FakeConn | None = None, **kwargs: object) -> dict:
    return build_ml_fresh_product_candidate_ranking_source_payload(
        conn or _FakeConn(),
        **_paths(tmp_path, monkeypatch),
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        family="emerging",
        repo_root=tmp_path,
        generated_at="2026-05-18T00:00:00Z",
        **kwargs,
    )


def test_happy_path_selects_source_with_enough_confirmatory_candidates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, monkeypatch, conn=conn)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_product_candidate_ranking_source"
    assert payload["metadata"]["source_version"] == SOURCE_VERSION
    assert payload["source_selection"]["status"] == "source_frozen_needs_materialization"
    assert payload["selected_source_freeze"]["ranking_run_id"] == "rank-big"
    assert payload["selected_source_freeze"]["confirmatory_eligible_work_count"] == 105
    assert payload["recommended_next_stage"] == "rerun_fresh_eval_surface_materialize_with_selected_source"
    assert "ml-fresh-eval-surface-hybrid-materialize" in payload["selected_source_freeze"]["use_with_materializer_command"]
    assert not re.search(r"\b(insert|update|delete|drop|alter|create)\b", "\n".join(conn.executed_sql).lower())


def test_blocked_when_no_source_meets_candidate_threshold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn(source_rows=[_source_rows()[1]], candidate_rows_by_source={("rank-3904fec89d", "emerging"): _candidate_rows_by_source()[("rank-3904fec89d", "emerging")]})
    payload = _build(tmp_path, monkeypatch, conn=conn)

    assert payload["source_selection"]["status"] == "blocked_no_source_meets_candidate_threshold"
    assert payload["selected_source_freeze"] is None
    assert payload["recommended_next_stage"] == "create_new_or_larger_candidate_snapshot"


def test_rank_3904_underpowered_source_is_considered_but_not_selected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    considered = {source["ranking_run_id"]: source for source in payload["candidate_sources_considered"]}

    assert "rank-3904fec89d" in considered
    assert considered["rank-3904fec89d"]["confirmatory_eligible_work_count"] == 44
    assert payload["selected_source_freeze"]["ranking_run_id"] == "rank-big"


def test_explicit_ranking_run_id_freezes_only_that_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch, ranking_run_id="rank-big")

    assert [source["ranking_run_id"] for source in payload["candidate_sources_considered"]] == ["rank-big"]
    assert payload["source_selection"]["status"] == "source_frozen_needs_materialization"
    assert payload["selected_source_freeze"]["ranking_run_id"] == "rank-big"
    assert "explicit ranking_run_id rank-big" in payload["source_selection"]["selection_rule"]


def test_old_overlap_excluded_from_confirmatory_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    selected = payload["selected_source_freeze"]

    assert selected["overlap_with_old_217_count"] == 15
    assert selected["confirmatory_eligible_work_count"] == 105
    big = next(source for source in payload["candidate_sources_considered"] if source["ranking_run_id"] == "rank-big")
    assert big["candidate_work_count"] == 120


def test_rejects_hosted_database_urls() -> None:
    with pytest.raises(MLFreshProductCandidateRankingSourceError, match="hosted production"):
        assert_local_database_url("postgresql://user:pass@project.neon.tech/db")


def test_shadow_and_production_are_always_false(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _build(tmp_path, monkeypatch)
    blockers = payload["shadow_and_production_blockers"]

    assert blockers["shadow_scoring_allowed"] is False
    assert blockers["production_default_allowed"] is False
    assert blockers["confirmatory_validation_complete"] is False


def test_no_forbidden_imports_and_cli_has_no_scoring_training_or_label_import_flags() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_product_candidate_ranking_source.py"
    ).read_text(encoding="utf-8").lower()
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "import openalex" not in module_source
    assert "from openalex" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

    cli_source = (package_root / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-fresh-product-candidate-ranking-source"')
    end = cli_source.index("ml_tiny_baseline_rollup_parser", start)
    parser_block = cli_source[start:end]
    assert "--database-url" in parser_block
    assert "--scoring-mode" not in parser_block
    assert "--audit-embedding-scorer-export" not in parser_block
    assert "--label-import" not in parser_block
    assert "--train" not in parser_block

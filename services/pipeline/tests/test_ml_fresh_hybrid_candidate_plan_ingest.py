"""Tests for fresh hybrid candidate plan ingest v1."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_fresh_hybrid_candidate_plan_ingest as ingest_mod
from pipeline.ml_fresh_hybrid_candidate_plan_ingest import (
    INGEST_VERSION,
    MLFreshHybridCandidatePlanIngestError,
    assert_local_database_url,
    build_ml_fresh_hybrid_candidate_plan_ingest_payload,
)


class _Result:
    def __init__(self, *, one: tuple | None = None, all_rows: list[tuple] | None = None) -> None:
        self._one = one
        self._all = all_rows or []

    def fetchone(self) -> tuple | None:
        return self._one

    def fetchall(self) -> list[tuple]:
        return self._all


class _Tx:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self.snapshot: dict | None = None

    def __enter__(self) -> "_Tx":
        self.snapshot = self.conn.snapshot_state()
        return self

    def __exit__(self, exc_type, _exc, _tb) -> bool:
        if exc_type is not None and self.snapshot is not None:
            self.conn.restore_state(self.snapshot)
            self.conn.rollback_count += 1
        return False


class _FakeConn:
    def __init__(self, *, fail_on_work_insert: bool = False, existing_snapshot: str | None = None) -> None:
        self.fail_on_work_insert = fail_on_work_insert
        self.existing_snapshot = existing_snapshot
        self.source_policies: set[str] = set()
        self.source_snapshot_versions: dict[str, dict] = {}
        self.ingest_runs: dict[str, dict] = {}
        self.works: dict[int, dict] = {}
        self.raw_openalex_works: list[dict] = []
        self.next_work_id = 1
        self.sql: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    def snapshot_state(self) -> dict:
        return {
            "works": copy.deepcopy(self.works),
            "raw_openalex_works": copy.deepcopy(self.raw_openalex_works),
            "next_work_id": self.next_work_id,
            "ingest_runs": copy.deepcopy(self.ingest_runs),
        }

    def restore_state(self, state: dict) -> None:
        self.works = state["works"]
        self.raw_openalex_works = state["raw_openalex_works"]
        self.next_work_id = state["next_work_id"]
        self.ingest_runs = state["ingest_runs"]

    def transaction(self) -> _Tx:
        return _Tx(self)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def execute(self, sql: str, params: tuple | None = None) -> _Result:
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        if compact.startswith("SELECT source_snapshot_version FROM source_snapshot_versions"):
            snapshot = str(params[0])
            exists = snapshot == self.existing_snapshot or snapshot in self.source_snapshot_versions
            return _Result(one=(snapshot,) if exists else None)
        if compact == "SELECT source_slug FROM source_policies":
            return _Result(all_rows=[(slug,) for slug in sorted(self.source_policies)])
        if compact.startswith("INSERT INTO source_snapshot_versions"):
            self.source_snapshot_versions[str(params[0])] = {
                "policy_name": params[1],
                "policy_hash": params[2],
                "ingest_mode": params[3],
                "note": params[4],
            }
            return _Result()
        if compact.startswith("INSERT INTO ingest_runs"):
            self.ingest_runs[str(params[0])] = {
                "source_snapshot_version": params[1],
                "policy_hash": params[2],
                "status": params[3],
                "config_json": params[5],
                "counts_json": None,
                "error_message": None,
            }
            return _Result()
        if compact.startswith("UPDATE ingest_runs"):
            run = self.ingest_runs[str(params[4])]
            run["status"] = params[0]
            run["counts_json"] = params[2]
            run["error_message"] = params[3]
            return _Result()
        if compact.startswith("SELECT id FROM works WHERE openalex_id"):
            for work_id, work in self.works.items():
                if work["openalex_id"] == params[0]:
                    return _Result(one=(work_id,))
            return _Result()
        if "lower(doi) = ANY" in compact:
            doi_set = set(params[0])
            for work_id, work in self.works.items():
                doi = work.get("doi")
                if doi and doi.casefold() in doi_set:
                    return _Result(one=(work_id,))
            return _Result()
        if compact.startswith("INSERT INTO raw_openalex_works"):
            self.raw_openalex_works.append(
                {"openalex_id": params[0], "ingest_run_id": params[1], "payload": json.loads(params[6])}
            )
            return _Result()
        if compact.startswith("INSERT INTO works"):
            if self.fail_on_work_insert:
                raise RuntimeError("controlled insert failure")
            work_id = self.next_work_id
            self.next_work_id += 1
            self.works[work_id] = {
                "openalex_id": params[0],
                "title": params[1],
                "abstract": params[2],
                "year": params[3],
                "doi": params[4],
                "type": params[5],
                "language": params[6],
                "source_slug": params[9],
                "citation_count": params[10],
                "is_core_corpus": params[11],
                "corpus_snapshot_version": params[13],
            }
            return _Result(one=(work_id,))
        if compact.startswith("UPDATE works"):
            work_id = int(params[-1])
            self.works[work_id].update(
                {
                    "openalex_id": params[0],
                    "title": params[1],
                    "abstract": params[2],
                    "year": params[3],
                    "doi": params[4],
                    "type": params[5],
                    "language": params[6],
                    "source_slug": params[9],
                    "citation_count": params[10],
                    "is_core_corpus": params[11],
                    "corpus_snapshot_version": params[13],
                }
            )
            return _Result()
        raise AssertionError(f"unhandled SQL: {compact}")


def _candidate(work_id: str, *, bucket_id: str = "audio_ml_signal_processing") -> dict:
    return {
        "openalex_id": f"https://openalex.org/{work_id}",
        "canonical_openalex_work_id": work_id,
        "title": f"Fresh hybrid candidate {work_id}",
        "year": 2026,
        "citation_count": 3,
        "source_display_name": "Fixture Venue",
        "bucket_id": bucket_id,
        "inclusion_reason": "bucket_allow_signal",
        "matched_terms": ["music"],
        "old_217_overlap": False,
        "underpowered_source_overlap": False,
        "confirmatory_metric_candidate": True,
        "negative_or_borderline_candidate": bucket_id == "audio_ml_signal_processing",
    }


def _plan_payload(*, threshold: bool = True, next_stage: str = "ingest_fresh_hybrid_candidate_plan_as_snapshot_v1") -> dict:
    rows = [_candidate("W1"), _candidate("W2", bucket_id="music_recommender_systems")]
    return {
        "metadata": {
            "artifact_type": "ml_fresh_hybrid_corpus_candidate_plan",
            "plan_version": "ml-fresh-hybrid-corpus-candidate-plan-v1",
            "openalex_contact_provenance": {
                "contact_mode": "none",
                "contact_provided": False,
                "auth_mode": "api_key",
                "api_key_provided": True,
            },
        },
        "planning_context": {"target_min": 1, "target_max": 10},
        "candidate_selection": {
            "selected_total": len(rows),
            "selected_candidates": rows,
            "selected_candidate_work_set_sha256": "sha-fixture",
            "estimated_confirmatory_eligible_after_old_217_exclusion": 100 if threshold else 99,
            "estimated_overlap_with_old_217": 0,
            "candidate_threshold_plausibly_met": threshold,
        },
        "bucket_summary": {"negative_or_borderline_candidate": {"selected_count": 1}},
        "readiness_estimate": {"expected_next_stage": next_stage},
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": ingest_mod.OLD_EVAL_WORK_SET_SHA256,
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, *, plan: dict | None = None, policy: dict | None = None) -> dict[str, Path]:
    return {
        "fresh_hybrid_corpus_candidate_plan_path": _write_json(tmp_path, "plan.json", plan or _plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn | None = None, **kwargs: object) -> dict:
    return build_ml_fresh_hybrid_candidate_plan_ingest_payload(
        **_paths(tmp_path),
        snapshot_version="source-snapshot-test",
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        repo_root=tmp_path,
        conn=conn or _FakeConn(),
        **kwargs,
    )


def test_happy_path_writes_snapshot_and_works(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn=conn)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_hybrid_candidate_plan_ingest"
    assert payload["metadata"]["ingest_version"] == INGEST_VERSION
    assert payload["snapshot"]["source_snapshot_version"] == "source-snapshot-test"
    assert payload["snapshot"]["eval_only"] is True
    assert payload["ingest_result"]["status"] == "succeeded"
    assert payload["ingest_result"]["selected_total"] == 2
    assert payload["ingest_result"]["inserted_count"] == 2
    assert payload["ingest_result"]["updated_count"] == 0
    assert payload["ingest_result"]["snapshot_work_count"] == 2
    assert len(conn.works) == 2
    assert conn.source_snapshot_versions["source-snapshot-test"]["ingest_mode"] == "snapshot-import"
    assert payload["snapshot"]["fresh_hybrid_ingest_intent"] == "fresh_hybrid_candidate_plan_snapshot_import"


def test_rejects_plan_when_threshold_not_met(tmp_path: Path) -> None:
    paths = _paths(tmp_path, plan=_plan_payload(threshold=False))
    with pytest.raises(MLFreshHybridCandidatePlanIngestError, match="candidate_threshold"):
        build_ml_fresh_hybrid_candidate_plan_ingest_payload(
            **paths,
            snapshot_version="source-snapshot-test",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_hosted_database_url() -> None:
    with pytest.raises(MLFreshHybridCandidatePlanIngestError, match="hosted production"):
        assert_local_database_url("postgresql://user:pass@project.neon.tech/db")


def test_refuses_ranking_runs_and_paper_scores_writes(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn=conn)
    sql = "\n".join(conn.sql).lower()

    assert "ranking_runs" not in sql
    assert "paper_scores" not in sql
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["sql_write_report"]["production_tables_modified"] is False


def test_duplicate_snapshot_version_fails(tmp_path: Path) -> None:
    with pytest.raises(MLFreshHybridCandidatePlanIngestError, match="already exists"):
        _build(tmp_path, conn=_FakeConn(existing_snapshot="source-snapshot-test"))


def test_transaction_rollback_on_injected_failure(tmp_path: Path) -> None:
    conn = _FakeConn(fail_on_work_insert=True)
    with pytest.raises(MLFreshHybridCandidatePlanIngestError, match="controlled insert failure"):
        _build(tmp_path, conn=conn)

    assert conn.works == {}
    assert conn.raw_openalex_works == []
    assert len(conn.ingest_runs) == 1
    assert next(iter(conn.ingest_runs.values()))["status"] == "failed"
    assert conn.rollback_count >= 1


def test_dry_run_performs_no_writes(tmp_path: Path) -> None:
    payload = build_ml_fresh_hybrid_candidate_plan_ingest_payload(
        **_paths(tmp_path),
        snapshot_version="source-snapshot-test",
        dry_run=True,
        repo_root=tmp_path,
    )

    assert payload["metadata"]["dry_run"] is True
    assert payload["ingest_result"]["inserted_count"] == 0
    assert payload["ingest_result"]["snapshot_work_count"] == 0
    assert payload["ingest_result"]["planned_candidate_count"] == 2
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["affected_row_counts"] == {}


def test_cli_writes_json_and_markdown_with_fake_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "ingest.json"
    out_md = tmp_path / "ingest.md"
    fake_conn = _FakeConn()
    monkeypatch.setattr(ingest_mod.psycopg, "connect", lambda *args, **kwargs: fake_conn)

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-hybrid-candidate-plan-ingest",
        "--fresh-hybrid-corpus-candidate-plan",
        str(paths["fresh_hybrid_corpus_candidate_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--snapshot-version",
        "source-snapshot-test",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["ingest_result"]["inserted_count"] == 2
    assert "DB Write Scope" in out_md.read_text(encoding="utf-8")


def test_no_openalex_openai_sklearn_imports() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (
        package_root / "pipeline" / "ml_fresh_hybrid_candidate_plan_ingest.py"
    ).read_text(encoding="utf-8").lower()

    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

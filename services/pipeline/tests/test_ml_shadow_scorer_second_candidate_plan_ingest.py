"""Tests for second shadow-generalization candidate plan ingest v1."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from cli_parser_source import read_cli_parser_source
import pipeline.ml_shadow_scorer_second_candidate_plan_ingest as ingest_mod
from pipeline.ml_shadow_scorer_second_candidate_plan_ingest import (
    EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
    INGEST_VERSION,
    MLShadowScorerSecondCandidatePlanIngestError,
    OLD_EVAL_WORK_SET_SHA256,
    assert_local_database_url,
    build_ml_shadow_scorer_second_candidate_plan_ingest_payload,
)
from tests.snapshot_membership_fake_sql import apply_membership_upsert

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


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
        self.memberships: set[tuple[int, str]] = set()
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
        if compact.startswith("INSERT INTO work_source_snapshot_memberships"):
            apply_membership_upsert(self.memberships, params)
            return _Result()
        raise AssertionError(f"unhandled SQL: {compact}")


def _candidate(idx: int) -> dict:
    bucket = "audio_ml_signal_processing" if idx <= 213 else "music_recommender_systems"
    return {
        "openalex_id": f"https://openalex.org/W{idx:07d}",
        "canonical_openalex_work_id": f"W{idx:07d}",
        "title": f"Second hybrid candidate {idx}",
        "year": 2024,
        "citation_count": idx % 11,
        "source_display_name": "Fixture Venue",
        "bucket_id": bucket,
        "inclusion_reason": "bucket_allow_signal",
        "matched_terms": ["music"],
        "old_217_overlap": idx <= 217,
        "first_validated_surface_overlap": idx <= 358,
        "underpowered_source_overlap": idx <= 16,
        "underpowered_overlap_basis": "preview",
        "confirmatory_metric_candidate_after_exclusions": idx <= 168,
        "negative_or_borderline_candidate": idx <= 213,
        "label_used_for_selection": False,
    }


def _plan_payload(*, threshold: bool = True, next_stage: str = "ingest_second_hybrid_candidate_plan_as_snapshot_v1") -> dict:
    rows = [_candidate(idx) for idx in range(1, 529)]
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_hybrid_candidate_plan",
            "plan_version": "ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1",
            "openalex_contact_provenance": {
                "contact_mode": "api_key_only",
                "contact_provided": False,
                "auth_mode": "api_key",
                "api_key_provided": True,
            },
        },
        "planning_context": {"target_min": 180, "target_max": 600},
        "candidate_selection": {
            "selected_total": len(rows),
            "selected_candidates": rows,
            "planned_candidate_work_set_sha256": EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
            "candidate_threshold_plausibly_met": threshold,
            "label_dataset_used_for_selection": False,
        },
        "bucket_summary": {
            "by_bucket": [{"bucket_id": "audio_ml_signal_processing", "selected_count": 213}],
            "rollups": {
                "borderline_or_negative_candidate": {
                    "rollup_bucket": "borderline_or_negative_candidate",
                    "selected_count": 213,
                }
            },
        },
        "readiness_estimate": {
            "selected_total": len(rows),
            "planned_candidate_work_set_sha256": EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
            "estimated_confirmatory_eligible_after_exclusions": 168 if threshold else 99,
            "estimated_overlap_with_old_217": 217,
            "estimated_overlap_with_first_validated_surface": 358,
            "full_underpowered_overlap_available": False,
            "underpowered_source_overlap_preview_count": 21,
            "expected_next_stage": next_stage,
        },
        "recommended_next_stage": next_stage,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "caveats": ["Dry-run plan only; no DB writes, no database writes, no snapshot."],
    }


def _generalization_plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_generalization_audit_plan",
            "plan_version": "ml-shadow-scorer-v1-generalization-audit-v1",
        },
        "generalization_audit_plan_defined": True,
        "runtime_implementation_authorized": False,
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
            "disallowed_eval_work_set_sha256": OLD_EVAL_WORK_SET_SHA256,
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, *, plan: dict | None = None, audit_plan: dict | None = None, policy: dict | None = None) -> dict[str, Path]:
    return {
        "second_hybrid_candidate_plan_path": _write_json(tmp_path, "plan.json", plan or _plan_payload()),
        "generalization_audit_plan_path": _write_json(
            tmp_path,
            "generalization-plan.json",
            audit_plan or _generalization_plan_payload(),
        ),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", policy or _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn | None = None, **kwargs: object) -> dict:
    return build_ml_shadow_scorer_second_candidate_plan_ingest_payload(
        **_paths(tmp_path),
        snapshot_version="source-snapshot-shadow-generalization-test",
        database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
        repo_root=tmp_path,
        conn=conn or _FakeConn(),
        **kwargs,
    )


def test_happy_path_writes_snapshot_and_528_works(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn=conn)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_candidate_plan_ingest"
    assert payload["metadata"]["ingest_version"] == INGEST_VERSION
    assert payload["snapshot"]["source_snapshot_version"] == "source-snapshot-shadow-generalization-test"
    assert payload["snapshot"]["eval_only"] is True
    assert payload["snapshot"]["shadow_generalization_candidate_source"] is True
    assert payload["ingest_result"]["status"] == "succeeded"
    assert payload["ingest_result"]["selected_total"] == 528
    assert payload["ingest_result"]["inserted_count"] == 528
    assert payload["ingest_result"]["updated_count"] == 0
    assert payload["ingest_result"]["snapshot_work_count"] == 528
    assert len(conn.works) == 528
    assert len(conn.raw_openalex_works) == 528
    assert payload["candidate_plan_summary"]["planned_candidate_work_set_sha256"] == EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256
    assert payload["overlap_summary"]["confirmatory_metric_candidate_after_exclusions_count_in_plan"] == 168
    assert payload["overlap_summary"]["underpowered_preview_overlap_count_in_plan"] == 16
    assert payload["shadow_and_production_blockers"]["missing_second_fresh_candidate_source"] is False


def test_rejects_plan_when_threshold_not_met(tmp_path: Path) -> None:
    paths = _paths(tmp_path, plan=_plan_payload(threshold=False))
    with pytest.raises(MLShadowScorerSecondCandidatePlanIngestError, match="candidate_threshold"):
        build_ml_shadow_scorer_second_candidate_plan_ingest_payload(
            **paths,
            snapshot_version="source-snapshot-test",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_hosted_database_url() -> None:
    with pytest.raises(MLShadowScorerSecondCandidatePlanIngestError, match="hosted production"):
        assert_local_database_url("postgresql://user:pass@project.neon.tech/db")


def test_dry_run_performs_no_writes(tmp_path: Path) -> None:
    payload = build_ml_shadow_scorer_second_candidate_plan_ingest_payload(
        **_paths(tmp_path),
        snapshot_version="source-snapshot-shadow-generalization-test",
        dry_run=True,
        repo_root=tmp_path,
    )

    assert payload["metadata"]["dry_run"] is True
    assert payload["ingest_result"]["status"] == "dry_run_validated"
    assert payload["ingest_result"]["inserted_count"] == 0
    assert payload["ingest_result"]["snapshot_work_count"] == 0
    assert payload["ingest_result"]["planned_candidate_count"] == 528
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["affected_row_counts"] == {}
    assert payload["shadow_and_production_blockers"]["missing_second_fresh_candidate_source"] is True


def test_duplicate_snapshot_version_fails(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondCandidatePlanIngestError, match="already exists"):
        _build(tmp_path, conn=_FakeConn(existing_snapshot="source-snapshot-shadow-generalization-test"))


def test_transaction_rollback_on_injected_failure(tmp_path: Path) -> None:
    conn = _FakeConn(fail_on_work_insert=True)
    with pytest.raises(MLShadowScorerSecondCandidatePlanIngestError, match="controlled insert failure"):
        _build(tmp_path, conn=conn)

    assert conn.works == {}
    assert conn.raw_openalex_works == []
    assert len(conn.ingest_runs) == 1
    assert next(iter(conn.ingest_runs.values()))["status"] == "failed"
    assert conn.rollback_count >= 1


def test_sql_write_report_blocks_ranking_scores_embeddings_and_production(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn=conn)
    sql = "\n".join(conn.sql).lower()

    assert "ranking_runs" not in sql
    assert "paper_scores" not in sql
    assert "embeddings" not in sql
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["sql_write_report"]["embeddings_written"] is False
    assert payload["sql_write_report"]["production_tables_modified"] is False
    assert payload["metadata"]["runtime_implementation_authorized"] is False
    assert payload["metadata"]["online_shadow_execution_enabled"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert payload["shadow_and_production_blockers"]["production_default_allowed"] is False


def test_cli_writes_json_and_markdown_with_fake_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    out_json = tmp_path / "ingest.json"
    out_md = tmp_path / "ingest.md"
    fake_conn = _FakeConn()
    monkeypatch.setattr(ingest_mod.psycopg, "connect", lambda *args, **kwargs: fake_conn)

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-shadow-scorer-second-candidate-plan-ingest",
        "--second-hybrid-candidate-plan",
        str(paths["second_hybrid_candidate_plan_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--snapshot-version",
        "source-snapshot-shadow-generalization-test",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["ingest_result"]["inserted_count"] == 528
    assert "DB Write Scope" in out_md.read_text(encoding="utf-8")


def test_no_openalex_client_openai_sklearn_imports_and_cli_flags() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_candidate_plan_ingest.py").read_text(
        encoding="utf-8"
    ).lower()
    cli_source = read_cli_parser_source(PACKAGE_ROOT)
    start = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"')
    end = cli_source.index('"ml-fresh-product-candidate-source-build"', start)
    cli_block = cli_source[start:end]

    assert "openalex_client" not in module_source
    assert "fetch_openalex" not in module_source
    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "--database-url" in cli_block
    assert "--openalex" not in cli_block.lower()
    assert "--ranking-run-id" not in cli_block
    assert "--embedding" not in cli_block.lower()

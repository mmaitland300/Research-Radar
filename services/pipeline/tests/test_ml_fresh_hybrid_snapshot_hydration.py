"""Tests for fresh hybrid snapshot hydration v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_fresh_hybrid_snapshot_hydration as hydration_mod
from pipeline.ml_fresh_hybrid_snapshot_hydration import (
    HYDRATION_VERSION,
    MLFreshHybridSnapshotHydrationError,
    build_ml_fresh_hybrid_snapshot_hydration_payload,
)
from pipeline.openalex_client import OPENALEX_API_KEY_ENV
from tests.snapshot_membership_fake_sql import (
    apply_hydrate_work_update,
    build_memberships_from_works,
    included_work_ids,
    is_hydrate_work_update,
    is_membership_included_work_select,
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

    def __enter__(self) -> "_Tx":
        return self

    def __exit__(self, exc_type, _exc, _tb) -> bool:
        if exc_type is not None:
            self.conn.rollback_count += 1
        return False


class _FakeConn:
    def __init__(self) -> None:
        self.sql: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.snapshots = {"source-snapshot-fresh-hybrid-v1-20260518"}
        self.ingest_runs: dict[str, dict] = {}
        self.raw_openalex_works: list[dict] = []
        self.works: dict[int, dict] = {
            1: {
                "id": 1,
                "openalex_id": "https://openalex.org/W1",
                "title": "Fresh candidate one",
                "abstract": None,
                "type": "unknown",
                "language": "en",
                "doi": None,
                "citation_count": 0,
                "year": 2026,
                "publication_date": None,
                "source_slug": "fresh",
                "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
                "last_ingest_run_id": "ingest-old",
            },
            2: {
                "id": 2,
                "openalex_id": "https://openalex.org/W2",
                "title": "Fresh candidate two",
                "abstract": None,
                "type": "unknown",
                "language": "en",
                "doi": None,
                "citation_count": 0,
                "year": 2026,
                "publication_date": None,
                "source_slug": "fresh",
                "corpus_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518",
                "last_ingest_run_id": "ingest-old",
            },
            3: {
                "id": 3,
                "openalex_id": "https://openalex.org/W9",
                "title": "Other snapshot work",
                "abstract": None,
                "type": "unknown",
                "language": "en",
                "doi": None,
                "citation_count": 0,
                "year": 2026,
                "publication_date": None,
                "source_slug": "fresh",
                "corpus_snapshot_version": "another-snapshot",
                "last_ingest_run_id": "ingest-other",
            },
        }
        self.memberships = build_memberships_from_works(self.works)

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

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
        if compact.startswith("SELECT 1 FROM source_snapshot_versions"):
            snapshot = str(params[0])
            return _Result(one=(1,) if snapshot in self.snapshots else None)
        if is_membership_included_work_select(compact):
            snapshot = str(params[0])
            rows = []
            for work_id in included_work_ids(self.works, self.memberships, snapshot):
                work = self.works[work_id]
                rows.append(
                    (
                        work["id"],
                        work["openalex_id"],
                        work["title"],
                        work["abstract"],
                        work["type"],
                        work["language"],
                        work["doi"],
                        work["citation_count"],
                        work["year"],
                        work["publication_date"],
                        work["source_slug"],
                    )
                )
            return _Result(all_rows=rows)
        if compact.startswith("SELECT id, openalex_id, title, abstract, type, language, doi, citation_count, year, publication_date, source_slug FROM works"):
            snapshot = str(params[0])
            rows = []
            for work in self.works.values():
                if work["corpus_snapshot_version"] == snapshot:
                    rows.append(
                        (
                            work["id"],
                            work["openalex_id"],
                            work["title"],
                            work["abstract"],
                            work["type"],
                            work["language"],
                            work["doi"],
                            work["citation_count"],
                            work["year"],
                            work["publication_date"],
                            work["source_slug"],
                        )
                    )
            rows.sort(key=lambda row: row[0])
            return _Result(all_rows=rows)
        if compact.startswith("INSERT INTO ingest_runs"):
            self.ingest_runs[str(params[0])] = {
                "snapshot": params[1],
                "policy_hash": params[2],
                "status": "running",
                "config_json": json.loads(params[4]),
                "counts_json": None,
                "error_message": None,
            }
            return _Result()
        if compact.startswith("UPDATE ingest_runs SET status = %s, finished_at = %s, counts_json = %s::jsonb, error_message = NULL"):
            run = self.ingest_runs[str(params[3])]
            run["status"] = str(params[0])
            run["counts_json"] = json.loads(params[2])
            return _Result()
        if compact.startswith("UPDATE ingest_runs SET status = 'failed'"):
            run = self.ingest_runs[str(params[2])]
            run["status"] = "failed"
            run["error_message"] = str(params[1])
            return _Result()
        if compact.startswith("INSERT INTO raw_openalex_works"):
            self.raw_openalex_works.append(
                {"openalex_id": params[0], "ingest_run_id": params[1], "payload": json.loads(params[6])}
            )
            return _Result()
        if is_hydrate_work_update(compact):
            apply_hydrate_work_update(self.works, params)
            return _Result()
        raise AssertionError(f"Unhandled SQL: {compact}")


def _ingest_payload(*, status: str = "succeeded", next_stage: str = "hydrate_fresh_hybrid_snapshot_metadata_v1") -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_hybrid_candidate_plan_ingest",
            "ingest_version": "ml-fresh-hybrid-candidate-plan-ingest-v1",
        },
        "snapshot": {"source_snapshot_version": "source-snapshot-fresh-hybrid-v1-20260518"},
        "candidate_plan_summary": {"selected_total": 2},
        "ingest_result": {
            "status": status,
            "recommended_next_stage": next_stage,
            "snapshot_work_count": 2,
        },
        "sql_write_report": {
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "production_tables_modified": False,
        },
    }


def _plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_hybrid_corpus_candidate_plan",
            "plan_version": "ml-fresh-hybrid-corpus-candidate-plan-v1",
        }
    }


def _policy_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_eval_surface_policy_hybrid",
            "policy_version": "ml-fresh-eval-surface-policy-hybrid-v1",
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, *, ingest: dict | None = None) -> dict[str, Path]:
    return {
        "fresh_hybrid_candidate_plan_ingest_path": _write_json(tmp_path, "ingest.json", ingest or _ingest_payload()),
        "fresh_hybrid_corpus_candidate_plan_path": _write_json(tmp_path, "plan.json", _plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn, **kwargs: object) -> dict:
    with patch.object(hydration_mod.psycopg, "connect", return_value=conn):
        return build_ml_fresh_hybrid_snapshot_hydration_payload(
            **_paths(tmp_path),
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
            **kwargs,
        )


def test_happy_path_with_mock_openalex(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()

    payload = _build(tmp_path, conn, mock_openalex=True)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_hybrid_snapshot_hydration"
    assert payload["metadata"]["hydration_version"] == HYDRATION_VERSION
    assert payload["hydration_result"]["status"] == "succeeded"
    assert payload["hydration_result"]["works_considered_count"] == 2
    assert payload["hydration_result"]["fetched_count"] == 2
    assert payload["hydration_result"]["updated_count"] == 2
    assert payload["hydration_result"]["failed_count"] == 0
    assert payload["hydration_result"]["abstract_before_count"] == 0
    assert payload["hydration_result"]["abstract_after_count"] == 2
    assert payload["hydration_result"]["embedding_ready_count"] == 2
    assert payload["hydration_result"]["embedding_blocked_count"] == 0
    assert payload["hydration_result"]["snapshot_embedding_ready"] is True
    assert payload["hydration_result"]["recommended_next_stage"] == "embed_fresh_hybrid_snapshot_v1"
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False


def test_rejects_ingest_status_not_succeeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    with pytest.raises(MLFreshHybridSnapshotHydrationError, match="status"):
        build_ml_fresh_hybrid_snapshot_hydration_payload(
            **_paths(tmp_path, ingest=_ingest_payload(status="failed")),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_wrong_ingest_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLFreshHybridSnapshotHydrationError, match="recommended_next_stage"):
        build_ml_fresh_hybrid_snapshot_hydration_payload(
            **_paths(tmp_path, ingest=_ingest_payload(next_stage="embed_fresh_hybrid_snapshot_v1")),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_hosted_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLFreshHybridSnapshotHydrationError, match="hosted production"):
        build_ml_fresh_hybrid_snapshot_hydration_payload(
            **_paths(tmp_path),
            database_url="postgresql://user:pass@project.neon.tech/db",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_dry_run_no_writes_no_openalex(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn, dry_run=True)

    assert payload["metadata"]["dry_run"] is True
    assert payload["hydration_result"]["status"] == "dry_run_validated"
    assert payload["hydration_result"]["fetched_count"] == 0
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["affected_row_counts"] == {}
    assert conn.ingest_runs == {}
    assert conn.raw_openalex_works == []


def test_missing_api_key_fails_clearly_when_live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    with patch.object(hydration_mod.psycopg, "connect", return_value=conn):
        with pytest.raises(MLFreshHybridSnapshotHydrationError, match=OPENALEX_API_KEY_ENV):
            build_ml_fresh_hybrid_snapshot_hydration_payload(
                **_paths(tmp_path),
                database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
                repo_root=tmp_path,
            )


def test_cli_writes_json_and_markdown_with_mock_openalex(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    monkeypatch.setattr(hydration_mod.psycopg, "connect", lambda *args, **kwargs: conn)
    paths = _paths(tmp_path)
    out_json = tmp_path / "hydration.json"
    out_md = tmp_path / "hydration.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-hybrid-snapshot-hydrate",
        "--fresh-hybrid-candidate-plan-ingest",
        str(paths["fresh_hybrid_candidate_plan_ingest_path"]),
        "--fresh-hybrid-corpus-candidate-plan",
        str(paths["fresh_hybrid_corpus_candidate_plan_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--mock-openalex",
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["hydration_result"]["snapshot_embedding_ready"] is True
    markdown = out_md.read_text(encoding="utf-8")
    assert "Not Ranking / Not Embeddings Yet / Not Shadow / Not Production" in markdown


def test_no_openai_sklearn_imports() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_fresh_hybrid_snapshot_hydration.py").read_text(
        encoding="utf-8"
    ).lower()

    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source

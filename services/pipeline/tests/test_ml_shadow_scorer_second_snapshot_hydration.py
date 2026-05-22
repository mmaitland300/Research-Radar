"""Tests for second shadow-generalization snapshot hydration v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.ml_shadow_scorer_second_snapshot_hydration as hydration_mod
from pipeline.ml_shadow_scorer_second_candidate_plan_ingest import (
    EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
)
from pipeline.ml_shadow_scorer_second_snapshot_hydration import (
    DEFAULT_SNAPSHOT_VERSION,
    HYDRATION_VERSION,
    MLShadowScorerSecondSnapshotHydrationError,
    build_ml_shadow_scorer_second_snapshot_hydration_payload,
)
from pipeline.openalex_client import OPENALEX_API_KEY_ENV

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

    def __enter__(self) -> "_Tx":
        return self

    def __exit__(self, exc_type, _exc, _tb) -> bool:
        if exc_type is not None:
            self.conn.rollback_count += 1
        return False


class _FakeConn:
    def __init__(self, *, work_count: int = 528) -> None:
        self.sql: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.snapshots = {DEFAULT_SNAPSHOT_VERSION}
        self.ingest_runs: dict[str, dict] = {}
        self.raw_openalex_works: list[dict] = []
        self.works: dict[int, dict] = {
            idx: {
                "id": idx,
                "openalex_id": f"https://openalex.org/W{idx:07d}",
                "title": f"Second candidate {idx}",
                "abstract": None,
                "type": "unknown",
                "language": "en",
                "doi": None,
                "citation_count": 0,
                "year": 2026,
                "publication_date": None,
                "source_slug": "second",
                "corpus_snapshot_version": DEFAULT_SNAPSHOT_VERSION,
                "last_ingest_run_id": "ingest-old",
            }
            for idx in range(1, work_count + 1)
        }
        self.works[9999] = {
            "id": 9999,
            "openalex_id": "https://openalex.org/W9999",
            "title": "Other snapshot work",
            "abstract": None,
            "type": "unknown",
            "language": "en",
            "doi": None,
            "citation_count": 0,
            "year": 2026,
            "publication_date": None,
            "source_slug": "other",
            "corpus_snapshot_version": "another-snapshot",
            "last_ingest_run_id": "ingest-other",
        }

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
        if compact.startswith("UPDATE works SET title = %s, abstract = %s, type = %s, language = %s, doi = %s, citation_count = %s, publication_date = %s, year = %s, updated_date = %s, last_ingest_run_id = %s, updated_at = NOW() WHERE id = %s AND corpus_snapshot_version = %s"):
            work_id = int(params[10])
            snapshot = str(params[11])
            work = self.works[work_id]
            if work["corpus_snapshot_version"] != snapshot:
                return _Result()
            work["title"] = params[0]
            work["abstract"] = params[1]
            work["type"] = params[2]
            work["language"] = params[3]
            work["doi"] = params[4]
            work["citation_count"] = params[5]
            work["publication_date"] = params[6]
            work["year"] = params[7]
            work["updated_date"] = params[8]
            work["last_ingest_run_id"] = params[9]
            return _Result()
        raise AssertionError(f"Unhandled SQL: {compact}")


def _plan_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_hybrid_candidate_plan",
            "plan_version": "ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1",
        }
    }


def _ingest_payload(
    *,
    plan_sha: str,
    status: str = "succeeded",
    next_stage: str = "hydrate_second_shadow_generalization_snapshot_metadata_v1",
    snapshot_version: str = DEFAULT_SNAPSHOT_VERSION,
    snapshot_work_count: int = 528,
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_shadow_scorer_second_candidate_plan_ingest",
            "ingest_version": "ml-shadow-scorer-v1-second-candidate-plan-ingest-v1",
            "inputs": [
                {
                    "name": "second_hybrid_candidate_plan",
                    "path": "docs/audit/ml-shadow-scorer-v1-second-hybrid-candidate-plan-v1.json",
                    "sha256": plan_sha,
                }
            ],
        },
        "snapshot": {
            "source_snapshot_version": snapshot_version,
            "shadow_generalization_candidate_source": True,
        },
        "candidate_plan_summary": {
            "selected_total": 528,
            "planned_candidate_work_set_sha256": EXPECTED_PLANNED_CANDIDATE_WORK_SET_SHA256,
        },
        "ingest_result": {
            "status": status,
            "recommended_next_stage": next_stage,
            "snapshot_work_count": snapshot_work_count,
        },
        "sql_write_report": {
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "embeddings_written": False,
            "production_tables_modified": False,
        },
        "recommended_next_stage": next_stage,
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
        }
    }


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def _paths(tmp_path: Path, *, ingest: dict | None = None, plan: dict | None = None) -> dict[str, Path]:
    plan_path = _write_json(tmp_path, "plan.json", plan or _plan_payload())
    plan_sha = hydration_mod.sha256_file(plan_path)
    ingest_path = _write_json(tmp_path, "ingest.json", ingest or _ingest_payload(plan_sha=plan_sha))
    return {
        "second_candidate_plan_ingest_path": ingest_path,
        "second_hybrid_candidate_plan_path": plan_path,
        "generalization_audit_plan_path": _write_json(tmp_path, "generalization-plan.json", _generalization_plan_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn, **kwargs: object) -> dict:
    with patch.object(hydration_mod.psycopg, "connect", return_value=conn):
        return build_ml_shadow_scorer_second_snapshot_hydration_payload(
            **_paths(tmp_path),
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
            **kwargs,
        )


def test_happy_path_with_mock_openalex_hydrates_528_works(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()

    payload = _build(tmp_path, conn, mock_openalex=True)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_second_snapshot_hydration"
    assert payload["metadata"]["hydration_version"] == HYDRATION_VERSION
    assert payload["ingest_provenance"]["snapshot_work_count"] == 528
    assert payload["hydration_result"]["status"] == "succeeded"
    assert payload["hydration_result"]["works_considered_count"] == 528
    assert payload["hydration_result"]["fetched_count"] == 528
    assert payload["hydration_result"]["updated_count"] == 528
    assert payload["hydration_result"]["failed_count"] == 0
    assert payload["hydration_result"]["abstract_before_count"] == 0
    assert payload["hydration_result"]["abstract_after_count"] == 528
    assert payload["hydration_result"]["type_unknown_before_count"] == 528
    assert payload["hydration_result"]["type_unknown_after_count"] == 0
    assert payload["hydration_result"]["language_defaulted_before_count"] == 528
    assert payload["hydration_result"]["language_defaulted_after_count"] == 528
    assert payload["hydration_result"]["embedding_ready_count"] == 528
    assert payload["hydration_result"]["embedding_blocked_count"] == 0
    assert payload["hydration_result"]["snapshot_embedding_ready"] is True
    assert payload["recommended_next_stage"] == "embed_second_shadow_generalization_snapshot_v1"
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["sql_write_report"]["embeddings_written"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False


def test_dry_run_no_writes_no_openalex(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn, dry_run=True)

    assert payload["metadata"]["dry_run"] is True
    assert payload["hydration_result"]["status"] == "dry_run_validated"
    assert payload["hydration_result"]["works_considered_count"] == 528
    assert payload["hydration_result"]["fetched_count"] == 0
    assert payload["hydration_result"]["abstract_before_count"] == payload["hydration_result"]["abstract_after_count"]
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["affected_row_counts"] == {}
    assert conn.ingest_runs == {}
    assert conn.raw_openalex_works == []


def test_rejects_wrong_ingest_next_stage(tmp_path: Path) -> None:
    plan_path = _write_json(tmp_path, "plan.json", _plan_payload())
    ingest = _ingest_payload(plan_sha=hydration_mod.sha256_file(plan_path), next_stage="embed_second_shadow_generalization_snapshot_v1")
    with pytest.raises(MLShadowScorerSecondSnapshotHydrationError, match="recommended_next_stage"):
        build_ml_shadow_scorer_second_snapshot_hydration_payload(
            **_paths(tmp_path, ingest=ingest, plan=json.loads(plan_path.read_text(encoding="utf-8"))),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_snapshot_version_mismatch(tmp_path: Path) -> None:
    conn = _FakeConn()
    with pytest.raises(MLShadowScorerSecondSnapshotHydrationError, match="snapshot-version"):
        _build(tmp_path, conn, snapshot_version="source-snapshot-other", dry_run=True)


def test_rejects_snapshot_work_count_not_528(tmp_path: Path) -> None:
    plan_path = _write_json(tmp_path, "plan.json", _plan_payload())
    ingest = _ingest_payload(plan_sha=hydration_mod.sha256_file(plan_path), snapshot_work_count=527)
    with pytest.raises(MLShadowScorerSecondSnapshotHydrationError, match="snapshot_work_count"):
        build_ml_shadow_scorer_second_snapshot_hydration_payload(
            **_paths(tmp_path, ingest=ingest, plan=json.loads(plan_path.read_text(encoding="utf-8"))),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_plan_sha_mismatch_vs_ingest_inputs(tmp_path: Path) -> None:
    ingest = _ingest_payload(plan_sha="wrong-sha")
    with pytest.raises(MLShadowScorerSecondSnapshotHydrationError, match="sha256 mismatch"):
        build_ml_shadow_scorer_second_snapshot_hydration_payload(
            **_paths(tmp_path, ingest=ingest),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_hosted_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLShadowScorerSecondSnapshotHydrationError, match="hosted production"):
        build_ml_shadow_scorer_second_snapshot_hydration_payload(
            **_paths(tmp_path),
            database_url="postgresql://user:pass@project.neon.tech/db",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_missing_api_key_fails_clearly_when_live(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    with patch.object(hydration_mod.psycopg, "connect", return_value=conn):
        with pytest.raises(MLShadowScorerSecondSnapshotHydrationError, match=OPENALEX_API_KEY_ENV):
            build_ml_shadow_scorer_second_snapshot_hydration_payload(
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
        "ml-shadow-scorer-second-snapshot-hydration",
        "--second-candidate-plan-ingest",
        str(paths["second_candidate_plan_ingest_path"]),
        "--second-hybrid-candidate-plan",
        str(paths["second_hybrid_candidate_plan_path"]),
        "--generalization-audit-plan",
        str(paths["generalization_audit_plan_path"]),
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


def test_no_forbidden_imports_and_cli_shape() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_second_snapshot_hydration.py").read_text(
        encoding="utf-8"
    ).lower()
    import_lines = "\n".join(
        line.strip()
        for line in module_source.splitlines()
        if line.lstrip().startswith(("import ", "from "))
    )
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-second-snapshot-hydration"')
    end = cli_source.index('"corpus-v2-embed"', start)
    cli_block = cli_source[start:end]

    assert "import openai" not in module_source
    assert "from openai" not in module_source
    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert "ranking" not in import_lines
    assert "ml_shadow_scorer_v1" not in import_lines
    assert "ml_hybrid_validation" not in import_lines
    assert "--database-url" in cli_block
    assert "--mock-openalex" in cli_block
    assert "--dry-run" in cli_block

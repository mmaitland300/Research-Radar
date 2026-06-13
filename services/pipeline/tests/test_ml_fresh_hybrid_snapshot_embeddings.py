"""Tests for fresh hybrid snapshot embeddings v1."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.corpus_v2_embed as corpus_embed
import pipeline.ml_fresh_hybrid_snapshot_embeddings as embed_mod
from pipeline.ml_fresh_hybrid_snapshot_embeddings import (
    ARTIFACT_VERSION,
    MLFreshHybridSnapshotEmbeddingsError,
    build_ml_fresh_hybrid_snapshot_embeddings_payload,
)
from pipeline.corpus_v2_embed import OPENAI_API_KEY_ENV
from tests.snapshot_membership_fake_sql import (
    build_memberships_from_works,
    count_embeddings_for_snapshot,
    delete_embeddings_for_snapshot,
    embedding_coverage_counts,
    included_work_ids,
    is_membership_included_work_select,
)

SNAPSHOT = "source-snapshot-fresh-hybrid-v1-20260518"
EMBEDDING_VERSION = "fresh-hybrid-text-embedding-v1"


class _Result:
    def __init__(self, *, one: tuple | None = None, all_rows: list[tuple] | None = None) -> None:
        self._one = one
        self._all = all_rows or []

    def fetchone(self) -> tuple | None:
        return self._one

    def fetchall(self) -> list[tuple]:
        return self._all


class _FakeConn:
    def __init__(self) -> None:
        self.snapshots = {SNAPSHOT, "another-snapshot"}
        self.works: dict[int, dict] = {
            1: {
                "title": "Fresh one",
                "abstract": "Music information retrieval abstract one",
                "type": "article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": SNAPSHOT,
            },
            2: {
                "title": "Fresh two",
                "abstract": "Audio recommendation abstract two",
                "type": "proceedings-article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": SNAPSHOT,
            },
            3: {
                "title": "Other snapshot",
                "abstract": "Other abstract",
                "type": "article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": "another-snapshot",
            },
        }
        self.embeddings: dict[tuple[int, str], str] = {}
        self.memberships = build_memberships_from_works(self.works)
        self.sql: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def execute(self, sql: str, params: tuple | None = None) -> _Result:
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        if compact.startswith("SELECT 1 FROM source_snapshot_versions"):
            return _Result(one=(1,) if str(params[0]) in self.snapshots else None)
        if is_membership_included_work_select(compact):
            snapshot = str(params[0])
            rows = []
            for work_id in included_work_ids(self.works, self.memberships, snapshot):
                work = self.works[work_id]
                rows.append((work_id, work["title"], work["abstract"], work["type"], work["language"]))
            return _Result(all_rows=rows)
        if compact.startswith("SELECT id, title, abstract, type, language FROM works"):
            snapshot = str(params[0])
            rows = []
            for work_id, work in self.works.items():
                if work["corpus_snapshot_version"] == snapshot and work["inclusion_status"] == "included":
                    rows.append((work_id, work["title"], work["abstract"], work["type"], work["language"]))
            rows.sort(key=lambda row: row[0])
            return _Result(all_rows=rows)
        if compact == "SELECT COUNT(*) FROM embeddings WHERE embedding_version = %s":
            version = str(params[0])
            return _Result(one=(sum(1 for (_work_id, ev) in self.embeddings if ev == version),))
        if compact.startswith("SELECT COUNT(*) FROM embeddings e JOIN work_source_snapshot_memberships wssm"):
            snapshot = str(params[0])
            version = str(params[1])
            return _Result(
                one=(
                    count_embeddings_for_snapshot(
                        self.embeddings,
                        self.memberships,
                        snapshot=snapshot,
                        version=version,
                    ),
                )
            )
        if compact.startswith("DELETE FROM embeddings e USING work_source_snapshot_memberships wssm"):
            snapshot = str(params[0])
            version = str(params[1])
            delete_embeddings_for_snapshot(
                self.embeddings,
                self.memberships,
                snapshot=snapshot,
                version=version,
            )
            return _Result()
        if compact.startswith("SELECT COUNT(*), COUNT(e.work_id) FROM works w JOIN work_source_snapshot_memberships wssm"):
            snapshot = str(params[0])
            version = str(params[1])
            total, embedded = embedding_coverage_counts(
                self.works,
                self.memberships,
                self.embeddings,
                snapshot=snapshot,
                version=version,
            )
            return _Result(one=(total, embedded))
        if compact.startswith("SELECT COUNT(*) FROM embeddings e JOIN works w ON w.id = e.work_id"):
            version = str(params[0])
            snapshot = str(params[1])
            count = sum(
                1
                for (work_id, ev) in self.embeddings
                if ev == version and self.works[work_id]["corpus_snapshot_version"] == snapshot
            )
            return _Result(one=(count,))
        if compact.startswith("SELECT COUNT(*), COUNT(e.work_id) FROM works w LEFT JOIN embeddings e"):
            version = str(params[0])
            snapshot = str(params[1])
            total = sum(
                1
                for work in self.works.values()
                if work["corpus_snapshot_version"] == snapshot and work["inclusion_status"] == "included"
            )
            embedded = sum(
                1
                for (work_id, ev) in self.embeddings
                if ev == version
                and self.works[work_id]["corpus_snapshot_version"] == snapshot
                and self.works[work_id]["inclusion_status"] == "included"
            )
            return _Result(one=(total, embedded))
        if compact.startswith("DELETE FROM embeddings e USING works w"):
            version = str(params[0])
            snapshot = str(params[1])
            for key in list(self.embeddings):
                work_id, ev = key
                if ev == version and self.works[work_id]["corpus_snapshot_version"] == snapshot:
                    del self.embeddings[key]
            return _Result()
        if compact.startswith("INSERT INTO embeddings"):
            work_id = int(params[0])
            version = str(params[1])
            self.embeddings[(work_id, version)] = str(params[2])
            return _Result()
        raise AssertionError(f"Unhandled SQL: {compact}")


def _hydration_payload(
    *,
    ready: bool = True,
    next_stage: str = "embed_fresh_hybrid_snapshot_v1",
) -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_hybrid_snapshot_hydration",
            "hydration_version": "ml-fresh-hybrid-snapshot-hydration-v1",
            "snapshot_version": SNAPSHOT,
        },
        "hydration_result": {
            "status": "succeeded",
            "works_considered_count": 2,
            "embedding_ready_count": 2 if ready else 1,
            "embedding_blocked_count": 0 if ready else 1,
            "snapshot_embedding_ready": ready,
            "recommended_next_stage": next_stage,
        },
        "sql_write_report": {
            "ranking_runs_written": False,
            "paper_scores_written": False,
            "production_tables_modified": False,
        },
    }


def _ingest_payload() -> dict:
    return {
        "metadata": {
            "artifact_type": "ml_fresh_hybrid_candidate_plan_ingest",
            "ingest_version": "ml-fresh-hybrid-candidate-plan-ingest-v1",
        },
        "snapshot": {"source_snapshot_version": SNAPSHOT},
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


def _paths(tmp_path: Path, *, hydration: dict | None = None) -> dict[str, Path]:
    return {
        "fresh_hybrid_snapshot_hydration_path": _write_json(
            tmp_path,
            "hydration.json",
            hydration or _hydration_payload(),
        ),
        "fresh_hybrid_candidate_plan_ingest_path": _write_json(tmp_path, "ingest.json", _ingest_payload()),
        "fresh_surface_policy_path": _write_json(tmp_path, "policy.json", _policy_payload()),
    }


def _build(tmp_path: Path, conn: _FakeConn, **kwargs: object) -> dict:
    with (
        patch.object(embed_mod.psycopg, "connect", return_value=conn),
        patch.object(corpus_embed.psycopg, "connect", return_value=conn),
    ):
        return build_ml_fresh_hybrid_snapshot_embeddings_payload(
            **_paths(tmp_path),
            embedding_version=EMBEDDING_VERSION,
            database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
            repo_root=tmp_path,
            **kwargs,
        )


def test_happy_path_with_mock_embeddings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENAI_API_KEY_ENV, raising=False)
    conn = _FakeConn()

    payload = _build(tmp_path, conn, mock_embeddings=True)

    assert payload["metadata"]["artifact_type"] == "ml_fresh_hybrid_snapshot_embeddings"
    assert payload["metadata"]["artifact_version"] == ARTIFACT_VERSION
    assert payload["embedding_result"]["status"] == "succeeded"
    assert payload["embedding_result"]["works_considered_count"] == 2
    assert payload["embedding_result"]["embeddings_written_count"] == 2
    assert payload["embedding_result"]["embedding_provider"] == "openai"
    assert payload["embedding_result"]["embedding_model"] == "text-embedding-3-small"
    assert payload["embedding_result"]["embedding_dimensions"] == 1536
    assert payload["coverage"] == {
        "snapshot_work_count": 2,
        "embedded_work_count": 2,
        "missing_embedding_count": 0,
    }
    assert payload["embedding_result"]["full_snapshot_embedding_coverage"] is True
    assert payload["sql_write_report"]["ranking_runs_written"] is False
    assert payload["sql_write_report"]["paper_scores_written"] is False
    assert payload["shadow_and_production_blockers"]["shadow_scoring_allowed"] is False
    assert set(conn.embeddings) == {(1, EMBEDDING_VERSION), (2, EMBEDDING_VERSION)}


def test_rejects_hydration_when_snapshot_not_ready(tmp_path: Path) -> None:
    with pytest.raises(MLFreshHybridSnapshotEmbeddingsError, match="snapshot_embedding_ready"):
        build_ml_fresh_hybrid_snapshot_embeddings_payload(
            **_paths(tmp_path, hydration=_hydration_payload(ready=False)),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_wrong_hydration_next_stage(tmp_path: Path) -> None:
    with pytest.raises(MLFreshHybridSnapshotEmbeddingsError, match="recommended_next_stage"):
        build_ml_fresh_hybrid_snapshot_embeddings_payload(
            **_paths(tmp_path, hydration=_hydration_payload(next_stage="run_fresh_hybrid_product_candidate_ranking_v1")),
            dry_run=True,
            repo_root=tmp_path,
        )


def test_rejects_hosted_database_url(tmp_path: Path) -> None:
    with pytest.raises(MLFreshHybridSnapshotEmbeddingsError, match="hosted production"):
        build_ml_fresh_hybrid_snapshot_embeddings_payload(
            **_paths(tmp_path),
            database_url="postgresql://user:pass@project.neon.tech/db",
            dry_run=True,
            repo_root=tmp_path,
        )


def test_embeds_only_target_snapshot_works(tmp_path: Path) -> None:
    conn = _FakeConn()
    _build(tmp_path, conn, mock_embeddings=True)

    assert (3, EMBEDDING_VERSION) not in conn.embeddings


def test_dry_run_performs_no_writes(tmp_path: Path) -> None:
    conn = _FakeConn()
    payload = _build(tmp_path, conn, dry_run=True)

    assert payload["metadata"]["dry_run"] is True
    assert payload["embedding_result"]["status"] == "dry_run_validated"
    assert payload["embedding_result"]["embeddings_written_count"] == 0
    assert payload["sql_write_report"]["writes_enabled"] is False
    assert payload["sql_write_report"]["affected_row_counts"] == {}
    assert conn.embeddings == {}


def test_missing_openai_key_fails_without_mock_or_dry_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENAI_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    with (
        patch.object(embed_mod.psycopg, "connect", return_value=conn),
        patch.object(corpus_embed.psycopg, "connect", return_value=conn),
    ):
        with pytest.raises(MLFreshHybridSnapshotEmbeddingsError, match=OPENAI_API_KEY_ENV):
            build_ml_fresh_hybrid_snapshot_embeddings_payload(
                **_paths(tmp_path),
                embedding_version=EMBEDDING_VERSION,
                database_url="postgresql://research_radar:research_radar@localhost:5432/research_radar",
                repo_root=tmp_path,
            )


def test_cli_writes_json_and_markdown_with_mock_embeddings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(OPENAI_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    monkeypatch.setattr(embed_mod.psycopg, "connect", lambda *args, **kwargs: conn)
    monkeypatch.setattr(corpus_embed.psycopg, "connect", lambda *args, **kwargs: conn)
    paths = _paths(tmp_path)
    out_json = tmp_path / "embeddings.json"
    out_md = tmp_path / "embeddings.md"

    import pipeline.cli as cli_main

    argv = [
        "pipeline.cli",
        "ml-fresh-hybrid-snapshot-embed",
        "--fresh-hybrid-snapshot-hydration",
        str(paths["fresh_hybrid_snapshot_hydration_path"]),
        "--fresh-hybrid-candidate-plan-ingest",
        str(paths["fresh_hybrid_candidate_plan_ingest_path"]),
        "--fresh-surface-policy",
        str(paths["fresh_surface_policy_path"]),
        "--mock-embeddings",
        "--embedding-version",
        EMBEDDING_VERSION,
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(tmp_path),
    ]
    with patch.object(sys, "argv", argv):
        cli_main.main()

    assert json.loads(out_json.read_text(encoding="utf-8"))["embedding_result"]["embedding_dimensions"] == 1536
    assert "Not Ranking Yet / Not Shadow / Not Production" in out_md.read_text(encoding="utf-8")


def test_no_sklearn_training_imports() -> None:
    package_root = Path(__file__).resolve().parents[1]
    module_source = (package_root / "pipeline" / "ml_fresh_hybrid_snapshot_embeddings.py").read_text(
        encoding="utf-8"
    ).lower()

    assert "from sklearn" not in module_source
    assert "import sklearn" not in module_source
    assert ".fit(" not in module_source

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.corpus_v2_hydrate_openalex import CorpusV2HydrateError, run_corpus_v2_hydrate_openalex
from pipeline.openalex_client import OPENALEX_API_KEY_ENV


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
        self.snapshots = {"source-snapshot-v2-candidate-plan-20260428"}
        self.ingest_runs: dict[str, dict] = {}
        self.raw_openalex_works: list[dict] = []
        self.works: dict[int, dict] = {
            1: {
                "id": 1,
                "openalex_id": "https://openalex.org/W1",
                "title": "Paper one",
                "abstract": None,
                "type": "unknown",
                "language": "en",
                "doi": None,
                "citation_count": 1,
                "year": 2022,
                "publication_date": None,
                "source_slug": "core",
                "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
                "last_ingest_run_id": "ingest-old",
            },
            2: {
                "id": 2,
                "openalex_id": "https://openalex.org/W2",
                "title": "Paper two",
                "abstract": None,
                "type": "unknown",
                "language": "en",
                "doi": None,
                "citation_count": 0,
                "year": 2023,
                "publication_date": None,
                "source_slug": "core",
                "corpus_snapshot_version": "source-snapshot-v2-candidate-plan-20260428",
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
                "year": 2021,
                "publication_date": None,
                "source_slug": "core",
                "corpus_snapshot_version": "another-snapshot",
                "last_ingest_run_id": "ingest-other",
            },
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
            rows.sort(key=lambda r: r[0])
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
                {
                    "openalex_id": params[0],
                    "ingest_run_id": params[1],
                    "payload": json.loads(params[6]),
                }
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


def _run(tmp_path: Path, conn: _FakeConn, *, fetch_work, mock_openalex: bool) -> dict:
    with patch("pipeline.corpus_v2_hydrate_openalex.psycopg.connect", return_value=conn):
        return run_corpus_v2_hydrate_openalex(
            snapshot_version="source-snapshot-v2-candidate-plan-20260428",
            output_path=tmp_path / "hydration_summary.json",
            markdown_output_path=tmp_path / "hydration_summary.md",
            database_url="postgresql://example",
            fetch_work=fetch_work,
            mock_openalex=mock_openalex,
        )


def test_live_mode_requires_openalex_api_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()

    with pytest.raises(CorpusV2HydrateError, match=OPENALEX_API_KEY_ENV):
        _run(tmp_path, conn, fetch_work=lambda _id: None, mock_openalex=False)


def test_mock_mode_runs_without_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv(OPENALEX_API_KEY_ENV, raising=False)
    conn = _FakeConn()
    summary = _run(tmp_path, conn, fetch_work=lambda _id: None, mock_openalex=True)
    assert summary["auth_mode"] == "mock"
    assert summary["api_key_provided"] is False
    assert summary["fetched_count"] == 0
    assert summary["failed_count"] == 2


def test_abstract_reconstruction_and_metadata_updates(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "oa-secret-never-serialize")
    conn = _FakeConn()

    payloads = {
        "https://openalex.org/W1": {
            "id": "https://openalex.org/W1",
            "title": "Paper one hydrated",
            "type": "article",
            "language": "en",
            "doi": "https://doi.org/10.1000/w1",
            "cited_by_count": 42,
            "publication_year": 2022,
            "publication_date": "2022-03-15",
            "updated_date": "2026-04-28",
            "abstract_inverted_index": {"music": [0], "retrieval": [1], "study": [2]},
        },
        "https://openalex.org/W2": {
            "id": "https://openalex.org/W2",
            "title": "Paper two hydrated",
            "type": "article",
            "language": "fr",
            "doi": "10.1000/w2",
            "cited_by_count": 5,
            "publication_year": 2023,
            "publication_date": "2023-07-01",
            "updated_date": "2026-04-28",
        },
    }
    summary = _run(tmp_path, conn, fetch_work=lambda oid: payloads.get(oid), mock_openalex=False)

    assert summary["works_considered_count"] == 2
    assert summary["fetched_count"] == 2
    assert summary["updated_count"] == 2
    assert summary["failed_count"] == 0
    assert summary["abstract_before_count"] == 0
    assert summary["abstract_after_count"] == 1
    assert summary["abstract_added_count"] == 1
    assert summary["type_unknown_before_count"] == 2
    assert summary["type_unknown_after_count"] == 0
    assert summary["type_resolved_count"] == 2
    assert summary["language_defaulted_before_count"] == 2
    assert summary["language_resolved_count"] == 1
    assert summary["doi_added_count"] == 2
    assert summary["raw_payload_upserted_count"] == 2
    assert conn.works[1]["abstract"] == "music retrieval study"
    assert summary["embedding_ready_count"] == 1
    assert summary["embedding_blocked_count"] == 1
    assert summary["snapshot_embedding_ready"] is False


def test_missing_abstract_remains_blocked(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "x")
    conn = _FakeConn()
    payload = {
        "id": "https://openalex.org/W1",
        "title": "No abstract",
        "type": "article",
        "language": "en",
    }
    summary = _run(tmp_path, conn, fetch_work=lambda _oid: payload, mock_openalex=False)
    assert summary["abstract_after_count"] == 0
    assert summary["embedding_blocked_count"] == 2


def test_no_secret_serialization(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    secret = "oa-secret-never-serialize-123"
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, secret)
    conn = _FakeConn()
    summary = _run(tmp_path, conn, fetch_work=lambda _oid: None, mock_openalex=True)
    blob = json.dumps(summary) + (tmp_path / "hydration_summary.md").read_text(encoding="utf-8")
    for run in conn.ingest_runs.values():
        blob += json.dumps(run)
    assert secret not in blob


def test_snapshot_version_filtering_and_no_other_tables(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(OPENALEX_API_KEY_ENV, "x")
    conn = _FakeConn()
    payload = {
        "id": "https://openalex.org/W1",
        "title": "Hydrated",
        "type": "article",
        "language": "en",
        "abstract_inverted_index": {"a": [0]},
    }
    _run(tmp_path, conn, fetch_work=lambda _oid: payload, mock_openalex=False)
    assert conn.works[3]["corpus_snapshot_version"] == "another-snapshot"
    assert conn.works[3]["last_ingest_run_id"] == "ingest-other"
    executed = "\n".join(conn.sql).casefold()
    assert "embeddings" not in executed
    assert "clustering_runs" not in executed
    assert "ranking_runs" not in executed
    assert "paper_scores" not in executed
    assert "bridge_weight" not in executed

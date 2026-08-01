"""Regression tests for bootstrap_loader snapshot membership behavior."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pipeline.bootstrap_loader import _upsert_work, persist_work, run_bootstrap_ingest
from pipeline.config import IngestRun, SourceSnapshotVersion
from pipeline.policy import CorpusPolicy


@dataclass
class _FakeWork:
    openalex_id: str
    title: str
    abstract: str | None
    year: int
    doi: str | None
    work_type: str
    language: str
    publication_date: str | None
    updated_date: str | None
    citation_count: int
    is_core_corpus: bool
    inclusion_status: str
    exclusion_reason: str | None
    source_openalex_id: str | None = None
    source_display_name: str | None = None


class _FakeConn:
    def __init__(self) -> None:
        self.next_work_id = 1
        self.works: dict[int, dict] = {}
        self.works_by_openalex: dict[str, int] = {}
        self.memberships: list[dict] = []
        self.sql: list[str] = []

    def execute(self, sql: str, params: tuple | None = None):
        params = params or ()
        compact = " ".join(sql.split())
        self.sql.append(compact)
        result = MagicMock()

        if compact.startswith("INSERT INTO works"):
            openalex_id = str(params[0])
            existing_id = self.works_by_openalex.get(openalex_id)
            if existing_id is not None:
                work = self.works[existing_id]
                work.update(
                    {
                        "title": params[1],
                        "abstract": params[2],
                        "year": params[3],
                        "doi": params[4],
                        "type": params[5],
                        "language": params[6],
                        "publication_date": params[7],
                        "updated_date": params[8],
                        "venue_id": params[9],
                        "source_slug": params[10],
                        "citation_count": params[11],
                        "is_core_corpus": params[12],
                        "inclusion_status": params[13],
                        "exclusion_reason": params[14],
                        "raw_content_hash": params[15],
                        "last_ingest_run_id": params[17],
                    }
                )
                result.fetchone.return_value = (existing_id,)
                return result

            work_id = self.next_work_id
            self.next_work_id += 1
            self.works[work_id] = {
                "openalex_id": openalex_id,
                "title": params[1],
                "abstract": params[2],
                "year": params[3],
                "doi": params[4],
                "type": params[5],
                "language": params[6],
                "publication_date": params[7],
                "updated_date": params[8],
                "venue_id": params[9],
                "source_slug": params[10],
                "citation_count": params[11],
                "is_core_corpus": params[12],
                "inclusion_status": params[13],
                "exclusion_reason": params[14],
                "raw_content_hash": params[15],
                "corpus_snapshot_version": params[16],
                "last_ingest_run_id": params[17],
            }
            self.works_by_openalex[openalex_id] = work_id
            result.fetchone.return_value = (work_id,)
            return result

        if compact.startswith("INSERT INTO work_source_snapshot_memberships"):
            self.memberships.append(
                {
                    "work_id": int(params[0]),
                    "source_snapshot_version": str(params[1]),
                    "inclusion_status": str(params[2]),
                    "source_slug": params[3],
                    "added_by_ingest_run_id": params[4],
                }
            )
            return result

        if compact.startswith("INSERT INTO raw_openalex_works"):
            return result
        if compact.startswith("DELETE FROM work_authors"):
            return result
        if compact.startswith("DELETE FROM work_topics"):
            return result

        raise AssertionError(f"unhandled SQL: {compact}")


def test_bootstrap_preflight_failure_persists_only_exception_type(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret_dsn = "postgresql://admin:super-secret@db.example.test/research"
    persisted: dict[str, str] = {}

    class SourceResolutionError(RuntimeError):
        pass

    def _fail_resolution(*args, **kwargs):
        raise SourceResolutionError(secret_dsn)

    monkeypatch.setattr("pipeline.bootstrap_loader.resolve_all_sources", _fail_resolution)
    monkeypatch.setattr(
        "pipeline.bootstrap_loader.write_bootstrap_preflight_failure",
        lambda output_dir, *, stage, message: persisted.update(stage=stage, message=message),
    )

    with pytest.raises(SourceResolutionError):
        run_bootstrap_ingest(
            policy=CorpusPolicy(),
            output_dir=tmp_path / "out",
            raw_root=tmp_path / "raw",
            note="test",
            database_url=secret_dsn,
        )

    assert persisted == {
        "stage": "source_resolution",
        "message": "SourceResolutionError: details redacted",
    }
    assert secret_dsn not in str(persisted)


def test_upsert_work_preserves_canonical_snapshot_on_openalex_conflict() -> None:
    conn = _FakeConn()
    conn.works[7] = {
        "openalex_id": "https://openalex.org/W7103890285",
        "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
        "title": "Pinned title",
    }
    conn.works_by_openalex["https://openalex.org/W7103890285"] = 7
    work = _FakeWork(
        openalex_id="https://openalex.org/W7103890285",
        title="Updated title",
        abstract="Abstract",
        year=2026,
        doi=None,
        work_type="article",
        language="en",
        publication_date=None,
        updated_date=None,
        citation_count=1,
        is_core_corpus=True,
        inclusion_status="included",
        exclusion_reason=None,
    )

    work_id = _upsert_work(
        conn,
        nw=work,
        venue_id=None,
        source_slug="ismir",
        raw_content_hash="abc123",
        corpus_snapshot_version="source-snapshot-ismir-expansion-v1-20260613",
        last_ingest_run_id="ingest-new",
    )

    assert work_id == 7
    assert conn.works[7]["corpus_snapshot_version"] == "source-snapshot-shadow-generalization-v1-20260521"
    assert conn.works[7]["title"] == "Updated title"
    assert "corpus_snapshot_version = EXCLUDED.corpus_snapshot_version" not in " ".join(conn.sql)


def test_persist_work_adds_membership_without_moving_canonical_snapshot(monkeypatch) -> None:
    conn = _FakeConn()
    pinned_snapshot = "source-snapshot-shadow-generalization-v1-20260521"
    new_snapshot = "source-snapshot-ismir-expansion-v1-20260613"
    conn.works[11] = {
        "openalex_id": "https://openalex.org/W7103890285",
        "corpus_snapshot_version": pinned_snapshot,
    }
    conn.works_by_openalex["https://openalex.org/W7103890285"] = 11

    hydrated = SimpleNamespace(
        work=_FakeWork(
            openalex_id="https://openalex.org/W7103890285",
            title="GuitarFlow",
            abstract="Neural guitar synthesis",
            year=2026,
            doi=None,
            work_type="article",
            language="en",
            publication_date=None,
            updated_date=None,
            citation_count=0,
            is_core_corpus=True,
            inclusion_status="included",
            exclusion_reason=None,
        ),
        authors=[],
        topics=[],
        citations=[],
        policy_decision=SimpleNamespace(venue_class="core"),
    )
    monkeypatch.setattr(
        "pipeline.bootstrap_loader.hydrate_work_record",
        lambda _work, _policy: hydrated,
    )

    snapshot = SourceSnapshotVersion(
        source_snapshot_version=new_snapshot,
        policy_name="research-radar-v1",
        policy_hash="abc123",
        ingest_mode="api-bootstrap",
        note="test",
        created_at=datetime.now(UTC),
    )
    ingest_run = IngestRun(
        ingest_run_id="ingest-test",
        source_snapshot_version=new_snapshot,
        policy_hash="abc123",
        status="running",
        started_at=datetime.now(UTC),
        config={},
    )

    persist_work(
        conn,
        policy=CorpusPolicy(),
        work={"id": "https://openalex.org/W7103890285", "title": "GuitarFlow"},
        snapshot=snapshot,
        ingest_run=ingest_run,
        source_slug_hint="ismir",
        page_cursor="*",
        citation_edges=set(),
        decisions=[],
        venue_ids_seen=set(),
        author_ids_seen=set(),
        topic_ids_seen=set(),
    )

    assert conn.works[11]["corpus_snapshot_version"] == pinned_snapshot
    assert conn.memberships == [
        {
            "work_id": 11,
            "source_snapshot_version": new_snapshot,
            "inclusion_status": "included",
            "source_slug": None,
            "added_by_ingest_run_id": "ingest-test",
        }
    ]

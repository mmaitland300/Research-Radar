from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

import pipeline.cli as cli_main
from pipeline.corpus_v2_embed import (
    CorpusV2EmbedError,
    build_corpus_v2_embedding_text,
    run_corpus_v2_embed,
)

SNAPSHOT = "source-snapshot-v2-candidate-plan-20260428"
EMBEDDING_VERSION = "v2-title-abstract-1536-cleantext-r1"


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
                "title": "Paper one",
                "abstract": "Abstract one",
                "type": "article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": SNAPSHOT,
            },
            2: {
                "title": "Paper two",
                "abstract": "Abstract two",
                "type": "proceedings-article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": SNAPSHOT,
            },
            3: {
                "title": "Other snapshot paper",
                "abstract": "Other abstract",
                "type": "article",
                "language": "en",
                "inclusion_status": "included",
                "corpus_snapshot_version": "another-snapshot",
            },
            4: {
                "title": "Excluded paper",
                "abstract": "Excluded abstract",
                "type": "article",
                "language": "en",
                "inclusion_status": "excluded",
                "corpus_snapshot_version": SNAPSHOT,
            },
        }
        self.embeddings: dict[tuple[int, str], str] = {}
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
        if compact.startswith("SELECT id, title, abstract, type, language FROM works"):
            snapshot = str(params[0])
            rows = []
            for work_id, work in self.works.items():
                if (
                    work["corpus_snapshot_version"] == snapshot
                    and work["inclusion_status"] == "included"
                ):
                    rows.append(
                        (
                            work_id,
                            work["title"],
                            work["abstract"],
                            work["type"],
                            work["language"],
                        )
                    )
            rows.sort(key=lambda row: row[0])
            return _Result(all_rows=rows)
        if compact == "SELECT COUNT(*) FROM embeddings WHERE embedding_version = %s":
            version = str(params[0])
            return _Result(one=(sum(1 for (_work_id, ev) in self.embeddings if ev == version),))
        if compact.startswith("SELECT COUNT(*) FROM embeddings e JOIN works w ON w.id = e.work_id"):
            version = str(params[0])
            snapshot = str(params[1])
            count = sum(
                1
                for (work_id, ev) in self.embeddings
                if ev == version and self.works[work_id]["corpus_snapshot_version"] == snapshot
            )
            return _Result(one=(count,))
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


class _FakeProvider:
    expected_dimensions = 3

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[float(index), float(index) + 0.1, float(index) + 0.2] for index, _ in enumerate(texts)]


def _run(tmp_path: Path, conn: _FakeConn, provider: _FakeProvider | None = None, *, replace: bool = False) -> dict:
    active_provider = provider or _FakeProvider()
    with patch("pipeline.corpus_v2_embed.psycopg.connect", return_value=conn):
        return run_corpus_v2_embed(
            snapshot_version=SNAPSHOT,
            embedding_version=EMBEDDING_VERSION,
            output_path=tmp_path / "embedding_summary.json",
            markdown_output_path=tmp_path / "embedding_summary.md",
            database_url="postgresql://example",
            model="test-model",
            batch_size=2,
            replace=replace,
            provider=active_provider,
        )


def test_cli_requires_explicit_snapshot_version(tmp_path: Path) -> None:
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "corpus-v2-embed",
            "--embedding-version",
            EMBEDDING_VERSION,
            "--output",
            str(tmp_path / "out.json"),
            "--markdown-output",
            str(tmp_path / "out.md"),
        ],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main()
    assert exc.value.code == 2


def test_cli_requires_explicit_embedding_version(tmp_path: Path) -> None:
    with patch.object(
        sys,
        "argv",
        [
            "pipeline.cli",
            "corpus-v2-embed",
            "--snapshot-version",
            SNAPSHOT,
            "--output",
            str(tmp_path / "out.json"),
            "--markdown-output",
            str(tmp_path / "out.md"),
        ],
    ):
        with pytest.raises(SystemExit) as exc:
            cli_main.main()
    assert exc.value.code == 2


def test_clean_embedding_text_requires_title_and_abstract() -> None:
    assert build_corpus_v2_embedding_text(" A title ", " An abstract ") == "Title: A title\n\nAbstract: An abstract"
    with pytest.raises(ValueError, match="abstract"):
        build_corpus_v2_embedding_text("A title", "   ")


def test_missing_abstract_blocks_embedding(tmp_path: Path) -> None:
    conn = _FakeConn()
    conn.works[2]["abstract"] = None
    provider = _FakeProvider()

    with pytest.raises(CorpusV2EmbedError, match="missing_abstract_count=1"):
        _run(tmp_path, conn, provider)

    assert provider.calls == []
    assert conn.embeddings == {}


def test_unknown_type_blocks_embedding(tmp_path: Path) -> None:
    conn = _FakeConn()
    conn.works[1]["type"] = "unknown"

    with pytest.raises(CorpusV2EmbedError, match="unknown_type_count=1"):
        _run(tmp_path, conn)

    assert conn.embeddings == {}


def test_old_embedding_version_is_not_reused(tmp_path: Path) -> None:
    conn = _FakeConn()
    with patch("pipeline.corpus_v2_embed.psycopg.connect", return_value=conn):
        with pytest.raises(CorpusV2EmbedError, match="v1 embedding_version"):
            run_corpus_v2_embed(
                snapshot_version=SNAPSHOT,
                embedding_version="v1-title-abstract-1536",
                output_path=tmp_path / "out.json",
                markdown_output_path=tmp_path / "out.md",
                database_url="postgresql://example",
                provider=_FakeProvider(),
            )


def test_duplicate_embedding_version_fails_unless_replace(tmp_path: Path) -> None:
    conn = _FakeConn()
    conn.embeddings[(1, EMBEDDING_VERSION)] = "[9.0,9.0,9.0]"

    with pytest.raises(CorpusV2EmbedError, match="already exists"):
        _run(tmp_path, conn)

    summary = _run(tmp_path, conn, replace=True)

    assert summary["replaced_existing_count"] == 1
    assert summary["embedded_count"] == 2
    assert conn.embeddings[(1, EMBEDDING_VERSION)] != "[9.0,9.0,9.0]"
    assert conn.embeddings[(2, EMBEDDING_VERSION)]


def test_coverage_summary_counts_artifacts_and_target_snapshot_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    secret = "sk-test-secret-never-serialize"
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    conn = _FakeConn()
    provider = _FakeProvider()

    summary = _run(tmp_path, conn, provider)

    assert summary["snapshot_version"] == SNAPSHOT
    assert summary["embedding_version"] == EMBEDDING_VERSION
    assert summary["works_considered_count"] == 2
    assert summary["embedding_ready_count"] == 2
    assert summary["embedded_count"] == 2
    assert summary["skipped_count"] == 0
    assert summary["failed_count"] == 0
    assert summary["embedding_dimension"] == 3
    assert summary["model"] == "test-model"
    assert summary["provider"] == "openai"
    assert summary["text_source"] == "title_abstract"
    assert summary["input_text_count"] == 2
    assert len(summary["input_text_sha256"]) == 64
    assert set(conn.embeddings) == {(1, EMBEDDING_VERSION), (2, EMBEDDING_VERSION)}
    assert provider.calls == [
        [
            "Title: Paper one\n\nAbstract: Abstract one",
            "Title: Paper two\n\nAbstract: Abstract two",
        ]
    ]
    blob = (tmp_path / "embedding_summary.json").read_text(encoding="utf-8")
    blob += (tmp_path / "embedding_summary.md").read_text(encoding="utf-8")
    assert secret not in blob
    assert "This is not clustering." in blob
    assert "This is not ranking." in blob
    assert "This is not bridge validation." in blob
    assert "Old/new corpus metrics are not same-pool comparable." in blob


def test_no_clustering_ranking_or_paper_scores_writes(tmp_path: Path) -> None:
    conn = _FakeConn()

    _run(tmp_path, conn)

    executed = "\n".join(conn.sql).casefold()
    assert "insert into embeddings" in executed
    assert "clustering_runs" not in executed
    assert "insert into clusters" not in executed
    assert "ranking_runs" not in executed
    assert "paper_scores" not in executed

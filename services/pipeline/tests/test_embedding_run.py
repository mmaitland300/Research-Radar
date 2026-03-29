from __future__ import annotations

from pipeline.embedding_persistence import EmbeddingCandidate
from pipeline.embedding_run import build_work_embedding_text, execute_embedding_run


class _FakeProvider:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[float(index), float(index) + 0.5, float(index) + 1.0] for index, _ in enumerate(texts)]


class _FakeConn:
    def __init__(self) -> None:
        self.commit_calls = 0

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def commit(self) -> None:
        self.commit_calls += 1


def test_build_work_embedding_text_includes_title_and_abstract() -> None:
    text = build_work_embedding_text("A paper", "Interesting abstract")
    assert text == "Title: A paper\n\nAbstract: Interesting abstract"


def test_build_work_embedding_text_omits_blank_abstract() -> None:
    text = build_work_embedding_text("A paper", "   ")
    assert text == "Title: A paper"


def test_execute_embedding_run_batches_missing_candidates(monkeypatch) -> None:
    read_conn = _FakeConn()
    write_conn = _FakeConn()
    provider = _FakeProvider()
    written_batches: list[list[tuple[int, list[float]]]] = []
    connections = iter([read_conn, write_conn])

    monkeypatch.setattr(
        "pipeline.embedding_run.psycopg.connect",
        lambda *args, **kwargs: next(connections),
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.latest_corpus_snapshot_version_with_works",
        lambda conn: "source-snapshot-1",
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.count_included_works_for_snapshot",
        lambda conn, snapshot: 4,
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.count_missing_embedding_candidates",
        lambda conn, corpus_snapshot_version, embedding_version: 3,
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.list_embedding_candidates",
        lambda conn, corpus_snapshot_version, embedding_version, limit: [
            EmbeddingCandidate(work_id=1, title="One", abstract="Alpha"),
            EmbeddingCandidate(work_id=2, title="Two", abstract=None),
            EmbeddingCandidate(work_id=3, title="Three", abstract="Gamma"),
        ],
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.upsert_work_embeddings",
        lambda conn, embedding_version, rows: written_batches.append(
            [(work_id, list(vector)) for work_id, vector in rows]
        ),
    )

    summary = execute_embedding_run(
        database_url="postgresql://example",
        embedding_version="v1-title-abstract-1536",
        model="test-model",
        batch_size=2,
        provider=provider,
    )

    assert summary.corpus_snapshot_version == "source-snapshot-1"
    assert summary.embedding_version == "v1-title-abstract-1536"
    assert summary.model == "test-model"
    assert summary.total_included_works == 4
    assert summary.already_embedded_works == 1
    assert summary.missing_embedding_works == 3
    assert summary.candidate_works == 3
    assert summary.rows_written == 3
    assert summary.batch_count == 2
    assert provider.calls == [
        ["Title: One\n\nAbstract: Alpha", "Title: Two"],
        ["Title: Three\n\nAbstract: Gamma"],
    ]
    assert written_batches == [
        [(1, [0.0, 0.5, 1.0]), (2, [1.0, 1.5, 2.0])],
        [(3, [0.0, 0.5, 1.0])],
    ]
    assert write_conn.commit_calls == 2


def test_execute_embedding_run_returns_empty_summary_when_all_rows_exist(monkeypatch) -> None:
    read_conn = _FakeConn()

    monkeypatch.setattr("pipeline.embedding_run.psycopg.connect", lambda *args, **kwargs: read_conn)
    monkeypatch.setattr(
        "pipeline.embedding_run.latest_corpus_snapshot_version_with_works",
        lambda conn: "source-snapshot-1",
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.count_included_works_for_snapshot",
        lambda conn, snapshot: 2,
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.count_missing_embedding_candidates",
        lambda conn, corpus_snapshot_version, embedding_version: 0,
    )
    monkeypatch.setattr(
        "pipeline.embedding_run.list_embedding_candidates",
        lambda conn, corpus_snapshot_version, embedding_version, limit: [],
    )

    summary = execute_embedding_run(
        database_url="postgresql://example",
        embedding_version="v1-title-abstract-1536",
        provider=_FakeProvider(),
    )

    assert summary.already_embedded_works == 2
    assert summary.rows_written == 0
    assert summary.batch_count == 0

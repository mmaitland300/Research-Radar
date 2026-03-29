from unittest.mock import MagicMock

import app.similarity_repo as similarity_repo
from app.similarity_repo import list_similar_papers


def test_list_similar_papers_returns_none_when_source_missing_embedding(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchone.return_value = None

    monkeypatch.setattr(similarity_repo.psycopg, "connect", lambda *a, **k: conn)

    assert (
        list_similar_papers(
            paper_id="https://openalex.org/W999",
            embedding_version="v1",
            limit=5,
        )
        is None
    )

    call = conn.execute.call_args
    assert "embeddings" in call[0][0]
    assert call[0][1] == ("v1", "https://openalex.org/W999")


def test_list_similar_papers_orders_by_similarity_and_maps_topics(monkeypatch) -> None:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    neighbor_rows = [
        {
            "openalex_id": "https://openalex.org/W2",
            "title": "B",
            "year": 2023,
            "citation_count": 1,
            "source_slug": "ismir",
            "topics": ["Topic A"],
            "similarity": 0.91,
        },
        {
            "openalex_id": "https://openalex.org/W3",
            "title": "C",
            "year": 2022,
            "citation_count": 2,
            "source_slug": None,
            "topics": ["Topic B", "Topic C"],
            "similarity": 0.88,
        },
    ]

    exec_mocks = iter(
        [
            MagicMock(fetchone=MagicMock(return_value={"openalex_id": "https://openalex.org/W1"})),
            MagicMock(fetchall=MagicMock(return_value=neighbor_rows)),
        ]
    )

    def fake_execute(_sql: str, _params=None):
        return next(exec_mocks)

    conn.execute.side_effect = fake_execute

    monkeypatch.setattr(similarity_repo.psycopg, "connect", lambda *a, **k: conn)

    result = list_similar_papers(
        paper_id="https://openalex.org/W1",
        embedding_version="v1-title-abstract-1536",
        limit=10,
    )

    assert result is not None
    assert result.paper_id == "https://openalex.org/W1"
    assert result.embedding_version == "v1-title-abstract-1536"
    assert len(result.items) == 2
    assert result.items[0].paper_id == "https://openalex.org/W2"
    assert result.items[0].similarity == 0.91
    assert result.items[0].topics == ["Topic A"]
    assert result.items[1].similarity == 0.88

    neighbor_call = conn.execute.call_args_list[1]
    sql = neighbor_call[0][0]
    assert "WITH target AS" in sql
    assert "ORDER BY e2.vector <=> t.vector ASC" in sql
    assert "w2.id <> t.id" in sql
    assert neighbor_call[0][1] == (
        "v1-title-abstract-1536",
        "https://openalex.org/W1",
        "v1-title-abstract-1536",
        10,
    )

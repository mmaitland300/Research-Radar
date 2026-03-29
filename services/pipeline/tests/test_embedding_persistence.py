from unittest.mock import MagicMock

from pipeline.embedding_persistence import (
    _vector_literal,
    count_missing_embedding_candidates,
    list_embedding_candidates,
    upsert_work_embeddings,
)


def test_list_embedding_candidates_filters_missing_embeddings_only() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [
        (1, "Paper one", "Abstract one"),
        (2, "Paper two", None),
    ]

    rows = list_embedding_candidates(
        conn,
        corpus_snapshot_version="snap-1",
        embedding_version="v1",
        limit=5,
    )

    assert [row.work_id for row in rows] == [1, 2]
    sql = conn.execute.call_args[0][0]
    params = conn.execute.call_args[0][1]
    assert "LEFT JOIN embeddings" in sql
    assert "e.work_id IS NULL" in sql
    assert params == ("v1", "snap-1", 5)


def test_count_missing_embedding_candidates_reads_scalar_count() -> None:
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (7,)

    count = count_missing_embedding_candidates(
        conn,
        corpus_snapshot_version="snap-1",
        embedding_version="v1",
    )

    assert count == 7


def test_upsert_work_embeddings_uses_vector_cast_and_version_key() -> None:
    conn = MagicMock()

    upsert_work_embeddings(
        conn,
        embedding_version="v1",
        rows=[(10, [0.1, 0.2, 0.3])],
    )

    sql = conn.execute.call_args[0][0]
    params = conn.execute.call_args[0][1]
    assert "INSERT INTO embeddings" in sql
    assert "::vector" in sql
    assert params[0] == 10
    assert params[1] == "v1"
    assert params[2] == "[0.1,0.2,0.3]"


def test_vector_literal_rejects_non_finite_values() -> None:
    try:
        _vector_literal([0.1, float("nan")])
    except ValueError as exc:
        assert "finite numbers" in str(exc)
    else:
        raise AssertionError("expected ValueError")

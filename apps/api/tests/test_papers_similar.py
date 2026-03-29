from fastapi.testclient import TestClient

from app import main
from app.similarity_repo import SimilarPaperRow, SimilarPapersResult


client = TestClient(main.app)


def test_get_paper_similar_smoke(monkeypatch) -> None:
    def fake_list_similar(*, paper_id: str, embedding_version: str, limit: int):
        assert paper_id == "https://openalex.org/W1"
        assert embedding_version == "v1-title-abstract-1536"
        assert limit == 10
        return SimilarPapersResult(
            paper_id="https://openalex.org/W1",
            embedding_version="v1-title-abstract-1536",
            items=[
                SimilarPaperRow(
                    paper_id="https://openalex.org/W2",
                    title="Neighbor",
                    year=2024,
                    citation_count=3,
                    source_slug="ismir",
                    topics=["mir"],
                    similarity=0.8732,
                )
            ],
        )

    monkeypatch.setattr(main, "list_similar_papers", fake_list_similar)
    response = client.get(
        "/api/v1/papers/https%3A%2F%2Fopenalex.org%2FW1/similar"
        "?embedding_version=v1-title-abstract-1536&limit=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_id"] == "https://openalex.org/W1"
    assert payload["embedding_version"] == "v1-title-abstract-1536"
    assert payload["total"] == 1
    assert payload["items"][0]["paper_id"] == "https://openalex.org/W2"
    assert payload["items"][0]["similarity"] == 0.8732
    assert payload["items"][0]["topics"] == ["mir"]


def test_get_paper_similar_404(monkeypatch) -> None:
    monkeypatch.setattr(main, "list_similar_papers", lambda **kwargs: None)
    response = client.get(
        "/api/v1/papers/https%3A%2F%2Fopenalex.org%2FW404/similar"
        "?embedding_version=v1-title-abstract-1536"
    )

    assert response.status_code == 404


def test_get_paper_similar_503(monkeypatch) -> None:
    def fake_raises(**kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(main, "list_similar_papers", fake_raises)
    response = client.get(
        "/api/v1/papers/https%3A%2F%2Fopenalex.org%2FW1/similar"
        "?embedding_version=v1-title-abstract-1536"
    )

    assert response.status_code == 503


def test_get_paper_similar_requires_embedding_version() -> None:
    response = client.get("/api/v1/papers/https%3A%2F%2Fopenalex.org%2FW1/similar")
    assert response.status_code == 422

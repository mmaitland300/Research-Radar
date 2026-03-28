from fastapi.testclient import TestClient

from app import main
from app.papers_repo import PaperDetailRow, PaperRow


client = TestClient(main.app)


def test_get_papers_smoke(monkeypatch) -> None:
    def fake_list_papers(limit: int, q: str | None = None) -> list[PaperRow]:
        assert limit == 5
        assert q == "audio"
        return [
            PaperRow(
                paper_id="W123",
                title="Audio Representation Learning",
                year=2024,
                citation_count=12,
                source_slug="ismir",
                is_core_corpus=True,
            )
        ]

    monkeypatch.setattr(main, "list_papers", fake_list_papers)
    response = client.get("/api/v1/papers?limit=5&q=audio")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["paper_id"] == "W123"


def test_get_paper_detail_smoke(monkeypatch) -> None:
    def fake_get_paper_detail(paper_id: str) -> PaperDetailRow | None:
        assert paper_id == "W456"
        return PaperDetailRow(
            paper_id="W456",
            title="Bridge Papers in MIR",
            abstract="A concrete abstract.",
            venue="International Society for Music Information Retrieval Conference",
            year=2023,
            citation_count=7,
            source_slug="ismir",
            is_core_corpus=True,
            authors=["Ada Lovelace", "Grace Hopper"],
            topics=["music information retrieval", "audio embeddings"],
        )

    monkeypatch.setattr(main, "get_paper_detail_row", fake_get_paper_detail)
    response = client.get("/api/v1/papers/W456")

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_id"] == "W456"
    assert payload["authors"] == ["Ada Lovelace", "Grace Hopper"]
    assert payload["topics"][0] == "music information retrieval"


def test_get_paper_detail_not_found(monkeypatch) -> None:
    monkeypatch.setattr(main, "get_paper_detail_row", lambda _paper_id: None)
    response = client.get("/api/v1/papers/W999")
    assert response.status_code == 404


def test_get_paper_detail_accepts_openalex_url_id(monkeypatch) -> None:
    expected_id = "https://openalex.org/W456"

    def fake_get_paper_detail(paper_id: str) -> PaperDetailRow | None:
        assert paper_id == expected_id
        return PaperDetailRow(
            paper_id=expected_id,
            title="Bridge Papers in MIR",
            abstract="A concrete abstract.",
            venue="International Society for Music Information Retrieval Conference",
            year=2023,
            citation_count=7,
            source_slug="ismir",
            is_core_corpus=True,
            authors=["Ada Lovelace", "Grace Hopper"],
            topics=["music information retrieval", "audio embeddings"],
        )

    monkeypatch.setattr(main, "get_paper_detail_row", fake_get_paper_detail)
    response = client.get("/api/v1/papers/https%3A%2F%2Fopenalex.org%2FW456")

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_id"] == expected_id

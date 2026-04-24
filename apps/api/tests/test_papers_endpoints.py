from fastapi.testclient import TestClient

from app import main
from app.papers_repo import PaperDetailRow, PaperRow
from app.scores_repo import PaperRankingFamilyRow, RankedRunContext


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
                source_label="Proc. ISMIR",
                is_core_corpus=True,
                topics=["audio representation learning", "self-supervised learning"],
            )
        ]

    monkeypatch.setattr(main, "list_papers", fake_list_papers)
    response = client.get("/api/v1/papers?limit=5&q=audio")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["paper_id"] == "W123"
    assert item["source_label"] == "Proc. ISMIR"
    assert isinstance(item["topics"], list)
    assert item["topics"] == ["audio representation learning", "self-supervised learning"]


def test_get_papers_list_topic_ordering_preserved(monkeypatch) -> None:
    """Repo/query orders topics score DESC, name ASC; API must not reorder."""

    def fake_list_papers(limit: int, q: str | None = None) -> list[PaperRow]:
        return [
            PaperRow(
                paper_id="W1",
                title="T",
                year=2024,
                citation_count=0,
                source_slug="tismir",
                source_label=None,
                is_core_corpus=True,
                topics=["Zebra Topic", "Alpha Topic"],
            )
        ]

    monkeypatch.setattr(main, "list_papers", fake_list_papers)
    response = client.get("/api/v1/papers?limit=5")
    assert response.status_code == 200
    assert response.json()["items"][0]["topics"] == ["Zebra Topic", "Alpha Topic"]


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


def test_get_paper_ranking_smoke(monkeypatch) -> None:
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

    def fake_get_paper_family_rankings(
        *,
        paper_id: str,
        corpus_snapshot_version: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
    ):
        assert paper_id == "W456"
        assert corpus_snapshot_version == "snap-1"
        assert ranking_run_id is None
        assert ranking_version == "v0-test"
        ctx = RankedRunContext(
            ranking_run_id="run-abc",
            ranking_version="v0-test",
            corpus_snapshot_version="snap-1",
        )
        rows = [
            PaperRankingFamilyRow(
                family="emerging",
                rank=14,
                final_score=0.88,
                reason_short="Strong momentum in-slice.",
                semantic_score=None,
                citation_velocity_score=0.7,
                topic_growth_score=0.6,
                bridge_score=None,
                diversity_penalty=0.1,
                bridge_eligible=None,
            ),
            PaperRankingFamilyRow(
                family="bridge",
                rank=61,
                final_score=0.42,
                reason_short="Outside the surfaced bridge slice.",
                semantic_score=0.4,
                citation_velocity_score=0.5,
                topic_growth_score=0.45,
                bridge_score=0.3,
                diversity_penalty=0.2,
                bridge_eligible=False,
            ),
            PaperRankingFamilyRow(
                family="undercited",
                rank=None,
                final_score=None,
                reason_short=None,
                semantic_score=None,
                citation_velocity_score=None,
                topic_growth_score=None,
                bridge_score=None,
                diversity_penalty=None,
                bridge_eligible=None,
            ),
        ]
        return ctx, rows, {}

    monkeypatch.setattr(main, "get_paper_detail_row", fake_get_paper_detail)
    monkeypatch.setattr(main, "get_paper_family_rankings", fake_get_paper_family_rankings)

    response = client.get(
        "/api/v1/papers/W456/ranking?top_n=50&corpus_snapshot_version=snap-1&ranking_version=v0-test"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_id"] == "W456"
    assert payload["ranking_run_id"] == "run-abc"
    assert payload["top_n"] == 50
    assert payload["rank_scope"] == "family_global"
    emerging = payload["families"][0]
    assert emerging["family"] == "emerging"
    assert emerging["present"] is True
    assert emerging["in_top_n"] is True
    assert emerging["rank"] == 14
    assert emerging["signals"]["citation_velocity"] == 0.7
    assert len(emerging["signal_explanations"]) >= 5
    bridge = payload["families"][1]
    assert bridge["family"] == "bridge"
    assert bridge["present"] is True
    assert bridge["in_top_n"] is False
    assert bridge["rank"] is None
    assert bridge["bridge_eligible"] is False
    undercited = payload["families"][2]
    assert undercited["family"] == "undercited"
    assert undercited["present"] is False
    assert undercited["signals"] is None
    assert undercited["signal_explanations"] == []


def test_get_paper_ranking_not_found(monkeypatch) -> None:
    monkeypatch.setattr(main, "get_paper_detail_row", lambda _paper_id: None)
    response = client.get("/api/v1/papers/W999/ranking")
    assert response.status_code == 404
    assert response.json()["detail"] == "Paper not found."


def test_get_paper_ranking_no_succeeded_run(monkeypatch) -> None:
    def fake_get_paper_detail(_paper_id: str) -> PaperDetailRow | None:
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
    monkeypatch.setattr(main, "get_paper_family_rankings", lambda **_kwargs: None)

    response = client.get("/api/v1/papers/W456/ranking")

    assert response.status_code == 404
    assert response.json()["detail"] == "No succeeded ranking run found for the given filters."

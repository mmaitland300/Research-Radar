from fastapi.testclient import TestClient

from app import main
from app.search_repo import SearchResolvedFiltersRow, SearchResponseRow, SearchResultRow


client = TestClient(main.app)


def test_get_search_smoke(monkeypatch) -> None:
    def fake_search_papers(
        *,
        q: str,
        limit: int,
        offset: int,
        year_from: int | None,
        year_to: int | None,
        included_scope: str,
        source_slug: str | None,
        topic: str | None,
        family_hint: str | None,
    ) -> SearchResponseRow:
        assert q == "music retrieval"
        assert limit == 5
        assert offset == 10
        assert year_from == 2020
        assert year_to == 2024
        assert included_scope == "core"
        assert source_slug == "ismir"
        assert topic == "audio embeddings"
        assert family_hint == "emerging"
        return SearchResponseRow(
            total=1,
            ordering="lexical_rank desc, year desc, citation_count desc, work_id asc",
            resolved_filters=SearchResolvedFiltersRow(
                q=q,
                limit=limit,
                offset=offset,
                year_from=year_from,
                year_to=year_to,
                included_scope="core",
                source_slug=source_slug,
                topic=topic,
                family_hint="emerging",
            ),
            items=[
                SearchResultRow(
                    paper_id="W123",
                    title="Music Retrieval with Audio Embeddings",
                    year=2024,
                    citation_count=9,
                    source_slug="ismir",
                    source_label="Proc. ISMIR",
                    is_core_corpus=True,
                    topics=["audio embeddings", "music information retrieval"],
                    preview="This paper studies lexical retrieval over audio papers.",
                    matched_fields=["title", "abstract"],
                    highlight_fragments=["Music [[Retrieval]] with Audio Embeddings"],
                    lexical_rank=0.912345,
                )
            ],
        )

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get(
        "/api/v1/search?q=music%20retrieval&limit=5&offset=10&year_from=2020&year_to=2024&included_scope=core&source_slug=ismir&topic=audio%20embeddings&family_hint=emerging"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["ordering"] == "lexical_rank desc, year desc, citation_count desc, work_id asc"
    assert payload["resolved_filters"]["included_scope"] == "core"
    item = payload["items"][0]
    assert item["paper_id"] == "W123"
    assert item["source_label"] == "Proc. ISMIR"
    assert item["match"]["matched_fields"] == ["title", "abstract"]
    assert item["match"]["highlight_fragments"] == ["Music [[Retrieval]] with Audio Embeddings"]


def test_get_search_invalid_year_range(monkeypatch) -> None:
    def fake_search_papers(**_kwargs) -> SearchResponseRow:
        raise ValueError("year_from must be less than or equal to year_to.")

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get("/api/v1/search?q=audio&year_from=2025&year_to=2020")

    assert response.status_code == 422
    assert response.json()["detail"] == "year_from must be less than or equal to year_to."

from fastapi.testclient import TestClient

from app import main
from app.search_repo import (
    SearchResolvedFiltersRow,
    SearchResponseRow,
    SearchResultRow,
    SearchRunContextNotFoundError,
)


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
        ranking_run_id: str | None,
        ranking_version: str | None,
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
        assert ranking_run_id is None
        assert ranking_version == "semantic-v1"
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
                ranking_run_id=None,
                ranking_version="semantic-v1",
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
            resolved_ranking_run_id="rank-123",
            resolved_ranking_version="semantic-v1",
            resolved_corpus_snapshot_version="snapshot-20260423",
        )

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get(
        "/api/v1/search?q=music%20retrieval&limit=5&offset=10&year_from=2020&year_to=2024&included_scope=core&source_slug=ismir&topic=audio%20embeddings&family_hint=emerging&ranking_version=semantic-v1"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["ordering"] == "lexical_rank desc, year desc, citation_count desc, work_id asc"
    assert payload["resolved_filters"]["included_scope"] == "core"
    assert payload["resolved_filters"]["ranking_version"] == "semantic-v1"
    assert payload["resolved_ranking_run_id"] == "rank-123"
    assert payload["resolved_ranking_version"] == "semantic-v1"
    assert payload["resolved_corpus_snapshot_version"] == "snapshot-20260423"
    item = payload["items"][0]
    assert item["paper_id"] == "W123"
    assert item["source_label"] == "Proc. ISMIR"
    assert item["match"]["matched_fields"] == ["title", "abstract"]
    assert item["match"]["highlight_fragments"] == ["Music [[Retrieval]] with Audio Embeddings"]


def test_get_search_lexical_only_omits_ranking_context(monkeypatch) -> None:
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
        ranking_run_id: str | None,
        ranking_version: str | None,
    ) -> SearchResponseRow:
        assert q == "audio"
        assert family_hint is None
        assert ranking_run_id is None
        assert ranking_version is None
        return SearchResponseRow(
            total=0,
            ordering="lexical_rank desc, year desc, citation_count desc, work_id asc",
            resolved_filters=SearchResolvedFiltersRow(
                q=q,
                limit=limit,
                offset=offset,
                year_from=year_from,
                year_to=year_to,
                included_scope="all_included",
                source_slug=source_slug,
                topic=topic,
                family_hint=None,
                ranking_run_id=None,
                ranking_version=None,
            ),
            items=[],
        )

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get("/api/v1/search?q=audio")

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolved_filters"]["q"] == "audio"
    assert "resolved_ranking_run_id" not in payload
    assert "resolved_ranking_version" not in payload
    assert "resolved_corpus_snapshot_version" not in payload
    assert "ranking_run_id" not in payload["resolved_filters"]
    assert "ranking_version" not in payload["resolved_filters"]


def test_get_search_with_exact_ranking_run_id(monkeypatch) -> None:
    def fake_search_papers(
        *,
        family_hint: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
        **kwargs,
    ) -> SearchResponseRow:
        assert family_hint == "bridge"
        assert ranking_run_id == "rank-explicit"
        assert ranking_version is None
        return SearchResponseRow(
            total=0,
            ordering="lexical_rank desc, year desc, citation_count desc, work_id asc",
            resolved_filters=SearchResolvedFiltersRow(
                q=str(kwargs["q"]),
                limit=int(kwargs["limit"]),
                offset=int(kwargs["offset"]),
                year_from=kwargs["year_from"],
                year_to=kwargs["year_to"],
                included_scope="all_included",
                source_slug=kwargs["source_slug"],
                topic=kwargs["topic"],
                family_hint="bridge",
                ranking_run_id="rank-explicit",
                ranking_version=None,
            ),
            items=[],
            resolved_ranking_run_id="rank-explicit",
            resolved_ranking_version="bridge-v2",
            resolved_corpus_snapshot_version="snapshot-a",
        )

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get("/api/v1/search?q=bridge&family_hint=bridge&ranking_run_id=rank-explicit")

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolved_filters"]["ranking_run_id"] == "rank-explicit"
    assert payload["resolved_ranking_run_id"] == "rank-explicit"
    assert payload["resolved_ranking_version"] == "bridge-v2"


def test_get_search_family_hint_uses_default_run_when_unpinned(monkeypatch) -> None:
    def fake_search_papers(
        *,
        family_hint: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
        **kwargs,
    ) -> SearchResponseRow:
        assert family_hint == "undercited"
        assert ranking_run_id is None
        assert ranking_version is None
        return SearchResponseRow(
            total=0,
            ordering="lexical_rank desc, year desc, citation_count desc, work_id asc",
            resolved_filters=SearchResolvedFiltersRow(
                q=str(kwargs["q"]),
                limit=int(kwargs["limit"]),
                offset=int(kwargs["offset"]),
                year_from=kwargs["year_from"],
                year_to=kwargs["year_to"],
                included_scope="all_included",
                source_slug=kwargs["source_slug"],
                topic=kwargs["topic"],
                family_hint="undercited",
                ranking_run_id=None,
                ranking_version=None,
            ),
            items=[],
            resolved_ranking_run_id="rank-default",
            resolved_ranking_version="semantic-v1",
            resolved_corpus_snapshot_version="snapshot-default",
        )

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get("/api/v1/search?q=signals&family_hint=undercited")

    assert response.status_code == 200
    payload = response.json()
    assert payload["resolved_ranking_run_id"] == "rank-default"
    assert payload["resolved_ranking_version"] == "semantic-v1"
    assert payload["resolved_corpus_snapshot_version"] == "snapshot-default"


def test_get_search_invalid_year_range(monkeypatch) -> None:
    def fake_search_papers(**_kwargs) -> SearchResponseRow:
        raise ValueError("year_from must be less than or equal to year_to.")

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get("/api/v1/search?q=audio&year_from=2025&year_to=2020")

    assert response.status_code == 422
    assert response.json()["detail"] == "year_from must be less than or equal to year_to."


def test_get_search_missing_ranking_context_returns_404(monkeypatch) -> None:
    def fake_search_papers(**_kwargs) -> SearchResponseRow:
        raise SearchRunContextNotFoundError("No succeeded ranking run found for the given search filters.")

    monkeypatch.setattr(main, "search_papers", fake_search_papers)
    response = client.get("/api/v1/search?q=audio&family_hint=emerging&ranking_version=missing-run")

    assert response.status_code == 404
    assert response.json()["detail"] == "No succeeded ranking run found for the given search filters."

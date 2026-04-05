from fastapi.testclient import TestClient

from app import main
from app.scores_repo import RankedRecommendationRow, RankedRunContext


client = TestClient(main.app)


def test_get_recommendations_ranked_smoke(monkeypatch) -> None:
    def fake_list_ranked_recommendations(
        *,
        family: str,
        limit: int,
        corpus_snapshot_version: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
    ):
        assert family == "undercited"
        assert limit == 10
        assert corpus_snapshot_version == "snap-1"
        assert ranking_run_id is None
        assert ranking_version == "v0-test"
        ctx = RankedRunContext(
            ranking_run_id="run-abc",
            ranking_version="v0-test",
            corpus_snapshot_version="snap-1",
        )
        rows = [
            RankedRecommendationRow(
                paper_id="W999",
                title="Ranked Paper",
                year=2022,
                citation_count=5,
                source_slug="ismir",
                topics=["mir"],
                semantic_score=None,
                citation_velocity_score=0.7,
                topic_growth_score=0.6,
                bridge_score=None,
                diversity_penalty=0.1,
                final_score=0.88,
                reason_short="undercited heuristic",
            )
        ]
        return ctx, rows

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_list_ranked_recommendations)
    response = client.get(
        "/api/v1/recommendations/ranked?family=undercited&limit=10"
        "&corpus_snapshot_version=snap-1&ranking_version=v0-test"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ranking_run_id"] == "run-abc"
    assert payload["ranking_version"] == "v0-test"
    assert payload["corpus_snapshot_version"] == "snap-1"
    assert payload["family"] == "undercited"
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["paper_id"] == "W999"
    assert item["signals"]["semantic"] is None
    assert "bridge" in item["signals"]
    assert item["signals"]["bridge"] is None
    assert item["signals"]["citation_velocity"] == 0.7
    assert item["final_score"] == 0.88


def test_get_recommendations_ranked_not_found(monkeypatch) -> None:
    def fake_returns_none(**_kwargs):
        return None

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_returns_none)
    response = client.get("/api/v1/recommendations/ranked?family=emerging")

    assert response.status_code == 404


def test_get_recommendations_ranked_db_error(monkeypatch) -> None:
    def fake_raises(**_kwargs):
        raise RuntimeError("db down")

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_raises)
    response = client.get("/api/v1/recommendations/ranked?family=bridge")

    assert response.status_code == 503

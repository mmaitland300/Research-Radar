from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from app.main import app
from app.serving_context import ServingContextNotFoundError, ServingContextUnavailableError
from app.trends_repo import TopicTrendRow, TopicTrendsResult


def sample_row() -> TopicTrendRow:
    return TopicTrendRow(
        topic_id=101,
        topic_name="Music and Audio Processing",
        total_works=12,
        recent_works=8,
        prior_works=4,
        delta=4,
        growth_label="rising",
    )


def test_get_topic_trends_smoke(monkeypatch) -> None:
    list_trends = MagicMock(
        return_value=TopicTrendsResult(
            corpus_snapshot_version="source-snapshot-20260301",
            rows=[sample_row()],
        )
    )
    monkeypatch.setattr("app.routers.trends.list_topic_trends", list_trends)
    client = TestClient(app)
    response = client.get(
        "/api/v1/trends/topics?limit=5&since_year=2025&min_works=2"
        "&ranking_run_id=rank-history&ranking_version=rank-v1"
        "&corpus_snapshot_version=source-snapshot-requested"
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["corpus_snapshot_version"] == "source-snapshot-20260301"
    assert payload["since_year"] == 2025
    assert payload["min_works"] == 2
    assert payload["total"] == 1
    assert payload["items"][0]["topic_name"] == "Music and Audio Processing"
    assert payload["items"][0]["growth_label"] == "rising"
    list_trends.assert_called_once_with(
        limit=5,
        since_year=2025,
        min_works=2,
        corpus_snapshot_version="source-snapshot-requested",
        ranking_run_id="rank-history",
        ranking_version="rank-v1",
    )


def test_get_topic_trends_historical_selector_not_found(monkeypatch) -> None:
    list_trends = MagicMock(
        side_effect=ServingContextNotFoundError(ranking_run_id="rank-missing")
    )
    monkeypatch.setattr("app.routers.trends.list_topic_trends", list_trends)

    client = TestClient(app)
    response = client.get("/api/v1/trends/topics?ranking_run_id=rank-missing")

    assert response.status_code == 404
    assert "rank-missing" in response.json()["detail"]


def test_get_topic_trends_unavailable_active_release(monkeypatch) -> None:
    promotion = MagicMock(promotion_id=7)
    promotion.run.ranking_run_id = "rank-active"
    list_trends = MagicMock(
        side_effect=ServingContextUnavailableError(
            promotion,
            failures=("missing_embeddings",),
        )
    )
    monkeypatch.setattr("app.routers.trends.list_topic_trends", list_trends)

    client = TestClient(app)
    response = client.get("/api/v1/trends/topics")

    assert response.status_code == 503
    assert "missing_embeddings" in response.json()["detail"]


def test_get_topic_trends_503(monkeypatch) -> None:
    def _boom(
        *,
        limit,
        since_year,
        min_works,
        corpus_snapshot_version,
        ranking_run_id,
        ranking_version,
    ):
        raise RuntimeError("db down")

    monkeypatch.setattr("app.routers.trends.list_topic_trends", _boom)
    client = TestClient(app)
    response = client.get("/api/v1/trends/topics")
    assert response.status_code == 503
    assert "topic data exists" in response.json()["detail"]

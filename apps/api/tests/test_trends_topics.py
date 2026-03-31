from fastapi.testclient import TestClient

from app.main import app
from app.trends_repo import TopicTrendRow


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
    monkeypatch.setattr(
        "app.main.list_topic_trends",
        lambda *, limit, since_year, min_works: [sample_row()],
    )
    client = TestClient(app)
    response = client.get("/api/v1/trends/topics?limit=5&since_year=2025&min_works=2")
    assert response.status_code == 200
    payload = response.json()
    assert payload["since_year"] == 2025
    assert payload["min_works"] == 2
    assert payload["total"] == 1
    assert payload["items"][0]["topic_name"] == "Music and Audio Processing"
    assert payload["items"][0]["growth_label"] == "rising"


def test_get_topic_trends_503(monkeypatch) -> None:
    def _boom(*, limit, since_year, min_works):
        raise RuntimeError("db down")

    monkeypatch.setattr("app.main.list_topic_trends", _boom)
    client = TestClient(app)
    response = client.get("/api/v1/trends/topics")
    assert response.status_code == 503
    assert "topic data exists" in response.json()["detail"]

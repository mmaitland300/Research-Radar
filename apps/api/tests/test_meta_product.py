from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.scores_repo import MaterializedRankingForMeta


def test_product_meta_includes_note_and_materialized_when_present(monkeypatch) -> None:
    sample = MaterializedRankingForMeta(
        ranking_run_id="rank-abc",
        ranking_version="v0-test",
        corpus_snapshot_version="source-snapshot-1",
        embedding_version="none-v0",
        config_json={"default_weights": {"semantic": 0.3}, "family_weights": {}},
    )
    monkeypatch.setattr(
        "app.main.fetch_latest_materialized_ranking_for_meta",
        lambda: sample,
    )

    client = TestClient(app)
    response = client.get("/api/v1/meta/product")
    assert response.status_code == 200
    payload = response.json()
    assert "ranking_metadata_note" in payload
    assert "illustrative" in payload["ranking_metadata_note"].lower() or "build brief" in payload[
        "ranking_metadata_note"
    ].lower()
    assert payload["materialized_ranking"] is not None
    assert payload["materialized_ranking"]["ranking_run_id"] == "rank-abc"
    assert payload["materialized_ranking"]["config_json"]["default_weights"]["semantic"] == 0.3


def test_product_meta_omits_materialized_when_fetch_fails(monkeypatch) -> None:
    def _boom() -> None:
        raise RuntimeError("no db")

    monkeypatch.setattr(
        "app.main.fetch_latest_materialized_ranking_for_meta",
        _boom,
    )

    client = TestClient(app)
    response = client.get("/api/v1/meta/product")
    assert response.status_code == 200
    payload = response.json()
    assert payload["materialized_ranking"] is None
    assert payload["ranking_weights"]["semantic"] == 0.3

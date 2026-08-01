from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.routers import meta as meta_router
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
        "app.routers.meta.fetch_latest_materialized_ranking_for_meta",
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


def test_product_meta_omits_materialized_when_fetch_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret_dsn = "postgresql://audit_user:db-secret@db.internal:5432/research_radar"

    def _boom() -> None:
        raise RuntimeError(f"connection failed for {secret_dsn}")

    monkeypatch.setattr(
        "app.routers.meta.fetch_latest_materialized_ranking_for_meta",
        _boom,
    )

    client = TestClient(app)
    with caplog.at_level(logging.ERROR, logger=meta_router.__name__):
        response = client.get("/api/v1/meta/product")

    assert response.status_code == 200
    payload = response.json()
    assert payload["materialized_ranking"] is None
    assert payload["ranking_weights"]["semantic"] == 0.3
    assert secret_dsn not in caplog.text
    assert "connection failed" not in caplog.text
    error_records = [record for record in caplog.records if record.name == meta_router.__name__]
    assert len(error_records) == 1
    error_record = error_records[0]
    assert error_record.getMessage() == (
        "Failed to load materialized ranking metadata for product summary "
        "exception_type=RuntimeError"
    )
    assert error_record.exception_type == "RuntimeError"
    assert error_record.exc_info is None
    assert secret_dsn not in repr(error_record.__dict__)

from fastapi.testclient import TestClient

from app import main

client = TestClient(main.app)


def _explode(*_args, **_kwargs):
    raise AssertionError("fixture mode should not call the Postgres-backed path")


def test_fixture_mode_serves_core_surfaces_without_database(monkeypatch) -> None:
    monkeypatch.setenv("RESEARCH_RADAR_DATA_MODE", "fixture")
    monkeypatch.setattr(main.psycopg, "connect", _explode)
    monkeypatch.setattr(main, "fetch_latest_materialized_ranking_for_meta", _explode)
    monkeypatch.setattr(main, "search_papers", _explode)
    monkeypatch.setattr(main, "list_ranked_recommendations", _explode)
    monkeypatch.setattr(main, "list_undercited_heuristic_v0", _explode)
    monkeypatch.setattr(main, "load_evaluation_compare", _explode)
    monkeypatch.setattr(main, "load_bridge_distinctness_report", _explode)
    monkeypatch.setattr(main, "list_topic_trends", _explode)
    monkeypatch.setattr(main, "load_cluster_inspection", _explode)
    monkeypatch.setattr(main, "get_paper_detail_row", _explode)
    monkeypatch.setattr(main, "get_paper_family_rankings", _explode)
    monkeypatch.setattr(main, "list_similar_papers", _explode)
    monkeypatch.setattr(main, "list_papers", _explode)

    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["database"] == "fixture-data"

    meta = client.get("/api/v1/meta/product")
    assert meta.status_code == 200
    assert (
        meta.json()["materialized_ranking"]["ranking_version"]
        == "fixture-demo-v0-no-db"
    )

    search = client.get("/api/v1/search?q=audio&family_hint=emerging&limit=3")
    assert search.status_code == 200
    assert search.json()["resolved_ranking_version"] == "fixture-demo-v0-no-db"
    assert search.json()["items"]

    ranked = client.get("/api/v1/recommendations/ranked?family=emerging&limit=2")
    assert ranked.status_code == 200
    assert ranked.json()["family"] == "emerging"
    assert ranked.json()["ranking_mode"] == "materialized_heuristic"
    assert ranked.json()["items"][0]["signal_explanations"]

    undercited = client.get("/api/v1/recommendations/undercited?limit=2")
    assert undercited.status_code == 200
    assert undercited.json()["heuristic_version"] == "fixture-v0"

    bridge = client.get(
        "/api/v1/evaluation/bridge-distinctness?ranking_run_id=ignored-in-fixture&k=2"
    )
    assert bridge.status_code == 200
    assert bridge.json()["ranking_run_id"] == "fixture-rank-demo-001"
    assert bridge.json()["cluster_version"] == "fixture-kmeans-v0"

    trends = client.get(
        "/api/v1/trends/topics?min_works=1&limit=3&corpus_snapshot_version=ignored-in-fixture"
    )
    assert trends.status_code == 200
    assert trends.json()["corpus_snapshot_version"] == "fixture-snapshot-mir-audio-2026"
    assert trends.json()["items"]

    clusters = client.get(
        "/api/v1/clusters/ignored-in-fixture/inspect?sample_per_cluster=2"
    )
    assert clusters.status_code == 200
    assert clusters.json()["cluster_version"] == "fixture-kmeans-v0"
    assert clusters.json()["groups"]

    evaluation = client.get("/api/v1/evaluation/compare?family=emerging&limit=2")
    assert evaluation.status_code == 200
    assert "Fixture outputs" in evaluation.json()["disclaimer"]["headline"]

    paper_id = "https%3A%2F%2Fopenalex.org%2FWF001"
    detail = client.get(f"/api/v1/papers/{paper_id}")
    assert detail.status_code == 200
    assert detail.json()["paper_id"] == "https://openalex.org/WF001"

    ranking = client.get(f"/api/v1/papers/{paper_id}/ranking")
    assert ranking.status_code == 200
    assert ranking.json()["families"]

    similar = client.get(
        f"/api/v1/papers/{paper_id}/similar?embedding_version=fixture-title-abstract-v0"
    )
    assert similar.status_code == 200
    assert similar.json()["items"]

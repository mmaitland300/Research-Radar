from fastapi.testclient import TestClient

from app import main

client = TestClient(main.app)


def test_evaluation_summary_is_proxy_not_human_benchmark() -> None:
    response = client.get("/api/v1/evaluation/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["is_human_labeled_benchmark_current"] is False
    assert "proxy" in data["current_evaluation_type"]
    assert "baselines" in data["current_evaluation_type"] or "ranked" in data["current_evaluation_type"]
    planned = data["planned_labeled_benchmark"]
    assert "corpus" in planned and "metrics" in planned
    assert planned["corpus"] == "100-200 papers"
    assert planned["metrics"] == ["precision@10", "precision@20"]
    assert "compatibility" in data["legacy_note"] or "roadmap" in data["legacy_note"]
    for m in data["primary_metrics"]:
        assert "roadmap" in m and "not a current" in m

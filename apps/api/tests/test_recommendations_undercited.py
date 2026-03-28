from fastapi.testclient import TestClient

from app import main
from app.papers_repo import UndercitedHeuristicRow


client = TestClient(main.app)


def test_get_recommendations_undercited_smoke(monkeypatch) -> None:
    def fake_list_undercited_heuristic_v0(
        *,
        limit: int,
        min_year: int,
        max_citations: int,
    ) -> list[UndercitedHeuristicRow]:
        assert limit == 5
        assert min_year == 2020
        assert max_citations == 10
        return [
            UndercitedHeuristicRow(
                paper_id="W777",
                title="Low-Cite Core Paper",
                year=2023,
                citation_count=3,
                source_slug="ismir",
                reason="Recent core paper with low citation count (heuristic v0 baseline).",
                signal_breakdown={
                    "recency": 0.75,
                    "low_citation_signal": 0.8,
                    "core_corpus_gate": 1.0,
                    "metadata_quality_gate": 1.0,
                    "heuristic_composite": 0.7775,
                },
            )
        ]

    monkeypatch.setattr(main, "list_undercited_heuristic_v0", fake_list_undercited_heuristic_v0)
    response = client.get(
        "/api/v1/recommendations/undercited?limit=5&min_year=2020&max_citations=10"
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["heuristic_version"] == "v0"
    assert payload["heuristic_label"] == "undercited-core-recent-v0"
    assert payload["total"] == 1
    assert payload["items"][0]["paper_id"] == "W777"
    assert "heuristic_composite" in payload["items"][0]["signal_breakdown"]

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
        bridge_eligible_only: bool = False,
    ):
        assert family == "undercited"
        assert limit == 10
        assert corpus_snapshot_version == "snap-1"
        assert ranking_run_id is None
        assert ranking_version == "v0-test"
        assert bridge_eligible_only is False
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
                bridge_eligible=None,
            )
        ]
        return ctx, rows, {}

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
    assert "list_explanation" in payload
    assert payload["list_explanation"]["family"] == "undercited"
    assert len(item["signal_explanations"]) >= 5
    assert item["bridge_eligible"] is None


def test_get_recommendations_ranked_bridge_eligible_on_bridge_family(monkeypatch) -> None:
    def fake_list_ranked_recommendations(
        *,
        family: str,
        limit: int,
        corpus_snapshot_version: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
        bridge_eligible_only: bool = False,
    ):
        assert family == "bridge"
        assert bridge_eligible_only is False
        ctx = RankedRunContext(
            ranking_run_id="run-b",
            ranking_version="v1",
            corpus_snapshot_version="snap-x",
        )
        rows = [
            RankedRecommendationRow(
                paper_id="W1",
                title="Bridge paper",
                year=2020,
                citation_count=3,
                source_slug=None,
                topics=[],
                semantic_score=None,
                citation_velocity_score=0.5,
                topic_growth_score=0.5,
                bridge_score=0.8,
                diversity_penalty=0.1,
                final_score=0.9,
                reason_short="structural",
                bridge_eligible=True,
            ),
            RankedRecommendationRow(
                paper_id="W2",
                title="Ineligible",
                year=2019,
                citation_count=1,
                source_slug=None,
                topics=[],
                semantic_score=None,
                citation_velocity_score=0.4,
                topic_growth_score=0.4,
                bridge_score=0.2,
                diversity_penalty=0.1,
                final_score=0.5,
                reason_short="structural",
                bridge_eligible=False,
            ),
        ]
        return ctx, rows, {}

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_list_ranked_recommendations)
    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=20")

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["items"][0]["bridge_eligible"] is True
    assert payload["items"][1]["bridge_eligible"] is False


def test_get_recommendations_ranked_bridge_eligible_only_forwarded_for_bridge(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def fake_list_ranked_recommendations(
        *,
        family: str,
        limit: int,
        corpus_snapshot_version: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
        bridge_eligible_only: bool = False,
    ):
        seen["bridge_eligible_only"] = bridge_eligible_only
        ctx = RankedRunContext(
            ranking_run_id="run-c",
            ranking_version="v1",
            corpus_snapshot_version="snap-y",
        )
        return ctx, [], {}

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_list_ranked_recommendations)
    response = client.get("/api/v1/recommendations/ranked?family=bridge&bridge_eligible_only=true")

    assert response.status_code == 200
    assert seen["bridge_eligible_only"] is True
    assert response.json()["total"] == 0


def test_get_recommendations_ranked_bridge_eligible_only_forwarded_but_ignored_family(monkeypatch) -> None:
    """API passes the flag through; repo must not apply the SQL filter for non-bridge families."""

    seen: dict[str, object] = {}

    def fake_list_ranked_recommendations(
        *,
        family: str,
        limit: int,
        corpus_snapshot_version: str | None,
        ranking_run_id: str | None,
        ranking_version: str | None,
        bridge_eligible_only: bool = False,
    ):
        seen["family"] = family
        seen["bridge_eligible_only"] = bridge_eligible_only
        ctx = RankedRunContext(
            ranking_run_id="run-d",
            ranking_version="v1",
            corpus_snapshot_version="snap-z",
        )
        rows = [
            RankedRecommendationRow(
                paper_id="W9",
                title="Emerging",
                year=2024,
                citation_count=0,
                source_slug=None,
                topics=[],
                semantic_score=None,
                citation_velocity_score=0.9,
                topic_growth_score=0.8,
                bridge_score=None,
                diversity_penalty=0.0,
                final_score=0.85,
                reason_short="emerging heuristic",
                bridge_eligible=None,
            )
        ]
        return ctx, rows, {}

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_list_ranked_recommendations)
    response = client.get(
        "/api/v1/recommendations/ranked?family=emerging&bridge_eligible_only=true&limit=5"
    )

    assert response.status_code == 200
    assert seen["family"] == "emerging"
    assert seen["bridge_eligible_only"] is True
    assert response.json()["total"] == 1


def test_get_recommendations_ranked_legacy_bridge_row_null_eligibility(monkeypatch) -> None:
    """Runs before neighbor_mix persist null bridge_eligible; API exposes null."""

    def fake_list_ranked_recommendations(**_kwargs):
        ctx = RankedRunContext(
            ranking_run_id="run-old",
            ranking_version="v0",
            corpus_snapshot_version="snap-old",
        )
        rows = [
            RankedRecommendationRow(
                paper_id="W0",
                title="Legacy bridge",
                year=2018,
                citation_count=10,
                source_slug=None,
                topics=[],
                semantic_score=None,
                citation_velocity_score=0.5,
                topic_growth_score=0.5,
                bridge_score=0.7,
                diversity_penalty=0.2,
                final_score=0.6,
                reason_short="structural",
                bridge_eligible=None,
            )
        ]
        return ctx, rows, {}

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_list_ranked_recommendations)
    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=5")
    assert response.status_code == 200
    assert response.json()["items"][0]["bridge_eligible"] is None


def test_get_recommendations_ranked_bridge_false_encodes_neighbor_mix_not_legacy_null(
    monkeypatch,
) -> None:
    """neighbor_mix_v1 runs use false for ineligible or missing mix support; legacy stays null."""

    def fake_list_ranked_recommendations(**_kwargs):
        ctx = RankedRunContext(
            ranking_run_id="run-nm",
            ranking_version="v2",
            corpus_snapshot_version="snap-nm",
        )
        rows = [
            RankedRecommendationRow(
                paper_id="W99",
                title="No mix support",
                year=2020,
                citation_count=2,
                source_slug=None,
                topics=[],
                semantic_score=None,
                citation_velocity_score=0.5,
                topic_growth_score=0.5,
                bridge_score=0.5,
                diversity_penalty=0.1,
                final_score=0.55,
                reason_short="structural",
                bridge_eligible=False,
            )
        ]
        return ctx, rows, {}

    monkeypatch.setattr(main, "list_ranked_recommendations", fake_list_ranked_recommendations)
    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=5")
    assert response.status_code == 200
    assert response.json()["items"][0]["bridge_eligible"] is False


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

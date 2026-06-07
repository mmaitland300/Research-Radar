from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import main
from app import ml_bridge_scorer_rollout as bridge_rollout
from app.ml_bridge_scorer_rollout import build_bridge_ranked_recommendations_response
from app.ml_bridge_scorer_rollout_gate import (
    PINNED_BRIDGE_RANKING_RUN_ID,
    get_bridge_rollout_served_count,
    reset_bridge_rollout_served_count,
)
from app.ml_scorer_rollout import build_ranked_recommendations_response
from app.ml_scorer_rollout_gate import (
    get_rollout_served_count,
    reset_rollout_served_count,
)
from app.scores_repo import RankedRecommendationRow, RankedRunContext

client = TestClient(main.app)

_BRIDGE_ENV_KEYS = (
    "ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED",
    "ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST",
    "ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP",
    "ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_ENABLED",
    "ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_PERCENT",
    "ML_BRIDGE_SCORER_V1_RANKING_RUN_ID",
)
_SHADOW_ENV_KEYS = (
    "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED",
    "ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST",
    "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP",
    "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_ENABLED",
    "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_PERCENT",
)


@pytest.fixture(autouse=True)
def _reset_env_and_counters(monkeypatch: pytest.MonkeyPatch):
    for key in (*_BRIDGE_ENV_KEYS, *_SHADOW_ENV_KEYS):
        monkeypatch.delenv(key, raising=False)
    reset_bridge_rollout_served_count()
    reset_rollout_served_count()
    yield
    reset_bridge_rollout_served_count()
    reset_rollout_served_count()


def _enable_bridge_gate(monkeypatch: pytest.MonkeyPatch, *, cap: str = "5") -> None:
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST", "canary-a")
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP", cap)
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_RANKING_RUN_ID", PINNED_BRIDGE_RANKING_RUN_ID)


def _enable_bridge_public_gate(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cap: str = "5",
    percent: str = "100",
    allowlist: str | None = None,
) -> None:
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_ENABLED", "true")
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_PERCENT", percent)
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP", cap)
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_RANKING_RUN_ID", PINNED_BRIDGE_RANKING_RUN_ID)
    if allowlist is not None:
        monkeypatch.setenv("ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST", allowlist)


def _ctx(
    *,
    ranking_run_id: str = "rank-baseline",
    ranking_version: str = "ranking-baseline-v1",
    corpus_snapshot_version: str = "source-snapshot-baseline",
) -> RankedRunContext:
    return RankedRunContext(
        ranking_run_id=ranking_run_id,
        ranking_version=ranking_version,
        corpus_snapshot_version=corpus_snapshot_version,
    )


def _pinned_bridge_ctx() -> RankedRunContext:
    return _ctx(
        ranking_run_id=PINNED_BRIDGE_RANKING_RUN_ID,
        ranking_version="bridge-rank-pct-hybrid-test-v1",
        corpus_snapshot_version="source-snapshot-bridge-test-v1",
    )


def _row(paper_id: str, *, score: float = 0.9, family: str = "bridge") -> RankedRecommendationRow:
    return RankedRecommendationRow(
        paper_id=paper_id,
        title=f"Paper {paper_id}",
        year=2024,
        citation_count=7,
        source_slug="test",
        topics=["topic"],
        semantic_score=0.8,
        citation_velocity_score=0.7,
        topic_growth_score=0.6,
        bridge_score=0.5 if family == "bridge" else None,
        diversity_penalty=0.1,
        final_score=score,
        reason_short="test reason",
        bridge_eligible=True if family == "bridge" else None,
    )


def _baseline_rows(family: str = "bridge") -> list[RankedRecommendationRow]:
    return [_row(f"W{family.upper()}BASE001", score=0.91, family=family)]


def _expected_json(ctx: RankedRunContext, rows: list[RankedRecommendationRow], family: str):
    return build_ranked_recommendations_response(ctx, rows, {}, family).model_dump(mode="json")


class _FakeServing:
    def __init__(
        self,
        ordered_ids: list[str],
        exc: Exception | None = None,
        metadata_extra: dict[str, Any] | None = None,
    ) -> None:
        self.ordered_ids = ordered_ids
        self.exc = exc
        self.metadata_extra = metadata_extra or {}
        self.calls = 0
        self.kwargs: list[dict[str, Any]] = []

    def rank_bridge_recommendations_with_scorer(self, **kwargs):
        self.calls += 1
        self.kwargs.append(dict(kwargs))
        if self.exc is not None:
            raise self.exc
        metadata = {
            "ranking_run_id": PINNED_BRIDGE_RANKING_RUN_ID,
            "family": "bridge",
            "primary_alpha": 0.5,
            "rank_pct_scope": "full_bridge_candidate_pool",
            "scorer_probability_source": "full_pool_frozen_inference_not_oof",
            "writes_performed": False,
        }
        metadata.update(self.metadata_extra)
        return (
            [{"openalex_id": paper_id, "work_id_token": paper_id} for paper_id in self.ordered_ids],
            metadata,
        )

    def map_bridge_scorer_rows_to_paper_ids(self, scored_rows, limit: int = 20):
        return [row["openalex_id"] for row in scored_rows[:limit]]


def _patch_bridge_open(
    monkeypatch: pytest.MonkeyPatch,
    *,
    serving: _FakeServing,
    ctx: RankedRunContext | None = None,
    baseline_rows: list[RankedRecommendationRow] | None = None,
) -> None:
    resolved_ctx = ctx or _pinned_bridge_ctx()
    rows = baseline_rows or _baseline_rows("bridge")
    monkeypatch.setattr(
        bridge_rollout,
        "list_ranked_recommendations",
        lambda **_kwargs: (resolved_ctx, rows, {}),
    )
    monkeypatch.setattr(
        main,
        "list_ranked_recommendations",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("fallback should not run")),
    )
    monkeypatch.setattr(bridge_rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        bridge_rollout,
        "hydrate_ranked_bridge_recommendation_rows_for_paper_ids",
        lambda **kwargs: [
            _row(paper_id, score=1.0 - (idx / 1000), family="bridge")
            for idx, paper_id in enumerate(kwargs["ordered_openalex_ids"])
        ],
    )


def test_default_env_missing_bridge_output_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx()
    rows = _baseline_rows("bridge")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("Bridge scorer must not load")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")
    assert response.json()["ranking_mode"] == "materialized_heuristic"


def test_bridge_pinned_request_without_canary_returns_materialized_fallback_not_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    captured: dict[str, Any] = {}

    def baseline(**kwargs):
        captured.update(kwargs)
        return ctx, rows, {}

    monkeypatch.setattr(main, "list_ranked_recommendations", baseline)
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("Bridge scorer must not load")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?"
        f"family=bridge&limit=20&ranking_run_id={PINNED_BRIDGE_RANKING_RUN_ID}"
    )

    assert response.status_code == 200
    assert captured["ranking_run_id"] == PINNED_BRIDGE_RANKING_RUN_ID
    assert response.json() == _expected_json(ctx, rows, "bridge")
    assert response.json()["ranking_mode"] == "materialized_heuristic"


def test_emerging_behavior_unchanged_with_bridge_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch)
    ctx = _ctx()
    rows = _baseline_rows("emerging")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        main,
        "maybe_build_bridge_scorer_ranked_response",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Emerging must not call Bridge gate")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_undercited_behavior_unchanged_with_bridge_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch)
    ctx = _ctx()
    rows = _baseline_rows("undercited")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        main,
        "maybe_build_bridge_scorer_ranked_response",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("Undercited must not call Bridge gate")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=undercited&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "undercited")


@pytest.mark.parametrize(
    "query",
    [
        "family=bridge&limit=19",
        f"family=bridge&limit=20&ranking_run_id=rank-not-{PINNED_BRIDGE_RANKING_RUN_ID}",
        "family=bridge&limit=20&bridge_eligible_only=true",
    ],
)
def test_bridge_gate_preconditions_fail_closed(query: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch)
    ctx = _ctx()
    rows = _baseline_rows("bridge")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("closed Bridge gate must not load scorer")),
    )

    response = client.get(f"/api/v1/recommendations/ranked?{query}")

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "materialized_heuristic"


def test_bridge_scorer_env_enabled_but_cap_zero_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch, cap="0")
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("cap zero must not call scorer")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")


def test_bridge_configured_ranking_run_mismatch_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch)
    monkeypatch.setenv("ML_BRIDGE_SCORER_V1_RANKING_RUN_ID", "rank-other")
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("wrong configured run must not call scorer")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")


def test_bridge_public_rollout_partial_percent_without_subject_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_bridge_public_gate(monkeypatch, percent="50")
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("partial public path must not call scorer")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")


def test_bridge_scorer_missing_artifact_or_scorer_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch, cap="1")
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    serving = _FakeServing([f"WBRIDGE{i:03d}" for i in range(20)], exc=RuntimeError("artifact missing"))

    monkeypatch.setattr(bridge_rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(bridge_rollout, "_load_pipeline_serving_module", lambda: serving)

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")
    assert get_bridge_rollout_served_count() == 0


def test_bridge_scorer_metadata_must_report_no_db_writes(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch, cap="1")
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    serving = _FakeServing(
        [f"WBRIDGE{i:03d}" for i in range(20)],
        metadata_extra={"writes_performed": True},
    )

    monkeypatch.setattr(bridge_rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(bridge_rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        bridge_rollout,
        "hydrate_ranked_bridge_recommendation_rows_for_paper_ids",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("write contract failure must not hydrate")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")
    assert get_bridge_rollout_served_count() == 0


def test_bridge_scorer_metadata_contract_mismatch_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch, cap="1")
    ctx = _pinned_bridge_ctx()
    rows = _baseline_rows("bridge")
    serving = _FakeServing(
        [f"WBRIDGE{i:03d}" for i in range(20)],
        metadata_extra={"primary_alpha": 0.7},
    )

    monkeypatch.setattr(bridge_rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(bridge_rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        bridge_rollout,
        "hydrate_ranked_bridge_recommendation_rows_for_paper_ids",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("contract mismatch must not hydrate")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")
    assert get_bridge_rollout_served_count() == 0


def test_bridge_scorer_enabled_with_pinned_run_returns_bounded_bridge_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_bridge_gate(monkeypatch)
    scorer_ids = [f"WBRIDGE{i:03d}" for i in range(20)]
    serving = _FakeServing(scorer_ids)
    _patch_bridge_open(monkeypatch, serving=serving)

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ranking_mode"] == "bounded_bridge_ml_scorer"
    assert [item["paper_id"] for item in payload["items"]] == scorer_ids
    assert serving.calls == 1
    assert serving.kwargs[0]["limit"] == 20


def test_served_bridge_response_preserves_item_schema_and_disclosure_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_bridge_gate(monkeypatch)
    scorer_ids = [f"WBRIDGE{i:03d}" for i in range(20)]
    _patch_bridge_open(monkeypatch, serving=_FakeServing(scorer_ids))

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    payload = response.json()
    item = payload["items"][0]
    for key in ("paper_id", "title", "signals", "final_score", "signal_explanations", "bridge_eligible"):
        assert key in item
    assert payload["scorer_surface"] == "bridge"
    assert payload["bridge_recommendations_ml_served"] is True
    assert payload["bridge_rank_pct_hybrid_alpha"] == 0.5
    assert payload["bridge_rank_pct_scope"] == "full_bridge_candidate_pool"
    assert payload["emitted_to_public_users"] is False


def test_emitted_to_public_users_true_on_public_rollout_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_public_gate(monkeypatch, allowlist="")
    _patch_bridge_open(monkeypatch, serving=_FakeServing([f"WPUBLIC{i:03d}" for i in range(20)]))

    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=20")

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "bounded_bridge_ml_scorer"
    assert response.json()["emitted_to_public_users"] is True


def test_emitted_to_public_users_false_on_cohort_canary_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_bridge_gate(monkeypatch)
    _patch_bridge_open(monkeypatch, serving=_FakeServing([f"WCANARY{i:03d}" for i in range(20)]))

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "bounded_bridge_ml_scorer"
    assert response.json()["emitted_to_public_users"] is False


def test_bridge_does_not_use_ml_shadow_scorer_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST", "canary-a")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP", "5")
    ctx = _ctx()
    rows = _baseline_rows("bridge")
    monkeypatch.setattr(main, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {}))
    monkeypatch.setattr(
        bridge_rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("ML_SHADOW env must not open Bridge scorer")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "materialized_heuristic"


def test_bridge_exposure_counter_is_separate_from_emerging_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_bridge_gate(monkeypatch, cap="1")
    _patch_bridge_open(monkeypatch, serving=_FakeServing([f"WCOUNT{i:03d}" for i in range(20)]))

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "bounded_bridge_ml_scorer"
    assert get_bridge_rollout_served_count() == 1
    assert get_rollout_served_count() == 0


def test_api_contract_accepts_bounded_bridge_ml_scorer() -> None:
    ctx = _pinned_bridge_ctx()
    response = build_bridge_ranked_recommendations_response(
        ctx,
        _baseline_rows("bridge"),
        {},
        ranking_mode="bounded_bridge_ml_scorer",
        ranking_mode_detail="Experimental Bridge ranking",
        bridge_recommendations_ml_served=True,
        emitted_to_public_users=False,
    )

    payload = response.model_dump(mode="json")
    assert payload["ranking_mode"] == "bounded_bridge_ml_scorer"
    assert payload["scorer_surface"] == "bridge"

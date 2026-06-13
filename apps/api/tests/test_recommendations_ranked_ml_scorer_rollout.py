import logging
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app import main
from app import ml_scorer_rollout as rollout
from app.ml_scorer_rollout import build_ranked_recommendations_response
from app.ml_scorer_rollout_gate import (
    PINNED_CORPUS_SNAPSHOT_VERSION,
    PINNED_RANKING_RUN_ID,
    PINNED_RANKING_VERSION,
    get_rollout_served_count,
    reset_rollout_served_count,
)
from app.scores_repo import RankedRecommendationRow, RankedRunContext
from app.routers import recommendations as recommendations_router

client = TestClient(main.app)

_ENV_KEYS = (
    "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED",
    "ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST",
    "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP",
    "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_ENABLED",
    "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_PERCENT",
)


@pytest.fixture(autouse=True)
def _reset_rollout(monkeypatch: pytest.MonkeyPatch):
    for key in _ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    reset_rollout_served_count()
    yield
    reset_rollout_served_count()


def _enable_gate(monkeypatch: pytest.MonkeyPatch, *, cap: str = "5") -> None:
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST", "canary-a")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP", cap)


def _enable_public_gate(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cap: str = "5",
    percent: str = "100",
    allowlist: str | None = None,
) -> None:
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_ENABLED", "true")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_PERCENT", percent)
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP", cap)
    if allowlist is not None:
        monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST", allowlist)


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


def _pinned_ctx() -> RankedRunContext:
    return _ctx(
        ranking_run_id=PINNED_RANKING_RUN_ID,
        ranking_version=PINNED_RANKING_VERSION,
        corpus_snapshot_version=PINNED_CORPUS_SNAPSHOT_VERSION,
    )


def _row(paper_id: str, *, score: float = 0.9, bridge_eligible: bool | None = None):
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
        bridge_score=None,
        diversity_penalty=0.1,
        final_score=score,
        reason_short="test reason",
        bridge_eligible=bridge_eligible,
    )


def _baseline_rows() -> list[RankedRecommendationRow]:
    return [_row("WBASE001", score=0.91)]


def _expected_json(
    ctx: RankedRunContext, rows: list[RankedRecommendationRow], family: str
):
    return build_ranked_recommendations_response(ctx, rows, {}, family).model_dump(
        mode="json"
    )


class _FakeServing:
    def __init__(self, ordered_ids: list[str], exc: Exception | None = None) -> None:
        self.ordered_ids = ordered_ids
        self.exc = exc
        self.calls = 0
        self.envs: list[dict[str, str]] = []

    def rank_emerging_recommendations_with_scorer(self, **kwargs):
        self.calls += 1
        self.envs.append(dict(kwargs.get("env") or {}))
        if self.exc is not None:
            raise self.exc
        return (
            [{"canonical_openalex_work_id": paper_id} for paper_id in self.ordered_ids],
            {"runtime_status": "succeeded_test_only"},
        )

    def map_shadow_rows_to_paper_ids(self, shadow_rows, limit: int = 20):
        return [row["canonical_openalex_work_id"] for row in shadow_rows[:limit]]


def test_default_env_absent_scorer_helper_not_called_and_baseline_called(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = {"baseline_calls": 0}
    ctx = _ctx()
    rows = _baseline_rows()

    def baseline(**_kwargs):
        seen["baseline_calls"] += 1
        return ctx, rows, {}

    monkeypatch.setattr(recommendations_router, "list_ranked_recommendations", baseline)
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("scorer helper should not load")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    assert seen["baseline_calls"] == 1
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_runtime_on_public_rollout_disabled_no_header_returns_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP", "5")
    ctx = _ctx()
    rows = _baseline_rows()

    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(
            AssertionError("public rollout disabled must not call scorer")
        ),
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_bridge_flag_on_scorer_not_called(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_gate(monkeypatch)
    ctx = _ctx()
    rows = [_row("WBRIDGE001", bridge_eligible=True)]

    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("bridge must not call scorer")),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=bridge&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "bridge")


def test_bridge_public_rollout_enabled_still_returns_materialized_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_public_gate(monkeypatch)
    ctx = _ctx()
    rows = [_row("WBRIDGE001", bridge_eligible=True)]

    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("bridge must not call scorer")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=bridge&limit=20")

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "materialized_heuristic"
    assert response.json() == _expected_json(ctx, rows, "bridge")


def test_undercited_public_rollout_enabled_still_returns_materialized_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_public_gate(monkeypatch)
    ctx = _ctx()
    rows = [_row("WUNDER001")]

    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(
            AssertionError("undercited must not call scorer")
        ),
    )

    response = client.get("/api/v1/recommendations/ranked?family=undercited&limit=20")

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "materialized_heuristic"
    assert response.json() == _expected_json(ctx, rows, "undercited")


def test_resolved_identity_mismatch_scorer_not_called_and_baseline_json_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_gate(monkeypatch)
    ctx = _ctx(ranking_run_id="rank-not-pinned")
    rows = _baseline_rows()

    def baseline(**_kwargs):
        return ctx, rows, {}

    monkeypatch.setattr(recommendations_router, "list_ranked_recommendations", baseline)
    monkeypatch.setattr(rollout, "list_ranked_recommendations", baseline)
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(
            AssertionError("identity mismatch must not call scorer")
        ),
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=emerging&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_scorer_gate_db_read_failure_falls_back_to_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_public_gate(monkeypatch)
    ctx = _ctx()
    rows = _baseline_rows()

    monkeypatch.setattr(
        rollout,
        "list_ranked_recommendations",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("db down")),
    )
    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(
            AssertionError("db read failure must not call scorer")
        ),
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_wildcard_allowlist_env_fails_closed_to_baseline(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_RUNTIME_ENABLED", "true")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST", "*")
    monkeypatch.setenv("ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP", "1")
    ctx = _ctx()
    rows = _baseline_rows()

    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(
            AssertionError("invalid env must not call scorer")
        ),
    )

    with caplog.at_level(logging.INFO, logger=rollout.__name__):
        response = client.get(
            "/api/v1/recommendations/ranked?family=emerging&limit=20",
            headers={"X-Research-Radar-Canary-Subject": "canary-a"},
        )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")
    assert "gate_config_failed" in caplog.text
    assert "ValueError" in caplog.text
    assert "*" not in caplog.text


def test_gate_open_pinned_context_scorer_order_hydrated_outside_baseline_top20(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_gate(monkeypatch)
    baseline_rows = [_row(f"WBASE{i:03d}", score=1.0 - (i / 1000)) for i in range(20)]
    scorer_ids = ["WOUTSIDE999"] + [f"WSCORER{i:03d}" for i in range(1, 20)]
    serving = _FakeServing(scorer_ids)
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        rollout,
        "list_ranked_recommendations",
        lambda **_kwargs: (_pinned_ctx(), baseline_rows, {}),
    )
    monkeypatch.setattr(
        recommendations_router,
        "list_ranked_recommendations",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("baseline fallback should not run")
        ),
    )
    monkeypatch.setattr(rollout, "_load_pipeline_serving_module", lambda: serving)

    def hydrate(**kwargs):
        captured["ordered_ids"] = list(kwargs["ordered_openalex_ids"])
        return [
            _row(paper_id, score=1.0 - (index / 1000))
            for index, paper_id in enumerate(kwargs["ordered_openalex_ids"])
        ]

    monkeypatch.setattr(
        rollout, "hydrate_ranked_recommendation_rows_for_paper_ids", hydrate
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=emerging&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["paper_id"] for item in payload["items"]] == scorer_ids
    assert captured["ordered_ids"][0] == "WOUTSIDE999"
    assert "WOUTSIDE999" not in {row.paper_id for row in baseline_rows}
    assert serving.calls == 1
    assert serving.envs[0]["ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"] == "true"


def test_public_rollout_percent_100_pinned_context_returns_scorer_mode_without_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_public_gate(monkeypatch, allowlist="")
    baseline_rows = [_row(f"WBASE{i:03d}", score=1.0 - (i / 1000)) for i in range(20)]
    scorer_ids = [f"WPUBLIC{i:03d}" for i in range(20)]
    serving = _FakeServing(scorer_ids)

    monkeypatch.setattr(
        rollout,
        "list_ranked_recommendations",
        lambda **_kwargs: (_pinned_ctx(), baseline_rows, {}),
    )
    monkeypatch.setattr(
        recommendations_router,
        "list_ranked_recommendations",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("baseline fallback should not run")
        ),
    )
    monkeypatch.setattr(rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        rollout,
        "hydrate_ranked_recommendation_rows_for_paper_ids",
        lambda **kwargs: [
            _row(paper_id, score=1.0 - (index / 1000))
            for index, paper_id in enumerate(kwargs["ordered_openalex_ids"])
        ],
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ranking_mode"] == "bounded_ml_scorer"
    assert payload["ranking_mode_detail"]
    assert [item["paper_id"] for item in payload["items"]] == scorer_ids
    assert serving.calls == 1


def test_public_rollout_cap_zero_returns_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_public_gate(monkeypatch, cap="0")
    ctx = _pinned_ctx()
    rows = _baseline_rows()

    monkeypatch.setattr(
        rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(AssertionError("cap zero must not call scorer")),
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_public_rollout_partial_percent_without_subject_returns_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_public_gate(monkeypatch, percent="25")
    ctx = _pinned_ctx()
    rows = _baseline_rows()

    monkeypatch.setattr(
        rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        rollout,
        "_load_pipeline_serving_module",
        lambda: (_ for _ in ()).throw(
            AssertionError("partial public rollout must not call scorer")
        ),
    )

    response = client.get("/api/v1/recommendations/ranked?family=emerging&limit=20")

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")


def test_hydration_incomplete_falls_back_to_baseline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_gate(monkeypatch, cap="1")
    ctx = _pinned_ctx()
    rows = _baseline_rows()
    serving = _FakeServing([f"WSCORER{i:03d}" for i in range(20)])

    monkeypatch.setattr(
        rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        rollout,
        "hydrate_ranked_recommendation_rows_for_paper_ids",
        lambda **_kwargs: None,
    )

    response = client.get(
        "/api/v1/recommendations/ranked?family=emerging&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")
    assert response.json()["ranking_mode"] == "materialized_heuristic"
    assert get_rollout_served_count() == 0


def test_scorer_helper_raises_falls_back_to_exact_baseline_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_gate(monkeypatch, cap="1")
    ctx = _pinned_ctx()
    rows = _baseline_rows()
    serving = _FakeServing(
        [f"WSCORER{i:03d}" for i in range(20)], exc=RuntimeError("boom")
    )

    monkeypatch.setattr(
        rollout, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )
    monkeypatch.setattr(rollout, "_load_pipeline_serving_module", lambda: serving)

    response = client.get(
        "/api/v1/recommendations/ranked?family=emerging&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert response.status_code == 200
    assert response.json() == _expected_json(ctx, rows, "emerging")
    assert response.json()["ranking_mode"] == "materialized_heuristic"
    assert get_rollout_served_count() == 0


def test_exposure_counter_increments_once_and_stops_at_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable_gate(monkeypatch, cap="1")
    ctx = _pinned_ctx()
    baseline_rows = _baseline_rows()
    scorer_ids = [f"WSCORER{i:03d}" for i in range(20)]
    serving = _FakeServing(scorer_ids)

    monkeypatch.setattr(
        rollout,
        "list_ranked_recommendations",
        lambda **_kwargs: (ctx, baseline_rows, {}),
    )
    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, baseline_rows, {})
    )
    monkeypatch.setattr(rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        rollout,
        "hydrate_ranked_recommendation_rows_for_paper_ids",
        lambda **kwargs: [
            _row(paper_id) for paper_id in kwargs["ordered_openalex_ids"]
        ],
    )

    first = client.get(
        "/api/v1/recommendations/ranked?family=emerging&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )
    second = client.get(
        "/api/v1/recommendations/ranked?family=emerging&limit=20",
        headers={"X-Research-Radar-Canary-Subject": "canary-a"},
    )

    assert first.status_code == 200
    assert first.json()["items"][0]["paper_id"] == "WSCORER000"
    assert second.status_code == 200
    assert second.json() == _expected_json(ctx, baseline_rows, "emerging")
    assert serving.calls == 1
    assert get_rollout_served_count() == 1


def test_subject_value_is_not_logged(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ctx = _ctx()
    rows = _baseline_rows()
    sensitive_subject = "secret-canary-subject"

    monkeypatch.setattr(
        recommendations_router, "list_ranked_recommendations", lambda **_kwargs: (ctx, rows, {})
    )

    with caplog.at_level(logging.INFO, logger=rollout.__name__):
        response = client.get(
            "/api/v1/recommendations/ranked?family=emerging&limit=20",
            headers={"X-Research-Radar-Canary-Subject": sensitive_subject},
        )

    assert response.status_code == 200
    assert sensitive_subject not in caplog.text
    assert "gate_closed" in caplog.text
    assert "exception_type=None" in caplog.text


def test_gate_open_log_omits_subject_value_and_includes_public_rollout_fields(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _enable_gate(monkeypatch)
    ctx = _pinned_ctx()
    sensitive_subject = "canary-a"
    serving = _FakeServing([f"WSCORER{i:03d}" for i in range(20)])

    monkeypatch.setattr(
        rollout,
        "list_ranked_recommendations",
        lambda **_kwargs: (ctx, _baseline_rows(), {}),
    )
    monkeypatch.setattr(
        recommendations_router,
        "list_ranked_recommendations",
        lambda **_kwargs: (ctx, _baseline_rows(), {}),
    )
    monkeypatch.setattr(rollout, "_load_pipeline_serving_module", lambda: serving)
    monkeypatch.setattr(
        rollout,
        "hydrate_ranked_recommendation_rows_for_paper_ids",
        lambda **kwargs: [
            _row(paper_id) for paper_id in kwargs["ordered_openalex_ids"]
        ],
    )

    with caplog.at_level(logging.INFO, logger=rollout.__name__):
        response = client.get(
            "/api/v1/recommendations/ranked?family=emerging&limit=20",
            headers={"X-Research-Radar-Canary-Subject": sensitive_subject},
        )

    assert response.status_code == 200
    assert response.json()["ranking_mode"] == "bounded_ml_scorer"
    assert sensitive_subject not in caplog.text
    assert "subject_present=True" in caplog.text
    assert "public_rollout_enabled=False" in caplog.text
    assert "public_rollout_percent=0" in caplog.text
    assert "cap=5" in caplog.text
    assert "current_served=0" in caplog.text

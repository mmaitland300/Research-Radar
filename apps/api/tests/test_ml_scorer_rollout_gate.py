import pytest

from app.ml_scorer_rollout_gate import (
    ROLLOUT_LIMIT,
    ROLLOUT_ROUTE,
    ScorerRolloutGate,
    build_gate_from_env,
    get_rollout_served_count,
    release_rollout_slot_for_failure,
    reset_rollout_served_count,
    try_reserve_rollout_slot,
)


def _open_gate(cap: str = "5") -> ScorerRolloutGate:
    return build_gate_from_env(
        {
            "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED": "true",
            "ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST": "canary-a",
            "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP": cap,
        }
    )


def _attempt(gate: ScorerRolloutGate, **overrides):
    params = {
        "route": ROLLOUT_ROUTE,
        "family": "emerging",
        "limit": ROLLOUT_LIMIT,
        "bridge_eligible_only": False,
        "subject": "canary-a",
        "current_served": 0,
        "cap": gate.exposure_cap,
    }
    params.update(overrides)
    return gate.should_attempt_scorer_path(**params)


def test_flag_off_returns_false_with_reason() -> None:
    gate = build_gate_from_env({})

    assert _attempt(gate) == (False, "flag_off")


def test_cap_zero_returns_false() -> None:
    gate = _open_gate(cap="0")

    assert _attempt(gate) == (False, "cap_exhausted")


def test_wrong_route_returns_false() -> None:
    gate = _open_gate()

    assert _attempt(gate, route="/api/v1/recommendations/other") == (
        False,
        "wrong_route",
    )


def test_bridge_and_undercited_return_false() -> None:
    gate = _open_gate()

    assert _attempt(gate, family="bridge") == (False, "bridge_family")
    assert _attempt(gate, family="undercited") == (False, "wrong_family")


def test_wrong_limit_returns_false() -> None:
    gate = _open_gate()

    assert _attempt(gate, limit=19) == (False, "wrong_limit")


def test_cohort_ineligible_returns_false() -> None:
    gate = _open_gate()

    assert _attempt(gate, subject=None) == (False, "public_rollout_disabled")
    assert _attempt(gate, subject="not-allowed") == (False, "cohort_ineligible")


def test_public_rollout_disabled_without_canary_returns_false() -> None:
    gate = _open_gate()

    assert gate.is_rollout_subject_eligible(None) == (False, "public_rollout_disabled")


def test_public_rollout_percent_closed_without_stable_subject() -> None:
    gate = build_gate_from_env(
        {
            "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED": "true",
            "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_ENABLED": "true",
            "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_PERCENT": "50",
            "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP": "5",
        }
    )

    assert _attempt(gate, subject=None) == (False, "public_rollout_percent_closed")


def test_public_rollout_percent_100_allows_empty_allowlist_without_subject() -> None:
    gate = build_gate_from_env(
        {
            "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED": "true",
            "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_ENABLED": "true",
            "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_PERCENT": "100",
            "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP": "5",
        }
    )

    assert gate.cohort_allowlist == frozenset()
    assert _attempt(gate, subject=None) == (True, None)


def test_cap_exhausted_returns_false() -> None:
    gate = _open_gate(cap="1")

    assert _attempt(gate, current_served=1) == (False, "cap_exhausted")


def test_all_pre_resolve_gates_pass() -> None:
    gate = _open_gate()

    assert _attempt(gate) == (True, None)


def test_wildcard_allowlist_raises() -> None:
    with pytest.raises(ValueError, match="Wildcard"):
        build_gate_from_env(
            {
                "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED": "true",
                "ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST": "*",
                "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP": "1",
            }
        )


def test_counter_reserve_release_reset_behavior() -> None:
    reset_rollout_served_count()

    assert get_rollout_served_count() == 0
    assert try_reserve_rollout_slot(2) is True
    assert get_rollout_served_count() == 1
    assert try_reserve_rollout_slot(2) is True
    assert get_rollout_served_count() == 2
    assert try_reserve_rollout_slot(2) is False

    release_rollout_slot_for_failure()
    assert get_rollout_served_count() == 1

    reset_rollout_served_count()
    assert get_rollout_served_count() == 0

    release_rollout_slot_for_failure()
    assert get_rollout_served_count() == 0

import json
from pathlib import Path
import re

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline.ml_shadow_scorer_shadow_transition import (
    TRANSITIONS,
    all_plan_modes,
    ordered_transitions_desc,
    transition_by_expect_flag,
    transition_by_plan_mode,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_transition_table_has_exact_committed_revision_ladder() -> None:
    assert len(TRANSITIONS) == 30
    assert [transition.revision for transition in TRANSITIONS] == list(range(1, 31))
    assert ordered_transitions_desc()[0].revision == 30

    for transition in TRANSITIONS:
        assert transition.revision == transition.to_revision
        assert transition.from_revision + 1 == transition.to_revision


def test_plan_modes_are_unique() -> None:
    modes = all_plan_modes()

    assert len(modes) == len(set(modes))
    for mode in modes:
        assert transition_by_plan_mode(mode).plan_mode == mode


def test_transition_constants_resolve_in_bundle_module() -> None:
    for transition in TRANSITIONS:
        assert hasattr(bundle_module, transition.bundle_revision_constant)
        assert hasattr(bundle_module, transition.to_next_stage_constant)
        assert getattr(bundle_module, transition.bundle_revision_constant) == transition.revision


def test_cli_expect_flags_map_to_one_transition_except_plan_not_filed() -> None:
    cli_source = (REPO_ROOT / "services/pipeline/pipeline/cli.py").read_text(encoding="utf-8")
    start = cli_source.index("ml-shadow-scorer-production-scoped-shadow-bundle-verify")
    end = cli_source.index("ml-shadow-scorer-second-candidate-plan-ingest", start)
    verify_parser_source = cli_source[start:end]
    cli_flags = {
        flag.replace("--", "").replace("-", "_")
        for flag in re.findall(r'"(--expect-[a-z0-9-]+(?:not-)?filed)"', verify_parser_source)
    }
    cli_flags.discard("expect_plan_not_filed")

    transition_flags = [transition.expect_flag_name for transition in TRANSITIONS]
    assert set(transition_flags) == cli_flags
    for flag in transition_flags:
        assert flag is not None
        assert transition_flags.count(flag) == 1
        assert transition_by_expect_flag(flag) is not None


def test_committed_rev30_bundle_verifies_with_limited_rollout_grant_expectation() -> None:
    result = bundle_module.verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=REPO_ROOT / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json",
        repo_root=REPO_ROOT,
        expect_limited_production_recommendation_rollout_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == "post_limited_production_recommendation_rollout_grant"
    assert result["bundle_revision"] == 30


def test_rev30_false_limited_rollout_grant_expectation_raises() -> None:
    payload = json.loads(
        (REPO_ROOT / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json").read_text(
            encoding="utf-8"
        )
    )

    with pytest.raises(
        bundle_module.MLShadowScorerProductionScopedShadowBundleError,
        match="limited production recommendation rollout authorization grant must not be filed",
    ):
        bundle_module.verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            payload,
            repo_root=REPO_ROOT,
            expect_limited_production_recommendation_rollout_grant_filed=False,
            verify_local_pilot_files=False,
        )


def test_stripped_rev29_false_limited_rollout_grant_infers_request() -> None:
    payload = json.loads(
        (REPO_ROOT / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json").read_text(
            encoding="utf-8"
        )
    )
    stripped = bundle_module._without_limited_production_recommendation_rollout_grant_payload(payload)

    result = bundle_module.verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=REPO_ROOT,
        expect_limited_production_recommendation_rollout_grant_filed=False,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == "post_limited_production_recommendation_rollout_request"
    assert result["bundle_revision"] == 29


@pytest.mark.parametrize(
    ("mode", "verifier_name"),
    [
        ("post_live_execution_request", "_verify_live_execution_request_payload"),
        ("post_live_execution_grant", "_verify_live_execution_grant_payload"),
        ("post_live_execution_pilot_run", "_verify_live_execution_pilot_run_payload"),
        ("post_live_read_only_pilot_review", "_verify_live_read_only_pilot_review_payload"),
    ],
)
def test_transition_payload_dispatch_uses_resolved_verifier(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    verifier_name: str,
) -> None:
    transition = transition_by_plan_mode(mode)
    assert transition.payload_verifier_name == verifier_name
    called = {}

    def fake_verifier(payload, *, repo_root, verify_local_pilot_files):
        called["payload"] = payload
        called["repo_root"] = repo_root
        called["verify_local_pilot_files"] = verify_local_pilot_files
        return {"verification_mode": mode, "bundle_revision": transition.revision}

    monkeypatch.setattr(bundle_module, "_infer_plan_mode", lambda *_args, **_kwargs: mode)
    monkeypatch.setattr(bundle_module, verifier_name, fake_verifier)
    payload = {
        "metadata": {
            "artifact_type": bundle_module.ARTIFACT_TYPE,
            "bundle_version": bundle_module.BUNDLE_VERSION,
        }
    }

    result = bundle_module.verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=REPO_ROOT,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == mode
    assert result["bundle_revision"] == transition.revision
    assert called["payload"] is payload
    assert called["verify_local_pilot_files"] is False

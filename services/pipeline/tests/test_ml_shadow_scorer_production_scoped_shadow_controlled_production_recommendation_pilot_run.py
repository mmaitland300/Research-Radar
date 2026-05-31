"""Tests for the controlled production recommendation pilot run."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import cli as cli_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline import (
    ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot
    as controlled_pilot_module,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_EXPECTED_FILES,
    CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_PASS_FAIL_CHECKS,
    MLShadowScorerProductionScopedShadowBundleError,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE,
    POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE,
    apply_production_scoped_shadow_controlled_production_recommendation_pilot_run,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_controlled_production_recommendation_pilot import (
    MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotError,
    run_controlled_production_recommendation_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.shadow_write_path_guards import PROD_SCOPED_SHADOW_ROOT
from test_ml_shadow_scorer_production_scoped_shadow_live_execution_pilot_run import (
    FIXTURE_RELS,
    LOCAL_DATABASE_URL,
    REPO_ROOT,
    FakeConnection,
    _copy_fixture_repo,
    _copy_template_repo,
    _live_rows,
    _load,
    _set_path,
    _write_json,
)


def _prepare_rev26_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 29:
        payload = bundle_module._without_limited_production_recommendation_rollout_request_payload(payload)
    if payload["metadata"]["bundle_revision"] == 28:
        payload = bundle_module._without_controlled_production_recommendation_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 27:
        payload = bundle_module._without_controlled_production_recommendation_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] != 26:
        raise AssertionError(
            "expected production-scoped bundle revision 26 grant state after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE:
        raise AssertionError("expected post controlled production recommendation grant next stage")
    _write_json(bundle_path, payload)


def _patch_fake_db(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]) -> FakeConnection:
    conn = FakeConnection(rows)
    monkeypatch.setattr(
        controlled_pilot_module,
        "_assert_live_read_only_database_url",
        lambda _url: {
            "database_target_redacted": "postgresql://research_radar:***@localhost:5432/research_radar",
            "local_database_url_confirmed": True,
            "read_only_contract": "SELECT-only queries; no database mutations",
        },
    )
    monkeypatch.setattr(controlled_pilot_module, "_connect_readonly", lambda _url: conn)
    return conn


@pytest.fixture(scope="module")
def rev26_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("controlled-production-recommendation-pilot-template"))
    _prepare_rev26_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _run_controlled_pilot(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    bundle_path: Path,
    rows: list[dict[str, Any]] | None = None,
    pilot_run_id: str = "prod-controlled-rec-happy",
    update_bundle: bool = True,
) -> dict[str, Any]:
    _patch_fake_db(monkeypatch, rows if rows is not None else _live_rows(root))
    return run_controlled_production_recommendation_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        database_url=LOCAL_DATABASE_URL,
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
        confirm_controlled_production_recommendation_pilot=True,
        confirm_live_read_only_prod_source_reads=True,
        generated_at="2026-05-31T03:00:00Z",
    )


def _valid_slice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    template_root: Path,
    *,
    run_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    root = _copy_template_repo(template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)
    result = _run_controlled_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id=run_id,
        update_bundle=False,
    )
    return before, result["execution"]


def test_happy_path_rev26_to_rev27_records_controlled_response_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    root = _copy_template_repo(rev26_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    result = _run_controlled_pilot(root, monkeypatch, bundle_path=bundle_path)
    updated = _load(bundle_path)
    run_dir = root / PROD_SCOPED_SHADOW_ROOT / "prod-controlled-rec-happy"

    assert result["prod_scoped_shadow_controlled_production_recommendation_pilot_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 27
    assert updated["authorization"] == before["authorization"]
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["review"] == before["review"]
    for key, value in before["execution"].items():
        assert updated["execution"][key] == value
    assert updated["posture"]["prod_scoped_shadow_controlled_production_recommendation_pilot_executed"] is True
    assert updated["posture"]["prod_scoped_shadow_controlled_production_recommendation_pilot_passed"] is True
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["writes_performed"] is False
    assert updated["runtime_writes_performed"] is False
    assert updated["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE

    pilot_run = updated["execution"]["controlled_production_recommendation_pilot_run"]
    assert pilot_run["pilot_surface"] == "bounded_controlled_production_recommendation_prod_scoped_pilot"
    assert pilot_run["input_join_summary"]["joined_candidate_count"] == 528
    assert pilot_run["runtime_drill"]["pilot"]["status"] == "succeeded_controlled_test_client"
    assert pilot_run["runtime_drill"]["pilot"]["shadow_row_count"] == 528
    assert set(CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_PASS_FAIL_CHECKS) == set(
        pilot_run["pass_fail_evaluation"]["checks"]
    )
    assert all(pilot_run["pass_fail_evaluation"]["checks"].values())
    response = pilot_run["controlled_response_summary"]
    assert response["allowed_route"] == "/api/v1/recommendations/ranked"
    assert response["recommendation_family"] == "emerging"
    assert response["response_status_code"] == 200
    assert response["emitted_to_allowlisted_pilot_client"] is True
    assert response["emitted_to_public_users"] is False
    assert response["public_user_traffic_received"] is False
    assert response["response_items_match_shadow_top_k"] is True
    assert len(response["items"]) == 20
    assert response["http_server_bound"] is False
    assert response["outbound_api_route_called"] is False
    assert response["paper_scores_written"] is False
    assert response["ranking_runs_written"] is False
    assert [record["relative_path"] for record in pilot_run["files_written"]] == list(
        CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_EXPECTED_FILES
    )
    assert set(path.name for path in run_dir.iterdir()) == set(
        CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_EXPECTED_FILES
    )

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_controlled_production_recommendation_pilot_run_filed=True,
        verify_local_pilot_files=True,
    )
    assert verified["verification_mode"] == "post_controlled_production_recommendation_pilot_run"


def test_compositional_strip_verifies_as_rev26_grant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    root = _copy_template_repo(rev26_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_controlled_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-controlled-rec-strip")
    payload = _load(bundle_path)

    stripped = bundle_module._without_controlled_production_recommendation_pilot_run_payload(payload)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_controlled_production_recommendation_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == "post_controlled_production_recommendation_grant"
    assert stripped["metadata"]["bundle_revision"] == 26
    assert stripped["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_GRANT_NEXT_STAGE


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 25, "bundle_revision"),
        ("recommended_next_stage", "wrong_stage", "recommended_next_stage"),
    ],
)
def test_rejects_wrong_revision_or_next_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev26_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotError,
        match=message,
    ):
        _run_controlled_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-controlled-rec-bad-precondition",
        )


def test_rejects_double_pilot_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    root = _copy_template_repo(rev26_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_controlled_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-controlled-rec-double")

    with pytest.raises(
        MLShadowScorerProductionScopedShadowControlledProductionRecommendationPilotError,
        match="already been filed",
    ):
        _run_controlled_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-controlled-rec-double-again",
        )


@pytest.mark.parametrize(
    "section_name",
    ["execution", "posture", "shadow_and_production_blockers"],
)
def test_rejects_already_executed_or_passed_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
    section_name: str,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id=f"prod-controlled-rec-valid-slice-{section_name}",
    )
    payload = json.loads(json.dumps(before))
    payload[section_name]["prod_scoped_shadow_controlled_production_recommendation_pilot_executed"] = True

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already|must not"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(payload, pilot_slice)


def test_rejects_failed_pass_fail_evaluation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id="prod-controlled-rec-valid-slice-failed",
    )
    failed = json.loads(json.dumps(pilot_slice))
    failed["pass_fail_evaluation"]["overall_passed"] = False
    failed["pass_fail_evaluation"]["failed_checks"] = ["response_status_200"]
    failed["pass_fail_evaluation"]["checks"]["response_status_200"] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="overall_passed"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(before, failed)


@pytest.mark.parametrize(
    "path",
    [
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
    ],
)
def test_rejects_forbidden_output_or_global_flags_true(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
    path: str,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id=f"prod-controlled-rec-valid-slice-{path.split('.')[-1].replace('_', '-')}",
    )
    payload = json.loads(json.dumps(before))
    _set_path(payload, path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(payload, pilot_slice)


@pytest.mark.parametrize(
    ("slice_path", "value"),
    [
        ("controlled_serving_probe.requested_route", "/api/v1/recommendations/bridge"),
        ("controlled_serving_probe.route_allowlisted", False),
        ("controlled_serving_probe.requested_family", "bridge"),
        ("controlled_serving_probe.family_allowlisted", False),
        ("controlled_response_summary.allowed_route", "/api/v1/recommendations/bridge"),
        ("controlled_response_summary.recommendation_family", "bridge"),
    ],
)
def test_controlled_route_and_family_allowlist_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
    slice_path: str,
    value: Any,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id=f"prod-controlled-rec-allowlist-{slice_path.split('.')[-1].replace('_', '-')}",
    )
    bad = json.loads(json.dumps(pilot_slice))
    _set_path(bad, slice_path, value)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(before, bad)


def test_bridge_family_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id="prod-controlled-rec-bridge-rejected",
    )
    bad = json.loads(json.dumps(pilot_slice))
    bad["controlled_serving_probe"]["bridge_family_probe"]["rejected"] = False

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bridge_family_probe"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(before, bad)


def test_public_user_traffic_true_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id="prod-controlled-rec-public-traffic",
    )
    bad = json.loads(json.dumps(pilot_slice))
    bad["controlled_response_summary"]["public_user_traffic_received"] = True

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="public_user_traffic"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(before, bad)


@pytest.mark.parametrize("target", ["paper_scores", "ranking_runs"])
def test_forbidden_write_counts_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
    target: str,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id=f"prod-controlled-rec-write-{target.replace('_', '-')}",
    )
    bad = json.loads(json.dumps(pilot_slice))
    bad["write_count_verification"]["write_counts_by_isolated_target"][target] = 1

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=target):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(before, bad)


def test_pilot_run_id_collision_with_production_default_pilot_run_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev26_template_root,
        run_id="prod-controlled-rec-collision",
    )
    payload = json.loads(json.dumps(before))
    payload["execution"]["production_default_api_user_visible_pilot_run"]["pilot_run_id"] = pilot_slice[
        "pilot_run_id"
    ]

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="differ"):
        apply_production_scoped_shadow_controlled_production_recommendation_pilot_run(payload, pilot_slice)


def test_verifier_checks_local_artifacts_only_when_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    root = _copy_template_repo(rev26_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_controlled_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-controlled-rec-local-files")
    payload = _load(bundle_path)
    payload["execution"]["controlled_production_recommendation_pilot_run"]["files_written"][0]["sha256"] = "bad"

    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_controlled_production_recommendation_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="sha256"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
            payload,
            repo_root=root,
            expect_controlled_production_recommendation_pilot_run_filed=True,
            verify_local_pilot_files=True,
        )


def test_cli_smoke_run_with_confirmation_then_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev26_template_root: Path,
) -> None:
    root = _copy_template_repo(rev26_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _patch_fake_db(monkeypatch, _live_rows(root))
    old_argv = sys.argv
    try:
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-controlled-production-recommendation-pilot-run",
            "--bundle",
            str(bundle_path),
            "--confirm-controlled-production-recommendation-pilot",
            "--confirm-live-read-only-prod-source-reads",
            "--database-url",
            LOCAL_DATABASE_URL,
            "--pilot-run-id",
            "prod-controlled-rec-cli",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
            "--bundle",
            str(bundle_path),
            "--expect-controlled-production-recommendation-pilot-run-filed",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
    finally:
        sys.argv = old_argv

    assert _load(bundle_path)["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE


def test_committed_bundle_verifies_with_expect_controlled_production_recommendation_pilot_run_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 27:
        pytest.skip("committed production-scoped bundle is not revision 27 yet")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_controlled_production_recommendation_pilot_run_filed=True,
        verify_local_pilot_files=True,
    )
    assert result["bundle_revision"] == 27
    assert result["recommended_next_stage"] == POST_CONTROLLED_PRODUCTION_RECOMMENDATION_PILOT_RUN_NEXT_STAGE

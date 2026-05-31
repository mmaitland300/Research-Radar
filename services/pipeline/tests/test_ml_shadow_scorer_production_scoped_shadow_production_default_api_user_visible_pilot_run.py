"""Tests for the production-scoped production default/API/user-visible pilot run."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline import cli as cli_module
from pipeline import ml_shadow_scorer_production_scoped_shadow_bundle as bundle_module
from pipeline import (
    ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot
    as production_default_pilot_module,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE,
    POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES,
    PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS,
    apply_production_scoped_shadow_production_default_api_user_visible_authorization_grant,
    apply_production_scoped_shadow_production_default_api_user_visible_pilot_run,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot import (
    MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotError,
    run_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot,
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


def _prepare_rev22_template_bundle(bundle_path: Path) -> None:
    payload = _load(bundle_path)
    if payload["metadata"]["bundle_revision"] == 24:
        payload = bundle_module._without_production_default_api_user_visible_pilot_review_payload(payload)
    if payload["metadata"]["bundle_revision"] == 23:
        payload = bundle_module._without_production_default_api_user_visible_pilot_run_payload(payload)
    if payload["metadata"]["bundle_revision"] == 21:
        payload = apply_production_scoped_shadow_production_default_api_user_visible_authorization_grant(
            payload,
            owner_documents_equivalent_review="owner equivalent production default grant review",
        )
    if payload["metadata"]["bundle_revision"] != 22:
        raise AssertionError(
            "expected production-scoped bundle revision 22 grant state after template preparation, "
            f"got {payload['metadata']['bundle_revision']}"
        )
    if payload.get("recommended_next_stage") != POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE:
        raise AssertionError("expected post production default/API/user-visible grant next stage")
    _write_json(bundle_path, payload)


def _patch_fake_db(monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]) -> FakeConnection:
    conn = FakeConnection(rows)
    monkeypatch.setattr(
        production_default_pilot_module,
        "_assert_live_read_only_database_url",
        lambda _url: {
            "database_target_redacted": "postgresql://research_radar:***@localhost:5432/research_radar",
            "local_database_url_confirmed": True,
            "read_only_contract": "SELECT-only queries; no database mutations",
        },
    )
    monkeypatch.setattr(production_default_pilot_module, "_connect_readonly", lambda _url: conn)
    return conn


@pytest.fixture(scope="module")
def rev22_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("production-default-api-user-visible-pilot-template"))
    _prepare_rev22_template_bundle(root / FIXTURE_RELS["production_scoped_bundle"])
    return root


def _run_production_default_pilot(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    bundle_path: Path,
    rows: list[dict[str, Any]] | None = None,
    pilot_run_id: str = "prod-output-happy",
    update_bundle: bool = True,
) -> dict[str, Any]:
    _patch_fake_db(monkeypatch, rows if rows is not None else _live_rows(root))
    return run_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        database_url=LOCAL_DATABASE_URL,
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
        confirm_production_default_api_user_visible_pilot=True,
        confirm_live_read_only_prod_source_reads=True,
        generated_at="2026-05-30T18:00:00Z",
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
    result = _run_production_default_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id=run_id,
        update_bundle=False,
    )
    return before, result["execution"]


def test_happy_path_rev22_to_rev23_records_bounded_probe_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
) -> None:
    root = _copy_template_repo(rev22_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    before = _load(bundle_path)

    result = _run_production_default_pilot(root, monkeypatch, bundle_path=bundle_path)
    updated = _load(bundle_path)
    run_dir = root / PROD_SCOPED_SHADOW_ROOT / "prod-output-happy"

    assert result["prod_scoped_shadow_production_default_api_user_visible_pilot_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 23
    assert updated["authorization"] == before["authorization"]
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["review"] == before["review"]
    for key, value in before["execution"].items():
        assert updated["execution"][key] == value
    assert updated["posture"]["prod_scoped_shadow_production_default_api_user_visible_pilot_executed"] is True
    assert updated["posture"]["prod_scoped_shadow_production_default_api_user_visible_pilot_passed"] is True
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["writes_performed"] is False
    assert updated["runtime_writes_performed"] is False
    assert updated["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE

    pilot_run = updated["execution"]["production_default_api_user_visible_pilot_run"]
    assert pilot_run["pilot_surface"] == "bounded_production_default_api_user_visible_prod_scoped_pilot"
    assert pilot_run["input_join_summary"]["joined_candidate_count"] == 528
    assert pilot_run["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]
    assert pilot_run["runtime_drill"]["pilot"]["shadow_row_count"] == 528
    assert set(PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS) == set(
        pilot_run["pass_fail_evaluation"]["checks"]
    )
    assert all(pilot_run["pass_fail_evaluation"]["checks"].values())
    probe = pilot_run["production_default_api_user_visible_probe"]
    assert probe["api_surface"] == "/api/v1/recommendations/ranked"
    assert probe["bridge_surface_included"] is False
    assert probe["user_visible_response_emitted_to_users"] is False
    assert probe["production_default_changed"] is False
    assert probe["api_web_changed"] is False
    assert probe["paper_scores_written"] is False
    assert probe["ranking_runs_written"] is False
    assert probe["http_server_bound"] is False
    assert probe["outbound_api_route_called"] is False
    assert [record["relative_path"] for record in pilot_run["files_written"]] == list(
        PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES
    )
    assert set(path.name for path in run_dir.iterdir()) == set(
        PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES
    )

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_production_default_api_user_visible_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert verified["verification_mode"] == "post_production_default_api_user_visible_pilot_run"


def test_compositional_verify_strips_rev23_overlay_back_to_rev22_grant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
) -> None:
    root = _copy_template_repo(rev22_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_production_default_pilot(
        root,
        monkeypatch,
        bundle_path=bundle_path,
        pilot_run_id="prod-output-strip",
    )
    payload = _load(bundle_path)

    stripped = bundle_module._without_production_default_api_user_visible_pilot_run_payload(payload)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        stripped,
        repo_root=root,
        expect_production_default_api_user_visible_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert result["verification_mode"] == "post_production_default_api_user_visible_grant"
    assert stripped["metadata"]["bundle_revision"] == 22
    assert stripped["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        ("metadata.bundle_revision", 21, "bundle_revision"),
        ("recommended_next_stage", "wrong_stage", "recommended_next_stage"),
    ],
)
def test_rejects_wrong_revision_or_next_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
    path: str,
    value: Any,
    message: str,
) -> None:
    root = _copy_template_repo(rev22_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    payload = _load(bundle_path)
    _set_path(payload, path, value)
    _write_json(bundle_path, payload)

    with pytest.raises(
        MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotError,
        match=message,
    ):
        _run_production_default_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-output-bad-precondition",
        )


def test_rejects_double_pilot_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
) -> None:
    root = _copy_template_repo(rev22_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _run_production_default_pilot(root, monkeypatch, bundle_path=bundle_path, pilot_run_id="prod-output-double")

    with pytest.raises(
        MLShadowScorerProductionScopedShadowProductionDefaultAPIUserVisiblePilotError,
        match="already been filed",
    ):
        _run_production_default_pilot(
            root,
            monkeypatch,
            bundle_path=bundle_path,
            pilot_run_id="prod-output-double-again",
        )


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
def test_rejects_accidental_output_enablement_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
    path: str,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev22_template_root,
        run_id=f"prod-output-valid-slice-{path.split('.')[-1].replace('_', '-')}",
    )
    payload = json.loads(json.dumps(before))
    _set_path(payload, path, True)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        apply_production_scoped_shadow_production_default_api_user_visible_pilot_run(payload, pilot_slice)


def test_rejects_missing_grant_slices_and_invalid_pass_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
) -> None:
    before, pilot_slice = _valid_slice(
        tmp_path,
        monkeypatch,
        rev22_template_root,
        run_id="prod-output-valid-slice-grant",
    )

    missing_grant = json.loads(json.dumps(before))
    missing_grant["authorization"].pop("production_default_api_user_visible_grant_decision")
    with pytest.raises(
        MLShadowScorerProductionScopedShadowBundleError,
        match="production_default_api_user_visible_grant_decision",
    ):
        apply_production_scoped_shadow_production_default_api_user_visible_pilot_run(
            missing_grant,
            pilot_slice,
        )

    failed = json.loads(json.dumps(pilot_slice))
    failed["pass_fail_evaluation"]["overall_passed"] = False
    failed["pass_fail_evaluation"]["failed_checks"] = ["expected_files_recorded"]
    failed["pass_fail_evaluation"]["checks"]["expected_files_recorded"] = False
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="overall_passed"):
        apply_production_scoped_shadow_production_default_api_user_visible_pilot_run(before, failed)


def test_cli_smoke_run_with_confirmation_then_verify(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rev22_template_root: Path,
) -> None:
    root = _copy_template_repo(rev22_template_root, tmp_path)
    bundle_path = root / FIXTURE_RELS["production_scoped_bundle"]
    _patch_fake_db(monkeypatch, _live_rows(root))
    old_argv = sys.argv
    try:
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-production-default-api-user-visible-pilot-run",
            "--bundle",
            str(bundle_path),
            "--confirm-production-default-api-user-visible-pilot",
            "--confirm-live-read-only-prod-source-reads",
            "--database-url",
            LOCAL_DATABASE_URL,
            "--pilot-run-id",
            "prod-output-cli",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
        sys.argv = [
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
            "--bundle",
            str(bundle_path),
            "--expect-production-default-api-user-visible-pilot-run-filed",
            "--repo-root",
            str(root),
        ]
        cli_module.main()
    finally:
        sys.argv = old_argv

    assert _load(bundle_path)["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE


def test_committed_bundle_verifies_with_expect_production_default_api_user_visible_pilot_run_filed() -> None:
    committed = REPO_ROOT / FIXTURE_RELS["production_scoped_bundle"]
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    payload = _load(committed)
    if payload["metadata"]["bundle_revision"] != 23:
        pytest.skip("committed production-scoped bundle is not revision 23 yet")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_production_default_api_user_visible_pilot_run_filed=True,
        verify_local_pilot_files=False,
    )
    assert result["bundle_revision"] == 23
    assert result["recommended_next_stage"] == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE

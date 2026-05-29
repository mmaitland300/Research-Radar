"""Tests for requesting production-scoped live read-only shadow authorization."""

from __future__ import annotations

import builtins
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    LIVE_READ_ONLY_GRANT_AUTHORIZES_FOR_CHAIN_ONLY,
    LIVE_READ_ONLY_GRANT_CAVEATS,
    LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED,
    LIVE_READ_ONLY_GRANT_TIME_BOUNDARIES,
    LIVE_READ_ONLY_GRANT_SCOPE,
    LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED,
    LIVE_READ_ONLY_REQUEST_CAVEATS,
    LIVE_READ_ONLY_REQUEST_FUTURE_GRANT_REQUIREMENTS,
    LIVE_READ_ONLY_REQUEST_SCOPE,
    MLShadowScorerProductionScopedShadowBundleError,
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle,
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    prove_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot import (
    run_ml_shadow_scorer_production_scoped_shadow_pilot,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness import (
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness_review import (
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_review import (
    review_ml_shadow_scorer_production_scoped_shadow_pilot,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "production_readiness_bundle": "docs/audit/bundles/production-readiness-v1/bundle.json",
    "production_readiness_bundle_md": "docs/audit/bundles/production-readiness-v1/bundle.md",
    "production_readiness_criteria": "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json",
    "phase2_bundle": "docs/audit/bundles/phase2-v1/bundle.json",
    "phase2_bundle_md": "docs/audit/bundles/phase2-v1/bundle.md",
    "online_shadow_policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
    "execution_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "phase2_write_mode_plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "phase2_write_mode_proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "generalization_audit_gates": "docs/audit/ml-shadow-scorer-v1-generalization-audit-gates-v1.json",
    "production_readiness_plan": "docs/audit/ml-production-readiness-plan-v1.json",
    "production_readiness_plan_md": "docs/audit/ml-production-readiness-plan-v1.md",
    "phase2_write_authorization_request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "phase2_write_authorization_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "phase1_review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "learned_probability": "docs/audit/ml-shadow-scorer-v1-second-surface-learned-probability-v1.json",
    "generalization_audit": "docs/audit/ml-shadow-scorer-v1-second-surface-generalization-audit-v1.json",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _copy_template_repo(template_root: Path, tmp_path: Path) -> Path:
    root = tmp_path / "case-repo"
    shutil.copytree(template_root, root)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _set_path(payload: dict[str, Any], dotted_path: str, value: Any) -> None:
    current: dict[str, Any] = payload
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        current = current[part]
    current[parts[-1]] = value


def _shadow_runs_files(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*") if path.is_file()}


def _optional_kwargs(root: Path) -> dict[str, Path]:
    return {
        "execution_authorization_grant_path": _fixture(root, "execution_authorization_grant"),
        "phase2_write_mode_plan_path": _fixture(root, "phase2_write_mode_plan"),
        "phase2_write_mode_proof_path": _fixture(root, "phase2_write_mode_proof"),
        "generalization_audit_gates_path": _fixture(root, "generalization_audit_gates"),
    }


def _write_pilot_review_bundle(root: Path) -> Path:
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    markdown_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    write_ml_shadow_scorer_production_scoped_shadow_bundle(
        production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
        output_path=bundle_path,
        markdown_output_path=markdown_path,
        repo_root=root,
        **_optional_kwargs(root),
    )
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id="live-request-proof",
        repo_root=root,
    )
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review=(
            "Owner reviewed the production-scoped proof, pilot request, and bounded pilot contract."
        ),
        repo_root=root,
    )
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        pilot_run_id="live-request-harness",
        repo_root=root,
    )
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        reviewer="Harness Reviewer",
        repo_root=root,
    )
    run_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        learned_probability_artifact_path=_fixture(root, "learned_probability"),
        second_surface_generalization_audit_path=_fixture(root, "generalization_audit"),
        pilot_run_id="live-request-pilot",
        repo_root=root,
    )
    review_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path,
        reviewer="Pilot Reviewer",
        repo_root=root,
        reviewed_at="2026-05-29T21:30:00Z",
    )
    return bundle_path


@pytest.fixture(scope="module")
def pilot_review_template_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = _copy_fixture_repo(tmp_path_factory.mktemp("live-read-only-template"))
    _write_pilot_review_bundle(root)
    return root


def _write_live_read_only_request_bundle(root: Path) -> Path:
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Live Requester",
        request_notes="live read-only request notes",
        repo_root=root,
    )
    return bundle_path


def test_happy_path_request_from_revision_eight_to_revision_nine(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    requested = request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        requester="Live Requester",
        request_notes="live read-only request notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_request_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 8
    assert before["authorization"]["prod_scoped_shadow_live_read_only_authorization_requested"] is False
    assert requested["metadata"]["bundle_revision"] == 9
    assert requested["plan"] == before["plan"]
    assert requested["proof"] == before["proof"]
    assert requested["execution"] == before["execution"]
    assert requested["review"] == before["review"]
    assert requested["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert requested["authorization"]["prod_scoped_shadow_pilot_execution_authorized"] is True
    assert requested["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert requested["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_authorization_requested"] is True
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_authorization_granted"] is False
    assert requested["authorization"]["prod_scoped_shadow_live_read_only_authorized"] is False
    assert requested["authorization"]["request_decision"]["decision"] == "requested"
    assert requested["authorization"]["request_decision"]["requester"] == "Live Requester"
    assert requested["authorization"]["request_decision"]["request_notes"] == "live read-only request notes"
    assert requested["authorization"]["requested_scope"]["authorization_scope"] == LIVE_READ_ONLY_REQUEST_SCOPE
    assert set(LIVE_READ_ONLY_REQUEST_FUTURE_GRANT_REQUIREMENTS).issubset(
        requested["authorization"]["requested_scope"]["future_grant_would_require"]
    )
    assert set(LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED).issubset(
        requested["authorization"]["requested_scope"]["explicitly_not_included"]
    )
    assert requested["posture"]["live_prod_source_reads_performed"] is False
    assert requested["posture"]["missing_prod_scoped_shadow_live_read_only_authorization"] is True
    assert requested["shadow_and_production_blockers"]["blockers_introduced_by_live_read_only_request"] == [
        "missing_prod_scoped_shadow_live_read_only_authorization"
    ]
    assert requested["shadow_and_production_blockers"]["blockers_cleared_by_live_read_only_request"] == []
    assert requested["shadow_and_production_blockers"]["blockers_unchanged_by_live_read_only_request"] is True
    assert "blockers_changed_by_live_read_only_request" not in requested["shadow_and_production_blockers"]
    assert requested["recommended_next_stage"] == (
        "record_production_scoped_online_shadow_live_read_only_authorization_grant_v1"
    )
    assert "Bundle live-read-only request milestone only; grants no live production source access." in requested["caveats"]
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_live_read_only_request"


def test_wrong_revision_rejection(tmp_path: Path, pilot_review_template_root: Path) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    payload = _load(bundle_path)
    payload["metadata"]["bundle_revision"] = 7
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_double_request_rejection(tmp_path: Path, pilot_review_template_root: Path) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )


def test_verifier_rejects_any_live_read_performed_flag(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    payload = _load(bundle_path)
    payload["execution"]["pilot_run"]["live_prod_source_reads_performed"] = True
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_prod_source_reads_performed"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_request_filed=True,
            verify_local_pilot_files=False,
        )


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        "authorization.prod_scoped_shadow_live_execution_authorized",
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
    ],
)
def test_verifier_rejects_accidental_grant_live_global_default_api_or_user_visible_enablement(
    tmp_path: Path,
    pilot_review_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=field_path.split(".")[-1]):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_request_filed=True,
            verify_local_pilot_files=False,
        )


def test_request_does_not_import_runtime_or_database_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    original_import = builtins.__import__
    forbidden = {
        "psycopg",
        "openai",
        "openalex",
        "sklearn",
        "pipeline.ml_shadow_scorer_online_shadow_runtime",
    }

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name in forbidden:
            raise AssertionError(f"live read-only request must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )


def test_request_does_not_open_shadow_runs_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"live read-only request must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
    )


def test_cli_smoke_request_live_read_only_then_verify(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    request_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-request-live-read-only",
        "--bundle",
        str(bundle_path),
        "--requester",
        "CLI Live Requester",
        "--request-notes",
        "cli live request notes",
        "--repo-root",
        str(root),
    ]
    requested = subprocess.run(request_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert requested.stdout.splitlines() == [
        "requested",
        "True",
        "record_production_scoped_online_shadow_live_read_only_authorization_grant_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-live-read-only-request-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_live_read_only_request",
        "online-shadow-production-scoped-v1",
        "record_production_scoped_online_shadow_live_read_only_authorization_grant_v1",
    ]


def test_happy_path_grant_from_revision_nine_to_revision_ten(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    before = _load(bundle_path)
    shadow_before = _shadow_runs_files(root)

    granted = grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner="Live Grant Owner",
        second_reviewer="Live Grant Reviewer",
        grant_notes="live read-only grant notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_live_read_only_grant_filed=True,
        verify_local_pilot_files=False,
    )

    assert before["metadata"]["bundle_revision"] == 9
    assert granted["metadata"]["bundle_revision"] == 10
    assert granted["plan"] == before["plan"]
    assert granted["proof"] == before["proof"]
    assert granted["execution"] == before["execution"]
    assert granted["review"] == before["review"]
    assert granted["metadata"]["legacy_artifacts_index"] == before["metadata"]["legacy_artifacts_index"]
    assert granted["authorization"]["grant_decision"] == before["authorization"]["grant_decision"]
    assert granted["authorization"]["granted_scope"] == before["authorization"]["granted_scope"]
    assert granted["authorization"]["request_decision"] == before["authorization"]["request_decision"]
    assert granted["authorization"]["requested_scope"] == before["authorization"]["requested_scope"]
    assert granted["authorization"]["prod_scoped_shadow_live_read_only_authorization_requested"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_read_only_authorization_granted"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_read_only_authorized"] is True
    assert granted["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert granted["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert granted["authorization"]["live_read_only_grant_decision"]["decision"] == "granted"
    assert granted["authorization"]["live_read_only_grant_decision"]["owner"] == "Live Grant Owner"
    assert granted["authorization"]["live_read_only_grant_decision"]["second_reviewer"] == "Live Grant Reviewer"
    assert granted["authorization"]["live_read_only_grant_decision"]["grant_notes"] == "live read-only grant notes"
    assert granted["authorization"]["live_read_only_granted_scope"]["authorization_scope"] == LIVE_READ_ONLY_GRANT_SCOPE
    assert set(LIVE_READ_ONLY_GRANT_AUTHORIZES_FOR_CHAIN_ONLY).issubset(
        granted["authorization"]["live_read_only_granted_scope"]["authorizes_for_chain_only"]
    )
    assert set(LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED).issubset(
        granted["authorization"]["live_read_only_granted_scope"]["explicitly_still_not_included"]
    )
    assert set(LIVE_READ_ONLY_GRANT_TIME_BOUNDARIES).issubset(
        granted["authorization"]["live_read_only_granted_scope"]["grant_time_live_read_boundaries"]
    )
    assert granted["posture"]["missing_prod_scoped_shadow_live_read_only_authorization"] is False
    assert granted["posture"]["live_prod_source_reads_performed"] is False
    assert granted["shadow_and_production_blockers"]["blockers_cleared_by_live_read_only_grant"] == [
        "missing_prod_scoped_shadow_live_read_only_authorization"
    ]
    assert granted["shadow_and_production_blockers"]["blockers_introduced_by_live_read_only_grant"] == []
    assert granted["shadow_and_production_blockers"]["blockers_unchanged_by_live_read_only_grant"] is True
    assert "blockers_changed_by_live_read_only_grant" not in granted["shadow_and_production_blockers"]
    assert granted["recommended_next_stage"] == "run_production_scoped_online_shadow_live_read_only_pilot_v1"
    assert set(LIVE_READ_ONLY_GRANT_CAVEATS).issubset(granted["caveats"])
    assert all(caveat not in granted["caveats"] for caveat in LIVE_READ_ONLY_REQUEST_CAVEATS)
    assert _shadow_runs_files(root) == shadow_before
    assert result["verification_mode"] == "post_live_read_only_grant"


def test_live_read_only_grant_rejects_wrong_revision(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent live read-only review",
            repo_root=root,
        )


def test_live_read_only_grant_rejects_double_grant(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="already been filed"):
        grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent live read-only review",
            repo_root=root,
        )


def test_live_read_only_grant_requires_independent_or_equivalent_review(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="requires second_reviewer"):
        grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
        )

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="second_reviewer"):
        grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner="Same Reviewer",
            second_reviewer="Same Reviewer",
            repo_root=root,
        )


def test_live_read_only_grant_rejects_incomplete_request_scope(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    payload = _load(bundle_path)
    payload["authorization"]["requested_scope"]["future_grant_would_require"].remove(
        LIVE_READ_ONLY_REQUEST_FUTURE_GRANT_REQUIREMENTS[0]
    )
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="future_grant_would_require"):
        grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            owner_documents_equivalent_review="owner equivalent live read-only review",
            repo_root=root,
        )


def test_live_read_only_grant_verifier_rejects_any_live_read_performed_flag(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    payload["execution"]["pilot_run"]["live_prod_source_reads_performed"] = True
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="live_prod_source_reads_performed"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_grant_filed=True,
            verify_local_pilot_files=False,
        )


@pytest.mark.parametrize(
    "field_path",
    [
        "authorization.prod_scoped_shadow_live_execution_authorized",
        "authorization.prod_scoped_shadow_execution_authorized",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "writes_performed",
        "runtime_writes_performed",
    ],
)
def test_live_read_only_grant_verifier_rejects_global_default_api_user_visible_or_write_enablement(
    tmp_path: Path,
    pilot_review_template_root: Path,
    field_path: str,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    _set_path(payload, field_path, True)
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match=field_path.split(".")[-1]):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_grant_filed=True,
            verify_local_pilot_files=False,
        )


def test_live_read_only_grant_rejects_stale_request_only_caveat(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )
    payload = _load(bundle_path)
    payload["caveats"].append("Bundle live-read-only request milestone only; grants no live production source access.")
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="request-only"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_live_read_only_grant_filed=True,
            verify_local_pilot_files=False,
        )


def test_live_read_only_grant_does_not_import_runtime_or_database_modules(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    original_import = builtins.__import__
    forbidden = {
        "psycopg",
        "openai",
        "openalex",
        "sklearn",
        "pipeline.ml_shadow_scorer_online_shadow_runtime",
    }

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name in forbidden:
            raise AssertionError(f"live read-only grant must not import {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )


def test_live_read_only_grant_does_not_open_shadow_runs_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    original_open = Path.open

    def guarded_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        normalized = self.resolve().as_posix()
        if "/docs/audit/shadow-runs/" in normalized:
            raise AssertionError(f"live read-only grant must not open shadow-runs path: {self}")
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent live read-only review",
        repo_root=root,
    )


def test_cli_smoke_grant_live_read_only_then_verify(
    tmp_path: Path,
    pilot_review_template_root: Path,
) -> None:
    root = _copy_template_repo(pilot_review_template_root, tmp_path)
    bundle_path = _write_live_read_only_request_bundle(root)
    grant_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-grant-live-read-only",
        "--bundle",
        str(bundle_path),
        "--owner-documents-equivalent-review",
        "Owner reviewed the live read-only request and grant contract.",
        "--repo-root",
        str(root),
    ]
    granted = subprocess.run(grant_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert granted.stdout.splitlines() == [
        "granted",
        "True",
        "run_production_scoped_online_shadow_live_read_only_pilot_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(bundle_path),
        "--expect-live-read-only-grant-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_live_read_only_grant",
        "online-shadow-production-scoped-v1",
        "run_production_scoped_online_shadow_live_read_only_pilot_v1",
    ]

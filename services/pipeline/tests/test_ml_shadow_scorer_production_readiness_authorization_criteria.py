"""Tests for production-readiness authorization criteria artifacts."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_production_readiness_authorization_criteria import (
    MLShadowScorerProductionReadinessAuthorizationCriteriaError,
    build_ml_shadow_scorer_production_readiness_authorization_criteria_payload,
    verify_ml_shadow_scorer_production_readiness_authorization_criteria,
    verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload,
    write_ml_shadow_scorer_production_readiness_authorization_criteria,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

FIXTURE_RELS = {
    "bundle": "docs/audit/bundles/phase2-v1/bundle.json",
    "bundle_md": "docs/audit/bundles/phase2-v1/bundle.md",
    "plan": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1.json",
    "proof": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1.json",
    "request": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1.json",
    "grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1.json",
    "review": "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json",
    "prior_grant": "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json",
    "policy": "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json",
    "superseded_plan": "docs/audit/ml-production-readiness-plan-v1.md",
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        dest = root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
    return root


def _fixture(root: Path, key: str) -> Path:
    return root / FIXTURE_RELS[key]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _shadow_runs_files(root: Path) -> set[str]:
    shadow_root = root / "docs/audit/shadow-runs"
    if not shadow_root.exists():
        return set()
    return {str(path.relative_to(root)).replace("\\", "/") for path in shadow_root.rglob("*") if path.is_file()}


def _write_criteria(root: Path) -> Path:
    out_json = root / "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json"
    out_md = root / "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.md"
    write_ml_shadow_scorer_production_readiness_authorization_criteria(
        phase_bundle_path=_fixture(root, "bundle"),
        superseded_production_readiness_plan_path=_fixture(root, "superseded_plan"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
    )
    return out_json


def test_happy_path_writes_json_and_markdown_from_revision_3_reviewed_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json"
    out_md = root / "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.md"

    payload = write_ml_shadow_scorer_production_readiness_authorization_criteria(
        phase_bundle_path=_fixture(root, "bundle"),
        superseded_production_readiness_plan_path=_fixture(root, "superseded_plan"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
    )
    persisted = _load(out_json)
    verified = verify_ml_shadow_scorer_production_readiness_authorization_criteria(
        criteria_path=out_json,
        repo_root=root,
    )

    assert persisted["metadata"]["artifact_type"] == "ml_shadow_scorer_production_readiness_authorization_criteria"
    assert persisted["production_readiness_criteria_defined"] is True
    assert persisted["phase2_write_pilot_review_accepted"] is True
    assert persisted["recommended_next_stage"] == "request_production_readiness_authorization_v1"
    assert persisted == payload
    assert verified["verification_status"] == "passed"
    assert "Production Readiness Authorization Criteria" in out_md.read_text(encoding="utf-8")


def test_rejects_bundle_not_reviewed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_fixture(root, "bundle"))
    bundle["metadata"]["bundle_revision"] = 2
    bundle["review"] = {
        "phase2_write_pilot_reviewed": False,
        "phase2_write_pilot_accepted": None,
    }
    bundle["recommended_next_stage"] = "review_online_shadow_phase2_isolated_audit_write_pilot_v1"
    _write_json(_fixture(root, "bundle"), bundle)

    with pytest.raises(MLShadowScorerProductionReadinessAuthorizationCriteriaError, match="phase2_write_pilot_reviewed"):
        build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
            phase_bundle_path=_fixture(root, "bundle"),
            repo_root=root,
        )


def test_rejects_bundle_review_not_accepted(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_fixture(root, "bundle"))
    bundle["review"]["phase2_write_pilot_accepted"] = False
    bundle["review"]["review_decision"]["decision"] = "not_accepted"
    bundle["recommended_next_stage"] = "remediate_online_shadow_phase2_write_pilot_v1"
    _write_json(_fixture(root, "bundle"), bundle)

    with pytest.raises(MLShadowScorerProductionReadinessAuthorizationCriteriaError, match="phase2_write_pilot_accepted"):
        build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
            phase_bundle_path=_fixture(root, "bundle"),
            repo_root=root,
        )


def test_rejects_wrong_recommended_next_stage(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_fixture(root, "bundle"))
    bundle["recommended_next_stage"] = "request_production_readiness_authorization_v1"
    _write_json(_fixture(root, "bundle"), bundle)

    with pytest.raises(MLShadowScorerProductionReadinessAuthorizationCriteriaError, match="recommended_next_stage"):
        build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
            phase_bundle_path=_fixture(root, "bundle"),
            repo_root=root,
        )


def test_rejects_pinned_identity_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle = _load(_fixture(root, "bundle"))
    bundle["posture"]["pinned_identity"]["scorer_id"] = "wrong-scorer"
    _write_json(_fixture(root, "bundle"), bundle)

    with pytest.raises(MLShadowScorerProductionReadinessAuthorizationCriteriaError, match="scorer_id"):
        build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
            phase_bundle_path=_fixture(root, "bundle"),
            repo_root=root,
        )


def test_rejects_phase_bundle_sha_reference_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
        phase_bundle_path=_fixture(root, "bundle"),
        superseded_production_readiness_plan_path=_fixture(root, "superseded_plan"),
        repo_root=root,
    )
    payload["metadata"]["inputs"][0]["sha256"] = "0" * 64

    with pytest.raises(MLShadowScorerProductionReadinessAuthorizationCriteriaError, match="sha256 mismatch"):
        verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload(payload, repo_root=root)


def test_criteria_grants_nothing_invariants(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
        phase_bundle_path=_fixture(root, "bundle"),
        superseded_production_readiness_plan_path=_fixture(root, "superseded_plan"),
        repo_root=root,
    )

    assert payload["production_readiness_authorization_requested"] is False
    assert payload["production_readiness_authorization_granted"] is False
    assert payload["production_default_allowed"] is False
    assert payload["api_web_changes_allowed"] is False
    assert payload["user_visible_ranking_changed"] is False
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["runtime_execution_performed"] is False
    assert payload["writes_performed"] is False
    assert payload["runtime_writes_performed"] is False
    assert payload["missing_production_readiness_authorization"] is True
    assert payload["criteria_artifact_grants_nothing"] is True
    assert payload["shadow_and_production_blockers"]["blockers_changed_by_criteria"] == []


def test_superseded_plan_reconciliation_present_when_path_provided(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    payload = build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
        phase_bundle_path=_fixture(root, "bundle"),
        superseded_production_readiness_plan_path=_fixture(root, "superseded_plan"),
        repo_root=root,
    )
    reconciliation = payload["superseded_plan_reconciliation"]

    assert reconciliation["superseded_plan_path"] == FIXTURE_RELS["superseded_plan"]
    assert reconciliation["superseded_plan_sha256"]
    assert reconciliation["superseded_by"] == "ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1"
    assert any("predates the accepted Phase 2 shadow ladder" in item for item in reconciliation["reconciliation_summary"])
    assert any("grants no production behavior" in item for item in reconciliation["reconciliation_summary"])


def test_no_runtime_calls_shadow_runs_writes_or_forbidden_imports(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_files(root)
    bundle_before = _fixture(root, "bundle").read_text(encoding="utf-8")
    plan_before = _fixture(root, "superseded_plan").read_text(encoding="utf-8")

    _write_criteria(root)

    assert _shadow_runs_files(root) == before
    assert _fixture(root, "bundle").read_text(encoding="utf-8") == bundle_before
    assert _fixture(root, "superseded_plan").read_text(encoding="utf-8") == plan_before
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_readiness_authorization_criteria.py"
    ).read_text(encoding="utf-8")
    assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in module_source
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.json"
    out_md = root / "docs/audit/ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-readiness-authorization-criteria",
        "--phase-bundle",
        str(_fixture(root, "bundle")),
        "--superseded-production-readiness-plan",
        str(_fixture(root, "superseded_plan")),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(root),
    ]

    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    persisted = _load(out_json)

    assert result.stdout.splitlines() == [
        "True",
        "True",
        "request_production_readiness_authorization_v1",
    ]
    assert persisted["production_readiness_criteria_defined"] is True
    assert out_md.exists()


def test_cli_has_no_database_url_for_criteria_command() -> None:
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    command_start = cli_source.index('"ml-shadow-scorer-production-readiness-authorization-criteria"')
    next_command = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', command_start)
    assert "--database-url" not in cli_source[command_start:next_command]

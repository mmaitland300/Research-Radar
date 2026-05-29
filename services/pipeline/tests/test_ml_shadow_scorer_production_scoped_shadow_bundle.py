"""Tests for the production-scoped online shadow plan bundle."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeline.ml_shadow_scorer_phase_bundle import verify_ml_shadow_scorer_phase_bundle
from pipeline.ml_shadow_scorer_production_readiness_bundle import (
    verify_ml_shadow_scorer_production_readiness_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    PLAN_SUBSECTIONS,
    apply_production_scoped_shadow_plan,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
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
}


def _copy_fixture_repo(tmp_path: Path) -> Path:
    root = tmp_path / "fixture-repo"
    for rel in sorted(FIXTURE_RELS.values()):
        src = REPO_ROOT / rel
        if not src.exists():
            continue
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _write_pre_plan_bundle(root: Path) -> Path:
    out_json = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    write_ml_shadow_scorer_production_scoped_shadow_bundle(
        production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
        phase_bundle_path=_fixture(root, "phase2_bundle"),
        online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
        output_path=out_json,
        markdown_output_path=out_md,
        repo_root=root,
        **_optional_kwargs(root),
    )
    return out_json


def _write_plan_bundle(root: Path) -> Path:
    bundle_path = _write_pre_plan_bundle(root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    return bundle_path


def test_assemble_pre_plan_revision_zero_and_verify_not_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)
    payload = _load(bundle_path)
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_plan_filed=False,
    )

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_production_scoped_shadow_bundle"
    assert payload["metadata"]["bundle_revision"] == 0
    assert payload["plan"]["prod_scoped_shadow_plan_defined"] is False
    assert payload["recommended_next_stage"] == "begin_production_scoped_online_shadow_plan_v1"
    assert result["verification_mode"] == "pre_plan"


def test_apply_plan_revision_one_and_verify_plan_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)

    planned = plan_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        planner="Planner Name",
        plan_notes="plan notes",
        repo_root=root,
    )
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_plan_filed=True,
    )

    assert planned["metadata"]["bundle_revision"] == 1
    assert planned["plan"]["prod_scoped_shadow_plan_defined"] is True
    assert planned["plan"]["plan_decision"]["planner"] == "Planner Name"
    assert planned["plan"]["plan_decision"]["plan_notes"] == "plan notes"
    assert planned["posture"]["missing_prod_scoped_shadow_proof"] is True
    assert planned["posture"]["prod_scoped_shadow_proof_authorized"] is False
    assert planned["authorization"]["prod_scoped_shadow_proof_authorized"] is False
    assert planned["recommended_next_stage"] == "implement_production_scoped_online_shadow_proof_v1"
    assert result["verification_mode"] == "post_plan"


def test_rejects_plan_apply_on_already_plan_filed_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)


def test_rejects_if_production_readiness_bundle_not_grant_filed(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    production_readiness = _load(_fixture(root, "production_readiness_bundle"))
    production_readiness["metadata"]["bundle_revision"] = 1
    production_readiness["authorization"]["production_readiness_authorization_granted"] = False
    production_readiness["posture"]["missing_production_readiness_authorization"] = True
    production_readiness["recommended_next_stage"] = "record_production_readiness_authorization_grant_v1"
    _write_json(_fixture(root, "production_readiness_bundle"), production_readiness)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        _write_pre_plan_bundle(root)


def test_rejects_if_phase2_bundle_not_accepted(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    phase2_copy = root / "docs/audit/bundles/phase2-v1/not-accepted.json"
    phase2 = _load(_fixture(root, "phase2_bundle"))
    phase2["review"]["phase2_write_pilot_accepted"] = False
    phase2["review"]["review_decision"]["decision"] = "not_accepted"
    _write_json(phase2_copy, phase2)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        write_ml_shadow_scorer_production_scoped_shadow_bundle(
            production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
            phase_bundle_path=phase2_copy,
            online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
            output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json",
            markdown_output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md",
            repo_root=root,
            **_optional_kwargs(root),
        )


def test_rejects_sha_tamper_and_identity_mismatch(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)
    payload = _load(bundle_path)
    payload["metadata"]["legacy_artifacts_index"][0]["sha256"] = "0" * 64
    _write_json(bundle_path, payload)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="sha256 mismatch"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_plan_filed=False,
        )

    phase2_copy = root / "docs/audit/bundles/phase2-v1/identity-mismatch.json"
    phase2 = _load(_fixture(root, "phase2_bundle"))
    phase2["posture"]["pinned_identity"]["family"] = "mismatch"
    _write_json(phase2_copy, phase2)
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError):
        write_ml_shadow_scorer_production_scoped_shadow_bundle(
            production_readiness_bundle_path=_fixture(root, "production_readiness_bundle"),
            phase_bundle_path=phase2_copy,
            online_shadow_policy_path=_fixture(root, "online_shadow_policy"),
            output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/identity.json",
            markdown_output_path=root / "docs/audit/bundles/production-scoped-shadow-v1/identity.md",
            repo_root=root,
            **_optional_kwargs(root),
        )


def test_plan_subsections_populated_and_caveats_do_not_imply_enablement(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)
    payload = _load(bundle_path)

    for subsection in PLAN_SUBSECTIONS:
        assert payload["plan"][subsection]
    assert payload["posture"]["online_shadow_execution_enabled"] is False
    assert payload["posture"]["production_default_allowed"] is False
    assert payload["posture"]["api_web_changes_allowed"] is False
    assert payload["posture"]["user_visible_ranking_changed"] is False
    caveat_text = " ".join(payload["caveats"]).lower()
    assert "authorizes proof execution" not in caveat_text
    assert "authorizes pilot" not in caveat_text
    assert "enables online shadow" not in caveat_text


def test_verify_rejects_missing_plan_subsection_post_plan(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_plan_bundle(root)
    payload = _load(bundle_path)
    del payload["plan"]["observability_and_slo_plan"]
    _write_json(bundle_path, payload)

    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="observability_and_slo_plan"):
        verify_ml_shadow_scorer_production_scoped_shadow_bundle(
            bundle_path=bundle_path,
            repo_root=root,
            expect_plan_filed=True,
        )


def test_no_shadow_runs_writes_or_upstream_mutation(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    before = _shadow_runs_files(root)
    production_readiness_before = _fixture(root, "production_readiness_bundle").read_text(encoding="utf-8")
    phase2_before = _fixture(root, "phase2_bundle").read_text(encoding="utf-8")

    _write_plan_bundle(root)

    assert _shadow_runs_files(root) == before
    assert _fixture(root, "production_readiness_bundle").read_text(encoding="utf-8") == production_readiness_before
    assert _fixture(root, "phase2_bundle").read_text(encoding="utf-8") == phase2_before


def test_apply_plan_in_memory_requires_revision_zero() -> None:
    payload = {
        "metadata": {"bundle_revision": 1},
        "plan": {"prod_scoped_shadow_plan_defined": True},
    }
    with pytest.raises(MLShadowScorerProductionScopedShadowBundleError, match="bundle_revision"):
        apply_production_scoped_shadow_plan(payload)


def test_cli_smoke_assemble_plan_verify(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    out_json = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    out_md = root / "docs/audit/bundles/production-scoped-shadow-v1/bundle.md"
    assemble_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-assemble",
        "--production-readiness-bundle",
        str(_fixture(root, "production_readiness_bundle")),
        "--phase-bundle",
        str(_fixture(root, "phase2_bundle")),
        "--online-shadow-policy",
        str(_fixture(root, "online_shadow_policy")),
        "--execution-authorization-grant",
        str(_fixture(root, "execution_authorization_grant")),
        "--phase2-write-mode-plan",
        str(_fixture(root, "phase2_write_mode_plan")),
        "--phase2-write-mode-proof",
        str(_fixture(root, "phase2_write_mode_proof")),
        "--generalization-audit-gates",
        str(_fixture(root, "generalization_audit_gates")),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
        "--repo-root",
        str(root),
    ]
    assemble = subprocess.run(assemble_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert assemble.stdout.splitlines() == [
        "online-shadow-production-scoped-v1",
        "False",
        "begin_production_scoped_online_shadow_plan_v1",
    ]

    plan_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-plan",
        "--bundle",
        str(out_json),
        "--planner",
        "CLI Planner",
        "--plan-notes",
        "cli plan notes",
        "--repo-root",
        str(root),
    ]
    planned = subprocess.run(plan_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert planned.stdout.splitlines() == [
        "planned",
        "True",
        "implement_production_scoped_online_shadow_proof_v1",
    ]

    verify_cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
        "--bundle",
        str(out_json),
        "--expect-plan-filed",
        "--repo-root",
        str(root),
    ]
    verified = subprocess.run(verify_cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert verified.stdout.splitlines() == [
        "passed",
        "post_plan",
        "online-shadow-production-scoped-v1",
        "implement_production_scoped_online_shadow_proof_v1",
    ]


def test_upstream_verifiers_still_pass(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    phase2_result = verify_ml_shadow_scorer_phase_bundle(
        bundle_path=_fixture(root, "phase2_bundle"),
        repo_root=root,
        expect_pilot_reviewed=True,
    )
    production_readiness_result = verify_ml_shadow_scorer_production_readiness_bundle(
        bundle_path=_fixture(root, "production_readiness_bundle"),
        repo_root=root,
        expect_grant_filed=True,
    )

    assert phase2_result["verification_mode"] == "post_review"
    assert production_readiness_result["verification_mode"] == "post_grant"


def test_committed_bundle_fixture_matches_post_plan_if_present() -> None:
    committed = REPO_ROOT / "docs/audit/bundles/production-scoped-shadow-v1/bundle.json"
    if not committed.exists():
        pytest.skip("production-scoped-shadow bundle not generated yet")
    result = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=committed,
        repo_root=REPO_ROOT,
        expect_plan_filed=True,
    )
    assert result["bundle_revision"] == 1
    assert result["recommended_next_stage"] == "implement_production_scoped_online_shadow_proof_v1"


def test_payload_verifier_infers_plan_mode(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_pre_plan_bundle(root)
    pre = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    post = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(_load(bundle_path), repo_root=root)

    assert pre["verification_mode"] == "pre_plan"
    assert post["verification_mode"] == "post_plan"


def test_no_forbidden_imports_or_database_url_on_bundle_cli() -> None:
    module_source = (PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_production_scoped_shadow_bundle.py").read_text(
        encoding="utf-8"
    )
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert f"import {forbidden}" not in module_source
        assert f"from {forbidden}" not in module_source
    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    assemble_start = cli_source.index('"ml-shadow-scorer-production-scoped-shadow-bundle-assemble"')
    next_command = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', assemble_start)
    assert "--database-url" not in cli_source[assemble_start:next_command]

"""Tests for the bounded production-scoped online shadow pilot."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import pytest

from pipeline import ml_shadow_scorer_production_scoped_shadow_pilot as pilot_module
from pipeline.ml_shadow_scorer_online_shadow_runtime import FEATURE_FLAG
from pipeline.ml_shadow_scorer_production_scoped_shadow_bundle import (
    MLShadowScorerProductionScopedShadowBundleError,
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    plan_ml_shadow_scorer_production_scoped_shadow_bundle,
    prove_ml_shadow_scorer_production_scoped_shadow_bundle,
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle,
    verify_ml_shadow_scorer_production_scoped_shadow_bundle,
    write_ml_shadow_scorer_production_scoped_shadow_bundle,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot import (
    MLShadowScorerProductionScopedShadowPilotError,
    run_ml_shadow_scorer_production_scoped_shadow_pilot,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness import (
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.ml_shadow_scorer_production_scoped_shadow_pilot_harness_review import (
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness,
)
from pipeline.shadow_write_path_guards import PROD_SCOPED_SHADOW_ROOT

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

EXPECTED_PILOT_FILES = {"manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json"}


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


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _write_harness_review_bundle(root: Path) -> Path:
    bundle_path = _write_pre_plan_bundle(root)
    plan_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    prove_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        pilot_run_id="proof-before-pilot",
        repo_root=root,
    )
    request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(bundle_path=bundle_path, repo_root=root)
    grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        owner_documents_equivalent_review="owner equivalent review for bounded pilot",
        repo_root=root,
    )
    run_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        pilot_run_id="pilot-harness-before-pilot",
        repo_root=root,
    )
    review_ml_shadow_scorer_production_scoped_shadow_pilot_harness(
        bundle_path=bundle_path,
        reviewer="Harness Reviewer",
        repo_root=root,
    )
    return bundle_path


def _run_pilot(
    root: Path,
    *,
    bundle_path: Path | None = None,
    pilot_run_id: str = "pilot-run-test",
    update_bundle: bool = True,
) -> dict[str, Any]:
    return run_ml_shadow_scorer_production_scoped_shadow_pilot(
        bundle_path=bundle_path or _write_harness_review_bundle(root),
        learned_probability_artifact_path=_fixture(root, "learned_probability"),
        second_surface_generalization_audit_path=_fixture(root, "generalization_audit"),
        pilot_run_id=pilot_run_id,
        repo_root=root,
        update_bundle=update_bundle,
    )


def test_happy_path_runs_528_row_audit_artifact_pilot_and_updates_bundle(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    before = _load(bundle_path)

    result = _run_pilot(root, bundle_path=bundle_path, pilot_run_id="pilot-run-happy")
    updated = _load(bundle_path)
    pilot_dir = root / PROD_SCOPED_SHADOW_ROOT / "pilot-run-happy"

    assert result["prod_scoped_shadow_pilot_passed"] is True
    assert updated["metadata"]["bundle_revision"] == 7
    assert updated["plan"] == before["plan"]
    assert updated["proof"] == before["proof"]
    assert updated["execution"]["pilot_harness"] == before["execution"]["pilot_harness"]
    assert updated["review"] == before["review"]
    assert updated["authorization"]["prod_scoped_shadow_pilot_execution_authorized"] is True
    assert updated["authorization"]["prod_scoped_shadow_live_execution_authorized"] is False
    assert updated["authorization"]["prod_scoped_shadow_execution_authorized"] is False
    assert updated["posture"]["prod_scoped_shadow_pilot_execution_authorized"] is True
    assert updated["posture"]["prod_scoped_shadow_pilot_executed"] is True
    assert updated["posture"]["prod_scoped_shadow_pilot_passed"] is True
    assert updated["posture"]["online_shadow_execution_enabled"] is False
    assert updated["posture"]["production_default_allowed"] is False
    assert updated["posture"]["api_web_changes_allowed"] is False
    assert updated["posture"]["user_visible_ranking_changed"] is False
    assert updated["posture"]["live_prod_source_reads_performed"] is False
    assert updated["recommended_next_stage"] == "review_production_scoped_online_shadow_pilot_v1"

    pilot_run = updated["execution"]["pilot_run"]
    assert pilot_run["pilot_surface"] == "bounded_read_only_audit_artifact_pilot"
    assert pilot_run["live_prod_source_reads_performed"] is False
    assert pilot_run["input_join_summary"]["joined_candidate_count"] == 528
    assert pilot_run["input_join_summary"]["runtime_row_count"] == 528
    assert pilot_run["pilot_run_id"] != before["execution"]["pilot_harness"]["pilot_run_id"]
    assert "harness" not in pilot_run["pilot_run_id"]
    assert set(path.name for path in pilot_dir.iterdir()) == EXPECTED_PILOT_FILES
    assert [record["relative_path"] for record in pilot_run["files_written"]] == [
        "manifest.json",
        "shadow_rows.jsonl",
        "observability.json",
        "write_counts.json",
    ]
    for record in pilot_run["files_written"]:
        artifact = pilot_dir / record["relative_path"]
        assert record["sha256"] == _sha256(artifact)
        assert record["write_target"] == "isolated_prod_scoped_audit_artifacts"
    assert len((pilot_dir / "shadow_rows.jsonl").read_text(encoding="utf-8").splitlines()) == 528
    assert pilot_run["source_artifacts"]["learned_probability_artifact"]["sha256"] == _sha256(
        _fixture(root, "learned_probability")
    )
    assert pilot_run["source_artifacts"]["second_surface_generalization_audit"]["sha256"] == _sha256(
        _fixture(root, "generalization_audit")
    )
    assert pilot_run["write_count_verification"]["write_counts_by_isolated_target"]["isolated_prod_scoped_audit_artifacts"] == 4
    assert pilot_run["write_count_verification"]["write_counts_by_isolated_target"]["prod_scoped_shadow_tables"] == 0

    verified = verify_ml_shadow_scorer_production_scoped_shadow_bundle(
        bundle_path=bundle_path,
        repo_root=root,
        expect_pilot_run_filed=True,
    )
    assert verified["verification_mode"] == "post_pilot_run"


def test_approved_text_artifact_hashes_accept_lf_checkout(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    for key in ("learned_probability", "generalization_audit"):
        artifact = _fixture(root, key)
        lf_text = artifact.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
        artifact.write_bytes(lf_text.encode("utf-8"))

    result = _run_pilot(root, bundle_path=bundle_path, pilot_run_id="line-ending-variant", update_bundle=False)

    assert result["prod_scoped_shadow_pilot_passed"] is True
    assert result["execution"]["source_artifacts"]["learned_probability_artifact"]["sha256"] == _sha256(
        _fixture(root, "learned_probability")
    )
    assert result["execution"]["source_artifacts"]["second_surface_generalization_audit"]["sha256"] == _sha256(
        _fixture(root, "generalization_audit")
    )


def test_rejects_fixture_input_argument_on_pilot_cli(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    command = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-pilot-run",
        "--bundle",
        str(bundle_path),
        "--learned-probability-artifact",
        str(_fixture(root, "learned_probability")),
        "--second-surface-generalization-audit",
        str(_fixture(root, "generalization_audit")),
        "--fixture-input",
        "forbidden.json",
        "--repo-root",
        str(root),
    ]

    result = subprocess.run(command, cwd=PACKAGE_ROOT, text=True, capture_output=True)

    assert result.returncode != 0
    assert "unrecognized arguments" in result.stderr


def test_rejects_missing_or_unapproved_artifact_path(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    unapproved = root / "docs/audit/unapproved-learned.json"
    shutil.copy2(_fixture(root, "learned_probability"), unapproved)

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotError, match="approved frozen audit artifact"):
        run_ml_shadow_scorer_production_scoped_shadow_pilot(
            bundle_path=bundle_path,
            learned_probability_artifact_path=unapproved,
            second_surface_generalization_audit_path=_fixture(root, "generalization_audit"),
            pilot_run_id="unapproved-artifact",
            repo_root=root,
            update_bundle=False,
        )


def test_rejects_sha_tamper(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    learned = _fixture(root, "learned_probability")
    learned.write_text(learned.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotError, match="sha256 mismatch"):
        _run_pilot(root, bundle_path=bundle_path, pilot_run_id="sha-tamper", update_bundle=False)


def test_rejects_identity_mismatch_after_sha_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    learned = _fixture(root, "learned_probability")
    payload = _load(learned)
    payload["metadata"]["family"] = "mismatch"
    _write_json(learned, payload)
    monkeypatch.setattr(pilot_module, "APPROVED_LEARNED_PROBABILITY_SHA256", _sha256(learned))

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotError, match="family"):
        _run_pilot(root, bundle_path=bundle_path, pilot_run_id="identity-mismatch", update_bundle=False)


def test_rejects_forbidden_write_count_nonzero_before_bundle_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    original_counts = pilot_module._write_counts_by_isolated_target

    def bad_counts(*, file_count: int = 4) -> dict[str, int]:
        counts = original_counts(file_count=file_count)
        counts["prod_scoped_shadow_tables"] = 1
        return counts

    monkeypatch.setattr(pilot_module, "_write_counts_by_isolated_target", bad_counts)

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotError, match="forbidden prod-scoped write targets"):
        _run_pilot(root, bundle_path=bundle_path, pilot_run_id="bad-counts")
    assert _load(bundle_path)["metadata"]["bundle_revision"] == 6


def test_environment_restored_and_runtime_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    monkeypatch.setenv(FEATURE_FLAG, "original-off")
    original_runtime = pilot_module.run_ml_shadow_scorer_v1_online_shadow_runtime
    calls: list[tuple[str | None, int]] = []

    def wrapped_runtime(candidate_rows: list[Mapping[str, Any]], *, env: Mapping[str, str] | None = None) -> dict[str, Any]:
        flag_value = env.get(FEATURE_FLAG) if env is not None else os.environ.get(FEATURE_FLAG)
        calls.append((flag_value, len(candidate_rows)))
        return original_runtime(candidate_rows, env=env)

    monkeypatch.setattr(pilot_module, "run_ml_shadow_scorer_v1_online_shadow_runtime", wrapped_runtime)

    result = _run_pilot(root, bundle_path=bundle_path, pilot_run_id="env-order", update_bundle=False)

    assert os.environ[FEATURE_FLAG] == "original-off"
    assert calls == [(None, 0), ("true", 528), (None, 0)]
    assert result["execution"]["runtime_drill"]["call_order"] == [
        "preflight_disabled",
        "pilot_enabled",
        "postflight_disabled",
    ]


def test_rejects_double_pilot_run(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    _run_pilot(root, bundle_path=bundle_path, pilot_run_id="first-pilot")

    with pytest.raises(MLShadowScorerProductionScopedShadowPilotError, match="bundle_revision|already"):
        _run_pilot(root, bundle_path=bundle_path, pilot_run_id="second-pilot")


def test_runtime_import_call_is_limited_to_pilot_runner_module() -> None:
    production_scoped_modules = list((PACKAGE_ROOT / "pipeline").glob("ml_shadow_scorer_production_scoped*.py"))
    for module_path in production_scoped_modules:
        source = module_path.read_text(encoding="utf-8")
        if module_path.name in {
            "ml_shadow_scorer_production_scoped_shadow_pilot.py",
            "ml_shadow_scorer_production_scoped_shadow_live_read_only_pilot.py",
            "ml_shadow_scorer_production_scoped_shadow_live_execution_pilot.py",
            "ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot.py",
        }:
            assert "run_ml_shadow_scorer_v1_online_shadow_runtime" in source
        else:
            assert "run_ml_shadow_scorer_v1_online_shadow_runtime" not in source


def test_no_tracked_shadow_run_artifacts() -> None:
    result = subprocess.run(
        ["git", "ls-files", "docs/audit/shadow-runs"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.strip() == ""


def test_cli_smoke_run_pilot_and_verify_rev7(tmp_path: Path) -> None:
    root = _copy_fixture_repo(tmp_path)
    bundle_path = _write_harness_review_bundle(root)
    command = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-production-scoped-shadow-pilot-run",
        "--bundle",
        str(bundle_path),
        "--learned-probability-artifact",
        str(_fixture(root, "learned_probability")),
        "--second-surface-generalization-audit",
        str(_fixture(root, "generalization_audit")),
        "--pilot-run-id",
        "cli-pilot",
        "--repo-root",
        str(root),
    ]
    run = subprocess.run(command, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    assert run.stdout.splitlines() == [
        "cli-pilot",
        "True",
        "review_production_scoped_online_shadow_pilot_v1",
    ]

    verify = subprocess.run(
        [
            sys.executable,
            "-m",
            "pipeline.cli",
            "ml-shadow-scorer-production-scoped-shadow-bundle-verify",
            "--bundle",
            str(bundle_path),
            "--expect-pilot-run-filed",
            "--repo-root",
            str(root),
        ],
        cwd=PACKAGE_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    assert verify.stdout.splitlines() == [
        "passed",
        "post_pilot_run",
        "online-shadow-production-scoped-v1",
        "review_production_scoped_online_shadow_pilot_v1",
    ]

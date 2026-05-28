"""Tests for ml-shadow-scorer-v1 Phase 2 isolated audit write-mode planning."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

from pipeline.ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan import (
    MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError,
    build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan_payload,
)


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]

REVIEW_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1.json"
GRANT_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1.json"
POLICY_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-policy.json"
PHASE1_PLAN_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-plan-v1.json"
PHASE1_RUN_PATH = REPO_ROOT / "docs/audit/ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-run-v1.json"


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _build(
    *,
    review_path: Path = REVIEW_PATH,
    grant_path: Path = GRANT_PATH,
    policy_path: Path = POLICY_PATH,
    phase1_plan_path: Path | None = None,
    phase1_run_path: Path | None = None,
    generated_at: str = "2026-05-28T00:00:00Z",
) -> dict:
    return build_ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan_payload(
        phase1_no_write_pilot_review_path=review_path,
        authorization_grant_path=grant_path,
        online_shadow_policy_path=policy_path,
        phase1_no_write_pilot_plan_path=phase1_plan_path,
        phase1_no_write_pilot_run_path=phase1_run_path,
        repo_root=REPO_ROOT,
        generated_at=generated_at,
    )


def test_happy_path_writes_phase2_write_mode_plan_semantics() -> None:
    payload = _build(phase1_plan_path=PHASE1_PLAN_PATH, phase1_run_path=PHASE1_RUN_PATH)

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan"
    assert payload["phase2_isolated_audit_write_mode_plan_defined"] is True
    assert payload["phase2_isolated_audit_write_mode_plan_executed"] is False
    assert payload["phase2_writes_authorized"] is False
    assert payload["phase2_write_mode_proof_executed"] is False
    assert payload["phase1_no_write_pilot_result_accepted"] is True
    assert payload["online_shadow_execution_enabled"] is False
    assert payload["writes_allowed"] is False
    assert payload["writes_performed"] is False
    assert payload["production_default_allowed"] is False
    assert payload["api_web_changes_allowed"] is False
    assert payload["user_visible_ranking_changed"] is False
    assert payload["missing_production_readiness_authorization"] is True
    assert payload["recommended_next_stage"] == "implement_online_shadow_phase2_isolated_audit_write_mode_proof_v1"
    assert payload["metadata"]["verified_optional_plan_inputs"]
    assert payload["metadata"]["verified_optional_run_inputs"]


def test_isolated_target_is_audit_file_tree_and_db_target_is_deferred() -> None:
    payload = _build()
    target = payload["isolated_write_target"]

    assert target["primary_target"] == "isolated audit artifact tree only"
    assert target["root_path"] == "docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/"
    assert target["pilot_run_subdirectory_pattern"].endswith("/<pilot_run_id>/")
    assert target["deferred_alternate_not_authorized"]["db_namespace_table_prefix"] == "ml_shadow_scorer_v1_audit_shadow"
    assert payload["allowed_write_contract"]["phase2_writes_authorized_now"] is False
    assert payload["schema_and_namespace"]["committed_gate_paths_may_not_be_overwritten"] is True


def test_write_count_observability_and_blockers_remain_closed() -> None:
    payload = _build()

    targets = payload["write_count_observability_plan"]["per_target_counts"]
    assert "isolated_audit_shadow_artifacts" in targets
    assert "isolated_audit_shadow_tables" in targets
    assert payload["write_count_observability_plan"]["isolated_audit_shadow_tables_expected_count"] == 0
    blockers = payload["shadow_and_production_blockers"]
    assert blockers["missing_phase1_no_write_pilot_review"] is False
    assert blockers["missing_phase2_write_mode_isolation_proof"] is True
    assert blockers["phase2_writes_authorized"] is False
    assert blockers["phase2_isolated_audit_write_mode_plan_defined"] is True
    assert blockers["online_shadow_execution_enabled"] is False


def test_rejects_phase1_review_not_accepted(tmp_path: Path) -> None:
    review = copy.deepcopy(_load(REVIEW_PATH))
    review["review_decision"]["phase1_no_write_pilot_result_accepted"] = False
    review["review_decision"]["decision"] = "not_accepted"
    review_path = _write_json(tmp_path, "review.json", review)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError, match="review_decision"):
        _build(review_path=review_path)


def test_rejects_grant_phase2_writes_authorized_true(tmp_path: Path) -> None:
    grant = copy.deepcopy(_load(GRANT_PATH))
    grant["phase2_writes_authorized"] = True
    grant_path = _write_json(tmp_path, "grant.json", grant)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError, match="phase2_writes_authorized"):
        _build(grant_path=grant_path)


@pytest.mark.parametrize("value", [None, False])
def test_accepts_missing_null_or_false_grant_phase2_writes_authorized(tmp_path: Path, value: bool | None) -> None:
    grant = copy.deepcopy(_load(GRANT_PATH))
    if value is None:
        grant["phase2_writes_authorized"] = None
    else:
        grant["phase2_writes_authorized"] = value
    grant_path = _write_json(tmp_path, "grant.json", grant)

    payload = _build(grant_path=grant_path)
    assert payload["phase2_writes_authorized"] is False


def test_rejects_tampered_review_input_sha(tmp_path: Path) -> None:
    review = copy.deepcopy(_load(REVIEW_PATH))
    review["metadata"]["inputs"][0]["sha256"] = "0" * 64
    review_path = _write_json(tmp_path, "review.json", review)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError, match="sha256 mismatch"):
        _build(review_path=review_path)


def test_rejects_policy_missing_allowed_write_scope(tmp_path: Path) -> None:
    policy = copy.deepcopy(_load(POLICY_PATH))
    del policy["allowed_write_scope"]
    policy_path = _write_json(tmp_path, "policy.json", policy)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError, match="allowed_write_scope"):
        _build(policy_path=policy_path)


def test_rejects_policy_without_forbidden_write_scope(tmp_path: Path) -> None:
    policy = copy.deepcopy(_load(POLICY_PATH))
    policy["forbidden_write_scope"] = []
    policy_path = _write_json(tmp_path, "policy.json", policy)

    with pytest.raises(MLShadowScorerOnlineShadowPhase2IsolatedAuditWriteModePlanError, match="forbidden_write_scope"):
        _build(policy_path=policy_path)


def test_cli_smoke_writes_json_and_markdown(tmp_path: Path) -> None:
    out_json = tmp_path / "phase2-plan.json"
    out_md = tmp_path / "phase2-plan.md"
    cmd = [
        sys.executable,
        "-m",
        "pipeline.cli",
        "ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-mode-plan",
        "--phase1-no-write-pilot-review",
        str(REVIEW_PATH),
        "--authorization-grant",
        str(GRANT_PATH),
        "--online-shadow-policy",
        str(POLICY_PATH),
        "--output",
        str(out_json),
        "--markdown-output",
        str(out_md),
    ]
    result = subprocess.run(cmd, cwd=PACKAGE_ROOT, text=True, capture_output=True, check=True)
    payload = json.loads(out_json.read_text(encoding="utf-8"))

    assert payload["metadata"]["artifact_type"] == "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan"
    assert payload["phase2_isolated_audit_write_mode_plan_defined"] is True
    assert payload["phase2_writes_authorized"] is False
    assert payload["recommended_next_stage"] == "implement_online_shadow_phase2_isolated_audit_write_mode_proof_v1"
    assert result.stdout.splitlines() == [
        "True",
        "False",
        "implement_online_shadow_phase2_isolated_audit_write_mode_proof_v1",
    ]
    assert "Online Shadow Phase 2 Isolated Audit Write-Mode Plan" in out_md.read_text(encoding="utf-8")


def test_no_forbidden_imports_cli_has_no_database_url_and_uses_normalized_hash_helper() -> None:
    module_source = (
        PACKAGE_ROOT / "pipeline" / "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan.py"
    ).read_text(encoding="utf-8")
    import_lines = "\n".join(line for line in module_source.lower().splitlines() if line.startswith(("import ", "from ")))
    for forbidden in ("psycopg", "openai", "openalex", "sklearn"):
        assert forbidden not in import_lines
    assert "recorded_sha256_matches_text_artifact" in module_source

    cli_source = (PACKAGE_ROOT / "pipeline" / "cli.py").read_text(encoding="utf-8")
    start = cli_source.index('"ml-shadow-scorer-online-shadow-phase2-isolated-audit-write-mode-plan"')
    end = cli_source.index('"ml-shadow-scorer-second-candidate-plan-ingest"', start)
    assert "--database-url" not in cli_source[start:end]

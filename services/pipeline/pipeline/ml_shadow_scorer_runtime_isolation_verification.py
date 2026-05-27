"""Runtime isolation verifier for ml-shadow-scorer-v1.

This command reads committed policy/runtime/gates artifacts and exercises the
disabled runtime entry point with in-memory fixtures only. It does not query
databases, write data stores, call services, generate embeddings, run ranking,
ingest labels, or enable online shadow execution.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    FLAG_OFF_VALUES,
    FLAG_ON_VALUES,
    RANKING_RUN_ID,
    RUNTIME_VERSION,
    parse_ml_shadow_scorer_v1_runtime_flag,
    run_ml_shadow_scorer_v1_online_shadow_runtime,
)
from pipeline.ml_shadow_scorer_v1 import compute_shadow_score_rows
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_runtime_isolation_verification"
VERIFICATION_VERSION = "ml-shadow-scorer-v1-runtime-isolation-verification-v1"

RUNTIME_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_runtime_disabled"
GATES_ARTIFACT_TYPE = "ml_shadow_scorer_generalization_audit_gates"
GATES_VERSION = "ml-shadow-scorer-v1-generalization-audit-gates-v1"
ONLINE_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"
ONLINE_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"

PASSED_NEXT_STAGE = "draft_online_shadow_execution_enablement_gates_v1"
FAILED_NEXT_STAGE = "repair_ml_shadow_scorer_v1_runtime_isolation"

RUNTIME_EXPECTED_NEXT_STAGE = "run_ml_shadow_scorer_v1_runtime_isolation_verification_v1"
GATES_EXPECTED_NEXT_STAGE = "implement_online_shadow_runtime_disabled_by_default"

FORBIDDEN_IMPORT_TOKENS = ("psycopg", "openai", "openalex", "sklearn")
FORBIDDEN_WEB_API_IMPORT_TOKENS = ("fastapi", "flask", "starlette", "requests", "httpx", "urllib", "aiohttp")
FORBIDDEN_SQL_VERBS = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "MERGE", "TRUNCATE")

CAVEATS = (
    "Passing runtime isolation verification does not enable online shadow execution.",
    "Passing does not authorize production/default/API/web behavior.",
    "The runtime remains disabled by default.",
    "Any future enablement still needs a separate gates/authorization artifact and production-readiness remains separate.",
)


class MLShadowScorerRuntimeIsolationVerificationError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerRuntimeIsolationVerificationError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerRuntimeIsolationVerificationError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerRuntimeIsolationVerificationError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerRuntimeIsolationVerificationError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerRuntimeIsolationVerificationError(f"{name} must be {expected!r}, got {observed!r}")


def _validate_identity(
    payload: Mapping[str, Any],
    *,
    name: str,
    artifact_type: str,
    version_field: str,
    version: str,
) -> Mapping[str, Any]:
    metadata = _metadata(payload, name=name)
    _require_equal(f"{name} metadata.artifact_type", metadata.get("artifact_type"), artifact_type)
    _require_equal(f"{name} metadata.{version_field}", metadata.get(version_field), version)
    return metadata


def _validate_runtime_artifact(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-runtime",
        artifact_type=RUNTIME_ARTIFACT_TYPE,
        version_field="runtime_version",
        version=RUNTIME_VERSION,
    )
    required = {
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": True,
        "runtime_default_state": "off",
        "runtime_feature_flag": FEATURE_FLAG,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "recommended_next_stage": RUNTIME_EXPECTED_NEXT_STAGE,
        "last_disabled_run.status": "skipped_runtime_disabled",
        "last_disabled_run.shadow_row_count": 0,
        "last_disabled_run.writes_performed": False,
    }
    for path, expected in required.items():
        _require_equal(f"runtime {path}", _get(payload, path), expected)
    for field, expected in _identity_fields().items():
        _require_equal(f"runtime metadata.{field}", metadata.get(field), expected)
    return metadata


def _validate_gates_artifact(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="generalization-audit-gates",
        artifact_type=GATES_ARTIFACT_TYPE,
        version_field="gates_version",
        version=GATES_VERSION,
    )
    required = {
        "generalization_audit_gates_passed": True,
        "second_surface_generalization_passed": True,
        "disabled_by_default_runtime_implementation_next_stage_allowed": True,
        "recommended_next_stage": GATES_EXPECTED_NEXT_STAGE,
        "runtime_implementation_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
    }
    for path, expected in required.items():
        _require_equal(f"gates {path}", _get(payload, path), expected)
    for field, expected in _identity_fields().items():
        _require_equal(f"gates metadata.{field}", metadata.get(field), expected)
    return metadata


def _validate_policy_artifact(payload: Mapping[str, Any], runtime_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _validate_identity(
        payload,
        name="online-shadow-policy",
        artifact_type=ONLINE_POLICY_ARTIFACT_TYPE,
        version_field="policy_version",
        version=ONLINE_POLICY_VERSION,
    )
    _require_equal("policy runtime_implementation_authorized", payload.get("runtime_implementation_authorized"), False)
    _require_equal("policy feature flag", _get(payload, "runtime_isolation_policy.feature_flag"), runtime_payload["runtime_feature_flag"])
    _require_equal("policy feature flag default off", _get(payload, "runtime_isolation_policy.feature_flag_default_off"), True)
    _require_equal("policy feature flag default", _get(payload, "runtime_isolation_policy.feature_flag_default"), "off")
    _require_equal("policy disable switch default", _get(payload, "disable_and_rollback_policy.disable_switch_default"), "off")
    return metadata


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
    }


def _fixture_rows() -> list[dict[str, Any]]:
    base = {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
    }
    return [
        {**base, "canonical_openalex_work_id": "W000000001", "final_score": 0.91, "audit_embedding_probability_work": 0.21},
        {**base, "canonical_openalex_work_id": "W000000002", "final_score": 0.44, "audit_embedding_probability_work": 0.82},
        {**base, "canonical_openalex_work_id": "W000000003", "final_score": 0.12, "audit_embedding_probability_work": 0.14},
    ]


def _sorted_expected_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    expected = compute_shadow_score_rows(rows)
    expected.sort(key=lambda row: (-float(row["ml_shadow_scorer_v1_score"]), str(row["canonical_openalex_work_id"])))
    return expected


def _runtime_flag_probes() -> dict[str, Any]:
    rows = _fixture_rows()
    flag_off_cases = {
        "unset": {},
        "empty": {FEATURE_FLAG: ""},
        "zero": {FEATURE_FLAG: "0"},
        "false": {FEATURE_FLAG: "false"},
        "off": {FEATURE_FLAG: "off"},
        "unknown": {FEATURE_FLAG: "banana"},
    }
    flag_off_results = {
        case: _probe_summary(run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env=env))
        for case, env in flag_off_cases.items()
    }

    expected_rows = _sorted_expected_rows(rows)
    flag_on_results: dict[str, dict[str, Any]] = {}
    for value in ("on", "true", "1"):
        result = run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env={FEATURE_FLAG: value})
        flag_on_results[value] = {
            **_probe_summary(result),
            "matches_compute_shadow_score_rows": result.get("shadow_rows") == expected_rows,
        }

    parser_results = {
        "on_values": {value: parse_ml_shadow_scorer_v1_runtime_flag(value) for value in FLAG_ON_VALUES},
        "off_values": {value or "<empty>": parse_ml_shadow_scorer_v1_runtime_flag(value) for value in FLAG_OFF_VALUES},
        "unknown_value": parse_ml_shadow_scorer_v1_runtime_flag("banana"),
        "unset_value": parse_ml_shadow_scorer_v1_runtime_flag(None),
    }
    return {
        "flag_off_results": flag_off_results,
        "flag_on_results": flag_on_results,
        "flag_parser_results": parser_results,
    }


def _runtime_negative_probes() -> dict[str, Any]:
    missing_final_score = _fixture_rows()
    del missing_final_score[0]["final_score"]
    missing_probability = _fixture_rows()
    del missing_probability[0]["audit_embedding_probability_work"]

    label_field_results: dict[str, dict[str, Any]] = {}
    for field, value in (("label_any_positive", True), ("good_or_acceptable", False), ("reviewer_notes", "not allowed")):
        rows = _fixture_rows()
        rows[0][field] = value
        label_field_results[field] = _probe_summary(
            run_ml_shadow_scorer_v1_online_shadow_runtime(rows, env={FEATURE_FLAG: "on"})
        )

    wrong_ranking = _fixture_rows()
    wrong_ranking[0]["ranking_run_id"] = "rank-wrong"
    wrong_sha = _fixture_rows()
    wrong_sha[0]["candidate_pool_work_set_sha256"] = "not-the-approved-candidate-sha"
    return {
        "incomplete_coverage_results": {
            "missing_final_score": _probe_summary(
                run_ml_shadow_scorer_v1_online_shadow_runtime(missing_final_score, env={FEATURE_FLAG: "on"})
            ),
            "missing_audit_embedding_probability_work": _probe_summary(
                run_ml_shadow_scorer_v1_online_shadow_runtime(missing_probability, env={FEATURE_FLAG: "on"})
            ),
        },
        "label_field_rejection_results": label_field_results,
        "identity_rejection_results": {
            "wrong_ranking_run_id": _probe_summary(
                run_ml_shadow_scorer_v1_online_shadow_runtime(wrong_ranking, env={FEATURE_FLAG: "on"})
            ),
            "wrong_candidate_pool_work_set_sha256": _probe_summary(
                run_ml_shadow_scorer_v1_online_shadow_runtime(wrong_sha, env={FEATURE_FLAG: "on"})
            ),
        },
    }


def _probe_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "shadow_row_count": result.get("shadow_row_count"),
        "writes_performed": result.get("writes_performed"),
        "production_default_changed": result.get("production_default_changed"),
        "user_visible_ranking_changed": result.get("user_visible_ranking_changed"),
        "labels_used_for_scoring": result.get("labels_used_for_scoring"),
    }


def source_guard_results(
    runtime_module_path: Path | None = None,
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    module_path = (
        Path(runtime_module_path).resolve()
        if runtime_module_path is not None
        else Path(__file__).resolve().with_name("ml_shadow_scorer_online_shadow_runtime.py")
    )
    source = module_path.read_text(encoding="utf-8")
    import_lines = [
        line.strip()
        for line in source.splitlines()
        if line.strip().startswith("import ") or line.strip().startswith("from ")
    ]
    import_text = "\n".join(import_lines).lower()
    forbidden_imports = sorted(token for token in FORBIDDEN_IMPORT_TOKENS if token in import_text)
    forbidden_web_imports = sorted(token for token in FORBIDDEN_WEB_API_IMPORT_TOKENS if token in import_text)
    write_sql_verbs = sorted(
        verb for verb in FORBIDDEN_SQL_VERBS if re.search(rf"\b{re.escape(verb)}\b", source, flags=re.IGNORECASE)
    )
    return {
        "runtime_module_path": portable_repo_path(module_path, repo_root=root),
        "forbidden_import_tokens_present": forbidden_imports,
        "web_api_import_tokens_present": forbidden_web_imports,
        "write_sql_verbs_present": write_sql_verbs,
        "passed": not forbidden_imports and not forbidden_web_imports and not write_sql_verbs,
    }


def _verification_gate(gate_id: str, title: str, passed: bool, observed: Any, rationale: str) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "title": title,
        "status": "pass" if passed else "fail",
        "passed": passed,
        "observed_value": observed,
        "rationale": rationale,
    }


def _all_status(results: Mapping[str, Mapping[str, Any]], status: str) -> bool:
    return all(result.get("status") == status for result in results.values())


def _all_zero_write(results: Mapping[str, Mapping[str, Any]]) -> bool:
    return all(result.get("writes_performed") is False for result in results.values())


def _build_verification_results(
    *,
    runtime_payload: Mapping[str, Any],
    gates_payload: Mapping[str, Any],
    policy_payload: Mapping[str, Any],
    runtime_probe_results: Mapping[str, Any],
    guard_results: Mapping[str, Any],
) -> list[dict[str, Any]]:
    flag_off = runtime_probe_results["flag_off_results"]
    flag_on = runtime_probe_results["flag_on_results"]
    incomplete = runtime_probe_results["incomplete_coverage_results"]
    label_rejections = runtime_probe_results["label_field_rejection_results"]
    identity_rejections = runtime_probe_results["identity_rejection_results"]
    parser = runtime_probe_results["flag_parser_results"]

    on_parser_ok = all(parser["on_values"].values())
    off_parser_ok = not any(parser["off_values"].values()) and parser["unknown_value"] is False and parser["unset_value"] is False
    flag_off_ok = _all_status(flag_off, "skipped_runtime_disabled") and all(
        result["shadow_row_count"] == 0 and result["writes_performed"] is False for result in flag_off.values()
    )
    flag_on_ok = _all_status(flag_on, "succeeded_test_only") and all(
        result["matches_compute_shadow_score_rows"] is True
        and result["writes_performed"] is False
        and result["production_default_changed"] is False
        and result["user_visible_ranking_changed"] is False
        for result in flag_on.values()
    )
    incomplete_ok = _all_status(incomplete, "skipped_incomplete_coverage") and all(
        result["shadow_row_count"] == 0 and result["writes_performed"] is False for result in incomplete.values()
    )
    label_ok = _all_status(label_rejections, "rejected_label_fields_present") and _all_zero_write(label_rejections)
    identity_ok = _all_status(identity_rejections, "rejected_identity_mismatch") and _all_zero_write(identity_rejections)
    policy_gate_ok = (
        runtime_payload.get("runtime_feature_flag") == FEATURE_FLAG
        and _get(policy_payload, "runtime_isolation_policy.feature_flag") == FEATURE_FLAG
        and _get(policy_payload, "runtime_isolation_policy.feature_flag_default_off") is True
        and _get(policy_payload, "disable_and_rollback_policy.disable_switch_default") == "off"
        and gates_payload.get("generalization_audit_gates_passed") is True
        and runtime_payload.get("runtime_disabled_by_default") is True
    )
    no_mutations_ok = (
        guard_results.get("write_sql_verbs_present") == []
        and all(result["writes_performed"] is False for result in flag_off.values())
        and all(result["writes_performed"] is False for result in flag_on.values())
        and all(result["production_default_changed"] is False for result in flag_on.values())
        and all(result["user_visible_ranking_changed"] is False for result in flag_on.values())
    )

    gates = [
        _verification_gate(
            "V01_default_off_behavior",
            "Default-off behavior",
            flag_off_ok,
            flag_off,
            "Unset and explicit off-like feature flag values must skip with zero rows and no writes.",
        ),
        _verification_gate(
            "V02_flag_parser_contract",
            "Feature flag parser contract",
            on_parser_ok and off_parser_ok,
            parser,
            "Only the approved on values enable in-memory scoring; all other values are off.",
        ),
        _verification_gate(
            "V03_in_memory_scoring_only",
            "In-memory scoring only",
            flag_on_ok,
            flag_on,
            "Explicitly enabled fixture probes must match compute_shadow_score_rows and perform no writes.",
        ),
        _verification_gate(
            "V04_skip_on_incomplete_coverage",
            "Skip on incomplete coverage",
            incomplete_ok,
            incomplete,
            "Missing final_score or audit_embedding_probability_work must skip the whole run.",
        ),
        _verification_gate(
            "V05_label_field_rejection",
            "Label field rejection",
            label_ok,
            label_rejections,
            "Runtime rows containing label or reviewer fields must be rejected before scoring.",
        ),
        _verification_gate(
            "V06_identity_scope_rejection",
            "Identity scope rejection",
            identity_ok,
            identity_rejections,
            "Runtime rows with mismatched second-surface identity fields must be rejected.",
        ),
        _verification_gate(
            "V07_no_db_or_network_or_training_imports",
            "No DB/network/training imports",
            guard_results.get("forbidden_import_tokens_present") == []
            and guard_results.get("web_api_import_tokens_present") == [],
            {
                "forbidden_import_tokens_present": guard_results.get("forbidden_import_tokens_present"),
                "web_api_import_tokens_present": guard_results.get("web_api_import_tokens_present"),
            },
            "Runtime source must not import DB, network/API, or training libraries.",
        ),
        _verification_gate(
            "V08_no_write_sql_or_production_mutation_paths",
            "No write SQL or production mutation paths",
            no_mutations_ok,
            {
                "write_sql_verbs_present": guard_results.get("write_sql_verbs_present"),
                "flag_on_writes_performed": [result["writes_performed"] for result in flag_on.values()],
                "production_default_changed": [result["production_default_changed"] for result in flag_on.values()],
                "user_visible_ranking_changed": [result["user_visible_ranking_changed"] for result in flag_on.values()],
            },
            "Runtime source and fixture probes must show no write SQL, production mutation, or user-visible ranking mutation.",
        ),
        _verification_gate(
            "V09_policy_and_gates_alignment",
            "Policy and gates alignment",
            policy_gate_ok,
            {
                "runtime_feature_flag": runtime_payload.get("runtime_feature_flag"),
                "policy_feature_flag": _get(policy_payload, "runtime_isolation_policy.feature_flag"),
                "policy_default_off": _get(policy_payload, "runtime_isolation_policy.feature_flag_default_off"),
                "gates_passed": gates_payload.get("generalization_audit_gates_passed"),
                "runtime_disabled_by_default": runtime_payload.get("runtime_disabled_by_default"),
            },
            "Runtime artifact, policy, and passed gates must agree on identity and default-off contract.",
        ),
    ]
    decision = all(gate["passed"] for gate in gates)
    gates.append(
        _verification_gate(
            "V10_runtime_isolation_verification_decision",
            "Runtime isolation verification decision",
            decision,
            {"required_gates_passed": decision},
            "All runtime isolation verification gates must pass.",
        )
    )
    return gates


def build_ml_shadow_scorer_runtime_isolation_verification_payload(
    *,
    online_shadow_runtime_path: Path,
    generalization_audit_gates_path: Path,
    online_shadow_policy_path: Path,
    verification_version: str = VERIFICATION_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
    runtime_module_path: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    runtime_path = Path(online_shadow_runtime_path).resolve()
    gates_path = Path(generalization_audit_gates_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()

    runtime_payload = _load_json_object(runtime_path)
    gates_payload = _load_json_object(gates_path)
    policy_payload = _load_json_object(policy_path)

    runtime_metadata = _validate_runtime_artifact(runtime_payload)
    gates_metadata = _validate_gates_artifact(gates_payload)
    policy_metadata = _validate_policy_artifact(policy_payload, runtime_payload)
    for field, expected in _identity_fields().items():
        _require_equal(f"runtime/gates identity {field}", runtime_metadata.get(field), gates_metadata.get(field))
        _require_equal(f"gates approved identity {field}", gates_metadata.get(field), expected)

    inputs = [
        _input_record("online_shadow_runtime", runtime_path, repo_root=root),
        _input_record("generalization_audit_gates", gates_path, repo_root=root),
        _input_record("online_shadow_policy", policy_path, repo_root=root),
    ]
    runtime_probe_results = {
        **_runtime_flag_probes(),
        **_runtime_negative_probes(),
    }
    guard_results = source_guard_results(runtime_module_path, repo_root=root)
    verification_results = _build_verification_results(
        runtime_payload=runtime_payload,
        gates_payload=gates_payload,
        policy_payload=policy_payload,
        runtime_probe_results=runtime_probe_results,
        guard_results=guard_results,
    )
    decision = all(result["passed"] for result in verification_results)
    blockers = {
        "missing_generalization_audit_on_second_surface": False,
        "missing_generalization_audit_gates": False,
        "missing_online_shadow_implementation_disabled_by_default": False,
        "missing_shadow_runtime_isolation_verification": not decision,
        "missing_production_readiness_authorization": True,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "runtime_implementation_authorized": False,
    }
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "verification_version": verification_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": inputs,
            "runtime_version": runtime_metadata.get("runtime_version"),
            "gates_version": gates_metadata.get("gates_version"),
            "policy_version": policy_metadata.get("policy_version"),
            "runtime_feature_flag": FEATURE_FLAG,
            **_identity_fields(),
        },
        "runtime_isolation_verification_passed": decision,
        "runtime_implementation_present": True,
        "runtime_disabled_by_default": True,
        "runtime_execution_authorized": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "verification_summary": {
            "verification_gates_passed": sum(1 for result in verification_results if result["passed"]),
            "verification_gates_total": len(verification_results),
            "flag_off_cases_verified": len(runtime_probe_results["flag_off_results"]),
            "flag_on_cases_verified": len(runtime_probe_results["flag_on_results"]),
            "incomplete_coverage_cases_verified": len(runtime_probe_results["incomplete_coverage_results"]),
            "label_rejection_cases_verified": len(runtime_probe_results["label_field_rejection_results"]),
            "identity_rejection_cases_verified": len(runtime_probe_results["identity_rejection_results"]),
            "source_guard_passed": guard_results["passed"],
            "runtime_execution_authorized": False,
        },
        "verification_results": verification_results,
        "runtime_probe_results": runtime_probe_results,
        "source_guard_results": guard_results,
        "policy_alignment": {
            "runtime_feature_flag": runtime_payload["runtime_feature_flag"],
            "policy_feature_flag": _get(policy_payload, "runtime_isolation_policy.feature_flag"),
            "policy_feature_flag_default_off": _get(policy_payload, "runtime_isolation_policy.feature_flag_default_off"),
            "policy_disable_switch_default": _get(policy_payload, "disable_and_rollback_policy.disable_switch_default"),
            "gates_recommended_next_stage": gates_payload["recommended_next_stage"],
            "runtime_recommended_next_stage": runtime_payload["recommended_next_stage"],
            "identity_matches_gates": True,
        },
        "blocker_update": {
            "missing_shadow_runtime_isolation_verification_before": _get(
                runtime_payload, "shadow_and_production_blockers.missing_shadow_runtime_isolation_verification"
            ),
            "missing_shadow_runtime_isolation_verification_after": not decision,
            "production_readiness_authorization_remains_required": True,
        },
        "shadow_and_production_blockers": blockers,
        "recommended_next_stage": PASSED_NEXT_STAGE if decision else FAILED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_runtime_isolation_verification(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    summary = payload["verification_summary"]
    blockers = payload["shadow_and_production_blockers"]
    source_guard = payload["source_guard_results"]
    lines = [
        f"# ml-shadow-scorer-v1 Runtime Isolation Verification ({metadata['verification_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact verifies the disabled ml-shadow-scorer-v1 runtime using committed artifacts and in-memory fixtures only. It does not enable online shadow execution or authorize production behavior.",
        "",
        f"- Runtime isolation verification passed: {payload['runtime_isolation_verification_passed']}",
        f"- Runtime disabled by default: {payload['runtime_disabled_by_default']}",
        f"- Runtime execution authorized: {payload['runtime_execution_authorized']}",
        f"- Feature flag: `{metadata['runtime_feature_flag']}`",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Probe Summary",
        "",
        f"- Flag-off cases verified: {summary['flag_off_cases_verified']}",
        f"- Flag-on in-memory cases verified: {summary['flag_on_cases_verified']}",
        f"- Incomplete coverage cases verified: {summary['incomplete_coverage_cases_verified']}",
        f"- Label rejection cases verified: {summary['label_rejection_cases_verified']}",
        f"- Identity rejection cases verified: {summary['identity_rejection_cases_verified']}",
        "",
        "## Verification Gates",
        "",
    ]
    lines.extend(
        f"- `{result['gate_id']}`: {result['status']} - {result['title']}"
        for result in payload["verification_results"]
    )
    lines.extend(
        [
            "",
            "## Source Guard",
            "",
            f"- Runtime module: `{source_guard['runtime_module_path']}`",
            f"- Forbidden import tokens present: {source_guard['forbidden_import_tokens_present']}",
            f"- Web/API import tokens present: {source_guard['web_api_import_tokens_present']}",
            f"- Write SQL verbs present: {source_guard['write_sql_verbs_present']}",
            "",
            "## Remaining Blockers",
            "",
        ]
    )
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_runtime_isolation_verification(
    *,
    online_shadow_runtime_path: Path,
    generalization_audit_gates_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    verification_version: str = VERIFICATION_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_runtime_isolation_verification_payload(
        online_shadow_runtime_path=online_shadow_runtime_path,
        generalization_audit_gates_path=generalization_audit_gates_path,
        online_shadow_policy_path=online_shadow_policy_path,
        verification_version=verification_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_runtime_isolation_verification(payload),
        encoding="utf-8",
    )
    return payload

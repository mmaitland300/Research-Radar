"""Request artifact for future ml-shadow-scorer-v1 online shadow execution authorization.

This module validates the executed enablement gates run and its recorded input
chain, then writes a request artifact. It does not grant authorization, enable
runtime execution, query data stores, write shadow outputs, or alter production
behavior.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_label_dataset import sha256_file
from pipeline.ml_shadow_scorer_online_shadow_enablement_gates import GATES_RUN_VERSION, RUN_ARTIFACT_TYPE
from pipeline.ml_shadow_scorer_online_shadow_runtime import (
    CANDIDATE_POOL_WORK_SET_SHA256,
    CORPUS_SNAPSHOT_VERSION,
    EMBEDDING_VERSION,
    FAMILY,
    FEATURE_FLAG,
    RANKING_RUN_ID,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_execution_authorization_request"
REQUEST_VERSION = "ml-shadow-scorer-v1-online-shadow-execution-authorization-request-v1"
RECOMMENDED_NEXT_STAGE = "record_online_shadow_execution_authorization_grant_v1"

PREREQUISITE_GATE_IDS = (
    "E01_generalization_gates_passed",
    "E02_runtime_disabled_by_default_implemented",
    "E03_runtime_isolation_verification_passed",
    "E04_feature_flag_default_off_and_disable_path_defined",
    "E05_no_production_default_or_api_web_change",
    "E06_shadow_write_isolation_requirement_documented_not_enabled",
    "E07_observability_requirements_defined_for_future_online_run",
    "E08_skip_on_incomplete_coverage_verified",
    "E09_production_default_chain_remains_separate",
)
E10_GATE_ID = "E10_online_shadow_enablement_decision_not_executed"

CAVEATS = (
    "Request artifact only; online shadow execution is not authorized.",
    "This request does not clear missing_online_shadow_execution_authorization.",
    "This request does not grant production readiness or production default authorization.",
    "The runtime feature flag remains default off and must not be enabled by this artifact.",
    "A separate future grant artifact is required before any online shadow execution.",
    "Production default authorization remains a separate chain.",
)


class MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, name: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(f"{name} JSON missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _identity_fields() -> dict[str, str]:
    return {
        "ranking_run_id": RANKING_RUN_ID,
        "family": FAMILY,
        "corpus_snapshot_version": CORPUS_SNAPSHOT_VERSION,
        "embedding_version": EMBEDDING_VERSION,
        "candidate_pool_work_set_sha256": CANDIDATE_POOL_WORK_SET_SHA256,
    }


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(f"Input {name} does not exist: {path}")
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": sha256_file(resolved),
    }


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError("metadata.inputs path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_recorded_input_chain(run_payload: Mapping[str, Any], *, repo_root: Path) -> list[dict[str, Any]]:
    inputs = _get(run_payload, "metadata.inputs")
    if not isinstance(inputs, list) or not inputs:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError("run metadata.inputs must be a non-empty list")
    verified: list[dict[str, Any]] = []
    for index, record in enumerate(inputs):
        if not isinstance(record, Mapping):
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(f"run metadata.inputs[{index}] must be an object")
        name = record.get("name")
        recorded_path = record.get("path")
        recorded_sha = record.get("sha256")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(f"run metadata.inputs[{index}].name missing")
        if not isinstance(recorded_sha, str) or not recorded_sha:
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
                f"run metadata.inputs[{index}].sha256 missing"
            )
        resolved = _resolve_recorded_path(recorded_path, repo_root=repo_root)
        if not resolved.exists():
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
                f"run metadata input {name} missing on disk: {recorded_path}"
            )
        actual_sha = sha256_file(resolved)
        if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
                f"run metadata input {name} sha256 mismatch: recorded {recorded_sha}, actual {actual_sha}"
            )
        verified.append(
            {
                "name": name,
                "path": portable_repo_path(resolved, repo_root=repo_root),
                "sha256": recorded_sha,
                "verification_status": "confirmed",
            }
        )
    return verified


def _gate_results_by_id(run_payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    results = run_payload.get("enablement_gate_results")
    if not isinstance(results, list):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError("enablement_gate_results must be a list")
    by_id: dict[str, Mapping[str, Any]] = {}
    for index, gate in enumerate(results):
        if not isinstance(gate, Mapping):
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(f"enablement_gate_results[{index}] must be an object")
        gate_id = gate.get("gate_id")
        if not isinstance(gate_id, str) or not gate_id:
            raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
                f"enablement_gate_results[{index}].gate_id missing"
            )
        by_id[gate_id] = gate
    return by_id


def _validate_enablement_gate_results(run_payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    _require_equal("run all_prerequisite_gates_satisfied", run_payload.get("all_prerequisite_gates_satisfied"), True)
    by_id = _gate_results_by_id(run_payload)
    missing = [gate_id for gate_id in (*PREREQUISITE_GATE_IDS, E10_GATE_ID) if gate_id not in by_id]
    if missing:
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
            f"enablement_gate_results missing required gates: {', '.join(missing)}"
        )
    for gate_id in PREREQUISITE_GATE_IDS:
        gate = by_id[gate_id]
        _require_equal(f"{gate_id} enablement_gate_executed", gate.get("enablement_gate_executed"), True)
        _require_equal(f"{gate_id} decision", gate.get("decision"), "passed")
    e10 = by_id[E10_GATE_ID]
    _require_equal("E10 enablement_gate_executed", e10.get("enablement_gate_executed"), True)
    _require_equal("E10 decision", e10.get("decision"), "enablement_evaluation_only_not_authorized")
    return by_id


def _validate_run_artifact(run_payload: Mapping[str, Any]) -> Mapping[str, Any]:
    metadata = _metadata(run_payload, name="enablement-gates-run")
    _require_equal("run metadata.artifact_type", metadata.get("artifact_type"), RUN_ARTIFACT_TYPE)
    _require_equal("run metadata.gates_run_version", metadata.get("gates_run_version"), GATES_RUN_VERSION)
    _require_equal("run online_shadow_enablement_gates_executed", run_payload.get("online_shadow_enablement_gates_executed"), True)
    for field, expected in _identity_fields().items():
        _require_equal(f"run metadata.{field}", metadata.get(field), expected)
    required_false = (
        "online_shadow_execution_enabled",
        "shadow_scoring_allowed",
        "runtime_execution_authorized",
        "runtime_implementation_authorized",
        "production_default_allowed",
        "api_web_changes_allowed",
        "user_visible_ranking_changed",
    )
    for field in required_false:
        _require_equal(f"run {field}", run_payload.get(field), False)
    blockers = run_payload.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerOnlineShadowExecutionAuthorizationRequestError(
            "run shadow_and_production_blockers must be an object"
        )
    _require_equal(
        "run shadow_and_production_blockers.missing_online_shadow_execution_authorization",
        blockers.get("missing_online_shadow_execution_authorization"),
        True,
    )
    _require_equal(
        "run shadow_and_production_blockers.missing_production_readiness_authorization",
        blockers.get("missing_production_readiness_authorization"),
        True,
    )
    _require_equal(
        "run shadow_and_production_blockers.missing_online_shadow_enablement_gates",
        blockers.get("missing_online_shadow_enablement_gates"),
        False,
    )
    _validate_enablement_gate_results(run_payload)
    return metadata


def _enablement_summary(by_id: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    prerequisite_summary = [
        {
            "gate_id": gate_id,
            "decision": str(by_id[gate_id].get("decision")),
            "title": str(by_id[gate_id].get("title") or ""),
        }
        for gate_id in PREREQUISITE_GATE_IDS
    ]
    failed = [item["gate_id"] for item in prerequisite_summary if item["decision"] != "passed"]
    return {
        "prerequisite_gates": prerequisite_summary,
        "failed_gate_ids": failed,
        "e10_decision": by_id[E10_GATE_ID].get("decision"),
    }


def build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
    *,
    enablement_gates_run_path: Path,
    request_version: str = REQUEST_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    run_path = Path(enablement_gates_run_path).resolve()
    run_payload = _load_json_object(run_path)
    run_metadata = _validate_run_artifact(run_payload)
    verified_chain = _verify_recorded_input_chain(run_payload, repo_root=root)
    gate_results = _validate_enablement_gate_results(run_payload)
    blockers = dict(run_payload["shadow_and_production_blockers"])
    _require_equal("blocker missing_online_shadow_execution_authorization", blockers.get("missing_online_shadow_execution_authorization"), True)
    _require_equal("blocker missing_production_readiness_authorization", blockers.get("missing_production_readiness_authorization"), True)
    _require_equal("blocker missing_online_shadow_enablement_gates", blockers.get("missing_online_shadow_enablement_gates"), False)
    return {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "request_version": request_version,
            "generated_at": generated_at or _now_iso_z(),
            "inputs": [_input_record("enablement_gates_run", run_path, repo_root=root)],
            "source_gates_run_version": run_metadata.get("gates_run_version"),
            "verified_input_chain": verified_chain,
            **_identity_fields(),
        },
        "online_shadow_enablement_gates_executed": True,
        "all_prerequisite_gates_satisfied": True,
        "online_shadow_execution_authorization_requested": True,
        "online_shadow_execution_authorized": False,
        "authorization_granted": False,
        "online_shadow_execution_enabled": False,
        "shadow_scoring_allowed": False,
        "runtime_execution_authorized": False,
        "runtime_implementation_authorized": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "shadow_and_production_blockers": {
            **blockers,
            "blockers_unchanged_by_request": True,
        },
        "enablement_summary": _enablement_summary(gate_results),
        "remaining_blockers_before_execution": [
            "No owner grant artifact.",
            "missing_online_shadow_execution_authorization remains true and is unchanged by this artifact.",
            f"Feature flag {FEATURE_FLAG} remains default off.",
            "No isolated pilot, observability sink verification, or write-mode isolation proof has been granted for execution.",
            "Production readiness remains research_only; production default authorization is a separate chain.",
        ],
        "consumer_guidance": {
            "executed_truth_source": "Use enablement_gate_results from the run artifact for executed gate truth.",
            "definition_provenance_note": "source_enablement_gate_contract is definition provenance only; do not use definition_only_* decisions.",
            "authorization_note": "This request artifact does not grant authorization and does not clear missing_online_shadow_execution_authorization.",
        },
        "production_separation_note": (
            "This request does not affect production_default_allowed, production readiness, API/web behavior, "
            "or user-visible ranking. Production default authorization remains a separate chain."
        ),
        "required_future_runbook": {
            "future_command": "ml-shadow-scorer-online-shadow-execution-authorization-grant",
            "implemented_in_this_task": False,
            "only_future_grant_may_set_online_shadow_execution_authorized_true": True,
            "only_future_grant_may_clear_missing_online_shadow_execution_authorization": True,
            "must_remain_separate_from": "ml-production-readiness-plan / production_default authorization",
        },
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }


def markdown_from_ml_shadow_scorer_online_shadow_execution_authorization_request(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Execution Authorization Request ({metadata['request_version']})",
        "",
        "## Executive Summary",
        "",
        "This artifact requests owner review for future online shadow execution authorization. It does not grant authorization, enable runtime execution, enable shadow scoring, or change production behavior.",
        "",
        f"- Authorization requested: {payload['online_shadow_execution_authorization_requested']}",
        f"- Authorization granted: {payload['authorization_granted']}",
        f"- Online shadow execution authorized: {payload['online_shadow_execution_authorized']}",
        f"- Online shadow execution enabled: {payload['online_shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Enablement Summary",
        "",
        "| Gate | Decision | Title |",
        "| --- | --- | --- |",
    ]
    for gate in payload["enablement_summary"]["prerequisite_gates"]:
        lines.append(f"| `{gate['gate_id']}` | `{gate['decision']}` | {gate['title']} |")
    lines.extend(
        [
            "",
            f"- Failed gate ids: {payload['enablement_summary']['failed_gate_ids']}",
            f"- E10 decision: `{payload['enablement_summary']['e10_decision']}`",
            "",
            "## Verified Input Chain",
            "",
        ]
    )
    for record in metadata["verified_input_chain"]:
        lines.append(f"- `{record['name']}`: `{record['path']}` ({record['verification_status']})")
    lines.extend(["", "## Remaining Blockers", ""])
    lines.extend(f"- {item}" for item in payload["remaining_blockers_before_execution"])
    lines.extend(["", "## NOT AUTHORIZED", ""])
    lines.extend(
        [
            "- Online shadow execution is not authorized by this artifact.",
            "- Runtime execution remains unauthorized.",
            "- missing_online_shadow_execution_authorization remains true.",
            "",
            "## Production Separation",
            "",
            f"- {payload['production_separation_note']}",
            "",
            "## Blocker State",
            "",
        ]
    )
    lines.extend(f"- `{key}`: {value}" for key, value in blockers.items())
    lines.extend(["", "## Caveats", ""])
    lines.extend(f"- {item}" for item in payload["caveats"])
    return "\n".join(lines).rstrip() + "\n"


def write_ml_shadow_scorer_online_shadow_execution_authorization_request(
    *,
    enablement_gates_run_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    request_version: str = REQUEST_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_online_shadow_execution_authorization_request_payload(
        enablement_gates_run_path=enablement_gates_run_path,
        request_version=request_version,
        repo_root=repo_root,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_online_shadow_execution_authorization_request(payload),
        encoding="utf-8",
    )
    return payload

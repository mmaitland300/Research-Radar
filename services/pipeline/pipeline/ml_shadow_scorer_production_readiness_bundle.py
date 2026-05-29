"""Production-readiness bundle for ml-shadow-scorer-v1.

This bundle is the canonical production-readiness ladder view. It references
the frozen criteria artifact, the reviewed Phase 2 bundle, and optional legacy
evidence by path and SHA. It records a production-readiness authorization
request, but grants nothing and does not run runtime scoring, touch
shadow-runs, access databases, or authorize production/default/API behavior.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_shadow_scorer_phase_bundle import (
    PINNED_IDENTITY,
    POST_REVIEW_ACCEPTED_NEXT_STAGE,
    verify_ml_shadow_scorer_phase_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_readiness_authorization_criteria import (
    ARTIFACT_TYPE as CRITERIA_ARTIFACT_TYPE,
    RECOMMENDED_NEXT_STAGE as CRITERIA_NEXT_STAGE,
    REQUIRED_GATE_FLAGS,
    verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_production_readiness_bundle"
BUNDLE_VERSION = "online-shadow-production-readiness-v1"
BUNDLE_REVISION = 1
PRE_REQUEST_NEXT_STAGE = "request_production_readiness_authorization_v1"
POST_REQUEST_NEXT_STAGE = "record_production_readiness_authorization_grant_v1"
AUTHORIZATION_SCOPE = "production_readiness_for_bounded_online_shadow_only"

REQUIRED_ROLES = ("production_readiness_criteria", "phase2_bundle")
OPTIONAL_ROLES = (
    "generalization_audit_gates",
    "online_shadow_policy",
    "execution_authorization_grant",
    "production_readiness_plan",
)
LEGACY_ARTIFACT_ROLES = REQUIRED_ROLES + OPTIONAL_ROLES

ALLOWED_GATE_STATUSES = {
    "satisfied_by_upstream",
    "partial",
    "open_for_grant",
    "blocked_separate_chain",
}

CAVEATS = (
    "Bundle request milestone only; grants nothing.",
    "Phase 2 accepted evidence is necessary but not sufficient.",
    "Gate partial/open statuses are inputs to owner grant review, not failures of this commit.",
    "This bundle does not enable online shadow execution.",
    "This bundle does not authorize production default/API/user-visible ranking behavior.",
)

EXPLICITLY_NOT_INCLUDED = (
    "online_shadow_execution_enabled globally",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
    "DB writes/DDL",
    "model refit, embedding generation, label ingest",
    "production default / API / fleet-wide flag enablement",
)

WOULD_ENABLE_AFTER_FUTURE_GRANT = (
    "future grant may authorize production-readiness review chain only",
    "future prod-scoped shadow plan/proof/pilot/enablement chain paperwork",
)


class MLShadowScorerProductionReadinessBundleError(Exception):
    def __init__(self, message: str, *, code: int = 2) -> None:
        super().__init__(message)
        self.code = code


def _now_iso_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MLShadowScorerProductionReadinessBundleError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionReadinessBundleError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, label: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerProductionReadinessBundleError(f"{label} missing metadata object")
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
        raise MLShadowScorerProductionReadinessBundleError(f"{name} must be {expected!r}, got {observed!r}")


def _require_true(name: str, observed: Any) -> None:
    _require_equal(name, observed, True)


def _require_false(name: str, observed: Any) -> None:
    _require_equal(name, observed, False)


def _artifact_record(role: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerProductionReadinessBundleError(f"{role} artifact does not exist: {path}")
    return {
        "role": role,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": _sha256_file(resolved),
    }


def _ref_from_record(record: Mapping[str, Any]) -> dict[str, str]:
    return {"path": str(record["path"]), "sha256": str(record["sha256"])}


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerProductionReadinessBundleError("referenced path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_reference(ref: Any, *, repo_root: Path, label: str) -> Path:
    if not isinstance(ref, Mapping):
        raise MLShadowScorerProductionReadinessBundleError(f"{label} reference must be an object")
    recorded_sha = ref.get("sha256")
    if not isinstance(recorded_sha, str) or not recorded_sha.strip():
        raise MLShadowScorerProductionReadinessBundleError(f"{label}.sha256 missing")
    resolved = _resolve_recorded_path(ref.get("path"), repo_root=repo_root)
    if not resolved.exists():
        raise MLShadowScorerProductionReadinessBundleError(f"{label} path missing on disk: {ref.get('path')}")
    if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
        raise MLShadowScorerProductionReadinessBundleError(
            f"{label} sha256 mismatch: recorded {recorded_sha}, actual {_sha256_file(resolved)}"
        )
    return resolved


def _validate_identity(identity: Any, *, label: str) -> None:
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionReadinessBundleError(f"{label} must be an object")
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(f"{label}.{field}", identity.get(field), expected)


def _records_by_role(records: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(records, list) or not records:
        raise MLShadowScorerProductionReadinessBundleError("metadata.legacy_artifacts_index must be a non-empty list")
    by_role: dict[str, Mapping[str, Any]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionReadinessBundleError(
                f"metadata.legacy_artifacts_index[{index}] must be an object"
            )
        role = record.get("role")
        if not isinstance(role, str) or not role:
            raise MLShadowScorerProductionReadinessBundleError(
                f"metadata.legacy_artifacts_index[{index}].role missing"
            )
        if role not in LEGACY_ARTIFACT_ROLES:
            raise MLShadowScorerProductionReadinessBundleError(f"unsupported legacy artifact role {role!r}")
        by_role[role] = record
    missing = [role for role in REQUIRED_ROLES if role not in by_role]
    if missing:
        raise MLShadowScorerProductionReadinessBundleError(
            "metadata.legacy_artifacts_index missing roles: " + ", ".join(missing)
        )
    return by_role


def _verify_legacy_index(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Path]]:
    records = _records_by_role(_metadata(bundle, label="production-readiness bundle").get("legacy_artifacts_index"))
    resolved: dict[str, Path] = {}
    for role, record in records.items():
        resolved[role] = _verify_reference(
            record,
            repo_root=repo_root,
            label=f"metadata.legacy_artifacts_index.{role}",
        )
    return records, resolved


def _validate_criteria(criteria: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
            criteria,
            repo_root=repo_root,
        )
    except Exception as exc:
        raise MLShadowScorerProductionReadinessBundleError(str(exc)) from exc
    _require_equal("criteria metadata.artifact_type", _get(criteria, "metadata.artifact_type"), CRITERIA_ARTIFACT_TYPE)
    _require_true("criteria production_readiness_criteria_defined", criteria.get("production_readiness_criteria_defined"))
    _require_false(
        "criteria production_readiness_authorization_requested",
        criteria.get("production_readiness_authorization_requested"),
    )
    _require_equal("criteria recommended_next_stage", criteria.get("recommended_next_stage"), CRITERIA_NEXT_STAGE)
    _require_true("criteria criteria_artifact_grants_nothing", criteria.get("criteria_artifact_grants_nothing"))
    _require_true("criteria phase2_write_pilot_review_accepted", criteria.get("phase2_write_pilot_review_accepted"))
    _validate_identity(_get(criteria, "metadata.pinned_identity"), label="criteria metadata.pinned_identity")
    gates = criteria.get("production_readiness_required_evidence_gates")
    if not isinstance(gates, Mapping):
        raise MLShadowScorerProductionReadinessBundleError(
            "criteria production_readiness_required_evidence_gates must be an object"
        )
    for gate in REQUIRED_GATE_FLAGS:
        _require_true(f"criteria production_readiness_required_evidence_gates.{gate}", gates.get(gate))


def _validate_phase2_bundle(phase2_bundle: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(
            phase2_bundle,
            repo_root=repo_root,
            expect_pilot_reviewed=True,
        )
    except Exception as exc:
        raise MLShadowScorerProductionReadinessBundleError(str(exc)) from exc
    revision = _get(phase2_bundle, "metadata.bundle_revision")
    if not isinstance(revision, int) or revision < 3:
        raise MLShadowScorerProductionReadinessBundleError(
            f"phase2 bundle metadata.bundle_revision must be >= 3, got {revision!r}"
        )
    _require_true("phase2 bundle review.phase2_write_pilot_accepted", _get(phase2_bundle, "review.phase2_write_pilot_accepted"))
    _require_true("phase2 bundle execution.phase2_write_pilot_passed", _get(phase2_bundle, "execution.phase2_write_pilot_passed"))
    _require_false(
        "phase2 bundle posture.online_shadow_execution_enabled",
        _get(phase2_bundle, "posture.online_shadow_execution_enabled"),
    )
    _validate_identity(_get(phase2_bundle, "posture.pinned_identity"), label="phase2 bundle posture.pinned_identity")


def _refs(records: Mapping[str, Mapping[str, Any]], roles: tuple[str, ...]) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    for role in roles:
        record = records.get(role)
        if record is not None:
            refs.append({"role": role, "path": str(record["path"]), "sha256": str(record["sha256"])})
    return refs


def _assessment(
    *,
    gate_id: str,
    status: str,
    rationale: str,
    records: Mapping[str, Mapping[str, Any]],
    roles: tuple[str, ...],
) -> dict[str, Any]:
    if status not in ALLOWED_GATE_STATUSES:
        raise MLShadowScorerProductionReadinessBundleError(f"unsupported gate status {status!r}")
    return {
        "gate_id": gate_id,
        "status": status,
        "rationale": rationale,
        "evidence_refs": _refs(records, roles),
        "satisfies_criteria_detail": status in {"satisfied_by_upstream", "blocked_separate_chain"},
    }


def _gate_assessments(
    *,
    criteria: Mapping[str, Any],
    phase2_bundle: Mapping[str, Any],
    records: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    joined_count = _get(phase2_bundle, "execution.input_join_summary.joined_candidate_count")
    return {
        "multi_reviewer_adjudication_required": _assessment(
            gate_id="multi_reviewer_adjudication_required",
            status="partial",
            rationale="Phase 2 write pilot review is accepted, but the bundle records one reviewer; owner grant review may require a second named reviewer or approved equivalent.",
            records=records,
            roles=("phase2_bundle",),
        ),
        "label_volume_and_balance_gate_required": _assessment(
            gate_id="label_volume_and_balance_gate_required",
            status="partial",
            rationale=f"Phase 2 covered {joined_count} second-surface rows, while the superseded production-readiness plan still documents label balance gaps that must be accounted for before grant.",
            records=records,
            roles=("phase2_bundle", "production_readiness_plan"),
        ),
        "leakage_control_review_required": _assessment(
            gate_id="leakage_control_review_required",
            status="satisfied_by_upstream",
            rationale="Phase 2 execution records labels_used_for_scoring=false, and upstream generalization gates document the second-surface gate context.",
            records=records,
            roles=("phase2_bundle", "generalization_audit_gates"),
        ),
        "offline_metric_gate_required": _assessment(
            gate_id="offline_metric_gate_required",
            status="satisfied_by_upstream",
            rationale="Generalization audit gates passed for the second surface with 528 candidate rows and material lift evidence before this request.",
            records=records,
            roles=("generalization_audit_gates", "phase2_bundle"),
        ),
        "calibration_and_threshold_review_required": _assessment(
            gate_id="calibration_and_threshold_review_required",
            status="partial",
            rationale="Phase 2 observability includes score and write-path evidence; production calibration thresholds and SLOs remain grant-time criteria.",
            records=records,
            roles=("phase2_bundle",),
        ),
        "subgroup_or_slice_regression_review_required": _assessment(
            gate_id="subgroup_or_slice_regression_review_required",
            status="partial",
            rationale="The accepted evidence is bounded to the emerging-family second surface; broader production slices remain to be reviewed before grant.",
            records=records,
            roles=("phase2_bundle", "generalization_audit_gates"),
        ),
        "production_scope_rollback_disable_drill_required": _assessment(
            gate_id="production_scope_rollback_disable_drill_required",
            status="satisfied_by_upstream",
            rationale="Phase 2 disable drill passed with environment restoration; prior execution grant and policy provide rollback/disable templates for grant review.",
            records=records,
            roles=("phase2_bundle", "execution_authorization_grant", "online_shadow_policy"),
        ),
        "production_observability_slo_required": _assessment(
            gate_id="production_observability_slo_required",
            status="partial",
            rationale="Phase 2 policy contract and observability fields are present, but production SLOs remain explicit grant-time requirements.",
            records=records,
            roles=("phase2_bundle", "online_shadow_policy"),
        ),
        "incident_response_and_revocation_plan_required": _assessment(
            gate_id="incident_response_and_revocation_plan_required",
            status="open_for_grant",
            rationale="Incident response and revocation details must be defined by the future production-readiness grant from existing rollback/revocation templates.",
            records=records,
            roles=("execution_authorization_grant", "online_shadow_policy"),
        ),
        "api_web_default_change_review_required": _assessment(
            gate_id="api_web_default_change_review_required",
            status="blocked_separate_chain",
            rationale="This request explicitly excludes production default and API/web changes; any such change requires a separate authorization chain.",
            records=records,
            roles=("production_readiness_criteria", "phase2_bundle"),
        ),
        "user_visible_ranking_change_review_required": _assessment(
            gate_id="user_visible_ranking_change_review_required",
            status="blocked_separate_chain",
            rationale="This request explicitly excludes user-visible ranking changes; production-facing ranking requires separate authorization.",
            records=records,
            roles=("production_readiness_criteria", "phase2_bundle"),
        ),
        "data_retention_and_auditability_review_required": _assessment(
            gate_id="data_retention_and_auditability_review_required",
            status="partial",
            rationale="Phase 2 file hashes and bundle provenance provide auditability; production retention and audit policy remain grant-time criteria.",
            records=records,
            roles=("production_readiness_criteria", "phase2_bundle", "online_shadow_policy"),
        ),
    }


def _requested_scope() -> dict[str, Any]:
    return {
        "authorization_scope": AUTHORIZATION_SCOPE,
        "would_enable_after_future_grant": list(WOULD_ENABLE_AFTER_FUTURE_GRANT),
        "explicitly_not_included": list(EXPLICITLY_NOT_INCLUDED),
    }


def _posture(*, requested: bool) -> dict[str, Any]:
    return {
        "online_shadow_execution_enabled": False,
        "missing_production_readiness_authorization": True,
        "production_readiness_authorization_requested": requested,
        "production_readiness_authorization_granted": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "phase2_write_pilot_accepted": True,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }


def _blockers(*, requested: bool) -> dict[str, Any]:
    return {
        "missing_production_readiness_authorization": True,
        "production_readiness_authorization_requested": requested,
        "production_readiness_authorization_granted": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "online_shadow_execution_enabled": False,
        "blockers_changed_by_request": [],
        "blockers_unchanged_by_request": True,
    }


def assemble_ml_shadow_scorer_production_readiness_bundle_payload(
    *,
    production_readiness_criteria_path: Path,
    phase_bundle_path: Path,
    generalization_audit_gates_path: Path | None = None,
    online_shadow_policy_path: Path | None = None,
    execution_authorization_grant_path: Path | None = None,
    production_readiness_plan_path: Path | None = None,
    bundle_version: str = BUNDLE_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    criteria_path = Path(production_readiness_criteria_path).resolve()
    phase2_path = Path(phase_bundle_path).resolve()
    criteria = _load_json_object(criteria_path)
    phase2_bundle = _load_json_object(phase2_path)
    _validate_criteria(criteria, repo_root=root)
    _validate_phase2_bundle(phase2_bundle, repo_root=root)
    _validate_identity(_get(criteria, "metadata.pinned_identity"), label="criteria metadata.pinned_identity")
    _validate_identity(_get(phase2_bundle, "posture.pinned_identity"), label="phase2 bundle posture.pinned_identity")

    paths: dict[str, Path] = {
        "production_readiness_criteria": criteria_path,
        "phase2_bundle": phase2_path,
    }
    optional_paths = {
        "generalization_audit_gates": generalization_audit_gates_path,
        "online_shadow_policy": online_shadow_policy_path,
        "execution_authorization_grant": execution_authorization_grant_path,
        "production_readiness_plan": production_readiness_plan_path,
    }
    for role, path in optional_paths.items():
        if path is not None:
            paths[role] = Path(path).resolve()

    records = {role: _artifact_record(role, paths[role], repo_root=root) for role in paths}
    ordered_records = [records[role] for role in LEGACY_ARTIFACT_ROLES if role in records]
    phase2_summary = {
        "phase2_bundle": _ref_from_record(records["phase2_bundle"]),
        "phase2_bundle_revision": _get(phase2_bundle, "metadata.bundle_revision"),
        "phase2_write_pilot_accepted": _get(phase2_bundle, "review.phase2_write_pilot_accepted"),
        "phase2_write_pilot_run_id": _get(phase2_bundle, "execution.pilot_run_id"),
        "phase2_write_pilot_hash_evidence": deepcopy(_get(phase2_bundle, "execution.isolated_file_writes")),
        "phase2_recommended_next_stage_satisfied_by_criteria": POST_REVIEW_ACCEPTED_NEXT_STAGE,
    }
    payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "bundle_version": bundle_version,
            "bundle_revision": BUNDLE_REVISION,
            "generated_at": generated_at or _now_iso_z(),
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "legacy_artifacts_index": ordered_records,
        },
        "criteria_ref": {
            **_ref_from_record(records["production_readiness_criteria"]),
            "production_readiness_criteria_defined": criteria["production_readiness_criteria_defined"],
            "upstream_recommended_next_stage": criteria["recommended_next_stage"],
        },
        "evidence": {
            "phase2_upstream_summary": phase2_summary,
            "superseded_plan_reconciliation": deepcopy(criteria.get("superseded_plan_reconciliation")),
            "gate_assessments": _gate_assessments(
                criteria=criteria,
                phase2_bundle=phase2_bundle,
                records=records,
            ),
        },
        "authorization": {
            "production_readiness_authorization_requested": False,
            "production_readiness_authorization_granted": False,
            "request_decision": None,
            "requested_scope": _requested_scope(),
        },
        "execution": {
            "production_readiness_execution_performed": False,
            "production_shadow_pilot_executed": False,
        },
        "review": {
            "production_readiness_grant_reviewed": False,
            "production_readiness_grant_accepted": None,
        },
        "posture": _posture(requested=False),
        "shadow_and_production_blockers": _blockers(requested=False),
        "writes_performed": False,
        "runtime_writes_performed": False,
        "recommended_next_stage": PRE_REQUEST_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }
    verify_ml_shadow_scorer_production_readiness_bundle_payload(
        payload,
        repo_root=root,
        expect_request_filed=False,
    )
    return payload


def apply_production_readiness_authorization_request(
    bundle: Mapping[str, Any],
    *,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("bundle must be an object")
    requested_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    metadata = deepcopy(dict(_metadata(updated, label="production-readiness bundle")))
    metadata["generated_at"] = requested_at
    metadata["bundle_revision"] = BUNDLE_REVISION
    updated["metadata"] = metadata
    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization["production_readiness_authorization_requested"] = True
    authorization["production_readiness_authorization_granted"] = False
    authorization["request_decision"] = {
        "decision": "requested",
        "requester": requester,
        "requested_at": requested_at,
        "request_notes": request_notes,
    }
    authorization["requested_scope"] = _requested_scope()
    updated["authorization"] = authorization
    updated["posture"] = _posture(requested=True)
    updated["shadow_and_production_blockers"] = _blockers(requested=True)
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_REQUEST_NEXT_STAGE
    return updated


def _infer_request_mode(
    bundle: Mapping[str, Any],
    *,
    expect_request_filed: bool | None,
) -> str:
    if expect_request_filed is True:
        return "post_request"
    if expect_request_filed is False:
        return "pre_request"
    return (
        "post_request"
        if _get(bundle, "authorization.production_readiness_authorization_requested") is True
        else "pre_request"
    )


def _verify_gate_assessments(
    assessments: Any,
    *,
    records: Mapping[str, Mapping[str, Any]],
) -> None:
    if not isinstance(assessments, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("evidence.gate_assessments must be an object")
    missing = [gate for gate in REQUIRED_GATE_FLAGS if gate not in assessments]
    if missing:
        raise MLShadowScorerProductionReadinessBundleError(
            "evidence.gate_assessments missing keys: " + ", ".join(missing)
        )
    for gate in REQUIRED_GATE_FLAGS:
        entry = assessments[gate]
        if not isinstance(entry, Mapping):
            raise MLShadowScorerProductionReadinessBundleError(f"evidence.gate_assessments.{gate} must be an object")
        _require_equal(f"evidence.gate_assessments.{gate}.gate_id", entry.get("gate_id"), gate)
        status = entry.get("status")
        if status not in ALLOWED_GATE_STATUSES:
            raise MLShadowScorerProductionReadinessBundleError(
                f"evidence.gate_assessments.{gate}.status unsupported: {status!r}"
            )
        rationale = entry.get("rationale")
        if not isinstance(rationale, str) or not rationale.strip():
            raise MLShadowScorerProductionReadinessBundleError(
                f"evidence.gate_assessments.{gate}.rationale must be non-empty"
            )
        expected_satisfies = status in {"satisfied_by_upstream", "blocked_separate_chain"}
        _require_equal(
            f"evidence.gate_assessments.{gate}.satisfies_criteria_detail",
            entry.get("satisfies_criteria_detail"),
            expected_satisfies,
        )
        refs = entry.get("evidence_refs")
        if refs is not None:
            if not isinstance(refs, list):
                raise MLShadowScorerProductionReadinessBundleError(
                    f"evidence.gate_assessments.{gate}.evidence_refs must be a list"
                )
            for index, ref in enumerate(refs):
                if not isinstance(ref, Mapping):
                    raise MLShadowScorerProductionReadinessBundleError(
                        f"evidence.gate_assessments.{gate}.evidence_refs[{index}] must be an object"
                    )
                role = ref.get("role")
                if role not in records:
                    raise MLShadowScorerProductionReadinessBundleError(
                        f"evidence.gate_assessments.{gate}.evidence_refs[{index}].role not indexed"
                    )
                _require_equal(
                    f"evidence.gate_assessments.{gate}.evidence_refs[{index}].path",
                    ref.get("path"),
                    records[str(role)].get("path"),
                )
                _require_equal(
                    f"evidence.gate_assessments.{gate}.evidence_refs[{index}].sha256",
                    ref.get("sha256"),
                    records[str(role)].get("sha256"),
                )


def verify_ml_shadow_scorer_production_readiness_bundle_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
    expect_request_filed: bool | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    metadata = _metadata(bundle, label="production-readiness bundle")
    _require_equal("metadata.artifact_type", metadata.get("artifact_type"), ARTIFACT_TYPE)
    _require_equal("metadata.bundle_version", metadata.get("bundle_version"), BUNDLE_VERSION)
    _require_equal("metadata.bundle_revision", metadata.get("bundle_revision"), BUNDLE_REVISION)
    _validate_identity(metadata.get("pinned_identity"), label="metadata.pinned_identity")
    records, resolved_paths = _verify_legacy_index(bundle, repo_root=root)

    criteria_ref = bundle.get("criteria_ref")
    if not isinstance(criteria_ref, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("criteria_ref must be an object")
    criteria_ref_path = _verify_reference(criteria_ref, repo_root=root, label="criteria_ref")
    if criteria_ref_path != resolved_paths["production_readiness_criteria"]:
        raise MLShadowScorerProductionReadinessBundleError("criteria_ref path must match legacy index")
    _require_true("criteria_ref.production_readiness_criteria_defined", criteria_ref.get("production_readiness_criteria_defined"))
    _require_equal("criteria_ref.upstream_recommended_next_stage", criteria_ref.get("upstream_recommended_next_stage"), CRITERIA_NEXT_STAGE)

    criteria = _load_json_object(resolved_paths["production_readiness_criteria"])
    phase2_bundle = _load_json_object(resolved_paths["phase2_bundle"])
    _validate_criteria(criteria, repo_root=root)
    _validate_phase2_bundle(phase2_bundle, repo_root=root)
    _validate_identity(_get(criteria, "metadata.pinned_identity"), label="criteria metadata.pinned_identity")
    _validate_identity(_get(phase2_bundle, "posture.pinned_identity"), label="phase2 bundle posture.pinned_identity")

    evidence = bundle.get("evidence")
    if not isinstance(evidence, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("evidence must be an object")
    phase2_summary = evidence.get("phase2_upstream_summary")
    if not isinstance(phase2_summary, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("evidence.phase2_upstream_summary must be an object")
    _require_true(
        "evidence.phase2_upstream_summary.phase2_write_pilot_accepted",
        phase2_summary.get("phase2_write_pilot_accepted"),
    )
    _require_equal(
        "evidence.phase2_upstream_summary.phase2_write_pilot_run_id",
        phase2_summary.get("phase2_write_pilot_run_id"),
        _get(phase2_bundle, "execution.pilot_run_id"),
    )
    if not isinstance(phase2_summary.get("phase2_write_pilot_hash_evidence"), Mapping):
        raise MLShadowScorerProductionReadinessBundleError(
            "evidence.phase2_upstream_summary.phase2_write_pilot_hash_evidence must be an object"
        )
    if not isinstance(evidence.get("superseded_plan_reconciliation"), Mapping):
        raise MLShadowScorerProductionReadinessBundleError(
            "evidence.superseded_plan_reconciliation must be an object"
        )
    _verify_gate_assessments(evidence.get("gate_assessments"), records=records)

    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("authorization must be an object")
    _require_false(
        "authorization.production_readiness_authorization_granted",
        authorization.get("production_readiness_authorization_granted"),
    )
    requested_scope = authorization.get("requested_scope")
    if not isinstance(requested_scope, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("authorization.requested_scope must be an object")
    _require_equal("authorization.requested_scope.authorization_scope", requested_scope.get("authorization_scope"), AUTHORIZATION_SCOPE)
    for item in EXPLICITLY_NOT_INCLUDED:
        if item not in requested_scope.get("explicitly_not_included", []):
            raise MLShadowScorerProductionReadinessBundleError(
                f"authorization.requested_scope.explicitly_not_included missing {item!r}"
            )

    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("posture must be an object")
    posture_required = {
        "online_shadow_execution_enabled": False,
        "missing_production_readiness_authorization": True,
        "production_readiness_authorization_granted": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "phase2_write_pilot_accepted": True,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    for field, expected in posture_required.items():
        _require_equal(f"posture.{field}", posture.get(field), expected)

    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("execution must be an object")
    _require_false("execution.production_readiness_execution_performed", execution.get("production_readiness_execution_performed"))
    _require_false("execution.production_shadow_pilot_executed", execution.get("production_shadow_pilot_executed"))
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("review must be an object")
    _require_false("review.production_readiness_grant_reviewed", review.get("production_readiness_grant_reviewed"))
    _require_equal("review.production_readiness_grant_accepted", review.get("production_readiness_grant_accepted"), None)
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionReadinessBundleError("shadow_and_production_blockers must be an object")
    _require_true("shadow_and_production_blockers.missing_production_readiness_authorization", blockers.get("missing_production_readiness_authorization"))
    _require_equal("shadow_and_production_blockers.blockers_changed_by_request", blockers.get("blockers_changed_by_request"), [])
    _require_true("shadow_and_production_blockers.blockers_unchanged_by_request", blockers.get("blockers_unchanged_by_request"))
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))

    mode = _infer_request_mode(bundle, expect_request_filed=expect_request_filed)
    if mode == "pre_request":
        _require_false(
            "authorization.production_readiness_authorization_requested",
            authorization.get("production_readiness_authorization_requested"),
        )
        _require_equal("authorization.request_decision", authorization.get("request_decision"), None)
        _require_false(
            "posture.production_readiness_authorization_requested",
            posture.get("production_readiness_authorization_requested"),
        )
        _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), PRE_REQUEST_NEXT_STAGE)
    else:
        _require_true(
            "authorization.production_readiness_authorization_requested",
            authorization.get("production_readiness_authorization_requested"),
        )
        request_decision = authorization.get("request_decision")
        if not isinstance(request_decision, Mapping):
            raise MLShadowScorerProductionReadinessBundleError("authorization.request_decision must be an object")
        _require_equal("authorization.request_decision.decision", request_decision.get("decision"), "requested")
        if not isinstance(request_decision.get("requester"), str) or not request_decision.get("requester"):
            raise MLShadowScorerProductionReadinessBundleError("authorization.request_decision.requester must be populated")
        if not isinstance(request_decision.get("requested_at"), str) or not request_decision.get("requested_at"):
            raise MLShadowScorerProductionReadinessBundleError("authorization.request_decision.requested_at must be populated")
        _require_true(
            "posture.production_readiness_authorization_requested",
            posture.get("production_readiness_authorization_requested"),
        )
        _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_REQUEST_NEXT_STAGE)

    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionReadinessBundleError("caveats must be a list")
    for caveat in CAVEATS:
        if caveat not in caveats:
            raise MLShadowScorerProductionReadinessBundleError(f"caveats missing {caveat!r}")
    return {
        "verification_status": "passed",
        "verification_mode": mode,
        "bundle_version": metadata.get("bundle_version"),
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
        "legacy_artifact_count": len(records),
    }


def verify_ml_shadow_scorer_production_readiness_bundle(
    *,
    bundle_path: Path,
    repo_root: Path | None = None,
    expect_request_filed: bool | None = None,
) -> dict[str, Any]:
    payload = _load_json_object(Path(bundle_path).resolve())
    return verify_ml_shadow_scorer_production_readiness_bundle_payload(
        payload,
        repo_root=repo_root,
        expect_request_filed=expect_request_filed,
    )


def markdown_from_ml_shadow_scorer_production_readiness_bundle(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    identity = metadata["pinned_identity"]
    authorization = payload["authorization"]
    evidence = payload["evidence"]
    posture = payload["posture"]
    lines = [
        f"# ml-shadow-scorer-v1 Production Readiness Bundle ({metadata['bundle_version']})",
        "",
        "## Executive Summary",
        "",
        "This bundle is the canonical production-readiness ladder view. It records a request for production-readiness authorization while granting nothing and preserving all production/API/default/user-visible blockers.",
        "",
        f"- Bundle revision: {metadata['bundle_revision']}",
        f"- Production readiness authorization requested: {authorization['production_readiness_authorization_requested']}",
        f"- Production readiness authorization granted: {authorization['production_readiness_authorization_granted']}",
        f"- Missing production readiness authorization: {posture['missing_production_readiness_authorization']}",
        f"- Online shadow execution enabled: {posture['online_shadow_execution_enabled']}",
        f"- Recommended next stage: `{payload['recommended_next_stage']}`",
        "",
        "## Pinned Identity",
        "",
    ]
    for key, value in identity.items():
        lines.append(f"- {key}: `{value}`")
    lines.extend(
        [
            "",
            "## Legacy Artifact Index",
            "",
            "| Role | Path | SHA-256 |",
            "| --- | --- | --- |",
        ]
    )
    for record in metadata["legacy_artifacts_index"]:
        lines.append(f"| {record['role']} | `{record['path']}` | `{record['sha256']}` |")
    lines.extend(
        [
            "",
            "## Criteria Reference",
            "",
            f"- Criteria: `{payload['criteria_ref']['path']}`",
            f"- Criteria SHA-256: `{payload['criteria_ref']['sha256']}`",
            f"- Criteria defined: {payload['criteria_ref']['production_readiness_criteria_defined']}",
            f"- Upstream next stage: `{payload['criteria_ref']['upstream_recommended_next_stage']}`",
            "",
            "## Phase 2 Evidence",
            "",
            f"- Phase 2 pilot accepted: {evidence['phase2_upstream_summary']['phase2_write_pilot_accepted']}",
            f"- Pilot run id: `{evidence['phase2_upstream_summary']['phase2_write_pilot_run_id']}`",
            "",
            "## Gate Assessments",
            "",
            "| Gate | Status | Satisfies Detail | Rationale |",
            "| --- | --- | --- | --- |",
        ]
    )
    for gate, assessment in evidence["gate_assessments"].items():
        rationale = str(assessment["rationale"]).replace("|", "\\|")
        lines.append(
            f"| `{gate}` | `{assessment['status']}` | {assessment['satisfies_criteria_detail']} | {rationale} |"
        )
    request_decision = authorization.get("request_decision") or {}
    lines.extend(
        [
            "",
            "## Authorization Request",
            "",
            f"- Decision: `{request_decision.get('decision')}`",
            f"- Requester: {request_decision.get('requester')}",
            f"- Requested at: {request_decision.get('requested_at')}",
            f"- Request notes: {request_decision.get('request_notes')}",
            f"- Requested scope: `{authorization['requested_scope']['authorization_scope']}`",
            "",
            "## Explicitly Not Included",
            "",
        ]
    )
    for item in authorization["requested_scope"]["explicitly_not_included"]:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Production/API/Default Separation",
            "",
            f"- Production default allowed: {posture['production_default_allowed']}",
            f"- API/web changes allowed: {posture['api_web_changes_allowed']}",
            f"- User-visible ranking changed: {posture['user_visible_ranking_changed']}",
            f"- Writes performed: {payload['writes_performed']}",
            f"- Runtime writes performed: {payload['runtime_writes_performed']}",
            "",
            "## Recommended Next Stage",
            "",
            f"`{payload['recommended_next_stage']}`",
            "",
            "## Caveats",
            "",
        ]
    )
    for caveat in payload["caveats"]:
        lines.append(f"- {caveat}")
    lines.append("")
    return "\n".join(lines)


def write_ml_shadow_scorer_production_readiness_bundle(
    *,
    production_readiness_criteria_path: Path,
    phase_bundle_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    generalization_audit_gates_path: Path | None = None,
    online_shadow_policy_path: Path | None = None,
    execution_authorization_grant_path: Path | None = None,
    production_readiness_plan_path: Path | None = None,
    bundle_version: str = BUNDLE_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = assemble_ml_shadow_scorer_production_readiness_bundle_payload(
        production_readiness_criteria_path=production_readiness_criteria_path,
        phase_bundle_path=phase_bundle_path,
        generalization_audit_gates_path=generalization_audit_gates_path,
        online_shadow_policy_path=online_shadow_policy_path,
        execution_authorization_grant_path=execution_authorization_grant_path,
        production_readiness_plan_path=production_readiness_plan_path,
        bundle_version=bundle_version,
        repo_root=repo_root,
    )
    output_path = Path(output_path)
    markdown_output_path = Path(markdown_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_production_readiness_bundle(payload), encoding="utf-8")
    return payload


def request_ml_shadow_scorer_production_readiness_bundle(
    *,
    bundle_path: Path,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    verify_ml_shadow_scorer_production_readiness_bundle_payload(
        payload,
        repo_root=root,
        expect_request_filed=False,
    )
    updated = apply_production_readiness_authorization_request(
        payload,
        requester=requester,
        request_notes=request_notes,
    )
    verify_ml_shadow_scorer_production_readiness_bundle_payload(
        updated,
        repo_root=root,
        expect_request_filed=True,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_readiness_bundle(updated),
        encoding="utf-8",
    )
    return updated

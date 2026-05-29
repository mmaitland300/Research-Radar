"""Open production-readiness authorization criteria for ml-shadow-scorer-v1.

This criteria artifact starts the production-readiness authorization chain by
recording the evidence gates required before any production/default/API or
user-visible ranking change can be requested. It grants nothing, runs no
runtime, writes no shadow-run files, and does not mutate the Phase 2 bundle or
legacy evidence artifacts.
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
    MLShadowScorerPhaseBundleError,
    PINNED_IDENTITY,
    POST_REVIEW_ACCEPTED_NEXT_STAGE,
    verify_ml_shadow_scorer_phase_bundle_payload,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_production_readiness_authorization_criteria"
CRITERIA_VERSION = "ml-shadow-scorer-v1-production-readiness-authorization-criteria-v1"
RECOMMENDED_NEXT_STAGE = "request_production_readiness_authorization_v1"

REQUIRED_GATE_FLAGS: tuple[str, ...] = (
    "multi_reviewer_adjudication_required",
    "label_volume_and_balance_gate_required",
    "leakage_control_review_required",
    "offline_metric_gate_required",
    "calibration_and_threshold_review_required",
    "subgroup_or_slice_regression_review_required",
    "production_scope_rollback_disable_drill_required",
    "production_observability_slo_required",
    "incident_response_and_revocation_plan_required",
    "api_web_default_change_review_required",
    "user_visible_ranking_change_review_required",
    "data_retention_and_auditability_review_required",
)

EXPLICIT_NON_AUTHORIZATIONS: tuple[str, ...] = (
    "This artifact does not request production readiness authorization.",
    "This artifact does not grant production readiness authorization.",
    "This artifact does not authorize production default changes.",
    "This artifact does not authorize API/web changes.",
    "This artifact does not authorize user-visible ranking changes.",
    "This artifact does not authorize DB writes or DDL.",
    "This artifact does not authorize global online shadow execution.",
    "This artifact does not authorize model/scorer refits, embedding generation, or label ingest.",
)

CAVEATS: tuple[str, ...] = (
    "Criteria artifact only; grants nothing.",
    "Does not run shadow scoring or any runtime.",
    "Does not write shadow-runs/ files.",
    "Does not mutate legacy evidence artifacts.",
    "Does not enable online shadow execution globally.",
    "Does not authorize production readiness, production default, API/web, or user-visible ranking changes.",
    "Supersedes only stale readiness posture language, not historical evidence.",
)


class MLShadowScorerProductionReadinessAuthorizationCriteriaError(Exception):
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
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            f"Failed to load JSON {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, label: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(f"{label} missing metadata object")
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
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _require_true(name: str, observed: Any) -> None:
    _require_equal(name, observed, True)


def _require_false(name: str, observed: Any) -> None:
    _require_equal(name, observed, False)


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            "recorded input path must be a non-empty string"
        )
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _input_record(name: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            f"Input {name} does not exist: {path}"
        )
    return {
        "name": name,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": _sha256_file(resolved),
    }


def _records_by_name(records: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(records, list) or not records:
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError("metadata.inputs must be a non-empty list")
    by_name: dict[str, Mapping[str, Any]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
                f"metadata.inputs[{index}] must be an object"
            )
        name = record.get("name")
        if not isinstance(name, str) or not name:
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
                f"metadata.inputs[{index}].name missing"
            )
        by_name[name] = record
    return by_name


def _verify_input_record(record: Mapping[str, Any], *, repo_root: Path, label: str) -> Path:
    recorded_sha = record.get("sha256")
    if not isinstance(recorded_sha, str) or not recorded_sha.strip():
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(f"{label}.sha256 missing")
    resolved = _resolve_recorded_path(record.get("path"), repo_root=repo_root)
    if not resolved.exists():
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            f"{label} path missing on disk: {record.get('path')}"
        )
    if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            f"{label} sha256 mismatch: recorded {recorded_sha}, actual {_sha256_file(resolved)}"
        )
    return resolved


def _validate_identity(identity: Any, *, label: str) -> None:
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(f"{label} must be an object")
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(f"{label}.{field}", identity.get(field), expected)


def _validate_phase_bundle(bundle: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(
            bundle,
            repo_root=repo_root,
            expect_pilot_reviewed=True,
        )
    except MLShadowScorerPhaseBundleError as exc:
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(str(exc)) from exc
    _require_equal("phase bundle metadata.artifact_type", _get(bundle, "metadata.artifact_type"), "ml_shadow_scorer_phase_bundle")
    _require_equal("phase bundle metadata.bundle_version", _get(bundle, "metadata.bundle_version"), "online-shadow-phase2-v1")
    revision = _get(bundle, "metadata.bundle_revision")
    if not isinstance(revision, int) or revision < 3:
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            f"phase bundle metadata.bundle_revision must be >= 3, got {revision!r}"
        )
    required = {
        "review.phase2_write_pilot_reviewed": True,
        "review.phase2_write_pilot_accepted": True,
        "review.review_decision.decision": "accepted",
        "recommended_next_stage": POST_REVIEW_ACCEPTED_NEXT_STAGE,
        "posture.online_shadow_execution_enabled": False,
        "posture.production_default_allowed": False,
        "posture.api_web_changes_allowed": False,
        "posture.user_visible_ranking_changed": False,
        "posture.missing_production_readiness_authorization": True,
        "execution.phase2_write_pilot_executed": True,
        "execution.phase2_write_pilot_passed": True,
        "execution.write_count_verification.forbidden_targets_zero": True,
        "execution.runtime_writes_performed": False,
        "execution.labels_used_for_scoring": False,
    }
    for path, expected in required.items():
        _require_equal(f"phase bundle {path}", _get(bundle, path), expected)
    _validate_identity(_get(bundle, "posture.pinned_identity"), label="phase bundle posture.pinned_identity")


def _source_evidence_from_bundle(
    bundle: Mapping[str, Any],
    *,
    phase_bundle_record: Mapping[str, str],
) -> dict[str, Any]:
    execution = bundle["execution"]
    review = bundle["review"]
    return {
        "phase_bundle": {
            "path": phase_bundle_record["path"],
            "sha256": phase_bundle_record["sha256"],
        },
        "phase_bundle_revision": _get(bundle, "metadata.bundle_revision"),
        "phase2_write_pilot_reviewed": review.get("phase2_write_pilot_reviewed"),
        "phase2_write_pilot_accepted": review.get("phase2_write_pilot_accepted"),
        "phase2_write_pilot_review_decision": _get(review, "review_decision.decision"),
        "phase2_write_pilot_run_id": execution.get("pilot_run_id"),
        "phase2_write_pilot_hash_evidence": deepcopy(execution.get("isolated_file_writes")),
        "upstream_recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _superseded_plan_reconciliation(record: Mapping[str, str] | None) -> dict[str, Any]:
    if record is None:
        return {
            "superseded_plan_path": None,
            "superseded_plan_sha256": None,
            "superseded_by": CRITERIA_VERSION,
            "reconciliation_summary": [
                "No superseded production-readiness plan path was provided.",
                "Production readiness remains blocked despite Phase 2 acceptance.",
                "This artifact defines criteria only and grants no production behavior.",
            ],
        }
    return {
        "superseded_plan_path": record["path"],
        "superseded_plan_sha256": record["sha256"],
        "superseded_by": CRITERIA_VERSION,
        "reconciliation_summary": [
            "The older production-readiness plan predates the accepted Phase 2 shadow ladder.",
            "Its claims that shadow scoring cannot start or shadow gates are not started are superseded by the accepted Phase 2 isolated-audit write pilot review.",
            "Production readiness remains blocked despite Phase 2 acceptance.",
            "This artifact defines criteria only and grants no production behavior.",
        ],
    }


def _required_gate_details() -> dict[str, Any]:
    return {
        "multi_reviewer_adjudication": [
            "must include at least two named reviewers or owner-approved equivalent",
            "must document accepted/rejected decision and dissent if any",
        ],
        "label_volume_and_balance": [
            "must document minimum sample counts, class balance, and second-surface coverage",
            "must reject production readiness if labels are sparse or materially skewed",
        ],
        "leakage_controls": [
            "must verify no label leakage into runtime features or scoring inputs",
            "must verify training/evaluation separation",
        ],
        "offline_metric_gates": [
            "must define pass thresholds before production authorization request",
            "must include baseline comparison against current production/default ranking",
        ],
        "rollback_disable_drill": [
            "must prove production-scope disable and rollback steps before any default/API exposure",
            "must keep ML_SHADOW_SCORER_V1_RUNTIME_ENABLED default off unless separately authorized",
        ],
        "observability_slo": [
            "must define run-level, component-level, error, latency, and write-target observability",
        ],
        "production_change_review": [
            "must separately authorize production default, API/web, and user-visible ranking changes",
        ],
    }


def build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
    *,
    phase_bundle_path: Path,
    superseded_production_readiness_plan_path: Path | None = None,
    criteria_version: str = CRITERIA_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    phase_bundle_path = Path(phase_bundle_path).resolve()
    phase_bundle = _load_json_object(phase_bundle_path)
    _validate_phase_bundle(phase_bundle, repo_root=root)

    inputs = [_input_record("phase_bundle", phase_bundle_path, repo_root=root)]
    superseded_record: dict[str, str] | None = None
    if superseded_production_readiness_plan_path is not None:
        superseded_record = _input_record(
            "superseded_production_readiness_plan",
            Path(superseded_production_readiness_plan_path).resolve(),
            repo_root=root,
        )
        inputs.append(superseded_record)

    payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "criteria_version": criteria_version,
            "generated_at": generated_at or _now_iso_z(),
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "inputs": inputs,
        },
        "production_readiness_criteria_defined": True,
        "production_readiness_authorization_requested": False,
        "production_readiness_authorization_granted": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "online_shadow_execution_enabled": False,
        "runtime_execution_performed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
        "missing_production_readiness_authorization": True,
        "criteria_artifact_grants_nothing": True,
        "phase2_write_pilot_review_accepted": True,
        "source_evidence": _source_evidence_from_bundle(phase_bundle, phase_bundle_record=inputs[0]),
        "superseded_plan_reconciliation": _superseded_plan_reconciliation(superseded_record),
        "production_readiness_required_evidence_gates": {name: True for name in REQUIRED_GATE_FLAGS},
        "required_gate_details": _required_gate_details(),
        "explicit_non_authorizations": list(EXPLICIT_NON_AUTHORIZATIONS),
        "shadow_and_production_blockers": {
            "missing_production_readiness_authorization": True,
            "production_readiness_authorization_requested": False,
            "production_readiness_authorization_granted": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "online_shadow_execution_enabled": False,
            "blockers_changed_by_criteria": [],
            "blockers_unchanged_by_criteria": True,
        },
        "consumer_guidance": [
            "Treat this artifact as criteria and scope only.",
            "A future request artifact must satisfy or explicitly account for every required gate.",
            "Only a future production-readiness grant may clear missing_production_readiness_authorization.",
            "Only a future explicit production/default/API authorization may change user-visible behavior.",
            "The accepted Phase 2 write pilot is necessary evidence, not sufficient production authorization.",
        ],
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
        "caveats": list(CAVEATS),
    }
    verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
        payload,
        repo_root=root,
        expected_criteria_version=criteria_version,
    )
    return payload


def verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
    payload: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
    expected_criteria_version: str = CRITERIA_VERSION,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    metadata = _metadata(payload, label="criteria")
    _require_equal("metadata.artifact_type", metadata.get("artifact_type"), ARTIFACT_TYPE)
    _require_equal("metadata.criteria_version", metadata.get("criteria_version"), expected_criteria_version)
    _validate_identity(metadata.get("pinned_identity"), label="metadata.pinned_identity")

    records = _records_by_name(metadata.get("inputs"))
    if "phase_bundle" not in records:
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError("metadata.inputs missing phase_bundle")
    phase_bundle_path = _verify_input_record(records["phase_bundle"], repo_root=root, label="metadata.inputs.phase_bundle")
    phase_bundle = _load_json_object(phase_bundle_path)
    _validate_phase_bundle(phase_bundle, repo_root=root)

    source = payload.get("source_evidence")
    if not isinstance(source, Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError("source_evidence must be an object")
    phase_bundle_source = source.get("phase_bundle")
    if not isinstance(phase_bundle_source, Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError("source_evidence.phase_bundle must be an object")
    _require_equal("source_evidence.phase_bundle.path", phase_bundle_source.get("path"), records["phase_bundle"].get("path"))
    _require_equal("source_evidence.phase_bundle.sha256", phase_bundle_source.get("sha256"), records["phase_bundle"].get("sha256"))
    _require_equal("source_evidence.phase_bundle_revision", source.get("phase_bundle_revision"), _get(phase_bundle, "metadata.bundle_revision"))
    _require_true("source_evidence.phase2_write_pilot_reviewed", source.get("phase2_write_pilot_reviewed"))
    _require_true("source_evidence.phase2_write_pilot_accepted", source.get("phase2_write_pilot_accepted"))
    _require_equal("source_evidence.phase2_write_pilot_review_decision", source.get("phase2_write_pilot_review_decision"), "accepted")
    _require_equal("source_evidence.upstream_recommended_next_stage", source.get("upstream_recommended_next_stage"), POST_REVIEW_ACCEPTED_NEXT_STAGE)
    if not isinstance(source.get("phase2_write_pilot_hash_evidence"), Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            "source_evidence.phase2_write_pilot_hash_evidence must be an object"
        )

    if "superseded_production_readiness_plan" in records:
        plan_path = _verify_input_record(
            records["superseded_production_readiness_plan"],
            repo_root=root,
            label="metadata.inputs.superseded_production_readiness_plan",
        )
        reconciliation = payload.get("superseded_plan_reconciliation")
        if not isinstance(reconciliation, Mapping):
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
                "superseded_plan_reconciliation must be an object"
            )
        _require_equal(
            "superseded_plan_reconciliation.superseded_plan_path",
            reconciliation.get("superseded_plan_path"),
            records["superseded_production_readiness_plan"].get("path"),
        )
        _require_equal(
            "superseded_plan_reconciliation.superseded_plan_sha256",
            reconciliation.get("superseded_plan_sha256"),
            records["superseded_production_readiness_plan"].get("sha256"),
        )
        if plan_path != _resolve_recorded_path(reconciliation.get("superseded_plan_path"), repo_root=root):
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
                "superseded_plan_reconciliation path must resolve to recorded plan path"
            )
        summary = reconciliation.get("reconciliation_summary")
        if not isinstance(summary, list) or len(summary) < 4:
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
                "superseded_plan_reconciliation.reconciliation_summary must document the superseded posture"
            )

    required_top_level = {
        "production_readiness_criteria_defined": True,
        "production_readiness_authorization_requested": False,
        "production_readiness_authorization_granted": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "online_shadow_execution_enabled": False,
        "runtime_execution_performed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
        "missing_production_readiness_authorization": True,
        "criteria_artifact_grants_nothing": True,
        "phase2_write_pilot_review_accepted": True,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
    }
    for field, expected in required_top_level.items():
        _require_equal(field, payload.get(field), expected)

    gates = payload.get("production_readiness_required_evidence_gates")
    if not isinstance(gates, Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            "production_readiness_required_evidence_gates must be an object"
        )
    for gate in REQUIRED_GATE_FLAGS:
        _require_true(f"production_readiness_required_evidence_gates.{gate}", gates.get(gate))
    if not isinstance(payload.get("required_gate_details"), Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError("required_gate_details must be an object")

    non_authorizations = payload.get("explicit_non_authorizations")
    if not isinstance(non_authorizations, list):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            "explicit_non_authorizations must be a list"
        )
    for statement in EXPLICIT_NON_AUTHORIZATIONS:
        if statement not in non_authorizations:
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
                f"explicit_non_authorizations missing {statement!r}"
            )

    blockers = payload.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(
            "shadow_and_production_blockers must be an object"
        )
    blocker_required = {
        "missing_production_readiness_authorization": True,
        "production_readiness_authorization_requested": False,
        "production_readiness_authorization_granted": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "online_shadow_execution_enabled": False,
        "blockers_changed_by_criteria": [],
        "blockers_unchanged_by_criteria": True,
    }
    for field, expected in blocker_required.items():
        _require_equal(f"shadow_and_production_blockers.{field}", blockers.get(field), expected)

    caveats = payload.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionReadinessAuthorizationCriteriaError("caveats must be a list")
    for caveat in CAVEATS:
        if caveat not in caveats:
            raise MLShadowScorerProductionReadinessAuthorizationCriteriaError(f"caveats missing {caveat!r}")

    return {
        "verification_status": "passed",
        "criteria_version": metadata.get("criteria_version"),
        "recommended_next_stage": payload.get("recommended_next_stage"),
        "phase_bundle_revision": source.get("phase_bundle_revision"),
    }


def verify_ml_shadow_scorer_production_readiness_authorization_criteria(
    *,
    criteria_path: Path,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = _load_json_object(Path(criteria_path).resolve())
    return verify_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
        payload,
        repo_root=repo_root,
    )


def markdown_from_ml_shadow_scorer_production_readiness_authorization_criteria(
    payload: Mapping[str, Any],
) -> str:
    metadata = payload["metadata"]
    identity = metadata["pinned_identity"]
    source = payload["source_evidence"]
    reconciliation = payload["superseded_plan_reconciliation"]
    gates = payload["production_readiness_required_evidence_gates"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        "# ml-shadow-scorer-v1 Production Readiness Authorization Criteria v1",
        "",
        "## Executive Summary",
        "",
        "This artifact opens the production-readiness authorization chain by defining criteria only. It grants no production readiness, production default, API/web, user-visible ranking, database, runtime, refit, embedding, or label-ingest authorization.",
        "",
        f"- Criteria version: `{metadata['criteria_version']}`",
        f"- Production readiness criteria defined: {payload['production_readiness_criteria_defined']}",
        f"- Production readiness authorization requested: {payload['production_readiness_authorization_requested']}",
        f"- Production readiness authorization granted: {payload['production_readiness_authorization_granted']}",
        f"- Missing production readiness authorization: {payload['missing_production_readiness_authorization']}",
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
            "## Upstream Accepted Phase 2 Evidence",
            "",
            f"- Phase bundle: `{source['phase_bundle']['path']}`",
            f"- Phase bundle SHA-256: `{source['phase_bundle']['sha256']}`",
            f"- Phase bundle revision: {source['phase_bundle_revision']}",
            f"- Phase 2 write pilot reviewed: {source['phase2_write_pilot_reviewed']}",
            f"- Phase 2 write pilot accepted: {source['phase2_write_pilot_accepted']}",
            f"- Phase 2 write pilot review decision: `{source['phase2_write_pilot_review_decision']}`",
            f"- Pilot run id: `{source['phase2_write_pilot_run_id']}`",
            f"- Upstream recommended next stage: `{source['upstream_recommended_next_stage']}`",
            "",
            "## Superseded Plan Reconciliation",
            "",
            f"- Superseded plan path: `{reconciliation['superseded_plan_path']}`",
            f"- Superseded plan SHA-256: `{reconciliation['superseded_plan_sha256']}`",
            f"- Superseded by: `{reconciliation['superseded_by']}`",
        ]
    )
    for item in reconciliation["reconciliation_summary"]:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Required Production-Readiness Evidence Gates",
            "",
        ]
    )
    for gate, required in gates.items():
        lines.append(f"- {gate}: {required}")
    lines.extend(
        [
            "",
            "## Explicit Non-Authorizations",
            "",
        ]
    )
    for statement in payload["explicit_non_authorizations"]:
        lines.append(f"- {statement}")
    lines.extend(
        [
            "",
            "## Remaining Blockers",
            "",
            f"- missing_production_readiness_authorization: {blockers['missing_production_readiness_authorization']}",
            f"- production_readiness_authorization_requested: {blockers['production_readiness_authorization_requested']}",
            f"- production_readiness_authorization_granted: {blockers['production_readiness_authorization_granted']}",
            f"- production_default_allowed: {blockers['production_default_allowed']}",
            f"- api_web_changes_allowed: {blockers['api_web_changes_allowed']}",
            f"- user_visible_ranking_changed: {blockers['user_visible_ranking_changed']}",
            f"- online_shadow_execution_enabled: {blockers['online_shadow_execution_enabled']}",
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


def write_ml_shadow_scorer_production_readiness_authorization_criteria(
    *,
    phase_bundle_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    superseded_production_readiness_plan_path: Path | None = None,
    criteria_version: str = CRITERIA_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = build_ml_shadow_scorer_production_readiness_authorization_criteria_payload(
        phase_bundle_path=phase_bundle_path,
        superseded_production_readiness_plan_path=superseded_production_readiness_plan_path,
        criteria_version=criteria_version,
        repo_root=repo_root,
    )
    output_path = Path(output_path)
    markdown_output_path = Path(markdown_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_production_readiness_authorization_criteria(payload),
        encoding="utf-8",
    )
    return payload

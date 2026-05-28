"""Assemble and verify canonical online shadow phase bundles.

The Phase 2 bundle is a forward-facing view over frozen legacy audit
artifacts. It validates the historical ladder by reference and SHA, but does
not run shadow scoring, enable feature flags, create shadow-run files, touch
databases, or change production/API/default ranking behavior.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.repo_paths import default_repo_root, portable_repo_path

ARTIFACT_TYPE = "ml_shadow_scorer_phase_bundle"
BUNDLE_VERSION = "online-shadow-phase2-v1"
BUNDLE_REVISION = 1
RECOMMENDED_NEXT_STAGE = "run_online_shadow_phase2_isolated_audit_write_pilot_v1"

PINNED_IDENTITY: dict[str, str] = {
    "ranking_run_id": "rank-83787b91ef",
    "family": "emerging",
    "corpus_snapshot_version": "source-snapshot-shadow-generalization-v1-20260521",
    "embedding_version": "shadow-generalization-text-embedding-v1",
    "candidate_pool_work_set_sha256": "f0f00911608dae99f71bd0394640bd9554315eee0c98c68c4bba836ae4320fcc",
    "formula_id": "hybrid_rank_mean_50_50",
    "scorer_id": "ml-shadow-scorer-v1",
}

PLAN_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_plan"
PROOF_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_mode_proof"
REQUEST_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_request"
GRANT_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase2_isolated_audit_write_authorization_grant"
PHASE1_REVIEW_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_phase1_no_write_pilot_review"
EXECUTION_GRANT_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_execution_authorization_grant"
ONLINE_SHADOW_POLICY_ARTIFACT_TYPE = "ml_shadow_scorer_online_shadow_policy"

PLAN_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-plan-v1"
PROOF_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-mode-proof-v1"
REQUEST_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-request-v1"
GRANT_VERSION = "ml-shadow-scorer-v1-online-shadow-phase2-isolated-audit-write-authorization-grant-v1"
PHASE1_REVIEW_VERSION = "ml-shadow-scorer-v1-online-shadow-phase1-no-write-pilot-review-v1"
EXECUTION_GRANT_VERSION = "ml-shadow-scorer-v1-online-shadow-execution-authorization-grant-v1"
ONLINE_SHADOW_POLICY_VERSION = "ml-shadow-scorer-v1-online-shadow-policy"

PHASE2_WRITE_TARGET_ROOT = "docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/"
PHASE2_WRITE_PILOT_SCOPE = "bounded_non_prod_phase2_isolated_audit_write_pilot_only"
PHASE2_WRITES_SCOPE = "isolated_audit_shadow_artifacts_only"

LEGACY_ARTIFACT_ROLES = (
    "phase2_write_mode_plan",
    "phase2_write_mode_proof",
    "phase2_write_authorization_request",
    "phase2_write_authorization_grant",
    "phase1_no_write_pilot_review",
    "prior_execution_authorization_grant",
    "online_shadow_policy",
)

CAVEATS = (
    "Bundle only; does not run the Phase 2 write pilot.",
    "Bundle does not enable online shadow execution.",
    "Bundle does not authorize production default/API/user-visible ranking behavior.",
    "Bundle does not authorize production readiness.",
    "Legacy artifacts remain frozen evidence and are referenced by path + SHA.",
    "Future pilot updates should modify the bundle execution section, not create new request/grant/proof artifact families.",
)


class MLShadowScorerPhaseBundleError(Exception):
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
        raise MLShadowScorerPhaseBundleError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerPhaseBundleError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, label: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerPhaseBundleError(f"{label} missing metadata object")
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
        raise MLShadowScorerPhaseBundleError(f"{name} must be {expected!r}, got {observed!r}")


def _require_true(name: str, observed: Any) -> None:
    _require_equal(name, observed, True)


def _require_false(name: str, observed: Any) -> None:
    _require_equal(name, observed, False)


def _artifact_record(role: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerPhaseBundleError(f"{role} artifact does not exist: {path}")
    return {
        "role": role,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": _sha256_file(resolved),
    }


def _ref_from_record(record: Mapping[str, Any]) -> dict[str, str]:
    return {"path": str(record["path"]), "sha256": str(record["sha256"])}


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerPhaseBundleError("referenced path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_reference(ref: Any, *, repo_root: Path, label: str) -> Path:
    if not isinstance(ref, Mapping):
        raise MLShadowScorerPhaseBundleError(f"{label} reference must be an object")
    recorded_sha = ref.get("sha256")
    if not isinstance(recorded_sha, str) or not recorded_sha.strip():
        raise MLShadowScorerPhaseBundleError(f"{label}.sha256 missing")
    resolved = _resolve_recorded_path(ref.get("path"), repo_root=repo_root)
    if not resolved.exists():
        raise MLShadowScorerPhaseBundleError(f"{label} path missing on disk: {ref.get('path')}")
    if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
        raise MLShadowScorerPhaseBundleError(
            f"{label} sha256 mismatch: recorded {recorded_sha}, actual {_sha256_file(resolved)}"
        )
    return resolved


def _validate_identity_mapping(identity: Any, *, label: str) -> None:
    if not isinstance(identity, Mapping):
        raise MLShadowScorerPhaseBundleError(f"{label} must be an object")
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(f"{label}.{field}", identity.get(field), expected)


def _validate_metadata_identity(payload: Mapping[str, Any], *, label: str, grant_scope_fallback: bool = False) -> None:
    metadata = _metadata(payload, label=label)
    for field, expected in PINNED_IDENTITY.items():
        observed = metadata.get(field)
        if observed is None and grant_scope_fallback:
            observed = _get(payload, f"grant_scope.{field}")
        if observed is not None:
            _require_equal(f"{label} identity {field}", observed, expected)


def _validate_plan(plan: Mapping[str, Any]) -> None:
    metadata = _metadata(plan, label="phase2 write-mode plan")
    _require_equal("plan metadata.artifact_type", metadata.get("artifact_type"), PLAN_ARTIFACT_TYPE)
    _require_equal("plan metadata.plan_version", metadata.get("plan_version"), PLAN_VERSION)
    _validate_metadata_identity(plan, label="plan")
    _require_true("plan phase2_isolated_audit_write_mode_plan_defined", plan.get("phase2_isolated_audit_write_mode_plan_defined"))
    _require_equal("plan isolated_write_target.root_path", _get(plan, "isolated_write_target.root_path"), PHASE2_WRITE_TARGET_ROOT)


def _validate_request(request: Mapping[str, Any]) -> None:
    metadata = _metadata(request, label="phase2 write authorization request")
    _require_equal("request metadata.artifact_type", metadata.get("artifact_type"), REQUEST_ARTIFACT_TYPE)
    _require_equal("request metadata.request_version", metadata.get("request_version"), REQUEST_VERSION)
    _validate_metadata_identity(request, label="request")
    required = {
        "phase2_isolated_audit_write_authorization_requested": True,
        "phase2_isolated_audit_write_authorization_granted": False,
        "phase2_write_pilot_authorized": False,
        "phase2_writes_authorized": False,
        "phase2_write_mode_proof_passed": True,
        "missing_phase2_write_mode_isolation_proof": False,
        "missing_phase2_isolated_audit_write_pilot_authorization": True,
        "recommended_next_stage": "record_online_shadow_phase2_isolated_audit_write_authorization_grant_v1",
    }
    for path, expected in required.items():
        _require_equal(f"request {path}", _get(request, path), expected)


def _validate_grant(grant: Mapping[str, Any]) -> None:
    metadata = _metadata(grant, label="phase2 write authorization grant")
    _require_equal("grant metadata.artifact_type", metadata.get("artifact_type"), GRANT_ARTIFACT_TYPE)
    _require_equal("grant metadata.grant_version", metadata.get("grant_version"), GRANT_VERSION)
    _validate_metadata_identity(grant, label="grant")
    required = {
        "phase2_isolated_audit_write_authorization_granted": True,
        "phase2_write_pilot_authorized": True,
        "phase2_writes_authorized": True,
        "phase2_write_pilot_authorization_scope": PHASE2_WRITE_PILOT_SCOPE,
        "phase2_writes_authorization_scope": PHASE2_WRITES_SCOPE,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "recommended_next_stage": RECOMMENDED_NEXT_STAGE,
    }
    for path, expected in required.items():
        _require_equal(f"grant {path}", _get(grant, path), expected)


def _validate_proof(proof: Mapping[str, Any]) -> None:
    metadata = _metadata(proof, label="phase2 write-mode proof")
    _require_equal("proof metadata.artifact_type", metadata.get("artifact_type"), PROOF_ARTIFACT_TYPE)
    _require_equal("proof metadata.proof_version", metadata.get("proof_version"), PROOF_VERSION)
    _validate_metadata_identity(proof, label="proof")
    required = {
        "phase2_write_mode_proof_executed": True,
        "phase2_write_mode_proof_passed": True,
        "missing_phase2_write_mode_isolation_proof": False,
    }
    for path, expected in required.items():
        _require_equal(f"proof {path}", _get(proof, path), expected)


def _validate_phase1_review(review: Mapping[str, Any]) -> None:
    metadata = _metadata(review, label="phase1 no-write pilot review")
    _require_equal("phase1 review metadata.artifact_type", metadata.get("artifact_type"), PHASE1_REVIEW_ARTIFACT_TYPE)
    _require_equal("phase1 review metadata.review_version", metadata.get("review_version"), PHASE1_REVIEW_VERSION)
    _validate_metadata_identity(review, label="phase1 review")
    _require_true("phase1 review phase1_no_write_pilot_result_accepted", review.get("phase1_no_write_pilot_result_accepted"))
    _require_equal("phase1 review review_decision.decision", _get(review, "review_decision.decision"), "accepted")


def _validate_prior_execution_grant(grant: Mapping[str, Any]) -> None:
    metadata = _metadata(grant, label="prior execution authorization grant")
    _require_equal("prior execution grant metadata.artifact_type", metadata.get("artifact_type"), EXECUTION_GRANT_ARTIFACT_TYPE)
    _require_equal("prior execution grant metadata.grant_version", metadata.get("grant_version"), EXECUTION_GRANT_VERSION)
    _validate_metadata_identity(grant, label="prior execution grant", grant_scope_fallback=True)
    _require_true("prior execution grant runtime_execution_authorized", grant.get("runtime_execution_authorized"))
    _require_true("prior execution grant shadow_scoring_allowed", grant.get("shadow_scoring_allowed"))


def _validate_online_shadow_policy(policy: Mapping[str, Any]) -> None:
    metadata = _metadata(policy, label="online shadow policy")
    _require_equal("online shadow policy metadata.artifact_type", metadata.get("artifact_type"), ONLINE_SHADOW_POLICY_ARTIFACT_TYPE)
    _require_equal("online shadow policy metadata.policy_version", metadata.get("policy_version"), ONLINE_SHADOW_POLICY_VERSION)


def _copy_grant_blockers(grant: Mapping[str, Any]) -> dict[str, Any]:
    blockers = grant.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerPhaseBundleError("grant shadow_and_production_blockers must be an object")
    copied = {
        key: deepcopy(value)
        for key, value in blockers.items()
        if key not in {"blockers_changed_by_request", "blockers_unchanged_by_request"}
    }
    copied.update(
        {
            "missing_online_shadow_execution_authorization": False,
            "missing_phase2_isolated_audit_write_pilot_authorization": False,
            "missing_phase2_write_mode_isolation_proof": False,
            "phase2_write_pilot_authorized": True,
            "phase2_writes_authorized": True,
            "online_shadow_execution_enabled": False,
            "missing_production_readiness_authorization": True,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    return copied


def _grant_decision_rollup(grant: Mapping[str, Any]) -> dict[str, Any]:
    decision = grant.get("grant_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerPhaseBundleError("grant grant_decision must be an object")
    return {
        "decision": decision.get("decision"),
        "owner": decision.get("owner"),
        "review_by": decision.get("review_by"),
        "expiry_date": decision.get("expiry_date"),
    }


def assemble_ml_shadow_scorer_phase_bundle_payload(
    *,
    phase2_write_mode_plan_path: Path,
    phase2_write_mode_proof_path: Path,
    phase2_write_authorization_request_path: Path,
    phase2_write_authorization_grant_path: Path,
    phase1_no_write_pilot_review_path: Path,
    prior_execution_authorization_grant_path: Path,
    online_shadow_policy_path: Path,
    bundle_version: str = BUNDLE_VERSION,
    bundle_revision: int = BUNDLE_REVISION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    paths = {
        "phase2_write_mode_plan": Path(phase2_write_mode_plan_path).resolve(),
        "phase2_write_mode_proof": Path(phase2_write_mode_proof_path).resolve(),
        "phase2_write_authorization_request": Path(phase2_write_authorization_request_path).resolve(),
        "phase2_write_authorization_grant": Path(phase2_write_authorization_grant_path).resolve(),
        "phase1_no_write_pilot_review": Path(phase1_no_write_pilot_review_path).resolve(),
        "prior_execution_authorization_grant": Path(prior_execution_authorization_grant_path).resolve(),
        "online_shadow_policy": Path(online_shadow_policy_path).resolve(),
    }
    artifacts = {role: _load_json_object(path) for role, path in paths.items()}
    _validate_plan(artifacts["phase2_write_mode_plan"])
    _validate_proof(artifacts["phase2_write_mode_proof"])
    _validate_request(artifacts["phase2_write_authorization_request"])
    _validate_grant(artifacts["phase2_write_authorization_grant"])
    _validate_phase1_review(artifacts["phase1_no_write_pilot_review"])
    _validate_prior_execution_grant(artifacts["prior_execution_authorization_grant"])
    _validate_online_shadow_policy(artifacts["online_shadow_policy"])

    records = {role: _artifact_record(role, paths[role], repo_root=root) for role in LEGACY_ARTIFACT_ROLES}
    grant = artifacts["phase2_write_authorization_grant"]
    proof = artifacts["phase2_write_mode_proof"]
    review = artifacts["phase1_no_write_pilot_review"]

    payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "bundle_version": bundle_version,
            "bundle_revision": bundle_revision,
            "generated_at": generated_at or _now_iso_z(),
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "legacy_artifacts_index": [deepcopy(records[role]) for role in LEGACY_ARTIFACT_ROLES],
        },
        "policy_ref": {
            "phase2_write_mode_plan": _ref_from_record(records["phase2_write_mode_plan"]),
            "online_shadow_policy": _ref_from_record(records["online_shadow_policy"]),
        },
        "authorization": {
            "phase2_write_authorization_request": _ref_from_record(records["phase2_write_authorization_request"]),
            "phase2_write_authorization_grant": _ref_from_record(records["phase2_write_authorization_grant"]),
            "grant_decision": _grant_decision_rollup(grant),
            "phase2_isolated_audit_write_authorization_granted": grant["phase2_isolated_audit_write_authorization_granted"],
            "phase2_write_pilot_authorized": grant["phase2_write_pilot_authorized"],
            "phase2_writes_authorized": grant["phase2_writes_authorized"],
            "phase2_write_pilot_authorization_scope": grant["phase2_write_pilot_authorization_scope"],
            "phase2_writes_authorization_scope": grant["phase2_writes_authorization_scope"],
        },
        "evidence": {
            "phase1_no_write_pilot_review": {
                **_ref_from_record(records["phase1_no_write_pilot_review"]),
                "phase1_no_write_pilot_result_accepted": review["phase1_no_write_pilot_result_accepted"],
            },
            "phase2_write_mode_proof": {
                **_ref_from_record(records["phase2_write_mode_proof"]),
                "phase2_write_mode_proof_passed": proof["phase2_write_mode_proof_passed"],
            },
            "proof_summary_ref": "phase2_write_authorization_grant.proof_summary",
        },
        "execution": {
            "phase2_write_pilot_executed": False,
            "phase2_write_pilot_run": None,
        },
        "review": {
            "phase2_write_pilot_reviewed": False,
            "phase2_write_pilot_accepted": None,
        },
        "posture": {
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "online_shadow_execution_enabled": False,
            "runtime_execution_authorized": True,
            "shadow_scoring_allowed": True,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "missing_online_shadow_execution_authorization": False,
            "missing_phase2_isolated_audit_write_pilot_authorization": False,
            "missing_phase2_write_mode_isolation_proof": False,
            "missing_production_readiness_authorization": True,
        },
        "shadow_and_production_blockers": _copy_grant_blockers(grant),
        "caveats": list(CAVEATS),
        "recommended_next_stage": grant["recommended_next_stage"],
    }
    verify_ml_shadow_scorer_phase_bundle_payload(payload, repo_root=root)
    return payload


def _legacy_index_by_role(bundle: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    metadata = _metadata(bundle, label="phase bundle")
    index = metadata.get("legacy_artifacts_index")
    if not isinstance(index, list):
        raise MLShadowScorerPhaseBundleError("metadata.legacy_artifacts_index must be a list")
    by_role: dict[str, Mapping[str, Any]] = {}
    for position, record in enumerate(index):
        if not isinstance(record, Mapping):
            raise MLShadowScorerPhaseBundleError(f"metadata.legacy_artifacts_index[{position}] must be an object")
        role = record.get("role")
        if not isinstance(role, str) or not role:
            raise MLShadowScorerPhaseBundleError(f"metadata.legacy_artifacts_index[{position}].role missing")
        by_role[role] = record
    missing = [role for role in LEGACY_ARTIFACT_ROLES if role not in by_role]
    if missing:
        raise MLShadowScorerPhaseBundleError("metadata.legacy_artifacts_index missing roles: " + ", ".join(missing))
    return by_role


def _resolve_bundle_pointer(pointer: str, *, grant: Mapping[str, Any]) -> Any:
    if pointer != "phase2_write_authorization_grant.proof_summary":
        return None
    return grant.get("proof_summary")


def verify_ml_shadow_scorer_phase_bundle_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    metadata = _metadata(bundle, label="phase bundle")
    _require_equal("bundle metadata.artifact_type", metadata.get("artifact_type"), ARTIFACT_TYPE)
    _require_equal("bundle metadata.bundle_version", metadata.get("bundle_version"), BUNDLE_VERSION)
    revision = metadata.get("bundle_revision")
    if not isinstance(revision, int) or revision <= 0:
        raise MLShadowScorerPhaseBundleError("metadata.bundle_revision must be a positive integer")
    _validate_identity_mapping(metadata.get("pinned_identity"), label="metadata.pinned_identity")
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerPhaseBundleError("posture must be an object")
    _validate_identity_mapping(posture.get("pinned_identity"), label="posture.pinned_identity")

    legacy_index = _legacy_index_by_role(bundle)
    resolved_paths = {
        role: _verify_reference(legacy_index[role], repo_root=root, label=f"metadata.legacy_artifacts_index.{role}")
        for role in LEGACY_ARTIFACT_ROLES
    }
    policy_ref = bundle.get("policy_ref")
    authorization = bundle.get("authorization")
    evidence = bundle.get("evidence")
    if not isinstance(policy_ref, Mapping):
        raise MLShadowScorerPhaseBundleError("policy_ref must be an object")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerPhaseBundleError("authorization must be an object")
    if not isinstance(evidence, Mapping):
        raise MLShadowScorerPhaseBundleError("evidence must be an object")

    reference_paths = {
        "policy_ref.phase2_write_mode_plan": _verify_reference(
            policy_ref.get("phase2_write_mode_plan"),
            repo_root=root,
            label="policy_ref.phase2_write_mode_plan",
        ),
        "policy_ref.online_shadow_policy": _verify_reference(
            policy_ref.get("online_shadow_policy"),
            repo_root=root,
            label="policy_ref.online_shadow_policy",
        ),
        "authorization.phase2_write_authorization_request": _verify_reference(
            authorization.get("phase2_write_authorization_request"),
            repo_root=root,
            label="authorization.phase2_write_authorization_request",
        ),
        "authorization.phase2_write_authorization_grant": _verify_reference(
            authorization.get("phase2_write_authorization_grant"),
            repo_root=root,
            label="authorization.phase2_write_authorization_grant",
        ),
        "evidence.phase1_no_write_pilot_review": _verify_reference(
            evidence.get("phase1_no_write_pilot_review"),
            repo_root=root,
            label="evidence.phase1_no_write_pilot_review",
        ),
        "evidence.phase2_write_mode_proof": _verify_reference(
            evidence.get("phase2_write_mode_proof"),
            repo_root=root,
            label="evidence.phase2_write_mode_proof",
        ),
    }
    expected_reference_roles = {
        "policy_ref.phase2_write_mode_plan": "phase2_write_mode_plan",
        "policy_ref.online_shadow_policy": "online_shadow_policy",
        "authorization.phase2_write_authorization_request": "phase2_write_authorization_request",
        "authorization.phase2_write_authorization_grant": "phase2_write_authorization_grant",
        "evidence.phase1_no_write_pilot_review": "phase1_no_write_pilot_review",
        "evidence.phase2_write_mode_proof": "phase2_write_mode_proof",
    }
    for label, role in expected_reference_roles.items():
        if reference_paths[label] != resolved_paths[role]:
            raise MLShadowScorerPhaseBundleError(f"{label} path must match metadata.legacy_artifacts_index.{role}")

    plan = _load_json_object(resolved_paths["phase2_write_mode_plan"])
    proof = _load_json_object(resolved_paths["phase2_write_mode_proof"])
    request = _load_json_object(resolved_paths["phase2_write_authorization_request"])
    grant = _load_json_object(resolved_paths["phase2_write_authorization_grant"])
    review = _load_json_object(resolved_paths["phase1_no_write_pilot_review"])
    prior_grant = _load_json_object(resolved_paths["prior_execution_authorization_grant"])
    policy = _load_json_object(resolved_paths["online_shadow_policy"])
    _validate_plan(plan)
    _validate_request(request)
    _validate_grant(grant)
    _validate_proof(proof)
    _validate_phase1_review(review)
    _validate_prior_execution_grant(prior_grant)
    _validate_online_shadow_policy(policy)

    grant_metadata = _metadata(grant, label="phase2 write authorization grant")
    for field, expected in PINNED_IDENTITY.items():
        if grant_metadata.get(field) is not None:
            _require_equal(f"grant metadata.{field}", grant_metadata.get(field), expected)

    for field in (
        "phase2_isolated_audit_write_authorization_granted",
        "phase2_write_pilot_authorized",
        "phase2_writes_authorized",
        "phase2_write_pilot_authorization_scope",
        "phase2_writes_authorization_scope",
    ):
        _require_equal(f"bundle authorization.{field}", authorization.get(field), grant.get(field))
    _require_equal("bundle authorization.grant_decision", authorization.get("grant_decision"), _grant_decision_rollup(grant))
    _require_true(
        "bundle evidence.phase1_no_write_pilot_review.phase1_no_write_pilot_result_accepted",
        _get(evidence, "phase1_no_write_pilot_review.phase1_no_write_pilot_result_accepted"),
    )
    _require_true(
        "bundle evidence.phase2_write_mode_proof.phase2_write_mode_proof_passed",
        _get(evidence, "phase2_write_mode_proof.phase2_write_mode_proof_passed"),
    )
    pointer = evidence.get("proof_summary_ref")
    _require_equal("bundle evidence.proof_summary_ref", pointer, "phase2_write_authorization_grant.proof_summary")
    resolved_pointer = _resolve_bundle_pointer(str(pointer), grant=grant)
    if not isinstance(resolved_pointer, Mapping):
        raise MLShadowScorerPhaseBundleError("bundle evidence.proof_summary_ref must resolve to a grant object")

    execution = bundle.get("execution")
    review_section = bundle.get("review")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerPhaseBundleError("execution must be an object")
    if not isinstance(review_section, Mapping):
        raise MLShadowScorerPhaseBundleError("review must be an object")
    _require_false("bundle execution.phase2_write_pilot_executed", execution.get("phase2_write_pilot_executed"))
    _require_equal("bundle execution.phase2_write_pilot_run", execution.get("phase2_write_pilot_run"), None)
    _require_false("bundle review.phase2_write_pilot_reviewed", review_section.get("phase2_write_pilot_reviewed"))
    _require_equal("bundle review.phase2_write_pilot_accepted", review_section.get("phase2_write_pilot_accepted"), None)
    posture_required = {
        "online_shadow_execution_enabled": False,
        "runtime_execution_authorized": True,
        "shadow_scoring_allowed": True,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "missing_online_shadow_execution_authorization": False,
        "missing_phase2_isolated_audit_write_pilot_authorization": False,
        "missing_phase2_write_mode_isolation_proof": False,
        "missing_production_readiness_authorization": True,
    }
    for field, expected in posture_required.items():
        _require_equal(f"bundle posture.{field}", posture.get(field), expected)
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerPhaseBundleError("shadow_and_production_blockers must be an object")
    if "blockers_changed_by_request" in blockers or "blockers_unchanged_by_request" in blockers:
        raise MLShadowScorerPhaseBundleError("shadow_and_production_blockers must not include request-era stale blocker fields")
    _require_false(
        "bundle shadow_and_production_blockers.missing_phase2_isolated_audit_write_pilot_authorization",
        blockers.get("missing_phase2_isolated_audit_write_pilot_authorization"),
    )
    _require_true("bundle shadow_and_production_blockers.phase2_write_pilot_authorized", blockers.get("phase2_write_pilot_authorized"))
    _require_true("bundle shadow_and_production_blockers.phase2_writes_authorized", blockers.get("phase2_writes_authorized"))
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list) or len(caveats) < len(CAVEATS):
        raise MLShadowScorerPhaseBundleError("bundle caveats must be present")
    _require_equal("bundle recommended_next_stage", bundle.get("recommended_next_stage"), grant.get("recommended_next_stage"))
    _require_equal("bundle recommended_next_stage", bundle.get("recommended_next_stage"), RECOMMENDED_NEXT_STAGE)
    return {
        "verification_status": "passed",
        "bundle_version": metadata.get("bundle_version"),
        "bundle_revision": revision,
        "recommended_next_stage": bundle.get("recommended_next_stage"),
        "legacy_artifact_count": len(LEGACY_ARTIFACT_ROLES),
    }


def verify_ml_shadow_scorer_phase_bundle(
    *,
    bundle_path: Path,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = _load_json_object(Path(bundle_path).resolve())
    return verify_ml_shadow_scorer_phase_bundle_payload(payload, repo_root=repo_root)


def markdown_from_ml_shadow_scorer_phase_bundle(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    identity = metadata["pinned_identity"]
    authorization = payload["authorization"]
    evidence = payload["evidence"]
    execution = payload["execution"]
    review = payload["review"]
    posture = payload["posture"]
    blockers = payload["shadow_and_production_blockers"]
    lines = [
        f"# ml-shadow-scorer-v1 Online Shadow Phase Bundle ({metadata['bundle_version']})",
        "",
        "## Executive Summary",
        "",
        "This bundle is the canonical forward-facing Phase 2 write-path status view. It references frozen legacy artifacts by path and SHA; it does not run a pilot, enable runtime execution, or change production behavior.",
        "",
        f"- Bundle revision: {metadata['bundle_revision']}",
        f"- Phase 2 write pilot authorized: {authorization['phase2_write_pilot_authorized']}",
        f"- Phase 2 writes authorized: {authorization['phase2_writes_authorized']}",
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
            "## Authorization Rollup",
            "",
            f"- Decision: `{authorization['grant_decision']['decision']}`",
            f"- Owner: {authorization['grant_decision']['owner']}",
            f"- Review by: {authorization['grant_decision']['review_by']}",
            f"- Expiry date: {authorization['grant_decision']['expiry_date']}",
            f"- Write pilot scope: `{authorization['phase2_write_pilot_authorization_scope']}`",
            f"- Write authorization scope: `{authorization['phase2_writes_authorization_scope']}`",
            "",
            "## Evidence Rollup",
            "",
            f"- Phase 1 no-write pilot accepted: {evidence['phase1_no_write_pilot_review']['phase1_no_write_pilot_result_accepted']}",
            f"- Phase 2 write-mode proof passed: {evidence['phase2_write_mode_proof']['phase2_write_mode_proof_passed']}",
            f"- Proof summary reference: `{evidence['proof_summary_ref']}`",
            "",
            "## Execution And Review",
            "",
            f"- Phase 2 write pilot executed: {execution['phase2_write_pilot_executed']}",
            f"- Phase 2 write pilot reviewed: {review['phase2_write_pilot_reviewed']}",
            "",
            "## Production/API/Default Separation",
            "",
            f"- Production default allowed: {posture['production_default_allowed']}",
            f"- API/web changes allowed: {posture['api_web_changes_allowed']}",
            f"- User-visible ranking changed: {posture['user_visible_ranking_changed']}",
            f"- Production readiness authorization missing: {posture['missing_production_readiness_authorization']}",
            f"- Phase 2 write pilot authorization missing: {blockers['missing_phase2_isolated_audit_write_pilot_authorization']}",
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


def write_ml_shadow_scorer_phase_bundle(
    *,
    phase2_write_mode_plan_path: Path,
    phase2_write_mode_proof_path: Path,
    phase2_write_authorization_request_path: Path,
    phase2_write_authorization_grant_path: Path,
    phase1_no_write_pilot_review_path: Path,
    prior_execution_authorization_grant_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    bundle_version: str = BUNDLE_VERSION,
    bundle_revision: int = BUNDLE_REVISION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = assemble_ml_shadow_scorer_phase_bundle_payload(
        phase2_write_mode_plan_path=phase2_write_mode_plan_path,
        phase2_write_mode_proof_path=phase2_write_mode_proof_path,
        phase2_write_authorization_request_path=phase2_write_authorization_request_path,
        phase2_write_authorization_grant_path=phase2_write_authorization_grant_path,
        phase1_no_write_pilot_review_path=phase1_no_write_pilot_review_path,
        prior_execution_authorization_grant_path=prior_execution_authorization_grant_path,
        online_shadow_policy_path=online_shadow_policy_path,
        bundle_version=bundle_version,
        bundle_revision=bundle_revision,
        repo_root=repo_root,
    )
    output_path = Path(output_path)
    markdown_output_path = Path(markdown_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.write_text(markdown_from_ml_shadow_scorer_phase_bundle(payload), encoding="utf-8")
    return payload

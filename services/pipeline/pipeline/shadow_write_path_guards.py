"""Path and write-count guards for isolated online shadow write pilots."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Mapping

PHASE2_PROOF_ROOT = "docs/audit/shadow-runs/ml-shadow-scorer-v1/phase2-proof/"
ISOLATED_AUDIT_SHADOW_ARTIFACTS = "isolated_audit_shadow_artifacts"
ISOLATED_AUDIT_SHADOW_TABLES = "isolated_audit_shadow_tables"
PILOT_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class ShadowWritePathGuardError(ValueError):
    """Raised when a pilot write path or write-count map is unsafe."""


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def validate_pilot_run_id(pilot_run_id: str) -> str:
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise ShadowWritePathGuardError("pilot_run_id must be non-empty")
    if not PILOT_RUN_ID_RE.fullmatch(pilot_run_id):
        raise ShadowWritePathGuardError(
            "pilot_run_id may contain only letters, digits, dot, underscore, or hyphen"
        )
    if ".." in pilot_run_id or "/" in pilot_run_id or "\\" in pilot_run_id or Path(pilot_run_id).is_absolute():
        raise ShadowWritePathGuardError(
            "pilot_run_id must not contain path traversal, separators, or absolute paths"
        )
    return pilot_run_id


def phase2_proof_root(repo_root: Path) -> Path:
    root = Path(repo_root).resolve()
    proof_root = (root / PHASE2_PROOF_ROOT).resolve()
    if not _is_relative_to(proof_root, root):
        raise ShadowWritePathGuardError("phase2-proof root must remain under repo_root")
    return proof_root


def resolve_pilot_directory(repo_root: Path, pilot_run_id: str) -> Path:
    run_id = validate_pilot_run_id(pilot_run_id)
    proof_root = phase2_proof_root(repo_root)
    pilot_dir = (proof_root / run_id).resolve()
    if pilot_dir == proof_root or pilot_dir.parent != proof_root:
        raise ShadowWritePathGuardError(
            "pilot output directory must be a direct child of the phase2-proof root"
        )
    if not _is_relative_to(pilot_dir, proof_root):
        raise ShadowWritePathGuardError(
            "pilot output directory must remain under the repository phase2-proof root"
        )
    return pilot_dir


def assert_write_path_allowed(resolved_path: Path, repo_root: Path) -> Path:
    candidate = Path(resolved_path).resolve()
    proof_root = phase2_proof_root(repo_root)
    if candidate == proof_root or not _is_relative_to(candidate, proof_root):
        raise ShadowWritePathGuardError("write path must be under the phase2-proof root")
    relative = candidate.relative_to(proof_root)
    if not relative.parts:
        raise ShadowWritePathGuardError("write path must be a strict child of the phase2-proof root")
    return candidate


def assert_forbidden_write_target_counts(write_counts: Mapping[str, int]) -> None:
    forbidden_nonzero = {
        str(target): count
        for target, count in write_counts.items()
        if target != ISOLATED_AUDIT_SHADOW_ARTIFACTS and count != 0
    }
    if forbidden_nonzero:
        raise ShadowWritePathGuardError(
            f"forbidden write targets must remain zero: {forbidden_nonzero}"
        )

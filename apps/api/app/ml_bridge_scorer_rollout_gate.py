"""Runtime guardrails for the bounded Bridge ML scorer rollout path."""

from __future__ import annotations

from dataclasses import dataclass
import os
import threading
from typing import Mapping

BRIDGE_ROLLOUT_ROUTE = "/api/v1/recommendations/ranked"
BRIDGE_ROLLOUT_FAMILY = "bridge"
BRIDGE_ROLLOUT_LIMIT = 20
PINNED_BRIDGE_RANKING_RUN_ID = "rank-5a7efa5ca3"

_FEATURE_FLAG = "ML_BRIDGE_SCORER_V1_RUNTIME_ENABLED"
_COHORT_ALLOWLIST = "ML_BRIDGE_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST"
_EXPOSURE_CAP = "ML_BRIDGE_SCORER_V1_ROLLOUT_EXPOSURE_CAP"
_PUBLIC_ROLLOUT_ENABLED = "ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_ENABLED"
_PUBLIC_ROLLOUT_PERCENT = "ML_BRIDGE_SCORER_V1_PUBLIC_ROLLOUT_PERCENT"
_RANKING_RUN_ID = "ML_BRIDGE_SCORER_V1_RANKING_RUN_ID"
_FLAG_ON_VALUES = frozenset({"1", "true", "on", "yes", "enabled"})

_bridge_served_count = 0
_bridge_served_count_lock = threading.Lock()


@dataclass(frozen=True)
class BridgeScorerGateDecision:
    should_attempt: bool
    reason: str | None = None
    emitted_to_public_users: bool = False


@dataclass(frozen=True)
class BridgeScorerRolloutGate:
    runtime_enabled: bool
    cohort_allowlist: frozenset[str]
    exposure_cap: int
    public_rollout_enabled: bool
    public_rollout_percent: int
    configured_ranking_run_id: str | None

    def is_flag_enabled(self) -> bool:
        return self.runtime_enabled

    def is_route_allowed(self, route: str) -> bool:
        return route == BRIDGE_ROLLOUT_ROUTE

    def is_family_allowed(self, family: str) -> bool:
        return family == BRIDGE_ROLLOUT_FAMILY

    def is_limit_allowed(self, limit: int) -> bool:
        return limit == BRIDGE_ROLLOUT_LIMIT

    def is_configured_for_pinned_run(self) -> bool:
        return self.configured_ranking_run_id == PINNED_BRIDGE_RANKING_RUN_ID

    def is_request_ranking_run_allowed(self, ranking_run_id: str | None) -> bool:
        requested = (ranking_run_id or "").strip()
        return not requested or requested == PINNED_BRIDGE_RANKING_RUN_ID

    def is_rollout_subject_eligible(
        self, subject: str | None
    ) -> tuple[bool, str | None, bool]:
        normalized = (subject or "").strip()
        if normalized:
            if normalized in self.cohort_allowlist:
                return True, None, False
            return False, "cohort_ineligible", False
        if not self.public_rollout_enabled:
            return False, "public_rollout_disabled", False
        if self.public_rollout_percent != 100:
            return False, "public_rollout_percent_closed", False
        return True, None, True

    def is_within_exposure_cap(self, current_served: int, cap: int) -> bool:
        return cap > 0 and current_served < cap

    def is_pinned_run_context(
        self,
        *,
        ranking_run_id: str,
        ranking_version: str | None,
        requested_ranking_version: str | None,
        corpus_snapshot_version: str | None,
        requested_corpus_snapshot_version: str | None,
    ) -> tuple[bool, str | None]:
        if ranking_run_id != PINNED_BRIDGE_RANKING_RUN_ID:
            return False, "ranking_run_id_mismatch"
        if requested_ranking_version and ranking_version and requested_ranking_version != ranking_version:
            return False, "ranking_version_mismatch"
        if (
            requested_corpus_snapshot_version
            and corpus_snapshot_version
            and requested_corpus_snapshot_version != corpus_snapshot_version
        ):
            return False, "corpus_snapshot_version_mismatch"
        return True, None

    def should_attempt_scorer_path(
        self,
        route: str,
        family: str,
        limit: int,
        bridge_eligible_only: bool,
        subject: str | None,
        current_served: int,
        cap: int,
        requested_ranking_run_id: str | None,
    ) -> BridgeScorerGateDecision:
        if not self.is_flag_enabled():
            return BridgeScorerGateDecision(False, "flag_off")
        if not self.is_route_allowed(route):
            return BridgeScorerGateDecision(False, "wrong_route")
        if not self.is_family_allowed(family):
            return BridgeScorerGateDecision(False, "wrong_family")
        if not self.is_limit_allowed(limit):
            return BridgeScorerGateDecision(False, "wrong_limit")
        if bridge_eligible_only:
            return BridgeScorerGateDecision(False, "bridge_eligible_only")
        if not self.is_configured_for_pinned_run():
            return BridgeScorerGateDecision(False, "configured_ranking_run_id_mismatch")
        if not self.is_request_ranking_run_allowed(requested_ranking_run_id):
            return BridgeScorerGateDecision(False, "requested_ranking_run_id_mismatch")
        subject_eligible, subject_reason, emitted_to_public_users = self.is_rollout_subject_eligible(subject)
        if not subject_eligible:
            return BridgeScorerGateDecision(False, subject_reason)
        if not self.is_within_exposure_cap(current_served, cap):
            return BridgeScorerGateDecision(False, "cap_exhausted")
        return BridgeScorerGateDecision(True, None, emitted_to_public_users)


def _parse_enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in _FLAG_ON_VALUES


def _parse_allowlist(raw: str | None) -> frozenset[str]:
    values = [value.strip() for value in str(raw or "").split(",")]
    allowlist = frozenset(value for value in values if value)
    if "*" in allowlist:
        raise ValueError("Wildcard Bridge scorer rollout cohort allowlist is not allowed.")
    return allowlist


def _parse_cap(raw: str | None) -> int:
    try:
        cap = int(str(raw or "").strip())
    except ValueError:
        return 0
    return cap if cap > 0 else 0


def _parse_percent(raw: str | None) -> int:
    try:
        percent = int(str(raw or "").strip())
    except ValueError:
        return 0
    return percent if 0 <= percent <= 100 else 0


def _parse_ranking_run_id(raw: str | None) -> str | None:
    value = str(raw or "").strip()
    return value or None


def get_bridge_rollout_served_count() -> int:
    with _bridge_served_count_lock:
        return _bridge_served_count


def try_reserve_bridge_rollout_slot(cap: int) -> bool:
    global _bridge_served_count
    with _bridge_served_count_lock:
        if cap <= 0 or _bridge_served_count >= cap:
            return False
        _bridge_served_count += 1
        return True


def release_bridge_rollout_slot_for_failure() -> None:
    global _bridge_served_count
    with _bridge_served_count_lock:
        _bridge_served_count = max(0, _bridge_served_count - 1)


def reset_bridge_rollout_served_count() -> None:
    global _bridge_served_count
    with _bridge_served_count_lock:
        _bridge_served_count = 0


def build_bridge_gate_from_env(env: Mapping[str, str] | None = None) -> BridgeScorerRolloutGate:
    source = os.environ if env is None else env
    return BridgeScorerRolloutGate(
        runtime_enabled=_parse_enabled(source.get(_FEATURE_FLAG)),
        cohort_allowlist=_parse_allowlist(source.get(_COHORT_ALLOWLIST)),
        exposure_cap=_parse_cap(source.get(_EXPOSURE_CAP)),
        public_rollout_enabled=_parse_enabled(source.get(_PUBLIC_ROLLOUT_ENABLED)),
        public_rollout_percent=_parse_percent(source.get(_PUBLIC_ROLLOUT_PERCENT)),
        configured_ranking_run_id=_parse_ranking_run_id(source.get(_RANKING_RUN_ID)),
    )

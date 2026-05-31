"""Runtime guardrails for the bounded ML scorer rollout path."""

from __future__ import annotations

from dataclasses import dataclass
import os
import threading
from typing import Mapping

ROLLOUT_ROUTE = "/api/v1/recommendations/ranked"
ROLLOUT_FAMILY = "emerging"
ROLLOUT_LIMIT = 20
PINNED_RANKING_RUN_ID = "rank-83787b91ef"
PINNED_CORPUS_SNAPSHOT_VERSION = "source-snapshot-shadow-generalization-v1-20260521"
PINNED_RANKING_VERSION = "shadow-generalization-product-candidate-ranking-v1"

_FEATURE_FLAG = "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"
_COHORT_ALLOWLIST = "ML_SHADOW_SCORER_V1_ROLLOUT_COHORT_ALLOWLIST"
_EXPOSURE_CAP = "ML_SHADOW_SCORER_V1_ROLLOUT_EXPOSURE_CAP"
_PUBLIC_ROLLOUT_ENABLED = "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_ENABLED"
_PUBLIC_ROLLOUT_PERCENT = "ML_SHADOW_SCORER_V1_PUBLIC_ROLLOUT_PERCENT"
_FLAG_ON_VALUES = frozenset({"1", "true", "on", "yes", "enabled"})

_served_count = 0
_served_count_lock = threading.Lock()


@dataclass(frozen=True)
class ScorerRolloutGate:
    runtime_enabled: bool
    cohort_allowlist: frozenset[str]
    exposure_cap: int
    public_rollout_enabled: bool = False
    public_rollout_percent: int = 0

    def is_flag_enabled(self) -> bool:
        return self.runtime_enabled

    def is_route_allowed(self, route: str) -> bool:
        return route == ROLLOUT_ROUTE

    def is_family_allowed(self, family: str) -> bool:
        return family == ROLLOUT_FAMILY

    def is_limit_allowed(self, limit: int) -> bool:
        return limit == ROLLOUT_LIMIT

    def is_bridge_request(self, family: str, bridge_eligible_only: bool) -> bool:
        _ = bridge_eligible_only
        return family == "bridge"

    def is_cohort_eligible(self, subject: str | None) -> bool:
        normalized = (subject or "").strip()
        return bool(normalized) and normalized in self.cohort_allowlist

    def is_rollout_subject_eligible(
        self, subject: str | None
    ) -> tuple[bool, str | None]:
        normalized = (subject or "").strip()
        if normalized and normalized in self.cohort_allowlist:
            return True, None
        if normalized and not self.public_rollout_enabled:
            return False, "cohort_ineligible"
        if not self.public_rollout_enabled:
            return False, "public_rollout_disabled"
        if self.public_rollout_percent != 100:
            return False, "public_rollout_percent_closed"
        return True, None

    def is_within_exposure_cap(self, current_served: int, cap: int) -> bool:
        return cap > 0 and current_served < cap

    def is_pinned_run_context(
        self,
        *,
        ranking_run_id: str,
        ranking_version: str,
        family: str,
        corpus_snapshot_version: str,
    ) -> bool:
        return (
            ranking_run_id == PINNED_RANKING_RUN_ID
            and ranking_version == PINNED_RANKING_VERSION
            and family == ROLLOUT_FAMILY
            and corpus_snapshot_version == PINNED_CORPUS_SNAPSHOT_VERSION
        )

    def should_attempt_scorer_path(
        self,
        route: str,
        family: str,
        limit: int,
        bridge_eligible_only: bool,
        subject: str | None,
        current_served: int,
        cap: int,
    ) -> tuple[bool, str | None]:
        if not self.is_flag_enabled():
            return False, "flag_off"
        if not self.is_route_allowed(route):
            return False, "wrong_route"
        if self.is_bridge_request(family, bridge_eligible_only):
            return False, "bridge_family"
        if not self.is_family_allowed(family):
            return False, "wrong_family"
        if not self.is_limit_allowed(limit):
            return False, "wrong_limit"
        subject_eligible, subject_reason = self.is_rollout_subject_eligible(subject)
        if not subject_eligible:
            return False, subject_reason
        if not self.is_within_exposure_cap(current_served, cap):
            return False, "cap_exhausted"
        return True, None


def _parse_enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in _FLAG_ON_VALUES


def _parse_allowlist(raw: str | None) -> frozenset[str]:
    values = [value.strip() for value in str(raw or "").split(",")]
    allowlist = frozenset(value for value in values if value)
    if "*" in allowlist:
        raise ValueError("Wildcard scorer rollout cohort allowlist is not allowed.")
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


def get_rollout_served_count() -> int:
    with _served_count_lock:
        return _served_count


def try_reserve_rollout_slot(cap: int) -> bool:
    global _served_count
    with _served_count_lock:
        if cap <= 0 or _served_count >= cap:
            return False
        _served_count += 1
        return True


def release_rollout_slot_for_failure() -> None:
    global _served_count
    with _served_count_lock:
        _served_count = max(0, _served_count - 1)


def reset_rollout_served_count() -> None:
    global _served_count
    with _served_count_lock:
        _served_count = 0


def build_gate_from_env(env: Mapping[str, str] | None = None) -> ScorerRolloutGate:
    source = os.environ if env is None else env
    return ScorerRolloutGate(
        runtime_enabled=_parse_enabled(source.get(_FEATURE_FLAG)),
        cohort_allowlist=_parse_allowlist(source.get(_COHORT_ALLOWLIST)),
        exposure_cap=_parse_cap(source.get(_EXPOSURE_CAP)),
        public_rollout_enabled=_parse_enabled(source.get(_PUBLIC_ROLLOUT_ENABLED)),
        public_rollout_percent=_parse_percent(source.get(_PUBLIC_ROLLOUT_PERCENT)),
    )

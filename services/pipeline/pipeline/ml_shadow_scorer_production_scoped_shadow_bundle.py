"""Production-scoped online shadow plan bundle for ml-shadow-scorer-v1.

The production-scoped-shadow bundle is the canonical ladder view after the
production-readiness grant. Revision 0 is an assemble-only pre-plan skeleton,
revision 1 records a paperwork-only plan contract, revision 2 records a bounded
fixture proof, revision 3 records a pilot authorization request, revision 4
records a pilot authorization grant, revision 5 records a bounded pilot harness,
revision 6 records a harness review, revision 7 records the bounded 528-work
audit-artifact pilot run, revision 8 records pilot review, revision 9 records a
live read-only authorization request, revision 10 records a live read-only
authorization grant, revision 11 records the bounded live read-only pilot run,
and later revisions extend the bounded live/flag-enable paperwork ladder through
the production default/API/user-visible authorization request. It does not
access databases, enable feature flags globally, or authorize production/default/API/user-visible behavior.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any, Mapping

from pipeline.audit_artifact_hash import recorded_sha256_matches_text_artifact
from pipeline.ml_shadow_scorer_phase_bundle import (
    PINNED_IDENTITY,
    verify_ml_shadow_scorer_phase_bundle_payload,
)
from pipeline.ml_shadow_scorer_production_readiness_bundle import (
    GRANT_BUNDLE_REVISION as PRODUCTION_READINESS_GRANT_REVISION,
    POST_GRANT_NEXT_STAGE as PRODUCTION_READINESS_POST_GRANT_NEXT_STAGE,
    verify_ml_shadow_scorer_production_readiness_bundle_payload,
)
from pipeline.repo_paths import default_repo_root, portable_repo_path
from pipeline.shadow_write_path_guards import (
    ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    PROD_SCOPED_SHADOW_ROOT,
    ShadowWritePathGuardError,
    assert_prod_scoped_forbidden_write_target_counts,
    assert_prod_scoped_write_path_allowed,
    prod_scoped_shadow_root,
    resolve_prod_scoped_pilot_directory,
    validate_pilot_run_id,
)

ARTIFACT_TYPE = "ml_shadow_scorer_production_scoped_shadow_bundle"
BUNDLE_VERSION = "online-shadow-production-scoped-v1"
PRE_PLAN_BUNDLE_REVISION = 0
POST_PLAN_BUNDLE_REVISION = 1
POST_PROOF_BUNDLE_REVISION = 2
POST_PILOT_REQUEST_BUNDLE_REVISION = 3
POST_PILOT_GRANT_BUNDLE_REVISION = 4
POST_PILOT_HARNESS_BUNDLE_REVISION = 5
POST_PILOT_HARNESS_REVIEW_BUNDLE_REVISION = 6
POST_PILOT_RUN_BUNDLE_REVISION = 7
POST_PILOT_REVIEW_BUNDLE_REVISION = 8
POST_LIVE_READ_ONLY_REQUEST_BUNDLE_REVISION = 9
POST_LIVE_READ_ONLY_GRANT_BUNDLE_REVISION = 10
POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION = 11
POST_LIVE_READ_ONLY_PILOT_REVIEW_BUNDLE_REVISION = 12
POST_LIVE_EXECUTION_REQUEST_BUNDLE_REVISION = 13
POST_LIVE_EXECUTION_GRANT_BUNDLE_REVISION = 14
POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION = 15
POST_LIVE_EXECUTION_PILOT_REVIEW_BUNDLE_REVISION = 16
POST_FLAG_ENABLEMENT_REQUEST_BUNDLE_REVISION = 17
POST_FLAG_ENABLEMENT_GRANT_BUNDLE_REVISION = 18
POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION = 19
POST_FLAG_ENABLEMENT_PILOT_REVIEW_BUNDLE_REVISION = 20
POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_BUNDLE_REVISION = 21
POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_BUNDLE_REVISION = 22
POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_BUNDLE_REVISION = 23
PRE_PLAN_NEXT_STAGE = "begin_production_scoped_online_shadow_plan_v1"
POST_PLAN_NEXT_STAGE = "implement_production_scoped_online_shadow_proof_v1"
POST_PROOF_NEXT_STAGE = "request_production_scoped_online_shadow_pilot_authorization_v1"
POST_PILOT_REQUEST_NEXT_STAGE = "record_production_scoped_online_shadow_pilot_authorization_grant_v1"
POST_PILOT_GRANT_NEXT_STAGE = "run_production_scoped_online_shadow_pilot_v1"
POST_PILOT_HARNESS_NEXT_STAGE = "run_production_scoped_online_shadow_pilot_v1"
POST_PILOT_HARNESS_REVIEW_ACCEPTED_NEXT_STAGE = "run_production_scoped_online_shadow_pilot_v1"
POST_PILOT_HARNESS_REVIEW_REJECTED_NEXT_STAGE = "remediate_production_scoped_online_shadow_pilot_harness_v1"
POST_PILOT_RUN_NEXT_STAGE = "review_production_scoped_online_shadow_pilot_v1"
POST_PILOT_RUN_REMEDIATE_NEXT_STAGE = "remediate_production_scoped_online_shadow_pilot_v1"
POST_PILOT_REVIEW_ACCEPTED_NEXT_STAGE = "request_production_scoped_online_shadow_live_read_only_authorization_v1"
POST_PILOT_REVIEW_REJECTED_NEXT_STAGE = "remediate_production_scoped_online_shadow_pilot_v1"
POST_LIVE_READ_ONLY_REQUEST_NEXT_STAGE = (
    "record_production_scoped_online_shadow_live_read_only_authorization_grant_v1"
)
POST_LIVE_READ_ONLY_GRANT_NEXT_STAGE = "run_production_scoped_online_shadow_live_read_only_pilot_v1"
POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE = "review_production_scoped_online_shadow_live_read_only_pilot_v1"
POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE = (
    "request_production_scoped_online_shadow_live_execution_authorization_v1"
)
POST_LIVE_READ_ONLY_PILOT_REVIEW_REJECTED_NEXT_STAGE = (
    "remediate_production_scoped_online_shadow_live_read_only_pilot_v1"
)
POST_LIVE_EXECUTION_REQUEST_NEXT_STAGE = (
    "record_production_scoped_online_shadow_live_execution_authorization_grant_v1"
)
POST_LIVE_EXECUTION_GRANT_NEXT_STAGE = "run_production_scoped_online_shadow_live_execution_pilot_v1"
POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE = "review_production_scoped_online_shadow_live_execution_pilot_v1"
POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE = (
    "request_production_scoped_online_shadow_flag_enablement_authorization_v1"
)
POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE = (
    "remediate_production_scoped_online_shadow_live_execution_pilot_v1"
)
POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE = (
    "record_production_scoped_online_shadow_flag_enablement_authorization_grant_v1"
)
POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE = "run_production_scoped_online_shadow_flag_enablement_pilot_v1"
POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE = "review_production_scoped_online_shadow_flag_enablement_pilot_v1"
POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE = (
    "request_production_scoped_online_shadow_production_default_api_user_visible_authorization_v1"
)
POST_FLAG_ENABLEMENT_PILOT_REVIEW_REJECTED_NEXT_STAGE = (
    "remediate_production_scoped_online_shadow_flag_enablement_pilot_v1"
)
POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE = (
    "record_production_scoped_online_shadow_production_default_api_user_visible_authorization_grant_v1"
)
POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE = (
    "run_production_scoped_online_shadow_production_default_api_user_visible_pilot_v1"
)
POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE = (
    "review_production_scoped_online_shadow_production_default_api_user_visible_pilot_v1"
)
FEATURE_FLAG = "ML_SHADOW_SCORER_V1_RUNTIME_ENABLED"
FUTURE_ARTIFACT_ROOT = "docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/<pilot_run_id>/"
PILOT_REQUEST_SCOPE = "production_scoped_shadow_pilot_paperwork_only"
PILOT_GRANT_SCOPE = "production_scoped_shadow_pilot_authorization_only"
LIVE_READ_ONLY_REQUEST_SCOPE = "production_scoped_shadow_live_read_only_paperwork_only"
LIVE_READ_ONLY_GRANT_SCOPE = "production_scoped_shadow_live_read_only_authorization_only"
LIVE_EXECUTION_REQUEST_SCOPE = "production_scoped_shadow_live_execution_paperwork_only"
LIVE_EXECUTION_GRANT_SCOPE = "production_scoped_shadow_live_execution_authorization_only"
FLAG_ENABLEMENT_REQUEST_SCOPE = "production_scoped_shadow_flag_enablement_paperwork_only"
FLAG_ENABLEMENT_GRANT_SCOPE = "production_scoped_shadow_flag_enablement_authorization_only"
PILOT_HARNESS_SURFACE = "bounded_fixture_pilot_harness"
PILOT_RUN_SURFACE = "bounded_read_only_audit_artifact_pilot"
LIVE_READ_ONLY_PILOT_RUN_SURFACE = "bounded_live_read_only_prod_scoped_pilot"
LIVE_READ_ONLY_PILOT_RUN_RANKING_VERSION = "shadow-generalization-product-candidate-ranking-v1"
LIVE_EXECUTION_PILOT_RUN_SURFACE = "bounded_live_execution_prod_scoped_pilot"
LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION = LIVE_READ_ONLY_PILOT_RUN_RANKING_VERSION
LIVE_EXECUTION_PILOT_RUN_ID_PREFIX = "prod-live-exec"
LIVE_READ_ONLY_PILOT_RUN_AUDIT_PROBABILITY_SOURCE = (
    "computed_from_live_embedding_vectors_with_frozen_scorer"
)
LIVE_READ_ONLY_PILOT_RUN_SCORER_PATH = "docs/audit/ml-offline-audit-embedding-scorer-v2.json"
LIVE_READ_ONLY_PILOT_RUN_SCORER_VERSION = "ml-offline-audit-embedding-scorer-v2"
PILOT_HARNESS_EXPECTED_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
PILOT_RUN_EXPECTED_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
LIVE_READ_ONLY_PILOT_RUN_EXPECTED_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
LIVE_EXECUTION_PILOT_RUN_EXPECTED_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
FLAG_ENABLEMENT_PILOT_RUN_SURFACE = "bounded_flag_enablement_prod_scoped_pilot"
FLAG_ENABLEMENT_PILOT_RUN_RANKING_VERSION = LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION
FLAG_ENABLEMENT_PILOT_RUN_ID_PREFIX = "prod-flag-enable"
FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES = ("manifest.json", "shadow_rows.jsonl", "observability.json", "write_counts.json")
PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_SURFACE = (
    "bounded_production_default_api_user_visible_prod_scoped_pilot"
)
PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_RANKING_VERSION = (
    "shadow-generalization-product-candidate-ranking-v1"
)
PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_ID_PREFIX = "prod-output"
PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES = (
    "manifest.json",
    "shadow_rows.jsonl",
    "observability.json",
    "write_counts.json",
)
PILOT_HARNESS_REVIEW_CHECKS = (
    "runtime_drill_pilot_status_succeeded_test_only",
    "fixture_row_count_3",
    "runtime_drill_call_order",
    "environment_restored",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "runtime_writes_false",
    "live_prod_source_reads_false",
    "pilot_surface_bounded_fixture",
    "actual_pilot_executed_false",
    "production_api_user_visible_unchanged",
    "labels_not_used",
    "pass_fail_overall_passed",
    "pass_fail_failed_checks_empty",
)
PILOT_RUN_REVIEW_CHECKS = (
    "pilot_run_pass_fail_overall_passed",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "environment_restored",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "source_artifacts_verified",
    "runtime_writes_false",
    "live_prod_source_reads_false",
    "pilot_surface_bounded_read_only_audit_artifact",
    "production_api_user_visible_unchanged",
    "global_live_execution_authorization_false",
)

LIVE_READ_ONLY_PILOT_RUN_PASS_FAIL_CHECKS = (
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "environment_restored",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "live_prod_source_reads_true",
    "live_source_reads_documented",
    "pilot_surface_bounded_live_read_only_prod_scoped",
    "no_labels_used_for_scoring",
    "no_refit_training_embedding_generation_or_label_ingest",
    "read_only_sql_and_allowlist_enforced",
    "production_api_user_visible_unchanged",
    "global_live_execution_authorization_false",
)

LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS = (
    "live_read_only_pilot_run_pass_fail_overall_passed",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "environment_restored",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "live_prod_source_reads_true",
    "live_source_reads_documented",
    "no_labels_refit_embedding_or_label_ingest",
    "pilot_surface_bounded_live_read_only_prod_scoped",
    "harness_and_audit_pilot_still_no_live_reads",
    "production_api_user_visible_unchanged",
    "global_live_execution_authorization_false",
    "live_read_only_grant_slices_present",
)

LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS = (
    "live_execution_grant_present",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "pilot_status_succeeded_test_only",
    "process_scoped_runtime_flag_only",
    "environment_restored",
    "incomplete_coverage_skip_verified",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "production_api_user_visible_unchanged",
    "no_labels_refit_embedding_generation_or_label_ingest",
    "global_execution_authorization_false",
)

LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS = (
    "live_execution_pilot_run_pass_fail_overall_passed",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "pilot_status_succeeded_test_only",
    "process_scoped_runtime_flag_only",
    "environment_restored",
    "incomplete_coverage_skip_verified",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "live_execution_grant_slices_present",
    "live_read_only_chain_still_valid",
    "production_api_user_visible_unchanged",
    "global_execution_authorization_false",
    "no_labels_refit_embedding_generation_or_label_ingest",
    "ranking_version_not_test_fixture",
)

FORBIDDEN_PROD_SCOPED_WRITE_TARGETS = (
    "ranking_runs",
    "paper_scores",
    "embeddings",
    "labels",
    "scorer_artifacts",
    "production_config",
    "production_default_pins",
    "api_visible_tables",
    "prod_scoped_shadow_tables",
)

OBSERVABILITY_SIGNALS = (
    "run status",
    "row counts",
    "error counters",
    "latency",
    "component coverage",
    "score distributions",
    "skipped runs/reasons",
    "forbidden write target counts (all zero)",
    "rank displacement audit-only",
)

REQUIRED_ROLES = ("production_readiness_bundle", "phase2_bundle", "online_shadow_policy")
OPTIONAL_ROLES = (
    "execution_authorization_grant",
    "phase2_write_mode_plan",
    "phase2_write_mode_proof",
    "generalization_audit_gates",
)
LEGACY_ARTIFACT_ROLES = REQUIRED_ROLES + OPTIONAL_ROLES

PLAN_SUBSECTIONS = (
    "prod_scoped_identity_and_rollout_boundaries",
    "feature_flag_iam_config_requirements",
    "prod_read_only_input_contract",
    "production_default_api_user_visible_separation",
    "observability_and_slo_plan",
    "rollback_and_revocation_drill_plan",
    "proof_and_pilot_prerequisites",
    "ci_and_live_gate_requirements",
)

COMMON_PLAN_CAVEATS = (
    "Bundle plan surface only; does not run runtime or shadow scoring.",
    "Bundle does not enable online shadow execution or change the global feature flag default.",
    "Bundle does not authorize production default/API/user-visible ranking behavior.",
    "Bundle does not write shadow-runs files, databases, embeddings, labels, or scorer artifacts.",
    "Frozen upstream bundles and legacy artifacts remain referenced by path and SHA only.",
)

PLAN_CAVEATS = (
    "Plan milestone only; does not authorize production-scoped proof execution or pilot execution.",
    "Future proof must clear missing_prod_scoped_shadow_proof before any prod-scoped pilot can be considered.",
    "Production default/API/user-visible behavior remain separate authorization chains.",
)

PROOF_CAVEATS = (
    "Proof is a bounded fixture/dry-run; not a live prod run and does not call runtime.",
    "Proof clears the prod-scoped shadow proof blocker only.",
    "Pilot authorization, live execution authorization, flag enablement, and prod default/API/user-visible remain separate gates.",
)

REQUEST_CAVEATS = (
    "Bundle pilot-request milestone only; grants no pilot authorization.",
    "Accepted proof evidence is necessary but not sufficient for pilot execution.",
    "Pilot grant remains a separate gate; does not enable global shadow or prod default/API/user-visible.",
)

GRANT_CAVEATS = (
    "Bundle pilot-grant milestone only; does not run the prod-scoped pilot.",
    "Clears prod-scoped pilot authorization blocker for the pilot chain only.",
    "Bounded pilot run still required before any enablement or prod default/API/user-visible change.",
    "Global shadow flag default remains off; prod default/API/user-visible remain separate chains.",
)

HARNESS_CAVEATS = (
    "This is a bounded fixture pilot harness, not live production traffic.",
    "No live prod source reads were performed.",
    "No production-scoped pilot execution is recorded.",
    "Global shadow remains disabled.",
    "Production default/API/user-visible behavior remains unchanged.",
    "Actual production-scoped pilot remains a separate milestone.",
)

HARNESS_REVIEW_CAVEATS = (
    "Review covers bounded fixture pilot harness plumbing evidence only, not live production traffic.",
    "No runtime rerun or shadow-runs artifact read was performed by the review.",
    "The actual production-scoped pilot remains unexecuted and separately gated.",
)

PILOT_RUN_CAVEATS = (
    "Pilot uses approved frozen second-surface audit artifacts, not live production traffic.",
    "No live production DB/source reads were performed.",
    "Live read-only production shadow access remains a separate future authorization chain.",
    "Global online shadow enablement remains false.",
    "Production default/API/user-visible behavior remains unchanged.",
)

PILOT_REVIEW_CAVEATS = (
    "Pilot review accepts bounded audit-artifact pilot evidence only.",
    "Pilot review does not grant live read-only production source access.",
    "Pilot review does not grant global/live/fleet online shadow execution.",
    "Production default/API/user-visible behavior remains unchanged after pilot review.",
)

LIVE_READ_ONLY_REQUEST_CAVEATS = (
    "Bundle live-read-only request milestone only; grants no live production source access.",
    "Accepted bounded audit-artifact pilot is necessary but not sufficient for live read-only access.",
    "Live reads remain unperformed until a separate grant and run milestone.",
)

LIVE_READ_ONLY_GRANT_CAVEATS = (
    "Bundle live-read-only grant milestone only; does not run the live read-only pilot.",
    "Clears live read-only authorization blocker for the live read-only pilot chain only.",
    "Grants read-only source access authorization only; does not perform live reads at grant time.",
    "Live read-only pilot run still required before any live prod source reads are recorded.",
    "Does not enable global/live/fleet online shadow execution.",
    "Production default/API/user-visible behavior remains unchanged.",
)

LIVE_READ_ONLY_PILOT_RUN_CAVEATS = (
    "Live read-only pilot uses approved read-only production sources under grant boundaries.",
    "First milestone where live prod source reads are recorded.",
    "Input-only scoring; labels are not scoring features.",
    "No model refit/training, embedding generation, or label ingest.",
    "Does not enable global/live/fleet online shadow execution.",
    "Production default/API/user-visible behavior remains unchanged.",
    "Does not perform production writes, committed artifact writes, or runtime writes.",
)

LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS = (
    "Live read-only pilot review evaluates recorded rev 11 evidence only.",
    "Review does not rerun runtime, open DB connections, or read shadow-runs files.",
    "Review does not grant global/live/fleet online shadow execution.",
    "Review does not change production default, API/web, or user-visible ranking.",
    "Accepted review clears the live read-only pilot evidence gate only.",
)

LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS = (
    "Harness evidence remains bounded fixture-only and records no live source reads.",
    "Audit-artifact pilot evidence remains frozen-artifact-only and records no live production DB/source reads.",
    "Pilot and harness reviews did not rerun runtime or read shadow-runs artifacts.",
    "Live read-only pilot run evidence records live prod source reads under grant boundaries.",
    "Live read-only pilot run evidence records no production writes, committed artifact writes, or runtime writes.",
)

LIVE_EXECUTION_REQUEST_CAVEATS = (
    "Bundle live-execution request milestone only; grants no live execution authorization.",
    "Accepted live read-only pilot review is necessary but not sufficient for live execution authorization.",
    "Live execution remains unauthorized until a separate grant milestone.",
    "Does not enable global/live/fleet online shadow execution.",
    "Production default/API/user-visible behavior remains unchanged.",
    "Live prod source reads already recorded by rev 11 remain unchanged; this request does not perform new reads.",
)

LIVE_EXECUTION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY = (
    "bounded prod-scoped live execution pilot authorization paperwork complete",
    "bounded live execution pilot run may be executed in a separate run milestone under request/grant contract",
    "bounded live execution pilot review may be recorded after the separate run milestone",
)

LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED = (
    "global enablement",
    "global/fleet online shadow execution",
    "production default",
    "API/web",
    "user-visible ranking changes",
    "production default/API/user-visible recommendation output",
    "DB writes/DDL",
    "refit/training",
    "fleet-wide enablement",
)

LIVE_EXECUTION_GRANT_TIME_BOUNDARIES = (
    "no bounded live execution pilot run performed at grant time",
    "no new live reads performed at grant time",
    "no writes performed at grant time",
    "no execution.live_execution_pilot_run slice recorded at grant time",
    "rev 15 run must prove bounded scope, flag-off behavior, and no forbidden writes",
)

LIVE_EXECUTION_GRANT_CAVEATS = (
    "Grant milestone only; does not run bounded live execution pilot.",
    "Does not enable global/live/fleet online shadow execution.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Does not perform new live reads at grant time.",
    "Bounded live execution pilot run remains a separate rev 15 milestone.",
)

LIVE_EXECUTION_PILOT_RUN_CAVEATS = (
    "Bounded live execution pilot run only; does not enable global/fleet online shadow execution.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Runtime flag is enabled only inside the bounded pilot drill and restored afterward.",
    "Forbidden production write targets remain zero.",
    "Review is required before any further enablement chain.",
)

LIVE_EXECUTION_PILOT_REVIEW_CAVEATS = (
    "Review milestone only; does not rerun bounded live execution pilot.",
    "Does not call runtime, connect to DB, read shadow-runs files, or perform new live reads.",
    "Accepted review is necessary but not sufficient for flag enablement.",
    "Does not enable global online shadow execution.",
    "Does not change production default, API/web, or user-visible ranking.",
)

FLAG_ENABLEMENT_REQUEST_FUTURE_GRANT_REQUIREMENTS = (
    "accepted live execution pilot review evidence",
    "exact production-scoped flag target and environment boundary",
    "rollout and rollback owner",
    "flag-off drill and disable-switch evidence",
    "observability for flag-enabled shadow execution",
    "incomplete coverage skip behavior remains enforced",
    "forbidden production writes remain zero",
    "production default/API/user-visible behavior remains unchanged",
    "no global/fleet enablement",
    "time-bound grant/review requirements",
)

FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED = (
    "global/fleet flag enablement",
    "production default",
    "API/web",
    "user-visible ranking changes",
    "production default/API/user-visible recommendation output",
    "DB writes/DDL",
    "refit/training",
    "label ingest",
    "live flag enablement at request time",
    "online_shadow_execution_enabled globally",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
)

FLAG_ENABLEMENT_REQUEST_CAVEATS = (
    "Bundle flag-enablement request milestone only; grants no flag enablement authorization.",
    "Accepted live execution pilot review is necessary but not sufficient for flag enablement.",
    "Runtime flag remains disabled globally and unchanged by this request.",
    "Does not enable global/fleet online shadow execution.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Does not perform runtime calls, DB reads, shadow-runs reads, writes, refit/training, or label ingest.",
)

FLAG_ENABLEMENT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY = (
    "bounded prod-scoped flag enablement authorization paperwork complete",
    "bounded flag-enablement pilot run may be executed in a separate run milestone under request/grant contract",
    "bounded flag-enablement pilot review may be recorded after the separate run milestone",
)

FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED = (
    "global/fleet online shadow execution",
    "global/fleet flag enablement",
    "production default",
    "API/web",
    "user-visible ranking changes",
    "production default/API/user-visible recommendation output",
    "DB writes/DDL",
    "refit/training",
    "label ingest",
    "live runtime flag enablement at grant time",
    "online_shadow_execution_enabled globally",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
)

FLAG_ENABLEMENT_GRANT_TIME_BOUNDARIES = (
    "no bounded flag-enablement pilot run performed at grant time",
    "no runtime flag enablement performed at grant time",
    "no new live reads performed at grant time",
    "no writes performed at grant time",
    "no execution.flag_enablement_pilot_run slice recorded at grant time",
    "rev 19 run must prove bounded scope, flag-off restoration, and no forbidden writes",
)

FLAG_ENABLEMENT_GRANT_CAVEATS = (
    "Grant milestone only; does not run bounded flag-enablement pilot.",
    "Does not enable ML_SHADOW_SCORER_V1_RUNTIME_ENABLED globally or fleet-wide.",
    "Does not set online_shadow_execution_enabled.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Does not perform runtime calls, DB reads, shadow-runs reads, writes, refit/training, or label ingest.",
    "Bounded flag-enablement pilot run remains a separate rev 19 milestone.",
)

FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS = (
    "flag_enablement_grant_slices_present",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "pilot_status_succeeded_test_only",
    "process_scoped_runtime_flag_only",
    "runtime_flag_enabled_only_during_pilot",
    "environment_restored",
    "incomplete_coverage_skip_verified",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "live_execution_chain_still_valid",
    "production_api_user_visible_unchanged",
    "global_execution_authorization_false",
    "plan_flag_authorized_now_false",
    "no_labels_refit_embedding_generation_or_label_ingest",
    "ranking_version_not_test_fixture",
)

FLAG_ENABLEMENT_PILOT_RUN_CAVEATS = (
    "Bounded flag-enablement pilot run only; does not enable global/fleet online shadow execution.",
    "Does not set online_shadow_execution_enabled.",
    "Does not set plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Runtime flag is enabled only inside the bounded pilot drill and restored afterward.",
    "Forbidden production write targets remain zero.",
    "Review is required before any further enablement chain.",
)

FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS = (
    "flag_enablement_pilot_run_pass_fail_overall_passed",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "pilot_status_succeeded_test_only",
    "process_scoped_runtime_flag_only",
    "runtime_flag_enabled_only_during_pilot",
    "environment_restored",
    "incomplete_coverage_skip_verified",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_4",
    "expected_files_recorded",
    "flag_enablement_grant_slices_present",
    "live_execution_chain_still_valid",
    "production_api_user_visible_unchanged",
    "global_execution_authorization_false",
    "plan_flag_authorized_now_false",
    "no_labels_refit_embedding_generation_or_label_ingest",
    "ranking_version_not_test_fixture",
)

FLAG_ENABLEMENT_PILOT_REVIEW_CAVEATS = (
    "Review milestone only; does not rerun bounded flag-enablement pilot.",
    "Does not call runtime, connect to DB, read shadow-runs files, or perform new live reads.",
    "Accepted review is necessary but not sufficient for production default/API/user-visible authorization.",
    "Does not enable global/fleet online shadow execution.",
    "Does not set plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now.",
    "Does not change production default, API/web, or user-visible ranking.",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE = (
    "production_scoped_shadow_production_default_api_user_visible_paperwork_only"
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_FUTURE_GRANT_REQUIREMENTS = (
    "accepted flag enablement pilot review evidence",
    "exact production default/API/user-visible surface scope",
    "explicit owner approval for production recommendation output",
    "rollback and disable-switch owner",
    "observability and incident response requirements",
    "canary or bounded exposure plan before any broad rollout",
    "production default/API/user-visible separation remains explicit",
    "forbidden production writes remain zero unless separately authorized",
    "no refit/training or label ingest",
    "time-bound grant/review requirements",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED = (
    "authorization grant",
    "production default changes at request time",
    "API/web changes at request time",
    "user-visible ranking changes at request time",
    "production recommendation output at request time",
    "global/fleet online shadow execution",
    "online_shadow_execution_enabled",
    "prod_scoped_shadow_execution_authorized",
    "DB writes/DDL",
    "refit/training",
    "label ingest",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_CAVEATS = (
    "Bundle production default/API/user-visible request milestone only; grants no authorization.",
    "Accepted flag-enablement pilot review is necessary but not sufficient for production output.",
    "Does not enable online_shadow_execution_enabled or prod_scoped_shadow_execution_authorized.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Does not perform runtime calls, DB reads, shadow-runs reads, writes, refit/training, or label ingest.",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE = (
    "production_scoped_shadow_production_default_api_user_visible_grant_paperwork_only"
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_AUTHORIZES_FOR_CHAIN_ONLY = (
    "bounded production-scoped production default/API/user-visible authorization paperwork complete",
    "bounded production default/API/user-visible pilot run may be executed in a separate rev 23 run milestone",
    "bounded production default/API/user-visible pilot review may be recorded after the separate rev 23 run milestone",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED = (
    "runtime enablement at grant time",
    "production default changes at grant time",
    "API/web changes at grant time",
    "user-visible ranking changes at grant time",
    "production default/API/user-visible recommendation output at grant time",
    "global/fleet online shadow execution",
    "online_shadow_execution_enabled globally",
    "prod_scoped_shadow_execution_authorized",
    "DB writes/DDL",
    "refit/training",
    "label ingest",
    "broad rollout",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_TIME_BOUNDARIES = (
    "no bounded production default/API/user-visible pilot run performed at grant time",
    "no production_default_allowed changes at grant time",
    "no api_web_changes_allowed changes at grant time",
    "no user_visible_ranking_changed changes at grant time",
    "no new live reads performed at grant time",
    "no writes performed at grant time",
    "no execution.production_default_api_user_visible_pilot_run slice recorded at grant time",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_CAVEATS = (
    "Grant milestone only; does not run bounded production default/API/user-visible pilot.",
    "Does not enable online_shadow_execution_enabled or prod_scoped_shadow_execution_authorized.",
    "Does not change production default, API/web, or user-visible ranking.",
    "Does not perform runtime calls, DB reads, shadow-runs reads, writes, refit/training, or label ingest.",
    "Bounded production default/API/user-visible pilot run remains a separate rev 23 milestone.",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS = (
    "production_default_api_user_visible_grant_slices_present",
    "joined_candidate_count_528",
    "runtime_row_count_528",
    "runtime_drill_call_order",
    "preflight_postflight_disabled",
    "pilot_status_succeeded_test_only",
    "process_scoped_runtime_flag_only",
    "runtime_flag_enabled_only_during_pilot",
    "environment_restored",
    "incomplete_coverage_skip_verified",
    "approved_source_reread_verified",
    "ranking_version_not_test_fixture",
    "bounded_api_surface_probe_performed",
    "would_be_shadow_scorer_output_built",
    "no_public_user_traffic",
    "production_default_api_user_visible_changed_false",
    "paper_scores_and_ranking_runs_not_written",
    "forbidden_write_counts_zero",
    "isolated_artifact_count_expected",
    "expected_files_recorded",
    "live_flag_and_read_only_chain_still_valid",
    "global_execution_authorization_false",
    "plan_flag_authorized_now_false",
    "bridge_surface_not_included",
    "no_labels_refit_embedding_generation_or_label_ingest",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_CAVEATS = (
    "Bounded production default/API/user-visible pilot run only; does not publish recommendations.",
    "Probe is in-process, audit-only, and emits no response to real users or clients.",
    "Does not bind an HTTP server or call outbound production API routes.",
    "Would-be scorer output and current read-path comparison are recorded as evidence only.",
    "Does not set online_shadow_execution_enabled, prod_scoped_shadow_execution_authorized, production_default_allowed, api_web_changes_allowed, or user_visible_ranking_changed.",
    "Does not write paper_scores, ranking_runs, production config, labels, embeddings, or scorer artifacts.",
    "Bridge recommendations remain out of scope for this emerging-family pilot.",
    "Review is required before any further production default/API/user-visible chain.",
)

FLAG_ENABLEMENT_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED = tuple(
    sorted(
        set(FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED)
        - set(FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED)
        - set(LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED)
    )
)

EXPLICITLY_NOT_INCLUDED = (
    "global flag enablement",
    "prod default",
    "API/web",
    "user-visible ranking",
    "DB writes/DDL",
)

PILOT_REQUEST_EXPLICITLY_NOT_INCLUDED = (
    "online_shadow_execution_enabled globally",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
    "DB writes/DDL",
    "model refit, embedding generation, label ingest",
    "fleet-wide flag enablement",
    "user-visible ranking changes",
    "live prod execution beyond an explicitly granted bounded pilot",
)

PILOT_REQUEST_WOULD_ENABLE_AFTER_FUTURE_GRANT = (
    "future grant may authorize bounded prod-scoped pilot execution chain only",
    "future pilot may use plan/proof contract under manual_or_scheduled_jobs_only",
)

PILOT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY = (
    "prod-scoped pilot authorization paperwork complete",
    "bounded prod-scoped pilot run may be executed in a separate run milestone under the plan/proof contract",
)

PILOT_GRANT_STILL_NOT_INCLUDED = (
    "online_shadow_execution_enabled globally",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
    "DB writes/DDL",
    "model refit, embedding generation, label ingest",
    "fleet-wide flag enablement",
    "user-visible ranking changes",
    "production default / API / fleet-wide enablement",
)

PILOT_GRANT_TIME_BOUNDARIES = (
    "manual_or_scheduled_jobs_only",
    "emerging-family / rank-83787b91ef identity only unless explicitly superseded",
    "prod-scoped artifact root under docs/audit/shadow-runs/ml-shadow-scorer-v1/prod-scoped/<pilot_run_id>/",
    "read-only prod input contract from plan unless future run milestone explicitly expands",
)

LIVE_READ_ONLY_REQUEST_FUTURE_GRANT_REQUIREMENTS = (
    "exact IAM/read-only scope",
    "approved production source allowlist",
    "pinned identity and ranking-run/family boundaries",
    "incomplete coverage behavior must skip the whole run",
    "forbidden write targets remain zero",
    "observability requirements for live reads",
    "rollback/flag-off drill against live read mode",
    "no production default/API/user-visible behavior changes",
    "no global/fleet enablement",
    "time-bound grant/review requirements",
    "owner/second-review requirements",
)

LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED = (
    "global enablement",
    "production default",
    "API/web",
    "user-visible ranking",
    "DB writes/DDL",
    "refit/training",
    "fleet-wide enablement",
    "live reads at request time",
)

LIVE_EXECUTION_REQUEST_FUTURE_GRANT_REQUIREMENTS = (
    "accepted live read-only pilot review evidence",
    "exact bounded live-execution scope under plan/proof contract",
    "pinned identity/ranking-run/family boundaries",
    "process-scoped runtime flag boundaries",
    "forbidden write targets remain zero",
    "rollback/flag-off drill for live execution mode",
    "observability requirements for live execution",
    "no production default/API/user-visible behavior changes",
    "no global/fleet enablement",
    "time-bound grant/review requirements",
    "owner/second-review requirements",
)

LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED = (
    "global enablement",
    "production default",
    "API/web",
    "user-visible ranking",
    "DB writes/DDL",
    "refit/training",
    "fleet-wide enablement",
    "live execution at request time",
    "online_shadow_execution_enabled globally",
    "production_default_allowed",
    "api_web_changes_allowed",
    "user_visible_ranking_changed",
)

LIVE_READ_ONLY_GRANT_AUTHORIZES_FOR_CHAIN_ONLY = (
    "live read-only prod-scoped shadow pilot authorization paperwork complete",
    "bounded live read-only pilot run may be executed in a separate run milestone under request/grant contract",
)

LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED = (
    "global enablement",
    "production default",
    "API/web",
    "user-visible ranking",
    "DB writes/DDL",
    "refit/training",
    "fleet-wide enablement",
    "live reads at grant time",
)

PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED = tuple(
    sorted(
        set(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED)
        - set(PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED)
        - set(FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED)
        - set(FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED)
        - set(LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED)
        - set(LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED)
        - set(PILOT_GRANT_STILL_NOT_INCLUDED)
        - set(PILOT_REQUEST_EXPLICITLY_NOT_INCLUDED)
    )
)

LIVE_READ_ONLY_GRANT_TIME_BOUNDARIES = (
    "read-only IAM scope",
    "approved source allowlist",
    "pinned identity/ranking-run/family boundaries",
    "skip-whole-run incomplete coverage",
    "process-scoped runtime flag",
    "zero forbidden writes",
    "rollback/flag-off drill",
)


class MLShadowScorerProductionScopedShadowBundleError(Exception):
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
        raise MLShadowScorerProductionScopedShadowBundleError(f"Failed to load JSON {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MLShadowScorerProductionScopedShadowBundleError(f"Expected JSON object in {path}")
    return payload


def _metadata(payload: Mapping[str, Any], *, label: str) -> Mapping[str, Any]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} missing metadata object")
    return metadata


def _get(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return None
    return current


def _iter_named_field_values(payload: Any, field_name: str, *, path: str = "") -> list[tuple[str, Any]]:
    matches: list[tuple[str, Any]] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            next_path = f"{path}.{key}" if path else str(key)
            if key == field_name:
                matches.append((next_path, value))
            matches.extend(_iter_named_field_values(value, field_name, path=next_path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            next_path = f"{path}[{index}]"
            matches.extend(_iter_named_field_values(value, field_name, path=next_path))
    return matches


def _require_named_flags_not_true(payload: Any, field_names: tuple[str, ...]) -> None:
    for field_name in field_names:
        for path, value in _iter_named_field_values(payload, field_name):
            if value is True:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{path} must not be true for this bundle stage"
                )


def _require_equal(name: str, observed: Any, expected: Any) -> None:
    if observed != expected:
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"{name} must be {expected!r}, got {observed!r}"
        )


def _require_true(name: str, observed: Any) -> None:
    _require_equal(name, observed, True)


def _require_false(name: str, observed: Any) -> None:
    _require_equal(name, observed, False)


def _validate_identity(identity: Any, *, label: str) -> None:
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} must be an object")
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(f"{label}.{field}", identity.get(field), expected)


def _artifact_record(role: str, path: Path, *, repo_root: Path) -> dict[str, str]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowBundleError(f"{role} artifact does not exist: {path}")
    return {
        "role": role,
        "path": portable_repo_path(resolved, repo_root=repo_root),
        "sha256": _sha256_file(resolved),
    }


def _ref_from_record(record: Mapping[str, Any]) -> dict[str, str]:
    return {"path": str(record["path"]), "sha256": str(record["sha256"])}


def _resolve_recorded_path(recorded_path: Any, *, repo_root: Path) -> Path:
    if not isinstance(recorded_path, str) or not recorded_path.strip():
        raise MLShadowScorerProductionScopedShadowBundleError("referenced path must be a non-empty string")
    candidate = Path(recorded_path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (repo_root / candidate).resolve()


def _verify_reference(ref: Any, *, repo_root: Path, label: str) -> Path:
    if not isinstance(ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} reference must be an object")
    recorded_sha = ref.get("sha256")
    if not isinstance(recorded_sha, str) or not recorded_sha.strip():
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label}.sha256 missing")
    resolved = _resolve_recorded_path(ref.get("path"), repo_root=repo_root)
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} path missing on disk: {ref.get('path')}")
    if not recorded_sha256_matches_text_artifact(resolved, recorded_sha):
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"{label} sha256 mismatch: recorded {recorded_sha}, actual {_sha256_file(resolved)}"
        )
    return resolved


def _records_by_role(records: Any) -> dict[str, Mapping[str, Any]]:
    if not isinstance(records, list) or not records:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "metadata.legacy_artifacts_index must be a non-empty list"
        )
    by_role: dict[str, Mapping[str, Any]] = {}
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"metadata.legacy_artifacts_index[{index}] must be an object"
            )
        role = record.get("role")
        if not isinstance(role, str) or not role:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"metadata.legacy_artifacts_index[{index}].role missing"
            )
        if role not in LEGACY_ARTIFACT_ROLES:
            raise MLShadowScorerProductionScopedShadowBundleError(f"unsupported legacy artifact role {role!r}")
        by_role[role] = record
    missing = [role for role in REQUIRED_ROLES if role not in by_role]
    if missing:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "metadata.legacy_artifacts_index missing roles: " + ", ".join(missing)
        )
    return by_role


def _verify_legacy_index(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
) -> tuple[dict[str, Mapping[str, Any]], dict[str, Path]]:
    records = _records_by_role(_metadata(bundle, label="production-scoped-shadow bundle").get("legacy_artifacts_index"))
    resolved: dict[str, Path] = {}
    for role, record in records.items():
        resolved[role] = _verify_reference(
            record,
            repo_root=repo_root,
            label=f"metadata.legacy_artifacts_index.{role}",
        )
    return records, resolved


def _validate_production_readiness_bundle(bundle: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_production_readiness_bundle_payload(
            bundle,
            repo_root=repo_root,
            expect_grant_filed=True,
        )
    except Exception as exc:
        raise MLShadowScorerProductionScopedShadowBundleError(str(exc)) from exc
    _require_equal(
        "production-readiness bundle metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        PRODUCTION_READINESS_GRANT_REVISION,
    )
    _require_true(
        "production-readiness bundle authorization.production_readiness_authorization_granted",
        _get(bundle, "authorization.production_readiness_authorization_granted"),
    )
    _require_false(
        "production-readiness bundle posture.missing_production_readiness_authorization",
        _get(bundle, "posture.missing_production_readiness_authorization"),
    )
    _require_false(
        "production-readiness bundle posture.online_shadow_execution_enabled",
        _get(bundle, "posture.online_shadow_execution_enabled"),
    )
    _require_equal(
        "production-readiness bundle recommended_next_stage",
        bundle.get("recommended_next_stage"),
        PRODUCTION_READINESS_POST_GRANT_NEXT_STAGE,
    )
    _validate_identity(_get(bundle, "metadata.pinned_identity"), label="production-readiness metadata.pinned_identity")


def _validate_phase2_bundle(bundle: Mapping[str, Any], *, repo_root: Path) -> None:
    try:
        verify_ml_shadow_scorer_phase_bundle_payload(
            bundle,
            repo_root=repo_root,
            expect_pilot_reviewed=True,
        )
    except Exception as exc:
        raise MLShadowScorerProductionScopedShadowBundleError(str(exc)) from exc
    revision = _get(bundle, "metadata.bundle_revision")
    if not isinstance(revision, int) or revision < 3:
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"phase2 bundle metadata.bundle_revision must be >= 3, got {revision!r}"
        )
    _require_true("phase2 bundle review.phase2_write_pilot_accepted", _get(bundle, "review.phase2_write_pilot_accepted"))
    _validate_identity(_get(bundle, "posture.pinned_identity"), label="phase2 posture.pinned_identity")


def _validate_online_shadow_policy(policy: Mapping[str, Any]) -> None:
    _require_equal(
        "online shadow policy metadata.artifact_type",
        _get(policy, "metadata.artifact_type"),
        "ml_shadow_scorer_online_shadow_policy",
    )
    _require_false("online shadow policy online_shadow_execution_enabled", policy.get("online_shadow_execution_enabled"))
    _require_false("online shadow policy production_default_allowed", policy.get("production_default_allowed"))
    _require_equal(
        "online shadow policy runtime_isolation_policy.feature_flag",
        _get(policy, "runtime_isolation_policy.feature_flag"),
        FEATURE_FLAG,
    )


def _caveats(*, mode: str) -> list[str]:
    caveats = list(COMMON_PLAN_CAVEATS)
    if mode == "pre_plan":
        return caveats
    if mode == "post_plan":
        caveats.extend(PLAN_CAVEATS)
        return caveats
    if mode == "post_proof":
        caveats.extend(PROOF_CAVEATS)
        return caveats
    if mode == "post_pilot_request":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(REQUEST_CAVEATS)
        return caveats
    if mode == "post_pilot_grant":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(GRANT_CAVEATS)
        return caveats
    if mode == "post_pilot_harness":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(GRANT_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        return caveats
    if mode == "post_pilot_harness_review":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(GRANT_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        caveats.extend(HARNESS_REVIEW_CAVEATS)
        return caveats
    if mode == "post_pilot_run":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        caveats.extend(HARNESS_REVIEW_CAVEATS)
        caveats.extend(PILOT_RUN_CAVEATS)
        return caveats
    if mode == "post_pilot_review":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        caveats.extend(HARNESS_REVIEW_CAVEATS)
        caveats.extend(PILOT_RUN_CAVEATS)
        caveats.extend(PILOT_REVIEW_CAVEATS)
        return caveats
    if mode == "post_live_read_only_request":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        caveats.extend(HARNESS_REVIEW_CAVEATS)
        caveats.extend(PILOT_RUN_CAVEATS)
        caveats.extend(PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_REQUEST_CAVEATS)
        return caveats
    if mode == "post_live_read_only_grant":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        caveats.extend(HARNESS_REVIEW_CAVEATS)
        caveats.extend(PILOT_RUN_CAVEATS)
        caveats.extend(PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_GRANT_CAVEATS)
        return caveats
    if mode == "post_live_read_only_pilot_run":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(HARNESS_CAVEATS)
        caveats.extend(HARNESS_REVIEW_CAVEATS)
        caveats.extend(PILOT_RUN_CAVEATS)
        caveats.extend(PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_GRANT_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_RUN_CAVEATS)
        return caveats
    if mode == "post_live_read_only_pilot_review":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        return caveats
    if mode == "post_live_execution_request":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        return caveats
    if mode == "post_live_execution_grant":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        return caveats
    if mode == "post_live_execution_pilot_run":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        caveats.extend(LIVE_EXECUTION_PILOT_RUN_CAVEATS)
        return caveats
    if mode == "post_live_execution_pilot_review":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        caveats.extend(LIVE_EXECUTION_PILOT_REVIEW_CAVEATS)
        return caveats
    if mode == "post_flag_enablement_request":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        caveats.extend(LIVE_EXECUTION_PILOT_REVIEW_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_REQUEST_CAVEATS)
        return caveats
    if mode == "post_flag_enablement_grant":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        caveats.extend(LIVE_EXECUTION_PILOT_REVIEW_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_REQUEST_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_GRANT_CAVEATS)
        return caveats
    if mode == "post_flag_enablement_pilot_run":
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        caveats.extend(LIVE_EXECUTION_PILOT_RUN_CAVEATS)
        caveats.extend(LIVE_EXECUTION_PILOT_REVIEW_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_REQUEST_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_GRANT_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_PILOT_RUN_CAVEATS)
        return caveats
    if mode == "post_flag_enablement_pilot_review":
        stale_pilot_run_review_caveat = "Review is required before any further enablement chain."
        caveats.extend(PROOF_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_HISTORICAL_CAVEATS)
        caveats.extend(LIVE_READ_ONLY_PILOT_REVIEW_CAVEATS)
        caveats.extend(LIVE_EXECUTION_REQUEST_CAVEATS)
        caveats.extend(LIVE_EXECUTION_GRANT_CAVEATS)
        caveats.extend(
            caveat
            for caveat in LIVE_EXECUTION_PILOT_RUN_CAVEATS
            if caveat != stale_pilot_run_review_caveat
        )
        caveats.extend(LIVE_EXECUTION_PILOT_REVIEW_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_REQUEST_CAVEATS)
        caveats.extend(FLAG_ENABLEMENT_GRANT_CAVEATS)
        caveats.extend(
            caveat
            for caveat in FLAG_ENABLEMENT_PILOT_RUN_CAVEATS
            if caveat != stale_pilot_run_review_caveat
        )
        caveats.extend(FLAG_ENABLEMENT_PILOT_REVIEW_CAVEATS)
        return caveats
    if mode == "post_production_default_api_user_visible_request":
        caveats = _caveats(mode="post_flag_enablement_pilot_review")
        caveats.extend(PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_CAVEATS)
        return caveats
    if mode == "post_production_default_api_user_visible_grant":
        caveats = _caveats(mode="post_production_default_api_user_visible_request")
        caveats.extend(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_CAVEATS)
        return caveats
    if mode == "post_production_default_api_user_visible_pilot_run":
        caveats = _caveats(mode="post_production_default_api_user_visible_grant")
        caveats.extend(PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_CAVEATS)
        return caveats
    raise MLShadowScorerProductionScopedShadowBundleError(f"unknown caveat mode {mode!r}")
    return caveats


def _authorization(
    *,
    proof_allowed_by_plan: bool = False,
    pilot_authorization_requested: bool = False,
    requester: str | None = None,
    requested_at: str | None = None,
    request_notes: str | None = None,
) -> dict[str, Any]:
    authorization = {
        "prod_scoped_shadow_plan_authorization_scope": "production_scoped_shadow_plan_paperwork_only",
        "prod_scoped_shadow_proof_allowed_by_plan": proof_allowed_by_plan,
        "prod_scoped_shadow_live_execution_authorized": False,
        "prod_scoped_shadow_execution_authorized": False,
        "prod_scoped_shadow_proof_authorized": False,
        "prod_scoped_shadow_pilot_authorization_requested": pilot_authorization_requested,
        "prod_scoped_shadow_pilot_authorized": False,
        "prod_scoped_shadow_live_read_only_authorization_requested": False,
        "prod_scoped_shadow_live_read_only_authorization_granted": False,
        "prod_scoped_shadow_live_read_only_authorized": False,
        "explicitly_not_included": list(EXPLICITLY_NOT_INCLUDED),
    }
    if pilot_authorization_requested:
        authorization["request_decision"] = {
            "decision": "requested",
            "requester": requester or "Matt Maitland",
            "requested_at": requested_at,
            "request_notes": request_notes,
        }
        authorization["requested_scope"] = {
            "authorization_scope": PILOT_REQUEST_SCOPE,
            "would_enable_after_future_grant": list(PILOT_REQUEST_WOULD_ENABLE_AFTER_FUTURE_GRANT),
            "explicitly_not_included": list(PILOT_REQUEST_EXPLICITLY_NOT_INCLUDED),
        }
        authorization["explicitly_not_included"] = sorted(
            set(authorization["explicitly_not_included"]).union(PILOT_REQUEST_EXPLICITLY_NOT_INCLUDED)
        )
    return authorization


def _execution(*, proof_executed: bool = False) -> dict[str, bool]:
    return {
        "prod_scoped_shadow_plan_execution_performed": False,
        "prod_scoped_shadow_proof_executed": proof_executed,
        "prod_scoped_shadow_pilot_executed": False,
    }


def _posture(
    *,
    plan_defined: bool,
    proof_passed: bool = False,
    pilot_authorization_requested: bool = False,
    pilot_authorization_granted: bool = False,
    pilot_harness_executed: bool = False,
    pilot_harness_passed: bool = False,
    pilot_harness_reviewed: bool = False,
    pilot_harness_accepted: bool = False,
    pilot_run_executed: bool = False,
    pilot_run_passed: bool = False,
    pilot_reviewed: bool = False,
    pilot_accepted: bool = False,
) -> dict[str, Any]:
    posture = {
        "prod_scoped_shadow_plan_defined": plan_defined,
        "prod_scoped_shadow_proof_passed": proof_passed,
        "prod_scoped_shadow_pilot_executed": pilot_run_executed,
        "prod_scoped_shadow_pilot_authorization_requested": pilot_authorization_requested,
        "prod_scoped_shadow_pilot_authorized": pilot_authorization_granted,
        "missing_prod_scoped_shadow_proof": not proof_passed,
        "missing_prod_scoped_shadow_pilot_authorization": (
            pilot_authorization_requested and not pilot_authorization_granted
        ),
        "prod_scoped_shadow_proof_authorized": False,
        "production_readiness_authorization_granted": True,
        "missing_production_readiness_authorization": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    if not pilot_authorization_requested:
        posture.pop("missing_prod_scoped_shadow_pilot_authorization")
    if pilot_authorization_requested:
        posture["prod_scoped_shadow_pilot_authorization_granted"] = pilot_authorization_granted
    if pilot_harness_executed:
        posture["prod_scoped_shadow_pilot_harness_executed"] = True
        posture["prod_scoped_shadow_pilot_harness_passed"] = pilot_harness_passed
        posture["live_prod_source_reads_performed"] = False
    if pilot_harness_reviewed:
        posture["prod_scoped_shadow_pilot_harness_reviewed"] = True
        posture["prod_scoped_shadow_pilot_harness_accepted"] = pilot_harness_accepted
    if pilot_run_executed:
        posture["prod_scoped_shadow_pilot_passed"] = pilot_run_passed
        posture["prod_scoped_shadow_pilot_execution_authorized"] = True
        posture["live_prod_source_reads_performed"] = False
    if pilot_reviewed:
        posture["prod_scoped_shadow_pilot_reviewed"] = True
        posture["prod_scoped_shadow_pilot_accepted"] = pilot_accepted
    return posture


def _blockers(production_readiness_bundle: Mapping[str, Any]) -> dict[str, Any]:
    upstream = _get(production_readiness_bundle, "shadow_and_production_blockers")
    blockers = deepcopy(dict(upstream)) if isinstance(upstream, Mapping) else {}
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": True,
            "prod_scoped_shadow_proof_authorized": False,
            "blockers_changed_by_plan": [],
            "blockers_unchanged_by_plan": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    return blockers


def _forbidden_write_target_counts() -> dict[str, int]:
    return {target: 0 for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS}


def _empty_plan() -> dict[str, Any]:
    return {
        "prod_scoped_shadow_plan_defined": False,
        "plan_decision": None,
    }


def _planned_sections(*, planner: str, planned_at: str, plan_notes: str | None) -> dict[str, Any]:
    return {
        "prod_scoped_shadow_plan_defined": True,
        "plan_decision": {
            "decision": "planned",
            "planner": planner,
            "planned_at": planned_at,
            "plan_notes": plan_notes,
        },
        "prod_scoped_identity_and_rollout_boundaries": {
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "family_scope": "emerging only",
            "ranking_run_id_scope": "rank-83787b91ef and explicitly bounded successors only",
            "rollout": {
                "manual_or_scheduled_jobs_only": True,
                "no_fleet_wide_enable": True,
                "no_cron_without_explicit_later_authorization": True,
            },
            "environment": "prod-scoped read-only evaluation surface; distinct from Phase 2 non-prod isolated file tree",
            "future_artifact_root_proposal": FUTURE_ARTIFACT_ROOT,
            "explicitly_not_in_scope": [
                "production default pins",
                "API-visible tables",
                "user-visible ranking paths",
            ],
        },
        "feature_flag_iam_config_requirements": {
            "runtime_feature_flag": FEATURE_FLAG,
            "feature_flag_default": "off",
            "global_default_unchanged_by_this_plan": True,
            "prod_scoped_flag_enablement_authorized_now": False,
            "iam_config": "read-only prod input access only; no write IAM expansion; no prod config/default bridge changes",
            "config_surfaces_that_may_be_read": [
                "ranking inputs",
                "candidate pool hashes",
                "scorer metadata",
            ],
            "config_surfaces_forbidden_to_change": [
                "production default",
                "API response shaping",
                "bridge weights",
                "fleet env toggles",
            ],
        },
        "prod_read_only_input_contract": {
            "inputs_are_read_only_from_approved_prod_sources": True,
            "labels_used_for_scoring": False,
            "must_include_fields_from_online_shadow_policy": [
                "ranking_run_id",
                "family",
                "candidate_pool_work_set_sha256",
                "final_score_rank_pct",
                "audit_embedding_probability_rank_pct",
                "component coverage",
                "generated_at",
                "input hashes",
            ],
            "input_hashes_traceability_required": True,
            "incomplete_coverage_behavior": "skip entire run per policy; no partial shadow scoring",
            "forbidden_writes": [
                "ranking_runs",
                "paper_scores production paths",
                "embeddings",
                "labels",
                "scorer artifacts",
            ],
        },
        "production_default_api_user_visible_separation": {
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "results_use": "audit/monitoring only",
            "must_not_affect": [
                "user-visible ranking",
                "API responses",
                "bridge defaults",
                "production defaults",
            ],
        },
        "observability_and_slo_plan": {
            "inherits_prod_readiness_grant_slo_targets": True,
            "extends_online_shadow_policy_observability_contract": True,
            "required_signals": [
                "run status",
                "row counts",
                "error counters",
                "latency",
                "component coverage",
                "score distributions",
                "skipped runs/reasons",
                "forbidden write target counts",
                "rank displacement audit-only",
            ],
            "slo_thresholds": "plan targets for proof/pilot verification; not enforced at this plan milestone",
            "forbidden_write_target_counts_must_remain_zero": True,
        },
        "rollback_and_revocation_drill_plan": {
            "first_response": f"flag-off ({FEATURE_FLAG}=off)",
            "stop_prod_scoped_jobs_before_cleanup": True,
            "cleanup_scope": "prod-scoped pilot subdirectory only when proof/pilot exist later",
            "revoke_path": "supersede bundle authorization or deny follow-up review",
            "reverify": "production ranking/API/default unchanged with flag on versus off",
            "derived_from": [
                "production-readiness grant incident_response_and_revocation_plan",
                "Phase 2 disable drill patterns",
            ],
        },
        "proof_and_pilot_prerequisites": {
            "prerequisites_before_proof": [
                "this plan filed",
                "production-readiness grant filed",
                "Phase 2 pilot accepted",
            ],
            "proof_must_demonstrate": [
                "read-only prod input contract honored",
                "zero forbidden writes",
                "observability complete",
                "rollback drill documented and executable",
                "CI gates pass",
            ],
            "pilot_prerequisites_deferred_to_post_proof_authorization_chain": True,
            "missing_prod_scoped_shadow_proof": True,
        },
        "ci_and_live_gate_requirements": {
            "ci_must_continue_to_verify": [
                "phase2 bundle post-review",
                "production-readiness bundle post-grant",
                "production-scoped-shadow bundle post-plan",
            ],
            "future_live_prod_execution_gates": [
                "manual job only",
                "explicit authorization artifact or bundle revision",
                "forbidden-write guards",
                "observability artifact emission",
                "rollback drill evidence",
            ],
            "this_plan_commit_adds_ci_bundle_verify_only": True,
        },
    }


def assemble_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
    *,
    production_readiness_bundle_path: Path,
    phase_bundle_path: Path,
    online_shadow_policy_path: Path,
    execution_authorization_grant_path: Path | None = None,
    phase2_write_mode_plan_path: Path | None = None,
    phase2_write_mode_proof_path: Path | None = None,
    generalization_audit_gates_path: Path | None = None,
    bundle_version: str = BUNDLE_VERSION,
    repo_root: Path | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    production_readiness_path = Path(production_readiness_bundle_path).resolve()
    phase2_path = Path(phase_bundle_path).resolve()
    policy_path = Path(online_shadow_policy_path).resolve()
    production_readiness_bundle = _load_json_object(production_readiness_path)
    phase2_bundle = _load_json_object(phase2_path)
    policy = _load_json_object(policy_path)
    _validate_production_readiness_bundle(production_readiness_bundle, repo_root=root)
    _validate_phase2_bundle(phase2_bundle, repo_root=root)
    _validate_online_shadow_policy(policy)
    _validate_identity(_get(production_readiness_bundle, "metadata.pinned_identity"), label="production-readiness pinned_identity")
    _validate_identity(_get(phase2_bundle, "posture.pinned_identity"), label="phase2 posture.pinned_identity")

    paths: dict[str, Path] = {
        "production_readiness_bundle": production_readiness_path,
        "phase2_bundle": phase2_path,
        "online_shadow_policy": policy_path,
    }
    optional_paths = {
        "execution_authorization_grant": execution_authorization_grant_path,
        "phase2_write_mode_plan": phase2_write_mode_plan_path,
        "phase2_write_mode_proof": phase2_write_mode_proof_path,
        "generalization_audit_gates": generalization_audit_gates_path,
    }
    for role, path in optional_paths.items():
        if path is not None:
            paths[role] = Path(path).resolve()
    records = {role: _artifact_record(role, paths[role], repo_root=root) for role in paths}
    ordered_records = [records[role] for role in LEGACY_ARTIFACT_ROLES if role in records]
    payload = {
        "metadata": {
            "artifact_type": ARTIFACT_TYPE,
            "bundle_version": bundle_version,
            "bundle_revision": PRE_PLAN_BUNDLE_REVISION,
            "generated_at": generated_at or _now_iso_z(),
            "pinned_identity": deepcopy(PINNED_IDENTITY),
            "legacy_artifacts_index": ordered_records,
        },
        "upstream_ref": {
            "production_readiness_bundle": {
                **_ref_from_record(records["production_readiness_bundle"]),
                "bundle_revision": _get(production_readiness_bundle, "metadata.bundle_revision"),
            },
            "phase2_bundle": {
                **_ref_from_record(records["phase2_bundle"]),
                "bundle_revision": _get(phase2_bundle, "metadata.bundle_revision"),
            },
            "production_readiness_authorization_granted": _get(
                production_readiness_bundle,
                "authorization.production_readiness_authorization_granted",
            ),
            "phase2_write_pilot_accepted": _get(phase2_bundle, "review.phase2_write_pilot_accepted"),
        },
        "plan": _empty_plan(),
        "authorization": _authorization(),
        "execution": _execution(),
        "posture": _posture(plan_defined=False),
        "shadow_and_production_blockers": _blockers(production_readiness_bundle),
        "writes_performed": False,
        "runtime_writes_performed": False,
        "recommended_next_stage": PRE_PLAN_NEXT_STAGE,
        "caveats": _caveats(mode="pre_plan"),
    }
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_plan_filed=False,
    )
    return payload


def apply_production_scoped_shadow_plan(
    bundle: Mapping[str, Any],
    *,
    planner: str = "Matt Maitland",
    plan_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _require_equal("metadata.bundle_revision", _get(bundle, "metadata.bundle_revision"), PRE_PLAN_BUNDLE_REVISION)
    _require_false("plan.prod_scoped_shadow_plan_defined", _get(bundle, "plan.prod_scoped_shadow_plan_defined"))
    planned_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PLAN_BUNDLE_REVISION
    metadata["generated_at"] = planned_at
    updated["metadata"] = metadata
    updated["plan"] = _planned_sections(planner=planner, planned_at=planned_at, plan_notes=plan_notes)
    updated["authorization"] = _authorization()
    updated["execution"] = _execution()
    updated["posture"] = _posture(plan_defined=True)
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": True,
            "prod_scoped_shadow_proof_authorized": False,
            "blockers_changed_by_plan": [],
            "blockers_unchanged_by_plan": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PLAN_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_plan")
    return updated


def _compact_timestamp(timestamp: str) -> str:
    return timestamp.replace("-", "").replace(":", "").replace("+00:00", "Z")


def _default_pilot_run_id(generated_at: str) -> str:
    compact = generated_at.replace("-", "").replace(":", "")
    if compact.endswith("Z"):
        compact = compact[:-1] + "Z"
    return f"{PINNED_IDENTITY['ranking_run_id']}-{compact}"


def _default_fixture_rows(generated_at: str) -> list[dict[str, Any]]:
    return [
        {
            "ranking_run_id": PINNED_IDENTITY["ranking_run_id"],
            "family": PINNED_IDENTITY["family"],
            "candidate_pool_work_set_sha256": PINNED_IDENTITY["candidate_pool_work_set_sha256"],
            "paper_id": "fixture-prod-scope-001",
            "title": "Synthetic prod-scoped shadow fixture row 1",
            "year": 2026,
            "final_score_rank_pct": 0.93,
            "audit_embedding_probability_rank_pct": 0.91,
            "component_coverage": {"final_score": True, "audit_embedding_probability": True},
            "generated_at": generated_at,
            "input_hashes": {"source": "synthetic-fixture", "row": "fixture-prod-scope-001"},
            "rank_displacement": 0,
        },
        {
            "ranking_run_id": PINNED_IDENTITY["ranking_run_id"],
            "family": PINNED_IDENTITY["family"],
            "candidate_pool_work_set_sha256": PINNED_IDENTITY["candidate_pool_work_set_sha256"],
            "paper_id": "fixture-prod-scope-002",
            "title": "Synthetic prod-scoped shadow fixture row 2",
            "year": 2026,
            "final_score_rank_pct": 0.72,
            "audit_embedding_probability_rank_pct": 0.69,
            "component_coverage": {"final_score": True, "audit_embedding_probability": True},
            "generated_at": generated_at,
            "input_hashes": {"source": "synthetic-fixture", "row": "fixture-prod-scope-002"},
            "rank_displacement": 1,
        },
        {
            "ranking_run_id": PINNED_IDENTITY["ranking_run_id"],
            "family": PINNED_IDENTITY["family"],
            "candidate_pool_work_set_sha256": PINNED_IDENTITY["candidate_pool_work_set_sha256"],
            "paper_id": "fixture-prod-scope-003",
            "title": "Synthetic prod-scoped shadow fixture row 3",
            "year": 2026,
            "final_score_rank_pct": 0.41,
            "audit_embedding_probability_rank_pct": 0.44,
            "component_coverage": {"final_score": True, "audit_embedding_probability": True},
            "generated_at": generated_at,
            "input_hashes": {"source": "synthetic-fixture", "row": "fixture-prod-scope-003"},
            "rank_displacement": -1,
        },
    ]


def _load_fixture_rows(path: Path | None, *, generated_at: str) -> list[dict[str, Any]]:
    if path is None:
        return _default_fixture_rows(generated_at)
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise MLShadowScorerProductionScopedShadowBundleError(f"fixture input does not exist: {path}")
    if resolved.suffix.lower() == ".jsonl":
        rows = [
            json.loads(line)
            for line in resolved.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
        rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list) or not rows:
        raise MLShadowScorerProductionScopedShadowBundleError("fixture input must contain a non-empty row list")
    normalized: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"fixture row {index} must be an object")
        normalized.append(dict(row))
    return normalized


def _validate_fixture_rows(rows: list[dict[str, Any]]) -> None:
    required = {
        "ranking_run_id",
        "family",
        "candidate_pool_work_set_sha256",
        "final_score_rank_pct",
        "audit_embedding_probability_rank_pct",
        "component_coverage",
        "generated_at",
        "input_hashes",
    }
    forbidden_label_fields = {
        "relevance_label",
        "novelty_label",
        "bridge_like_label",
        "good_or_acceptable",
        "label",
        "labels",
    }
    for index, row in enumerate(rows):
        missing = sorted(required - set(row))
        if missing:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"fixture row {index} missing required read-only input fields: {', '.join(missing)}"
            )
        for field, expected in (
            ("ranking_run_id", PINNED_IDENTITY["ranking_run_id"]),
            ("family", PINNED_IDENTITY["family"]),
            ("candidate_pool_work_set_sha256", PINNED_IDENTITY["candidate_pool_work_set_sha256"]),
        ):
            _require_equal(f"fixture row {index}.{field}", row.get(field), expected)
        present_labels = sorted(forbidden_label_fields.intersection(row))
        if present_labels:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"fixture row {index} contains label fields forbidden for scoring: {', '.join(present_labels)}"
            )


def _write_json_file(path: Path, payload: Mapping[str, Any], *, repo_root: Path) -> None:
    assert_prod_scoped_write_path_allowed(path, repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl_file(path: Path, rows: list[Mapping[str, Any]], *, repo_root: Path) -> None:
    assert_prod_scoped_write_path_allowed(path, repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(row, sort_keys=True) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _file_record(path: Path, *, pilot_dir: Path, row_count: int | None = None) -> dict[str, Any]:
    return {
        "relative_path": path.resolve().relative_to(pilot_dir.resolve()).as_posix(),
        "byte_count": path.stat().st_size,
        "row_count": row_count,
        "sha256": _sha256_file(path),
        "write_target": ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
    }


def _prod_scoped_artifact_root_for_run(pilot_run_id: str) -> str:
    return f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/"


def _proof_artifact_payloads(
    *,
    pilot_run_id: str,
    generated_at: str,
    rows: list[dict[str, Any]],
    forbidden_counts: Mapping[str, int],
) -> dict[str, Any]:
    row_count = len(rows)
    input_digest = hashlib.sha256(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows).encode("utf-8")
    ).hexdigest()
    manifest = {
        "artifact_type": "ml_shadow_scorer_prod_scoped_shadow_fixture_proof_manifest",
        "generated_at": generated_at,
        "pilot_run_id": pilot_run_id,
        "proof_surface": "bounded_fixture_dry_run",
        "pinned_identity": deepcopy(PINNED_IDENTITY),
        "input_contract": {
            "rows": row_count,
            "input_sha256": input_digest,
            "labels_used_for_scoring": False,
            "inputs_read_only": True,
        },
    }
    observability = {
        "generated_at": generated_at,
        "pilot_run_id": pilot_run_id,
        "signals_emitted": list(OBSERVABILITY_SIGNALS),
        "observability_complete": True,
        "run_status": "succeeded_fixture_dry_run",
        "row_counts": {"fixture_rows": row_count, "scored_rows": row_count, "skipped_rows": 0},
        "error_counters": {"runtime_errors": 0, "write_guard_errors": 0},
        "latency": {"dry_run_elapsed_ms": 0},
        "component_coverage": {"complete": True, "rows_with_required_components": row_count},
        "score_distributions": {
            "final_score_rank_pct_min": min(row["final_score_rank_pct"] for row in rows),
            "final_score_rank_pct_max": max(row["final_score_rank_pct"] for row in rows),
            "audit_embedding_probability_rank_pct_min": min(
                row["audit_embedding_probability_rank_pct"] for row in rows
            ),
            "audit_embedding_probability_rank_pct_max": max(
                row["audit_embedding_probability_rank_pct"] for row in rows
            ),
        },
        "skipped_runs": [],
        "forbidden_write_target_counts": dict(forbidden_counts),
        "rank_displacement_audit_only": [row.get("rank_displacement", 0) for row in rows],
    }
    write_counts = {
        "write_target": ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        "local_artifact_tree_files": 4,
        "forbidden_write_target_counts": dict(forbidden_counts),
        "forbidden_write_counts_zero": True,
        "production_writes_performed": False,
        "committed_artifact_writes_performed": False,
        "runtime_writes_performed": False,
    }
    return {
        "manifest": manifest,
        "fixture_rows": rows,
        "observability": observability,
        "write_counts": write_counts,
    }


def _build_and_write_proof_artifacts(
    *,
    repo_root: Path,
    pilot_run_id: str,
    generated_at: str,
    fixture_input_path: Path | None,
    cleanup_after_proof: bool,
) -> dict[str, Any]:
    run_id = validate_pilot_run_id(pilot_run_id)
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, run_id)
    scoped_root = prod_scoped_shadow_root(repo_root)
    rows = _load_fixture_rows(fixture_input_path, generated_at=generated_at)
    _validate_fixture_rows(rows)
    forbidden_counts = _forbidden_write_target_counts()
    assert_prod_scoped_forbidden_write_target_counts(
        {ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS: 4, **forbidden_counts}
    )
    payloads = _proof_artifact_payloads(
        pilot_run_id=run_id,
        generated_at=generated_at,
        rows=rows,
        forbidden_counts=forbidden_counts,
    )
    files = {
        "manifest.json": payloads["manifest"],
        "observability.json": payloads["observability"],
        "write_counts.json": payloads["write_counts"],
    }
    for name, payload in files.items():
        _write_json_file(pilot_dir / name, payload, repo_root=repo_root)
    _write_jsonl_file(pilot_dir / "fixture_rows.jsonl", payloads["fixture_rows"], repo_root=repo_root)
    files_written = [
        _file_record(pilot_dir / "manifest.json", pilot_dir=pilot_dir),
        _file_record(pilot_dir / "fixture_rows.jsonl", pilot_dir=pilot_dir, row_count=len(rows)),
        _file_record(pilot_dir / "observability.json", pilot_dir=pilot_dir),
        _file_record(pilot_dir / "write_counts.json", pilot_dir=pilot_dir),
    ]
    cleanup_status = "retained_for_inspection"
    if cleanup_after_proof:
        if pilot_dir == scoped_root or pilot_dir.parent != scoped_root:
            raise MLShadowScorerProductionScopedShadowBundleError("cleanup target must be a direct prod-scoped child")
        shutil.rmtree(pilot_dir)
        cleanup_status = "cleaned"
    return {
        "pilot_run_id": run_id,
        "prod_scoped_artifact_root": _prod_scoped_artifact_root_for_run(run_id),
        "pilot_dir_cleanup": cleanup_status,
        "row_count": len(rows),
        "files_written": files_written,
        "forbidden_write_target_counts": forbidden_counts,
        "observability": payloads["observability"],
    }


def _proof_section(
    *,
    proof_artifacts: Mapping[str, Any],
    prover: str,
    proof_notes: str | None,
    proven_at: str,
) -> dict[str, Any]:
    return {
        "prod_scoped_shadow_proof_filed": True,
        "proof_decision": {
            "decision": "proven",
            "prover": prover,
            "proven_at": proven_at,
            "proof_notes": proof_notes,
        },
        "proof_surface": "bounded_fixture_dry_run",
        "pilot_run_id": proof_artifacts["pilot_run_id"],
        "input_contract_evidence": {
            "inputs_read_only": True,
            "labels_used_for_scoring": False,
            "input_hashes_traceable": True,
            "incomplete_coverage_skip_honored": "n/a_fixture_complete",
            "fixture_row_count": proof_artifacts["row_count"],
        },
        "write_evidence": {
            "prod_scoped_artifact_root": proof_artifacts["prod_scoped_artifact_root"],
            "local_artifact_tree_writes_performed": True,
            "production_writes_performed": False,
            "committed_artifact_writes_performed": False,
            "runtime_writes_performed": False,
            "forbidden_write_target_counts": dict(proof_artifacts["forbidden_write_target_counts"]),
            "forbidden_write_counts_zero": True,
            "files_written": deepcopy(proof_artifacts["files_written"]),
            "write_target": ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        },
        "observability_evidence": {
            "signals_emitted": list(OBSERVABILITY_SIGNALS),
            "observability_complete": True,
            "observability_summary": deepcopy(proof_artifacts["observability"]),
        },
        "rollback_drill_evidence": {
            "flag_enablement_attempted": False,
            "flag_off_verified": True,
            "production_ranking_api_default_mutation_attempted": False,
            "production_ranking_api_default_unchanged_by_construction": True,
            "no_further_writes_after_flag_off": True,
            "pilot_dir_cleanup": proof_artifacts["pilot_dir_cleanup"],
        },
        "proof_pass_fail": {
            "read_only_contract_honored": True,
            "forbidden_targets_zero": True,
            "observability_complete": True,
            "rollback_drill_executable": True,
            "overall_passed": True,
        },
    }


def apply_production_scoped_shadow_proof(
    bundle: Mapping[str, Any],
    *,
    proof_artifacts: Mapping[str, Any],
    prover: str = "Matt Maitland",
    proof_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _require_equal("metadata.bundle_revision", _get(bundle, "metadata.bundle_revision"), POST_PLAN_BUNDLE_REVISION)
    _require_true("plan.prod_scoped_shadow_plan_defined", _get(bundle, "plan.prod_scoped_shadow_plan_defined"))
    _require_true("posture.missing_prod_scoped_shadow_proof", _get(bundle, "posture.missing_prod_scoped_shadow_proof"))
    _require_false("posture.prod_scoped_shadow_proof_passed", _get(bundle, "posture.prod_scoped_shadow_proof_passed"))
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PLAN_NEXT_STAGE)
    proven_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PROOF_BUNDLE_REVISION
    metadata["generated_at"] = proven_at
    updated["metadata"] = metadata
    updated["proof"] = _proof_section(
        proof_artifacts=proof_artifacts,
        prover=prover,
        proof_notes=proof_notes,
        proven_at=proven_at,
    )
    updated["authorization"] = _authorization(proof_allowed_by_plan=True)
    updated["execution"] = _execution(proof_executed=True)
    updated["posture"] = _posture(plan_defined=True, proof_passed=True)
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": False,
            "prod_scoped_shadow_proof_authorized": False,
            "blockers_changed_by_proof": ["missing_prod_scoped_shadow_proof"],
            "blockers_unchanged_by_proof": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PROOF_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_proof")
    return updated


def apply_production_scoped_shadow_pilot_authorization_request(
    bundle: Mapping[str, Any],
    *,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _require_equal("metadata.bundle_revision", _get(bundle, "metadata.bundle_revision"), POST_PROOF_BUNDLE_REVISION)
    _require_true("proof.prod_scoped_shadow_proof_filed", _get(bundle, "proof.prod_scoped_shadow_proof_filed"))
    _require_true("posture.prod_scoped_shadow_proof_passed", _get(bundle, "posture.prod_scoped_shadow_proof_passed"))
    _require_false("posture.missing_prod_scoped_shadow_proof", _get(bundle, "posture.missing_prod_scoped_shadow_proof"))
    if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.prod_scoped_shadow_pilot_authorization_requested must be false before request"
        )
    _require_false(
        "authorization.prod_scoped_shadow_pilot_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized"),
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PROOF_NEXT_STAGE)
    requested_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PILOT_REQUEST_BUNDLE_REVISION
    metadata["generated_at"] = requested_at
    updated["metadata"] = metadata
    updated["authorization"] = _authorization(
        proof_allowed_by_plan=True,
        pilot_authorization_requested=True,
        requester=requester,
        requested_at=requested_at,
        request_notes=request_notes,
    )
    updated["execution"] = _execution(proof_executed=True)
    updated["posture"] = _posture(
        plan_defined=True,
        proof_passed=True,
        pilot_authorization_requested=True,
    )
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": False,
            "prod_scoped_shadow_pilot_authorization_requested": True,
            "missing_prod_scoped_shadow_pilot_authorization": True,
            "prod_scoped_shadow_pilot_authorized": False,
            "blockers_introduced_by_pilot_request": ["missing_prod_scoped_shadow_pilot_authorization"],
            "blockers_cleared_by_pilot_request": [],
            "blockers_unchanged_by_pilot_request": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PILOT_REQUEST_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_pilot_request")
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot request must preserve proof section")
    return updated


def _validate_pilot_grant_review(
    *,
    owner: str,
    second_reviewer: str | None,
    owner_documents_equivalent_review: str | None,
) -> None:
    if not isinstance(owner, str) or not owner.strip():
        raise MLShadowScorerProductionScopedShadowBundleError("owner must be populated")
    has_second_reviewer = isinstance(second_reviewer, str) and bool(second_reviewer.strip())
    has_equivalent_review = isinstance(owner_documents_equivalent_review, str) and bool(
        owner_documents_equivalent_review.strip()
    )
    if has_second_reviewer and second_reviewer.strip() == owner.strip():
        raise MLShadowScorerProductionScopedShadowBundleError("second_reviewer must differ from owner")
    if not has_second_reviewer and not has_equivalent_review:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "pilot grant requires second_reviewer or owner_documents_equivalent_review"
        )


def apply_production_scoped_shadow_pilot_authorization_grant(
    bundle: Mapping[str, Any],
    *,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _validate_pilot_grant_review(
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
    )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PILOT_REQUEST_BUNDLE_REVISION,
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_pilot_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized"),
    )
    if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.prod_scoped_shadow_pilot_authorization_granted must be false before grant"
        )
    _require_true(
        "posture.missing_prod_scoped_shadow_pilot_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_pilot_authorization"),
    )
    _require_true("posture.prod_scoped_shadow_proof_passed", _get(bundle, "posture.prod_scoped_shadow_proof_passed"))
    _require_false("posture.missing_prod_scoped_shadow_proof", _get(bundle, "posture.missing_prod_scoped_shadow_proof"))
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PILOT_REQUEST_NEXT_STAGE)
    granted_at = generated_at or _now_iso_z()
    resolved_review_by = review_by or expiry_date
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PILOT_GRANT_BUNDLE_REVISION
    metadata["generated_at"] = granted_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_proof_allowed_by_plan": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_proof_authorized": False,
            "prod_scoped_shadow_pilot_authorization_requested": True,
            "prod_scoped_shadow_pilot_authorization_granted": True,
            "prod_scoped_shadow_pilot_authorized": True,
            "grant_decision": {
                "decision": "granted",
                "owner": owner,
                "granted_at": granted_at,
                "expiry_date": expiry_date,
                "review_by": resolved_review_by,
                "grant_notes": grant_notes,
            },
            "granted_scope": {
                "authorization_scope": PILOT_GRANT_SCOPE,
                "authorizes_for_chain_only": list(PILOT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY),
                "explicitly_still_not_included": list(PILOT_GRANT_STILL_NOT_INCLUDED),
                "grant_time_pilot_boundaries": list(PILOT_GRANT_TIME_BOUNDARIES),
            },
        }
    )
    if second_reviewer is not None and second_reviewer.strip():
        authorization["grant_decision"]["second_reviewer"] = second_reviewer
    if owner_documents_equivalent_review is not None and owner_documents_equivalent_review.strip():
        authorization["grant_decision"]["owner_documents_equivalent_review"] = owner_documents_equivalent_review
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included", []))
        .union(PILOT_REQUEST_EXPLICITLY_NOT_INCLUDED)
        .union(PILOT_GRANT_STILL_NOT_INCLUDED)
    )
    updated["authorization"] = authorization
    updated["execution"] = _execution(proof_executed=True)
    updated["posture"] = _posture(
        plan_defined=True,
        proof_passed=True,
        pilot_authorization_requested=True,
        pilot_authorization_granted=True,
    )
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": False,
            "prod_scoped_shadow_pilot_authorization_requested": True,
            "prod_scoped_shadow_pilot_authorization_granted": True,
            "missing_prod_scoped_shadow_pilot_authorization": False,
            "prod_scoped_shadow_pilot_authorized": True,
            "blockers_cleared_by_pilot_grant": ["missing_prod_scoped_shadow_pilot_authorization"],
            "blockers_introduced_by_pilot_grant": [],
            "blockers_unchanged_by_pilot_grant": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PILOT_GRANT_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_pilot_grant")
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot grant must preserve proof section")
    return updated


def apply_production_scoped_shadow_pilot_harness(
    bundle: Mapping[str, Any],
    harness_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(harness_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("harness_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PILOT_GRANT_BUNDLE_REVISION,
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_pilot_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_pilot_authorization"),
    )
    _require_false("execution.prod_scoped_shadow_pilot_executed", _get(bundle, "execution.prod_scoped_shadow_pilot_executed"))
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PILOT_GRANT_NEXT_STAGE)
    _require_true(
        "harness_slice.pass_fail_evaluation.overall_passed",
        _get(harness_slice, "pass_fail_evaluation.overall_passed"),
    )
    _require_equal("harness_slice.pilot_surface", harness_slice.get("pilot_surface"), "bounded_fixture_pilot_harness")
    _require_false(
        "harness_slice.live_prod_source_reads_performed",
        harness_slice.get("live_prod_source_reads_performed"),
    )
    executed_at = str(harness_slice.get("executed_at") or generated_at or _now_iso_z())
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PILOT_HARNESS_BUNDLE_REVISION
    metadata["generated_at"] = executed_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_pilot_harness_allowed_by_grant": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_proof_authorized": False,
            "prod_scoped_shadow_pilot_authorized": True,
        }
    )
    updated["authorization"] = authorization
    updated["execution"] = {
        "prod_scoped_shadow_plan_execution_performed": False,
        "prod_scoped_shadow_proof_executed": True,
        "prod_scoped_shadow_pilot_harness_executed": True,
        "prod_scoped_shadow_pilot_harness_passed": True,
        "prod_scoped_shadow_pilot_executed": False,
        "pilot_harness": deepcopy(dict(harness_slice)),
    }
    updated["posture"] = _posture(
        plan_defined=True,
        proof_passed=True,
        pilot_authorization_requested=True,
        pilot_authorization_granted=True,
        pilot_harness_executed=True,
        pilot_harness_passed=True,
    )
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": False,
            "missing_prod_scoped_shadow_pilot_authorization": False,
            "prod_scoped_shadow_pilot_authorized": True,
            "prod_scoped_shadow_pilot_executed": False,
            "blockers_cleared_by_pilot_harness": [],
            "blockers_introduced_by_pilot_harness": [],
            "blockers_unchanged_by_pilot_harness": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PILOT_HARNESS_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_pilot_harness")
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot harness must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot harness must preserve proof section")
    return updated


def _validate_pilot_harness_review_slice(review_slice: Mapping[str, Any]) -> bool:
    _require_true(
        "review_slice.prod_scoped_shadow_pilot_harness_reviewed",
        review_slice.get("prod_scoped_shadow_pilot_harness_reviewed"),
    )
    accepted = review_slice.get("prod_scoped_shadow_pilot_harness_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.prod_scoped_shadow_pilot_harness_accepted must be a boolean"
        )
    decision = review_slice.get("review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice.review_decision must be an object")
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal("review_slice.review_decision.decision", decision.get("decision"), expected_decision)
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice.review_decision.checks must be an object")
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.review_decision.failed_review_checks must be a string list"
        )
    for check_name in PILOT_HARNESS_REVIEW_CHECKS:
        if check_name not in checks or not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in PILOT_HARNESS_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal("review_slice.review_decision.failed_review_checks", sorted(failed), expected_failed)
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted pilot harness review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted pilot harness review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.review_decision.{field} must be a non-empty string list"
            )
    return accepted


def apply_production_scoped_shadow_pilot_harness_review(
    bundle: Mapping[str, Any],
    review_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(review_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PILOT_HARNESS_BUNDLE_REVISION,
    )
    if isinstance(bundle.get("review"), Mapping) and _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot harness review has already been filed")
    _require_true(
        "execution.prod_scoped_shadow_pilot_harness_executed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_harness_passed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_harness_passed"),
    )
    _require_false("execution.prod_scoped_shadow_pilot_executed", _get(bundle, "execution.prod_scoped_shadow_pilot_executed"))
    _require_equal(
        "execution.pilot_harness.pilot_surface",
        _get(bundle, "execution.pilot_harness.pilot_surface"),
        PILOT_HARNESS_SURFACE,
    )
    _require_false(
        "execution.pilot_harness.live_prod_source_reads_performed",
        _get(bundle, "execution.pilot_harness.live_prod_source_reads_performed"),
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PILOT_HARNESS_NEXT_STAGE)
    _require_true(
        "authorization.prod_scoped_shadow_pilot_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_harness_allowed_by_grant",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_harness_allowed_by_grant"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    accepted = _validate_pilot_harness_review_slice(review_slice)
    decision = review_slice["review_decision"]
    reviewed_at = str(decision.get("reviewed_at") or generated_at or _now_iso_z())

    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PILOT_HARNESS_REVIEW_BUNDLE_REVISION
    metadata["generated_at"] = reviewed_at
    updated["metadata"] = metadata
    review = deepcopy(dict(review_slice))
    review_decision = deepcopy(dict(review["review_decision"]))
    review_decision["reviewed_at"] = reviewed_at
    review["review_decision"] = review_decision
    updated["review"] = review

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_pilot_harness_reviewed": True,
            "prod_scoped_shadow_pilot_harness_accepted": accepted,
            "prod_scoped_shadow_pilot_executed": False,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
            "live_prod_source_reads_performed": False,
        }
    )
    updated["posture"] = posture
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = (
        POST_PILOT_HARNESS_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted
        else POST_PILOT_HARNESS_REVIEW_REJECTED_NEXT_STAGE
    )
    updated["caveats"] = _caveats(mode="post_pilot_harness_review")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot harness review must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot harness review must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot harness review must preserve execution section")
    return updated


def _validate_pilot_run_slice(pilot_slice: Mapping[str, Any]) -> None:
    _require_equal("pilot_slice.pilot_surface", pilot_slice.get("pilot_surface"), PILOT_RUN_SURFACE)
    _require_false("pilot_slice.live_prod_source_reads_performed", pilot_slice.get("live_prod_source_reads_performed"))
    _require_true("pilot_slice.pass_fail_evaluation.overall_passed", _get(pilot_slice, "pass_fail_evaluation.overall_passed"))
    _require_equal("pilot_slice.pass_fail_evaluation.failed_checks", _get(pilot_slice, "pass_fail_evaluation.failed_checks"), [])
    _require_equal("pilot_slice.input_join_summary.joined_candidate_count", _get(pilot_slice, "input_join_summary.joined_candidate_count"), 528)
    _require_equal("pilot_slice.input_join_summary.runtime_row_count", _get(pilot_slice, "input_join_summary.runtime_row_count"), 528)
    _require_equal("pilot_slice.runtime_drill.call_order", _get(pilot_slice, "runtime_drill.call_order"), ["preflight_disabled", "pilot_enabled", "postflight_disabled"])
    _require_true("pilot_slice.runtime_drill.environment_restored", _get(pilot_slice, "runtime_drill.environment_restored"))
    _require_equal("pilot_slice.runtime_drill.pilot.status", _get(pilot_slice, "runtime_drill.pilot.status"), "succeeded_test_only")
    _require_equal("pilot_slice.runtime_drill.pilot.shadow_row_count", _get(pilot_slice, "runtime_drill.pilot.shadow_row_count"), 528)
    _require_false("pilot_slice.runtime_drill.pilot.writes_performed", _get(pilot_slice, "runtime_drill.pilot.writes_performed"))
    _require_equal("pilot_slice.runtime_drill.preflight.status", _get(pilot_slice, "runtime_drill.preflight.status"), "skipped_runtime_disabled")
    _require_equal("pilot_slice.runtime_drill.postflight.status", _get(pilot_slice, "runtime_drill.postflight.status"), "skipped_runtime_disabled")
    write_counts = pilot_slice.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("pilot_slice.write_count_verification must be an object")
    _require_true("pilot_slice.write_count_verification.local_artifact_tree_writes_performed", write_counts.get("local_artifact_tree_writes_performed"))
    _require_false("pilot_slice.write_count_verification.production_writes_performed", write_counts.get("production_writes_performed"))
    _require_false("pilot_slice.write_count_verification.committed_artifact_writes_performed", write_counts.get("committed_artifact_writes_performed"))
    _require_false("pilot_slice.write_count_verification.runtime_writes_performed", write_counts.get("runtime_writes_performed"))
    _require_true("pilot_slice.write_count_verification.forbidden_write_counts_zero", write_counts.get("forbidden_write_counts_zero"))
    _require_equal("pilot_slice.write_count_verification.file_count", write_counts.get("file_count"), 4)
    _require_equal("pilot_slice.write_count_verification.write_count", write_counts.get("write_count"), 4)
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "pilot_slice.write_count_verification.write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"pilot_slice.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(f"pilot_slice.write_count_verification.write_counts_by_isolated_target.{target}", counts.get(target), 0)
    files = pilot_slice.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot_slice.files_written must contain four files")
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal("pilot_slice.files_written names", observed_files, set(PILOT_RUN_EXPECTED_FILES))
    source_artifacts = pilot_slice.get("source_artifacts")
    if not isinstance(source_artifacts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("pilot_slice.source_artifacts must be an object")
    for role in ("learned_probability_artifact", "second_surface_generalization_audit"):
        record = source_artifacts.get(role)
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"pilot_slice.source_artifacts.{role} must be an object")
        for field in ("path", "sha256", "verification_status"):
            if not record.get(field):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"pilot_slice.source_artifacts.{role}.{field} must be populated"
                )


def apply_production_scoped_shadow_pilot_run(
    bundle: Mapping[str, Any],
    pilot_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(pilot_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("pilot_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PILOT_HARNESS_REVIEW_BUNDLE_REVISION,
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_harness_reviewed",
        _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_harness_accepted",
        _get(bundle, "review.prod_scoped_shadow_pilot_harness_accepted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized"),
    )
    _require_false("execution.prod_scoped_shadow_pilot_executed", _get(bundle, "execution.prod_scoped_shadow_pilot_executed"))
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_PILOT_HARNESS_REVIEW_ACCEPTED_NEXT_STAGE,
    )
    _validate_pilot_run_slice(pilot_slice)

    executed_at = str(pilot_slice.get("executed_at") or generated_at or _now_iso_z())
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    harness_before = deepcopy(_get(updated, "execution.pilot_harness"))
    review_before = deepcopy(updated.get("review"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PILOT_RUN_BUNDLE_REVISION
    metadata["generated_at"] = executed_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_pilot_execution_authorized": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_proof_authorized": False,
            "prod_scoped_shadow_pilot_authorized": True,
        }
    )
    updated["authorization"] = authorization

    execution = deepcopy(dict(updated.get("execution") or {}))
    execution.update(
        {
            "prod_scoped_shadow_pilot_executed": True,
            "prod_scoped_shadow_pilot_passed": True,
            "pilot_run": deepcopy(dict(pilot_slice)),
        }
    )
    updated["execution"] = execution
    updated["posture"] = _posture(
        plan_defined=True,
        proof_passed=True,
        pilot_authorization_requested=True,
        pilot_authorization_granted=True,
        pilot_harness_executed=True,
        pilot_harness_passed=True,
        pilot_harness_reviewed=True,
        pilot_harness_accepted=True,
        pilot_run_executed=True,
        pilot_run_passed=True,
    )
    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "missing_prod_scoped_shadow_proof": False,
            "missing_prod_scoped_shadow_pilot_authorization": False,
            "prod_scoped_shadow_pilot_authorized": True,
            "prod_scoped_shadow_pilot_executed": True,
            "prod_scoped_shadow_pilot_passed": True,
            "blockers_cleared_by_pilot_run": [],
            "blockers_introduced_by_pilot_run": [],
            "blockers_unchanged_by_pilot_run": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PILOT_RUN_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_pilot_run")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot run must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot run must preserve proof section")
    if _get(updated, "execution.pilot_harness") != harness_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot run must preserve pilot_harness section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot run must preserve review section")
    return updated


def _validate_pilot_review_slice(review_slice: Mapping[str, Any]) -> bool:
    _require_true(
        "review_slice.prod_scoped_shadow_pilot_reviewed",
        review_slice.get("prod_scoped_shadow_pilot_reviewed"),
    )
    accepted = review_slice.get("prod_scoped_shadow_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.prod_scoped_shadow_pilot_accepted must be a boolean"
        )
    decision = review_slice.get("pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal("review_slice.pilot_review_decision.decision", decision.get("decision"), expected_decision)
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice.pilot_review_decision.checks must be an object")
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in PILOT_RUN_REVIEW_CHECKS:
        if check_name not in checks or not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal("review_slice.pilot_review_decision.failed_review_checks", sorted(failed), expected_failed)
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.pilot_review_decision.{field} must be a non-empty string list"
            )
    return accepted


def apply_production_scoped_shadow_pilot_review(
    bundle: Mapping[str, Any],
    review_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(review_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PILOT_RUN_BUNDLE_REVISION,
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_passed"),
    )
    _require_equal("execution.pilot_run.pilot_surface", _get(bundle, "execution.pilot_run.pilot_surface"), PILOT_RUN_SURFACE)
    _require_false("execution.pilot_run.live_prod_source_reads_performed", _get(bundle, "execution.pilot_run.live_prod_source_reads_performed"))
    _require_true("review.prod_scoped_shadow_pilot_harness_reviewed", _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed"))
    _require_true("review.prod_scoped_shadow_pilot_harness_accepted", _get(bundle, "review.prod_scoped_shadow_pilot_harness_accepted"))
    if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot review has already been filed")
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PILOT_RUN_NEXT_STAGE)
    _require_false("authorization.prod_scoped_shadow_live_execution_authorized", _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"))
    _require_false("authorization.prod_scoped_shadow_execution_authorized", _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"))
    _require_true("authorization.prod_scoped_shadow_pilot_execution_authorized", _get(bundle, "authorization.prod_scoped_shadow_pilot_execution_authorized"))
    accepted = _validate_pilot_review_slice(review_slice)
    decision = review_slice["pilot_review_decision"]
    reviewed_at = str(decision.get("reviewed_at") or generated_at or _now_iso_z())

    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    harness_review_before = deepcopy(_get(updated, "review.review_decision"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PILOT_REVIEW_BUNDLE_REVISION
    metadata["generated_at"] = reviewed_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_live_read_only_authorization_requested": False,
            "prod_scoped_shadow_live_read_only_authorization_granted": False,
            "prod_scoped_shadow_live_read_only_authorized": False,
        }
    )
    updated["authorization"] = authorization

    review = deepcopy(dict(updated.get("review") or {}))
    pilot_decision = deepcopy(dict(decision))
    pilot_decision["reviewed_at"] = reviewed_at
    review.update(
        {
            "prod_scoped_shadow_pilot_reviewed": True,
            "prod_scoped_shadow_pilot_accepted": accepted,
            "pilot_review_decision": pilot_decision,
        }
    )
    updated["review"] = review

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_pilot_reviewed": True,
            "prod_scoped_shadow_pilot_accepted": accepted,
            "prod_scoped_shadow_pilot_executed": True,
            "prod_scoped_shadow_pilot_passed": True,
            "prod_scoped_shadow_pilot_execution_authorized": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
            "live_prod_source_reads_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_pilot_reviewed": True,
            "prod_scoped_shadow_pilot_accepted": accepted,
            "prod_scoped_shadow_pilot_executed": True,
            "prod_scoped_shadow_pilot_passed": True,
            "blockers_cleared_by_pilot_review": [],
            "blockers_introduced_by_pilot_review": [],
            "blockers_unchanged_by_pilot_review": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = (
        POST_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted
        else POST_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    updated["caveats"] = _caveats(mode="post_pilot_review")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot review must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot review must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot review must preserve execution section")
    if _get(updated, "review.review_decision") != harness_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot review must preserve harness review decision")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot review must preserve legacy_artifacts_index")
    return updated


def apply_production_scoped_shadow_live_read_only_authorization_request(
    bundle: Mapping[str, Any],
    *,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only authorization request has already been filed"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PILOT_REVIEW_BUNDLE_REVISION,
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_pilot_accepted"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_passed"),
    )
    _require_false(
        "execution.pilot_run.live_prod_source_reads_performed",
        _get(bundle, "execution.pilot_run.live_prod_source_reads_performed"),
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_PILOT_REVIEW_ACCEPTED_NEXT_STAGE)
    _require_false(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    for path in (
        "posture.live_prod_source_reads_performed",
        "posture.online_shadow_execution_enabled",
        "posture.production_default_allowed",
        "posture.api_web_changes_allowed",
        "posture.user_visible_ranking_changed",
        "writes_performed",
        "runtime_writes_performed",
    ):
        _require_false(path, _get(bundle, path))

    requested_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_READ_ONLY_REQUEST_BUNDLE_REVISION
    metadata["generated_at"] = requested_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_pilot_execution_authorized": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": False,
            "prod_scoped_shadow_live_read_only_authorized": False,
            "request_decision": {
                "decision": "requested",
                "requester": requester,
                "requested_at": requested_at,
                "request_notes": request_notes,
            },
            "requested_scope": {
                "authorization_scope": LIVE_READ_ONLY_REQUEST_SCOPE,
                "future_grant_would_require": list(LIVE_READ_ONLY_REQUEST_FUTURE_GRANT_REQUIREMENTS),
                "explicitly_not_included": list(LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED),
            },
        }
    )
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED)
    )
    updated["authorization"] = authorization

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_pilot_reviewed": True,
            "prod_scoped_shadow_pilot_accepted": True,
            "prod_scoped_shadow_pilot_executed": True,
            "prod_scoped_shadow_pilot_passed": True,
            "prod_scoped_shadow_pilot_execution_authorized": True,
            "live_prod_source_reads_performed": False,
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": False,
            "prod_scoped_shadow_live_read_only_authorized": False,
            "missing_prod_scoped_shadow_live_read_only_authorization": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_pilot_reviewed": True,
            "prod_scoped_shadow_pilot_accepted": True,
            "prod_scoped_shadow_pilot_executed": True,
            "prod_scoped_shadow_pilot_passed": True,
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorized": False,
            "missing_prod_scoped_shadow_live_read_only_authorization": True,
            "blockers_introduced_by_live_read_only_request": [
                "missing_prod_scoped_shadow_live_read_only_authorization"
            ],
            "blockers_cleared_by_live_read_only_request": [],
            "blockers_unchanged_by_live_read_only_request": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_read_only_request", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_LIVE_READ_ONLY_REQUEST_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_live_read_only_request")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve execution section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only request must preserve legacy_artifacts_index"
        )
    return updated


def apply_production_scoped_shadow_live_read_only_authorization_grant(
    bundle: Mapping[str, Any],
    *,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _validate_pilot_grant_review(
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
    )
    if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only authorization grant has already been filed"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_READ_ONLY_REQUEST_BUNDLE_REVISION,
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_READ_ONLY_REQUEST_NEXT_STAGE)
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "posture.missing_prod_scoped_shadow_live_read_only_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_read_only_authorization"),
    )
    _verify_live_read_only_request_section(bundle.get("authorization") or {})
    _require_true(
        "review.prod_scoped_shadow_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_pilot_accepted"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_passed"),
    )
    _require_named_flags_not_true(bundle, ("live_prod_source_reads_performed",))
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_pilot_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_pilot_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_pilot_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_pilot_execution_authorized"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ),
    )

    granted_at = generated_at or _now_iso_z()
    resolved_review_by = review_by or expiry_date
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_READ_ONLY_GRANT_BUNDLE_REVISION
    metadata["generated_at"] = granted_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "prod_scoped_shadow_live_read_only_execution_authorized": False,
            "prod_scoped_shadow_pilot_execution_authorized": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
            "live_read_only_grant_decision": {
                "decision": "granted",
                "owner": owner,
                "granted_at": granted_at,
                "expiry_date": expiry_date,
                "review_by": resolved_review_by,
                "grant_notes": grant_notes,
            },
            "live_read_only_granted_scope": {
                "authorization_scope": LIVE_READ_ONLY_GRANT_SCOPE,
                "authorizes_for_chain_only": list(LIVE_READ_ONLY_GRANT_AUTHORIZES_FOR_CHAIN_ONLY),
                "explicitly_still_not_included": list(LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED),
                "grant_time_live_read_boundaries": list(LIVE_READ_ONLY_GRANT_TIME_BOUNDARIES),
            },
        }
    )
    if second_reviewer is not None:
        authorization["live_read_only_grant_decision"]["second_reviewer"] = second_reviewer
    if owner_documents_equivalent_review is not None:
        authorization["live_read_only_grant_decision"][
            "owner_documents_equivalent_review"
        ] = owner_documents_equivalent_review
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED)
    )
    updated["authorization"] = authorization

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "missing_prod_scoped_shadow_live_read_only_authorization": False,
            "live_prod_source_reads_performed": False,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "missing_prod_scoped_shadow_live_read_only_authorization": False,
            "blockers_cleared_by_live_read_only_grant": [
                "missing_prod_scoped_shadow_live_read_only_authorization"
            ],
            "blockers_introduced_by_live_read_only_grant": [],
            "blockers_unchanged_by_live_read_only_grant": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_read_only_grant", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_LIVE_READ_ONLY_GRANT_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_live_read_only_grant")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve execution section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve live request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve live requested scope"
        )
    return updated


def _validate_live_read_only_pilot_run_slice(live_read_only_pilot_slice: Mapping[str, Any]) -> None:
    _require_equal(
        "live_read_only_pilot_slice.pilot_surface",
        live_read_only_pilot_slice.get("pilot_surface"),
        LIVE_READ_ONLY_PILOT_RUN_SURFACE,
    )
    _require_true(
        "live_read_only_pilot_slice.live_prod_source_reads_performed",
        live_read_only_pilot_slice.get("live_prod_source_reads_performed"),
    )
    _require_true(
        "live_read_only_pilot_slice.pass_fail_evaluation.overall_passed",
        _get(live_read_only_pilot_slice, "pass_fail_evaluation.overall_passed"),
    )
    _require_equal(
        "live_read_only_pilot_slice.pass_fail_evaluation.failed_checks",
        _get(live_read_only_pilot_slice, "pass_fail_evaluation.failed_checks"),
        [],
    )
    pass_fail = live_read_only_pilot_slice.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.pass_fail_evaluation must be an object"
        )
    checks = pass_fail.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.pass_fail_evaluation.checks must be an object"
        )
    for check_name in LIVE_READ_ONLY_PILOT_RUN_PASS_FAIL_CHECKS:
        _require_true(
            f"live_read_only_pilot_slice.pass_fail_evaluation.checks.{check_name}",
            checks.get(check_name),
        )
    _require_equal(
        "live_read_only_pilot_slice.input_join_summary.joined_candidate_count",
        _get(live_read_only_pilot_slice, "input_join_summary.joined_candidate_count"),
        528,
    )
    _require_equal(
        "live_read_only_pilot_slice.input_join_summary.runtime_row_count",
        _get(live_read_only_pilot_slice, "input_join_summary.runtime_row_count"),
        528,
    )
    _require_equal(
        "live_read_only_pilot_slice.runtime_drill.call_order",
        _get(live_read_only_pilot_slice, "runtime_drill.call_order"),
        ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
    )
    _require_true(
        "live_read_only_pilot_slice.runtime_drill.environment_restored",
        _get(live_read_only_pilot_slice, "runtime_drill.environment_restored"),
    )
    _require_equal(
        "live_read_only_pilot_slice.runtime_drill.pilot.status",
        _get(live_read_only_pilot_slice, "runtime_drill.pilot.status"),
        "succeeded_test_only",
    )
    _require_equal(
        "live_read_only_pilot_slice.runtime_drill.pilot.shadow_row_count",
        _get(live_read_only_pilot_slice, "runtime_drill.pilot.shadow_row_count"),
        528,
    )
    _require_false(
        "live_read_only_pilot_slice.runtime_drill.pilot.writes_performed",
        _get(live_read_only_pilot_slice, "runtime_drill.pilot.writes_performed"),
    )
    _require_equal(
        "live_read_only_pilot_slice.runtime_drill.preflight.status",
        _get(live_read_only_pilot_slice, "runtime_drill.preflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "live_read_only_pilot_slice.runtime_drill.postflight.status",
        _get(live_read_only_pilot_slice, "runtime_drill.postflight.status"),
        "skipped_runtime_disabled",
    )

    live_source_reads = live_read_only_pilot_slice.get("live_source_reads")
    if not isinstance(live_source_reads, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads must be an object"
        )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.approved_tables",
        sorted(live_source_reads.get("approved_tables") or []),
        ["embeddings", "paper_scores", "ranking_runs", "works"],
    )
    if not isinstance(live_source_reads.get("database_url_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.database_url_scope must be an object"
        )
    row_counts = live_source_reads.get("row_counts")
    if not isinstance(row_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.row_counts must be an object"
        )
    for field, expected in {
        "ranking_runs": 1,
        "paper_scores": 528,
        "works": 528,
        "embeddings": 528,
        "joined_candidate_count": 528,
    }.items():
        _require_equal(f"live_read_only_pilot_slice.live_source_reads.row_counts.{field}", row_counts.get(field), expected)
    ranking_run = live_source_reads.get("ranking_run")
    if not isinstance(ranking_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.ranking_run must be an object"
        )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.ranking_run.ranking_run_id",
        ranking_run.get("ranking_run_id"),
        PINNED_IDENTITY["ranking_run_id"],
    )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.ranking_run.ranking_version",
        ranking_run.get("ranking_version"),
        LIVE_READ_ONLY_PILOT_RUN_RANKING_VERSION,
    )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.ranking_run.status",
        ranking_run.get("status"),
        "succeeded",
    )
    derivation = live_source_reads.get("audit_embedding_probability_derivation")
    if not isinstance(derivation, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation must be an object"
        )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.source",
        derivation.get("source"),
        LIVE_READ_ONLY_PILOT_RUN_AUDIT_PROBABILITY_SOURCE,
    )
    _require_true(
        "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.live_embedding_vectors_used",
        derivation.get("live_embedding_vectors_used"),
    )
    _require_false(
        "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.frozen_candidate_score_artifact_used_as_primary_input",
        derivation.get("frozen_candidate_score_artifact_used_as_primary_input"),
    )
    scorer = derivation.get("scorer")
    if not isinstance(scorer, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.scorer must be an object"
        )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.scorer.path",
        scorer.get("path"),
        LIVE_READ_ONLY_PILOT_RUN_SCORER_PATH,
    )
    _require_equal(
        "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.scorer.scorer_version",
        scorer.get("scorer_version"),
        LIVE_READ_ONLY_PILOT_RUN_SCORER_VERSION,
    )
    _require_true(
        "live_read_only_pilot_slice.live_source_reads.audit_embedding_probability_derivation.scorer.loaded_after_confirmation",
        scorer.get("loaded_after_confirmation"),
    )
    identity = live_source_reads.get("input_identity_verification")
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.input_identity_verification must be an object"
        )
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(
            f"live_read_only_pilot_slice.live_source_reads.input_identity_verification.{field}",
            identity.get(field),
            expected,
        )
    _require_true(
        "live_read_only_pilot_slice.live_source_reads.input_identity_verification.matches_pinned_identity",
        identity.get("matches_pinned_identity"),
    )
    read_only = live_source_reads.get("read_only_assertions")
    if not isinstance(read_only, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.live_source_reads.read_only_assertions must be an object"
        )
    for field in (
        "select_only_sql_enforced",
        "approved_source_allowlist_enforced",
        "default_transaction_read_only",
        "no_write_sql_detected",
    ):
        _require_true(f"live_read_only_pilot_slice.live_source_reads.read_only_assertions.{field}", read_only.get(field))
    _require_true(
        "live_read_only_pilot_slice.live_source_reads.labels_not_used_for_scoring",
        live_source_reads.get("labels_not_used_for_scoring"),
    )
    _require_false(
        "live_read_only_pilot_slice.live_source_reads.refit_training_performed",
        live_source_reads.get("refit_training_performed"),
    )
    _require_false(
        "live_read_only_pilot_slice.live_source_reads.embedding_generation_performed",
        live_source_reads.get("embedding_generation_performed"),
    )
    _require_false(
        "live_read_only_pilot_slice.live_source_reads.label_ingest_performed",
        live_source_reads.get("label_ingest_performed"),
    )

    write_counts = live_read_only_pilot_slice.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.write_count_verification must be an object"
        )
    _require_true(
        "live_read_only_pilot_slice.write_count_verification.local_artifact_tree_writes_performed",
        write_counts.get("local_artifact_tree_writes_performed"),
    )
    _require_false(
        "live_read_only_pilot_slice.write_count_verification.production_writes_performed",
        write_counts.get("production_writes_performed"),
    )
    _require_false(
        "live_read_only_pilot_slice.write_count_verification.committed_artifact_writes_performed",
        write_counts.get("committed_artifact_writes_performed"),
    )
    _require_false(
        "live_read_only_pilot_slice.write_count_verification.runtime_writes_performed",
        write_counts.get("runtime_writes_performed"),
    )
    _require_true(
        "live_read_only_pilot_slice.write_count_verification.forbidden_write_counts_zero",
        write_counts.get("forbidden_write_counts_zero"),
    )
    _require_equal("live_read_only_pilot_slice.write_count_verification.file_count", write_counts.get("file_count"), 4)
    _require_equal("live_read_only_pilot_slice.write_count_verification.write_count", write_counts.get("write_count"), 4)
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.write_count_verification.write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"live_read_only_pilot_slice.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(
            f"live_read_only_pilot_slice.write_count_verification.write_counts_by_isolated_target.{target}",
            counts.get(target),
            0,
        )

    files = live_read_only_pilot_slice.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_read_only_pilot_slice.files_written must contain four files"
        )
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal(
        "live_read_only_pilot_slice.files_written names",
        observed_files,
        set(LIVE_READ_ONLY_PILOT_RUN_EXPECTED_FILES),
    )
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"live_read_only_pilot_slice.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"live_read_only_pilot_slice.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"live_read_only_pilot_slice.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )


def apply_production_scoped_shadow_live_read_only_pilot_run(
    bundle: Mapping[str, Any],
    live_read_only_pilot_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(live_read_only_pilot_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("live_read_only_pilot_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_READ_ONLY_GRANT_BUNDLE_REVISION,
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_READ_ONLY_GRANT_NEXT_STAGE)
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_pilot_accepted"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_pilot_passed"),
    )
    if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run has already been filed"
        )
    _require_named_flags_not_true(bundle, ("live_prod_source_reads_performed",))
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    _validate_live_read_only_pilot_run_slice(live_read_only_pilot_slice)

    executed_at = str(live_read_only_pilot_slice.get("executed_at") or generated_at or _now_iso_z())
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    harness_before = deepcopy(_get(updated, "execution.pilot_harness"))
    pilot_run_before = deepcopy(_get(updated, "execution.pilot_run"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION
    metadata["generated_at"] = executed_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
        }
    )
    updated["authorization"] = authorization

    execution = deepcopy(dict(updated.get("execution") or {}))
    execution.update(
        {
            "prod_scoped_shadow_live_read_only_pilot_executed": True,
            "prod_scoped_shadow_live_read_only_pilot_passed": True,
            "live_read_only_pilot_run": deepcopy(dict(live_read_only_pilot_slice)),
        }
    )
    updated["execution"] = execution

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_read_only_pilot_executed": True,
            "prod_scoped_shadow_live_read_only_pilot_passed": True,
            "live_prod_source_reads_performed": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_read_only_pilot_executed": True,
            "prod_scoped_shadow_live_read_only_pilot_passed": True,
            "blockers_cleared_by_live_read_only_pilot_run": [],
            "blockers_introduced_by_live_read_only_pilot_run": [],
            "blockers_unchanged_by_live_read_only_pilot_run": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_read_only_pilot_run", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_live_read_only_pilot_run")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve proof section"
        )
    if _get(updated, "execution.pilot_harness") != harness_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve pilot_harness section"
        )
    if _get(updated, "execution.pilot_run") != pilot_run_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve pilot_run section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve review section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve live request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve live requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve live grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot run must preserve live granted scope"
        )
    _require_false(
        "execution.pilot_harness.live_prod_source_reads_performed",
        _get(updated, "execution.pilot_harness.live_prod_source_reads_performed"),
    )
    _require_false(
        "execution.pilot_run.live_prod_source_reads_performed",
        _get(updated, "execution.pilot_run.live_prod_source_reads_performed"),
    )
    return updated


def _validate_live_read_only_pilot_review_slice(review_slice: Mapping[str, Any]) -> bool:
    _require_true(
        "review_slice.prod_scoped_shadow_live_read_only_pilot_reviewed",
        review_slice.get("prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    accepted = review_slice.get("prod_scoped_shadow_live_read_only_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.prod_scoped_shadow_live_read_only_pilot_accepted must be a boolean"
        )
    decision = review_slice.get("live_read_only_pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_read_only_pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal(
        "review_slice.live_read_only_pilot_review_decision.decision",
        decision.get("decision"),
        expected_decision,
    )
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.live_read_only_pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_read_only_pilot_review_decision.checks must be an object"
        )
    observed_check_names = set(checks)
    expected_check_names = set(LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS)
    if observed_check_names != expected_check_names:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_read_only_pilot_review_decision.checks must match "
            "LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS"
        )
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_read_only_pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS:
        if not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.live_read_only_pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal(
        "review_slice.live_read_only_pilot_review_decision.failed_review_checks",
        sorted(failed),
        expected_failed,
    )
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted live read-only pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted live read-only pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.live_read_only_pilot_review_decision.{field} must be a non-empty string list"
            )
    return accepted


def apply_production_scoped_shadow_live_read_only_pilot_review(
    bundle: Mapping[str, Any],
    review_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(review_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION,
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE)
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    _require_equal(
        "execution.live_read_only_pilot_run.pilot_surface",
        _get(bundle, "execution.live_read_only_pilot_run.pilot_surface"),
        LIVE_READ_ONLY_PILOT_RUN_SURFACE,
    )
    _require_true(
        "execution.live_read_only_pilot_run.live_prod_source_reads_performed",
        _get(bundle, "execution.live_read_only_pilot_run.live_prod_source_reads_performed"),
    )
    if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only pilot review has already been filed")
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    if not isinstance(_get(bundle, "authorization.live_read_only_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_grant_decision must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_read_only_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope must be an object"
        )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    accepted = _validate_live_read_only_pilot_review_slice(review_slice)
    decision = review_slice["live_read_only_pilot_review_decision"]
    reviewed_at = str(decision.get("reviewed_at") or generated_at or _now_iso_z())

    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    harness_review_before = deepcopy(_get(updated, "review.review_decision"))
    pilot_review_before = deepcopy(_get(updated, "review.pilot_review_decision"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_READ_ONLY_PILOT_REVIEW_BUNDLE_REVISION
    metadata["generated_at"] = reviewed_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
        }
    )
    updated["authorization"] = authorization

    review = deepcopy(dict(updated.get("review") or {}))
    review_decision = deepcopy(dict(decision))
    review_decision["reviewed_at"] = reviewed_at
    review.update(
        {
            "prod_scoped_shadow_live_read_only_pilot_reviewed": True,
            "prod_scoped_shadow_live_read_only_pilot_accepted": accepted,
            "live_read_only_pilot_review_decision": review_decision,
        }
    )
    updated["review"] = review

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_read_only_pilot_executed": True,
            "prod_scoped_shadow_live_read_only_pilot_passed": True,
            "prod_scoped_shadow_live_read_only_pilot_reviewed": True,
            "prod_scoped_shadow_live_read_only_pilot_accepted": accepted,
            "live_prod_source_reads_performed": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_read_only_pilot_executed": True,
            "prod_scoped_shadow_live_read_only_pilot_passed": True,
            "prod_scoped_shadow_live_read_only_pilot_reviewed": True,
            "prod_scoped_shadow_live_read_only_pilot_accepted": accepted,
            "blockers_cleared_by_live_read_only_pilot_review": [],
            "blockers_introduced_by_live_read_only_pilot_review": [],
            "blockers_unchanged_by_live_read_only_pilot_review": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_read_only_pilot_review", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = (
        POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted
        else POST_LIVE_READ_ONLY_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    updated["caveats"] = _caveats(mode="post_live_read_only_pilot_review")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve execution section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve live request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve live requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve live grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve live granted scope"
        )
    if _get(updated, "review.review_decision") != harness_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve harness review decision"
        )
    if _get(updated, "review.pilot_review_decision") != pilot_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only pilot review must preserve pilot review decision"
        )
    return updated


def apply_production_scoped_shadow_live_execution_authorization_request(
    bundle: Mapping[str, Any],
    *,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution authorization request has already been filed"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_READ_ONLY_PILOT_REVIEW_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_accepted"),
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.decision",
        _get(bundle, "review.live_read_only_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_read_only_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution authorization grant must not already be filed"
        )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    if not isinstance(_get(bundle, "authorization.request_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization.request_decision must be an object")
    if not isinstance(_get(bundle, "authorization.requested_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization.requested_scope must be an object")
    if not isinstance(_get(bundle, "authorization.live_read_only_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_grant_decision must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_read_only_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope must be an object"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _require_true(
        "posture.live_prod_source_reads_performed",
        _get(bundle, "posture.live_prod_source_reads_performed"),
    )

    requested_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_REQUEST_BUNDLE_REVISION
    metadata["generated_at"] = requested_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": False,
            "prod_scoped_shadow_live_execution_authorized": False,
            "prod_scoped_shadow_execution_authorized": False,
            "live_execution_request_decision": {
                "decision": "requested",
                "requester": requester,
                "requested_at": requested_at,
                "request_notes": request_notes,
            },
            "live_execution_requested_scope": {
                "authorization_scope": LIVE_EXECUTION_REQUEST_SCOPE,
                "future_grant_would_require": list(LIVE_EXECUTION_REQUEST_FUTURE_GRANT_REQUIREMENTS),
                "explicitly_not_included": list(LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED),
            },
        }
    )
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED)
    )
    updated["authorization"] = authorization

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_read_only_pilot_reviewed": True,
            "prod_scoped_shadow_live_read_only_pilot_accepted": True,
            "prod_scoped_shadow_live_read_only_pilot_executed": True,
            "prod_scoped_shadow_live_read_only_pilot_passed": True,
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": False,
            "prod_scoped_shadow_live_execution_authorized": False,
            "missing_prod_scoped_shadow_live_execution_authorization": True,
            "live_prod_source_reads_performed": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorized": False,
            "missing_prod_scoped_shadow_live_execution_authorization": True,
            "blockers_introduced_by_live_execution_request": [
                "missing_prod_scoped_shadow_live_execution_authorization"
            ],
            "blockers_cleared_by_live_execution_request": [],
            "blockers_unchanged_by_live_execution_request": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_execution_request", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_LIVE_EXECUTION_REQUEST_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_live_execution_request")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve review section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only granted scope"
        )
    return updated


def apply_production_scoped_shadow_live_execution_authorization_grant(
    bundle: Mapping[str, Any],
    *,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    grant_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _validate_pilot_grant_review(
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
    )
    if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution authorization grant has already been filed"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_EXECUTION_REQUEST_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_LIVE_EXECUTION_REQUEST_NEXT_STAGE,
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_accepted"),
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.decision",
        _get(bundle, "review.live_read_only_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_read_only_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    if not isinstance(_get(bundle, "authorization.request_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization.request_decision must be an object")
    if not isinstance(_get(bundle, "authorization.requested_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization.requested_scope must be an object")
    if not isinstance(_get(bundle, "authorization.live_read_only_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_grant_decision must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_read_only_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_execution_request_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_request_decision must be an object"
        )
    if not isinstance(_get(bundle, "authorization.live_execution_requested_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_requested_scope must be an object"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_execution_request_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "posture.live_prod_source_reads_performed",
        _get(bundle, "posture.live_prod_source_reads_performed"),
    )
    _require_true(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ),
    )

    granted_at = generated_at or _now_iso_z()
    resolved_review_by = review_by or expiry_date
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_GRANT_BUNDLE_REVISION
    metadata["generated_at"] = granted_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_live_read_only_authorization_requested": True,
            "prod_scoped_shadow_live_read_only_authorization_granted": True,
            "prod_scoped_shadow_live_read_only_authorized": True,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "live_execution_grant_decision": {
                "decision": "granted",
                "owner": owner,
                "granted_at": granted_at,
                "expiry_date": expiry_date,
                "review_by": resolved_review_by,
                "grant_notes": grant_notes,
            },
            "live_execution_granted_scope": {
                "authorization_scope": LIVE_EXECUTION_GRANT_SCOPE,
                "authorizes_for_chain_only": list(LIVE_EXECUTION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY),
                "explicitly_still_not_included": list(LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED),
                "grant_time_live_execution_boundaries": list(LIVE_EXECUTION_GRANT_TIME_BOUNDARIES),
            },
        }
    )
    if second_reviewer is not None:
        authorization["live_execution_grant_decision"]["second_reviewer"] = second_reviewer
    if owner_documents_equivalent_review is not None:
        authorization["live_execution_grant_decision"][
            "owner_documents_equivalent_review"
        ] = owner_documents_equivalent_review
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED)
    )
    updated["authorization"] = authorization

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "live_prod_source_reads_performed": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "blockers_cleared_by_live_execution_grant": [
                "missing_prod_scoped_shadow_live_execution_authorization"
            ],
            "blockers_introduced_by_live_execution_grant": [],
            "blockers_unchanged_by_live_execution_grant": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_execution_grant", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_LIVE_EXECUTION_GRANT_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_live_execution_grant")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve execution section")
    if "live_execution_pilot_run" in (updated.get("execution") or {}):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must not add execution.live_execution_pilot_run"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live execution requested scope"
        )
    return updated


def _validate_live_execution_pilot_run_slice(live_execution_pilot_slice: Mapping[str, Any]) -> None:
    _require_equal(
        "live_execution_pilot_slice.pilot_surface",
        live_execution_pilot_slice.get("pilot_surface"),
        LIVE_EXECUTION_PILOT_RUN_SURFACE,
    )
    _require_true(
        "live_execution_pilot_slice.live_prod_source_reads_performed",
        live_execution_pilot_slice.get("live_prod_source_reads_performed"),
    )
    pass_fail = live_execution_pilot_slice.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.pass_fail_evaluation must be an object"
        )
    _require_true(
        "live_execution_pilot_slice.pass_fail_evaluation.overall_passed",
        pass_fail.get("overall_passed"),
    )
    _require_equal(
        "live_execution_pilot_slice.pass_fail_evaluation.failed_checks",
        pass_fail.get("failed_checks"),
        [],
    )
    checks = pass_fail.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.pass_fail_evaluation.checks must be an object"
        )
    if set(checks) != set(LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.pass_fail_evaluation.checks must match "
            "LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS"
        )
    for check_name in LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS:
        _require_true(
            f"live_execution_pilot_slice.pass_fail_evaluation.checks.{check_name}",
            checks.get(check_name),
        )
    pass_fail_checks = live_execution_pilot_slice.get("pass_fail_checks")
    if isinstance(pass_fail_checks, Mapping):
        for check_name in LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS:
            _require_true(f"live_execution_pilot_slice.pass_fail_checks.{check_name}", pass_fail_checks.get(check_name))

    _require_equal(
        "live_execution_pilot_slice.input_join_summary.joined_candidate_count",
        _get(live_execution_pilot_slice, "input_join_summary.joined_candidate_count"),
        528,
    )
    _require_equal(
        "live_execution_pilot_slice.input_join_summary.runtime_row_count",
        _get(live_execution_pilot_slice, "input_join_summary.runtime_row_count"),
        528,
    )

    runtime_drill = live_execution_pilot_slice.get("runtime_drill")
    if not isinstance(runtime_drill, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.runtime_drill must be an object"
        )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.call_order",
        runtime_drill.get("call_order"),
        ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
    )
    _require_true(
        "live_execution_pilot_slice.runtime_drill.environment_restored",
        runtime_drill.get("environment_restored"),
    )
    _require_true(
        "live_execution_pilot_slice.runtime_drill.process_scoped_runtime_flag_only",
        runtime_drill.get("process_scoped_runtime_flag_only"),
    )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.preflight.status",
        _get(runtime_drill, "preflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.preflight.shadow_row_count",
        _get(runtime_drill, "preflight.shadow_row_count"),
        0,
    )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.pilot.status",
        _get(runtime_drill, "pilot.status"),
        "succeeded_test_only",
    )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.pilot.shadow_row_count",
        _get(runtime_drill, "pilot.shadow_row_count"),
        528,
    )
    _require_false(
        "live_execution_pilot_slice.runtime_drill.pilot.writes_performed",
        _get(runtime_drill, "pilot.writes_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.runtime_drill.pilot.labels_used_for_scoring",
        _get(runtime_drill, "pilot.labels_used_for_scoring"),
    )
    _require_false(
        "live_execution_pilot_slice.runtime_drill.pilot.production_default_changed",
        _get(runtime_drill, "pilot.production_default_changed"),
    )
    _require_false(
        "live_execution_pilot_slice.runtime_drill.pilot.user_visible_ranking_changed",
        _get(runtime_drill, "pilot.user_visible_ranking_changed"),
    )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.postflight.status",
        _get(runtime_drill, "postflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "live_execution_pilot_slice.runtime_drill.postflight.shadow_row_count",
        _get(runtime_drill, "postflight.shadow_row_count"),
        0,
    )

    incomplete = live_execution_pilot_slice.get("incomplete_coverage_drill")
    if not isinstance(incomplete, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.incomplete_coverage_drill must be an object"
        )
    _require_equal(
        "live_execution_pilot_slice.incomplete_coverage_drill.status",
        incomplete.get("status"),
        "skipped_incomplete_coverage",
    )
    _require_equal(
        "live_execution_pilot_slice.incomplete_coverage_drill.shadow_row_count",
        incomplete.get("shadow_row_count"),
        0,
    )
    _require_false(
        "live_execution_pilot_slice.incomplete_coverage_drill.writes_performed",
        incomplete.get("writes_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.incomplete_coverage_drill.live_prod_source_reads_performed",
        incomplete.get("live_prod_source_reads_performed"),
    )

    live_source_reads = live_execution_pilot_slice.get("live_source_reads")
    if not isinstance(live_source_reads, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_source_reads must be an object"
        )
    _require_equal(
        "live_execution_pilot_slice.live_source_reads.approved_tables",
        sorted(live_source_reads.get("approved_tables") or []),
        ["embeddings", "paper_scores", "ranking_runs", "works"],
    )
    row_counts = live_source_reads.get("row_counts")
    if not isinstance(row_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_source_reads.row_counts must be an object"
        )
    for field, expected in {
        "ranking_runs": 1,
        "paper_scores": 528,
        "works": 528,
        "embeddings": 528,
        "joined_candidate_count": 528,
    }.items():
        _require_equal(f"live_execution_pilot_slice.live_source_reads.row_counts.{field}", row_counts.get(field), expected)
    ranking_run = live_source_reads.get("ranking_run")
    if not isinstance(ranking_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_source_reads.ranking_run must be an object"
        )
    _require_equal(
        "live_execution_pilot_slice.live_source_reads.ranking_run.ranking_run_id",
        ranking_run.get("ranking_run_id"),
        PINNED_IDENTITY["ranking_run_id"],
    )
    _require_equal(
        "live_execution_pilot_slice.live_source_reads.ranking_run.ranking_version",
        ranking_run.get("ranking_version"),
        LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION,
    )
    if "fixture" in str(ranking_run.get("ranking_version", "")).lower():
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_source_reads.ranking_run.ranking_version must not be a test fixture"
        )
    identity = live_source_reads.get("input_identity_verification")
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_source_reads.input_identity_verification must be an object"
        )
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(
            f"live_execution_pilot_slice.live_source_reads.input_identity_verification.{field}",
            identity.get(field),
            expected,
        )
    _require_true(
        "live_execution_pilot_slice.live_source_reads.input_identity_verification.matches_pinned_identity",
        identity.get("matches_pinned_identity"),
    )
    read_only = live_source_reads.get("read_only_assertions")
    if not isinstance(read_only, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_source_reads.read_only_assertions must be an object"
        )
    for field in (
        "select_only_sql_enforced",
        "approved_source_allowlist_enforced",
        "default_transaction_read_only",
        "no_write_sql_detected",
    ):
        _require_true(f"live_execution_pilot_slice.live_source_reads.read_only_assertions.{field}", read_only.get(field))
    _require_true(
        "live_execution_pilot_slice.live_source_reads.labels_not_used_for_scoring",
        live_source_reads.get("labels_not_used_for_scoring"),
    )
    _require_false(
        "live_execution_pilot_slice.live_source_reads.refit_training_performed",
        live_source_reads.get("refit_training_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.live_source_reads.embedding_generation_performed",
        live_source_reads.get("embedding_generation_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.live_source_reads.label_ingest_performed",
        live_source_reads.get("label_ingest_performed"),
    )

    provenance = live_execution_pilot_slice.get("input_provenance")
    if not isinstance(provenance, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.input_provenance must be an object"
        )
    if not isinstance(provenance.get("previous_live_read_only_pilot_run_id"), str) or not provenance.get(
        "previous_live_read_only_pilot_run_id"
    ):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.input_provenance.previous_live_read_only_pilot_run_id must be populated"
        )
    _require_true(
        "live_execution_pilot_slice.input_provenance.reread_approved_production_sources",
        provenance.get("reread_approved_production_sources"),
    )
    _require_false(
        "live_execution_pilot_slice.input_provenance.fixture_ranking_version_used",
        provenance.get("fixture_ranking_version_used"),
    )
    _require_equal(
        "live_execution_pilot_slice.input_provenance.ranking_version",
        provenance.get("ranking_version"),
        LIVE_EXECUTION_PILOT_RUN_RANKING_VERSION,
    )

    scope = live_execution_pilot_slice.get("live_execution_scope")
    if not isinstance(scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.live_execution_scope must be an object"
        )
    _require_true(
        "live_execution_pilot_slice.live_execution_scope.bounded_live_execution_pilot_only",
        scope.get("bounded_live_execution_pilot_only"),
    )
    _require_false(
        "live_execution_pilot_slice.live_execution_scope.prod_scoped_shadow_execution_authorized",
        scope.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "live_execution_pilot_slice.live_execution_scope.production_default_allowed",
        scope.get("production_default_allowed"),
    )
    _require_false(
        "live_execution_pilot_slice.live_execution_scope.api_web_changes_allowed",
        scope.get("api_web_changes_allowed"),
    )
    _require_false(
        "live_execution_pilot_slice.live_execution_scope.user_visible_ranking_changed",
        scope.get("user_visible_ranking_changed"),
    )

    write_counts = live_execution_pilot_slice.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.write_count_verification must be an object"
        )
    _require_true(
        "live_execution_pilot_slice.write_count_verification.local_artifact_tree_writes_performed",
        write_counts.get("local_artifact_tree_writes_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.write_count_verification.production_writes_performed",
        write_counts.get("production_writes_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.write_count_verification.committed_artifact_writes_performed",
        write_counts.get("committed_artifact_writes_performed"),
    )
    _require_false(
        "live_execution_pilot_slice.write_count_verification.runtime_writes_performed",
        write_counts.get("runtime_writes_performed"),
    )
    _require_true(
        "live_execution_pilot_slice.write_count_verification.forbidden_write_counts_zero",
        write_counts.get("forbidden_write_counts_zero"),
    )
    _require_equal("live_execution_pilot_slice.write_count_verification.file_count", write_counts.get("file_count"), 4)
    _require_equal("live_execution_pilot_slice.write_count_verification.write_count", write_counts.get("write_count"), 4)
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.write_count_verification.write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"live_execution_pilot_slice.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(
            f"live_execution_pilot_slice.write_count_verification.write_counts_by_isolated_target.{target}",
            counts.get(target),
            0,
        )

    files = live_execution_pilot_slice.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.files_written must contain four files"
        )
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal(
        "live_execution_pilot_slice.files_written names",
        observed_files,
        set(LIVE_EXECUTION_PILOT_RUN_EXPECTED_FILES),
    )
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"live_execution_pilot_slice.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"live_execution_pilot_slice.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"live_execution_pilot_slice.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )

    observability = live_execution_pilot_slice.get("observability_summary")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_execution_pilot_slice.observability_summary must be an object"
        )
    _require_true(
        "live_execution_pilot_slice.observability_summary.observability_complete",
        observability.get("observability_complete"),
    )
    _require_true(
        "live_execution_pilot_slice.observability_summary.live_prod_source_reads_performed",
        observability.get("live_prod_source_reads_performed"),
    )
    _require_equal(
        "live_execution_pilot_slice.observability_summary.row_counts.shadow_rows",
        _get(observability, "row_counts.shadow_rows"),
        528,
    )


def apply_production_scoped_shadow_live_execution_pilot_run(
    bundle: Mapping[str, Any],
    live_execution_pilot_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(live_execution_pilot_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("live_execution_pilot_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_EXECUTION_GRANT_BUNDLE_REVISION,
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_EXECUTION_GRANT_NEXT_STAGE)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    if "live_execution_pilot_run" in execution:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution pilot run has already been filed")
    if execution.get("prod_scoped_shadow_live_execution_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.prod_scoped_shadow_live_execution_pilot_executed is already true"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    if not isinstance(execution.get("live_read_only_pilot_run"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run must be present before live execution pilot run"
        )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_accepted"),
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.decision",
        _get(bundle, "review.live_read_only_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_read_only_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", _get(bundle, "posture.live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_slice)

    executed_at = str(live_execution_pilot_slice.get("executed_at") or generated_at or _now_iso_z())
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    review_before = deepcopy(updated.get("review"))
    execution_before = deepcopy(updated.get("execution"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(updated, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(updated, "authorization.live_execution_granted_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION
    metadata["generated_at"] = executed_at
    updated["metadata"] = metadata

    execution = deepcopy(dict(updated.get("execution") or {}))
    execution.update(
        {
            "prod_scoped_shadow_live_execution_pilot_executed": True,
            "prod_scoped_shadow_live_execution_pilot_passed": True,
            "live_execution_pilot_run": deepcopy(dict(live_execution_pilot_slice)),
        }
    )
    updated["execution"] = execution

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_execution_pilot_executed": True,
            "prod_scoped_shadow_live_execution_pilot_passed": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "live_prod_source_reads_performed": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_execution_pilot_executed": True,
            "prod_scoped_shadow_live_execution_pilot_passed": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "blockers_cleared_by_live_execution_pilot_run": [],
            "blockers_introduced_by_live_execution_pilot_run": [],
            "blockers_unchanged_by_live_execution_pilot_run": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_live_execution_pilot_run", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_live_execution_pilot_run")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve proof section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve review section"
        )
    for key, before_value in execution_before.items():
        if _get(updated, f"execution.{key}") != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"live execution pilot run must preserve execution.{key}"
            )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live execution requested scope"
        )
    if _get(updated, "authorization.live_execution_grant_decision") != live_execution_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live execution grant decision"
        )
    if _get(updated, "authorization.live_execution_granted_scope") != live_execution_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot run must preserve live execution granted scope"
        )
    return updated


def _validate_live_execution_pilot_review_slice(review_slice: Mapping[str, Any]) -> bool:
    _require_true(
        "review_slice.prod_scoped_shadow_live_execution_pilot_reviewed",
        review_slice.get("prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    accepted = review_slice.get("prod_scoped_shadow_live_execution_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.prod_scoped_shadow_live_execution_pilot_accepted must be a boolean"
        )
    decision = review_slice.get("live_execution_pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_execution_pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal(
        "review_slice.live_execution_pilot_review_decision.decision",
        decision.get("decision"),
        expected_decision,
    )
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.live_execution_pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_execution_pilot_review_decision.checks must be an object"
        )
    observed_check_names = set(checks)
    expected_check_names = set(LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS)
    if observed_check_names != expected_check_names:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_execution_pilot_review_decision.checks must match "
            "LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS"
        )
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.live_execution_pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS:
        if not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.live_execution_pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal(
        "review_slice.live_execution_pilot_review_decision.failed_review_checks",
        sorted(failed),
        expected_failed,
    )
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted live execution pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted live execution pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.live_execution_pilot_review_decision.{field} must be a non-empty string list"
            )
    return accepted


def apply_production_scoped_shadow_live_execution_pilot_review(
    bundle: Mapping[str, Any],
    review_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(review_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE,
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_passed"),
    )
    live_execution_pilot_run = _get(bundle, "execution.live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review has already been filed"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_executed",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_passed",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", _get(bundle, "posture.live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false("posture.online_shadow_execution_enabled", _get(bundle, "posture.online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", _get(bundle, "posture.production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", _get(bundle, "posture.api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", _get(bundle, "posture.user_visible_ranking_changed"))
    _require_false("posture.writes_performed", _get(bundle, "posture.writes_performed"))
    _require_false("posture.runtime_writes_performed", _get(bundle, "posture.runtime_writes_performed"))

    accepted = _validate_live_execution_pilot_review_slice(review_slice)
    decision = review_slice["live_execution_pilot_review_decision"]
    reviewed_at = str(decision.get("reviewed_at") or generated_at or _now_iso_z())

    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    authorization_before = deepcopy(updated.get("authorization"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(updated, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(updated, "authorization.live_execution_granted_scope"))
    harness_review_before = deepcopy(_get(updated, "review.review_decision"))
    pilot_review_before = deepcopy(_get(updated, "review.pilot_review_decision"))
    live_read_only_pilot_review_before = deepcopy(_get(updated, "review.live_read_only_pilot_review_decision"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_PILOT_REVIEW_BUNDLE_REVISION
    metadata["generated_at"] = reviewed_at
    updated["metadata"] = metadata

    review = deepcopy(dict(updated.get("review") or {}))
    review_decision = deepcopy(dict(decision))
    review_decision["reviewed_at"] = reviewed_at
    review.update(
        {
            "prod_scoped_shadow_live_execution_pilot_reviewed": True,
            "prod_scoped_shadow_live_execution_pilot_accepted": accepted,
            "live_execution_pilot_review_decision": review_decision,
        }
    )
    updated["review"] = review

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_live_execution_pilot_reviewed": True,
            "prod_scoped_shadow_live_execution_pilot_accepted": accepted,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_live_execution_pilot_reviewed": True,
            "prod_scoped_shadow_live_execution_pilot_accepted": accepted,
            "prod_scoped_shadow_live_execution_pilot_executed": True,
            "prod_scoped_shadow_live_execution_pilot_passed": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "blockers_cleared_by_live_execution_pilot_review": [],
            "blockers_introduced_by_live_execution_pilot_review": [],
            "blockers_unchanged_by_live_execution_pilot_review": True,
        }
    )
    blockers.pop("blockers_changed_by_live_execution_pilot_review", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["recommended_next_stage"] = (
        POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted
        else POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    updated["caveats"] = _caveats(mode="post_live_execution_pilot_review")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve execution section"
        )
    if updated.get("authorization") != authorization_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve authorization section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live execution requested scope"
        )
    if _get(updated, "authorization.live_execution_grant_decision") != live_execution_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live execution grant decision"
        )
    if _get(updated, "authorization.live_execution_granted_scope") != live_execution_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live execution granted scope"
        )
    if _get(updated, "review.review_decision") != harness_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve harness review decision"
        )
    if _get(updated, "review.pilot_review_decision") != pilot_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve pilot review decision"
        )
    if _get(updated, "review.live_read_only_pilot_review_decision") != live_read_only_pilot_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution pilot review must preserve live read-only pilot review decision"
        )
    return updated


def apply_production_scoped_shadow_flag_enablement_authorization_request(
    bundle: Mapping[str, Any],
    *,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization request has already been filed"
        )
    if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization grant must not already be filed"
        )
    if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorized") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization must not already be granted"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_LIVE_EXECUTION_PILOT_REVIEW_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_accepted"),
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.decision",
        _get(bundle, "review.live_execution_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_execution_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_passed"),
    )
    live_execution_pilot_run = _get(bundle, "execution.live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _verify_live_execution_pilot_review_section(review)
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", _get(bundle, "posture.live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false("posture.online_shadow_execution_enabled", _get(bundle, "posture.online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", _get(bundle, "posture.production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", _get(bundle, "posture.api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", _get(bundle, "posture.user_visible_ranking_changed"))
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )

    requested_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(updated, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(updated, "authorization.live_execution_granted_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_REQUEST_BUNDLE_REVISION
    metadata["generated_at"] = requested_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": False,
            "prod_scoped_shadow_flag_enablement_authorized": False,
            "flag_enablement_request_decision": {
                "decision": "requested",
                "requester": requester,
                "requested_at": requested_at,
                "request_notes": request_notes,
            },
            "flag_enablement_requested_scope": {
                "authorization_scope": FLAG_ENABLEMENT_REQUEST_SCOPE,
                "runtime_feature_flag": FEATURE_FLAG,
                "future_grant_would_require": list(FLAG_ENABLEMENT_REQUEST_FUTURE_GRANT_REQUIREMENTS),
                "explicitly_not_included": list(FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED),
            },
        }
    )
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED)
    )
    updated["authorization"] = authorization

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": False,
            "prod_scoped_shadow_flag_enablement_authorized": False,
            "prod_scoped_shadow_live_execution_pilot_reviewed": True,
            "prod_scoped_shadow_live_execution_pilot_accepted": True,
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "live_prod_source_reads_performed": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "missing_prod_scoped_shadow_flag_enablement_authorization": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": False,
            "prod_scoped_shadow_flag_enablement_authorized": False,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "missing_prod_scoped_shadow_flag_enablement_authorization": True,
            "blockers_introduced_by_flag_enablement_request": [
                "missing_prod_scoped_shadow_flag_enablement_authorization"
            ],
            "blockers_cleared_by_flag_enablement_request": [],
            "blockers_unchanged_by_flag_enablement_request": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_flag_enablement_request", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_flag_enablement_request")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement request must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement request must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution requested scope"
        )
    if _get(updated, "authorization.live_execution_grant_decision") != live_execution_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution grant decision"
        )
    if _get(updated, "authorization.live_execution_granted_scope") != live_execution_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution granted scope"
        )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(updated, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(updated, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(updated, "authorization.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(updated, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    return updated


def apply_production_scoped_shadow_flag_enablement_authorization_grant(
    bundle: Mapping[str, Any],
    *,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    grant_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _validate_pilot_grant_review(
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
    )
    if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization grant has already been filed"
        )
    if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorized") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization must not already be granted"
        )
    if isinstance(_get(bundle, "authorization.flag_enablement_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_grant_decision must not already exist"
        )
    if isinstance(_get(bundle, "authorization.flag_enablement_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_granted_scope must not already exist"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_FLAG_ENABLEMENT_REQUEST_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE,
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_accepted"),
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.decision",
        _get(bundle, "review.live_execution_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_execution_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_passed"),
    )
    live_execution_pilot_run = _get(bundle, "execution.live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_execution_pilot_review_section(bundle.get("review"))
    _verify_flag_enablement_request_section(authorization)
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", _get(bundle, "posture.live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "proof.rollback_drill_evidence.flag_enablement_attempted",
        _get(bundle, "proof.rollback_drill_evidence.flag_enablement_attempted"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ),
    )

    granted_at = generated_at or _now_iso_z()
    resolved_review_by = review_by or expiry_date
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(updated, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(updated, "authorization.live_execution_granted_scope"))
    flag_enablement_request_decision_before = deepcopy(_get(updated, "authorization.flag_enablement_request_decision"))
    flag_enablement_requested_scope_before = deepcopy(_get(updated, "authorization.flag_enablement_requested_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_GRANT_BUNDLE_REVISION
    metadata["generated_at"] = granted_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "flag_enablement_grant_decision": {
                "decision": "granted",
                "owner": owner,
                "granted_at": granted_at,
                "expiry_date": expiry_date,
                "review_by": resolved_review_by,
                "grant_notes": grant_notes,
            },
            "flag_enablement_granted_scope": {
                "authorization_scope": FLAG_ENABLEMENT_GRANT_SCOPE,
                "runtime_feature_flag": FEATURE_FLAG,
                "authorizes_for_chain_only": list(FLAG_ENABLEMENT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY),
                "explicitly_still_not_included": list(FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED),
                "grant_time_flag_enablement_boundaries": list(FLAG_ENABLEMENT_GRANT_TIME_BOUNDARIES),
            },
        }
    )
    if second_reviewer is not None:
        authorization["flag_enablement_grant_decision"]["second_reviewer"] = second_reviewer
    if owner_documents_equivalent_review is not None:
        authorization["flag_enablement_grant_decision"][
            "owner_documents_equivalent_review"
        ] = owner_documents_equivalent_review
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED)
    )
    updated["authorization"] = authorization

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "missing_prod_scoped_shadow_flag_enablement_authorization": False,
            "prod_scoped_shadow_live_execution_authorization_requested": True,
            "prod_scoped_shadow_live_execution_authorization_granted": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "live_prod_source_reads_performed": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "missing_prod_scoped_shadow_flag_enablement_authorization": False,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "blockers_cleared_by_flag_enablement_grant": [
                "missing_prod_scoped_shadow_flag_enablement_authorization"
            ],
            "blockers_introduced_by_flag_enablement_grant": [],
            "blockers_unchanged_by_flag_enablement_grant": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_flag_enablement_grant", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_flag_enablement_grant")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve execution section")
    if "flag_enablement_pilot_run" in (updated.get("execution") or {}):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must not add execution.flag_enablement_pilot_run"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution requested scope"
        )
    if _get(updated, "authorization.live_execution_grant_decision") != live_execution_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution grant decision"
        )
    if _get(updated, "authorization.live_execution_granted_scope") != live_execution_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution granted scope"
        )
    if _get(updated, "authorization.flag_enablement_request_decision") != flag_enablement_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve flag enablement request decision"
        )
    if _get(updated, "authorization.flag_enablement_requested_scope") != flag_enablement_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve flag enablement requested scope"
        )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        _get(updated, "authorization.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        _get(updated, "authorization.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(updated, "authorization.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(updated, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false("posture.online_shadow_execution_enabled", _get(updated, "posture.online_shadow_execution_enabled"))
    return updated


def _validate_flag_enablement_pilot_run_slice(flag_enablement_pilot_slice: Mapping[str, Any]) -> None:
    pilot_run_id = flag_enablement_pilot_slice.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.pilot_run_id must be populated"
        )
    validate_pilot_run_id(pilot_run_id)
    if not pilot_run_id.startswith(f"{FLAG_ENABLEMENT_PILOT_RUN_ID_PREFIX}-"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.pilot_run_id must use prod-flag-enable prefix"
        )
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.pilot_run_id must not contain harness"
        )
    _require_equal(
        "flag_enablement_pilot_slice.pilot_surface",
        flag_enablement_pilot_slice.get("pilot_surface"),
        FLAG_ENABLEMENT_PILOT_RUN_SURFACE,
    )
    _require_true(
        "flag_enablement_pilot_slice.live_prod_source_reads_performed",
        flag_enablement_pilot_slice.get("live_prod_source_reads_performed"),
    )
    pass_fail = flag_enablement_pilot_slice.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.pass_fail_evaluation must be an object"
        )
    _require_true(
        "flag_enablement_pilot_slice.pass_fail_evaluation.overall_passed",
        pass_fail.get("overall_passed"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.pass_fail_evaluation.failed_checks",
        pass_fail.get("failed_checks"),
        [],
    )
    checks = pass_fail.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.pass_fail_evaluation.checks must be an object"
        )
    if set(checks) != set(FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.pass_fail_evaluation.checks must match "
            "FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS"
        )
    for check_name in FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS:
        _require_true(
            f"flag_enablement_pilot_slice.pass_fail_evaluation.checks.{check_name}",
            checks.get(check_name),
        )
    pass_fail_checks = flag_enablement_pilot_slice.get("pass_fail_checks")
    if isinstance(pass_fail_checks, Mapping):
        for check_name in FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS:
            _require_true(
                f"flag_enablement_pilot_slice.pass_fail_checks.{check_name}",
                pass_fail_checks.get(check_name),
            )

    _require_equal(
        "flag_enablement_pilot_slice.input_join_summary.joined_candidate_count",
        _get(flag_enablement_pilot_slice, "input_join_summary.joined_candidate_count"),
        528,
    )
    _require_equal(
        "flag_enablement_pilot_slice.input_join_summary.runtime_row_count",
        _get(flag_enablement_pilot_slice, "input_join_summary.runtime_row_count"),
        528,
    )

    runtime_drill = flag_enablement_pilot_slice.get("runtime_drill")
    if not isinstance(runtime_drill, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.runtime_drill must be an object"
        )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.call_order",
        runtime_drill.get("call_order"),
        ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
    )
    _require_true(
        "flag_enablement_pilot_slice.runtime_drill.environment_restored",
        runtime_drill.get("environment_restored"),
    )
    _require_true(
        "flag_enablement_pilot_slice.runtime_drill.process_scoped_runtime_flag_only",
        runtime_drill.get("process_scoped_runtime_flag_only"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.preflight.status",
        _get(runtime_drill, "preflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.preflight.shadow_row_count",
        _get(runtime_drill, "preflight.shadow_row_count"),
        0,
    )
    _require_false(
        "flag_enablement_pilot_slice.runtime_drill.preflight.runtime_enabled",
        _get(runtime_drill, "preflight.runtime_enabled"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.pilot.status",
        _get(runtime_drill, "pilot.status"),
        "succeeded_test_only",
    )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.pilot.shadow_row_count",
        _get(runtime_drill, "pilot.shadow_row_count"),
        528,
    )
    _require_true(
        "flag_enablement_pilot_slice.runtime_drill.pilot.runtime_enabled",
        _get(runtime_drill, "pilot.runtime_enabled"),
    )
    _require_false(
        "flag_enablement_pilot_slice.runtime_drill.pilot.writes_performed",
        _get(runtime_drill, "pilot.writes_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.runtime_drill.pilot.labels_used_for_scoring",
        _get(runtime_drill, "pilot.labels_used_for_scoring"),
    )
    _require_false(
        "flag_enablement_pilot_slice.runtime_drill.pilot.production_default_changed",
        _get(runtime_drill, "pilot.production_default_changed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.runtime_drill.pilot.user_visible_ranking_changed",
        _get(runtime_drill, "pilot.user_visible_ranking_changed"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.postflight.status",
        _get(runtime_drill, "postflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "flag_enablement_pilot_slice.runtime_drill.postflight.shadow_row_count",
        _get(runtime_drill, "postflight.shadow_row_count"),
        0,
    )
    _require_false(
        "flag_enablement_pilot_slice.runtime_drill.postflight.runtime_enabled",
        _get(runtime_drill, "postflight.runtime_enabled"),
    )

    incomplete = flag_enablement_pilot_slice.get("incomplete_coverage_drill")
    if not isinstance(incomplete, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.incomplete_coverage_drill must be an object"
        )
    _require_equal(
        "flag_enablement_pilot_slice.incomplete_coverage_drill.status",
        incomplete.get("status"),
        "skipped_incomplete_coverage",
    )
    _require_equal(
        "flag_enablement_pilot_slice.incomplete_coverage_drill.shadow_row_count",
        incomplete.get("shadow_row_count"),
        0,
    )
    _require_false(
        "flag_enablement_pilot_slice.incomplete_coverage_drill.writes_performed",
        incomplete.get("writes_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.incomplete_coverage_drill.live_prod_source_reads_performed",
        incomplete.get("live_prod_source_reads_performed"),
    )

    live_source_reads = flag_enablement_pilot_slice.get("live_source_reads")
    if not isinstance(live_source_reads, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.live_source_reads must be an object"
        )
    _require_equal(
        "flag_enablement_pilot_slice.live_source_reads.approved_tables",
        sorted(live_source_reads.get("approved_tables") or []),
        ["embeddings", "paper_scores", "ranking_runs", "works"],
    )
    row_counts = live_source_reads.get("row_counts")
    if not isinstance(row_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.live_source_reads.row_counts must be an object"
        )
    for field, expected in {
        "ranking_runs": 1,
        "paper_scores": 528,
        "works": 528,
        "embeddings": 528,
        "joined_candidate_count": 528,
    }.items():
        _require_equal(
            f"flag_enablement_pilot_slice.live_source_reads.row_counts.{field}",
            row_counts.get(field),
            expected,
        )
    ranking_run = live_source_reads.get("ranking_run")
    if not isinstance(ranking_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.live_source_reads.ranking_run must be an object"
        )
    _require_equal(
        "flag_enablement_pilot_slice.live_source_reads.ranking_run.ranking_run_id",
        ranking_run.get("ranking_run_id"),
        PINNED_IDENTITY["ranking_run_id"],
    )
    _require_equal(
        "flag_enablement_pilot_slice.live_source_reads.ranking_run.ranking_version",
        ranking_run.get("ranking_version"),
        FLAG_ENABLEMENT_PILOT_RUN_RANKING_VERSION,
    )
    if "fixture" in str(ranking_run.get("ranking_version", "")).lower():
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.live_source_reads.ranking_run.ranking_version must not be a test fixture"
        )
    identity = live_source_reads.get("input_identity_verification")
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.live_source_reads.input_identity_verification must be an object"
        )
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(
            f"flag_enablement_pilot_slice.live_source_reads.input_identity_verification.{field}",
            identity.get(field),
            expected,
        )
    _require_true(
        "flag_enablement_pilot_slice.live_source_reads.input_identity_verification.matches_pinned_identity",
        identity.get("matches_pinned_identity"),
    )
    read_only = live_source_reads.get("read_only_assertions")
    if not isinstance(read_only, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.live_source_reads.read_only_assertions must be an object"
        )
    for field in (
        "select_only_sql_enforced",
        "approved_source_allowlist_enforced",
        "default_transaction_read_only",
        "no_write_sql_detected",
    ):
        _require_true(
            f"flag_enablement_pilot_slice.live_source_reads.read_only_assertions.{field}",
            read_only.get(field),
        )
    _require_true(
        "flag_enablement_pilot_slice.live_source_reads.labels_not_used_for_scoring",
        live_source_reads.get("labels_not_used_for_scoring"),
    )
    _require_false(
        "flag_enablement_pilot_slice.live_source_reads.refit_training_performed",
        live_source_reads.get("refit_training_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.live_source_reads.embedding_generation_performed",
        live_source_reads.get("embedding_generation_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.live_source_reads.label_ingest_performed",
        live_source_reads.get("label_ingest_performed"),
    )

    provenance = flag_enablement_pilot_slice.get("input_provenance")
    if not isinstance(provenance, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.input_provenance must be an object"
        )
    if not isinstance(provenance.get("previous_live_read_only_pilot_run_id"), str) or not provenance.get(
        "previous_live_read_only_pilot_run_id"
    ):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.input_provenance.previous_live_read_only_pilot_run_id must be populated"
        )
    if not isinstance(provenance.get("previous_live_execution_pilot_run_id"), str) or not provenance.get(
        "previous_live_execution_pilot_run_id"
    ):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.input_provenance.previous_live_execution_pilot_run_id must be populated"
        )
    _require_true(
        "flag_enablement_pilot_slice.input_provenance.reread_approved_production_sources",
        provenance.get("reread_approved_production_sources"),
    )
    _require_false(
        "flag_enablement_pilot_slice.input_provenance.fixture_ranking_version_used",
        provenance.get("fixture_ranking_version_used"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.input_provenance.ranking_version",
        provenance.get("ranking_version"),
        FLAG_ENABLEMENT_PILOT_RUN_RANKING_VERSION,
    )

    scope = flag_enablement_pilot_slice.get("flag_enablement_scope")
    if not isinstance(scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.flag_enablement_scope must be an object"
        )
    if not isinstance(scope.get("flag_enablement_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.flag_enablement_scope.flag_enablement_grant_decision must be an object"
        )
    if not isinstance(scope.get("flag_enablement_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.flag_enablement_scope.flag_enablement_granted_scope must be an object"
        )
    _require_true(
        "flag_enablement_pilot_slice.flag_enablement_scope.bounded_flag_enablement_pilot_only",
        scope.get("bounded_flag_enablement_pilot_only"),
    )
    _require_true(
        "flag_enablement_pilot_slice.flag_enablement_scope.prod_scoped_shadow_flag_enablement_authorized",
        scope.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "flag_enablement_pilot_slice.flag_enablement_scope.prod_scoped_shadow_execution_authorized",
        scope.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "flag_enablement_pilot_slice.flag_enablement_scope.production_default_allowed",
        scope.get("production_default_allowed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.flag_enablement_scope.api_web_changes_allowed",
        scope.get("api_web_changes_allowed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.flag_enablement_scope.user_visible_ranking_changed",
        scope.get("user_visible_ranking_changed"),
    )

    write_counts = flag_enablement_pilot_slice.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.write_count_verification must be an object"
        )
    _require_true(
        "flag_enablement_pilot_slice.write_count_verification.local_artifact_tree_writes_performed",
        write_counts.get("local_artifact_tree_writes_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.write_count_verification.production_writes_performed",
        write_counts.get("production_writes_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.write_count_verification.committed_artifact_writes_performed",
        write_counts.get("committed_artifact_writes_performed"),
    )
    _require_false(
        "flag_enablement_pilot_slice.write_count_verification.runtime_writes_performed",
        write_counts.get("runtime_writes_performed"),
    )
    _require_true(
        "flag_enablement_pilot_slice.write_count_verification.forbidden_write_counts_zero",
        write_counts.get("forbidden_write_counts_zero"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.write_count_verification.file_count",
        write_counts.get("file_count"),
        4,
    )
    _require_equal(
        "flag_enablement_pilot_slice.write_count_verification.write_count",
        write_counts.get("write_count"),
        4,
    )
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.write_count_verification.write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"flag_enablement_pilot_slice.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(
            f"flag_enablement_pilot_slice.write_count_verification.write_counts_by_isolated_target.{target}",
            counts.get(target),
            0,
        )

    files = flag_enablement_pilot_slice.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.files_written must contain four files"
        )
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal(
        "flag_enablement_pilot_slice.files_written names",
        observed_files,
        set(FLAG_ENABLEMENT_PILOT_RUN_EXPECTED_FILES),
    )
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"flag_enablement_pilot_slice.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"flag_enablement_pilot_slice.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"flag_enablement_pilot_slice.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )

    observability = flag_enablement_pilot_slice.get("observability_summary")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag_enablement_pilot_slice.observability_summary must be an object"
        )
    _require_true(
        "flag_enablement_pilot_slice.observability_summary.observability_complete",
        observability.get("observability_complete"),
    )
    _require_true(
        "flag_enablement_pilot_slice.observability_summary.live_prod_source_reads_performed",
        observability.get("live_prod_source_reads_performed"),
    )
    _require_equal(
        "flag_enablement_pilot_slice.observability_summary.row_counts.shadow_rows",
        _get(observability, "row_counts.shadow_rows"),
        528,
    )


def apply_production_scoped_shadow_flag_enablement_pilot_run(
    bundle: Mapping[str, Any],
    flag_enablement_pilot_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(flag_enablement_pilot_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("flag_enablement_pilot_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_FLAG_ENABLEMENT_GRANT_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE,
    )
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    if "flag_enablement_pilot_run" in execution:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement pilot run has already been filed")
    if execution.get("prod_scoped_shadow_flag_enablement_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.prod_scoped_shadow_flag_enablement_pilot_executed is already true"
        )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_live_read_only_pilot_review_section(bundle.get("review"))
    _verify_live_execution_pilot_review_section(bundle.get("review"))
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    live_read_only_pilot_run = execution.get("live_read_only_pilot_run")
    if not isinstance(live_read_only_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run must be present before flag enablement pilot run"
        )
    _validate_live_read_only_pilot_run_slice(live_read_only_pilot_run)
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        execution.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        execution.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    live_execution_pilot_run = execution.get("live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be present before flag enablement pilot run"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    _require_false(
        "proof.rollback_drill_evidence.flag_enablement_attempted",
        _get(bundle, "proof.rollback_drill_evidence.flag_enablement_attempted"),
    )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_requested",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_granted",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorized",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", _get(bundle, "posture.live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false("posture.online_shadow_execution_enabled", _get(bundle, "posture.online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", _get(bundle, "posture.production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", _get(bundle, "posture.api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", _get(bundle, "posture.user_visible_ranking_changed"))
    _require_false("posture.writes_performed", _get(bundle, "posture.writes_performed"))
    _require_false("posture.runtime_writes_performed", _get(bundle, "posture.runtime_writes_performed"))
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_requested",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_granted",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorized",
        blockers.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_flag_enablement_authorization",
        blockers.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )
    _validate_flag_enablement_pilot_run_slice(flag_enablement_pilot_slice)

    executed_at = str(flag_enablement_pilot_slice.get("executed_at") or generated_at or _now_iso_z())
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    review_before = deepcopy(updated.get("review"))
    execution_before = deepcopy(updated.get("execution"))
    authorization_before = deepcopy(updated.get("authorization"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION
    metadata["generated_at"] = executed_at
    updated["metadata"] = metadata

    execution = deepcopy(dict(updated.get("execution") or {}))
    execution.update(
        {
            "prod_scoped_shadow_flag_enablement_pilot_executed": True,
            "prod_scoped_shadow_flag_enablement_pilot_passed": True,
            "flag_enablement_pilot_run": deepcopy(dict(flag_enablement_pilot_slice)),
        }
    )
    updated["execution"] = execution

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_flag_enablement_pilot_executed": True,
            "prod_scoped_shadow_flag_enablement_pilot_passed": True,
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "live_prod_source_reads_performed": True,
            "missing_prod_scoped_shadow_flag_enablement_authorization": False,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(
        {
            "prod_scoped_shadow_flag_enablement_pilot_executed": True,
            "prod_scoped_shadow_flag_enablement_pilot_passed": True,
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "prod_scoped_shadow_live_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "missing_prod_scoped_shadow_flag_enablement_authorization": False,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "blockers_cleared_by_flag_enablement_pilot_run": [],
            "blockers_introduced_by_flag_enablement_pilot_run": [],
            "blockers_unchanged_by_flag_enablement_pilot_run": True,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
        }
    )
    blockers.pop("blockers_changed_by_flag_enablement_pilot_run", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_flag_enablement_pilot_run")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve proof section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve review section"
        )
    if updated.get("authorization") != authorization_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve authorization section"
        )
    for key, before_value in execution_before.items():
        if _get(updated, f"execution.{key}") != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"flag enablement pilot run must preserve execution.{key}"
            )
    if _get(updated, "execution.live_read_only_pilot_run") != _get(execution_before, "live_read_only_pilot_run"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve execution.live_read_only_pilot_run"
        )
    if _get(updated, "execution.live_execution_pilot_run") != _get(execution_before, "live_execution_pilot_run"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve execution.live_execution_pilot_run"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot run must preserve legacy_artifacts_index"
        )
    return updated


def _without_flag_enablement_pilot_run_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_GRANT_BUNDLE_REVISION
    payload["metadata"] = metadata
    execution = deepcopy(dict(payload.get("execution") or {}))
    execution.pop("prod_scoped_shadow_flag_enablement_pilot_executed", None)
    execution.pop("prod_scoped_shadow_flag_enablement_pilot_passed", None)
    execution.pop("flag_enablement_pilot_run", None)
    payload["execution"] = execution
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_flag_enablement_pilot_executed", None)
    posture.pop("prod_scoped_shadow_flag_enablement_pilot_passed", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_flag_enablement_pilot_executed", None)
    blockers.pop("prod_scoped_shadow_flag_enablement_pilot_passed", None)
    blockers["prod_scoped_shadow_execution_authorized"] = False
    blockers.pop("blockers_cleared_by_flag_enablement_pilot_run", None)
    blockers.pop("blockers_introduced_by_flag_enablement_pilot_run", None)
    blockers.pop("blockers_unchanged_by_flag_enablement_pilot_run", None)
    blockers.pop("blockers_changed_by_flag_enablement_pilot_run", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_flag_enablement_grant")
    return payload


def _verify_flag_enablement_pilot_run_section(
    flag_enablement_pilot_run: Any,
    *,
    repo_root: Path,
    verify_local_files: bool = True,
) -> None:
    if not isinstance(flag_enablement_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run must be an object"
        )
    _validate_flag_enablement_pilot_run_slice(flag_enablement_pilot_run)
    pilot_run_id = flag_enablement_pilot_run.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run.pilot_run_id must be populated"
        )
    validate_pilot_run_id(pilot_run_id)
    if not pilot_run_id.startswith(f"{FLAG_ENABLEMENT_PILOT_RUN_ID_PREFIX}-"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run.pilot_run_id must use prod-flag-enable prefix"
        )
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run.pilot_run_id must not contain harness"
        )
    pilot_dir_ref = flag_enablement_pilot_run.get("pilot_run_directory")
    if not isinstance(pilot_dir_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run.pilot_run_directory must be an object"
        )
    _require_equal(
        "execution.flag_enablement_pilot_run.pilot_run_directory.root_path",
        pilot_dir_ref.get("root_path"),
        PROD_SCOPED_SHADOW_ROOT,
    )
    _require_equal(
        "execution.flag_enablement_pilot_run.pilot_run_directory.relative_path",
        pilot_dir_ref.get("relative_path"),
        f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/",
    )
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    for index, record in enumerate(flag_enablement_pilot_run["files_written"]):
        local_file = pilot_dir / str(record["relative_path"])
        if verify_local_files and local_file.exists():
            _require_equal(
                f"execution.flag_enablement_pilot_run.files_written[{index}].sha256",
                _sha256_file(local_file),
                record.get("sha256"),
            )
            _require_equal(
                f"execution.flag_enablement_pilot_run.files_written[{index}].byte_count",
                local_file.stat().st_size,
                record.get("byte_count"),
            )


def _verify_flag_enablement_pilot_run_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_flag_enablement_pilot_run_payload(bundle),
        repo_root=repo_root,
        expect_flag_enablement_grant_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_live_read_only_pilot_review_section(bundle.get("review"))
    _verify_live_execution_pilot_review_section(bundle.get("review"))
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_passed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _verify_live_read_only_pilot_run_section(
        execution.get("live_read_only_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_live_execution_pilot_run_section(
        execution.get("live_execution_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_flag_enablement_pilot_run_section(
        execution.get("flag_enablement_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_executed",
        posture.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_passed",
        posture.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorized",
        posture.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        posture.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        posture.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    allowed_true_paths = {
        "posture.live_prod_source_reads_performed",
        "execution.live_read_only_pilot_run.live_prod_source_reads_performed",
        "execution.live_read_only_pilot_run.observability_summary.live_prod_source_reads_performed",
        "execution.live_execution_pilot_run.live_prod_source_reads_performed",
        "execution.live_execution_pilot_run.observability_summary.live_prod_source_reads_performed",
        "execution.flag_enablement_pilot_run.live_prod_source_reads_performed",
        "execution.flag_enablement_pilot_run.observability_summary.live_prod_source_reads_performed",
    }
    observed_true_paths: set[str] = set()
    for path, value in _iter_named_field_values(bundle, "live_prod_source_reads_performed"):
        if path in allowed_true_paths:
            _require_true(path, value)
            observed_true_paths.add(path)
        else:
            _require_false(path, value)
    missing_true = sorted(allowed_true_paths - observed_true_paths)
    if missing_true:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_prod_source_reads_performed missing true paths: " + ", ".join(missing_true)
        )
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_pilot_executed",
        blockers.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_pilot_passed",
        blockers.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorized",
        blockers.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_flag_enablement_authorization",
        blockers.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_flag_enablement_pilot_run",
        blockers.get("blockers_cleared_by_flag_enablement_pilot_run"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_flag_enablement_pilot_run",
        blockers.get("blockers_introduced_by_flag_enablement_pilot_run"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_flag_enablement_pilot_run",
        blockers.get("blockers_unchanged_by_flag_enablement_pilot_run"),
    )
    if "blockers_changed_by_flag_enablement_pilot_run" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_flag_enablement_pilot_run must not be used"
        )
    _require_false("shadow_and_production_blockers.online_shadow_execution_enabled", blockers.get("online_shadow_execution_enabled"))
    _require_false("shadow_and_production_blockers.production_default_allowed", blockers.get("production_default_allowed"))
    _require_false("shadow_and_production_blockers.api_web_changes_allowed", blockers.get("api_web_changes_allowed"))
    _require_false(
        "shadow_and_production_blockers.user_visible_ranking_changed",
        blockers.get("user_visible_ranking_changed"),
    )
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE,
    )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_flag_enablement_pilot_run"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_flag_enablement_pilot_run caveats missing {caveat!r}"
            )
    for item in FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_flag_enablement_pilot_run",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _validate_flag_enablement_pilot_review_slice(review_slice: Mapping[str, Any]) -> bool:
    _require_true(
        "review_slice.prod_scoped_shadow_flag_enablement_pilot_reviewed",
        review_slice.get("prod_scoped_shadow_flag_enablement_pilot_reviewed"),
    )
    accepted = review_slice.get("prod_scoped_shadow_flag_enablement_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.prod_scoped_shadow_flag_enablement_pilot_accepted must be a boolean"
        )
    decision = review_slice.get("flag_enablement_pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.flag_enablement_pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal(
        "review_slice.flag_enablement_pilot_review_decision.decision",
        decision.get("decision"),
        expected_decision,
    )
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.flag_enablement_pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.flag_enablement_pilot_review_decision.checks must be an object"
        )
    observed_check_names = set(checks)
    expected_check_names = set(FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS)
    if observed_check_names != expected_check_names:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.flag_enablement_pilot_review_decision.checks must match "
            "FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS"
        )
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review_slice.flag_enablement_pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS:
        if not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.flag_enablement_pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal(
        "review_slice.flag_enablement_pilot_review_decision.failed_review_checks",
        sorted(failed),
        expected_failed,
    )
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted flag enablement pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted flag enablement pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review_slice.flag_enablement_pilot_review_decision.{field} must be a non-empty string list"
            )
    return accepted


def apply_production_scoped_shadow_flag_enablement_pilot_review(
    bundle: Mapping[str, Any],
    review_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(review_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review_slice must be an object")
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE,
    )
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_passed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    flag_enablement_pilot_run = execution.get("flag_enablement_pilot_run")
    if not isinstance(flag_enablement_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run must be an object"
        )
    _validate_flag_enablement_pilot_run_slice(flag_enablement_pilot_run)
    pass_fail = flag_enablement_pilot_run.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run.pass_fail_evaluation must be an object"
        )
    _require_true(
        "execution.flag_enablement_pilot_run.pass_fail_evaluation.overall_passed",
        pass_fail.get("overall_passed"),
    )
    _require_equal(
        "execution.flag_enablement_pilot_run.pass_fail_evaluation.failed_checks",
        pass_fail.get("failed_checks"),
        [],
    )
    live_read_only_pilot_run = execution.get("live_read_only_pilot_run")
    if not isinstance(live_read_only_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run must be an object"
        )
    _validate_live_read_only_pilot_run_slice(live_read_only_pilot_run)
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    live_execution_pilot_run = execution.get("live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        execution.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        execution.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    if review.get("prod_scoped_shadow_flag_enablement_pilot_reviewed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review has already been filed"
        )
    if review.get("flag_enablement_pilot_review_decision") is not None:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision must not already exist"
        )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_reviewed",
        review.get("prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_accepted",
        review.get("prod_scoped_shadow_live_read_only_pilot_accepted"),
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.decision",
        _get(review, "live_read_only_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.failed_review_checks",
        _get(review, "live_read_only_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_reviewed",
        review.get("prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_accepted",
        review.get("prod_scoped_shadow_live_execution_pilot_accepted"),
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.decision",
        _get(review, "live_execution_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.failed_review_checks",
        _get(review, "live_execution_pilot_review_decision.failed_review_checks"),
        [],
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_read_only_pilot_review_section(review)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_execution_pilot_review_section(review)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_requested",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_granted",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorized",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_executed",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_passed",
        _get(bundle, "posture.prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", _get(bundle, "posture.live_prod_source_reads_performed"))
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        _get(bundle, "posture.prod_scoped_shadow_execution_authorized"),
    )
    _require_false("posture.online_shadow_execution_enabled", _get(bundle, "posture.online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", _get(bundle, "posture.production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", _get(bundle, "posture.api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", _get(bundle, "posture.user_visible_ranking_changed"))
    _require_false("posture.writes_performed", _get(bundle, "posture.writes_performed"))
    _require_false("posture.runtime_writes_performed", _get(bundle, "posture.runtime_writes_performed"))
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_requested",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_granted",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorized",
        blockers.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_flag_enablement_authorization",
        blockers.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "proof.rollback_drill_evidence.flag_enablement_attempted",
        _get(bundle, "proof.rollback_drill_evidence.flag_enablement_attempted"),
    )

    accepted = _validate_flag_enablement_pilot_review_slice(review_slice)
    decision = review_slice["flag_enablement_pilot_review_decision"]
    reviewed_at = str(decision.get("reviewed_at") or generated_at or _now_iso_z())

    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    authorization_before = deepcopy(updated.get("authorization"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    harness_review_before = deepcopy(_get(updated, "review.review_decision"))
    pilot_review_before = deepcopy(_get(updated, "review.pilot_review_decision"))
    live_read_only_pilot_review_before = deepcopy(_get(updated, "review.live_read_only_pilot_review_decision"))
    live_execution_pilot_review_before = deepcopy(_get(updated, "review.live_execution_pilot_review_decision"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_PILOT_REVIEW_BUNDLE_REVISION
    metadata["generated_at"] = reviewed_at
    updated["metadata"] = metadata

    review_updated = deepcopy(dict(updated.get("review") or {}))
    review_decision = deepcopy(dict(decision))
    review_decision["reviewed_at"] = reviewed_at
    review_updated.update(
        {
            "prod_scoped_shadow_flag_enablement_pilot_reviewed": True,
            "prod_scoped_shadow_flag_enablement_pilot_accepted": accepted,
            "flag_enablement_pilot_review_decision": review_decision,
        }
    )
    updated["review"] = review_updated

    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(
        {
            "prod_scoped_shadow_flag_enablement_pilot_reviewed": True,
            "prod_scoped_shadow_flag_enablement_pilot_accepted": accepted,
            "prod_scoped_shadow_flag_enablement_pilot_executed": True,
            "prod_scoped_shadow_flag_enablement_pilot_passed": True,
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "missing_prod_scoped_shadow_flag_enablement_authorization": False,
            "prod_scoped_shadow_live_execution_authorized": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "prod_scoped_shadow_live_read_only_execution_authorized": True,
            "prod_scoped_shadow_execution_authorized": False,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "writes_performed": False,
            "runtime_writes_performed": False,
        }
    )
    updated["posture"] = posture

    blockers_updated = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers_updated.update(
        {
            "prod_scoped_shadow_flag_enablement_pilot_reviewed": True,
            "prod_scoped_shadow_flag_enablement_pilot_accepted": accepted,
            "prod_scoped_shadow_flag_enablement_pilot_executed": True,
            "prod_scoped_shadow_flag_enablement_pilot_passed": True,
            "prod_scoped_shadow_flag_enablement_authorization_requested": True,
            "prod_scoped_shadow_flag_enablement_authorization_granted": True,
            "prod_scoped_shadow_flag_enablement_authorized": True,
            "missing_prod_scoped_shadow_flag_enablement_authorization": False,
            "prod_scoped_shadow_live_execution_authorized": True,
            "missing_prod_scoped_shadow_live_execution_authorization": False,
            "prod_scoped_shadow_execution_authorized": False,
            "online_shadow_execution_enabled": False,
            "production_default_allowed": False,
            "api_web_changes_allowed": False,
            "user_visible_ranking_changed": False,
            "blockers_cleared_by_flag_enablement_pilot_review": [],
            "blockers_introduced_by_flag_enablement_pilot_review": [],
            "blockers_unchanged_by_flag_enablement_pilot_review": True,
        }
    )
    blockers_updated.pop("blockers_changed_by_flag_enablement_pilot_review", None)
    updated["shadow_and_production_blockers"] = blockers_updated
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = (
        POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted
        else POST_FLAG_ENABLEMENT_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    updated["caveats"] = _caveats(mode="post_flag_enablement_pilot_review")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve execution section"
        )
    if updated.get("authorization") != authorization_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve authorization section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve legacy_artifacts_index"
        )
    if _get(updated, "review.review_decision") != harness_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve harness review decision"
        )
    if _get(updated, "review.pilot_review_decision") != pilot_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve pilot review decision"
        )
    if _get(updated, "review.live_read_only_pilot_review_decision") != live_read_only_pilot_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve live read-only pilot review decision"
        )
    if _get(updated, "review.live_execution_pilot_review_decision") != live_execution_pilot_review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement pilot review must preserve live execution pilot review decision"
        )
    return updated


def apply_production_scoped_shadow_production_default_api_user_visible_authorization_request(
    bundle: Mapping[str, Any],
    *,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if _get(
        bundle,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
    ) is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization request has already been filed"
        )
    if _get(
        bundle,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
    ) is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization grant must not already be filed"
        )
    if _get(bundle, "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization must not already be granted"
        )
    if isinstance(_get(bundle, "authorization.production_default_api_user_visible_request_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_request_decision must not already exist"
        )
    if isinstance(_get(bundle, "authorization.production_default_api_user_visible_requested_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_requested_scope must not already exist"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_FLAG_ENABLEMENT_PILOT_REVIEW_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE,
    )
    _require_true(
        "review.prod_scoped_shadow_flag_enablement_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_flag_enablement_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_accepted"),
    )
    _require_equal(
        "review.flag_enablement_pilot_review_decision.decision",
        _get(bundle, "review.flag_enablement_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.flag_enablement_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.flag_enablement_pilot_review_decision.failed_review_checks"),
        [],
    )
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_passed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    live_read_only_pilot_run = execution.get("live_read_only_pilot_run")
    if not isinstance(live_read_only_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run must be an object"
        )
    _validate_live_read_only_pilot_run_slice(live_read_only_pilot_run)
    live_execution_pilot_run = execution.get("live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    flag_enablement_pilot_run = execution.get("flag_enablement_pilot_run")
    if not isinstance(flag_enablement_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run must be an object"
        )
    _validate_flag_enablement_pilot_run_slice(flag_enablement_pilot_run)

    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_execution_pilot_review_section(bundle.get("review"))
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_flag_enablement_pilot_review_section(bundle.get("review"))
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )

    for prefix, section in (
        ("posture", bundle.get("posture")),
        ("shadow_and_production_blockers", bundle.get("shadow_and_production_blockers")),
    ):
        if not isinstance(section, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"{prefix} must be an object")
        for field in (
            "prod_scoped_shadow_flag_enablement_pilot_executed",
            "prod_scoped_shadow_flag_enablement_pilot_passed",
            "prod_scoped_shadow_flag_enablement_pilot_reviewed",
            "prod_scoped_shadow_flag_enablement_pilot_accepted",
            "prod_scoped_shadow_flag_enablement_authorization_requested",
            "prod_scoped_shadow_flag_enablement_authorization_granted",
            "prod_scoped_shadow_flag_enablement_authorized",
            "prod_scoped_shadow_live_execution_authorized",
            "prod_scoped_shadow_live_read_only_execution_authorized",
        ):
            _require_true(f"{prefix}.{field}", section.get(field))
        if prefix == "posture" or "live_prod_source_reads_performed" in section:
            _require_true(
                f"{prefix}.live_prod_source_reads_performed",
                section.get("live_prod_source_reads_performed"),
            )
        for field in (
            "missing_prod_scoped_shadow_flag_enablement_authorization",
            "missing_prod_scoped_shadow_live_execution_authorization",
            "prod_scoped_shadow_execution_authorized",
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ):
            if field in section:
                _require_false(f"{prefix}.{field}", section.get(field))
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(bundle, "plan.production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )

    requested_at = generated_at or _now_iso_z()
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(updated, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(updated, "authorization.live_execution_granted_scope"))
    flag_enablement_request_decision_before = deepcopy(_get(updated, "authorization.flag_enablement_request_decision"))
    flag_enablement_requested_scope_before = deepcopy(_get(updated, "authorization.flag_enablement_requested_scope"))
    flag_enablement_grant_decision_before = deepcopy(_get(updated, "authorization.flag_enablement_grant_decision"))
    flag_enablement_granted_scope_before = deepcopy(_get(updated, "authorization.flag_enablement_granted_scope"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_BUNDLE_REVISION
    metadata["generated_at"] = requested_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_production_default_api_user_visible_authorization_requested": True,
            "prod_scoped_shadow_production_default_api_user_visible_authorization_granted": False,
            "prod_scoped_shadow_production_default_api_user_visible_authorized": False,
            "production_default_api_user_visible_request_decision": {
                "decision": "requested",
                "requester": requester,
                "requested_at": requested_at,
                "request_notes": request_notes,
            },
            "production_default_api_user_visible_requested_scope": {
                "authorization_scope": PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE,
                "future_grant_would_require": list(
                    PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_FUTURE_GRANT_REQUIREMENTS
                ),
                "explicitly_not_included": list(
                    PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED
                ),
            },
        }
    )
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(
            PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED
        )
    )
    updated["authorization"] = authorization

    new_posture_fields = {
        "prod_scoped_shadow_production_default_api_user_visible_authorization_requested": True,
        "prod_scoped_shadow_production_default_api_user_visible_authorization_granted": False,
        "prod_scoped_shadow_production_default_api_user_visible_authorized": False,
        "missing_prod_scoped_shadow_production_default_api_user_visible_authorization": True,
        "prod_scoped_shadow_flag_enablement_pilot_reviewed": True,
        "prod_scoped_shadow_flag_enablement_pilot_accepted": True,
        "prod_scoped_shadow_flag_enablement_authorized": True,
        "prod_scoped_shadow_live_execution_authorized": True,
        "prod_scoped_shadow_live_read_only_execution_authorized": True,
        "missing_prod_scoped_shadow_flag_enablement_authorization": False,
        "missing_prod_scoped_shadow_live_execution_authorization": False,
        "live_prod_source_reads_performed": True,
        "prod_scoped_shadow_execution_authorized": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(new_posture_fields)
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(new_posture_fields)
    blockers.update(
        {
            "blockers_introduced_by_production_default_api_user_visible_request": [
                "missing_prod_scoped_shadow_production_default_api_user_visible_authorization"
            ],
            "blockers_cleared_by_production_default_api_user_visible_request": [],
            "blockers_unchanged_by_production_default_api_user_visible_request": True,
        }
    )
    blockers.pop("blockers_changed_by_production_default_api_user_visible_request", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_production_default_api_user_visible_request")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve review section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve legacy_artifacts_index"
        )
    for path, before_value, label in (
        ("authorization.grant_decision", pilot_grant_decision_before, "pilot grant decision"),
        ("authorization.granted_scope", pilot_granted_scope_before, "pilot granted scope"),
        ("authorization.request_decision", live_request_decision_before, "live read-only request decision"),
        ("authorization.requested_scope", live_requested_scope_before, "live read-only requested scope"),
        (
            "authorization.live_read_only_grant_decision",
            live_read_only_grant_decision_before,
            "live read-only grant decision",
        ),
        (
            "authorization.live_read_only_granted_scope",
            live_read_only_granted_scope_before,
            "live read-only granted scope",
        ),
        (
            "authorization.live_execution_request_decision",
            live_execution_request_decision_before,
            "live execution request decision",
        ),
        (
            "authorization.live_execution_requested_scope",
            live_execution_requested_scope_before,
            "live execution requested scope",
        ),
        (
            "authorization.live_execution_grant_decision",
            live_execution_grant_decision_before,
            "live execution grant decision",
        ),
        (
            "authorization.live_execution_granted_scope",
            live_execution_granted_scope_before,
            "live execution granted scope",
        ),
        (
            "authorization.flag_enablement_request_decision",
            flag_enablement_request_decision_before,
            "flag enablement request decision",
        ),
        (
            "authorization.flag_enablement_requested_scope",
            flag_enablement_requested_scope_before,
            "flag enablement requested scope",
        ),
        (
            "authorization.flag_enablement_grant_decision",
            flag_enablement_grant_decision_before,
            "flag enablement grant decision",
        ),
        (
            "authorization.flag_enablement_granted_scope",
            flag_enablement_granted_scope_before,
            "flag enablement granted scope",
        ),
    ):
        if _get(updated, path) != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production default/API/user-visible request must preserve {label}"
            )
    return updated


def apply_production_scoped_shadow_production_default_api_user_visible_authorization_grant(
    bundle: Mapping[str, Any],
    *,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    grant_notes: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    _validate_pilot_grant_review(
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
    )
    if _get(
        bundle,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
    ) is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization grant has already been filed"
        )
    if _get(bundle, "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization must not already be granted"
        )
    if isinstance(_get(bundle, "authorization.production_default_api_user_visible_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_grant_decision must not already exist"
        )
    if isinstance(_get(bundle, "authorization.production_default_api_user_visible_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_granted_scope must not already exist"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE,
    )
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
        _get(bundle, "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        _get(bundle, "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
        _get(bundle, "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized"),
    )
    _require_true(
        "posture.missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
        _get(bundle, "posture.missing_prod_scoped_shadow_production_default_api_user_visible_authorization"),
    )
    _require_true(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
        _get(
            bundle,
            "shadow_and_production_blockers.missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
        ),
    )
    _require_true(
        "review.prod_scoped_shadow_flag_enablement_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_flag_enablement_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_accepted"),
    )
    _require_equal(
        "review.flag_enablement_pilot_review_decision.decision",
        _get(bundle, "review.flag_enablement_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.flag_enablement_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.flag_enablement_pilot_review_decision.failed_review_checks"),
        [],
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_reviewed",
        _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_accepted",
        _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_accepted"),
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.decision",
        _get(bundle, "review.live_execution_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_execution_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_execution_pilot_review_decision.failed_review_checks"),
        [],
    )
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    flag_enablement_pilot_run = execution.get("flag_enablement_pilot_run")
    if not isinstance(flag_enablement_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run must be an object"
        )
    _validate_flag_enablement_pilot_run_slice(flag_enablement_pilot_run)
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_execution_pilot_review_section(review)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_flag_enablement_pilot_review_section(review)
    _verify_production_default_api_user_visible_request_section(authorization)
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(bundle, "plan.production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )

    granted_at = generated_at or _now_iso_z()
    resolved_review_by = review_by or expiry_date
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    execution_before = deepcopy(updated.get("execution"))
    review_before = deepcopy(updated.get("review"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(updated, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(updated, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(updated, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(updated, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(updated, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(updated, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(updated, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(updated, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(updated, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(updated, "authorization.live_execution_granted_scope"))
    flag_enablement_request_decision_before = deepcopy(_get(updated, "authorization.flag_enablement_request_decision"))
    flag_enablement_requested_scope_before = deepcopy(_get(updated, "authorization.flag_enablement_requested_scope"))
    flag_enablement_grant_decision_before = deepcopy(_get(updated, "authorization.flag_enablement_grant_decision"))
    flag_enablement_granted_scope_before = deepcopy(_get(updated, "authorization.flag_enablement_granted_scope"))
    production_default_request_decision_before = deepcopy(
        _get(updated, "authorization.production_default_api_user_visible_request_decision")
    )
    production_default_requested_scope_before = deepcopy(
        _get(updated, "authorization.production_default_api_user_visible_requested_scope")
    )

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_BUNDLE_REVISION
    metadata["generated_at"] = granted_at
    updated["metadata"] = metadata

    authorization = deepcopy(dict(updated.get("authorization") or {}))
    authorization.update(
        {
            "prod_scoped_shadow_production_default_api_user_visible_authorization_requested": True,
            "prod_scoped_shadow_production_default_api_user_visible_authorization_granted": True,
            "prod_scoped_shadow_production_default_api_user_visible_authorized": True,
            "production_default_api_user_visible_grant_decision": {
                "decision": "granted",
                "owner": owner,
                "granted_at": granted_at,
                "expiry_date": expiry_date,
                "review_by": resolved_review_by,
                "grant_notes": grant_notes,
            },
            "production_default_api_user_visible_granted_scope": {
                "authorization_scope": PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE,
                "authorizes_for_chain_only": list(
                    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_AUTHORIZES_FOR_CHAIN_ONLY
                ),
                "explicitly_still_not_included": list(
                    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED
                ),
                "grant_time_production_default_api_user_visible_boundaries": list(
                    PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_TIME_BOUNDARIES
                ),
            },
        }
    )
    if second_reviewer is not None:
        authorization["production_default_api_user_visible_grant_decision"]["second_reviewer"] = second_reviewer
    if owner_documents_equivalent_review is not None:
        authorization["production_default_api_user_visible_grant_decision"][
            "owner_documents_equivalent_review"
        ] = owner_documents_equivalent_review
    authorization["explicitly_not_included"] = sorted(
        set(authorization.get("explicitly_not_included") or []).union(
            PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED
        )
    )
    updated["authorization"] = authorization

    grant_posture_fields = {
        "prod_scoped_shadow_production_default_api_user_visible_authorization_requested": True,
        "prod_scoped_shadow_production_default_api_user_visible_authorization_granted": True,
        "prod_scoped_shadow_production_default_api_user_visible_authorized": True,
        "missing_prod_scoped_shadow_production_default_api_user_visible_authorization": False,
        "prod_scoped_shadow_flag_enablement_authorized": True,
        "missing_prod_scoped_shadow_flag_enablement_authorization": False,
        "prod_scoped_shadow_live_execution_authorized": True,
        "missing_prod_scoped_shadow_live_execution_authorization": False,
        "prod_scoped_shadow_live_read_only_execution_authorized": True,
        "live_prod_source_reads_performed": True,
        "prod_scoped_shadow_execution_authorized": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(grant_posture_fields)
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(grant_posture_fields)
    blockers.update(
        {
            "blockers_cleared_by_production_default_api_user_visible_grant": [
                "missing_prod_scoped_shadow_production_default_api_user_visible_authorization"
            ],
            "blockers_introduced_by_production_default_api_user_visible_grant": [],
            "blockers_unchanged_by_production_default_api_user_visible_grant": True,
        }
    )
    blockers.pop("blockers_changed_by_production_default_api_user_visible_grant", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_production_default_api_user_visible_grant")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve execution section"
        )
    if "production_default_api_user_visible_pilot_run" in (updated.get("execution") or {}):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must not add execution.production_default_api_user_visible_pilot_run"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve review section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve legacy_artifacts_index"
        )
    for path, before_value, label in (
        ("authorization.grant_decision", pilot_grant_decision_before, "pilot grant decision"),
        ("authorization.granted_scope", pilot_granted_scope_before, "pilot granted scope"),
        ("authorization.request_decision", live_request_decision_before, "live read-only request decision"),
        ("authorization.requested_scope", live_requested_scope_before, "live read-only requested scope"),
        (
            "authorization.live_read_only_grant_decision",
            live_read_only_grant_decision_before,
            "live read-only grant decision",
        ),
        (
            "authorization.live_read_only_granted_scope",
            live_read_only_granted_scope_before,
            "live read-only granted scope",
        ),
        (
            "authorization.live_execution_request_decision",
            live_execution_request_decision_before,
            "live execution request decision",
        ),
        (
            "authorization.live_execution_requested_scope",
            live_execution_requested_scope_before,
            "live execution requested scope",
        ),
        (
            "authorization.live_execution_grant_decision",
            live_execution_grant_decision_before,
            "live execution grant decision",
        ),
        (
            "authorization.live_execution_granted_scope",
            live_execution_granted_scope_before,
            "live execution granted scope",
        ),
        (
            "authorization.flag_enablement_request_decision",
            flag_enablement_request_decision_before,
            "flag enablement request decision",
        ),
        (
            "authorization.flag_enablement_requested_scope",
            flag_enablement_requested_scope_before,
            "flag enablement requested scope",
        ),
        (
            "authorization.flag_enablement_grant_decision",
            flag_enablement_grant_decision_before,
            "flag enablement grant decision",
        ),
        (
            "authorization.flag_enablement_granted_scope",
            flag_enablement_granted_scope_before,
            "flag enablement granted scope",
        ),
        (
            "authorization.production_default_api_user_visible_request_decision",
            production_default_request_decision_before,
            "production default/API/user-visible request decision",
        ),
        (
            "authorization.production_default_api_user_visible_requested_scope",
            production_default_requested_scope_before,
            "production default/API/user-visible requested scope",
        ),
    ):
        if _get(updated, path) != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production default/API/user-visible grant must preserve {label}"
            )
    return updated


def _validate_production_default_api_user_visible_probe(
    probe: Mapping[str, Any],
    *,
    label: str,
) -> None:
    _require_equal(f"{label}.api_surface", probe.get("api_surface"), "/api/v1/recommendations/ranked")
    _require_equal(f"{label}.family", probe.get("family"), "emerging")
    _require_equal(f"{label}.ranking_run_id", probe.get("ranking_run_id"), "rank-83787b91ef")
    _require_equal(f"{label}.limit", probe.get("limit"), 20)
    for field in (
        "in_process_audit_only_probe",
        "current_output_read_only_probe_performed",
        "would_be_shadow_scorer_output_built",
    ):
        _require_true(f"{label}.{field}", probe.get(field))
    for field in (
        "bridge_surface_included",
        "user_visible_response_emitted_to_users",
        "production_default_changed",
        "api_web_changed",
        "user_visible_ranking_changed",
        "paper_scores_written",
        "ranking_runs_written",
        "production_config_written",
        "http_server_bound",
        "outbound_api_route_called",
    ):
        _require_false(f"{label}.{field}", probe.get(field))
    if "current_output_top_20" in probe:
        current_output = probe.get("current_output_top_20")
        if not isinstance(current_output, list) or len(current_output) > 20:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"{label}.current_output_top_20 must be a list with at most 20 rows"
            )
    if "would_be_shadow_scorer_output_top_20" in probe:
        would_be_output = probe.get("would_be_shadow_scorer_output_top_20")
        if not isinstance(would_be_output, list) or len(would_be_output) > 20:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"{label}.would_be_shadow_scorer_output_top_20 must be a list with at most 20 rows"
            )


def _validate_production_default_api_user_visible_pilot_run_slice(
    production_default_api_user_visible_pilot_slice: Mapping[str, Any],
) -> None:
    pilot_run_id = production_default_api_user_visible_pilot_slice.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.pilot_run_id must be populated"
        )
    validate_pilot_run_id(pilot_run_id)
    if not pilot_run_id.startswith(f"{PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_ID_PREFIX}-"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.pilot_run_id must use prod-output prefix"
        )
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.pilot_run_id must not contain harness"
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.pilot_surface",
        production_default_api_user_visible_pilot_slice.get("pilot_surface"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_SURFACE,
    )
    _require_true(
        "production_default_api_user_visible_pilot_slice.live_prod_source_reads_performed",
        production_default_api_user_visible_pilot_slice.get("live_prod_source_reads_performed"),
    )

    pass_fail = production_default_api_user_visible_pilot_slice.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.pass_fail_evaluation must be an object"
        )
    _require_true(
        "production_default_api_user_visible_pilot_slice.pass_fail_evaluation.overall_passed",
        pass_fail.get("overall_passed"),
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.pass_fail_evaluation.failed_checks",
        pass_fail.get("failed_checks"),
        [],
    )
    checks = pass_fail.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.pass_fail_evaluation.checks must be an object"
        )
    if set(checks) != set(PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.pass_fail_evaluation.checks must match "
            "PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS"
        )
    for check_name in PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS:
        _require_true(
            f"production_default_api_user_visible_pilot_slice.pass_fail_evaluation.checks.{check_name}",
            checks.get(check_name),
        )
    pass_fail_checks = production_default_api_user_visible_pilot_slice.get("pass_fail_checks")
    if isinstance(pass_fail_checks, Mapping):
        for check_name in PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS:
            _require_true(
                f"production_default_api_user_visible_pilot_slice.pass_fail_checks.{check_name}",
                pass_fail_checks.get(check_name),
            )

    _require_equal(
        "production_default_api_user_visible_pilot_slice.input_join_summary.joined_candidate_count",
        _get(production_default_api_user_visible_pilot_slice, "input_join_summary.joined_candidate_count"),
        528,
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.input_join_summary.runtime_row_count",
        _get(production_default_api_user_visible_pilot_slice, "input_join_summary.runtime_row_count"),
        528,
    )

    runtime_drill = production_default_api_user_visible_pilot_slice.get("runtime_drill")
    if not isinstance(runtime_drill, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.runtime_drill must be an object"
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.call_order",
        runtime_drill.get("call_order"),
        ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
    )
    for field in ("environment_restored", "process_scoped_runtime_flag_only"):
        _require_true(
            f"production_default_api_user_visible_pilot_slice.runtime_drill.{field}",
            runtime_drill.get(field),
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.preflight.status",
        _get(runtime_drill, "preflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.preflight.shadow_row_count",
        _get(runtime_drill, "preflight.shadow_row_count"),
        0,
    )
    _require_false(
        "production_default_api_user_visible_pilot_slice.runtime_drill.preflight.runtime_enabled",
        _get(runtime_drill, "preflight.runtime_enabled"),
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.pilot.status",
        _get(runtime_drill, "pilot.status"),
        "succeeded_test_only",
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.pilot.shadow_row_count",
        _get(runtime_drill, "pilot.shadow_row_count"),
        528,
    )
    _require_true(
        "production_default_api_user_visible_pilot_slice.runtime_drill.pilot.runtime_enabled",
        _get(runtime_drill, "pilot.runtime_enabled"),
    )
    for field in (
        "writes_performed",
        "labels_used_for_scoring",
        "production_default_changed",
        "user_visible_ranking_changed",
    ):
        _require_false(
            f"production_default_api_user_visible_pilot_slice.runtime_drill.pilot.{field}",
            _get(runtime_drill, f"pilot.{field}"),
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.postflight.status",
        _get(runtime_drill, "postflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.runtime_drill.postflight.shadow_row_count",
        _get(runtime_drill, "postflight.shadow_row_count"),
        0,
    )
    _require_false(
        "production_default_api_user_visible_pilot_slice.runtime_drill.postflight.runtime_enabled",
        _get(runtime_drill, "postflight.runtime_enabled"),
    )

    incomplete = production_default_api_user_visible_pilot_slice.get("incomplete_coverage_drill")
    if not isinstance(incomplete, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.incomplete_coverage_drill must be an object"
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.incomplete_coverage_drill.status",
        incomplete.get("status"),
        "skipped_incomplete_coverage",
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.incomplete_coverage_drill.shadow_row_count",
        incomplete.get("shadow_row_count"),
        0,
    )
    _require_false(
        "production_default_api_user_visible_pilot_slice.incomplete_coverage_drill.writes_performed",
        incomplete.get("writes_performed"),
    )
    _require_false(
        "production_default_api_user_visible_pilot_slice.incomplete_coverage_drill.live_prod_source_reads_performed",
        incomplete.get("live_prod_source_reads_performed"),
    )

    live_source_reads = production_default_api_user_visible_pilot_slice.get("live_source_reads")
    if not isinstance(live_source_reads, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.live_source_reads must be an object"
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.live_source_reads.approved_tables",
        sorted(live_source_reads.get("approved_tables") or []),
        ["embeddings", "paper_scores", "ranking_runs", "works"],
    )
    row_counts = live_source_reads.get("row_counts")
    if not isinstance(row_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.live_source_reads.row_counts must be an object"
        )
    for field, expected in {
        "ranking_runs": 1,
        "paper_scores": 528,
        "works": 528,
        "embeddings": 528,
        "joined_candidate_count": 528,
    }.items():
        _require_equal(
            f"production_default_api_user_visible_pilot_slice.live_source_reads.row_counts.{field}",
            row_counts.get(field),
            expected,
        )
    ranking_run = live_source_reads.get("ranking_run")
    if not isinstance(ranking_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.live_source_reads.ranking_run must be an object"
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.live_source_reads.ranking_run.ranking_run_id",
        ranking_run.get("ranking_run_id"),
        PINNED_IDENTITY["ranking_run_id"],
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.live_source_reads.ranking_run.ranking_version",
        ranking_run.get("ranking_version"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_RANKING_VERSION,
    )
    if "fixture" in str(ranking_run.get("ranking_version", "")).lower():
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.live_source_reads.ranking_run.ranking_version "
            "must not be a test fixture"
        )
    identity = live_source_reads.get("input_identity_verification")
    if not isinstance(identity, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.live_source_reads.input_identity_verification "
            "must be an object"
        )
    for field, expected in PINNED_IDENTITY.items():
        _require_equal(
            f"production_default_api_user_visible_pilot_slice.live_source_reads.input_identity_verification.{field}",
            identity.get(field),
            expected,
        )
    _require_true(
        "production_default_api_user_visible_pilot_slice.live_source_reads.input_identity_verification.matches_pinned_identity",
        identity.get("matches_pinned_identity"),
    )
    read_only = live_source_reads.get("read_only_assertions")
    if not isinstance(read_only, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.live_source_reads.read_only_assertions must be an object"
        )
    for field in (
        "select_only_sql_enforced",
        "approved_source_allowlist_enforced",
        "default_transaction_read_only",
        "no_write_sql_detected",
    ):
        _require_true(
            f"production_default_api_user_visible_pilot_slice.live_source_reads.read_only_assertions.{field}",
            read_only.get(field),
        )
    _require_true(
        "production_default_api_user_visible_pilot_slice.live_source_reads.labels_not_used_for_scoring",
        live_source_reads.get("labels_not_used_for_scoring"),
    )
    for field in ("refit_training_performed", "embedding_generation_performed", "label_ingest_performed"):
        _require_false(
            f"production_default_api_user_visible_pilot_slice.live_source_reads.{field}",
            live_source_reads.get(field),
        )

    provenance = production_default_api_user_visible_pilot_slice.get("input_provenance")
    if not isinstance(provenance, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.input_provenance must be an object"
        )
    for field in (
        "previous_live_read_only_pilot_run_id",
        "previous_live_execution_pilot_run_id",
        "previous_flag_enablement_pilot_run_id",
    ):
        if not isinstance(provenance.get(field), str) or not provenance.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production_default_api_user_visible_pilot_slice.input_provenance.{field} must be populated"
            )
    _require_true(
        "production_default_api_user_visible_pilot_slice.input_provenance.reread_approved_production_sources",
        provenance.get("reread_approved_production_sources"),
    )
    for field in ("fixture_ranking_version_used", "fixture_rows_used_for_main_join"):
        _require_false(
            f"production_default_api_user_visible_pilot_slice.input_provenance.{field}",
            provenance.get(field),
        )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.input_provenance.ranking_version",
        provenance.get("ranking_version"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_RANKING_VERSION,
    )

    scope = production_default_api_user_visible_pilot_slice.get("production_default_api_user_visible_scope")
    if not isinstance(scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.production_default_api_user_visible_scope "
            "must be an object"
        )
    if not isinstance(scope.get("production_default_api_user_visible_grant_decision"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.production_default_api_user_visible_scope."
            "production_default_api_user_visible_grant_decision must be an object"
        )
    if not isinstance(scope.get("production_default_api_user_visible_granted_scope"), Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.production_default_api_user_visible_scope."
            "production_default_api_user_visible_granted_scope must be an object"
        )
    for field in (
        "bounded_production_default_api_user_visible_pilot_only",
        "prod_scoped_shadow_production_default_api_user_visible_authorized",
    ):
        _require_true(
            f"production_default_api_user_visible_pilot_slice.production_default_api_user_visible_scope.{field}",
            scope.get(field),
        )
    for field in (
        "prod_scoped_shadow_execution_authorized",
        "production_default_allowed",
        "api_web_changes_allowed",
        "user_visible_ranking_changed",
    ):
        _require_false(
            f"production_default_api_user_visible_pilot_slice.production_default_api_user_visible_scope.{field}",
            scope.get(field),
        )

    probe = production_default_api_user_visible_pilot_slice.get("production_default_api_user_visible_probe")
    if not isinstance(probe, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.production_default_api_user_visible_probe "
            "must be an object"
        )
    _validate_production_default_api_user_visible_probe(
        probe,
        label="production_default_api_user_visible_pilot_slice.production_default_api_user_visible_probe",
    )

    write_counts = production_default_api_user_visible_pilot_slice.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.write_count_verification must be an object"
        )
    _require_true(
        "production_default_api_user_visible_pilot_slice.write_count_verification.local_artifact_tree_writes_performed",
        write_counts.get("local_artifact_tree_writes_performed"),
    )
    for field in ("production_writes_performed", "committed_artifact_writes_performed", "runtime_writes_performed"):
        _require_false(
            f"production_default_api_user_visible_pilot_slice.write_count_verification.{field}",
            write_counts.get(field),
        )
    _require_true(
        "production_default_api_user_visible_pilot_slice.write_count_verification.forbidden_write_counts_zero",
        write_counts.get("forbidden_write_counts_zero"),
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.write_count_verification.file_count",
        write_counts.get("file_count"),
        4,
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.write_count_verification.write_count",
        write_counts.get("write_count"),
        4,
    )
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.write_count_verification."
            "write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"production_default_api_user_visible_pilot_slice.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(
            f"production_default_api_user_visible_pilot_slice.write_count_verification.write_counts_by_isolated_target.{target}",
            counts.get(target),
            0,
        )

    files = production_default_api_user_visible_pilot_slice.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.files_written must contain four files"
        )
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal(
        "production_default_api_user_visible_pilot_slice.files_written names",
        observed_files,
        set(PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_EXPECTED_FILES),
    )
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production_default_api_user_visible_pilot_slice.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"production_default_api_user_visible_pilot_slice.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"production_default_api_user_visible_pilot_slice.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )

    observability = production_default_api_user_visible_pilot_slice.get("observability_summary")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice.observability_summary must be an object"
        )
    _require_true(
        "production_default_api_user_visible_pilot_slice.observability_summary.observability_complete",
        observability.get("observability_complete"),
    )
    _require_true(
        "production_default_api_user_visible_pilot_slice.observability_summary.live_prod_source_reads_performed",
        observability.get("live_prod_source_reads_performed"),
    )
    _require_equal(
        "production_default_api_user_visible_pilot_slice.observability_summary.row_counts.shadow_rows",
        _get(observability, "row_counts.shadow_rows"),
        528,
    )


def apply_production_scoped_shadow_production_default_api_user_visible_pilot_run(
    bundle: Mapping[str, Any],
    production_default_api_user_visible_pilot_slice: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if not isinstance(bundle, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("bundle must be an object")
    if not isinstance(production_default_api_user_visible_pilot_slice, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production_default_api_user_visible_pilot_slice must be an object"
        )
    _require_equal(
        "metadata.bundle_revision",
        _get(bundle, "metadata.bundle_revision"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_BUNDLE_REVISION,
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE,
    )
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    if "production_default_api_user_visible_pilot_run" in execution:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible pilot run has already been filed"
        )
    if execution.get("prod_scoped_shadow_production_default_api_user_visible_pilot_executed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_executed is already true"
        )
    if execution.get("prod_scoped_shadow_production_default_api_user_visible_pilot_passed") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_passed is already true"
        )

    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_execution_pilot_review_section(bundle.get("review"))
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_flag_enablement_pilot_review_section(bundle.get("review"))
    _verify_production_default_api_user_visible_request_section(authorization)
    _verify_production_default_api_user_visible_grant_section(authorization)
    for field in (
        "prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
        "prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        "prod_scoped_shadow_production_default_api_user_visible_authorized",
    ):
        _require_true(f"authorization.{field}", authorization.get(field))
    _require_equal(
        "authorization.production_default_api_user_visible_request_decision.decision",
        _get(authorization, "production_default_api_user_visible_request_decision.decision"),
        "requested",
    )
    _require_equal(
        "authorization.production_default_api_user_visible_requested_scope.authorization_scope",
        _get(authorization, "production_default_api_user_visible_requested_scope.authorization_scope"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE,
    )
    _require_equal(
        "authorization.production_default_api_user_visible_grant_decision.decision",
        _get(authorization, "production_default_api_user_visible_grant_decision.decision"),
        "granted",
    )
    _require_equal(
        "authorization.production_default_api_user_visible_granted_scope.authorization_scope",
        _get(authorization, "production_default_api_user_visible_granted_scope.authorization_scope"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE,
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )

    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_executed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_read_only_pilot_passed",
        execution.get("prod_scoped_shadow_live_read_only_pilot_passed"),
    )
    _validate_live_read_only_pilot_run_slice(execution.get("live_read_only_pilot_run"))
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        execution.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        execution.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _validate_live_execution_pilot_run_slice(execution.get("live_execution_pilot_run"))
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_passed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _validate_flag_enablement_pilot_run_slice(execution.get("flag_enablement_pilot_run"))

    for prefix, section in (
        ("posture", bundle.get("posture")),
        ("shadow_and_production_blockers", bundle.get("shadow_and_production_blockers")),
    ):
        if not isinstance(section, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"{prefix} must be an object")
        for field in (
            "prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
            "prod_scoped_shadow_production_default_api_user_visible_authorized",
            "prod_scoped_shadow_flag_enablement_authorized",
            "prod_scoped_shadow_live_execution_authorized",
            "prod_scoped_shadow_live_read_only_execution_authorized",
            "live_prod_source_reads_performed",
        ):
            _require_true(f"{prefix}.{field}", section.get(field))
        for field in (
            "missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
            "missing_prod_scoped_shadow_flag_enablement_authorization",
            "missing_prod_scoped_shadow_live_execution_authorization",
            "prod_scoped_shadow_execution_authorized",
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ):
            if field in section:
                _require_false(f"{prefix}.{field}", section.get(field))
        if section.get("prod_scoped_shadow_production_default_api_user_visible_pilot_executed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_pilot_executed "
                "must not already be true"
            )
        if section.get("prod_scoped_shadow_production_default_api_user_visible_pilot_passed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_pilot_passed "
                "must not already be true"
            )

    for path in (
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
    ):
        _require_false(path, _get(bundle, path))
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )
    _validate_production_default_api_user_visible_pilot_run_slice(
        production_default_api_user_visible_pilot_slice
    )

    executed_at = str(
        production_default_api_user_visible_pilot_slice.get("executed_at") or generated_at or _now_iso_z()
    )
    updated = deepcopy(dict(bundle))
    plan_before = deepcopy(updated.get("plan"))
    proof_before = deepcopy(updated.get("proof"))
    review_before = deepcopy(updated.get("review"))
    execution_before = deepcopy(updated.get("execution"))
    authorization_before = deepcopy(updated.get("authorization"))
    legacy_index_before = deepcopy(_get(updated, "metadata.legacy_artifacts_index"))

    metadata = deepcopy(dict(_metadata(updated, label="production-scoped-shadow bundle")))
    metadata["bundle_revision"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_BUNDLE_REVISION
    metadata["generated_at"] = executed_at
    updated["metadata"] = metadata

    execution = deepcopy(dict(updated.get("execution") or {}))
    execution.update(
        {
            "prod_scoped_shadow_production_default_api_user_visible_pilot_executed": True,
            "prod_scoped_shadow_production_default_api_user_visible_pilot_passed": True,
            "production_default_api_user_visible_pilot_run": deepcopy(
                dict(production_default_api_user_visible_pilot_slice)
            ),
        }
    )
    updated["execution"] = execution

    pilot_posture_fields = {
        "prod_scoped_shadow_production_default_api_user_visible_pilot_executed": True,
        "prod_scoped_shadow_production_default_api_user_visible_pilot_passed": True,
        "prod_scoped_shadow_production_default_api_user_visible_authorization_requested": True,
        "prod_scoped_shadow_production_default_api_user_visible_authorization_granted": True,
        "prod_scoped_shadow_production_default_api_user_visible_authorized": True,
        "missing_prod_scoped_shadow_production_default_api_user_visible_authorization": False,
        "prod_scoped_shadow_flag_enablement_authorized": True,
        "missing_prod_scoped_shadow_flag_enablement_authorization": False,
        "prod_scoped_shadow_live_execution_authorized": True,
        "missing_prod_scoped_shadow_live_execution_authorization": False,
        "prod_scoped_shadow_live_read_only_execution_authorized": True,
        "live_prod_source_reads_performed": True,
        "prod_scoped_shadow_execution_authorized": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    posture = deepcopy(dict(updated.get("posture") or {}))
    posture.update(pilot_posture_fields)
    updated["posture"] = posture

    blockers = deepcopy(dict(updated.get("shadow_and_production_blockers") or {}))
    blockers.update(pilot_posture_fields)
    blockers.update(
        {
            "blockers_cleared_by_production_default_api_user_visible_pilot_run": [],
            "blockers_introduced_by_production_default_api_user_visible_pilot_run": [],
            "blockers_unchanged_by_production_default_api_user_visible_pilot_run": True,
        }
    )
    blockers.pop("blockers_changed_by_production_default_api_user_visible_pilot_run", None)
    updated["shadow_and_production_blockers"] = blockers
    updated["writes_performed"] = False
    updated["runtime_writes_performed"] = False
    updated["recommended_next_stage"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE
    updated["caveats"] = _caveats(mode="post_production_default_api_user_visible_pilot_run")

    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible pilot run must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible pilot run must preserve proof section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible pilot run must preserve review section"
        )
    if updated.get("authorization") != authorization_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible pilot run must preserve authorization section"
        )
    for key, before_value in execution_before.items():
        if _get(updated, f"execution.{key}") != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production default/API/user-visible pilot run must preserve execution.{key}"
            )
    for key in ("pilot_harness", "pilot_run", "live_read_only_pilot_run", "live_execution_pilot_run", "flag_enablement_pilot_run"):
        if _get(updated, f"execution.{key}") != _get(execution_before, key):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production default/API/user-visible pilot run must preserve execution.{key}"
            )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible pilot run must preserve legacy_artifacts_index"
        )
    return updated


def run_flag_enablement_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    database_url: str | None = None,
    pilot_run_id: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
    confirm_flag_enablement_pilot: bool = False,
    confirm_live_read_only_prod_source_reads: bool = False,
) -> dict[str, Any]:
    from pipeline.ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot import (
        run_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot,
    )

    return run_ml_shadow_scorer_production_scoped_shadow_flag_enablement_pilot(
        bundle_path=bundle_path,
        database_url=database_url,
        pilot_run_id=pilot_run_id,
        repo_root=repo_root,
        update_bundle=update_bundle,
        generated_at=generated_at,
        confirm_flag_enablement_pilot=confirm_flag_enablement_pilot,
        confirm_live_read_only_prod_source_reads=confirm_live_read_only_prod_source_reads,
    )


def run_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    database_url: str | None = None,
    pilot_run_id: str | None = None,
    repo_root: Path | None = None,
    update_bundle: bool = True,
    generated_at: str | None = None,
    confirm_production_default_api_user_visible_pilot: bool = False,
    confirm_live_read_only_prod_source_reads: bool = False,
) -> dict[str, Any]:
    from pipeline.ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot import (
        run_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot,
    )

    return run_ml_shadow_scorer_production_scoped_shadow_production_default_api_user_visible_pilot(
        bundle_path=bundle_path,
        database_url=database_url,
        pilot_run_id=pilot_run_id,
        repo_root=repo_root,
        update_bundle=update_bundle,
        generated_at=generated_at,
        confirm_production_default_api_user_visible_pilot=confirm_production_default_api_user_visible_pilot,
        confirm_live_read_only_prod_source_reads=confirm_live_read_only_prod_source_reads,
    )


def _infer_plan_mode(
    bundle: Mapping[str, Any],
    *,
    expect_plan_filed: bool | None,
    expect_proof_filed: bool | None,
    expect_pilot_request_filed: bool | None,
    expect_pilot_grant_filed: bool | None,
    expect_pilot_harness_filed: bool | None,
    expect_pilot_harness_review_filed: bool | None,
    expect_pilot_run_filed: bool | None,
    expect_pilot_review_filed: bool | None,
    expect_live_read_only_request_filed: bool | None,
    expect_live_read_only_grant_filed: bool | None,
    expect_live_read_only_pilot_run_filed: bool | None,
    expect_live_read_only_pilot_review_filed: bool | None,
    expect_live_execution_request_filed: bool | None,
    expect_live_execution_grant_filed: bool | None,
    expect_live_execution_pilot_run_filed: bool | None,
    expect_live_execution_pilot_review_filed: bool | None,
    expect_flag_enablement_request_filed: bool | None,
    expect_flag_enablement_grant_filed: bool | None,
    expect_flag_enablement_pilot_run_filed: bool | None,
    expect_flag_enablement_pilot_review_filed: bool | None,
    expect_production_default_api_user_visible_request_filed: bool | None,
    expect_production_default_api_user_visible_grant_filed: bool | None,
    expect_production_default_api_user_visible_pilot_run_filed: bool | None,
) -> str:
    explicit = [
        expectation is not None
        for expectation in (
            expect_plan_filed,
            expect_proof_filed,
            expect_pilot_request_filed,
            expect_pilot_grant_filed,
            expect_pilot_harness_filed,
            expect_pilot_harness_review_filed,
            expect_pilot_run_filed,
            expect_pilot_review_filed,
            expect_live_read_only_request_filed,
            expect_live_read_only_grant_filed,
            expect_live_read_only_pilot_run_filed,
            expect_live_read_only_pilot_review_filed,
            expect_live_execution_request_filed,
            expect_live_execution_grant_filed,
            expect_live_execution_pilot_run_filed,
            expect_live_execution_pilot_review_filed,
            expect_flag_enablement_request_filed,
            expect_flag_enablement_grant_filed,
            expect_flag_enablement_pilot_run_filed,
            expect_flag_enablement_pilot_review_filed,
            expect_production_default_api_user_visible_request_filed,
            expect_production_default_api_user_visible_grant_filed,
            expect_production_default_api_user_visible_pilot_run_filed,
        )
    ]
    if sum(explicit) > 1:
        raise MLShadowScorerProductionScopedShadowBundleError("plan/proof/pilot/live read-only expectations conflict")
    if expect_production_default_api_user_visible_pilot_run_filed is True:
        return "post_production_default_api_user_visible_pilot_run"
    if expect_production_default_api_user_visible_pilot_run_filed is False:
        if _get(
            bundle,
            "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_executed",
        ) is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "production default/API/user-visible pilot run must not be filed"
            )
        if (
            _get(
                bundle,
                "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
            )
            is True
        ):
            return "post_production_default_api_user_visible_grant"
        if (
            _get(
                bundle,
                "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
            )
            is True
        ):
            return "post_production_default_api_user_visible_request"
        if _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed") is True:
            return "post_flag_enablement_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_executed") is True:
            return "post_flag_enablement_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            return "post_flag_enablement_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            return "post_flag_enablement_request"
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_production_default_api_user_visible_grant_filed is True:
        return "post_production_default_api_user_visible_grant"
    if expect_production_default_api_user_visible_grant_filed is False:
        if _get(
            bundle,
            "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        ) is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "production default/API/user-visible authorization grant must not be filed"
            )
        if (
            _get(
                bundle,
                "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
            )
            is True
        ):
            return "post_production_default_api_user_visible_request"
        if _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed") is True:
            return "post_flag_enablement_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_executed") is True:
            return "post_flag_enablement_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            return "post_flag_enablement_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            return "post_flag_enablement_request"
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_production_default_api_user_visible_request_filed is True:
        return "post_production_default_api_user_visible_request"
    if expect_production_default_api_user_visible_request_filed is False:
        if (
            _get(
                bundle,
                "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
            )
            is True
        ):
            raise MLShadowScorerProductionScopedShadowBundleError(
                "production default/API/user-visible authorization request must not be filed"
            )
        if _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed") is True:
            return "post_flag_enablement_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_executed") is True:
            return "post_flag_enablement_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            return "post_flag_enablement_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            return "post_flag_enablement_request"
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_flag_enablement_pilot_review_filed is True:
        return "post_flag_enablement_pilot_review"
    if expect_flag_enablement_pilot_review_filed is False:
        if _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "flag enablement pilot review must not be filed"
            )
        if _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_executed") is True:
            return "post_flag_enablement_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            return "post_flag_enablement_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            return "post_flag_enablement_request"
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_flag_enablement_pilot_run_filed is True:
        return "post_flag_enablement_pilot_run"
    if expect_flag_enablement_pilot_run_filed is False:
        if _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_executed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("flag enablement pilot run must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            return "post_flag_enablement_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            return "post_flag_enablement_request"
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_flag_enablement_grant_filed is True:
        return "post_flag_enablement_grant"
    if expect_flag_enablement_grant_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "flag enablement authorization grant must not be filed"
            )
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            return "post_flag_enablement_request"
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_flag_enablement_request_filed is True:
        return "post_flag_enablement_request"
    if expect_flag_enablement_request_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "flag enablement authorization grant must not be filed when expecting request-only state"
            )
        if _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "flag enablement authorization request must not be filed"
            )
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            return "post_live_execution_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_execution_pilot_review_filed is True:
        return "post_live_execution_pilot_review"
    if expect_live_execution_pilot_review_filed is False:
        if _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "live execution pilot review must not be filed"
            )
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            return "post_live_execution_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_execution_pilot_run_filed is True:
        return "post_live_execution_pilot_run"
    if expect_live_execution_pilot_run_filed is False:
        if _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live execution pilot run must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            return "post_live_execution_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_execution_grant_filed is True:
        return "post_live_execution_grant"
    if expect_live_execution_grant_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            return "post_live_execution_request"
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_execution_request_filed is True:
        return "post_live_execution_request"
    if expect_live_execution_request_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live execution request must not be filed")
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            return "post_live_read_only_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_read_only_pilot_review_filed is True:
        return "post_live_read_only_pilot_review"
    if expect_live_read_only_pilot_review_filed is False:
        if _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live read-only pilot review must not be filed")
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            return "post_live_read_only_pilot_run"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_read_only_pilot_run_filed is True:
        return "post_live_read_only_pilot_run"
    if expect_live_read_only_pilot_run_filed is False:
        if _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live read-only pilot run must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            return "post_live_read_only_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_read_only_grant_filed is True:
        return "post_live_read_only_grant"
    if expect_live_read_only_grant_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            return "post_live_read_only_request"
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_live_read_only_request_filed is True:
        return "post_live_read_only_request"
    if expect_live_read_only_request_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must not be filed")
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            return "post_pilot_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_pilot_review_filed is True:
        return "post_pilot_review"
    if expect_pilot_review_filed is False:
        if _get(bundle, "review.prod_scoped_shadow_pilot_reviewed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("pilot review must not be filed")
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            return "post_pilot_run"
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_pilot_run_filed is True:
        return "post_pilot_run"
    if expect_pilot_run_filed is False:
        if _get(bundle, "execution.prod_scoped_shadow_pilot_executed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("pilot run must not be filed")
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            return "post_pilot_harness_review"
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_pilot_harness_review_filed is True:
        return "post_pilot_harness_review"
    if expect_pilot_harness_review_filed is False:
        if _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("pilot harness review must not be filed")
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            return "post_pilot_harness"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_pilot_harness_filed is True:
        return "post_pilot_harness"
    if expect_pilot_harness_filed is False:
        if _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("pilot harness must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            return "post_pilot_grant"
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_pilot_grant_filed is True:
        return "post_pilot_grant"
    if expect_pilot_grant_filed is False:
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized") is True:
            raise MLShadowScorerProductionScopedShadowBundleError("pilot grant must not be filed")
        if _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested") is True:
            return "post_pilot_request"
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_pilot_request_filed is True:
        return "post_pilot_request"
    if expect_pilot_request_filed is False:
        requested = _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested")
        if requested is True:
            raise MLShadowScorerProductionScopedShadowBundleError("pilot request must not be filed")
        revision = _get(bundle, "metadata.bundle_revision")
        proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
        return "post_proof" if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True else "post_plan"
    if expect_proof_filed is True:
        return "post_proof"
    if expect_proof_filed is False:
        revision = _get(bundle, "metadata.bundle_revision")
        plan_defined = _get(bundle, "plan.prod_scoped_shadow_plan_defined")
        return "post_plan" if revision == POST_PLAN_BUNDLE_REVISION and plan_defined is True else "pre_plan"
    if expect_plan_filed is True:
        return "post_plan"
    if expect_plan_filed is False:
        return "pre_plan"
    revision = _get(bundle, "metadata.bundle_revision")
    plan_defined = _get(bundle, "plan.prod_scoped_shadow_plan_defined")
    proof_passed = _get(bundle, "posture.prod_scoped_shadow_proof_passed")
    pilot_executed = _get(bundle, "execution.prod_scoped_shadow_pilot_executed")
    pilot_harness_executed = _get(bundle, "execution.prod_scoped_shadow_pilot_harness_executed")
    pilot_harness_passed = _get(bundle, "execution.prod_scoped_shadow_pilot_harness_passed")
    pilot_harness_reviewed = _get(bundle, "review.prod_scoped_shadow_pilot_harness_reviewed")
    pilot_harness_accepted = _get(bundle, "review.prod_scoped_shadow_pilot_harness_accepted")
    pilot_reviewed = _get(bundle, "review.prod_scoped_shadow_pilot_reviewed")
    pilot_accepted = _get(bundle, "review.prod_scoped_shadow_pilot_accepted")
    pilot_request = _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_requested")
    pilot_grant = _get(bundle, "authorization.prod_scoped_shadow_pilot_authorization_granted")
    pilot_authorized = _get(bundle, "authorization.prod_scoped_shadow_pilot_authorized")
    live_read_only_request = _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_requested")
    live_read_only_grant = _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorization_granted")
    live_read_only_authorized = _get(bundle, "authorization.prod_scoped_shadow_live_read_only_authorized")
    live_read_only_pilot_executed = _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_executed")
    live_read_only_pilot_passed = _get(bundle, "execution.prod_scoped_shadow_live_read_only_pilot_passed")
    live_read_only_pilot_reviewed = _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_reviewed")
    live_read_only_pilot_accepted = _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_accepted")
    live_execution_request = _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_requested")
    live_execution_grant = _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorization_granted")
    live_execution_authorized = _get(bundle, "authorization.prod_scoped_shadow_live_execution_authorized")
    live_execution_pilot_executed = _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_executed")
    live_execution_pilot_passed = _get(bundle, "execution.prod_scoped_shadow_live_execution_pilot_passed")
    live_execution_pilot_reviewed = _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_reviewed")
    live_execution_pilot_accepted = _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_accepted")
    flag_enablement_request = _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested")
    flag_enablement_grant = _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted")
    flag_enablement_authorized = _get(bundle, "authorization.prod_scoped_shadow_flag_enablement_authorized")
    flag_enablement_pilot_executed = _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_executed")
    flag_enablement_pilot_passed = _get(bundle, "execution.prod_scoped_shadow_flag_enablement_pilot_passed")
    flag_enablement_pilot_reviewed = _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_reviewed")
    flag_enablement_pilot_accepted = _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_accepted")
    production_default_api_user_visible_request = _get(
        bundle,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
    )
    production_default_api_user_visible_grant = _get(
        bundle,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
    )
    production_default_api_user_visible_authorized = _get(
        bundle,
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
    )
    production_default_api_user_visible_pilot_executed = _get(
        bundle,
        "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_executed",
    )
    production_default_api_user_visible_pilot_passed = _get(
        bundle,
        "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_passed",
    )
    if (
        revision == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is True
        and flag_enablement_authorized is True
        and flag_enablement_pilot_executed is True
        and flag_enablement_pilot_passed is True
        and flag_enablement_pilot_reviewed is True
        and flag_enablement_pilot_accepted is True
        and production_default_api_user_visible_request is True
        and production_default_api_user_visible_grant is True
        and production_default_api_user_visible_authorized is True
        and production_default_api_user_visible_pilot_executed is True
        and production_default_api_user_visible_pilot_passed is True
    ):
        return "post_production_default_api_user_visible_pilot_run"
    if (
        revision == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is True
        and flag_enablement_authorized is True
        and flag_enablement_pilot_executed is True
        and flag_enablement_pilot_passed is True
        and flag_enablement_pilot_reviewed is True
        and flag_enablement_pilot_accepted is True
        and production_default_api_user_visible_request is True
        and production_default_api_user_visible_grant is True
        and production_default_api_user_visible_authorized is True
    ):
        return "post_production_default_api_user_visible_grant"
    if (
        revision == POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is True
        and flag_enablement_authorized is True
        and flag_enablement_pilot_executed is True
        and flag_enablement_pilot_passed is True
        and flag_enablement_pilot_reviewed is True
        and flag_enablement_pilot_accepted is True
        and production_default_api_user_visible_request is True
        and production_default_api_user_visible_grant is False
        and production_default_api_user_visible_authorized is False
    ):
        return "post_production_default_api_user_visible_request"
    if (
        revision == POST_FLAG_ENABLEMENT_PILOT_REVIEW_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is True
        and flag_enablement_authorized is True
        and flag_enablement_pilot_executed is True
        and flag_enablement_pilot_passed is True
        and flag_enablement_pilot_reviewed is True
        and isinstance(flag_enablement_pilot_accepted, bool)
    ):
        return "post_flag_enablement_pilot_review"
    if (
        revision == POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is True
        and flag_enablement_authorized is True
        and flag_enablement_pilot_executed is True
        and flag_enablement_pilot_passed is True
    ):
        return "post_flag_enablement_pilot_run"
    if (
        revision == POST_FLAG_ENABLEMENT_GRANT_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is True
        and flag_enablement_authorized is True
    ):
        return "post_flag_enablement_grant"
    if (
        revision == POST_FLAG_ENABLEMENT_REQUEST_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and live_execution_pilot_accepted is True
        and flag_enablement_request is True
        and flag_enablement_grant is False
    ):
        return "post_flag_enablement_request"
    if (
        revision == POST_LIVE_EXECUTION_PILOT_REVIEW_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
        and live_execution_pilot_reviewed is True
        and isinstance(live_execution_pilot_accepted, bool)
    ):
        return "post_live_execution_pilot_review"
    if (
        revision == POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
        and live_execution_pilot_executed is True
        and live_execution_pilot_passed is True
    ):
        return "post_live_execution_pilot_run"
    if (
        revision == POST_LIVE_EXECUTION_GRANT_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
        and live_execution_grant is True
        and live_execution_authorized is True
    ):
        return "post_live_execution_grant"
    if (
        revision == POST_LIVE_EXECUTION_REQUEST_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and live_read_only_pilot_accepted is True
        and live_execution_request is True
    ):
        return "post_live_execution_request"
    if (
        revision == POST_LIVE_READ_ONLY_PILOT_REVIEW_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
        and live_read_only_pilot_reviewed is True
        and isinstance(live_read_only_pilot_accepted, bool)
    ):
        return "post_live_read_only_pilot_review"
    if (
        revision == POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
        and live_read_only_pilot_executed is True
        and live_read_only_pilot_passed is True
    ):
        return "post_live_read_only_pilot_run"
    if (
        revision == POST_LIVE_READ_ONLY_GRANT_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is True
        and live_read_only_authorized is True
    ):
        return "post_live_read_only_grant"
    if (
        revision == POST_LIVE_READ_ONLY_REQUEST_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and pilot_accepted is True
        and live_read_only_request is True
        and live_read_only_grant is False
        and live_read_only_authorized is False
    ):
        return "post_live_read_only_request"
    if (
        revision == POST_PILOT_REVIEW_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
        and pilot_reviewed is True
        and isinstance(pilot_accepted, bool)
    ):
        return "post_pilot_review"
    if (
        revision == POST_PILOT_RUN_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and pilot_harness_accepted is True
        and pilot_executed is True
    ):
        return "post_pilot_run"
    if (
        revision == POST_PILOT_HARNESS_REVIEW_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_harness_reviewed is True
        and isinstance(pilot_harness_accepted, bool)
        and pilot_executed is False
    ):
        return "post_pilot_harness_review"
    if (
        revision == POST_PILOT_HARNESS_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_harness_executed is True
        and pilot_harness_passed is True
        and pilot_executed is False
    ):
        return "post_pilot_harness"
    if (
        revision == POST_PILOT_GRANT_BUNDLE_REVISION
        and pilot_request is True
        and pilot_grant is True
        and pilot_authorized is True
        and pilot_executed is False
    ):
        return "post_pilot_grant"
    if (
        revision == POST_PILOT_REQUEST_BUNDLE_REVISION
        and pilot_request is True
        and pilot_authorized is False
        and pilot_executed is False
    ):
        return "post_pilot_request"
    if revision == POST_PROOF_BUNDLE_REVISION and proof_passed is True and pilot_executed is False:
        return "post_proof"
    if revision == POST_PLAN_BUNDLE_REVISION and plan_defined is True and proof_passed is False:
        return "post_plan"
    if revision == PRE_PLAN_BUNDLE_REVISION and plan_defined is False:
        return "pre_plan"
    raise MLShadowScorerProductionScopedShadowBundleError(
        "could not infer production-scoped-shadow bundle mode from revision and plan/proof state"
    )


def _verify_proof_section(proof: Any) -> None:
    if not isinstance(proof, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("proof must be an object")
    _require_true("proof.prod_scoped_shadow_proof_filed", proof.get("prod_scoped_shadow_proof_filed"))
    _require_equal("proof.proof_decision.decision", _get(proof, "proof_decision.decision"), "proven")
    if not isinstance(_get(proof, "proof_decision.prover"), str) or not _get(proof, "proof_decision.prover"):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.proof_decision.prover must be populated")
    if not isinstance(_get(proof, "proof_decision.proven_at"), str) or not _get(proof, "proof_decision.proven_at"):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.proof_decision.proven_at must be populated")
    _require_equal("proof.proof_surface", proof.get("proof_surface"), "bounded_fixture_dry_run")
    if not isinstance(proof.get("pilot_run_id"), str) or not proof.get("pilot_run_id"):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.pilot_run_id must be populated")
    validate_pilot_run_id(str(proof["pilot_run_id"]))
    _require_true("proof.input_contract_evidence.inputs_read_only", _get(proof, "input_contract_evidence.inputs_read_only"))
    _require_false(
        "proof.input_contract_evidence.labels_used_for_scoring",
        _get(proof, "input_contract_evidence.labels_used_for_scoring"),
    )
    _require_true(
        "proof.input_contract_evidence.input_hashes_traceable",
        _get(proof, "input_contract_evidence.input_hashes_traceable"),
    )
    write_evidence = proof.get("write_evidence")
    if not isinstance(write_evidence, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.write_evidence must be an object")
    _require_true(
        "proof.write_evidence.local_artifact_tree_writes_performed",
        write_evidence.get("local_artifact_tree_writes_performed"),
    )
    _require_false("proof.write_evidence.production_writes_performed", write_evidence.get("production_writes_performed"))
    _require_false(
        "proof.write_evidence.committed_artifact_writes_performed",
        write_evidence.get("committed_artifact_writes_performed"),
    )
    _require_false("proof.write_evidence.runtime_writes_performed", write_evidence.get("runtime_writes_performed"))
    _require_equal("proof.write_evidence.write_target", write_evidence.get("write_target"), ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS)
    _require_true("proof.write_evidence.forbidden_write_counts_zero", write_evidence.get("forbidden_write_counts_zero"))
    counts = write_evidence.get("forbidden_write_target_counts")
    if not isinstance(counts, Mapping) or not counts:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "proof.write_evidence.forbidden_write_target_counts must be a non-empty object"
        )
    missing = [target for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS if target not in counts]
    if missing:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "proof.write_evidence.forbidden_write_target_counts missing keys: " + ", ".join(missing)
        )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(f"proof.write_evidence.forbidden_write_target_counts.{target}", counts.get(target), 0)
    files_written = write_evidence.get("files_written")
    if not isinstance(files_written, list) or not files_written:
        raise MLShadowScorerProductionScopedShadowBundleError("proof.write_evidence.files_written must be populated")
    for index, record in enumerate(files_written):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"proof.write_evidence.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"proof.write_evidence.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"proof.write_evidence.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
    _require_true(
        "proof.observability_evidence.observability_complete",
        _get(proof, "observability_evidence.observability_complete"),
    )
    signals = _get(proof, "observability_evidence.signals_emitted")
    if not isinstance(signals, list):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.observability_evidence.signals_emitted must be a list")
    for signal in OBSERVABILITY_SIGNALS:
        if signal not in signals:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"proof.observability_evidence.signals_emitted missing {signal!r}"
            )
    rollback = proof.get("rollback_drill_evidence")
    if not isinstance(rollback, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.rollback_drill_evidence must be an object")
    _require_false("proof.rollback_drill_evidence.flag_enablement_attempted", rollback.get("flag_enablement_attempted"))
    _require_true("proof.rollback_drill_evidence.flag_off_verified", rollback.get("flag_off_verified"))
    _require_false(
        "proof.rollback_drill_evidence.production_ranking_api_default_mutation_attempted",
        rollback.get("production_ranking_api_default_mutation_attempted"),
    )
    _require_true(
        "proof.rollback_drill_evidence.production_ranking_api_default_unchanged_by_construction",
        rollback.get("production_ranking_api_default_unchanged_by_construction"),
    )
    _require_true("proof.rollback_drill_evidence.no_further_writes_after_flag_off", rollback.get("no_further_writes_after_flag_off"))
    if rollback.get("pilot_dir_cleanup") not in {"retained_for_inspection", "cleaned"}:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "proof.rollback_drill_evidence.pilot_dir_cleanup must be retained_for_inspection or cleaned"
        )
    pass_fail = proof.get("proof_pass_fail")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("proof.proof_pass_fail must be an object")
    for field in (
        "read_only_contract_honored",
        "forbidden_targets_zero",
        "observability_complete",
        "rollback_drill_executable",
        "overall_passed",
    ):
        _require_true(f"proof.proof_pass_fail.{field}", pass_fail.get(field))

def _verify_plan_subsections(plan: Mapping[str, Any]) -> None:
    for subsection in PLAN_SUBSECTIONS:
        value = plan.get(subsection)
        if not isinstance(value, Mapping) or not value:
            raise MLShadowScorerProductionScopedShadowBundleError(f"plan.{subsection} must be populated")
    _require_equal("plan.plan_decision.decision", _get(plan, "plan_decision.decision"), "planned")
    if not isinstance(_get(plan, "plan_decision.planner"), str) or not _get(plan, "plan_decision.planner"):
        raise MLShadowScorerProductionScopedShadowBundleError("plan.plan_decision.planner must be populated")
    if not isinstance(_get(plan, "plan_decision.planned_at"), str) or not _get(plan, "plan_decision.planned_at"):
        raise MLShadowScorerProductionScopedShadowBundleError("plan.plan_decision.planned_at must be populated")
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(plan, "feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "plan.prod_read_only_input_contract.labels_used_for_scoring",
        _get(plan, "prod_read_only_input_contract.labels_used_for_scoring"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(plan, "production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(plan, "production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(plan, "production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    _require_true(
        "plan.proof_and_pilot_prerequisites.missing_prod_scoped_shadow_proof",
        _get(plan, "proof_and_pilot_prerequisites.missing_prod_scoped_shadow_proof"),
    )


def _verify_pilot_harness_section(harness: Any, *, repo_root: Path, verify_local_files: bool = True) -> None:
    if not isinstance(harness, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness must be an object")
    pilot_run_id = harness.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.pilot_run_id must be populated")
    validate_pilot_run_id(pilot_run_id)
    _require_equal("execution.pilot_harness.pilot_surface", harness.get("pilot_surface"), PILOT_HARNESS_SURFACE)
    _require_equal("execution.pilot_harness.fixture_row_count", harness.get("fixture_row_count"), 3)
    _require_false(
        "execution.pilot_harness.live_prod_source_reads_performed",
        harness.get("live_prod_source_reads_performed"),
    )
    pilot_dir_ref = harness.get("pilot_run_directory")
    if not isinstance(pilot_dir_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.pilot_run_directory must be an object")
    expected_root = f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/"
    _require_equal(
        "execution.pilot_harness.pilot_run_directory.root_path",
        pilot_dir_ref.get("root_path"),
        PROD_SCOPED_SHADOW_ROOT,
    )
    _require_equal(
        "execution.pilot_harness.pilot_run_directory.relative_path",
        pilot_dir_ref.get("relative_path"),
        expected_root,
    )
    runtime_drill = harness.get("runtime_drill")
    if not isinstance(runtime_drill, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.runtime_drill must be an object")
    _require_equal(
        "execution.pilot_harness.runtime_drill.call_order",
        runtime_drill.get("call_order"),
        ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
    )
    _require_true("execution.pilot_harness.runtime_drill.environment_restored", runtime_drill.get("environment_restored"))
    _require_equal(
        "execution.pilot_harness.runtime_drill.preflight.status",
        _get(runtime_drill, "preflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal(
        "execution.pilot_harness.runtime_drill.pilot.status",
        _get(runtime_drill, "pilot.status"),
        "succeeded_test_only",
    )
    _require_equal(
        "execution.pilot_harness.runtime_drill.postflight.status",
        _get(runtime_drill, "postflight.status"),
        "skipped_runtime_disabled",
    )
    _require_equal("execution.pilot_harness.runtime_drill.pilot.shadow_row_count", _get(runtime_drill, "pilot.shadow_row_count"), 3)
    _require_false("execution.pilot_harness.runtime_drill.pilot.writes_performed", _get(runtime_drill, "pilot.writes_performed"))

    write_counts = harness.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.pilot_harness.write_count_verification must be an object"
        )
    _require_true(
        "execution.pilot_harness.write_count_verification.local_artifact_tree_writes_performed",
        write_counts.get("local_artifact_tree_writes_performed"),
    )
    _require_false(
        "execution.pilot_harness.write_count_verification.production_writes_performed",
        write_counts.get("production_writes_performed"),
    )
    _require_false(
        "execution.pilot_harness.write_count_verification.committed_artifact_writes_performed",
        write_counts.get("committed_artifact_writes_performed"),
    )
    _require_false(
        "execution.pilot_harness.write_count_verification.runtime_writes_performed",
        write_counts.get("runtime_writes_performed"),
    )
    _require_equal("execution.pilot_harness.write_count_verification.file_count", write_counts.get("file_count"), 4)
    _require_equal("execution.pilot_harness.write_count_verification.write_count", write_counts.get("write_count"), 4)
    _require_true(
        "execution.pilot_harness.write_count_verification.forbidden_write_counts_zero",
        write_counts.get("forbidden_write_counts_zero"),
    )
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.pilot_harness.write_count_verification.write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"execution.pilot_harness.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(
            f"execution.pilot_harness.write_count_verification.write_counts_by_isolated_target.{target}",
            counts.get(target),
            0,
        )

    files = harness.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.files_written must contain four files")
    expected_files = set(PILOT_HARNESS_EXPECTED_FILES)
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal("execution.pilot_harness.files_written names", observed_files, expected_files)
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"execution.pilot_harness.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"execution.pilot_harness.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"execution.pilot_harness.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        local_file = pilot_dir / str(record["relative_path"])
        if verify_local_files and local_file.exists():
            _require_equal(
                f"execution.pilot_harness.files_written[{index}].sha256",
                _sha256_file(local_file),
                record.get("sha256"),
            )
            _require_equal(
                f"execution.pilot_harness.files_written[{index}].byte_count",
                local_file.stat().st_size,
                record.get("byte_count"),
            )

    observability = harness.get("observability_summary")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.observability_summary must be an object")
    _require_true("execution.pilot_harness.observability_summary.observability_complete", observability.get("observability_complete"))
    _require_false(
        "execution.pilot_harness.observability_summary.live_prod_source_reads_performed",
        observability.get("live_prod_source_reads_performed"),
    )
    pass_fail = harness.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.pass_fail_evaluation must be an object")
    _require_true("execution.pilot_harness.pass_fail_evaluation.overall_passed", pass_fail.get("overall_passed"))
    _require_equal("execution.pilot_harness.pass_fail_evaluation.failed_checks", pass_fail.get("failed_checks"), [])
    checks = pass_fail.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_harness.pass_fail_evaluation.checks must be an object")
    for field in (
        "fixture_row_count_3",
        "preflight_disabled",
        "pilot_runtime_succeeded",
        "postflight_disabled",
        "environment_restored",
        "runtime_drill_order",
        "forbidden_write_counts_zero",
        "isolated_artifact_target_count_4",
        "live_prod_source_reads_false",
        "runtime_writes_false",
        "production_default_changed_false",
        "user_visible_ranking_changed_false",
        "labels_used_for_scoring_false",
        "pilot_executed_false",
    ):
        _require_true(f"execution.pilot_harness.pass_fail_evaluation.checks.{field}", checks.get(field))


def _verify_source_artifact_record(record: Any, *, repo_root: Path, label: str) -> None:
    if not isinstance(record, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} must be an object")
    path = record.get("path")
    sha256 = record.get("sha256")
    if not isinstance(path, str) or not path:
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label}.path must be populated")
    if not isinstance(sha256, str) or not sha256:
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label}.sha256 must be populated")
    resolved = Path(path)
    resolved = resolved if resolved.is_absolute() else repo_root / resolved
    if resolved.exists() and not recorded_sha256_matches_text_artifact(resolved, sha256):
        raise MLShadowScorerProductionScopedShadowBundleError(f"{label} sha256 mismatch")
    _require_equal(f"{label}.verification_status", record.get("verification_status"), "confirmed")


def _verify_pilot_run_section(pilot_run: Any, *, repo_root: Path, verify_local_files: bool = True) -> None:
    if not isinstance(pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run must be an object")
    pilot_run_id = pilot_run.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.pilot_run_id must be populated")
    validate_pilot_run_id(pilot_run_id)
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.pilot_run_id must not contain harness")
    _require_equal("execution.pilot_run.pilot_surface", pilot_run.get("pilot_surface"), PILOT_RUN_SURFACE)
    _require_false("execution.pilot_run.live_prod_source_reads_performed", pilot_run.get("live_prod_source_reads_performed"))
    pilot_dir_ref = pilot_run.get("pilot_run_directory")
    if not isinstance(pilot_dir_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.pilot_run_directory must be an object")
    _require_equal("execution.pilot_run.pilot_run_directory.root_path", pilot_dir_ref.get("root_path"), PROD_SCOPED_SHADOW_ROOT)
    _require_equal(
        "execution.pilot_run.pilot_run_directory.relative_path",
        pilot_dir_ref.get("relative_path"),
        f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/",
    )
    source_artifacts = pilot_run.get("source_artifacts")
    if not isinstance(source_artifacts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.source_artifacts must be an object")
    for role in ("learned_probability_artifact", "second_surface_generalization_audit"):
        _verify_source_artifact_record(source_artifacts.get(role), repo_root=repo_root, label=f"execution.pilot_run.source_artifacts.{role}")
    _require_equal("execution.pilot_run.input_join_summary.joined_candidate_count", _get(pilot_run, "input_join_summary.joined_candidate_count"), 528)
    _require_equal("execution.pilot_run.input_join_summary.runtime_row_count", _get(pilot_run, "input_join_summary.runtime_row_count"), 528)
    runtime_drill = pilot_run.get("runtime_drill")
    if not isinstance(runtime_drill, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.runtime_drill must be an object")
    _require_equal(
        "execution.pilot_run.runtime_drill.call_order",
        runtime_drill.get("call_order"),
        ["preflight_disabled", "pilot_enabled", "postflight_disabled"],
    )
    _require_true("execution.pilot_run.runtime_drill.environment_restored", runtime_drill.get("environment_restored"))
    _require_equal("execution.pilot_run.runtime_drill.preflight.status", _get(runtime_drill, "preflight.status"), "skipped_runtime_disabled")
    _require_equal("execution.pilot_run.runtime_drill.pilot.status", _get(runtime_drill, "pilot.status"), "succeeded_test_only")
    _require_equal("execution.pilot_run.runtime_drill.pilot.shadow_row_count", _get(runtime_drill, "pilot.shadow_row_count"), 528)
    _require_false("execution.pilot_run.runtime_drill.pilot.writes_performed", _get(runtime_drill, "pilot.writes_performed"))
    _require_equal("execution.pilot_run.runtime_drill.postflight.status", _get(runtime_drill, "postflight.status"), "skipped_runtime_disabled")

    write_counts = pilot_run.get("write_count_verification")
    if not isinstance(write_counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.write_count_verification must be an object")
    _require_true("execution.pilot_run.write_count_verification.local_artifact_tree_writes_performed", write_counts.get("local_artifact_tree_writes_performed"))
    _require_false("execution.pilot_run.write_count_verification.production_writes_performed", write_counts.get("production_writes_performed"))
    _require_false("execution.pilot_run.write_count_verification.committed_artifact_writes_performed", write_counts.get("committed_artifact_writes_performed"))
    _require_false("execution.pilot_run.write_count_verification.runtime_writes_performed", write_counts.get("runtime_writes_performed"))
    _require_equal("execution.pilot_run.write_count_verification.file_count", write_counts.get("file_count"), 4)
    _require_equal("execution.pilot_run.write_count_verification.write_count", write_counts.get("write_count"), 4)
    _require_true("execution.pilot_run.write_count_verification.forbidden_write_counts_zero", write_counts.get("forbidden_write_counts_zero"))
    counts = write_counts.get("write_counts_by_isolated_target")
    if not isinstance(counts, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.pilot_run.write_count_verification.write_counts_by_isolated_target must be an object"
        )
    _require_equal(
        f"execution.pilot_run.write_count_verification.write_counts_by_isolated_target.{ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS}",
        counts.get(ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS),
        4,
    )
    for target in FORBIDDEN_PROD_SCOPED_WRITE_TARGETS:
        _require_equal(f"execution.pilot_run.write_count_verification.write_counts_by_isolated_target.{target}", counts.get(target), 0)

    files = pilot_run.get("files_written")
    if not isinstance(files, list) or len(files) != 4:
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.files_written must contain four files")
    observed_files = {record.get("relative_path") for record in files if isinstance(record, Mapping)}
    _require_equal("execution.pilot_run.files_written names", observed_files, set(PILOT_RUN_EXPECTED_FILES))
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    for index, record in enumerate(files):
        if not isinstance(record, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"execution.pilot_run.files_written[{index}] must be an object"
            )
        for field in ("relative_path", "byte_count", "sha256", "write_target"):
            if field not in record:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"execution.pilot_run.files_written[{index}].{field} missing"
                )
        _require_equal(
            f"execution.pilot_run.files_written[{index}].write_target",
            record.get("write_target"),
            ISOLATED_PROD_SCOPED_AUDIT_ARTIFACTS,
        )
        local_file = pilot_dir / str(record["relative_path"])
        if verify_local_files and local_file.exists():
            _require_equal(f"execution.pilot_run.files_written[{index}].sha256", _sha256_file(local_file), record.get("sha256"))
            _require_equal(f"execution.pilot_run.files_written[{index}].byte_count", local_file.stat().st_size, record.get("byte_count"))

    observability = pilot_run.get("observability_summary")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.observability_summary must be an object")
    _require_true("execution.pilot_run.observability_summary.observability_complete", observability.get("observability_complete"))
    _require_false("execution.pilot_run.observability_summary.live_prod_source_reads_performed", observability.get("live_prod_source_reads_performed"))
    _require_equal("execution.pilot_run.observability_summary.row_counts.shadow_rows", _get(observability, "row_counts.shadow_rows"), 528)
    pass_fail = pilot_run.get("pass_fail_evaluation")
    if not isinstance(pass_fail, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution.pilot_run.pass_fail_evaluation must be an object")
    _require_true("execution.pilot_run.pass_fail_evaluation.overall_passed", pass_fail.get("overall_passed"))
    _require_equal("execution.pilot_run.pass_fail_evaluation.failed_checks", pass_fail.get("failed_checks"), [])


def _verify_live_read_only_pilot_run_section(
    live_read_only_pilot_run: Any,
    *,
    repo_root: Path,
    verify_local_files: bool = True,
) -> None:
    if not isinstance(live_read_only_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run must be an object"
        )
    _validate_live_read_only_pilot_run_slice(live_read_only_pilot_run)
    pilot_run_id = live_read_only_pilot_run.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run.pilot_run_id must be populated"
        )
    validate_pilot_run_id(pilot_run_id)
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run.pilot_run_id must not contain harness"
        )
    pilot_dir_ref = live_read_only_pilot_run.get("pilot_run_directory")
    if not isinstance(pilot_dir_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run.pilot_run_directory must be an object"
        )
    _require_equal(
        "execution.live_read_only_pilot_run.pilot_run_directory.root_path",
        pilot_dir_ref.get("root_path"),
        PROD_SCOPED_SHADOW_ROOT,
    )
    _require_equal(
        "execution.live_read_only_pilot_run.pilot_run_directory.relative_path",
        pilot_dir_ref.get("relative_path"),
        f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/",
    )
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    for index, record in enumerate(live_read_only_pilot_run["files_written"]):
        local_file = pilot_dir / str(record["relative_path"])
        if verify_local_files and local_file.exists():
            _require_equal(
                f"execution.live_read_only_pilot_run.files_written[{index}].sha256",
                _sha256_file(local_file),
                record.get("sha256"),
            )
            _require_equal(
                f"execution.live_read_only_pilot_run.files_written[{index}].byte_count",
                local_file.stat().st_size,
                record.get("byte_count"),
            )
    observability = live_read_only_pilot_run.get("observability_summary")
    if not isinstance(observability, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_read_only_pilot_run.observability_summary must be an object"
        )
    _require_true(
        "execution.live_read_only_pilot_run.observability_summary.observability_complete",
        observability.get("observability_complete"),
    )
    _require_true(
        "execution.live_read_only_pilot_run.observability_summary.live_prod_source_reads_performed",
        observability.get("live_prod_source_reads_performed"),
    )
    _require_equal(
        "execution.live_read_only_pilot_run.observability_summary.row_counts.shadow_rows",
        _get(observability, "row_counts.shadow_rows"),
        528,
    )


def _verify_live_prod_source_read_flags(bundle: Mapping[str, Any]) -> None:
    allowed_true_paths = {
        "posture.live_prod_source_reads_performed",
        "execution.live_read_only_pilot_run.live_prod_source_reads_performed",
        "execution.live_read_only_pilot_run.observability_summary.live_prod_source_reads_performed",
    }
    observed_true_paths: set[str] = set()
    for path, value in _iter_named_field_values(bundle, "live_prod_source_reads_performed"):
        if path in allowed_true_paths:
            _require_true(path, value)
            observed_true_paths.add(path)
        else:
            _require_false(path, value)
    missing_true = sorted(allowed_true_paths - observed_true_paths)
    if missing_true:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_prod_source_reads_performed missing true paths: " + ", ".join(missing_true)
        )
    _require_false(
        "execution.pilot_harness.live_prod_source_reads_performed",
        _get(bundle, "execution.pilot_harness.live_prod_source_reads_performed"),
    )
    _require_false(
        "execution.pilot_run.live_prod_source_reads_performed",
        _get(bundle, "execution.pilot_run.live_prod_source_reads_performed"),
    )


def _verify_pilot_harness_review_section(review: Any) -> None:
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    accepted = _validate_pilot_harness_review_slice(review)
    decision = review["review_decision"]
    if accepted:
        if "runtime drill succeeded in test-only harness context" not in decision.get("accepted_evidence", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                "review.review_decision.accepted_evidence must include runtime drill evidence"
            )
    limitations_text = " ".join(str(item).lower() for item in decision.get("limitations", []))
    for phrase in ("not live production traffic", "actual production-scoped pilot remains unexecuted"):
        if phrase not in limitations_text:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.review_decision.limitations must include {phrase!r}"
            )


def _verify_pilot_review_section(review: Any) -> None:
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _require_true("review.prod_scoped_shadow_pilot_reviewed", review.get("prod_scoped_shadow_pilot_reviewed"))
    accepted = review.get("prod_scoped_shadow_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError("review.prod_scoped_shadow_pilot_accepted must be a boolean")
    decision = review.get("pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review.pilot_review_decision must be an object")
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal("review.pilot_review_decision.decision", decision.get("decision"), expected_decision)
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review.pilot_review_decision.checks must be an object")
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in PILOT_RUN_REVIEW_CHECKS:
        if check_name not in checks or not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal("review.pilot_review_decision.failed_review_checks", sorted(failed), expected_failed)
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError("accepted pilot review must have no failed checks")
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError("not_accepted pilot review must list failed checks")
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.pilot_review_decision.{field} must be a non-empty string list"
            )
    accepted_evidence = "bounded 528-work audit-artifact pilot passed all review checks"
    if accepted and accepted_evidence not in decision.get("accepted_evidence", []):
        raise MLShadowScorerProductionScopedShadowBundleError(
            f"review.pilot_review_decision.accepted_evidence must include {accepted_evidence!r}"
        )
    limitations_text = " ".join(str(item).lower() for item in decision.get("limitations", []))
    for phrase in ("not live production traffic", "no live read-only production source access was reviewed"):
        if phrase not in limitations_text:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.pilot_review_decision.limitations must include {phrase!r}"
            )


def _verify_live_read_only_request_section(authorization: Mapping[str, Any]) -> None:
    request_decision = authorization.get("request_decision")
    if not isinstance(request_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization.request_decision must be an object")
    _require_equal("authorization.request_decision.decision", request_decision.get("decision"), "requested")
    for field in ("requester", "requested_at"):
        if not isinstance(request_decision.get(field), str) or not request_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.request_decision.{field} must be populated"
            )
    requested_scope = authorization.get("requested_scope")
    if not isinstance(requested_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization.requested_scope must be an object")
    _require_equal(
        "authorization.requested_scope.authorization_scope",
        requested_scope.get("authorization_scope"),
        LIVE_READ_ONLY_REQUEST_SCOPE,
    )
    future_requirements = requested_scope.get("future_grant_would_require")
    if not isinstance(future_requirements, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.requested_scope.future_grant_would_require must be a list"
        )
    for item in LIVE_READ_ONLY_REQUEST_FUTURE_GRANT_REQUIREMENTS:
        if item not in future_requirements:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.requested_scope.future_grant_would_require missing {item!r}"
            )
    explicitly_not_included = requested_scope.get("explicitly_not_included")
    if not isinstance(explicitly_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.requested_scope.explicitly_not_included must be a list"
        )
    for item in LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in explicitly_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.requested_scope.explicitly_not_included missing {item!r}"
            )


def _verify_live_execution_request_section(authorization: Mapping[str, Any]) -> None:
    request_decision = authorization.get("live_execution_request_decision")
    if not isinstance(request_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_request_decision must be an object"
        )
    _require_equal(
        "authorization.live_execution_request_decision.decision",
        request_decision.get("decision"),
        "requested",
    )
    for field in ("requester", "requested_at"):
        if not isinstance(request_decision.get(field), str) or not request_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_request_decision.{field} must be populated"
            )
    requested_scope = authorization.get("live_execution_requested_scope")
    if not isinstance(requested_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_requested_scope must be an object"
        )
    _require_equal(
        "authorization.live_execution_requested_scope.authorization_scope",
        requested_scope.get("authorization_scope"),
        LIVE_EXECUTION_REQUEST_SCOPE,
    )
    future_requirements = requested_scope.get("future_grant_would_require")
    if not isinstance(future_requirements, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_requested_scope.future_grant_would_require must be a list"
        )
    for item in LIVE_EXECUTION_REQUEST_FUTURE_GRANT_REQUIREMENTS:
        if item not in future_requirements:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_requested_scope.future_grant_would_require missing {item!r}"
            )
    explicitly_not_included = requested_scope.get("explicitly_not_included")
    if not isinstance(explicitly_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_requested_scope.explicitly_not_included must be a list"
        )
    for item in LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in explicitly_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_requested_scope.explicitly_not_included missing {item!r}"
            )


def _verify_flag_enablement_request_section(authorization: Mapping[str, Any]) -> None:
    request_decision = authorization.get("flag_enablement_request_decision")
    if not isinstance(request_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_request_decision must be an object"
        )
    _require_equal(
        "authorization.flag_enablement_request_decision.decision",
        request_decision.get("decision"),
        "requested",
    )
    for field in ("requester", "requested_at"):
        if not isinstance(request_decision.get(field), str) or not request_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_request_decision.{field} must be populated"
            )
    requested_scope = authorization.get("flag_enablement_requested_scope")
    if not isinstance(requested_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_requested_scope must be an object"
        )
    _require_equal(
        "authorization.flag_enablement_requested_scope.authorization_scope",
        requested_scope.get("authorization_scope"),
        FLAG_ENABLEMENT_REQUEST_SCOPE,
    )
    _require_equal(
        "authorization.flag_enablement_requested_scope.runtime_feature_flag",
        requested_scope.get("runtime_feature_flag"),
        FEATURE_FLAG,
    )
    future_requirements = requested_scope.get("future_grant_would_require")
    if not isinstance(future_requirements, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_requested_scope.future_grant_would_require must be a list"
        )
    for item in FLAG_ENABLEMENT_REQUEST_FUTURE_GRANT_REQUIREMENTS:
        if item not in future_requirements:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_requested_scope.future_grant_would_require missing {item!r}"
            )
    explicitly_not_included = requested_scope.get("explicitly_not_included")
    if not isinstance(explicitly_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_requested_scope.explicitly_not_included must be a list"
        )
    for item in FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in explicitly_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_requested_scope.explicitly_not_included missing {item!r}"
            )


def _verify_production_default_api_user_visible_request_section(authorization: Mapping[str, Any]) -> None:
    request_decision = authorization.get("production_default_api_user_visible_request_decision")
    if not isinstance(request_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_request_decision must be an object"
        )
    _require_equal(
        "authorization.production_default_api_user_visible_request_decision.decision",
        request_decision.get("decision"),
        "requested",
    )
    for field in ("requester", "requested_at"):
        if not isinstance(request_decision.get(field), str) or not request_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.production_default_api_user_visible_request_decision.{field} must be populated"
            )
    requested_scope = authorization.get("production_default_api_user_visible_requested_scope")
    if not isinstance(requested_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_requested_scope must be an object"
        )
    _require_equal(
        "authorization.production_default_api_user_visible_requested_scope.authorization_scope",
        requested_scope.get("authorization_scope"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE,
    )
    future_requirements = requested_scope.get("future_grant_would_require")
    if not isinstance(future_requirements, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_requested_scope.future_grant_would_require must be a list"
        )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_FUTURE_GRANT_REQUIREMENTS:
        if item not in future_requirements:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "authorization.production_default_api_user_visible_requested_scope.future_grant_would_require "
                f"missing {item!r}"
            )
    explicitly_not_included = requested_scope.get("explicitly_not_included")
    if not isinstance(explicitly_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_requested_scope.explicitly_not_included must be a list"
        )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in explicitly_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "authorization.production_default_api_user_visible_requested_scope.explicitly_not_included "
                f"missing {item!r}"
            )


def _verify_production_default_api_user_visible_grant_section(authorization: Mapping[str, Any]) -> None:
    grant_decision = authorization.get("production_default_api_user_visible_grant_decision")
    if not isinstance(grant_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_grant_decision must be an object"
        )
    _require_equal(
        "authorization.production_default_api_user_visible_grant_decision.decision",
        grant_decision.get("decision"),
        "granted",
    )
    for field in ("owner", "granted_at", "expiry_date", "review_by"):
        if not isinstance(grant_decision.get(field), str) or not grant_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.production_default_api_user_visible_grant_decision.{field} must be populated"
            )
    _validate_pilot_grant_review(
        owner=str(grant_decision["owner"]),
        second_reviewer=(
            str(grant_decision["second_reviewer"]) if grant_decision.get("second_reviewer") is not None else None
        ),
        owner_documents_equivalent_review=(
            str(grant_decision["owner_documents_equivalent_review"])
            if grant_decision.get("owner_documents_equivalent_review") is not None
            else None
        ),
    )
    granted_scope = authorization.get("production_default_api_user_visible_granted_scope")
    if not isinstance(granted_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_granted_scope must be an object"
        )
    _require_equal(
        "authorization.production_default_api_user_visible_granted_scope.authorization_scope",
        granted_scope.get("authorization_scope"),
        PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE,
    )
    authorizes_for_chain_only = granted_scope.get("authorizes_for_chain_only")
    if not isinstance(authorizes_for_chain_only, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_granted_scope.authorizes_for_chain_only must be a list"
        )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_AUTHORIZES_FOR_CHAIN_ONLY:
        if item not in authorizes_for_chain_only:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "authorization.production_default_api_user_visible_granted_scope.authorizes_for_chain_only "
                f"missing {item!r}"
            )
    explicitly_still_not_included = granted_scope.get("explicitly_still_not_included")
    if not isinstance(explicitly_still_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_granted_scope.explicitly_still_not_included "
            "must be a list"
        )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED:
        if item not in explicitly_still_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "authorization.production_default_api_user_visible_granted_scope.explicitly_still_not_included "
                f"missing {item!r}"
            )
    grant_time_boundaries = granted_scope.get("grant_time_production_default_api_user_visible_boundaries")
    if not isinstance(grant_time_boundaries, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.production_default_api_user_visible_granted_scope."
            "grant_time_production_default_api_user_visible_boundaries must be a list"
        )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_TIME_BOUNDARIES:
        if item not in grant_time_boundaries:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "authorization.production_default_api_user_visible_granted_scope."
                f"grant_time_production_default_api_user_visible_boundaries missing {item!r}"
            )


def _verify_flag_enablement_grant_section(authorization: Mapping[str, Any]) -> None:
    grant_decision = authorization.get("flag_enablement_grant_decision")
    if not isinstance(grant_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_grant_decision must be an object"
        )
    _require_equal(
        "authorization.flag_enablement_grant_decision.decision",
        grant_decision.get("decision"),
        "granted",
    )
    for field in ("owner", "granted_at", "expiry_date", "review_by"):
        if not isinstance(grant_decision.get(field), str) or not grant_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_grant_decision.{field} must be populated"
            )
    _validate_pilot_grant_review(
        owner=str(grant_decision["owner"]),
        second_reviewer=(
            str(grant_decision["second_reviewer"]) if grant_decision.get("second_reviewer") is not None else None
        ),
        owner_documents_equivalent_review=(
            str(grant_decision["owner_documents_equivalent_review"])
            if grant_decision.get("owner_documents_equivalent_review") is not None
            else None
        ),
    )
    granted_scope = authorization.get("flag_enablement_granted_scope")
    if not isinstance(granted_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_granted_scope must be an object"
        )
    _require_equal(
        "authorization.flag_enablement_granted_scope.authorization_scope",
        granted_scope.get("authorization_scope"),
        FLAG_ENABLEMENT_GRANT_SCOPE,
    )
    _require_equal(
        "authorization.flag_enablement_granted_scope.runtime_feature_flag",
        granted_scope.get("runtime_feature_flag"),
        FEATURE_FLAG,
    )
    authorizes_for_chain_only = granted_scope.get("authorizes_for_chain_only")
    if not isinstance(authorizes_for_chain_only, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_granted_scope.authorizes_for_chain_only must be a list"
        )
    for item in FLAG_ENABLEMENT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY:
        if item not in authorizes_for_chain_only:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_granted_scope.authorizes_for_chain_only missing {item!r}"
            )
    explicitly_still_not_included = granted_scope.get("explicitly_still_not_included")
    if not isinstance(explicitly_still_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_granted_scope.explicitly_still_not_included must be a list"
        )
    for item in FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED:
        if item not in explicitly_still_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_granted_scope.explicitly_still_not_included missing {item!r}"
            )
    grant_time_boundaries = granted_scope.get("grant_time_flag_enablement_boundaries")
    if not isinstance(grant_time_boundaries, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.flag_enablement_granted_scope.grant_time_flag_enablement_boundaries must be a list"
        )
    for item in FLAG_ENABLEMENT_GRANT_TIME_BOUNDARIES:
        if item not in grant_time_boundaries:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.flag_enablement_granted_scope.grant_time_flag_enablement_boundaries missing {item!r}"
            )


def _verify_live_execution_grant_section(authorization: Mapping[str, Any]) -> None:
    grant_decision = authorization.get("live_execution_grant_decision")
    if not isinstance(grant_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_grant_decision must be an object"
        )
    _require_equal(
        "authorization.live_execution_grant_decision.decision",
        grant_decision.get("decision"),
        "granted",
    )
    for field in ("owner", "granted_at", "expiry_date", "review_by"):
        if not isinstance(grant_decision.get(field), str) or not grant_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_grant_decision.{field} must be populated"
            )
    _validate_pilot_grant_review(
        owner=str(grant_decision["owner"]),
        second_reviewer=(
            str(grant_decision["second_reviewer"]) if grant_decision.get("second_reviewer") is not None else None
        ),
        owner_documents_equivalent_review=(
            str(grant_decision["owner_documents_equivalent_review"])
            if grant_decision.get("owner_documents_equivalent_review") is not None
            else None
        ),
    )
    granted_scope = authorization.get("live_execution_granted_scope")
    if not isinstance(granted_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_granted_scope must be an object"
        )
    _require_equal(
        "authorization.live_execution_granted_scope.authorization_scope",
        granted_scope.get("authorization_scope"),
        LIVE_EXECUTION_GRANT_SCOPE,
    )
    authorizes_for_chain_only = granted_scope.get("authorizes_for_chain_only")
    if not isinstance(authorizes_for_chain_only, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_granted_scope.authorizes_for_chain_only must be a list"
        )
    for item in LIVE_EXECUTION_GRANT_AUTHORIZES_FOR_CHAIN_ONLY:
        if item not in authorizes_for_chain_only:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_granted_scope.authorizes_for_chain_only missing {item!r}"
            )
    explicitly_still_not_included = granted_scope.get("explicitly_still_not_included")
    if not isinstance(explicitly_still_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_granted_scope.explicitly_still_not_included must be a list"
        )
    for item in LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED:
        if item not in explicitly_still_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_granted_scope.explicitly_still_not_included missing {item!r}"
            )
    grant_time_boundaries = granted_scope.get("grant_time_live_execution_boundaries")
    if not isinstance(grant_time_boundaries, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_execution_granted_scope.grant_time_live_execution_boundaries must be a list"
        )
    for item in LIVE_EXECUTION_GRANT_TIME_BOUNDARIES:
        if item not in grant_time_boundaries:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_execution_granted_scope.grant_time_live_execution_boundaries missing {item!r}"
            )


def _verify_live_read_only_grant_section(authorization: Mapping[str, Any]) -> None:
    grant_decision = authorization.get("live_read_only_grant_decision")
    if not isinstance(grant_decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_grant_decision must be an object"
        )
    _require_equal(
        "authorization.live_read_only_grant_decision.decision",
        grant_decision.get("decision"),
        "granted",
    )
    for field in ("owner", "granted_at", "expiry_date", "review_by"):
        if not isinstance(grant_decision.get(field), str) or not grant_decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_read_only_grant_decision.{field} must be populated"
            )
    _validate_pilot_grant_review(
        owner=str(grant_decision["owner"]),
        second_reviewer=(
            str(grant_decision["second_reviewer"]) if grant_decision.get("second_reviewer") is not None else None
        ),
        owner_documents_equivalent_review=(
            str(grant_decision["owner_documents_equivalent_review"])
            if grant_decision.get("owner_documents_equivalent_review") is not None
            else None
        ),
    )
    granted_scope = authorization.get("live_read_only_granted_scope")
    if not isinstance(granted_scope, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope must be an object"
        )
    _require_equal(
        "authorization.live_read_only_granted_scope.authorization_scope",
        granted_scope.get("authorization_scope"),
        LIVE_READ_ONLY_GRANT_SCOPE,
    )
    authorizes_for_chain_only = granted_scope.get("authorizes_for_chain_only")
    if not isinstance(authorizes_for_chain_only, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope.authorizes_for_chain_only must be a list"
        )
    for item in LIVE_READ_ONLY_GRANT_AUTHORIZES_FOR_CHAIN_ONLY:
        if item not in authorizes_for_chain_only:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_read_only_granted_scope.authorizes_for_chain_only missing {item!r}"
            )
    explicitly_still_not_included = granted_scope.get("explicitly_still_not_included")
    if not isinstance(explicitly_still_not_included, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope.explicitly_still_not_included must be a list"
        )
    for item in LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED:
        if item not in explicitly_still_not_included:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_read_only_granted_scope.explicitly_still_not_included missing {item!r}"
            )
    grant_time_boundaries = granted_scope.get("grant_time_live_read_boundaries")
    if not isinstance(grant_time_boundaries, list):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "authorization.live_read_only_granted_scope.grant_time_live_read_boundaries must be a list"
        )
    for item in LIVE_READ_ONLY_GRANT_TIME_BOUNDARIES:
        if item not in grant_time_boundaries:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.live_read_only_granted_scope.grant_time_live_read_boundaries missing {item!r}"
            )


def _verify_live_read_only_pilot_review_section(review: Any) -> None:
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _require_true(
        "review.prod_scoped_shadow_live_read_only_pilot_reviewed",
        review.get("prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    accepted = review.get("prod_scoped_shadow_live_read_only_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.prod_scoped_shadow_live_read_only_pilot_accepted must be a boolean"
        )
    decision = review.get("live_read_only_pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_read_only_pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal("review.live_read_only_pilot_review_decision.decision", decision.get("decision"), expected_decision)
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_read_only_pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_read_only_pilot_review_decision.checks must be an object"
        )
    if set(checks) != set(LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_read_only_pilot_review_decision.checks must align with "
            "LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS"
        )
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_read_only_pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS:
        if not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_read_only_pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal("review.live_read_only_pilot_review_decision.failed_review_checks", sorted(failed), expected_failed)
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted live read-only pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted live read-only pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_read_only_pilot_review_decision.{field} must be a non-empty string list"
            )
    limitations_text = " ".join(str(item).lower() for item in decision.get("limitations", []))
    for phrase in ("no runtime rerun", "no shadow-runs", "global/live/fleet online shadow execution"):
        if phrase not in limitations_text:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_read_only_pilot_review_decision.limitations must include {phrase!r}"
            )


def _without_live_read_only_pilot_review_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION
    payload["metadata"] = metadata
    review = deepcopy(dict(payload.get("review") or {}))
    review.pop("prod_scoped_shadow_live_read_only_pilot_reviewed", None)
    review.pop("prod_scoped_shadow_live_read_only_pilot_accepted", None)
    review.pop("live_read_only_pilot_review_decision", None)
    payload["review"] = review
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_live_read_only_pilot_reviewed", None)
    posture.pop("prod_scoped_shadow_live_read_only_pilot_accepted", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_live_read_only_pilot_reviewed", None)
    blockers.pop("prod_scoped_shadow_live_read_only_pilot_accepted", None)
    blockers.pop("blockers_cleared_by_live_read_only_pilot_review", None)
    blockers.pop("blockers_introduced_by_live_read_only_pilot_review", None)
    blockers.pop("blockers_unchanged_by_live_read_only_pilot_review", None)
    blockers.pop("blockers_changed_by_live_read_only_pilot_review", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_live_read_only_pilot_run")
    return payload


def _verify_live_read_only_pilot_review_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_LIVE_READ_ONLY_PILOT_REVIEW_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_live_read_only_pilot_review_payload(bundle),
        repo_root=repo_root,
        expect_live_read_only_pilot_run_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    review = bundle.get("review")
    _verify_live_read_only_pilot_review_section(review)
    accepted = _get(bundle, "review.prod_scoped_shadow_live_read_only_pilot_accepted")
    expected_next = (
        POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted is True
        else POST_LIVE_READ_ONLY_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), expected_next)
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_pilot_reviewed",
        posture.get("prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    _require_equal(
        "posture.prod_scoped_shadow_live_read_only_pilot_accepted",
        posture.get("prod_scoped_shadow_live_read_only_pilot_accepted"),
        accepted,
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
        authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_pilot_reviewed",
        blockers.get("prod_scoped_shadow_live_read_only_pilot_reviewed"),
    )
    _require_equal(
        "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_pilot_accepted",
        blockers.get("prod_scoped_shadow_live_read_only_pilot_accepted"),
        accepted,
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_live_read_only_pilot_review",
        blockers.get("blockers_cleared_by_live_read_only_pilot_review"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_live_read_only_pilot_review",
        blockers.get("blockers_introduced_by_live_read_only_pilot_review"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_live_read_only_pilot_review",
        blockers.get("blockers_unchanged_by_live_read_only_pilot_review"),
    )
    if "blockers_changed_by_live_read_only_pilot_review" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_live_read_only_pilot_review must not be used"
        )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_live_read_only_pilot_review"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_read_only_pilot_review caveats missing {caveat!r}"
            )
    stale_caveats = (
        "Live read-only pilot run still required before any live prod source reads are recorded.",
        "No live production DB/source reads were performed.",
        "Live read-only production shadow access remains a separate future authorization chain.",
    )
    for caveat in stale_caveats:
        if caveat in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_read_only_pilot_review caveats must not include stale caveat {caveat!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_live_read_only_pilot_review",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_live_execution_request_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_LIVE_READ_ONLY_PILOT_REVIEW_BUNDLE_REVISION
    payload["metadata"] = metadata
    authorization = deepcopy(dict(payload.get("authorization") or {}))
    authorization.pop("prod_scoped_shadow_live_execution_authorization_requested", None)
    authorization.pop("prod_scoped_shadow_live_execution_authorization_granted", None)
    authorization.pop("live_execution_request_decision", None)
    authorization.pop("live_execution_requested_scope", None)
    payload["authorization"] = authorization
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_live_execution_authorization_requested", None)
    posture.pop("prod_scoped_shadow_live_execution_authorization_granted", None)
    posture.pop("prod_scoped_shadow_live_execution_authorized", None)
    posture.pop("missing_prod_scoped_shadow_live_execution_authorization", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_live_execution_authorization_requested", None)
    blockers.pop("prod_scoped_shadow_live_execution_authorized", None)
    blockers.pop("missing_prod_scoped_shadow_live_execution_authorization", None)
    blockers.pop("blockers_cleared_by_live_execution_request", None)
    blockers.pop("blockers_introduced_by_live_execution_request", None)
    blockers.pop("blockers_unchanged_by_live_execution_request", None)
    blockers.pop("blockers_changed_by_live_execution_request", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_LIVE_READ_ONLY_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_live_read_only_pilot_review")
    return payload


def _verify_live_execution_request_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_LIVE_EXECUTION_REQUEST_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_live_execution_request_payload(bundle),
        repo_root=repo_root,
        expect_live_read_only_pilot_review_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _verify_live_execution_request_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _require_equal(
        "review.live_read_only_pilot_review_decision.decision",
        _get(bundle, "review.live_read_only_pilot_review_decision.decision"),
        "accepted",
    )
    _require_equal(
        "review.live_read_only_pilot_review_decision.failed_review_checks",
        _get(bundle, "review.live_read_only_pilot_review_decision.failed_review_checks"),
        [],
    )
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.live_prod_source_reads_performed",
        posture.get("live_prod_source_reads_performed"),
    )
    _require_true(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorization_requested",
        posture.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_false(
        "posture.prod_scoped_shadow_live_execution_authorization_granted",
        posture.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_false(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _verify_live_prod_source_read_flags(bundle)
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorization_requested",
        blockers.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_true(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_live_execution_request",
        blockers.get("blockers_introduced_by_live_execution_request"),
        ["missing_prod_scoped_shadow_live_execution_authorization"],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_live_execution_request",
        blockers.get("blockers_cleared_by_live_execution_request"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_live_execution_request",
        blockers.get("blockers_unchanged_by_live_execution_request"),
    )
    if "blockers_changed_by_live_execution_request" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_live_execution_request must not be used"
        )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_EXECUTION_REQUEST_NEXT_STAGE)
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_live_execution_request"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_execution_request caveats missing {caveat!r}"
            )
    for item in LIVE_EXECUTION_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_live_execution_request",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_live_execution_grant_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_REQUEST_BUNDLE_REVISION
    payload["metadata"] = metadata
    authorization = deepcopy(dict(payload.get("authorization") or {}))
    authorization["prod_scoped_shadow_live_execution_authorization_granted"] = False
    authorization["prod_scoped_shadow_live_execution_authorized"] = False
    authorization.pop("live_execution_grant_decision", None)
    authorization.pop("live_execution_granted_scope", None)
    payload["authorization"] = authorization
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture["prod_scoped_shadow_live_execution_authorization_granted"] = False
    posture["prod_scoped_shadow_live_execution_authorized"] = False
    posture["missing_prod_scoped_shadow_live_execution_authorization"] = True
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_live_execution_authorization_granted", None)
    blockers["prod_scoped_shadow_live_execution_authorized"] = False
    blockers["missing_prod_scoped_shadow_live_execution_authorization"] = True
    blockers.pop("blockers_cleared_by_live_execution_grant", None)
    blockers.pop("blockers_introduced_by_live_execution_grant", None)
    blockers.pop("blockers_unchanged_by_live_execution_grant", None)
    blockers.pop("blockers_changed_by_live_execution_grant", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_LIVE_EXECUTION_REQUEST_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_live_execution_request")
    return payload


def _verify_live_execution_grant_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_LIVE_EXECUTION_GRANT_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_live_execution_grant_payload(bundle),
        repo_root=repo_root,
        expect_live_execution_request_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    if "live_execution_pilot_run" in execution:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must not be present at post_live_execution_grant"
        )
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorization_granted",
        posture.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    _verify_live_prod_source_read_flags(bundle)
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorization_granted",
        blockers.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_live_execution_grant",
        blockers.get("blockers_cleared_by_live_execution_grant"),
        ["missing_prod_scoped_shadow_live_execution_authorization"],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_live_execution_grant",
        blockers.get("blockers_introduced_by_live_execution_grant"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_live_execution_grant",
        blockers.get("blockers_unchanged_by_live_execution_grant"),
    )
    if "blockers_changed_by_live_execution_grant" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_live_execution_grant must not be used"
        )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_EXECUTION_GRANT_NEXT_STAGE)
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_live_execution_grant"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_execution_grant caveats missing {caveat!r}"
            )
    for item in LIVE_EXECUTION_GRANT_STILL_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_live_execution_grant",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_live_execution_pilot_run_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_GRANT_BUNDLE_REVISION
    payload["metadata"] = metadata
    execution = deepcopy(dict(payload.get("execution") or {}))
    execution.pop("prod_scoped_shadow_live_execution_pilot_executed", None)
    execution.pop("prod_scoped_shadow_live_execution_pilot_passed", None)
    execution.pop("live_execution_pilot_run", None)
    payload["execution"] = execution
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_live_execution_pilot_executed", None)
    posture.pop("prod_scoped_shadow_live_execution_pilot_passed", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_live_execution_pilot_executed", None)
    blockers.pop("prod_scoped_shadow_live_execution_pilot_passed", None)
    blockers.pop("prod_scoped_shadow_execution_authorized", None)
    blockers.pop("blockers_cleared_by_live_execution_pilot_run", None)
    blockers.pop("blockers_introduced_by_live_execution_pilot_run", None)
    blockers.pop("blockers_unchanged_by_live_execution_pilot_run", None)
    blockers.pop("blockers_changed_by_live_execution_pilot_run", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_LIVE_EXECUTION_GRANT_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_live_execution_grant")
    return payload


def _verify_live_execution_prod_source_read_flags(bundle: Mapping[str, Any]) -> None:
    allowed_true_paths = {
        "posture.live_prod_source_reads_performed",
        "execution.live_read_only_pilot_run.live_prod_source_reads_performed",
        "execution.live_read_only_pilot_run.observability_summary.live_prod_source_reads_performed",
        "execution.live_execution_pilot_run.live_prod_source_reads_performed",
        "execution.live_execution_pilot_run.observability_summary.live_prod_source_reads_performed",
    }
    execution = bundle.get("execution")
    if isinstance(execution, Mapping) and isinstance(execution.get("flag_enablement_pilot_run"), Mapping):
        allowed_true_paths.update(
            {
                "execution.flag_enablement_pilot_run.live_prod_source_reads_performed",
                "execution.flag_enablement_pilot_run.observability_summary.live_prod_source_reads_performed",
            }
        )
    observed_true_paths: set[str] = set()
    for path, value in _iter_named_field_values(bundle, "live_prod_source_reads_performed"):
        if path in allowed_true_paths:
            _require_true(path, value)
            observed_true_paths.add(path)
        else:
            _require_false(path, value)
    missing_true = sorted(allowed_true_paths - observed_true_paths)
    if missing_true:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live_prod_source_reads_performed missing true paths: " + ", ".join(missing_true)
        )


def _verify_live_execution_pilot_run_section(
    live_execution_pilot_run: Any,
    *,
    repo_root: Path,
    verify_local_files: bool = True,
) -> None:
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    pilot_run_id = live_execution_pilot_run.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run.pilot_run_id must be populated"
        )
    validate_pilot_run_id(pilot_run_id)
    if not pilot_run_id.startswith(f"{LIVE_EXECUTION_PILOT_RUN_ID_PREFIX}-"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run.pilot_run_id must use prod-live-exec prefix"
        )
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run.pilot_run_id must not contain harness"
        )
    pilot_dir_ref = live_execution_pilot_run.get("pilot_run_directory")
    if not isinstance(pilot_dir_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run.pilot_run_directory must be an object"
        )
    _require_equal(
        "execution.live_execution_pilot_run.pilot_run_directory.root_path",
        pilot_dir_ref.get("root_path"),
        PROD_SCOPED_SHADOW_ROOT,
    )
    _require_equal(
        "execution.live_execution_pilot_run.pilot_run_directory.relative_path",
        pilot_dir_ref.get("relative_path"),
        f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/",
    )
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    for index, record in enumerate(live_execution_pilot_run["files_written"]):
        local_file = pilot_dir / str(record["relative_path"])
        if verify_local_files and local_file.exists():
            _require_equal(
                f"execution.live_execution_pilot_run.files_written[{index}].sha256",
                _sha256_file(local_file),
                record.get("sha256"),
            )
            _require_equal(
                f"execution.live_execution_pilot_run.files_written[{index}].byte_count",
                local_file.stat().st_size,
                record.get("byte_count"),
            )


def _verify_live_execution_pilot_run_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_live_execution_pilot_run_payload(bundle),
        repo_root=repo_root,
        expect_live_execution_grant_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_executed",
        execution.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_live_execution_pilot_passed",
        execution.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _verify_live_execution_pilot_run_section(
        execution.get("live_execution_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_executed",
        posture.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_passed",
        posture.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        posture.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false("posture.missing_prod_scoped_shadow_live_execution_authorization", posture.get("missing_prod_scoped_shadow_live_execution_authorization"))
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    _verify_live_execution_prod_source_read_flags(bundle)
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_pilot_executed",
        blockers.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_pilot_passed",
        blockers.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_live_execution_pilot_run",
        blockers.get("blockers_cleared_by_live_execution_pilot_run"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_live_execution_pilot_run",
        blockers.get("blockers_introduced_by_live_execution_pilot_run"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_live_execution_pilot_run",
        blockers.get("blockers_unchanged_by_live_execution_pilot_run"),
    )
    if "blockers_changed_by_live_execution_pilot_run" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_live_execution_pilot_run must not be used"
        )
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE)
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_live_execution_pilot_run"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_execution_pilot_run caveats missing {caveat!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_live_execution_pilot_run",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _verify_live_execution_pilot_review_section(review: Any) -> None:
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _require_true(
        "review.prod_scoped_shadow_live_execution_pilot_reviewed",
        review.get("prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    accepted = review.get("prod_scoped_shadow_live_execution_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.prod_scoped_shadow_live_execution_pilot_accepted must be a boolean"
        )
    decision = review.get("live_execution_pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_execution_pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal(
        "review.live_execution_pilot_review_decision.decision",
        decision.get("decision"),
        expected_decision,
    )
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_execution_pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_execution_pilot_review_decision.checks must be an object"
        )
    if set(checks) != set(LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_execution_pilot_review_decision.checks must align with "
            "LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS"
        )
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.live_execution_pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS:
        if not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_execution_pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal(
        "review.live_execution_pilot_review_decision.failed_review_checks",
        sorted(failed),
        expected_failed,
    )
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted live execution pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted live execution pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_execution_pilot_review_decision.{field} must be a non-empty string list"
            )
    limitations_text = " ".join(str(item).lower() for item in decision.get("limitations", []))
    for phrase in ("no runtime rerun", "no shadow-runs", "no database connection", "global online shadow execution"):
        if phrase not in limitations_text:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.live_execution_pilot_review_decision.limitations must include {phrase!r}"
            )


def _without_live_execution_pilot_review_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_PILOT_RUN_BUNDLE_REVISION
    payload["metadata"] = metadata
    review = deepcopy(dict(payload.get("review") or {}))
    review.pop("prod_scoped_shadow_live_execution_pilot_reviewed", None)
    review.pop("prod_scoped_shadow_live_execution_pilot_accepted", None)
    review.pop("live_execution_pilot_review_decision", None)
    payload["review"] = review
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_live_execution_pilot_reviewed", None)
    posture.pop("prod_scoped_shadow_live_execution_pilot_accepted", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_live_execution_pilot_reviewed", None)
    blockers.pop("prod_scoped_shadow_live_execution_pilot_accepted", None)
    blockers.pop("blockers_cleared_by_live_execution_pilot_review", None)
    blockers.pop("blockers_introduced_by_live_execution_pilot_review", None)
    blockers.pop("blockers_unchanged_by_live_execution_pilot_review", None)
    blockers.pop("blockers_changed_by_live_execution_pilot_review", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_LIVE_EXECUTION_PILOT_RUN_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_live_execution_pilot_run")
    return payload


def _verify_live_execution_pilot_review_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_LIVE_EXECUTION_PILOT_REVIEW_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_live_execution_pilot_review_payload(bundle),
        repo_root=repo_root,
        expect_live_execution_pilot_run_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    review = bundle.get("review")
    _verify_live_execution_pilot_review_section(review)
    accepted = _get(bundle, "review.prod_scoped_shadow_live_execution_pilot_accepted")
    expected_next = (
        POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted is True
        else POST_LIVE_EXECUTION_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), expected_next)
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_reviewed",
        posture.get("prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    _require_equal(
        "posture.prod_scoped_shadow_live_execution_pilot_accepted",
        posture.get("prod_scoped_shadow_live_execution_pilot_accepted"),
        accepted,
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_executed",
        posture.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_pilot_passed",
        posture.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        posture.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_requested",
        authorization.get("prod_scoped_shadow_live_execution_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorization_granted",
        authorization.get("prod_scoped_shadow_live_execution_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_pilot_reviewed",
        blockers.get("prod_scoped_shadow_live_execution_pilot_reviewed"),
    )
    _require_equal(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_pilot_accepted",
        blockers.get("prod_scoped_shadow_live_execution_pilot_accepted"),
        accepted,
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_pilot_executed",
        blockers.get("prod_scoped_shadow_live_execution_pilot_executed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_pilot_passed",
        blockers.get("prod_scoped_shadow_live_execution_pilot_passed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_live_execution_pilot_review",
        blockers.get("blockers_cleared_by_live_execution_pilot_review"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_live_execution_pilot_review",
        blockers.get("blockers_introduced_by_live_execution_pilot_review"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_live_execution_pilot_review",
        blockers.get("blockers_unchanged_by_live_execution_pilot_review"),
    )
    if "blockers_changed_by_live_execution_pilot_review" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_live_execution_pilot_review must not be used"
        )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_live_execution_pilot_review"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_execution_pilot_review caveats missing {caveat!r}"
            )
    stale_caveats = (
        "Review is required before any further enablement chain.",
    )
    for caveat in stale_caveats:
        if caveat in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_live_execution_pilot_review caveats must not include stale caveat {caveat!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_live_execution_pilot_review",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _verify_flag_enablement_pilot_review_section(review: Any) -> None:
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _require_true(
        "review.prod_scoped_shadow_flag_enablement_pilot_reviewed",
        review.get("prod_scoped_shadow_flag_enablement_pilot_reviewed"),
    )
    accepted = review.get("prod_scoped_shadow_flag_enablement_pilot_accepted")
    if not isinstance(accepted, bool):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.prod_scoped_shadow_flag_enablement_pilot_accepted must be a boolean"
        )
    decision = review.get("flag_enablement_pilot_review_decision")
    if not isinstance(decision, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision must be an object"
        )
    expected_decision = "accepted" if accepted else "not_accepted"
    _require_equal(
        "review.flag_enablement_pilot_review_decision.decision",
        decision.get("decision"),
        expected_decision,
    )
    for field in ("reviewer", "reviewed_at"):
        if not isinstance(decision.get(field), str) or not decision.get(field):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.flag_enablement_pilot_review_decision.{field} must be populated"
            )
    checks = decision.get("checks")
    if not isinstance(checks, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision.checks must be an object"
        )
    if set(checks) != set(FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision.checks must align with "
            "FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS"
        )
    failed = decision.get("failed_review_checks")
    if not isinstance(failed, list) or not all(isinstance(item, str) for item in failed):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision.failed_review_checks must be a string list"
        )
    for check_name in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS:
        if not isinstance(checks.get(check_name), bool):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.flag_enablement_pilot_review_decision.checks.{check_name} must be a boolean"
            )
    expected_failed = sorted(name for name in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS if checks.get(name) is False)
    _require_equal(
        "review.flag_enablement_pilot_review_decision.failed_review_checks",
        sorted(failed),
        expected_failed,
    )
    if accepted and failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "accepted flag enablement pilot review must have no failed checks"
        )
    if not accepted and not failed:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "not_accepted flag enablement pilot review must list failed checks"
        )
    for field in ("accepted_evidence", "limitations"):
        value = decision.get(field)
        if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.flag_enablement_pilot_review_decision.{field} must be a non-empty string list"
            )
    limitations_text = " ".join(str(item).lower() for item in decision.get("limitations", []))
    for phrase in ("no runtime rerun", "no shadow-runs", "no database connection"):
        if phrase not in limitations_text:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"review.flag_enablement_pilot_review_decision.limitations must include {phrase!r}"
            )
    if "global online shadow execution" not in limitations_text and "global/fleet" not in limitations_text:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision.limitations must mention global online shadow execution "
            "or global/fleet scope"
        )
    if "production default" not in limitations_text or "user-visible" not in limitations_text:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "review.flag_enablement_pilot_review_decision.limitations must mention production default and "
            "user-visible unchanged scope"
        )


def _without_flag_enablement_pilot_review_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_PILOT_RUN_BUNDLE_REVISION
    payload["metadata"] = metadata
    review = deepcopy(dict(payload.get("review") or {}))
    review.pop("prod_scoped_shadow_flag_enablement_pilot_reviewed", None)
    review.pop("prod_scoped_shadow_flag_enablement_pilot_accepted", None)
    review.pop("flag_enablement_pilot_review_decision", None)
    payload["review"] = review
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_flag_enablement_pilot_reviewed", None)
    posture.pop("prod_scoped_shadow_flag_enablement_pilot_accepted", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_flag_enablement_pilot_reviewed", None)
    blockers.pop("prod_scoped_shadow_flag_enablement_pilot_accepted", None)
    blockers.pop("blockers_cleared_by_flag_enablement_pilot_review", None)
    blockers.pop("blockers_introduced_by_flag_enablement_pilot_review", None)
    blockers.pop("blockers_unchanged_by_flag_enablement_pilot_review", None)
    blockers.pop("blockers_changed_by_flag_enablement_pilot_review", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_FLAG_ENABLEMENT_PILOT_RUN_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_flag_enablement_pilot_run")
    return payload


def _verify_flag_enablement_pilot_review_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_FLAG_ENABLEMENT_PILOT_REVIEW_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_flag_enablement_pilot_review_payload(bundle),
        repo_root=repo_root,
        expect_flag_enablement_pilot_run_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    review = bundle.get("review")
    _verify_flag_enablement_pilot_review_section(review)
    accepted = _get(bundle, "review.prod_scoped_shadow_flag_enablement_pilot_accepted")
    expected_next = (
        POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
        if accepted is True
        else POST_FLAG_ENABLEMENT_PILOT_REVIEW_REJECTED_NEXT_STAGE
    )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), expected_next)
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_reviewed",
        posture.get("prod_scoped_shadow_flag_enablement_pilot_reviewed"),
    )
    _require_equal(
        "posture.prod_scoped_shadow_flag_enablement_pilot_accepted",
        posture.get("prod_scoped_shadow_flag_enablement_pilot_accepted"),
        accepted,
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_executed",
        posture.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_pilot_passed",
        posture.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_requested",
        posture.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_granted",
        posture.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorized",
        posture.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        posture.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        posture.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_pilot_reviewed",
        blockers.get("prod_scoped_shadow_flag_enablement_pilot_reviewed"),
    )
    _require_equal(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_pilot_accepted",
        blockers.get("prod_scoped_shadow_flag_enablement_pilot_accepted"),
        accepted,
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_pilot_executed",
        blockers.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_pilot_passed",
        blockers.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_requested",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_granted",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorized",
        blockers.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_flag_enablement_authorization",
        blockers.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.online_shadow_execution_enabled",
        blockers.get("online_shadow_execution_enabled"),
    )
    _require_false("shadow_and_production_blockers.production_default_allowed", blockers.get("production_default_allowed"))
    _require_false("shadow_and_production_blockers.api_web_changes_allowed", blockers.get("api_web_changes_allowed"))
    _require_false(
        "shadow_and_production_blockers.user_visible_ranking_changed",
        blockers.get("user_visible_ranking_changed"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_flag_enablement_pilot_review",
        blockers.get("blockers_cleared_by_flag_enablement_pilot_review"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_flag_enablement_pilot_review",
        blockers.get("blockers_introduced_by_flag_enablement_pilot_review"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_flag_enablement_pilot_review",
        blockers.get("blockers_unchanged_by_flag_enablement_pilot_review"),
    )
    if "blockers_changed_by_flag_enablement_pilot_review" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_flag_enablement_pilot_review must not be used"
        )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _verify_flag_enablement_pilot_run_section(
        execution.get("flag_enablement_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_live_read_only_pilot_run_section(
        execution.get("live_read_only_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_live_execution_pilot_run_section(
        execution.get("live_execution_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_flag_enablement_pilot_review"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_flag_enablement_pilot_review caveats missing {caveat!r}"
            )
    stale_caveats = ("Review is required before any further enablement chain.",)
    for caveat in stale_caveats:
        if caveat in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_flag_enablement_pilot_review caveats must not include stale caveat {caveat!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_flag_enablement_pilot_review",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_production_default_api_user_visible_request_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_PILOT_REVIEW_BUNDLE_REVISION
    payload["metadata"] = metadata
    authorization = deepcopy(dict(payload.get("authorization") or {}))
    authorization.pop("prod_scoped_shadow_production_default_api_user_visible_authorization_requested", None)
    authorization.pop("prod_scoped_shadow_production_default_api_user_visible_authorization_granted", None)
    authorization.pop("prod_scoped_shadow_production_default_api_user_visible_authorized", None)
    authorization.pop("production_default_api_user_visible_request_decision", None)
    authorization.pop("production_default_api_user_visible_requested_scope", None)
    payload["authorization"] = authorization
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_production_default_api_user_visible_authorization_requested", None)
    posture.pop("prod_scoped_shadow_production_default_api_user_visible_authorization_granted", None)
    posture.pop("prod_scoped_shadow_production_default_api_user_visible_authorized", None)
    posture.pop("missing_prod_scoped_shadow_production_default_api_user_visible_authorization", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_production_default_api_user_visible_authorization_requested", None)
    blockers.pop("prod_scoped_shadow_production_default_api_user_visible_authorization_granted", None)
    blockers.pop("prod_scoped_shadow_production_default_api_user_visible_authorized", None)
    blockers.pop("missing_prod_scoped_shadow_production_default_api_user_visible_authorization", None)
    blockers.pop("live_prod_source_reads_performed", None)
    blockers.pop("writes_performed", None)
    blockers.pop("runtime_writes_performed", None)
    blockers.pop("blockers_introduced_by_production_default_api_user_visible_request", None)
    blockers.pop("blockers_cleared_by_production_default_api_user_visible_request", None)
    blockers.pop("blockers_unchanged_by_production_default_api_user_visible_request", None)
    blockers.pop("blockers_changed_by_production_default_api_user_visible_request", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_FLAG_ENABLEMENT_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_flag_enablement_pilot_review")
    return payload


def _verify_production_default_api_user_visible_request_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_production_default_api_user_visible_request_payload(bundle),
        repo_root=repo_root,
        expect_flag_enablement_pilot_review_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_production_default_api_user_visible_request_section(authorization)

    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _verify_live_execution_pilot_review_section(review)
    _verify_flag_enablement_pilot_review_section(review)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_executed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_flag_enablement_pilot_passed",
        execution.get("prod_scoped_shadow_flag_enablement_pilot_passed"),
    )
    _verify_live_read_only_pilot_run_section(
        execution.get("live_read_only_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_live_execution_pilot_run_section(
        execution.get("live_execution_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_flag_enablement_pilot_run_section(
        execution.get("flag_enablement_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )

    for prefix, section in (
        ("posture", bundle.get("posture")),
        ("shadow_and_production_blockers", bundle.get("shadow_and_production_blockers")),
    ):
        if not isinstance(section, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"{prefix} must be an object")
        _require_true(
            f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
            section.get("prod_scoped_shadow_production_default_api_user_visible_authorization_requested"),
        )
        _require_false(
            f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
            section.get("prod_scoped_shadow_production_default_api_user_visible_authorization_granted"),
        )
        _require_false(
            f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_authorized",
            section.get("prod_scoped_shadow_production_default_api_user_visible_authorized"),
        )
        _require_true(
            f"{prefix}.missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
            section.get("missing_prod_scoped_shadow_production_default_api_user_visible_authorization"),
        )
        for field in (
            "prod_scoped_shadow_flag_enablement_pilot_reviewed",
            "prod_scoped_shadow_flag_enablement_pilot_accepted",
            "prod_scoped_shadow_flag_enablement_authorized",
            "prod_scoped_shadow_live_execution_authorized",
            "prod_scoped_shadow_live_read_only_execution_authorized",
        ):
            _require_true(f"{prefix}.{field}", section.get(field))
        if prefix == "posture" or "live_prod_source_reads_performed" in section:
            _require_true(
                f"{prefix}.live_prod_source_reads_performed",
                section.get("live_prod_source_reads_performed"),
            )
        for field in (
            "missing_prod_scoped_shadow_flag_enablement_authorization",
            "missing_prod_scoped_shadow_live_execution_authorization",
            "prod_scoped_shadow_execution_authorized",
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ):
            if field in section:
                _require_false(f"{prefix}.{field}", section.get(field))
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_production_default_api_user_visible_request",
        blockers.get("blockers_introduced_by_production_default_api_user_visible_request"),
        ["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_production_default_api_user_visible_request",
        blockers.get("blockers_cleared_by_production_default_api_user_visible_request"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_production_default_api_user_visible_request",
        blockers.get("blockers_unchanged_by_production_default_api_user_visible_request"),
    )
    if "blockers_changed_by_production_default_api_user_visible_request" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_production_default_api_user_visible_request "
            "must not be used"
        )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(bundle, "plan.production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE,
    )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_production_default_api_user_visible_request"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_production_default_api_user_visible_request caveats missing {caveat!r}"
            )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_production_default_api_user_visible_request",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_production_default_api_user_visible_grant_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_BUNDLE_REVISION
    payload["metadata"] = metadata
    authorization = deepcopy(dict(payload.get("authorization") or {}))
    authorization["prod_scoped_shadow_production_default_api_user_visible_authorization_granted"] = False
    authorization["prod_scoped_shadow_production_default_api_user_visible_authorized"] = False
    authorization.pop("production_default_api_user_visible_grant_decision", None)
    authorization.pop("production_default_api_user_visible_granted_scope", None)
    if "explicitly_not_included" in authorization:
        authorization["explicitly_not_included"] = sorted(
            set(authorization["explicitly_not_included"])
            - set(PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED)
        )
    payload["authorization"] = authorization
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture["prod_scoped_shadow_production_default_api_user_visible_authorization_granted"] = False
    posture["prod_scoped_shadow_production_default_api_user_visible_authorized"] = False
    posture["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"] = True
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers["prod_scoped_shadow_production_default_api_user_visible_authorization_granted"] = False
    blockers["prod_scoped_shadow_production_default_api_user_visible_authorized"] = False
    blockers["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"] = True
    blockers["blockers_introduced_by_production_default_api_user_visible_request"] = [
        "missing_prod_scoped_shadow_production_default_api_user_visible_authorization"
    ]
    blockers["blockers_cleared_by_production_default_api_user_visible_request"] = []
    blockers["blockers_unchanged_by_production_default_api_user_visible_request"] = True
    blockers.pop("blockers_cleared_by_production_default_api_user_visible_grant", None)
    blockers.pop("blockers_introduced_by_production_default_api_user_visible_grant", None)
    blockers.pop("blockers_unchanged_by_production_default_api_user_visible_grant", None)
    blockers.pop("blockers_changed_by_production_default_api_user_visible_grant", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_production_default_api_user_visible_request")
    return payload


def _verify_production_default_api_user_visible_grant_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_production_default_api_user_visible_grant_payload(bundle),
        repo_root=repo_root,
        expect_production_default_api_user_visible_request_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_production_default_api_user_visible_request_section(authorization)
    _verify_production_default_api_user_visible_grant_section(authorization)

    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _verify_live_execution_pilot_review_section(review)
    _verify_flag_enablement_pilot_review_section(review)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    if "production_default_api_user_visible_pilot_run" in execution:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.production_default_api_user_visible_pilot_run must not be present "
            "at post_production_default_api_user_visible_grant"
        )
    _verify_live_read_only_pilot_run_section(
        execution.get("live_read_only_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_live_execution_pilot_run_section(
        execution.get("live_execution_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_flag_enablement_pilot_run_section(
        execution.get("flag_enablement_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )

    for prefix, section in (
        ("posture", bundle.get("posture")),
        ("shadow_and_production_blockers", bundle.get("shadow_and_production_blockers")),
    ):
        if not isinstance(section, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"{prefix} must be an object")
        _require_true(
            f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
            section.get("prod_scoped_shadow_production_default_api_user_visible_authorization_requested"),
        )
        _require_true(
            f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
            section.get("prod_scoped_shadow_production_default_api_user_visible_authorization_granted"),
        )
        _require_true(
            f"{prefix}.prod_scoped_shadow_production_default_api_user_visible_authorized",
            section.get("prod_scoped_shadow_production_default_api_user_visible_authorized"),
        )
        _require_false(
            f"{prefix}.missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
            section.get("missing_prod_scoped_shadow_production_default_api_user_visible_authorization"),
        )
        for field in (
            "prod_scoped_shadow_flag_enablement_authorized",
            "prod_scoped_shadow_live_execution_authorized",
            "prod_scoped_shadow_live_read_only_execution_authorized",
        ):
            _require_true(f"{prefix}.{field}", section.get(field))
        if prefix == "posture" or "live_prod_source_reads_performed" in section:
            _require_true(
                f"{prefix}.live_prod_source_reads_performed",
                section.get("live_prod_source_reads_performed"),
            )
        for field in (
            "missing_prod_scoped_shadow_flag_enablement_authorization",
            "missing_prod_scoped_shadow_live_execution_authorization",
            "prod_scoped_shadow_execution_authorized",
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ):
            if field in section:
                _require_false(f"{prefix}.{field}", section.get(field))
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_production_default_api_user_visible_grant",
        blockers.get("blockers_cleared_by_production_default_api_user_visible_grant"),
        ["missing_prod_scoped_shadow_production_default_api_user_visible_authorization"],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_production_default_api_user_visible_grant",
        blockers.get("blockers_introduced_by_production_default_api_user_visible_grant"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_production_default_api_user_visible_grant",
        blockers.get("blockers_unchanged_by_production_default_api_user_visible_grant"),
    )
    if "blockers_changed_by_production_default_api_user_visible_grant" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_production_default_api_user_visible_grant "
            "must not be used"
        )
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.production_default_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        _get(bundle, "plan.production_default_api_user_visible_separation.api_web_changes_allowed"),
    )
    _require_false(
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
        _get(bundle, "plan.production_default_api_user_visible_separation.user_visible_ranking_changed"),
    )
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE,
    )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_production_default_api_user_visible_grant"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_production_default_api_user_visible_grant caveats missing {caveat!r}"
            )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_production_default_api_user_visible_grant",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_production_default_api_user_visible_pilot_run_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_BUNDLE_REVISION
    payload["metadata"] = metadata
    execution = deepcopy(dict(payload.get("execution") or {}))
    execution.pop("prod_scoped_shadow_production_default_api_user_visible_pilot_executed", None)
    execution.pop("prod_scoped_shadow_production_default_api_user_visible_pilot_passed", None)
    execution.pop("production_default_api_user_visible_pilot_run", None)
    payload["execution"] = execution
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_production_default_api_user_visible_pilot_executed", None)
    posture.pop("prod_scoped_shadow_production_default_api_user_visible_pilot_passed", None)
    posture["prod_scoped_shadow_execution_authorized"] = False
    posture["online_shadow_execution_enabled"] = False
    posture["production_default_allowed"] = False
    posture["api_web_changes_allowed"] = False
    posture["user_visible_ranking_changed"] = False
    posture["writes_performed"] = False
    posture["runtime_writes_performed"] = False
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_production_default_api_user_visible_pilot_executed", None)
    blockers.pop("prod_scoped_shadow_production_default_api_user_visible_pilot_passed", None)
    blockers["prod_scoped_shadow_execution_authorized"] = False
    blockers["online_shadow_execution_enabled"] = False
    blockers["production_default_allowed"] = False
    blockers["api_web_changes_allowed"] = False
    blockers["user_visible_ranking_changed"] = False
    blockers.pop("blockers_cleared_by_production_default_api_user_visible_pilot_run", None)
    blockers.pop("blockers_introduced_by_production_default_api_user_visible_pilot_run", None)
    blockers.pop("blockers_unchanged_by_production_default_api_user_visible_pilot_run", None)
    blockers.pop("blockers_changed_by_production_default_api_user_visible_pilot_run", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["writes_performed"] = False
    payload["runtime_writes_performed"] = False
    payload["recommended_next_stage"] = POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_production_default_api_user_visible_grant")
    return payload


def _verify_production_default_api_user_visible_pilot_run_section(
    production_default_api_user_visible_pilot_run: Any,
    *,
    repo_root: Path,
    verify_local_files: bool = True,
) -> None:
    if not isinstance(production_default_api_user_visible_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.production_default_api_user_visible_pilot_run must be an object"
        )
    _validate_production_default_api_user_visible_pilot_run_slice(
        production_default_api_user_visible_pilot_run
    )
    pilot_run_id = production_default_api_user_visible_pilot_run.get("pilot_run_id")
    if not isinstance(pilot_run_id, str) or not pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.production_default_api_user_visible_pilot_run.pilot_run_id must be populated"
        )
    validate_pilot_run_id(pilot_run_id)
    if not pilot_run_id.startswith(f"{PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_ID_PREFIX}-"):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.production_default_api_user_visible_pilot_run.pilot_run_id must use prod-output prefix"
        )
    if "harness" in pilot_run_id:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.production_default_api_user_visible_pilot_run.pilot_run_id must not contain harness"
        )
    pilot_dir_ref = production_default_api_user_visible_pilot_run.get("pilot_run_directory")
    if not isinstance(pilot_dir_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.production_default_api_user_visible_pilot_run.pilot_run_directory must be an object"
        )
    _require_equal(
        "execution.production_default_api_user_visible_pilot_run.pilot_run_directory.root_path",
        pilot_dir_ref.get("root_path"),
        PROD_SCOPED_SHADOW_ROOT,
    )
    _require_equal(
        "execution.production_default_api_user_visible_pilot_run.pilot_run_directory.relative_path",
        pilot_dir_ref.get("relative_path"),
        f"{PROD_SCOPED_SHADOW_ROOT}{pilot_run_id}/",
    )
    pilot_dir = resolve_prod_scoped_pilot_directory(repo_root, pilot_run_id)
    for index, record in enumerate(production_default_api_user_visible_pilot_run["files_written"]):
        local_file = pilot_dir / str(record["relative_path"])
        if verify_local_files and local_file.exists():
            _require_equal(
                f"execution.production_default_api_user_visible_pilot_run.files_written[{index}].sha256",
                _sha256_file(local_file),
                record.get("sha256"),
            )
            _require_equal(
                f"execution.production_default_api_user_visible_pilot_run.files_written[{index}].byte_count",
                local_file.stat().st_size,
                record.get("byte_count"),
            )


def _verify_production_default_api_user_visible_pilot_run_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_production_default_api_user_visible_pilot_run_payload(bundle),
        repo_root=repo_root,
        expect_production_default_api_user_visible_grant_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_production_default_api_user_visible_authorized",
        authorization.get("prod_scoped_shadow_production_default_api_user_visible_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_production_default_api_user_visible_request_section(authorization)
    _verify_production_default_api_user_visible_grant_section(authorization)
    _verify_live_execution_pilot_review_section(bundle.get("review"))
    _verify_flag_enablement_pilot_review_section(bundle.get("review"))

    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_true(
        "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_executed",
        execution.get("prod_scoped_shadow_production_default_api_user_visible_pilot_executed"),
    )
    _require_true(
        "execution.prod_scoped_shadow_production_default_api_user_visible_pilot_passed",
        execution.get("prod_scoped_shadow_production_default_api_user_visible_pilot_passed"),
    )
    _verify_live_read_only_pilot_run_section(
        execution.get("live_read_only_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_live_execution_pilot_run_section(
        execution.get("live_execution_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_flag_enablement_pilot_run_section(
        execution.get("flag_enablement_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )
    _verify_production_default_api_user_visible_pilot_run_section(
        execution.get("production_default_api_user_visible_pilot_run"),
        repo_root=repo_root,
        verify_local_files=verify_local_pilot_files,
    )

    for prefix, section in (
        ("posture", bundle.get("posture")),
        ("shadow_and_production_blockers", bundle.get("shadow_and_production_blockers")),
    ):
        if not isinstance(section, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError(f"{prefix} must be an object")
        for field in (
            "prod_scoped_shadow_production_default_api_user_visible_pilot_executed",
            "prod_scoped_shadow_production_default_api_user_visible_pilot_passed",
            "prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
            "prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
            "prod_scoped_shadow_production_default_api_user_visible_authorized",
            "prod_scoped_shadow_flag_enablement_authorized",
            "prod_scoped_shadow_live_execution_authorized",
            "prod_scoped_shadow_live_read_only_execution_authorized",
            "live_prod_source_reads_performed",
        ):
            _require_true(f"{prefix}.{field}", section.get(field))
        for field in (
            "missing_prod_scoped_shadow_production_default_api_user_visible_authorization",
            "missing_prod_scoped_shadow_flag_enablement_authorization",
            "missing_prod_scoped_shadow_live_execution_authorization",
            "prod_scoped_shadow_execution_authorized",
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
        ):
            if field in section:
                _require_false(f"{prefix}.{field}", section.get(field))
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_production_default_api_user_visible_pilot_run",
        blockers.get("blockers_cleared_by_production_default_api_user_visible_pilot_run"),
        [],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_production_default_api_user_visible_pilot_run",
        blockers.get("blockers_introduced_by_production_default_api_user_visible_pilot_run"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_production_default_api_user_visible_pilot_run",
        blockers.get("blockers_unchanged_by_production_default_api_user_visible_pilot_run"),
    )
    if "blockers_changed_by_production_default_api_user_visible_pilot_run" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_production_default_api_user_visible_pilot_run "
            "must not be used"
        )
    for path in (
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        "plan.production_default_api_user_visible_separation.production_default_allowed",
        "plan.production_default_api_user_visible_separation.api_web_changes_allowed",
        "plan.production_default_api_user_visible_separation.user_visible_ranking_changed",
    ):
        _require_false(path, _get(bundle, path))
    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    _require_named_flags_not_true(
        bundle,
        (
            "online_shadow_execution_enabled",
            "production_default_allowed",
            "api_web_changes_allowed",
            "user_visible_ranking_changed",
            "writes_performed",
            "runtime_writes_performed",
            "prod_scoped_shadow_execution_authorized",
        ),
    )
    _require_equal(
        "recommended_next_stage",
        bundle.get("recommended_next_stage"),
        POST_PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_NEXT_STAGE,
    )
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_production_default_api_user_visible_pilot_run"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_production_default_api_user_visible_pilot_run caveats missing {caveat!r}"
            )
    for item in PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_STILL_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_production_default_api_user_visible_pilot_run",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_flag_enablement_request_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_LIVE_EXECUTION_PILOT_REVIEW_BUNDLE_REVISION
    payload["metadata"] = metadata
    authorization = deepcopy(dict(payload.get("authorization") or {}))
    authorization.pop("prod_scoped_shadow_flag_enablement_authorization_requested", None)
    authorization.pop("prod_scoped_shadow_flag_enablement_authorization_granted", None)
    authorization.pop("prod_scoped_shadow_flag_enablement_authorized", None)
    authorization.pop("flag_enablement_request_decision", None)
    authorization.pop("flag_enablement_requested_scope", None)
    payload["authorization"] = authorization
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture.pop("prod_scoped_shadow_flag_enablement_authorization_requested", None)
    posture.pop("prod_scoped_shadow_flag_enablement_authorization_granted", None)
    posture.pop("prod_scoped_shadow_flag_enablement_authorized", None)
    posture.pop("missing_prod_scoped_shadow_flag_enablement_authorization", None)
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers.pop("prod_scoped_shadow_flag_enablement_authorization_requested", None)
    blockers.pop("prod_scoped_shadow_flag_enablement_authorization_granted", None)
    blockers.pop("prod_scoped_shadow_flag_enablement_authorized", None)
    blockers.pop("missing_prod_scoped_shadow_flag_enablement_authorization", None)
    blockers.pop("blockers_cleared_by_flag_enablement_request", None)
    blockers.pop("blockers_introduced_by_flag_enablement_request", None)
    blockers.pop("blockers_unchanged_by_flag_enablement_request", None)
    blockers.pop("blockers_changed_by_flag_enablement_request", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_LIVE_EXECUTION_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_live_execution_pilot_review")
    return payload


def _verify_flag_enablement_request_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_FLAG_ENABLEMENT_REQUEST_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_flag_enablement_request_payload(bundle),
        repo_root=repo_root,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _verify_flag_enablement_request_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _verify_live_execution_pilot_review_section(review)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    live_execution_pilot_run = execution.get("live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_requested",
        posture.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_false(
        "posture.prod_scoped_shadow_flag_enablement_authorization_granted",
        posture.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_false(
        "posture.prod_scoped_shadow_flag_enablement_authorized",
        posture.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        posture.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        posture.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_requested",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_granted",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorized",
        blockers.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_true(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_flag_enablement_authorization",
        blockers.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_flag_enablement_request",
        blockers.get("blockers_introduced_by_flag_enablement_request"),
        ["missing_prod_scoped_shadow_flag_enablement_authorization"],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_flag_enablement_request",
        blockers.get("blockers_cleared_by_flag_enablement_request"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_flag_enablement_request",
        blockers.get("blockers_unchanged_by_flag_enablement_request"),
    )
    if "blockers_changed_by_flag_enablement_request" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_flag_enablement_request must not be used"
        )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE)
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_flag_enablement_request"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_flag_enablement_request caveats missing {caveat!r}"
            )
    for item in FLAG_ENABLEMENT_REQUEST_EXPLICITLY_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_flag_enablement_request",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def _without_flag_enablement_grant_payload(bundle: Mapping[str, Any]) -> dict[str, Any]:
    payload = deepcopy(dict(bundle))
    metadata = deepcopy(dict(payload.get("metadata") or {}))
    metadata["bundle_revision"] = POST_FLAG_ENABLEMENT_REQUEST_BUNDLE_REVISION
    payload["metadata"] = metadata
    authorization = deepcopy(dict(payload.get("authorization") or {}))
    authorization["prod_scoped_shadow_flag_enablement_authorization_granted"] = False
    authorization["prod_scoped_shadow_flag_enablement_authorized"] = False
    authorization.pop("flag_enablement_grant_decision", None)
    authorization.pop("flag_enablement_granted_scope", None)
    if "explicitly_not_included" in authorization:
        authorization["explicitly_not_included"] = sorted(
            set(authorization["explicitly_not_included"]) - set(FLAG_ENABLEMENT_GRANT_ONLY_EXPLICITLY_NOT_INCLUDED)
        )
    payload["authorization"] = authorization
    posture = deepcopy(dict(payload.get("posture") or {}))
    posture["prod_scoped_shadow_flag_enablement_authorization_granted"] = False
    posture["prod_scoped_shadow_flag_enablement_authorized"] = False
    posture["missing_prod_scoped_shadow_flag_enablement_authorization"] = True
    payload["posture"] = posture
    blockers = deepcopy(dict(payload.get("shadow_and_production_blockers") or {}))
    blockers["prod_scoped_shadow_flag_enablement_authorization_granted"] = False
    blockers["prod_scoped_shadow_flag_enablement_authorized"] = False
    blockers["missing_prod_scoped_shadow_flag_enablement_authorization"] = True
    blockers.pop("blockers_cleared_by_flag_enablement_grant", None)
    blockers.pop("blockers_introduced_by_flag_enablement_grant", None)
    blockers.pop("blockers_unchanged_by_flag_enablement_grant", None)
    blockers.pop("blockers_changed_by_flag_enablement_grant", None)
    payload["shadow_and_production_blockers"] = blockers
    payload["recommended_next_stage"] = POST_FLAG_ENABLEMENT_REQUEST_NEXT_STAGE
    payload["caveats"] = _caveats(mode="post_flag_enablement_request")
    return payload


def _verify_flag_enablement_grant_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path,
    verify_local_pilot_files: bool,
) -> dict[str, Any]:
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal(
        "metadata.bundle_revision",
        metadata.get("bundle_revision"),
        POST_FLAG_ENABLEMENT_GRANT_BUNDLE_REVISION,
    )
    base_result = verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        _without_flag_enablement_grant_payload(bundle),
        repo_root=repo_root,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=verify_local_pilot_files,
    )
    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_requested",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_requested"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorization_granted",
        authorization.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_flag_enablement_authorized",
        authorization.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_execution_authorized",
        authorization.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
        authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _verify_flag_enablement_request_section(authorization)
    _verify_flag_enablement_grant_section(authorization)
    _verify_live_read_only_request_section(authorization)
    _verify_live_read_only_grant_section(authorization)
    _verify_live_execution_request_section(authorization)
    _verify_live_execution_grant_section(authorization)
    review = bundle.get("review")
    if not isinstance(review, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("review must be an object")
    _verify_live_execution_pilot_review_section(review)
    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    if "flag_enablement_pilot_run" in execution:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.flag_enablement_pilot_run must not be present at post_flag_enablement_grant"
        )
    live_execution_pilot_run = execution.get("live_execution_pilot_run")
    if not isinstance(live_execution_pilot_run, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "execution.live_execution_pilot_run must be an object"
        )
    _validate_live_execution_pilot_run_slice(live_execution_pilot_run)
    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorization_granted",
        posture.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "posture.prod_scoped_shadow_flag_enablement_authorized",
        posture.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_flag_enablement_authorization",
        posture.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_execution_authorized",
        posture.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "posture.missing_prod_scoped_shadow_live_execution_authorization",
        posture.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false(
        "posture.prod_scoped_shadow_execution_authorized",
        posture.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_true(
        "posture.prod_scoped_shadow_live_read_only_execution_authorized",
        posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
    )
    _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    _require_false("posture.online_shadow_execution_enabled", posture.get("online_shadow_execution_enabled"))
    _require_false("posture.production_default_allowed", posture.get("production_default_allowed"))
    _require_false("posture.api_web_changes_allowed", posture.get("api_web_changes_allowed"))
    _require_false("posture.user_visible_ranking_changed", posture.get("user_visible_ranking_changed"))
    _require_false("posture.writes_performed", posture.get("writes_performed"))
    _require_false("posture.runtime_writes_performed", posture.get("runtime_writes_performed"))
    _require_false(
        "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now",
        _get(bundle, "plan.feature_flag_iam_config_requirements.prod_scoped_flag_enablement_authorized_now"),
    )
    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorization_granted",
        blockers.get("prod_scoped_shadow_flag_enablement_authorization_granted"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_flag_enablement_authorized",
        blockers.get("prod_scoped_shadow_flag_enablement_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_flag_enablement_authorization",
        blockers.get("missing_prod_scoped_shadow_flag_enablement_authorization"),
    )
    _require_true(
        "shadow_and_production_blockers.prod_scoped_shadow_live_execution_authorized",
        blockers.get("prod_scoped_shadow_live_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_execution_authorized",
        blockers.get("prod_scoped_shadow_execution_authorized"),
    )
    _require_false(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_live_execution_authorization",
        blockers.get("missing_prod_scoped_shadow_live_execution_authorization"),
    )
    _require_false("shadow_and_production_blockers.online_shadow_execution_enabled", blockers.get("online_shadow_execution_enabled"))
    _require_false("shadow_and_production_blockers.production_default_allowed", blockers.get("production_default_allowed"))
    _require_false("shadow_and_production_blockers.api_web_changes_allowed", blockers.get("api_web_changes_allowed"))
    _require_false(
        "shadow_and_production_blockers.user_visible_ranking_changed",
        blockers.get("user_visible_ranking_changed"),
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_cleared_by_flag_enablement_grant",
        blockers.get("blockers_cleared_by_flag_enablement_grant"),
        ["missing_prod_scoped_shadow_flag_enablement_authorization"],
    )
    _require_equal(
        "shadow_and_production_blockers.blockers_introduced_by_flag_enablement_grant",
        blockers.get("blockers_introduced_by_flag_enablement_grant"),
        [],
    )
    _require_true(
        "shadow_and_production_blockers.blockers_unchanged_by_flag_enablement_grant",
        blockers.get("blockers_unchanged_by_flag_enablement_grant"),
    )
    if "blockers_changed_by_flag_enablement_grant" in blockers:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "shadow_and_production_blockers.blockers_changed_by_flag_enablement_grant must not be used"
        )
    _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), POST_FLAG_ENABLEMENT_GRANT_NEXT_STAGE)
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    for caveat in _caveats(mode="post_flag_enablement_grant"):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"post_flag_enablement_grant caveats missing {caveat!r}"
            )
    for item in FLAG_ENABLEMENT_GRANT_STILL_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )
    return {
        **base_result,
        "verification_mode": "post_flag_enablement_grant",
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
    }


def verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
    bundle: Mapping[str, Any],
    *,
    repo_root: Path | None = None,
    expect_plan_filed: bool | None = None,
    expect_proof_filed: bool | None = None,
    expect_pilot_request_filed: bool | None = None,
    expect_pilot_grant_filed: bool | None = None,
    expect_pilot_harness_filed: bool | None = None,
    expect_pilot_harness_review_filed: bool | None = None,
    expect_pilot_run_filed: bool | None = None,
    expect_pilot_review_filed: bool | None = None,
    expect_live_read_only_request_filed: bool | None = None,
    expect_live_read_only_grant_filed: bool | None = None,
    expect_live_read_only_pilot_run_filed: bool | None = None,
    expect_live_read_only_pilot_review_filed: bool | None = None,
    expect_live_execution_request_filed: bool | None = None,
    expect_live_execution_grant_filed: bool | None = None,
    expect_live_execution_pilot_run_filed: bool | None = None,
    expect_live_execution_pilot_review_filed: bool | None = None,
    expect_flag_enablement_request_filed: bool | None = None,
    expect_flag_enablement_grant_filed: bool | None = None,
    expect_flag_enablement_pilot_run_filed: bool | None = None,
    expect_flag_enablement_pilot_review_filed: bool | None = None,
    expect_production_default_api_user_visible_request_filed: bool | None = None,
    expect_production_default_api_user_visible_grant_filed: bool | None = None,
    expect_production_default_api_user_visible_pilot_run_filed: bool | None = None,
    verify_local_pilot_files: bool = True,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    metadata = _metadata(bundle, label="production-scoped-shadow bundle")
    _require_equal("metadata.artifact_type", metadata.get("artifact_type"), ARTIFACT_TYPE)
    _require_equal("metadata.bundle_version", metadata.get("bundle_version"), BUNDLE_VERSION)
    mode = _infer_plan_mode(
        bundle,
        expect_plan_filed=expect_plan_filed,
        expect_proof_filed=expect_proof_filed,
        expect_pilot_request_filed=expect_pilot_request_filed,
        expect_pilot_grant_filed=expect_pilot_grant_filed,
        expect_pilot_harness_filed=expect_pilot_harness_filed,
        expect_pilot_harness_review_filed=expect_pilot_harness_review_filed,
        expect_pilot_run_filed=expect_pilot_run_filed,
        expect_pilot_review_filed=expect_pilot_review_filed,
        expect_live_read_only_request_filed=expect_live_read_only_request_filed,
        expect_live_read_only_grant_filed=expect_live_read_only_grant_filed,
        expect_live_read_only_pilot_run_filed=expect_live_read_only_pilot_run_filed,
        expect_live_read_only_pilot_review_filed=expect_live_read_only_pilot_review_filed,
        expect_live_execution_request_filed=expect_live_execution_request_filed,
        expect_live_execution_grant_filed=expect_live_execution_grant_filed,
        expect_live_execution_pilot_run_filed=expect_live_execution_pilot_run_filed,
        expect_live_execution_pilot_review_filed=expect_live_execution_pilot_review_filed,
        expect_flag_enablement_request_filed=expect_flag_enablement_request_filed,
        expect_flag_enablement_grant_filed=expect_flag_enablement_grant_filed,
        expect_flag_enablement_pilot_run_filed=expect_flag_enablement_pilot_run_filed,
        expect_flag_enablement_pilot_review_filed=expect_flag_enablement_pilot_review_filed,
        expect_production_default_api_user_visible_request_filed=(
            expect_production_default_api_user_visible_request_filed
        ),
        expect_production_default_api_user_visible_grant_filed=(
            expect_production_default_api_user_visible_grant_filed
        ),
        expect_production_default_api_user_visible_pilot_run_filed=(
            expect_production_default_api_user_visible_pilot_run_filed
        ),
    )
    if mode == "post_production_default_api_user_visible_pilot_run":
        return _verify_production_default_api_user_visible_pilot_run_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_production_default_api_user_visible_grant":
        return _verify_production_default_api_user_visible_grant_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_production_default_api_user_visible_request":
        return _verify_production_default_api_user_visible_request_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_flag_enablement_pilot_review":
        return _verify_flag_enablement_pilot_review_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_flag_enablement_pilot_run":
        return _verify_flag_enablement_pilot_run_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_flag_enablement_grant":
        return _verify_flag_enablement_grant_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_flag_enablement_request":
        return _verify_flag_enablement_request_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_live_execution_pilot_review":
        return _verify_live_execution_pilot_review_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_live_execution_pilot_run":
        return _verify_live_execution_pilot_run_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_live_execution_grant":
        return _verify_live_execution_grant_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_live_execution_request":
        return _verify_live_execution_request_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    if mode == "post_live_read_only_pilot_review":
        return _verify_live_read_only_pilot_review_payload(
            bundle,
            repo_root=root,
            verify_local_pilot_files=verify_local_pilot_files,
        )
    expected_revision = {
        "pre_plan": PRE_PLAN_BUNDLE_REVISION,
        "post_plan": POST_PLAN_BUNDLE_REVISION,
        "post_proof": POST_PROOF_BUNDLE_REVISION,
        "post_pilot_request": POST_PILOT_REQUEST_BUNDLE_REVISION,
        "post_pilot_grant": POST_PILOT_GRANT_BUNDLE_REVISION,
        "post_pilot_harness": POST_PILOT_HARNESS_BUNDLE_REVISION,
        "post_pilot_harness_review": POST_PILOT_HARNESS_REVIEW_BUNDLE_REVISION,
        "post_pilot_run": POST_PILOT_RUN_BUNDLE_REVISION,
        "post_pilot_review": POST_PILOT_REVIEW_BUNDLE_REVISION,
        "post_live_read_only_request": POST_LIVE_READ_ONLY_REQUEST_BUNDLE_REVISION,
        "post_live_read_only_grant": POST_LIVE_READ_ONLY_GRANT_BUNDLE_REVISION,
        "post_live_read_only_pilot_run": POST_LIVE_READ_ONLY_PILOT_RUN_BUNDLE_REVISION,
    }[mode]
    _require_equal("metadata.bundle_revision", metadata.get("bundle_revision"), expected_revision)
    _validate_identity(metadata.get("pinned_identity"), label="metadata.pinned_identity")
    records, resolved_paths = _verify_legacy_index(bundle, repo_root=root)

    production_readiness_bundle = _load_json_object(resolved_paths["production_readiness_bundle"])
    phase2_bundle = _load_json_object(resolved_paths["phase2_bundle"])
    policy = _load_json_object(resolved_paths["online_shadow_policy"])
    _validate_production_readiness_bundle(production_readiness_bundle, repo_root=root)
    _validate_phase2_bundle(phase2_bundle, repo_root=root)
    _validate_online_shadow_policy(policy)

    upstream_ref = bundle.get("upstream_ref")
    if not isinstance(upstream_ref, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("upstream_ref must be an object")
    production_readiness_ref_path = _verify_reference(
        upstream_ref.get("production_readiness_bundle"),
        repo_root=root,
        label="upstream_ref.production_readiness_bundle",
    )
    phase2_ref_path = _verify_reference(
        upstream_ref.get("phase2_bundle"),
        repo_root=root,
        label="upstream_ref.phase2_bundle",
    )
    if production_readiness_ref_path != resolved_paths["production_readiness_bundle"]:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "upstream_ref.production_readiness_bundle path must match legacy index"
        )
    if phase2_ref_path != resolved_paths["phase2_bundle"]:
        raise MLShadowScorerProductionScopedShadowBundleError("upstream_ref.phase2_bundle path must match legacy index")
    _require_equal(
        "upstream_ref.production_readiness_bundle.bundle_revision",
        _get(upstream_ref, "production_readiness_bundle.bundle_revision"),
        PRODUCTION_READINESS_GRANT_REVISION,
    )
    _require_equal(
        "upstream_ref.phase2_bundle.bundle_revision",
        _get(upstream_ref, "phase2_bundle.bundle_revision"),
        _get(phase2_bundle, "metadata.bundle_revision"),
    )
    _require_true(
        "upstream_ref.production_readiness_authorization_granted",
        upstream_ref.get("production_readiness_authorization_granted"),
    )
    _require_true("upstream_ref.phase2_write_pilot_accepted", upstream_ref.get("phase2_write_pilot_accepted"))

    plan = bundle.get("plan")
    if not isinstance(plan, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("plan must be an object")
    if mode == "pre_plan":
        _require_false("plan.prod_scoped_shadow_plan_defined", plan.get("prod_scoped_shadow_plan_defined"))
        _require_equal("plan.plan_decision", plan.get("plan_decision"), None)
        _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), PRE_PLAN_NEXT_STAGE)
    else:
        _require_true("plan.prod_scoped_shadow_plan_defined", plan.get("prod_scoped_shadow_plan_defined"))
        _verify_plan_subsections(plan)
        if mode == "post_live_read_only_pilot_run":
            expected_next = POST_LIVE_READ_ONLY_PILOT_RUN_NEXT_STAGE
        elif mode == "post_live_read_only_grant":
            expected_next = POST_LIVE_READ_ONLY_GRANT_NEXT_STAGE
        elif mode == "post_live_read_only_request":
            expected_next = POST_LIVE_READ_ONLY_REQUEST_NEXT_STAGE
        elif mode == "post_pilot_review":
            review_accepted = _get(bundle, "review.prod_scoped_shadow_pilot_accepted")
            expected_next = (
                POST_PILOT_REVIEW_ACCEPTED_NEXT_STAGE
                if review_accepted is True
                else POST_PILOT_REVIEW_REJECTED_NEXT_STAGE
            )
        elif mode == "post_pilot_run":
            expected_next = POST_PILOT_RUN_NEXT_STAGE
        elif mode == "post_pilot_harness_review":
            review_accepted = _get(bundle, "review.prod_scoped_shadow_pilot_harness_accepted")
            expected_next = (
                POST_PILOT_HARNESS_REVIEW_ACCEPTED_NEXT_STAGE
                if review_accepted is True
                else POST_PILOT_HARNESS_REVIEW_REJECTED_NEXT_STAGE
            )
        elif mode == "post_pilot_harness":
            expected_next = POST_PILOT_HARNESS_NEXT_STAGE
        elif mode == "post_pilot_grant":
            expected_next = POST_PILOT_GRANT_NEXT_STAGE
        elif mode == "post_pilot_request":
            expected_next = POST_PILOT_REQUEST_NEXT_STAGE
        elif mode == "post_proof":
            expected_next = POST_PROOF_NEXT_STAGE
        else:
            expected_next = POST_PLAN_NEXT_STAGE
        _require_equal("recommended_next_stage", bundle.get("recommended_next_stage"), expected_next)

    authorization = bundle.get("authorization")
    if not isinstance(authorization, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("authorization must be an object")
    _require_equal(
        "authorization.prod_scoped_shadow_plan_authorization_scope",
        authorization.get("prod_scoped_shadow_plan_authorization_scope"),
        "production_scoped_shadow_plan_paperwork_only",
    )
    _require_false(
        "authorization.prod_scoped_shadow_execution_authorized",
        authorization.get("prod_scoped_shadow_execution_authorized"),
    )
    if mode in {
        "post_proof",
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    } or authorization.get(
        "prod_scoped_shadow_live_execution_authorized"
    ) is not None:
        _require_false(
            "authorization.prod_scoped_shadow_live_execution_authorized",
            authorization.get("prod_scoped_shadow_live_execution_authorized"),
        )
    _require_false(
        "authorization.prod_scoped_shadow_proof_authorized",
        authorization.get("prod_scoped_shadow_proof_authorized"),
    )
    pilot_grant_expected = mode in {
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }
    _require_equal(
        "authorization.prod_scoped_shadow_pilot_authorized",
        authorization.get("prod_scoped_shadow_pilot_authorized"),
        pilot_grant_expected,
    )
    pilot_request_expected = mode in {
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }
    if (
        mode in {
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        }
        or authorization.get("prod_scoped_shadow_pilot_authorization_requested") is not None
    ):
        _require_equal(
            "authorization.prod_scoped_shadow_pilot_authorization_requested",
            authorization.get("prod_scoped_shadow_pilot_authorization_requested"),
            pilot_request_expected,
        )
    if mode == "post_pilot_grant" or authorization.get("prod_scoped_shadow_pilot_authorization_granted") is not None:
        _require_equal(
            "authorization.prod_scoped_shadow_pilot_authorization_granted",
            authorization.get("prod_scoped_shadow_pilot_authorization_granted"),
            pilot_grant_expected,
        )
    if mode in {
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
    }:
        request_decision = authorization.get("request_decision")
        if not isinstance(request_decision, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError("authorization.request_decision must be an object")
        _require_equal("authorization.request_decision.decision", request_decision.get("decision"), "requested")
        if not isinstance(request_decision.get("requester"), str) or not request_decision.get("requester"):
            raise MLShadowScorerProductionScopedShadowBundleError("authorization.request_decision.requester must be populated")
        if not isinstance(request_decision.get("requested_at"), str) or not request_decision.get("requested_at"):
            raise MLShadowScorerProductionScopedShadowBundleError("authorization.request_decision.requested_at must be populated")
        requested_scope = authorization.get("requested_scope")
        if not isinstance(requested_scope, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError("authorization.requested_scope must be an object")
        _require_equal(
            "authorization.requested_scope.authorization_scope",
            requested_scope.get("authorization_scope"),
            PILOT_REQUEST_SCOPE,
        )
        for item in PILOT_REQUEST_EXPLICITLY_NOT_INCLUDED:
            if item not in requested_scope.get("explicitly_not_included", []):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.requested_scope.explicitly_not_included missing {item!r}"
                )
    if mode in {
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        grant_decision = authorization.get("grant_decision")
        if not isinstance(grant_decision, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError("authorization.grant_decision must be an object")
        _require_equal("authorization.grant_decision.decision", grant_decision.get("decision"), "granted")
        for field in ("owner", "granted_at", "expiry_date", "review_by"):
            if not isinstance(grant_decision.get(field), str) or not grant_decision.get(field):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.grant_decision.{field} must be populated"
                )
        granted_scope = authorization.get("granted_scope")
        if not isinstance(granted_scope, Mapping):
            raise MLShadowScorerProductionScopedShadowBundleError("authorization.granted_scope must be an object")
        _require_equal(
            "authorization.granted_scope.authorization_scope",
            granted_scope.get("authorization_scope"),
            PILOT_GRANT_SCOPE,
        )
        for item in PILOT_GRANT_STILL_NOT_INCLUDED:
            if item not in granted_scope.get("explicitly_still_not_included", []):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.granted_scope.explicitly_still_not_included missing {item!r}"
                )
        for item in PILOT_GRANT_AUTHORIZES_FOR_CHAIN_ONLY:
            if item not in granted_scope.get("authorizes_for_chain_only", []):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.granted_scope.authorizes_for_chain_only missing {item!r}"
                )
        for item in PILOT_GRANT_TIME_BOUNDARIES:
            if item not in granted_scope.get("grant_time_pilot_boundaries", []):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.granted_scope.grant_time_pilot_boundaries missing {item!r}"
                )
    if mode in {
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    } or authorization.get("prod_scoped_shadow_pilot_execution_authorized") is not None:
        _require_equal(
            "authorization.prod_scoped_shadow_pilot_execution_authorized",
            authorization.get("prod_scoped_shadow_pilot_execution_authorized"),
            mode
            in {
                "post_pilot_run",
                "post_pilot_review",
                "post_live_read_only_request",
                "post_live_read_only_grant",
                "post_live_read_only_pilot_run",
            },
        )
    if mode == "post_live_read_only_request":
        _require_true(
            "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
            authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
        )
        _require_false(
            "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
            authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
        )
        _require_false(
            "authorization.prod_scoped_shadow_live_read_only_authorized",
            authorization.get("prod_scoped_shadow_live_read_only_authorized"),
        )
        _verify_live_read_only_request_section(authorization)
        for item in LIVE_READ_ONLY_REQUEST_EXPLICITLY_NOT_INCLUDED:
            if item not in authorization.get("explicitly_not_included", []):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.explicitly_not_included missing {item!r}"
                )
    if mode in {"post_live_read_only_grant", "post_live_read_only_pilot_run"}:
        _require_true(
            "authorization.prod_scoped_shadow_live_read_only_authorization_requested",
            authorization.get("prod_scoped_shadow_live_read_only_authorization_requested"),
        )
        _require_true(
            "authorization.prod_scoped_shadow_live_read_only_authorization_granted",
            authorization.get("prod_scoped_shadow_live_read_only_authorization_granted"),
        )
        _require_true(
            "authorization.prod_scoped_shadow_live_read_only_authorized",
            authorization.get("prod_scoped_shadow_live_read_only_authorized"),
        )
        _verify_live_read_only_request_section(authorization)
        _verify_live_read_only_grant_section(authorization)
        for item in LIVE_READ_ONLY_GRANT_STILL_NOT_INCLUDED:
            if item not in authorization.get("explicitly_not_included", []):
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"authorization.explicitly_not_included missing {item!r}"
                )
        if mode == "post_live_read_only_pilot_run":
            _require_true(
                "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
                authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
            )
        elif "prod_scoped_shadow_live_read_only_execution_authorized" in authorization:
            _require_false(
                "authorization.prod_scoped_shadow_live_read_only_execution_authorized",
                authorization.get("prod_scoped_shadow_live_read_only_execution_authorized"),
            )
    for item in EXPLICITLY_NOT_INCLUDED:
        if item not in authorization.get("explicitly_not_included", []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"authorization.explicitly_not_included missing {item!r}"
            )

    execution = bundle.get("execution")
    if not isinstance(execution, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("execution must be an object")
    _require_false(
        "execution.prod_scoped_shadow_plan_execution_performed",
        execution.get("prod_scoped_shadow_plan_execution_performed"),
    )
    _require_equal(
        "execution.prod_scoped_shadow_proof_executed",
        execution.get("prod_scoped_shadow_proof_executed"),
        mode in {
            "post_proof",
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
    )
    _require_equal(
        "execution.prod_scoped_shadow_pilot_executed",
        execution.get("prod_scoped_shadow_pilot_executed"),
        mode
        in {
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
    )
    if mode == "post_live_read_only_pilot_run":
        _require_true(
            "execution.prod_scoped_shadow_live_read_only_pilot_executed",
            execution.get("prod_scoped_shadow_live_read_only_pilot_executed"),
        )
        _require_true(
            "execution.prod_scoped_shadow_live_read_only_pilot_passed",
            execution.get("prod_scoped_shadow_live_read_only_pilot_passed"),
        )

    posture = bundle.get("posture")
    if not isinstance(posture, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("posture must be an object")
    posture_required = {
        "prod_scoped_shadow_plan_defined": mode in {
            "post_plan",
            "post_proof",
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
        "prod_scoped_shadow_proof_passed": mode in {
            "post_proof",
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
        "prod_scoped_shadow_pilot_executed": mode in {
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
        "missing_prod_scoped_shadow_proof": mode not in {
            "post_proof",
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
        "prod_scoped_shadow_proof_authorized": False,
        "production_readiness_authorization_granted": True,
        "missing_production_readiness_authorization": False,
        "online_shadow_execution_enabled": False,
        "production_default_allowed": False,
        "api_web_changes_allowed": False,
        "user_visible_ranking_changed": False,
        "writes_performed": False,
        "runtime_writes_performed": False,
    }
    for field, expected in posture_required.items():
        _require_equal(f"posture.{field}", posture.get(field), expected)
    if mode in {
        "post_proof",
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    } or posture.get(
        "prod_scoped_shadow_pilot_authorized"
    ) is not None:
        _require_equal(
            "posture.prod_scoped_shadow_pilot_authorized",
            posture.get("prod_scoped_shadow_pilot_authorized"),
            pilot_grant_expected,
        )
    if (
        mode in {
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        }
        or posture.get("prod_scoped_shadow_pilot_authorization_requested") is not None
    ):
        _require_equal(
            "posture.prod_scoped_shadow_pilot_authorization_requested",
            posture.get("prod_scoped_shadow_pilot_authorization_requested"),
            pilot_request_expected,
        )
    if mode == "post_pilot_grant" or posture.get("prod_scoped_shadow_pilot_authorization_granted") is not None:
        _require_equal(
            "posture.prod_scoped_shadow_pilot_authorization_granted",
            posture.get("prod_scoped_shadow_pilot_authorization_granted"),
            pilot_grant_expected,
        )
    if mode == "post_pilot_request":
        _require_true(
            "posture.missing_prod_scoped_shadow_pilot_authorization",
            posture.get("missing_prod_scoped_shadow_pilot_authorization"),
        )
    if mode in {
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_false(
            "posture.missing_prod_scoped_shadow_pilot_authorization",
            posture.get("missing_prod_scoped_shadow_pilot_authorization"),
        )
    if mode in {
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true(
            "posture.prod_scoped_shadow_pilot_harness_executed",
            posture.get("prod_scoped_shadow_pilot_harness_executed"),
        )
        _require_true(
            "posture.prod_scoped_shadow_pilot_harness_passed",
            posture.get("prod_scoped_shadow_pilot_harness_passed"),
        )
        if mode == "post_live_read_only_pilot_run":
            _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
        else:
            _require_false("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    if mode in {
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true(
            "posture.prod_scoped_shadow_pilot_harness_reviewed",
            posture.get("prod_scoped_shadow_pilot_harness_reviewed"),
        )
        _require_equal(
            "posture.prod_scoped_shadow_pilot_harness_accepted",
            posture.get("prod_scoped_shadow_pilot_harness_accepted"),
            _get(bundle, "review.prod_scoped_shadow_pilot_harness_accepted"),
        )
    if mode in {
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true("posture.prod_scoped_shadow_pilot_passed", posture.get("prod_scoped_shadow_pilot_passed"))
        _require_true(
            "posture.prod_scoped_shadow_pilot_execution_authorized",
            posture.get("prod_scoped_shadow_pilot_execution_authorized"),
        )
    if mode in {
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true("posture.prod_scoped_shadow_pilot_reviewed", posture.get("prod_scoped_shadow_pilot_reviewed"))
        _require_equal(
            "posture.prod_scoped_shadow_pilot_accepted",
            posture.get("prod_scoped_shadow_pilot_accepted"),
            _get(bundle, "review.prod_scoped_shadow_pilot_accepted"),
        )
    if mode == "post_live_read_only_request":
        _require_true(
            "posture.prod_scoped_shadow_live_read_only_authorization_requested",
            posture.get("prod_scoped_shadow_live_read_only_authorization_requested"),
        )
        _require_false(
            "posture.prod_scoped_shadow_live_read_only_authorization_granted",
            posture.get("prod_scoped_shadow_live_read_only_authorization_granted"),
        )
        _require_false(
            "posture.prod_scoped_shadow_live_read_only_authorized",
            posture.get("prod_scoped_shadow_live_read_only_authorized"),
        )
        _require_true(
            "posture.missing_prod_scoped_shadow_live_read_only_authorization",
            posture.get("missing_prod_scoped_shadow_live_read_only_authorization"),
        )
        _require_false("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
    if mode in {"post_live_read_only_grant", "post_live_read_only_pilot_run"}:
        _require_true(
            "posture.prod_scoped_shadow_live_read_only_authorization_requested",
            posture.get("prod_scoped_shadow_live_read_only_authorization_requested"),
        )
        _require_true(
            "posture.prod_scoped_shadow_live_read_only_authorization_granted",
            posture.get("prod_scoped_shadow_live_read_only_authorization_granted"),
        )
        _require_true(
            "posture.prod_scoped_shadow_live_read_only_authorized",
            posture.get("prod_scoped_shadow_live_read_only_authorized"),
        )
        _require_false(
            "posture.missing_prod_scoped_shadow_live_read_only_authorization",
            posture.get("missing_prod_scoped_shadow_live_read_only_authorization"),
        )
        if mode == "post_live_read_only_pilot_run":
            _require_true("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))
            _require_true(
                "posture.prod_scoped_shadow_live_read_only_execution_authorized",
                posture.get("prod_scoped_shadow_live_read_only_execution_authorized"),
            )
            _require_true(
                "posture.prod_scoped_shadow_live_read_only_pilot_executed",
                posture.get("prod_scoped_shadow_live_read_only_pilot_executed"),
            )
            _require_true(
                "posture.prod_scoped_shadow_live_read_only_pilot_passed",
                posture.get("prod_scoped_shadow_live_read_only_pilot_passed"),
            )
        else:
            _require_false("posture.live_prod_source_reads_performed", posture.get("live_prod_source_reads_performed"))

    blockers = bundle.get("shadow_and_production_blockers")
    if not isinstance(blockers, Mapping):
        raise MLShadowScorerProductionScopedShadowBundleError("shadow_and_production_blockers must be an object")
    _require_equal(
        "shadow_and_production_blockers.missing_prod_scoped_shadow_proof",
        blockers.get("missing_prod_scoped_shadow_proof"),
        mode not in {
            "post_proof",
            "post_pilot_request",
            "post_pilot_grant",
            "post_pilot_harness",
            "post_pilot_harness_review",
            "post_pilot_run",
            "post_pilot_review",
            "post_live_read_only_request",
            "post_live_read_only_grant",
            "post_live_read_only_pilot_run",
        },
    )
    _require_false(
        "shadow_and_production_blockers.prod_scoped_shadow_proof_authorized",
        blockers.get("prod_scoped_shadow_proof_authorized"),
    )
    _require_equal("shadow_and_production_blockers.blockers_changed_by_plan", blockers.get("blockers_changed_by_plan"), [])
    _require_true("shadow_and_production_blockers.blockers_unchanged_by_plan", blockers.get("blockers_unchanged_by_plan"))
    if mode in {
        "post_proof",
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        changed_by_proof = blockers.get("blockers_changed_by_proof")
        if "missing_prod_scoped_shadow_proof" not in (changed_by_proof or []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                "shadow_and_production_blockers.blockers_changed_by_proof must include missing_prod_scoped_shadow_proof"
            )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_proof",
            blockers.get("blockers_unchanged_by_proof"),
        )
    _require_false(
        "shadow_and_production_blockers.online_shadow_execution_enabled",
        blockers.get("online_shadow_execution_enabled"),
    )
    _require_false(
        "shadow_and_production_blockers.production_default_allowed",
        blockers.get("production_default_allowed"),
    )
    _require_false("shadow_and_production_blockers.api_web_changes_allowed", blockers.get("api_web_changes_allowed"))
    _require_false(
        "shadow_and_production_blockers.user_visible_ranking_changed",
        blockers.get("user_visible_ranking_changed"),
    )

    _require_false("writes_performed", bundle.get("writes_performed"))
    _require_false("runtime_writes_performed", bundle.get("runtime_writes_performed"))
    caveats = bundle.get("caveats")
    if not isinstance(caveats, list):
        raise MLShadowScorerProductionScopedShadowBundleError("caveats must be a list")
    if mode in {
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        introduced = blockers.get("blockers_introduced_by_pilot_request")
        if "missing_prod_scoped_shadow_pilot_authorization" not in (introduced or []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                "shadow_and_production_blockers.blockers_introduced_by_pilot_request must include missing_prod_scoped_shadow_pilot_authorization"
            )
        _require_equal(
            "shadow_and_production_blockers.blockers_cleared_by_pilot_request",
            blockers.get("blockers_cleared_by_pilot_request"),
            [],
        )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_pilot_request",
            blockers.get("blockers_unchanged_by_pilot_request"),
        )
        _require_equal(
            "shadow_and_production_blockers.prod_scoped_shadow_pilot_authorized",
            blockers.get("prod_scoped_shadow_pilot_authorized"),
            pilot_grant_expected,
        )
    if mode == "post_pilot_request":
        _require_true(
            "shadow_and_production_blockers.missing_prod_scoped_shadow_pilot_authorization",
            blockers.get("missing_prod_scoped_shadow_pilot_authorization"),
        )
    if mode in {
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_false(
            "shadow_and_production_blockers.missing_prod_scoped_shadow_pilot_authorization",
            blockers.get("missing_prod_scoped_shadow_pilot_authorization"),
        )
        _require_true(
            "shadow_and_production_blockers.prod_scoped_shadow_pilot_authorization_granted",
            blockers.get("prod_scoped_shadow_pilot_authorization_granted"),
        )
        cleared_by_grant = blockers.get("blockers_cleared_by_pilot_grant")
        if "missing_prod_scoped_shadow_pilot_authorization" not in (cleared_by_grant or []):
            raise MLShadowScorerProductionScopedShadowBundleError(
                "shadow_and_production_blockers.blockers_cleared_by_pilot_grant must include missing_prod_scoped_shadow_pilot_authorization"
            )
        _require_equal(
            "shadow_and_production_blockers.blockers_introduced_by_pilot_grant",
            blockers.get("blockers_introduced_by_pilot_grant"),
            [],
        )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_pilot_grant",
            blockers.get("blockers_unchanged_by_pilot_grant"),
        )
    if mode in {
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_equal(
            "shadow_and_production_blockers.prod_scoped_shadow_pilot_executed",
            blockers.get("prod_scoped_shadow_pilot_executed"),
            mode
            in {
                "post_pilot_run",
                "post_pilot_review",
                "post_live_read_only_request",
                "post_live_read_only_grant",
                "post_live_read_only_pilot_run",
            },
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_cleared_by_pilot_harness",
            blockers.get("blockers_cleared_by_pilot_harness"),
            [],
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_introduced_by_pilot_harness",
            blockers.get("blockers_introduced_by_pilot_harness"),
            [],
        )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_pilot_harness",
            blockers.get("blockers_unchanged_by_pilot_harness"),
        )
    if mode in {
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true("shadow_and_production_blockers.prod_scoped_shadow_pilot_passed", blockers.get("prod_scoped_shadow_pilot_passed"))
        _require_equal("shadow_and_production_blockers.blockers_cleared_by_pilot_run", blockers.get("blockers_cleared_by_pilot_run"), [])
        _require_equal("shadow_and_production_blockers.blockers_introduced_by_pilot_run", blockers.get("blockers_introduced_by_pilot_run"), [])
        _require_true("shadow_and_production_blockers.blockers_unchanged_by_pilot_run", blockers.get("blockers_unchanged_by_pilot_run"))
    if mode in {
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true("shadow_and_production_blockers.prod_scoped_shadow_pilot_reviewed", blockers.get("prod_scoped_shadow_pilot_reviewed"))
        _require_equal(
            "shadow_and_production_blockers.prod_scoped_shadow_pilot_accepted",
            blockers.get("prod_scoped_shadow_pilot_accepted"),
            _get(bundle, "review.prod_scoped_shadow_pilot_accepted"),
        )
        _require_equal("shadow_and_production_blockers.blockers_cleared_by_pilot_review", blockers.get("blockers_cleared_by_pilot_review"), [])
        _require_equal("shadow_and_production_blockers.blockers_introduced_by_pilot_review", blockers.get("blockers_introduced_by_pilot_review"), [])
        _require_true("shadow_and_production_blockers.blockers_unchanged_by_pilot_review", blockers.get("blockers_unchanged_by_pilot_review"))
    if mode == "post_live_read_only_request":
        _require_true(
            "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_authorization_requested",
            blockers.get("prod_scoped_shadow_live_read_only_authorization_requested"),
        )
        _require_false(
            "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_authorized",
            blockers.get("prod_scoped_shadow_live_read_only_authorized"),
        )
        _require_true(
            "shadow_and_production_blockers.missing_prod_scoped_shadow_live_read_only_authorization",
            blockers.get("missing_prod_scoped_shadow_live_read_only_authorization"),
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_introduced_by_live_read_only_request",
            blockers.get("blockers_introduced_by_live_read_only_request"),
            ["missing_prod_scoped_shadow_live_read_only_authorization"],
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_cleared_by_live_read_only_request",
            blockers.get("blockers_cleared_by_live_read_only_request"),
            [],
        )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_live_read_only_request",
            blockers.get("blockers_unchanged_by_live_read_only_request"),
        )
        if "blockers_changed_by_live_read_only_request" in blockers:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "shadow_and_production_blockers.blockers_changed_by_live_read_only_request must not be used"
            )
    if mode in {"post_live_read_only_grant", "post_live_read_only_pilot_run"}:
        _require_true(
            "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_authorization_requested",
            blockers.get("prod_scoped_shadow_live_read_only_authorization_requested"),
        )
        _require_true(
            "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_authorization_granted",
            blockers.get("prod_scoped_shadow_live_read_only_authorization_granted"),
        )
        _require_true(
            "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_authorized",
            blockers.get("prod_scoped_shadow_live_read_only_authorized"),
        )
        _require_false(
            "shadow_and_production_blockers.missing_prod_scoped_shadow_live_read_only_authorization",
            blockers.get("missing_prod_scoped_shadow_live_read_only_authorization"),
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_introduced_by_live_read_only_request",
            blockers.get("blockers_introduced_by_live_read_only_request"),
            ["missing_prod_scoped_shadow_live_read_only_authorization"],
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_cleared_by_live_read_only_request",
            blockers.get("blockers_cleared_by_live_read_only_request"),
            [],
        )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_live_read_only_request",
            blockers.get("blockers_unchanged_by_live_read_only_request"),
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_cleared_by_live_read_only_grant",
            blockers.get("blockers_cleared_by_live_read_only_grant"),
            ["missing_prod_scoped_shadow_live_read_only_authorization"],
        )
        _require_equal(
            "shadow_and_production_blockers.blockers_introduced_by_live_read_only_grant",
            blockers.get("blockers_introduced_by_live_read_only_grant"),
            [],
        )
        _require_true(
            "shadow_and_production_blockers.blockers_unchanged_by_live_read_only_grant",
            blockers.get("blockers_unchanged_by_live_read_only_grant"),
        )
        if "blockers_changed_by_live_read_only_request" in blockers:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "shadow_and_production_blockers.blockers_changed_by_live_read_only_request must not be used"
            )
        if "blockers_changed_by_live_read_only_grant" in blockers:
            raise MLShadowScorerProductionScopedShadowBundleError(
                "shadow_and_production_blockers.blockers_changed_by_live_read_only_grant must not be used"
            )
        if mode == "post_live_read_only_pilot_run":
            _require_true(
                "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_execution_authorized",
                blockers.get("prod_scoped_shadow_live_read_only_execution_authorized"),
            )
            _require_true(
                "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_pilot_executed",
                blockers.get("prod_scoped_shadow_live_read_only_pilot_executed"),
            )
            _require_true(
                "shadow_and_production_blockers.prod_scoped_shadow_live_read_only_pilot_passed",
                blockers.get("prod_scoped_shadow_live_read_only_pilot_passed"),
            )
            if "live_prod_source_reads_performed" in blockers:
                _require_false(
                    "shadow_and_production_blockers.live_prod_source_reads_performed",
                    blockers.get("live_prod_source_reads_performed"),
                )
            _require_equal(
                "shadow_and_production_blockers.blockers_cleared_by_live_read_only_pilot_run",
                blockers.get("blockers_cleared_by_live_read_only_pilot_run"),
                [],
            )
            _require_equal(
                "shadow_and_production_blockers.blockers_introduced_by_live_read_only_pilot_run",
                blockers.get("blockers_introduced_by_live_read_only_pilot_run"),
                [],
            )
            _require_true(
                "shadow_and_production_blockers.blockers_unchanged_by_live_read_only_pilot_run",
                blockers.get("blockers_unchanged_by_live_read_only_pilot_run"),
            )
            if "blockers_changed_by_live_read_only_pilot_run" in blockers:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    "shadow_and_production_blockers.blockers_changed_by_live_read_only_pilot_run must not be used"
                )
    if mode in {
        "post_proof",
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _verify_proof_section(bundle.get("proof"))
        _require_true(
            "authorization.prod_scoped_shadow_proof_allowed_by_plan",
            authorization.get("prod_scoped_shadow_proof_allowed_by_plan"),
        )
    else:
        if authorization.get("prod_scoped_shadow_proof_allowed_by_plan") is not None:
            _require_false(
                "authorization.prod_scoped_shadow_proof_allowed_by_plan",
                authorization.get("prod_scoped_shadow_proof_allowed_by_plan"),
            )
    for caveat in _caveats(mode=mode):
        if caveat not in caveats:
            raise MLShadowScorerProductionScopedShadowBundleError(f"caveats missing {caveat!r}")
    if mode == "pre_plan":
        for caveat in PLAN_CAVEATS:
            if caveat in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"pre-plan caveats must not include post-plan {caveat!r}"
                )
    if mode == "post_plan":
        caveat_text = " ".join(str(caveat).lower() for caveat in caveats)
        forbidden_phrases = (
            "authorizes proof execution",
            "authorizes pilot",
            "enables online shadow",
            "production default allowed",
        )
        for phrase in forbidden_phrases:
            if phrase in caveat_text:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post-plan caveats imply forbidden enablement: {phrase}"
                )
    if mode in {
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _require_true(
            "authorization.prod_scoped_shadow_pilot_harness_allowed_by_grant",
            authorization.get("prod_scoped_shadow_pilot_harness_allowed_by_grant"),
        )
        _verify_pilot_harness_section(
            _get(bundle, "execution.pilot_harness"),
            repo_root=root,
            verify_local_files=(mode == "post_pilot_harness"),
        )
    if mode in {
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        _verify_pilot_run_section(
            _get(bundle, "execution.pilot_run"),
            repo_root=root,
            verify_local_files=verify_local_pilot_files,
        )
    if mode == "post_live_read_only_pilot_run":
        _verify_live_read_only_pilot_run_section(
            _get(bundle, "execution.live_read_only_pilot_run"),
            repo_root=root,
            verify_local_files=verify_local_pilot_files,
        )
        _verify_live_prod_source_read_flags(bundle)
    if mode in {
        "post_proof",
        "post_pilot_request",
        "post_pilot_grant",
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        for caveat in PLAN_CAVEATS:
            if caveat in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats must not include plan-only {caveat!r}"
                )
    if mode == "post_pilot_request":
        for caveat in REQUEST_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post_pilot_request caveats missing request caveat {caveat!r}"
                )
    if mode == "post_pilot_grant":
        for caveat in GRANT_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post_pilot_grant caveats missing grant caveat {caveat!r}"
                )
        for caveat in REQUEST_CAVEATS:
            if caveat in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post_pilot_grant caveats must not include request-only {caveat!r}"
                )
    if mode in {
        "post_pilot_harness",
        "post_pilot_harness_review",
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        for caveat in HARNESS_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats missing harness caveat {caveat!r}"
                )
        for caveat in REQUEST_CAVEATS:
            if caveat in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats must not include request-only {caveat!r}"
                )
        caveat_text = " ".join(str(caveat).lower() for caveat in caveats)
        for phrase in ("live prod execution", "live production traffic executed", "production-scoped pilot has run"):
            if phrase in caveat_text:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats must not claim live pilot execution: {phrase}"
                )
    if mode == "post_pilot_harness_review":
        for caveat in HARNESS_REVIEW_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post_pilot_harness_review caveats missing review caveat {caveat!r}"
                )
        _verify_pilot_harness_review_section(bundle.get("review"))
    if mode in {
        "post_pilot_run",
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        for caveat in HARNESS_REVIEW_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats missing harness review caveat {caveat!r}"
                )
        for caveat in PILOT_RUN_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats missing pilot run caveat {caveat!r}"
                )
        _verify_pilot_harness_review_section(bundle.get("review"))
    if mode in {
        "post_pilot_review",
        "post_live_read_only_request",
        "post_live_read_only_grant",
        "post_live_read_only_pilot_run",
    }:
        for caveat in PILOT_REVIEW_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats missing pilot review caveat {caveat!r}"
                )
        _verify_pilot_review_section(bundle.get("review"))
    if mode == "post_live_read_only_request":
        for caveat in LIVE_READ_ONLY_REQUEST_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post_live_read_only_request caveats missing live read-only request caveat {caveat!r}"
                )
        _require_named_flags_not_true(
            bundle,
            (
                "live_prod_source_reads_performed",
                "online_shadow_execution_enabled",
                "production_default_allowed",
                "api_web_changes_allowed",
                "user_visible_ranking_changed",
                "prod_scoped_shadow_live_execution_authorized",
                "prod_scoped_shadow_execution_authorized",
                "prod_scoped_shadow_live_read_only_authorization_granted",
                "prod_scoped_shadow_live_read_only_authorized",
            ),
        )
    if mode in {"post_live_read_only_grant", "post_live_read_only_pilot_run"}:
        for caveat in LIVE_READ_ONLY_GRANT_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats missing live read-only grant caveat {caveat!r}"
                )
        for caveat in LIVE_READ_ONLY_REQUEST_CAVEATS:
            if caveat in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"{mode} caveats must not include request-only {caveat!r}"
                )
        if mode == "post_live_read_only_grant":
            _require_named_flags_not_true(
                bundle,
                (
                    "live_prod_source_reads_performed",
                    "online_shadow_execution_enabled",
                    "production_default_allowed",
                    "api_web_changes_allowed",
                    "user_visible_ranking_changed",
                    "prod_scoped_shadow_live_execution_authorized",
                    "prod_scoped_shadow_execution_authorized",
                ),
            )
    if mode == "post_live_read_only_pilot_run":
        for caveat in LIVE_READ_ONLY_PILOT_RUN_CAVEATS:
            if caveat not in caveats:
                raise MLShadowScorerProductionScopedShadowBundleError(
                    f"post_live_read_only_pilot_run caveats missing live read-only pilot run caveat {caveat!r}"
                )
        _require_named_flags_not_true(
            bundle,
            (
                "online_shadow_execution_enabled",
                "production_default_allowed",
                "api_web_changes_allowed",
                "user_visible_ranking_changed",
                "prod_scoped_shadow_live_execution_authorized",
                "prod_scoped_shadow_execution_authorized",
            ),
        )

    return {
        "verification_status": "passed",
        "verification_mode": mode,
        "bundle_version": metadata.get("bundle_version"),
        "bundle_revision": metadata.get("bundle_revision"),
        "recommended_next_stage": bundle.get("recommended_next_stage"),
        "legacy_artifact_count": len(records),
    }


def verify_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    repo_root: Path | None = None,
    expect_plan_filed: bool | None = None,
    expect_proof_filed: bool | None = None,
    expect_pilot_request_filed: bool | None = None,
    expect_pilot_grant_filed: bool | None = None,
    expect_pilot_harness_filed: bool | None = None,
    expect_pilot_harness_review_filed: bool | None = None,
    expect_pilot_run_filed: bool | None = None,
    expect_pilot_review_filed: bool | None = None,
    expect_live_read_only_request_filed: bool | None = None,
    expect_live_read_only_grant_filed: bool | None = None,
    expect_live_read_only_pilot_run_filed: bool | None = None,
    expect_live_read_only_pilot_review_filed: bool | None = None,
    expect_live_execution_request_filed: bool | None = None,
    expect_live_execution_grant_filed: bool | None = None,
    expect_live_execution_pilot_run_filed: bool | None = None,
    expect_live_execution_pilot_review_filed: bool | None = None,
    expect_flag_enablement_request_filed: bool | None = None,
    expect_flag_enablement_grant_filed: bool | None = None,
    expect_flag_enablement_pilot_run_filed: bool | None = None,
    expect_flag_enablement_pilot_review_filed: bool | None = None,
    expect_production_default_api_user_visible_request_filed: bool | None = None,
    expect_production_default_api_user_visible_grant_filed: bool | None = None,
    expect_production_default_api_user_visible_pilot_run_filed: bool | None = None,
    verify_local_pilot_files: bool = True,
) -> dict[str, Any]:
    payload = _load_json_object(Path(bundle_path).resolve())
    return verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=repo_root,
        expect_plan_filed=expect_plan_filed,
        expect_proof_filed=expect_proof_filed,
        expect_pilot_request_filed=expect_pilot_request_filed,
        expect_pilot_grant_filed=expect_pilot_grant_filed,
        expect_pilot_harness_filed=expect_pilot_harness_filed,
        expect_pilot_harness_review_filed=expect_pilot_harness_review_filed,
        expect_pilot_run_filed=expect_pilot_run_filed,
        expect_pilot_review_filed=expect_pilot_review_filed,
        expect_live_read_only_request_filed=expect_live_read_only_request_filed,
        expect_live_read_only_grant_filed=expect_live_read_only_grant_filed,
        expect_live_read_only_pilot_run_filed=expect_live_read_only_pilot_run_filed,
        expect_live_read_only_pilot_review_filed=expect_live_read_only_pilot_review_filed,
        expect_live_execution_request_filed=expect_live_execution_request_filed,
        expect_live_execution_grant_filed=expect_live_execution_grant_filed,
        expect_live_execution_pilot_run_filed=expect_live_execution_pilot_run_filed,
        expect_live_execution_pilot_review_filed=expect_live_execution_pilot_review_filed,
        expect_flag_enablement_request_filed=expect_flag_enablement_request_filed,
        expect_flag_enablement_grant_filed=expect_flag_enablement_grant_filed,
        expect_flag_enablement_pilot_run_filed=expect_flag_enablement_pilot_run_filed,
        expect_flag_enablement_pilot_review_filed=expect_flag_enablement_pilot_review_filed,
        expect_production_default_api_user_visible_request_filed=(
            expect_production_default_api_user_visible_request_filed
        ),
        expect_production_default_api_user_visible_grant_filed=(
            expect_production_default_api_user_visible_grant_filed
        ),
        expect_production_default_api_user_visible_pilot_run_filed=(
            expect_production_default_api_user_visible_pilot_run_filed
        ),
        verify_local_pilot_files=verify_local_pilot_files,
    )


def markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(payload: Mapping[str, Any]) -> str:
    metadata = payload["metadata"]
    identity = metadata["pinned_identity"]
    upstream = payload["upstream_ref"]
    plan = payload["plan"]
    authorization = payload["authorization"]
    posture = payload["posture"]
    execution = payload["execution"]
    proof = payload.get("proof") if isinstance(payload.get("proof"), Mapping) else None
    pilot_harness = execution.get("pilot_harness") if isinstance(execution.get("pilot_harness"), Mapping) else None
    pilot_run = execution.get("pilot_run") if isinstance(execution.get("pilot_run"), Mapping) else None
    live_read_only_pilot_run = (
        execution.get("live_read_only_pilot_run")
        if isinstance(execution.get("live_read_only_pilot_run"), Mapping)
        else None
    )
    live_execution_pilot_run = (
        execution.get("live_execution_pilot_run")
        if isinstance(execution.get("live_execution_pilot_run"), Mapping)
        else None
    )
    flag_enablement_pilot_run = (
        execution.get("flag_enablement_pilot_run")
        if isinstance(execution.get("flag_enablement_pilot_run"), Mapping)
        else None
    )
    production_default_api_user_visible_pilot_run = (
        execution.get("production_default_api_user_visible_pilot_run")
        if isinstance(execution.get("production_default_api_user_visible_pilot_run"), Mapping)
        else None
    )
    review = payload.get("review") if isinstance(payload.get("review"), Mapping) else None
    requested_scope = authorization.get("requested_scope") if isinstance(authorization.get("requested_scope"), Mapping) else None
    request_decision = (
        authorization.get("request_decision") if isinstance(authorization.get("request_decision"), Mapping) else None
    )
    pilot_request = (
        request_decision
        if isinstance(requested_scope, Mapping) and requested_scope.get("authorization_scope") == PILOT_REQUEST_SCOPE
        else None
    )
    live_read_only_request = (
        request_decision
        if isinstance(requested_scope, Mapping)
        and requested_scope.get("authorization_scope") == LIVE_READ_ONLY_REQUEST_SCOPE
        else None
    )
    live_read_only_grant = (
        authorization.get("live_read_only_grant_decision")
        if isinstance(authorization.get("live_read_only_grant_decision"), Mapping)
        else None
    )
    live_execution_request = (
        authorization.get("live_execution_request_decision")
        if isinstance(authorization.get("live_execution_request_decision"), Mapping)
        else None
    )
    live_execution_grant = (
        authorization.get("live_execution_grant_decision")
        if isinstance(authorization.get("live_execution_grant_decision"), Mapping)
        else None
    )
    pilot_grant = authorization.get("grant_decision") if isinstance(authorization.get("grant_decision"), Mapping) else None
    pilot_review = review.get("pilot_review_decision") if isinstance(review, Mapping) and isinstance(review.get("pilot_review_decision"), Mapping) else None
    live_read_only_pilot_review = (
        review.get("live_read_only_pilot_review_decision")
        if isinstance(review, Mapping) and isinstance(review.get("live_read_only_pilot_review_decision"), Mapping)
        else None
    )
    live_execution_pilot_review = (
        review.get("live_execution_pilot_review_decision")
        if isinstance(review, Mapping) and isinstance(review.get("live_execution_pilot_review_decision"), Mapping)
        else None
    )
    flag_enablement_pilot_review = (
        review.get("flag_enablement_pilot_review_decision")
        if isinstance(review, Mapping) and isinstance(review.get("flag_enablement_pilot_review_decision"), Mapping)
        else None
    )
    flag_enablement_request = (
        authorization.get("flag_enablement_request_decision")
        if isinstance(authorization.get("flag_enablement_request_decision"), Mapping)
        and isinstance(authorization.get("flag_enablement_requested_scope"), Mapping)
        and authorization["flag_enablement_requested_scope"].get("authorization_scope")
        == FLAG_ENABLEMENT_REQUEST_SCOPE
        else None
    )
    flag_enablement_grant = (
        authorization.get("flag_enablement_grant_decision")
        if isinstance(authorization.get("flag_enablement_grant_decision"), Mapping)
        and isinstance(authorization.get("flag_enablement_granted_scope"), Mapping)
        and authorization["flag_enablement_granted_scope"].get("authorization_scope")
        == FLAG_ENABLEMENT_GRANT_SCOPE
        else None
    )
    production_default_api_user_visible_request = (
        authorization.get("production_default_api_user_visible_request_decision")
        if isinstance(authorization.get("production_default_api_user_visible_request_decision"), Mapping)
        and isinstance(authorization.get("production_default_api_user_visible_requested_scope"), Mapping)
        and authorization["production_default_api_user_visible_requested_scope"].get("authorization_scope")
        == PRODUCTION_DEFAULT_API_USER_VISIBLE_REQUEST_SCOPE
        else None
    )
    production_default_api_user_visible_grant = (
        authorization.get("production_default_api_user_visible_grant_decision")
        if isinstance(authorization.get("production_default_api_user_visible_grant_decision"), Mapping)
        and isinstance(authorization.get("production_default_api_user_visible_granted_scope"), Mapping)
        and authorization["production_default_api_user_visible_granted_scope"].get("authorization_scope")
        == PRODUCTION_DEFAULT_API_USER_VISIBLE_GRANT_SCOPE
        else None
    )
    if production_default_api_user_visible_pilot_run:
        summary = (
            "This bundle records the bounded production-scoped online shadow production default/API/user-visible pilot run while keeping production output, API/web behavior, user-visible ranking, global/fleet execution, writes, refit/training, and label ingest disabled."
        )
    elif production_default_api_user_visible_grant:
        summary = (
            "This bundle records the production-scoped online shadow production default/API/user-visible authorization grant while keeping runtime enablement, production default, API/web, user-visible ranking, global/fleet execution, writes, refit/training, and label ingest disabled."
        )
    elif production_default_api_user_visible_request:
        summary = (
            "This bundle records the production-scoped online shadow production default/API/user-visible authorization request while granting no authorization, enabling no production output, and keeping online_shadow_execution_enabled, production default, API/web, and user-visible behavior disabled."
        )
    elif flag_enablement_pilot_review:
        accepted = review.get("prod_scoped_shadow_flag_enablement_pilot_accepted") if isinstance(review, Mapping) else None
        summary = (
            f"This bundle records the production-scoped online shadow flag enablement pilot review ({'accepted' if accepted else 'not accepted'}) while keeping global/fleet shadow enablement, online_shadow_execution_enabled, production default, API/web, and user-visible behavior disabled."
        )
    elif flag_enablement_pilot_run:
        summary = (
            "This bundle records the bounded production-scoped online shadow flag enablement pilot run while keeping global/fleet shadow enablement, online_shadow_execution_enabled, production default, API/web, and user-visible behavior disabled."
        )
    elif flag_enablement_grant:
        summary = (
            "This bundle records the production-scoped online shadow flag enablement authorization grant while keeping bounded flag-enablement pilot runs separate, runtime flag enablement disabled, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif flag_enablement_request:
        summary = (
            "This bundle records the production-scoped online shadow flag enablement authorization request while keeping runtime flag enablement disabled, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_execution_pilot_review:
        accepted = review.get("prod_scoped_shadow_live_execution_pilot_accepted") if isinstance(review, Mapping) else None
        summary = (
            f"This bundle records the production-scoped online shadow live execution pilot review ({'accepted' if accepted else 'not accepted'}) while keeping global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_execution_pilot_run:
        summary = (
            "This bundle records the bounded production-scoped online shadow live execution pilot run while keeping global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_execution_grant:
        summary = (
            "This bundle records the production-scoped online shadow live execution authorization grant while keeping bounded live execution pilot runs separate, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_execution_request:
        summary = (
            "This bundle records the production-scoped online shadow live execution authorization request while keeping live execution unauthorized, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_read_only_pilot_review:
        accepted = review.get("prod_scoped_shadow_live_read_only_pilot_accepted") if isinstance(review, Mapping) else None
        summary = (
            f"This bundle records the production-scoped online shadow live read-only pilot review ({'accepted' if accepted else 'not accepted'}) while keeping global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_read_only_pilot_run:
        summary = (
            "This bundle records the bounded production-scoped online shadow live read-only pilot run while keeping global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_read_only_grant:
        summary = (
            "This bundle records the production-scoped online shadow live read-only authorization grant while keeping live reads at grant time, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif live_read_only_request:
        summary = (
            "This bundle records the production-scoped online shadow live read-only authorization request while keeping live production source reads, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif pilot_review:
        accepted = review.get("prod_scoped_shadow_pilot_accepted") if isinstance(review, Mapping) else None
        summary = (
            f"This bundle records the bounded 528-work audit-artifact production-scoped shadow pilot review ({'accepted' if accepted else 'not accepted'}) while keeping live production source reads, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif pilot_run:
        summary = (
            "This bundle records the bounded 528-work audit-artifact production-scoped shadow pilot while keeping live production source reads, global shadow enablement, production default, API/web, and user-visible behavior disabled."
        )
    elif review:
        accepted = review.get("prod_scoped_shadow_pilot_harness_accepted")
        summary = (
            f"This bundle records the bounded fixture production-scoped shadow pilot harness review ({'accepted' if accepted else 'not accepted'}) while keeping live pilot execution, production default, API/web, and user-visible behavior disabled."
        )
    elif pilot_harness:
        summary = (
            "This bundle records the bounded fixture production-scoped shadow pilot harness while keeping live pilot execution, production default, API/web, and user-visible behavior disabled."
        )
    elif pilot_grant:
        summary = (
            "This bundle records the production-scoped shadow pilot authorization grant while keeping pilot execution, runtime, production default, API/web, and user-visible behavior disabled."
        )
    elif pilot_request:
        summary = (
            "This bundle records the production-scoped shadow pilot authorization request while keeping pilot authorization, runtime, production default, API/web, and user-visible behavior disabled."
        )
    elif proof:
        summary = (
            "This bundle records the bounded fixture/dry-run production-scoped shadow proof while keeping pilot, runtime, production default, API/web, and user-visible behavior disabled."
        )
    else:
        summary = (
            "This bundle defines the production-scoped online shadow plan contract while keeping proof, pilot, runtime, production default, API/web, and user-visible behavior disabled."
        )
    lines = [
        f"# ml-shadow-scorer-v1 Production-Scoped Shadow Bundle ({metadata['bundle_version']})",
        "",
        "## Executive Summary",
        "",
        summary,
        "",
        f"- Bundle revision: {metadata['bundle_revision']}",
        f"- Production-scoped plan defined: {plan['prod_scoped_shadow_plan_defined']}",
        f"- Production-scoped proof passed: {posture['prod_scoped_shadow_proof_passed']}",
        f"- Missing production-scoped shadow proof: {posture['missing_prod_scoped_shadow_proof']}",
        f"- Pilot authorization requested: {authorization.get('prod_scoped_shadow_pilot_authorization_requested')}",
        f"- Pilot authorization granted: {authorization.get('prod_scoped_shadow_pilot_authorization_granted')}",
        f"- Pilot authorized: {authorization.get('prod_scoped_shadow_pilot_authorized')}",
        f"- Pilot harness executed: {posture.get('prod_scoped_shadow_pilot_harness_executed')}",
        f"- Pilot harness reviewed: {posture.get('prod_scoped_shadow_pilot_harness_reviewed')}",
        f"- Pilot harness accepted: {posture.get('prod_scoped_shadow_pilot_harness_accepted')}",
        f"- Production-scoped pilot executed: {posture.get('prod_scoped_shadow_pilot_executed')}",
        f"- Production-scoped pilot passed: {posture.get('prod_scoped_shadow_pilot_passed')}",
        f"- Production-scoped pilot reviewed: {posture.get('prod_scoped_shadow_pilot_reviewed')}",
        f"- Production-scoped pilot accepted: {posture.get('prod_scoped_shadow_pilot_accepted')}",
        f"- Live read-only authorization requested: {authorization.get('prod_scoped_shadow_live_read_only_authorization_requested')}",
        f"- Live read-only authorization granted: {authorization.get('prod_scoped_shadow_live_read_only_authorization_granted')}",
        f"- Live read-only authorized: {authorization.get('prod_scoped_shadow_live_read_only_authorized')}",
        f"- Live read-only pilot executed: {posture.get('prod_scoped_shadow_live_read_only_pilot_executed')}",
        f"- Live read-only pilot passed: {posture.get('prod_scoped_shadow_live_read_only_pilot_passed')}",
        f"- Live read-only pilot reviewed: {posture.get('prod_scoped_shadow_live_read_only_pilot_reviewed')}",
        f"- Live read-only pilot accepted: {posture.get('prod_scoped_shadow_live_read_only_pilot_accepted')}",
        f"- Live execution authorization requested: {authorization.get('prod_scoped_shadow_live_execution_authorization_requested')}",
        f"- Live execution authorization granted: {authorization.get('prod_scoped_shadow_live_execution_authorization_granted')}",
        f"- Live execution authorized: {authorization.get('prod_scoped_shadow_live_execution_authorized')}",
        f"- Live execution pilot executed: {posture.get('prod_scoped_shadow_live_execution_pilot_executed')}",
        f"- Live execution pilot passed: {posture.get('prod_scoped_shadow_live_execution_pilot_passed')}",
        f"- Live execution pilot reviewed: {posture.get('prod_scoped_shadow_live_execution_pilot_reviewed')}",
        f"- Live execution pilot accepted: {posture.get('prod_scoped_shadow_live_execution_pilot_accepted')}",
        f"- Flag enablement authorization requested: {authorization.get('prod_scoped_shadow_flag_enablement_authorization_requested')}",
        f"- Flag enablement authorization granted: {authorization.get('prod_scoped_shadow_flag_enablement_authorization_granted')}",
        f"- Flag enablement authorized: {authorization.get('prod_scoped_shadow_flag_enablement_authorized')}",
        f"- Flag enablement pilot executed: {posture.get('prod_scoped_shadow_flag_enablement_pilot_executed')}",
        f"- Flag enablement pilot passed: {posture.get('prod_scoped_shadow_flag_enablement_pilot_passed')}",
        f"- Flag enablement pilot reviewed: {posture.get('prod_scoped_shadow_flag_enablement_pilot_reviewed')}",
        f"- Flag enablement pilot accepted: {posture.get('prod_scoped_shadow_flag_enablement_pilot_accepted')}",
        f"- Production default/API/user-visible pilot executed: {posture.get('prod_scoped_shadow_production_default_api_user_visible_pilot_executed')}",
        f"- Production default/API/user-visible pilot passed: {posture.get('prod_scoped_shadow_production_default_api_user_visible_pilot_passed')}",
        f"- Missing flag enablement authorization: {posture.get('missing_prod_scoped_shadow_flag_enablement_authorization')}",
        f"- Missing live execution authorization: {posture.get('missing_prod_scoped_shadow_live_execution_authorization')}",
        f"- Live production source reads performed: {posture.get('live_prod_source_reads_performed')}",
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
            "## Upstream Evidence",
            "",
            f"- Production-readiness bundle: `{upstream['production_readiness_bundle']['path']}`",
            f"- Production-readiness revision: {upstream['production_readiness_bundle']['bundle_revision']}",
            f"- Production-readiness authorization granted: {upstream['production_readiness_authorization_granted']}",
            f"- Phase 2 bundle: `{upstream['phase2_bundle']['path']}`",
            f"- Phase 2 revision: {upstream['phase2_bundle']['bundle_revision']}",
            f"- Phase 2 write pilot accepted: {upstream['phase2_write_pilot_accepted']}",
            "",
            "## Plan Contract",
            "",
        ]
    )
    if plan["prod_scoped_shadow_plan_defined"]:
        decision = plan["plan_decision"]
        lines.extend(
            [
                f"- Decision: `{decision['decision']}`",
                f"- Planner: {decision['planner']}",
                f"- Planned at: {decision['planned_at']}",
                f"- Plan notes: {decision.get('plan_notes')}",
                f"- Future artifact root proposal: `{plan['prod_scoped_identity_and_rollout_boundaries']['future_artifact_root_proposal']}`",
                f"- Runtime feature flag: `{plan['feature_flag_iam_config_requirements']['runtime_feature_flag']}`",
                f"- Results use: {plan['production_default_api_user_visible_separation']['results_use']}",
                "",
                "## Plan Sections",
                "",
            ]
        )
        for subsection in PLAN_SUBSECTIONS:
            lines.append(f"- `{subsection}`")
    else:
        lines.append("- Plan not filed yet.")
    if proof:
        lines.extend(
            [
                "",
                "## Proof Evidence",
                "",
                f"- Decision: `{proof['proof_decision']['decision']}`",
                f"- Prover: {proof['proof_decision']['prover']}",
                f"- Proven at: {proof['proof_decision']['proven_at']}",
                f"- Proof surface: `{proof['proof_surface']}`",
                f"- Pilot run id: `{proof['pilot_run_id']}`",
                f"- Local artifact root: `{proof['write_evidence']['prod_scoped_artifact_root']}`",
                f"- Local artifact writes performed: {proof['write_evidence']['local_artifact_tree_writes_performed']}",
                f"- Production writes performed: {proof['write_evidence']['production_writes_performed']}",
                f"- Forbidden write counts zero: {proof['write_evidence']['forbidden_write_counts_zero']}",
                f"- Observability complete: {proof['observability_evidence']['observability_complete']}",
                f"- Rollback flag-off verified: {proof['rollback_drill_evidence']['flag_off_verified']}",
                f"- Overall passed: {proof['proof_pass_fail']['overall_passed']}",
                "",
                "## Proof Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in proof["write_evidence"]["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
    lines.extend(
        [
            "",
            "## Authorization Boundaries",
            "",
            f"- Plan authorization scope: `{authorization['prod_scoped_shadow_plan_authorization_scope']}`",
            f"- Proof allowed by plan: {authorization['prod_scoped_shadow_proof_allowed_by_plan']}",
            f"- Pilot authorization requested: {authorization.get('prod_scoped_shadow_pilot_authorization_requested')}",
            f"- Live execution authorized: {authorization['prod_scoped_shadow_live_execution_authorized']}",
            f"- Execution authorized: {authorization['prod_scoped_shadow_execution_authorized']}",
            f"- Pilot execution authorized: {authorization.get('prod_scoped_shadow_pilot_execution_authorized')}",
            f"- Live read-only authorization requested: {authorization.get('prod_scoped_shadow_live_read_only_authorization_requested')}",
            f"- Live read-only authorization granted: {authorization.get('prod_scoped_shadow_live_read_only_authorization_granted')}",
            f"- Live read-only authorized: {authorization.get('prod_scoped_shadow_live_read_only_authorized')}",
            f"- Live execution authorization requested: {authorization.get('prod_scoped_shadow_live_execution_authorization_requested')}",
            f"- Live execution authorized: {authorization.get('prod_scoped_shadow_live_execution_authorized')}",
            f"- Proof authorized: {authorization['prod_scoped_shadow_proof_authorized']}",
            f"- Pilot authorized: {authorization['prod_scoped_shadow_pilot_authorized']}",
        ]
    )
    if pilot_request:
        request_decision = pilot_request
        lines.extend(
            [
                "",
                "## Pilot Authorization Request",
                "",
                f"- Decision: `{request_decision['decision']}`",
                f"- Requester: {request_decision['requester']}",
                f"- Requested at: {request_decision['requested_at']}",
                f"- Request notes: {request_decision.get('request_notes')}",
                f"- Requested scope: `{authorization['requested_scope']['authorization_scope']}`",
                f"- Missing pilot authorization: {posture.get('missing_prod_scoped_shadow_pilot_authorization')}",
                "",
            ]
        )
    if live_read_only_request:
        request_decision = live_read_only_request
        lines.extend(
            [
                "",
                "## Live Read-Only Authorization Request",
                "",
                f"- Decision: `{request_decision['decision']}`",
                f"- Requester: {request_decision['requester']}",
                f"- Requested at: {request_decision['requested_at']}",
                f"- Request notes: {request_decision.get('request_notes')}",
                f"- Requested scope: `{authorization['requested_scope']['authorization_scope']}`",
                f"- Missing live read-only authorization: {posture.get('missing_prod_scoped_shadow_live_read_only_authorization')}",
                f"- Live reads performed: {posture.get('live_prod_source_reads_performed')}",
                "",
                "## Live Read-Only Future Grant Requirements",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in authorization["requested_scope"]["future_grant_would_require"])
        lines.extend(["", "## Live Read-Only Request Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in authorization["requested_scope"]["explicitly_not_included"])
        lines.append("")
    if live_execution_request:
        request_decision = live_execution_request
        lines.extend(
            [
                "## Live Execution Authorization Request",
                "",
                f"- Decision: `{request_decision['decision']}`",
                f"- Requester: {request_decision['requester']}",
                f"- Requested at: {request_decision['requested_at']}",
                f"- Request notes: {request_decision.get('request_notes')}",
                f"- Requested scope: `{authorization['live_execution_requested_scope']['authorization_scope']}`",
                f"- Missing live execution authorization: {posture.get('missing_prod_scoped_shadow_live_execution_authorization')}",
                f"- Live reads performed: {posture.get('live_prod_source_reads_performed')}",
                "",
                "## Live Execution Future Grant Requirements",
                "",
            ]
        )
        lines.extend(
            f"- {item}" for item in authorization["live_execution_requested_scope"]["future_grant_would_require"]
        )
        lines.extend(["", "## Live Execution Request Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in authorization["live_execution_requested_scope"]["explicitly_not_included"])
        lines.append("")
    if live_execution_grant:
        grant_decision = live_execution_grant
        granted_scope = authorization["live_execution_granted_scope"]
        lines.extend(
            [
                "## Live Execution Authorization Grant",
                "",
                f"- Decision: `{grant_decision['decision']}`",
                f"- Owner: {grant_decision['owner']}",
                f"- Granted at: {grant_decision['granted_at']}",
                f"- Expiry date: {grant_decision['expiry_date']}",
                f"- Review by: {grant_decision['review_by']}",
                f"- Grant notes: {grant_decision.get('grant_notes')}",
                f"- Second reviewer: {grant_decision.get('second_reviewer')}",
                f"- Owner equivalent review: {grant_decision.get('owner_documents_equivalent_review')}",
                f"- Granted scope: `{granted_scope['authorization_scope']}`",
                f"- Missing live execution authorization: {posture.get('missing_prod_scoped_shadow_live_execution_authorization')}",
                f"- Live reads performed: {posture.get('live_prod_source_reads_performed')}",
                "",
                "## Live Execution Grant Boundaries",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in granted_scope["grant_time_live_execution_boundaries"])
        lines.extend(["", "## Live Execution Grant Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in granted_scope["explicitly_still_not_included"])
        lines.append("")
    if live_execution_pilot_run:
        live_source_reads = live_execution_pilot_run["live_source_reads"]
        row_counts = live_source_reads["row_counts"]
        incomplete = live_execution_pilot_run["incomplete_coverage_drill"]
        lines.extend(
            [
                "## Live Execution Pilot Run",
                "",
                f"- Pilot surface: `{live_execution_pilot_run['pilot_surface']}`",
                f"- Pilot run id: `{live_execution_pilot_run['pilot_run_id']}`",
                f"- Joined candidate count: {live_execution_pilot_run['input_join_summary']['joined_candidate_count']}",
                f"- Live prod source reads performed: {live_execution_pilot_run['live_prod_source_reads_performed']}",
                f"- Pilot passed: {live_execution_pilot_run['pass_fail_evaluation']['overall_passed']}",
                f"- Pilot run directory: `{live_execution_pilot_run['pilot_run_directory']['relative_path']}`",
                f"- Incomplete coverage status: `{incomplete['status']}`",
                f"- Incomplete coverage shadow rows: {incomplete['shadow_row_count']}",
                "",
                "## Live Execution Source Reads",
                "",
                f"- Approved tables: {', '.join(live_source_reads['approved_tables'])}",
                f"- Ranking runs: {row_counts['ranking_runs']}",
                f"- Paper scores: {row_counts['paper_scores']}",
                f"- Works: {row_counts['works']}",
                f"- Embeddings: {row_counts['embeddings']}",
                f"- Candidate SHA: `{live_source_reads['input_identity_verification']['candidate_pool_work_set_sha256']}`",
                "",
                "## Live Execution Pilot Checks",
                "",
            ]
        )
        for check_name in LIVE_EXECUTION_PILOT_RUN_PASS_FAIL_CHECKS:
            lines.append(f"- `{check_name}`: {live_execution_pilot_run['pass_fail_evaluation']['checks'][check_name]}")
        lines.extend(
            [
                "",
                "## Live Execution Pilot Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in live_execution_pilot_run["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
        lines.append("")
    if live_execution_pilot_review:
        lines.extend(
            [
                "## Live Execution Pilot Review",
                "",
                f"- Decision: `{live_execution_pilot_review['decision']}`",
                f"- Reviewer: {live_execution_pilot_review['reviewer']}",
                f"- Reviewed at: {live_execution_pilot_review['reviewed_at']}",
                f"- Review notes: {live_execution_pilot_review.get('review_notes')}",
                f"- Live execution pilot accepted: {review['prod_scoped_shadow_live_execution_pilot_accepted']}",
                f"- Failed review checks: {', '.join(live_execution_pilot_review['failed_review_checks']) if live_execution_pilot_review['failed_review_checks'] else 'None'}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Live Execution Pilot Review Checks",
                "",
            ]
        )
        for check_name in LIVE_EXECUTION_PILOT_RUN_REVIEW_CHECKS:
            lines.append(f"- `{check_name}`: {live_execution_pilot_review['checks'][check_name]}")
        lines.extend(["", "## Live Execution Pilot Review Accepted Evidence", ""])
        lines.extend(f"- {item}" for item in live_execution_pilot_review["accepted_evidence"])
        lines.extend(["", "## Live Execution Pilot Review Limitations", ""])
        lines.extend(f"- {item}" for item in live_execution_pilot_review["limitations"])
        lines.append("")
    if flag_enablement_request:
        request_decision = flag_enablement_request
        requested_scope = authorization["flag_enablement_requested_scope"]
        lines.extend(
            [
                "## Flag Enablement Authorization Request",
                "",
                f"- Decision: `{request_decision['decision']}`",
                f"- Requester: {request_decision['requester']}",
                f"- Requested at: {request_decision['requested_at']}",
                f"- Request notes: {request_decision.get('request_notes')}",
                f"- Requested scope: `{requested_scope['authorization_scope']}`",
                f"- Runtime feature flag: `{requested_scope['runtime_feature_flag']}`",
                f"- Missing flag enablement authorization: {posture.get('missing_prod_scoped_shadow_flag_enablement_authorization')}",
                f"- Live execution authorized: {authorization.get('prod_scoped_shadow_live_execution_authorized')}",
                f"- Online shadow execution enabled: {posture.get('online_shadow_execution_enabled')}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Flag Enablement Future Grant Requirements",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in requested_scope["future_grant_would_require"])
        lines.extend(["", "## Flag Enablement Request Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in requested_scope["explicitly_not_included"])
        lines.append("")
    if flag_enablement_grant:
        grant_decision = flag_enablement_grant
        granted_scope = authorization["flag_enablement_granted_scope"]
        lines.extend(
            [
                "## Flag Enablement Authorization Grant",
                "",
                f"- Decision: `{grant_decision['decision']}`",
                f"- Owner: {grant_decision['owner']}",
                f"- Granted at: {grant_decision['granted_at']}",
                f"- Expiry date: {grant_decision['expiry_date']}",
                f"- Review by: {grant_decision['review_by']}",
                f"- Grant notes: {grant_decision.get('grant_notes')}",
                f"- Second reviewer: {grant_decision.get('second_reviewer')}",
                f"- Owner equivalent review: {grant_decision.get('owner_documents_equivalent_review')}",
                f"- Granted scope: `{granted_scope['authorization_scope']}`",
                f"- Runtime feature flag: `{granted_scope['runtime_feature_flag']}`",
                f"- Missing flag enablement authorization: {posture.get('missing_prod_scoped_shadow_flag_enablement_authorization')}",
                f"- Live execution authorized: {authorization.get('prod_scoped_shadow_live_execution_authorized')}",
                f"- Online shadow execution enabled: {posture.get('online_shadow_execution_enabled')}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Flag Enablement Grant Authorizes For Chain Only",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in granted_scope["authorizes_for_chain_only"])
        lines.extend(["", "## Flag Enablement Grant Boundaries", ""])
        lines.extend(f"- {item}" for item in granted_scope["grant_time_flag_enablement_boundaries"])
        lines.extend(["", "## Flag Enablement Grant Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in granted_scope["explicitly_still_not_included"])
        lines.append("")
    if flag_enablement_pilot_run:
        live_source_reads = flag_enablement_pilot_run["live_source_reads"]
        row_counts = live_source_reads["row_counts"]
        incomplete = flag_enablement_pilot_run["incomplete_coverage_drill"]
        lines.extend(
            [
                "## Flag Enablement Pilot Run",
                "",
                f"- Pilot surface: `{flag_enablement_pilot_run['pilot_surface']}`",
                f"- Pilot run id: `{flag_enablement_pilot_run['pilot_run_id']}`",
                f"- Joined candidate count: {flag_enablement_pilot_run['input_join_summary']['joined_candidate_count']}",
                f"- Live prod source reads performed: {flag_enablement_pilot_run['live_prod_source_reads_performed']}",
                f"- Pilot passed: {flag_enablement_pilot_run['pass_fail_evaluation']['overall_passed']}",
                f"- Pilot run directory: `{flag_enablement_pilot_run['pilot_run_directory']['relative_path']}`",
                f"- Incomplete coverage status: `{incomplete['status']}`",
                f"- Incomplete coverage shadow rows: {incomplete['shadow_row_count']}",
                "",
                "## Flag Enablement Source Reads",
                "",
                f"- Approved tables: {', '.join(live_source_reads['approved_tables'])}",
                f"- Ranking runs: {row_counts['ranking_runs']}",
                f"- Paper scores: {row_counts['paper_scores']}",
                f"- Works: {row_counts['works']}",
                f"- Embeddings: {row_counts['embeddings']}",
                f"- Candidate SHA: `{live_source_reads['input_identity_verification']['candidate_pool_work_set_sha256']}`",
                "",
                "## Flag Enablement Pilot Checks",
                "",
            ]
        )
        for check_name in FLAG_ENABLEMENT_PILOT_RUN_PASS_FAIL_CHECKS:
            lines.append(
                f"- `{check_name}`: {flag_enablement_pilot_run['pass_fail_evaluation']['checks'][check_name]}"
            )
        lines.extend(
            [
                "",
                "## Flag Enablement Pilot Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in flag_enablement_pilot_run["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
        lines.append("")
    if flag_enablement_pilot_review:
        lines.extend(
            [
                "## Flag Enablement Pilot Review",
                "",
                f"- Decision: `{flag_enablement_pilot_review['decision']}`",
                f"- Reviewer: {flag_enablement_pilot_review['reviewer']}",
                f"- Reviewed at: {flag_enablement_pilot_review['reviewed_at']}",
                f"- Review notes: {flag_enablement_pilot_review.get('review_notes')}",
                f"- Flag enablement pilot accepted: {review['prod_scoped_shadow_flag_enablement_pilot_accepted']}",
                f"- Failed review checks: {', '.join(flag_enablement_pilot_review['failed_review_checks']) if flag_enablement_pilot_review['failed_review_checks'] else 'None'}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Flag Enablement Pilot Review Checks",
                "",
            ]
        )
        for check_name in FLAG_ENABLEMENT_PILOT_RUN_REVIEW_CHECKS:
            lines.append(f"- `{check_name}`: {flag_enablement_pilot_review['checks'][check_name]}")
        lines.extend(["", "## Flag Enablement Pilot Review Accepted Evidence", ""])
        lines.extend(f"- {item}" for item in flag_enablement_pilot_review["accepted_evidence"])
        lines.extend(["", "## Flag Enablement Pilot Review Limitations", ""])
        lines.extend(f"- {item}" for item in flag_enablement_pilot_review["limitations"])
        lines.append("")
    if production_default_api_user_visible_request:
        request_decision = production_default_api_user_visible_request
        requested_scope = authorization["production_default_api_user_visible_requested_scope"]
        lines.extend(
            [
                "## Production Default/API/User-Visible Authorization Request",
                "",
                f"- Decision: `{request_decision['decision']}`",
                f"- Requester: {request_decision['requester']}",
                f"- Requested at: {request_decision['requested_at']}",
                f"- Request notes: {request_decision.get('request_notes')}",
                f"- Requested scope: `{requested_scope['authorization_scope']}`",
                f"- Missing production default/API/user-visible authorization: {posture.get('missing_prod_scoped_shadow_production_default_api_user_visible_authorization')}",
                f"- Flag enablement authorized: {authorization.get('prod_scoped_shadow_flag_enablement_authorized')}",
                f"- Live execution authorized: {authorization.get('prod_scoped_shadow_live_execution_authorized')}",
                f"- Production default allowed: {posture.get('production_default_allowed')}",
                f"- API/web changes allowed: {posture.get('api_web_changes_allowed')}",
                f"- User-visible ranking changed: {posture.get('user_visible_ranking_changed')}",
                f"- Online shadow execution enabled: {posture.get('online_shadow_execution_enabled')}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Production Default/API/User-Visible Future Grant Requirements",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in requested_scope["future_grant_would_require"])
        lines.extend(["", "## Production Default/API/User-Visible Request Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in requested_scope["explicitly_not_included"])
        lines.extend(f"- {item}" for item in requested_scope["explicitly_not_included"])
        lines.append("")
    if production_default_api_user_visible_grant:
        grant_decision = production_default_api_user_visible_grant
        granted_scope = authorization["production_default_api_user_visible_granted_scope"]
        lines.extend(
            [
                "## Production Default/API/User-Visible Authorization Grant",
                "",
                f"- Decision: `{grant_decision['decision']}`",
                f"- Owner: {grant_decision['owner']}",
                f"- Granted at: {grant_decision['granted_at']}",
                f"- Expiry date: {grant_decision['expiry_date']}",
                f"- Review by: {grant_decision['review_by']}",
                f"- Grant notes: {grant_decision.get('grant_notes')}",
                f"- Second reviewer: {grant_decision.get('second_reviewer')}",
                f"- Owner equivalent review: {grant_decision.get('owner_documents_equivalent_review')}",
                f"- Granted scope: `{granted_scope['authorization_scope']}`",
                f"- Missing production default/API/user-visible authorization: {posture.get('missing_prod_scoped_shadow_production_default_api_user_visible_authorization')}",
                f"- Production default allowed: {posture.get('production_default_allowed')}",
                f"- API/web changes allowed: {posture.get('api_web_changes_allowed')}",
                f"- User-visible ranking changed: {posture.get('user_visible_ranking_changed')}",
                f"- Online shadow execution enabled: {posture.get('online_shadow_execution_enabled')}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Production Default/API/User-Visible Grant Authorizes For Chain Only",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in granted_scope["authorizes_for_chain_only"])
        lines.extend(["", "## Production Default/API/User-Visible Grant Boundaries", ""])
        lines.extend(
            f"- {item}" for item in granted_scope["grant_time_production_default_api_user_visible_boundaries"]
        )
        lines.extend(["", "## Production Default/API/User-Visible Grant Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in granted_scope["explicitly_still_not_included"])
        lines.append("")
    if production_default_api_user_visible_pilot_run:
        live_source_reads = production_default_api_user_visible_pilot_run["live_source_reads"]
        row_counts = live_source_reads["row_counts"]
        incomplete = production_default_api_user_visible_pilot_run["incomplete_coverage_drill"]
        probe = production_default_api_user_visible_pilot_run["production_default_api_user_visible_probe"]
        lines.extend(
            [
                "## Production Default/API/User-Visible Pilot Run",
                "",
                f"- Pilot surface: `{production_default_api_user_visible_pilot_run['pilot_surface']}`",
                f"- Pilot run id: `{production_default_api_user_visible_pilot_run['pilot_run_id']}`",
                f"- API surface probe: `{probe['api_surface']}`",
                f"- Joined candidate count: {production_default_api_user_visible_pilot_run['input_join_summary']['joined_candidate_count']}",
                f"- Live prod source reads performed: {production_default_api_user_visible_pilot_run['live_prod_source_reads_performed']}",
                f"- Pilot passed: {production_default_api_user_visible_pilot_run['pass_fail_evaluation']['overall_passed']}",
                f"- User-visible response emitted: {probe['user_visible_response_emitted_to_users']}",
                f"- Production default changed: {probe['production_default_changed']}",
                f"- API/web changed: {probe['api_web_changed']}",
                f"- User-visible ranking changed: {probe['user_visible_ranking_changed']}",
                f"- Bridge surface included: {probe['bridge_surface_included']}",
                f"- Pilot run directory: `{production_default_api_user_visible_pilot_run['pilot_run_directory']['relative_path']}`",
                f"- Incomplete coverage status: `{incomplete['status']}`",
                f"- Incomplete coverage shadow rows: {incomplete['shadow_row_count']}",
                "",
                "## Production Default/API/User-Visible Source Reads",
                "",
                f"- Approved tables: {', '.join(live_source_reads['approved_tables'])}",
                f"- Ranking runs: {row_counts['ranking_runs']}",
                f"- Paper scores: {row_counts['paper_scores']}",
                f"- Works: {row_counts['works']}",
                f"- Embeddings: {row_counts['embeddings']}",
                f"- Candidate SHA: `{live_source_reads['input_identity_verification']['candidate_pool_work_set_sha256']}`",
                "",
                "## Production Default/API/User-Visible Pilot Checks",
                "",
            ]
        )
        for check_name in PRODUCTION_DEFAULT_API_USER_VISIBLE_PILOT_RUN_PASS_FAIL_CHECKS:
            lines.append(
                "- "
                f"`{check_name}`: "
                f"{production_default_api_user_visible_pilot_run['pass_fail_evaluation']['checks'][check_name]}"
            )
        lines.extend(
            [
                "",
                "## Production Default/API/User-Visible Pilot Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in production_default_api_user_visible_pilot_run["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
        lines.append("")
    if live_read_only_grant:
        grant_decision = live_read_only_grant
        granted_scope = authorization["live_read_only_granted_scope"]
        lines.extend(
            [
                "## Live Read-Only Authorization Grant",
                "",
                f"- Decision: `{grant_decision['decision']}`",
                f"- Owner: {grant_decision['owner']}",
                f"- Granted at: {grant_decision['granted_at']}",
                f"- Expiry date: {grant_decision['expiry_date']}",
                f"- Review by: {grant_decision['review_by']}",
                f"- Grant notes: {grant_decision.get('grant_notes')}",
                f"- Second reviewer: {grant_decision.get('second_reviewer')}",
                f"- Owner equivalent review: {grant_decision.get('owner_documents_equivalent_review')}",
                f"- Granted scope: `{granted_scope['authorization_scope']}`",
                f"- Missing live read-only authorization: {posture.get('missing_prod_scoped_shadow_live_read_only_authorization')}",
                f"- Live reads performed: {posture.get('live_prod_source_reads_performed')}",
                "",
                "## Live Read-Only Grant Boundaries",
                "",
            ]
        )
        lines.extend(f"- {item}" for item in granted_scope["grant_time_live_read_boundaries"])
        lines.extend(["", "## Live Read-Only Grant Explicitly Not Included", ""])
        lines.extend(f"- {item}" for item in granted_scope["explicitly_still_not_included"])
        lines.append("")
    if live_read_only_pilot_run:
        live_source_reads = live_read_only_pilot_run["live_source_reads"]
        row_counts = live_source_reads["row_counts"]
        lines.extend(
            [
                "## Live Read-Only Pilot Run",
                "",
                f"- Pilot surface: `{live_read_only_pilot_run['pilot_surface']}`",
                f"- Pilot run id: `{live_read_only_pilot_run['pilot_run_id']}`",
                f"- Joined candidate count: {live_read_only_pilot_run['input_join_summary']['joined_candidate_count']}",
                f"- Live prod source reads performed: {live_read_only_pilot_run['live_prod_source_reads_performed']}",
                f"- Pilot passed: {live_read_only_pilot_run['pass_fail_evaluation']['overall_passed']}",
                f"- Pilot run directory: `{live_read_only_pilot_run['pilot_run_directory']['relative_path']}`",
                f"- Labels used for scoring: {not live_source_reads['labels_not_used_for_scoring']}",
                f"- Refit/training performed: {live_source_reads['refit_training_performed']}",
                f"- Embedding generation performed: {live_source_reads['embedding_generation_performed']}",
                f"- Label ingest performed: {live_source_reads['label_ingest_performed']}",
                "",
                "## Live Read-Only Source Reads",
                "",
                f"- Approved tables: {', '.join(live_source_reads['approved_tables'])}",
                f"- Ranking runs: {row_counts['ranking_runs']}",
                f"- Paper scores: {row_counts['paper_scores']}",
                f"- Works: {row_counts['works']}",
                f"- Embeddings: {row_counts['embeddings']}",
                f"- Candidate SHA: `{live_source_reads['input_identity_verification']['candidate_pool_work_set_sha256']}`",
                "",
                "## Live Read-Only Pilot Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in live_read_only_pilot_run["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
        lines.append("")
    if live_read_only_pilot_review:
        lines.extend(
            [
                "## Live Read-Only Pilot Review",
                "",
                f"- Decision: `{live_read_only_pilot_review['decision']}`",
                f"- Reviewer: {live_read_only_pilot_review['reviewer']}",
                f"- Reviewed at: {live_read_only_pilot_review['reviewed_at']}",
                f"- Review notes: {live_read_only_pilot_review.get('review_notes')}",
                f"- Live read-only pilot accepted: {review['prod_scoped_shadow_live_read_only_pilot_accepted']}",
                f"- Failed review checks: {', '.join(live_read_only_pilot_review['failed_review_checks']) if live_read_only_pilot_review['failed_review_checks'] else 'None'}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Live Read-Only Pilot Review Checks",
                "",
            ]
        )
        for check_name in LIVE_READ_ONLY_PILOT_RUN_REVIEW_CHECKS:
            lines.append(f"- `{check_name}`: {live_read_only_pilot_review['checks'][check_name]}")
        lines.extend(["", "## Live Read-Only Pilot Review Limitations", ""])
        lines.extend(f"- {item}" for item in live_read_only_pilot_review["limitations"])
        lines.append("")
    if authorization.get("grant_decision"):
        grant_decision = authorization["grant_decision"]
        lines.extend(
            [
                "## Pilot Authorization Grant",
                "",
                f"- Decision: `{grant_decision['decision']}`",
                f"- Owner: {grant_decision['owner']}",
                f"- Granted at: {grant_decision['granted_at']}",
                f"- Expiry date: {grant_decision['expiry_date']}",
                f"- Review by: {grant_decision['review_by']}",
                f"- Grant notes: {grant_decision.get('grant_notes')}",
                f"- Second reviewer: {grant_decision.get('second_reviewer')}",
                f"- Owner equivalent review: {grant_decision.get('owner_documents_equivalent_review')}",
                f"- Granted scope: `{authorization['granted_scope']['authorization_scope']}`",
                f"- Missing pilot authorization: {posture.get('missing_prod_scoped_shadow_pilot_authorization')}",
                "",
            ]
        )
    if pilot_harness:
        lines.extend(
            [
                "## Pilot Harness",
                "",
                f"- Pilot surface: `{pilot_harness['pilot_surface']}`",
                f"- Pilot run id: `{pilot_harness['pilot_run_id']}`",
                f"- Fixture row count: {pilot_harness['fixture_row_count']}",
                f"- Live prod source reads performed: {pilot_harness['live_prod_source_reads_performed']}",
                f"- Harness passed: {pilot_harness['pass_fail_evaluation']['overall_passed']}",
                f"- Pilot executed: {execution['prod_scoped_shadow_pilot_executed']}",
                f"- Pilot run directory: `{pilot_harness['pilot_run_directory']['relative_path']}`",
                "",
                "## Pilot Harness Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in pilot_harness["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
        lines.append("")
    if review:
        decision = review["review_decision"]
        lines.extend(
            [
                "## Pilot Harness Review",
                "",
                f"- Decision: `{decision['decision']}`",
                f"- Reviewer: {decision['reviewer']}",
                f"- Reviewed at: {decision['reviewed_at']}",
                f"- Review notes: {decision.get('review_notes')}",
                f"- Harness accepted: {review['prod_scoped_shadow_pilot_harness_accepted']}",
                f"- Failed review checks: {', '.join(decision['failed_review_checks']) if decision['failed_review_checks'] else 'None'}",
                f"- Pilot executed: {execution['prod_scoped_shadow_pilot_executed']}",
                "",
                "## Pilot Harness Review Checks",
                "",
            ]
        )
        for check_name in PILOT_HARNESS_REVIEW_CHECKS:
            lines.append(f"- `{check_name}`: {decision['checks'][check_name]}")
        lines.extend(["", "## Pilot Harness Review Limitations", ""])
        lines.extend(f"- {item}" for item in decision["limitations"])
        lines.append("")
    if pilot_run:
        lines.extend(
            [
                "## Production-Scoped Pilot Run",
                "",
                f"- Pilot surface: `{pilot_run['pilot_surface']}`",
                f"- Pilot run id: `{pilot_run['pilot_run_id']}`",
                f"- Joined candidate count: {pilot_run['input_join_summary']['joined_candidate_count']}",
                f"- Live prod source reads performed: {pilot_run['live_prod_source_reads_performed']}",
                f"- Pilot passed: {pilot_run['pass_fail_evaluation']['overall_passed']}",
                f"- Pilot run directory: `{pilot_run['pilot_run_directory']['relative_path']}`",
                "",
                "## Production-Scoped Pilot Source Artifacts",
                "",
                "| Role | Path | SHA-256 |",
                "| --- | --- | --- |",
            ]
        )
        for role, record in pilot_run["source_artifacts"].items():
            lines.append(f"| {role} | `{record['path']}` | `{record['sha256']}` |")
        lines.extend(
            [
                "",
                "## Production-Scoped Pilot Files",
                "",
                "| Path | Bytes | Rows | SHA-256 |",
                "| --- | ---: | ---: | --- |",
            ]
        )
        for file_record in pilot_run["files_written"]:
            lines.append(
                f"| `{file_record['relative_path']}` | {file_record['byte_count']} | {file_record.get('row_count')} | `{file_record['sha256']}` |"
            )
        lines.append("")
    if pilot_review:
        lines.extend(
            [
                "## Production-Scoped Pilot Review",
                "",
                f"- Decision: `{pilot_review['decision']}`",
                f"- Reviewer: {pilot_review['reviewer']}",
                f"- Reviewed at: {pilot_review['reviewed_at']}",
                f"- Review notes: {pilot_review.get('review_notes')}",
                f"- Pilot accepted: {review['prod_scoped_shadow_pilot_accepted']}",
                f"- Failed review checks: {', '.join(pilot_review['failed_review_checks']) if pilot_review['failed_review_checks'] else 'None'}",
                f"- Next stage: `{payload['recommended_next_stage']}`",
                "",
                "## Production-Scoped Pilot Review Checks",
                "",
            ]
        )
        for check_name in PILOT_RUN_REVIEW_CHECKS:
            lines.append(f"- `{check_name}`: {pilot_review['checks'][check_name]}")
        lines.extend(["", "## Production-Scoped Pilot Review Limitations", ""])
        lines.extend(f"- {item}" for item in pilot_review["limitations"])
        lines.append("")
    lines.extend(
        [
            "## Explicitly Not Included",
            "",
        ]
    )
    for item in authorization["explicitly_not_included"]:
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


def write_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    production_readiness_bundle_path: Path,
    phase_bundle_path: Path,
    online_shadow_policy_path: Path,
    output_path: Path,
    markdown_output_path: Path,
    execution_authorization_grant_path: Path | None = None,
    phase2_write_mode_plan_path: Path | None = None,
    phase2_write_mode_proof_path: Path | None = None,
    generalization_audit_gates_path: Path | None = None,
    bundle_version: str = BUNDLE_VERSION,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    payload = assemble_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        production_readiness_bundle_path=production_readiness_bundle_path,
        phase_bundle_path=phase_bundle_path,
        online_shadow_policy_path=online_shadow_policy_path,
        execution_authorization_grant_path=execution_authorization_grant_path,
        phase2_write_mode_plan_path=phase2_write_mode_plan_path,
        phase2_write_mode_proof_path=phase2_write_mode_proof_path,
        generalization_audit_gates_path=generalization_audit_gates_path,
        bundle_version=bundle_version,
        repo_root=repo_root,
    )
    output_path = Path(output_path)
    markdown_output_path = Path(markdown_output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_output_path.write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(payload),
        encoding="utf-8",
    )
    return payload


def plan_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    planner: str = "Matt Maitland",
    plan_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_plan_filed=False,
    )
    updated = apply_production_scoped_shadow_plan(
        payload,
        planner=planner,
        plan_notes=plan_notes,
    )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_plan_filed=True,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def prove_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    pilot_run_id: str | None = None,
    fixture_input_path: Path | None = None,
    cleanup_after_proof: bool = False,
    prover: str = "Matt Maitland",
    proof_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_plan_filed=True,
    )
    proven_at = _now_iso_z()
    run_id = pilot_run_id or _default_pilot_run_id(proven_at)
    try:
        proof_artifacts = _build_and_write_proof_artifacts(
            repo_root=root,
            pilot_run_id=run_id,
            generated_at=proven_at,
            fixture_input_path=fixture_input_path,
            cleanup_after_proof=cleanup_after_proof,
        )
    except ShadowWritePathGuardError as exc:
        raise MLShadowScorerProductionScopedShadowBundleError(str(exc)) from exc
    updated = apply_production_scoped_shadow_proof(
        payload,
        proof_artifacts=proof_artifacts,
        prover=prover,
        proof_notes=proof_notes,
        generated_at=proven_at,
    )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_proof_filed=True,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def request_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_proof_filed=True,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    updated = apply_production_scoped_shadow_pilot_authorization_request(
        payload,
        requester=requester,
        request_notes=request_notes,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot request must preserve proof section")
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_pilot_request_filed=True,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def grant_pilot_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_pilot_request_filed=True,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    updated = apply_production_scoped_shadow_pilot_authorization_grant(
        payload,
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
        grant_notes=grant_notes,
        expiry_date=expiry_date,
        review_by=review_by,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("pilot grant must preserve proof section")
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_pilot_grant_filed=True,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def request_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if _get(payload, "authorization.prod_scoped_shadow_live_read_only_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only authorization request has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    updated = apply_production_scoped_shadow_live_read_only_authorization_request(
        payload,
        requester=requester,
        request_notes=request_notes,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve execution section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only request must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only request must preserve legacy_artifacts_index"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_live_read_only_request_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def request_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if _get(payload, "authorization.prod_scoped_shadow_live_execution_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution authorization request has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_live_read_only_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    live_grant_decision_before = deepcopy(_get(payload, "authorization.live_read_only_grant_decision"))
    live_granted_scope_before = deepcopy(_get(payload, "authorization.live_read_only_granted_scope"))
    updated = apply_production_scoped_shadow_live_execution_authorization_request(
        payload,
        requester=requester,
        request_notes=request_notes,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution request must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution request must preserve execution section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution request must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution request must preserve live read-only granted scope"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_live_execution_request_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def grant_live_execution_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if _get(payload, "authorization.prod_scoped_shadow_live_execution_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution authorization grant has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_live_execution_request_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(payload, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(payload, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(payload, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(payload, "authorization.live_execution_requested_scope"))
    updated = apply_production_scoped_shadow_live_execution_authorization_grant(
        payload,
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
        grant_notes=grant_notes,
        expiry_date=expiry_date,
        review_by=review_by,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve execution section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live execution grant must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live execution grant must preserve live execution requested scope"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_live_execution_grant_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def grant_live_read_only_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if _get(payload, "authorization.prod_scoped_shadow_live_read_only_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only authorization grant has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_live_read_only_request_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    updated = apply_production_scoped_shadow_live_read_only_authorization_grant(
        payload,
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
        grant_notes=grant_notes,
        expiry_date=expiry_date,
        review_by=review_by,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve execution section")
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("live read-only grant must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve live request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "live read-only grant must preserve live requested scope"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_live_read_only_grant_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def request_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if _get(payload, "authorization.prod_scoped_shadow_flag_enablement_authorization_requested") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization request has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_live_execution_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(payload, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(payload, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(payload, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(payload, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(payload, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(payload, "authorization.live_execution_granted_scope"))
    updated = apply_production_scoped_shadow_flag_enablement_authorization_request(
        payload,
        requester=requester,
        request_notes=request_notes,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement request must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement request must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement request must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution requested scope"
        )
    if _get(updated, "authorization.live_execution_grant_decision") != live_execution_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution grant decision"
        )
    if _get(updated, "authorization.live_execution_granted_scope") != live_execution_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement request must preserve live execution granted scope"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def request_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    requester: str = "Matt Maitland",
    request_notes: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if (
        _get(
            payload,
            "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_requested",
        )
        is True
    ):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization request has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_flag_enablement_pilot_review_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(payload, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(payload, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(payload, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(payload, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(payload, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(payload, "authorization.live_execution_granted_scope"))
    flag_enablement_request_decision_before = deepcopy(_get(payload, "authorization.flag_enablement_request_decision"))
    flag_enablement_requested_scope_before = deepcopy(_get(payload, "authorization.flag_enablement_requested_scope"))
    flag_enablement_grant_decision_before = deepcopy(_get(payload, "authorization.flag_enablement_grant_decision"))
    flag_enablement_granted_scope_before = deepcopy(_get(payload, "authorization.flag_enablement_granted_scope"))
    updated = apply_production_scoped_shadow_production_default_api_user_visible_authorization_request(
        payload,
        requester=requester,
        request_notes=request_notes,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve review section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible request must preserve legacy_artifacts_index"
        )
    for path, before_value, label in (
        ("authorization.grant_decision", pilot_grant_decision_before, "pilot grant decision"),
        ("authorization.granted_scope", pilot_granted_scope_before, "pilot granted scope"),
        ("authorization.request_decision", live_request_decision_before, "live read-only request decision"),
        ("authorization.requested_scope", live_requested_scope_before, "live read-only requested scope"),
        (
            "authorization.live_read_only_grant_decision",
            live_read_only_grant_decision_before,
            "live read-only grant decision",
        ),
        (
            "authorization.live_read_only_granted_scope",
            live_read_only_granted_scope_before,
            "live read-only granted scope",
        ),
        (
            "authorization.live_execution_request_decision",
            live_execution_request_decision_before,
            "live execution request decision",
        ),
        (
            "authorization.live_execution_requested_scope",
            live_execution_requested_scope_before,
            "live execution requested scope",
        ),
        (
            "authorization.live_execution_grant_decision",
            live_execution_grant_decision_before,
            "live execution grant decision",
        ),
        (
            "authorization.live_execution_granted_scope",
            live_execution_granted_scope_before,
            "live execution granted scope",
        ),
        (
            "authorization.flag_enablement_request_decision",
            flag_enablement_request_decision_before,
            "flag enablement request decision",
        ),
        (
            "authorization.flag_enablement_requested_scope",
            flag_enablement_requested_scope_before,
            "flag enablement requested scope",
        ),
        (
            "authorization.flag_enablement_grant_decision",
            flag_enablement_grant_decision_before,
            "flag enablement grant decision",
        ),
        (
            "authorization.flag_enablement_granted_scope",
            flag_enablement_granted_scope_before,
            "flag enablement granted scope",
        ),
    ):
        if _get(updated, path) != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production default/API/user-visible request must preserve {label}"
            )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_production_default_api_user_visible_request_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def grant_flag_enablement_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if _get(payload, "authorization.prod_scoped_shadow_flag_enablement_authorization_granted") is True:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement authorization grant has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_flag_enablement_request_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(payload, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(payload, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(payload, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(payload, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(payload, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(payload, "authorization.live_execution_granted_scope"))
    flag_enablement_request_decision_before = deepcopy(_get(payload, "authorization.flag_enablement_request_decision"))
    flag_enablement_requested_scope_before = deepcopy(_get(payload, "authorization.flag_enablement_requested_scope"))
    updated = apply_production_scoped_shadow_flag_enablement_authorization_grant(
        payload,
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
        grant_notes=grant_notes,
        expiry_date=expiry_date,
        review_by=review_by,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve plan section")
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve proof section")
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError("flag enablement grant must preserve review section")
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve legacy_artifacts_index"
        )
    if _get(updated, "authorization.grant_decision") != pilot_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve pilot grant decision"
        )
    if _get(updated, "authorization.granted_scope") != pilot_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve pilot granted scope"
        )
    if _get(updated, "authorization.request_decision") != live_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only request decision"
        )
    if _get(updated, "authorization.requested_scope") != live_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only requested scope"
        )
    if _get(updated, "authorization.live_read_only_grant_decision") != live_read_only_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only grant decision"
        )
    if _get(updated, "authorization.live_read_only_granted_scope") != live_read_only_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live read-only granted scope"
        )
    if _get(updated, "authorization.live_execution_request_decision") != live_execution_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution request decision"
        )
    if _get(updated, "authorization.live_execution_requested_scope") != live_execution_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution requested scope"
        )
    if _get(updated, "authorization.live_execution_grant_decision") != live_execution_grant_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution grant decision"
        )
    if _get(updated, "authorization.live_execution_granted_scope") != live_execution_granted_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve live execution granted scope"
        )
    if _get(updated, "authorization.flag_enablement_request_decision") != flag_enablement_request_decision_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve flag enablement request decision"
        )
    if _get(updated, "authorization.flag_enablement_requested_scope") != flag_enablement_requested_scope_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "flag enablement grant must preserve flag enablement requested scope"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_flag_enablement_grant_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated


def grant_production_default_api_user_visible_ml_shadow_scorer_production_scoped_shadow_bundle(
    *,
    bundle_path: Path,
    owner: str = "Matt Maitland",
    second_reviewer: str | None = None,
    owner_documents_equivalent_review: str | None = None,
    grant_notes: str | None = None,
    expiry_date: str = "2026-08-27",
    review_by: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve() if repo_root is not None else default_repo_root()
    bundle_path = Path(bundle_path).resolve()
    payload = _load_json_object(bundle_path)
    if (
        _get(
            payload,
            "authorization.prod_scoped_shadow_production_default_api_user_visible_authorization_granted",
        )
        is True
    ):
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible authorization grant has already been filed"
        )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        payload,
        repo_root=root,
        expect_production_default_api_user_visible_request_filed=True,
        verify_local_pilot_files=False,
    )
    plan_before = deepcopy(payload.get("plan"))
    proof_before = deepcopy(payload.get("proof"))
    execution_before = deepcopy(payload.get("execution"))
    review_before = deepcopy(payload.get("review"))
    legacy_index_before = deepcopy(_get(payload, "metadata.legacy_artifacts_index"))
    pilot_grant_decision_before = deepcopy(_get(payload, "authorization.grant_decision"))
    pilot_granted_scope_before = deepcopy(_get(payload, "authorization.granted_scope"))
    live_request_decision_before = deepcopy(_get(payload, "authorization.request_decision"))
    live_requested_scope_before = deepcopy(_get(payload, "authorization.requested_scope"))
    live_read_only_grant_decision_before = deepcopy(_get(payload, "authorization.live_read_only_grant_decision"))
    live_read_only_granted_scope_before = deepcopy(_get(payload, "authorization.live_read_only_granted_scope"))
    live_execution_request_decision_before = deepcopy(_get(payload, "authorization.live_execution_request_decision"))
    live_execution_requested_scope_before = deepcopy(_get(payload, "authorization.live_execution_requested_scope"))
    live_execution_grant_decision_before = deepcopy(_get(payload, "authorization.live_execution_grant_decision"))
    live_execution_granted_scope_before = deepcopy(_get(payload, "authorization.live_execution_granted_scope"))
    flag_enablement_request_decision_before = deepcopy(_get(payload, "authorization.flag_enablement_request_decision"))
    flag_enablement_requested_scope_before = deepcopy(_get(payload, "authorization.flag_enablement_requested_scope"))
    flag_enablement_grant_decision_before = deepcopy(_get(payload, "authorization.flag_enablement_grant_decision"))
    flag_enablement_granted_scope_before = deepcopy(_get(payload, "authorization.flag_enablement_granted_scope"))
    production_default_request_decision_before = deepcopy(
        _get(payload, "authorization.production_default_api_user_visible_request_decision")
    )
    production_default_requested_scope_before = deepcopy(
        _get(payload, "authorization.production_default_api_user_visible_requested_scope")
    )
    updated = apply_production_scoped_shadow_production_default_api_user_visible_authorization_grant(
        payload,
        owner=owner,
        second_reviewer=second_reviewer,
        owner_documents_equivalent_review=owner_documents_equivalent_review,
        grant_notes=grant_notes,
        expiry_date=expiry_date,
        review_by=review_by,
    )
    if updated.get("plan") != plan_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve plan section"
        )
    if updated.get("proof") != proof_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve proof section"
        )
    if updated.get("execution") != execution_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve execution section"
        )
    if updated.get("review") != review_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve review section"
        )
    if _get(updated, "metadata.legacy_artifacts_index") != legacy_index_before:
        raise MLShadowScorerProductionScopedShadowBundleError(
            "production default/API/user-visible grant must preserve legacy_artifacts_index"
        )
    for path, before_value, label in (
        ("authorization.grant_decision", pilot_grant_decision_before, "pilot grant decision"),
        ("authorization.granted_scope", pilot_granted_scope_before, "pilot granted scope"),
        ("authorization.request_decision", live_request_decision_before, "live read-only request decision"),
        ("authorization.requested_scope", live_requested_scope_before, "live read-only requested scope"),
        (
            "authorization.live_read_only_grant_decision",
            live_read_only_grant_decision_before,
            "live read-only grant decision",
        ),
        (
            "authorization.live_read_only_granted_scope",
            live_read_only_granted_scope_before,
            "live read-only granted scope",
        ),
        (
            "authorization.live_execution_request_decision",
            live_execution_request_decision_before,
            "live execution request decision",
        ),
        (
            "authorization.live_execution_requested_scope",
            live_execution_requested_scope_before,
            "live execution requested scope",
        ),
        (
            "authorization.live_execution_grant_decision",
            live_execution_grant_decision_before,
            "live execution grant decision",
        ),
        (
            "authorization.live_execution_granted_scope",
            live_execution_granted_scope_before,
            "live execution granted scope",
        ),
        (
            "authorization.flag_enablement_request_decision",
            flag_enablement_request_decision_before,
            "flag enablement request decision",
        ),
        (
            "authorization.flag_enablement_requested_scope",
            flag_enablement_requested_scope_before,
            "flag enablement requested scope",
        ),
        (
            "authorization.flag_enablement_grant_decision",
            flag_enablement_grant_decision_before,
            "flag enablement grant decision",
        ),
        (
            "authorization.flag_enablement_granted_scope",
            flag_enablement_granted_scope_before,
            "flag enablement granted scope",
        ),
        (
            "authorization.production_default_api_user_visible_request_decision",
            production_default_request_decision_before,
            "production default/API/user-visible request decision",
        ),
        (
            "authorization.production_default_api_user_visible_requested_scope",
            production_default_requested_scope_before,
            "production default/API/user-visible requested scope",
        ),
    ):
        if _get(updated, path) != before_value:
            raise MLShadowScorerProductionScopedShadowBundleError(
                f"production default/API/user-visible grant must preserve {label}"
            )
    verify_ml_shadow_scorer_production_scoped_shadow_bundle_payload(
        updated,
        repo_root=root,
        expect_production_default_api_user_visible_grant_filed=True,
        verify_local_pilot_files=False,
    )
    bundle_path.write_text(json.dumps(updated, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    bundle_path.with_name("bundle.md").write_text(
        markdown_from_ml_shadow_scorer_production_scoped_shadow_bundle(updated),
        encoding="utf-8",
    )
    return updated

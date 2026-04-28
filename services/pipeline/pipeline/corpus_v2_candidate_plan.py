"""Corpus v2 candidate selection dry-run: OpenAlex only, no Postgres, no snapshot."""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from pipeline.corpus_expansion_preview import (
    _works_list_url,  # noqa: SLF001
    expansion_bucket_definitions,
    resolve_corpus_expansion_preview_mailto,
)
from pipeline.openalex import DEFAULT_SELECT_FIELDS
from pipeline.openalex_client import fetch_openalex_json
from pipeline.openalex_text import abstract_plain_text
from pipeline.policy import CorpusPolicy

PreviewFetch = Callable[[str], Mapping[str, Any]]

# First-pass selection caps (per bucket) — aligned with expansion preview bucket ids.
V2_BUCKET_CAPS: dict[str, int] = {
    "core_mir_existing_sources": 80,
    "ismir_proceedings_or_mir_conference": 80,
    "audio_ml_signal_processing": 60,
    "music_recommender_systems": 50,
    "symbolic_music_and_harmony": 40,
    "source_separation_benchmarks": 40,
    "cultural_computational_musicology": 25,
    "ethics_law_fairness_user_studies": 25,
}

V2_BUCKET_ORDER: tuple[str, ...] = tuple(V2_BUCKET_CAPS.keys())

DEFER_BUCKETS: frozenset[str] = frozenset(
    {"cultural_computational_musicology", "ethics_law_fairness_user_studies"}
)

FETCH_PAGE_SIZE = 25
MAX_REJECTED_EXAMPLES = 8


def _plan_text_blob(work: Mapping[str, Any]) -> str:
    title = str(work.get("title") or "")
    abstract = abstract_plain_text(work)
    topics = work.get("topics") or ()
    topic_names: list[str] = []
    for topic in topics:
        if isinstance(topic, Mapping):
            name = topic.get("display_name") or topic.get("name")
            if name:
                topic_names.append(str(name))
    return " ".join([title, abstract, *topic_names]).casefold()


def _norm_doi(work: Mapping[str, Any]) -> str | None:
    raw = work.get("doi")
    if not raw or not isinstance(raw, str):
        return None
    d = raw.strip().casefold()
    for prefix in ("https://doi.org/", "http://doi.org/"):
        if d.startswith(prefix):
            d = d[len(prefix) :]
    return d or None


def _norm_oa_id(work: Mapping[str, Any]) -> str:
    return str(work.get("id") or "").strip()


def _norm_title_key(work: Mapping[str, Any]) -> str:
    t = str(work.get("title") or "").strip().casefold()
    t = re.sub(r"\s+", " ", t)
    return t[:280] if t else ""


def _music_mir_hook(blob: str) -> bool:
    hooks = (
        "music",
        "musical",
        "midi",
        "ismir",
        "music information retrieval",
        " mir",
        "mir ",
        "playlist",
        "harmony",
        "melody",
        "score",
        "symbolic",
        "demix",
        "musdb",
        "sisec",
        "source separation",
        "audio representation",
        "music tagging",
        "beat track",
    )
    return any(h in blob for h in hooks)


def _strong_topic_matches(policy: CorpusPolicy, blob: str) -> list[str]:
    out: list[str] = []
    for term in policy.strong_topic_signals:
        if term in blob:
            out.append(term)
    for term in policy.controlled_edge_terms:
        if term in blob:
            out.append(term)
    return sorted(set(out))


def _base_policy_pass(work: Mapping[str, Any], policy: CorpusPolicy) -> tuple[bool, str]:
    year = int(work.get("publication_year") or work.get("year") or 0)
    if year < policy.min_year:
        return False, "before_year_floor"
    lang = str(work.get("language") or "").casefold()
    if lang != policy.language:
        return False, "language_mismatch"
    wtype = str(work.get("type") or "").casefold()
    if wtype not in policy.include_document_types:
        return False, "document_type_excluded"
    if policy.exclude_retracted and bool(work.get("is_retracted")):
        return False, "retracted"
    plain = abstract_plain_text(work)
    has_abs = bool(work.get("has_abstract")) or bool(plain.strip())
    if policy.require_abstract and not has_abs:
        return False, "missing_abstract"
    return True, ""


def _exclusion_hits(policy: CorpusPolicy, blob: str) -> list[str]:
    return [t for t in policy.exclusion_terms if t in blob]


def _noise_animal_only(blob: str) -> bool:
    animals = (
        "manatee",
        "dugong",
        "marine mammal",
        "ornithology",
        "zoology",
        "wildlife conservation",
        "whale song",
    )
    return any(a in blob for a in animals) and not _music_mir_hook(blob)


def _noise_generic_database(blob: str) -> bool:
    db = ("relational database", "rdbms", "sql server", "database management system")
    generic = ("nearest neighbor", "k-nearest neighbor", "knn ")
    if any(d in blob for d in db) and not _music_mir_hook(blob):
        return True
    if any(g in blob for g in generic) and "music" not in blob and "audio" not in blob:
        return True
    return False


def _noise_speech_only(blob: str) -> bool:
    speech = (
        "speaker verification",
        "speaker recognition",
        "voice biometrics",
        "automatic speech recognition",
        "speech-only",
        "speech only",
        "text-to-speech",
        "speech synthesis",
    )
    return any(s in blob for s in speech) and not _music_mir_hook(blob)


def _noise_biomedical_audio(blob: str) -> bool:
    med = ("medical audio", "biomedical audio", "clinical audio", "patient monitoring audio")
    return any(m in blob for m in med) and not _music_mir_hook(blob)


def _noise_ecommerce_recsys(blob: str) -> bool:
    bad = ("e-commerce", "ecommerce", "product recommendation", "shopping cart", "retail recommendation")
    return any(b in blob for b in bad) and "music" not in blob


def _bucket_allow(bucket_id: str, blob: str, policy: CorpusPolicy) -> tuple[bool, list[str]]:
    matched: list[str] = []
    if bucket_id == "core_mir_existing_sources":
        return True, ["core_source_query"]
    if bucket_id == "ismir_proceedings_or_mir_conference":
        ok = (
            "ismir" in blob
            or "music information retrieval" in blob
            or "music retrieval" in blob
            or ("audio" in blob and "music" in blob)
        )
        if ok:
            matched.append("ismir_or_mir_context")
        return ok, matched
    if bucket_id == "audio_ml_signal_processing":
        ok = ("audio" in blob or "signal" in blob) and (
            "music" in blob
            or "mir" in blob
            or "representation" in blob
            or "embedding" in blob
            or "separation" in blob
            or "spectrogram" in blob
            or "music information retrieval" in blob
        )
        if ok:
            matched.append("audio_ml_music_context")
        return ok, matched
    if bucket_id == "music_recommender_systems":
        ok = "music" in blob and (
            "recommend" in blob or "playlist" in blob or "listener" in blob or "session" in blob
        )
        if ok:
            matched.append("music_recsys_context")
        return ok, matched
    if bucket_id == "symbolic_music_and_harmony":
        ok = (
            "harmony" in blob
            or "counterpoint" in blob
            or "musicxml" in blob
            or ("score" in blob and "music" in blob)
            or "symbolic music" in blob
            or "chord" in blob
        )
        if ok:
            matched.append("symbolic_music_context")
        return ok, matched
    if bucket_id == "source_separation_benchmarks":
        sep = "source separation" in blob or "demix" in blob or "stem" in blob or "musdb" in blob or "sisec" in blob
        ok = sep and _music_mir_hook(blob)
        if ok:
            matched.append("music_separation_context")
        return ok, matched
    if bucket_id == "cultural_computational_musicology":
        ok = "musicology" in blob or "computational music" in blob or "digital musicology" in blob
        if ok:
            matched.append("musicology_context")
        return ok, matched
    if bucket_id == "ethics_law_fairness_user_studies":
        ok = (
            "music" in blob
            and (
                "fairness" in blob
                or "ethics" in blob
                or "user study" in blob
                or "dataset" in blob
                or "bias" in blob
            )
        )
        if ok:
            matched.append("music_ethics_context")
        return ok, matched
    return False, []


def _defer_bucket_pass(
    bucket_id: str,
    blob: str,
    policy: CorpusPolicy,
    strong: list[str],
) -> tuple[bool, str]:
    if bucket_id == "cultural_computational_musicology":
        if len(strong) < 1:
            return False, "defer_bucket_requires_strong_topic_signal"
        if not ("musicology" in blob or "computational music" in blob or "digital musicology" in blob):
            return False, "defer_bucket_weak_musicology_hook"
        return True, ""
    if bucket_id == "ethics_law_fairness_user_studies":
        if len(strong) < 1:
            return False, "defer_bucket_requires_strong_topic_signal"
        if "music" not in blob:
            return False, "defer_bucket_weak_music_hook"
        return True, ""
    return True, ""


def evaluate_v2_candidate(
    work: Mapping[str, Any],
    *,
    policy: CorpusPolicy,
    bucket_id: str,
) -> dict[str, Any]:
    """
    Deterministic filter. Returns dict with keys: included (bool), exclusion_reason, matched_terms,
    inclusion_reason.
    """
    blob = _plan_text_blob(work)
    ok, reason = _base_policy_pass(work, policy)
    if not ok:
        return {
            "included": False,
            "exclusion_reason": reason,
            "matched_terms": [],
            "inclusion_reason": None,
        }
    excl = _exclusion_hits(policy, blob)
    if excl:
        return {
            "included": False,
            "exclusion_reason": "explicit_exclusion_term",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if _noise_animal_only(blob):
        return {
            "included": False,
            "exclusion_reason": "noise_animal_or_non_music_biology",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if _noise_generic_database(blob):
        return {
            "included": False,
            "exclusion_reason": "noise_generic_database_without_music_hook",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if _noise_speech_only(blob):
        return {
            "included": False,
            "exclusion_reason": "noise_speech_focus_without_music_hook",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if _noise_biomedical_audio(blob):
        return {
            "included": False,
            "exclusion_reason": "noise_biomedical_audio_without_music_hook",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if bucket_id == "music_recommender_systems" and _noise_ecommerce_recsys(blob):
        return {
            "included": False,
            "exclusion_reason": "noise_ecommerce_recommendation_without_music",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if bucket_id == "symbolic_music_and_harmony" and "sheet music" in blob and "ocr" in blob and "music" not in blob:
        return {
            "included": False,
            "exclusion_reason": "noise_sheet_image_ocr_without_music_context",
            "matched_terms": [],
            "inclusion_reason": None,
        }

    strong = _strong_topic_matches(policy, blob)
    allow_ok, allow_matched = _bucket_allow(bucket_id, blob, policy)
    if not strong and not allow_ok:
        return {
            "included": False,
            "exclusion_reason": "no_strong_topic_or_bucket_allow_signal",
            "matched_terms": [],
            "inclusion_reason": None,
        }
    if bucket_id in DEFER_BUCKETS:
        d_ok, d_reason = _defer_bucket_pass(bucket_id, blob, policy, strong)
        if not d_ok:
            return {
                "included": False,
                "exclusion_reason": d_reason,
                "matched_terms": strong + allow_matched,
                "inclusion_reason": None,
            }

    matched = list(dict.fromkeys(strong + allow_matched))
    if strong and allow_ok:
        inc_reason = "strong_topic_and_bucket_allow"
    elif strong:
        inc_reason = "strong_topic_signal"
    else:
        inc_reason = "bucket_allow_signal"
    return {
        "included": True,
        "exclusion_reason": None,
        "matched_terms": matched,
        "inclusion_reason": inc_reason,
    }


def _work_to_candidate_row(work: Mapping[str, Any], *, bucket_id: str, ev: dict[str, Any]) -> dict[str, Any]:
    pl = work.get("primary_location")
    src_name = None
    if isinstance(pl, Mapping):
        s = pl.get("source")
        if isinstance(s, Mapping):
            src_name = s.get("display_name")
    return {
        "openalex_id": _norm_oa_id(work),
        "doi": _norm_doi(work),
        "title": work.get("title"),
        "year": work.get("publication_year"),
        "citation_count": int(work.get("cited_by_count") or 0),
        "source_display_name": str(src_name) if src_name else None,
        "bucket_id": bucket_id,
        "inclusion_reason": ev["inclusion_reason"],
        "matched_terms": ev["matched_terms"],
        "exclusion_reason": ev.get("exclusion_reason"),
    }


@dataclass
class _DedupTracker:
    seen_oa: set[str] = field(default_factory=set)
    seen_doi: set[str] = field(default_factory=set)
    seen_title: set[str] = field(default_factory=set)
    drops_by_oa: int = 0
    drops_by_doi: int = 0
    drops_by_title: int = 0

    def try_add(self, work: Mapping[str, Any]) -> bool:
        oa = _norm_oa_id(work)
        doi = _norm_doi(work)
        tk = _norm_title_key(work)
        if not oa:
            return False
        if oa in self.seen_oa:
            self.drops_by_oa += 1
            return False
        if doi and doi in self.seen_doi:
            self.drops_by_doi += 1
            return False
        if tk and tk in self.seen_title:
            self.drops_by_title += 1
            return False
        self.seen_oa.add(oa)
        if doi:
            self.seen_doi.add(doi)
        if tk:
            self.seen_title.add(tk)
        return True


def run_corpus_v2_candidate_plan(
    *,
    policy: CorpusPolicy,
    mailto: str,
    per_bucket_limit: int,
    target_min: int,
    target_max: int,
    fetch: PreviewFetch | None = None,
    mock_openalex: bool = False,
) -> dict[str, Any]:
    if per_bucket_limit < 1 or per_bucket_limit > 500:
        raise ValueError("per_bucket_limit must be between 1 and 500")
    if target_min < 1 or target_max < target_min:
        raise ValueError("invalid target_min/target_max")
    if fetch is not None:
        fetch_fn = fetch
    elif mock_openalex:
        fetch_fn = lambda _u: {"meta": {"count": 0, "next_cursor": None}, "results": []}
    else:
        fetch_fn = lambda u: fetch_openalex_json(u, mailto=mailto, timeout_sec=90.0)

    cli_mail = (mailto or "").strip()
    env_mail = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    if mock_openalex:
        contact_mode = "mock"
        contact_provided = bool(cli_mail or env_mail)
    elif cli_mail:
        contact_mode = "cli"
        contact_provided = True
    elif env_mail:
        contact_mode = "env"
        contact_provided = True
    else:
        contact_mode = "none"
        contact_provided = False

    specs = {s.bucket_id: s for s in expansion_bucket_definitions()}
    bucket_summaries: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    rejected_examples: dict[str, list[dict[str, Any]]] = {bid: [] for bid in V2_BUCKET_ORDER}
    dedup = _DedupTracker()

    for bucket_id in V2_BUCKET_ORDER:
        cap = V2_BUCKET_CAPS[bucket_id]
        spec = specs.get(bucket_id)
        if spec is None:
            continue
        if len(selected) >= target_max:
            bucket_summaries.append(
                {
                    "bucket_id": bucket_id,
                    "per_bucket_cap": cap,
                    "raw_candidates_fetched": 0,
                    "passed_filter_count": 0,
                    "selected_count_after_dedup_and_cap": 0,
                    "skipped_due_to_global_target_max": True,
                }
            )
            continue
        raw_params = spec.build_params(policy)
        raw_fetched = 0
        passed_filter = 0
        selected_after_dedup = 0
        bucket_selected_rows: list[dict[str, Any]] = []

        params = dict(raw_params)
        params["per-page"] = str(min(FETCH_PAGE_SIZE, per_bucket_limit))
        cursor = "*"
        while raw_fetched < per_bucket_limit and len(selected) < target_max:
            p = dict(params)
            p["cursor"] = cursor
            url = _works_list_url(p, DEFAULT_SELECT_FIELDS)
            payload = fetch_fn(url)
            results = list(payload.get("results") or [])
            meta = payload.get("meta") or {}
            next_cursor = meta.get("next_cursor")
            for w in results:
                if raw_fetched >= per_bucket_limit or len(selected) >= target_max:
                    break
                if not isinstance(w, Mapping):
                    continue
                raw_fetched += 1
                ev = evaluate_v2_candidate(w, policy=policy, bucket_id=bucket_id)
                if ev["included"]:
                    passed_filter += 1
                else:
                    if len(rejected_examples[bucket_id]) < MAX_REJECTED_EXAMPLES:
                        rejected_examples[bucket_id].append(
                            {
                                "openalex_id": _norm_oa_id(w),
                                "title": w.get("title"),
                                "exclusion_reason": ev["exclusion_reason"],
                                "matched_terms": ev.get("matched_terms") or [],
                            }
                        )
                    continue
                if selected_after_dedup >= cap:
                    continue
                if len(selected) >= target_max:
                    break
                if not dedup.try_add(w):
                    continue
                row = _work_to_candidate_row(w, bucket_id=bucket_id, ev=ev)
                selected.append(row)
                bucket_selected_rows.append(row)
                selected_after_dedup += 1
            if raw_fetched >= per_bucket_limit or len(selected) >= target_max:
                break
            if not results or not next_cursor:
                break
            cursor = str(next_cursor)

        bucket_summaries.append(
            {
                "bucket_id": bucket_id,
                "per_bucket_cap": cap,
                "raw_candidates_fetched": raw_fetched,
                "passed_filter_count": passed_filter,
                "selected_count_after_dedup_and_cap": selected_after_dedup,
            }
        )

    selected_total = len(selected)
    caveats: list[str] = [
        "Dry-run only: no Postgres writes, no snapshot, no embeddings, clustering, or ranking.",
        "Candidate list is not a benchmark and does not validate retrieval or bridge quality.",
        "Old vs new corpus metrics must not be compared as same-pool performance.",
    ]
    if selected_total < target_min:
        caveats.append(
            f"selected_total ({selected_total}) is below target_min ({target_min}); widen limits, "
            "add buckets, or run live fetch with higher per_bucket_limit after policy review."
        )
    if selected_total >= target_max:
        caveats.append(f"Selection capped at target_max ({target_max}); some passing candidates may be truncated.")

    versioning = {
        "new_corpus_snapshot_version": "required_before_ingest",
        "new_embedding_version": "required_after_snapshot",
        "new_cluster_version": "required_after_embeddings",
        "new_zero_bridge_ranking_version": "required_before_bridge_weight_experiments",
    }
    recommended_scope = (
        "Approve a corpus-v2 ingest policy update that uses this candidate set (or a subset) as the first expansion "
        "tranche, then create a new snapshot version, re-embed, re-cluster, and run a zero–bridge-weight ranking "
        "before any bridge-weight tuning."
    )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "contact_provided": contact_provided,
        "contact_mode": contact_mode,
        "policy_reference": {"name": policy.name, "policy_hash": policy.policy_hash},
        "target_min": target_min,
        "target_max": target_max,
        "per_bucket_fetch_limit": per_bucket_limit,
        "selected_total": selected_total,
        "bucket_summaries": bucket_summaries,
        "selected_candidates": selected,
        "rejected_noise_examples_by_bucket": rejected_examples,
        "dedup_statistics": {
            "drops_by_openalex_id": dedup.drops_by_oa,
            "drops_by_doi": dedup.drops_by_doi,
            "drops_by_normalized_title": dedup.drops_by_title,
            "unique_openalex_ids_kept": len(dedup.seen_oa),
        },
        "recommended_ingest_scope": recommended_scope,
        "versioning_implications": versioning,
        "caveats": caveats,
    }


def render_corpus_v2_plan_markdown(plan: Mapping[str, Any]) -> str:
    lines = [
        "# Corpus v2 candidate plan (dry-run)",
        "",
        "This document is **planning output only**. It does **not** validate benchmarks, retrieval quality, or bridge "
        "readiness. **No database writes** were performed to produce this artifact.",
        "",
        "## Totals",
        "",
        f"- **selected_total:** `{plan.get('selected_total')}` (target range `{plan.get('target_min')}`–`{plan.get('target_max')}`)",
        f"- **contact_mode:** `{plan.get('contact_mode')}` (raw mailto is not stored in this file)",
        "",
        "## Selected by bucket",
        "",
    ]
    for b in plan.get("bucket_summaries") or []:
        lines.append(
            f"- **{b.get('bucket_id')}:** selected `{b.get('selected_count_after_dedup_and_cap')}` "
            f"(raw fetched `{b.get('raw_candidates_fetched')}`, passed filter `{b.get('passed_filter_count')}`, "
            f"cap `{b.get('per_bucket_cap')}`)"
        )
    lines.extend(
        [
            "",
            "## Recommended first-pass corpus-v2 scope",
            "",
            str(plan.get("recommended_ingest_scope") or ""),
            "",
            "## Noisy / defer-heavy buckets",
            "",
            "- Buckets `cultural_computational_musicology` and `ethics_law_fairness_user_studies` use stricter "
            "music/MIR hooks in this dry-run; low counts are expected.",
            "",
            "## Dedup statistics",
            "",
        ]
    )
    ds = plan.get("dedup_statistics") or {}
    lines.append(f"- **drops_by_openalex_id:** `{ds.get('drops_by_openalex_id')}`")
    lines.append(f"- **drops_by_doi:** `{ds.get('drops_by_doi')}`")
    lines.append(f"- **drops_by_normalized_title:** `{ds.get('drops_by_normalized_title')}`")
    lines.append(f"- **unique_openalex_ids_kept:** `{ds.get('unique_openalex_ids_kept')}`")
    lines.extend(["", "## Accepted examples (first 5)", ""])
    for row in (plan.get("selected_candidates") or [])[:5]:
        lines.append(f"- `{row.get('openalex_id')}` — {row.get('title')!r} (`{row.get('bucket_id')}`)")
    lines.extend(["", "## Rejected examples (first 3 per bucket, truncated)", ""])
    for bid, examples in (plan.get("rejected_noise_examples_by_bucket") or {}).items():
        if not examples:
            continue
        lines.append(f"### {bid}")
        for ex in examples[:3]:
            lines.append(
                f"- `{ex.get('openalex_id')}` — {ex.get('title')!r} — **{ex.get('exclusion_reason')}**"
            )
    lines.extend(["", "## Caveats", ""])
    for c in plan.get("caveats") or []:
        lines.append(f"- {c}")
    lines.extend(
        [
            "",
            "## Versioning implications",
            "",
        ]
    )
    for k, v in (plan.get("versioning_implications") or {}).items():
        lines.append(f"- **{k}:** {v}")
    return "\n".join(lines).rstrip() + "\n"


def run_corpus_v2_candidate_plan_from_cli(
    *,
    output: Path,
    markdown_output: Path,
    mailto: str,
    per_bucket_limit: int,
    target_min: int,
    target_max: int,
    mock_openalex: bool,
) -> None:
    policy = CorpusPolicy()
    m = resolve_corpus_expansion_preview_mailto(mailto=mailto, mock_openalex=mock_openalex)
    if not mock_openalex and not (mailto or "").strip() and not (os.environ.get("OPENALEX_MAILTO") or "").strip():
        print(
            "corpus-v2-candidate-plan: live mode requires contact: pass --mailto or set OPENALEX_MAILTO",
            file=sys.stderr,
        )
        raise SystemExit(2)
    plan = run_corpus_v2_candidate_plan(
        policy=policy,
        mailto=m,
        per_bucket_limit=per_bucket_limit,
        target_min=target_min,
        target_max=target_max,
        mock_openalex=mock_openalex,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(render_corpus_v2_plan_markdown(plan), encoding="utf-8", newline="\n")


__all__ = [
    "V2_BUCKET_CAPS",
    "V2_BUCKET_ORDER",
    "evaluate_v2_candidate",
    "render_corpus_v2_plan_markdown",
    "run_corpus_v2_candidate_plan",
    "run_corpus_v2_candidate_plan_from_cli",
]

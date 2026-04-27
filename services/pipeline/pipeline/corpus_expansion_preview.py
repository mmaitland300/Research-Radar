from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode

from pipeline.openalex import DEFAULT_PER_PAGE, DEFAULT_SELECT_FIELDS, OPENALEX_WORKS_URL
from pipeline.openalex_client import fetch_openalex_json
from pipeline.openalex_text import abstract_plain_text
from pipeline.policy import CorpusPolicy

PreviewFetch = Callable[[str], Mapping[str, Any]]

# Schema surfaces for tests
REQUIRED_PREVIEW_TOP_LEVEL_KEYS: tuple[str, ...] = (
    "generated_at",
    "mailto",
    "policy_reference",
    "buckets",
    "recommendation",
    "versioning_implications",
    "caveats",
    "openalex_mode",
)
REQUIRED_BUCKET_KEYS: tuple[str, ...] = (
    "bucket_id",
    "rationale",
    "expected_role_in_bridge_discovery",
    "openalex_query_filter_strategy",
    "estimated_candidate_count",
    "sample_works",
    "likely_risks_noise_sources",
    "api_request",
)
REQUIRED_SAMPLE_WORK_KEYS: tuple[str, ...] = (
    "openalex_id",
    "title",
    "year",
    "citation_count",
    "source_display_name",
    "abstract_present",
)


@dataclass(frozen=True)
class CorpusExpansionBucketSpec:
    """Static definition of one candidate expansion bucket (no network)."""

    bucket_id: str
    rationale: str
    expected_role_in_bridge_discovery: str
    openalex_query_filter_strategy: str
    likely_risks_noise_sources: tuple[str, ...]
    # OpenAlex /works query params (filter, search, sort, per-page, cursor, select)
    build_params: Callable[[CorpusPolicy], dict[str, str]]
    uses_search: bool = False


def _base_filters(policy: CorpusPolicy) -> list[str]:
    return [
        f"from_publication_date:{policy.min_year}-01-01",
        f"language:{policy.language}",
        f"type:{'|'.join(policy.include_document_types)}",
        "is_retracted:false",
        "has_abstract:true",
    ]


def _core_source_ids(policy: CorpusPolicy) -> str:
    ids: list[str] = []
    for s in policy.source_policies:
        if s.openalex_source_id:
            ids.append(s.openalex_source_id)
    if not ids:
        raise ValueError("CorpusPolicy has no resolved openalex_source_id on source_policies; cannot build core bucket.")
    return "|".join(ids)


def _params_for_works(
    policy: CorpusPolicy,
    *,
    extra_filters: Sequence[str] | None = None,
    search: str | None = None,
    per_page: int = DEFAULT_PER_PAGE,
    sort: str = "publication_date:desc",
) -> dict[str, str]:
    filters = _base_filters(policy)
    if extra_filters:
        filters.extend(extra_filters)
    params: dict[str, str] = {
        "filter": ",".join(filters),
        "per-page": str(per_page),
        "sort": sort,
        "cursor": "*",
    }
    if search is not None:
        params["search"] = search
    return params


def expansion_bucket_definitions() -> list[CorpusExpansionBucketSpec]:
    """All candidate expansion buckets (id-stable for tests and policy docs)."""
    return [
        CorpusExpansionBucketSpec(
            bucket_id="core_mir_existing_sources",
            rationale=(
                "Extend coverage within venues already in policy (TISMIR, JAES) before adding new sources. "
                "Increases depth in established MIR and audio-ML core neighborhoods."
            ),
            expected_role_in_bridge_discovery=(
                "Refines the stable core and emerging head; gives bridge scoring more between-cluster "
                "contrast if peripheral venues are added later, without first diluting the core."
            ),
            openalex_query_filter_strategy=(
                "Filter: from_publication_date, language, document types, is_retracted:false, has_abstract:true, "
                f"primary_location.source.id: OR of policy core sources (pipe-separated). No search. "
                f"Sort: publication_date:desc. Aligns with existing bootstrap_work_plans in openalex.py."
            ),
            likely_risks_noise_sources=(
                "Papers in-scope for venue but weak MIR relevance if topic gating is relaxed in a future policy.",
                "Duplicate or near-duplicate across venues if DOI dedup is not enforced at ingest.",
            ),
            build_params=lambda p: _params_for_works(
                p,
                extra_filters=[f"primary_location.source.id:{_core_source_ids(p)}"],
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="ismir_proceedings_or_mir_conference",
            rationale=(
                "Conference-proceedings expansion via ISMIR and related venues, where much MIR work is published. "
                "Complements journal-heavy core with timely methods papers."
            ),
            expected_role_in_bridge_discovery=(
                "Adds a distinct *proceedings* mass that often sits between year cohorts and topic clusters, "
                "increasing the chance of boundary-like works that separate emerging from the MIR mainstream."
            ),
            openalex_query_filter_strategy=(
                "Search: short query targeting ISMIR / 'music information retrieval' in title+abstract. "
                "Filter: same base as policy (years, en, types, has_abstract, not retracted). "
                "Optional: cited_by_count floor only if the pool is huge (not applied in default preview)."
            ),
            likely_risks_noise_sources=(
                "Tutorials, panels, and short papers with thin abstracts.",
                "Non-English metadata marked en.",
                "Version noise (preprint + proceedings duplicate) if not merged.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="ISMIR music information retrieval",
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="audio_ml_signal_processing",
            rationale=(
                "Papers in audio representation, separation front-ends, SSL audio, and DSP+learning at signal-processing "
                "venues that still intersect MIR-relevant methods."
            ),
            expected_role_in_bridge_discovery=(
                "Pulls embedding- and metric-space structure toward audio physics and separation; can separate "
                "MIR-pure topic clusters from audio-ML neighborhoods for clearer bridge vs emerging heads."
            ),
            openalex_query_filter_strategy=(
                "Search: 'audio representation' OR 'self-supervised audio' OR 'audio signal' combined in one search string. "
                "Filter: base policy filters; cited_by_count optional (e.g. :>0) to drop uncited noise if needed in ingest."
            ),
            likely_risks_noise_sources=(
                "Generic speech ASR and speaker ID unless exclusion terms in policy are applied at ingest.",
                "Medical/biomed audio if not excluded by policy.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="self-supervised audio music representation",
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="music_recommender_systems",
            rationale=(
                "Music recommendation, playlist modeling, and session-aware learning — adjacent to core MIR and "
                "often well cited; supports user-facing evaluation narratives."
            ),
            expected_role_in_bridge_discovery=(
                "Creates a *systems / ranking* neighborhood distinct from 'audio content' work; strong candidate "
                "for cluster separation between application track and method-centric papers."
            ),
            openalex_query_filter_strategy=(
                "Search: 'music recommendation' OR 'playlist' with base filters. "
                "cited_by_count and publication_year can bound era (e.g. 2016–) via from_publication_date (already) "
                "or extra publication_year: range if the API is used in later tooling."
            ),
            likely_risks_noise_sources=(
                "E-commerce and generic recsys without music focus.",
                "Cold-start papers that are mostly industry narratives.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="music recommendation playlist",
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="cultural_computational_musicology",
            rationale=(
                "Computational musicology, digital humanities, corpora, and style analysis; diversifies the qualitative "
                "and symbolic side of the corpus for cultural bridge discovery."
            ),
            expected_role_in_bridge_discovery=(
                "Adds non-audio-ML but structured-MIR and humanities-adjacent clusters; can separate 'culture/history' "
                "from pure content models — useful for stricter bridge eligibility that rewards cross-cluster position."
            ),
            openalex_query_filter_strategy=(
                "Search: 'computational musicology' OR 'digital musicology' with base filters. "
                "publication_date / publication_year filters already aligned via from_publication_date in base."
            ),
            likely_risks_noise_sources=(
                "Purely historical non-computational work if search is too broad.",
                "Non-English even with language filter misfires.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="computational musicology digital",
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="ethics_law_fairness_user_studies",
            rationale=(
                "MIR and audio-ML with fairness, dataset governance, user studies, or legal/ethical framing — "
                "rare in small corpora but important for review diversity."
            ),
            expected_role_in_bridge_discovery=(
                "May form sparse bridge-like clusters between policy/social and technical MIR; high leverage for "
                "distinct top-k in bridge if eligibility tightens, but can overlap emerging if the pool is too small."
            ),
            openalex_query_filter_strategy=(
                "Search: 'music' AND (fairness OR 'user study' OR dataset) combined in one string. "
                "Filter: base; has_abstract true for human-review quality."
            ),
            likely_risks_noise_sources=(
                "General 'AI ethics' with no music hook.",
                "User studies in speech, not music.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="music fairness user study dataset",
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="symbolic_music_and_harmony",
            rationale=(
                "Symbolically oriented work: harmony, counterpoint, score, MusicXML, pitch spelling — reduces "
                "acoustic-embedding dominance in the corpus."
            ),
            expected_role_in_bridge_discovery=(
                "Pushes one cluster away from 'waveform/embedding' geometry; if separated in embedding space, "
                "improves interpretability of bridge as cross-modal or cross-representation."
            ),
            openalex_query_filter_strategy=(
                "Search: 'music' AND (harmony OR counterpoint OR 'symbolic' OR 'score' OR 'MusicXML') with base filters. "
            ),
            likely_risks_noise_sources=(
                "Computer vision for sheet images without audio relevance if policy topic gate is too loose.",
                "Education or small datasets with minimal novelty.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="music harmony symbolic score",
            ),
        ),
        CorpusExpansionBucketSpec(
            bucket_id="source_separation_benchmarks",
            rationale=(
                "Source separation, demixing, and benchmark papers (MUSDB, SISEC-style) to anchor a separation-heavy "
                "neighborhood and evaluation references."
            ),
            expected_role_in_bridge_discovery=(
                "Tends to sit between classic SP and learning-based MIR; useful for disambiguating 'emerging' "
                "separation from generic emerging methods if the cluster count is high enough."
            ),
            openalex_query_filter_strategy=(
                "Search: 'source separation' AND music OR benchmark. "
                "Filter: base; optional cited_by_count:>5 in a stricter pass to emphasize established baselines. "
                "For this preview, use search + base only."
            ),
            likely_risks_noise_sources=(
                "Speech-only separation unless exclusion policy catches it in text.",
                "Dereverberation without musical context.",
            ),
            uses_search=True,
            build_params=lambda p: _params_for_works(
                p,
                search="music source separation benchmark",
            ),
        ),
    ]


def _works_list_url(params: dict[str, str], select: Sequence[str] | None) -> str:
    q = dict(params)
    q["select"] = ",".join(select or DEFAULT_SELECT_FIELDS)
    return f"{OPENALEX_WORKS_URL}?{urlencode(q)}"


def work_to_sample_row(
    work: Mapping[str, Any], *, has_extra_abstract: bool
) -> dict[str, Any]:
    pl = work.get("primary_location")
    source_name: str | None
    if isinstance(pl, Mapping):
        src = pl.get("source")
        if isinstance(src, Mapping):
            source_name = str(src.get("display_name") or "")
        else:
            source_name = None
    else:
        source_name = None
    if not str(source_name or "").strip():
        source_name = None
    abstract_text = abstract_plain_text(work) if has_extra_abstract else ""
    abstract_present = bool(work.get("has_abstract")) or bool(abstract_text.strip())
    return {
        "openalex_id": str(work.get("id") or ""),
        "title": work.get("title"),
        "year": work.get("publication_year"),
        "citation_count": int(work.get("cited_by_count") or 0),
        "source_display_name": source_name,
        "abstract_present": abstract_present,
    }


def _mock_fetch_zero(_url: str) -> dict[str, Any]:
    return {"meta": {"count": 0}, "results": []}


def run_corpus_expansion_preview(
    *,
    policy: CorpusPolicy,
    mailto: str,
    per_bucket_sample: int = 20,
    fetch: PreviewFetch | None = None,
    openalex_mode: str = "live",
    include_abstract_in_select: bool = True,
) -> dict[str, Any]:
    """
    OpenAlex read-only preview: one small request per bucket, no database.

    `fetch` defaults to `fetch_openalex_json` from the pipeline client (mailto + retries).
    `openalex_mode` is "live" | "mock" for documentation and tests.
    """
    if not (10 <= per_bucket_sample <= 25):
        raise ValueError("per_bucket_sample must be between 10 and 25")
    if fetch is not None:
        fetch_fn = fetch
    elif openalex_mode == "mock":
        fetch_fn = _mock_fetch_zero
    else:
        fetch_fn = lambda u: fetch_openalex_json(u, mailto=mailto, timeout_sec=60.0)
    if include_abstract_in_select:
        select: tuple[str, ...] = DEFAULT_SELECT_FIELDS
    else:
        select = tuple(f for f in DEFAULT_SELECT_FIELDS if f != "abstract_inverted_index")
    buckets: list[dict[str, Any]] = []
    definitions = expansion_bucket_definitions()

    for spec in definitions:
        params = spec.build_params(policy)
        # Small preview page
        small = dict(params)
        small["per-page"] = str(per_bucket_sample)
        url = _works_list_url(small, select)
        payload = fetch_fn(url)
        meta = payload.get("meta") or {}
        count: int | None
        if isinstance(meta.get("count"), int):
            count = int(meta["count"])
        else:
            count = None
        results = list(payload.get("results") or [])
        sample: list[dict[str, Any]] = []
        for w in results[:per_bucket_sample]:
            if not isinstance(w, Mapping):
                continue
            sample.append(
                work_to_sample_row(
                    w,
                    has_extra_abstract=include_abstract_in_select and "abstract_inverted_index" in select,
                )
            )
        bucket_out = {
            "bucket_id": spec.bucket_id,
            "rationale": spec.rationale,
            "expected_role_in_bridge_discovery": spec.expected_role_in_bridge_discovery,
            "openalex_query_filter_strategy": spec.openalex_query_filter_strategy,
            "openalex_uses_search_parameter": spec.uses_search,
            "estimated_candidate_count": count,
            "sample_works": sample,
            "likely_risks_noise_sources": list(spec.likely_risks_noise_sources),
            "api_request": {
                "works_url": url,
                "mailto_user_agent": mailto,
                "uses_openalex_search_parameter": spec.uses_search,
            },
        }
        for k in REQUIRED_BUCKET_KEYS:
            if k not in bucket_out:
                raise RuntimeError(f"internal: missing bucket key {k!r}")
        buckets.append(bucket_out)

    recommendation = {
        "keep_current_corpus_for_smoke_only": True,
        "expand_before_next_bridge_weight_experiment": True,
        "suggested_target_corpus_size_range": [200, 500],
        "recommended_first_pass_bucket_ids": [
            "core_mir_existing_sources",
            "ismir_proceedings_or_mir_conference",
            "audio_ml_signal_processing",
            "music_recommender_systems",
        ],
        "buckets_likely_defer_due_to_noise": [
            "ethics_law_fairness_user_studies",
            "cultural_computational_musicology",
        ],
        "stricter_bridge_eligibility_recommended_for_next_zero_weight_run": True,
    }

    versioning = {
        "new_corpus_snapshot_version": "Ingesting an expanded source set or policy will produce a new source_snapshot_version and corpus identity for downstream steps.",
        "repaired_work_text_artifact": "If text repair runs, corrected title/abstract text is tied to the snapshot; label embedding inputs accordingly.",
        "new_embedding_version": "New embedding_version label after text change or model change; required before clustering and ranking on the new pool.",
        "new_cluster_version": "New cluster_version after re-embed; bridge_score depends on cluster boundary definitions.",
        "new_zero_bridge_ranking_version": "A zero–bridge-weight ranking run should be the baseline before any bridge weight experiment on the new pool.",
        "fresh_review_worksheets_and_summaries": "Manual review CSVs, family summaries, and rollups are invalid across corpus pools; regenerate after a new snapshot.",
    }

    caveats = [
        "The current ~59-work corpus is suitable for smoke, plumbing, and demo evidence only, not for strong ML-quality generalization claims.",
        "Expanding the corpus changes candidate pools and label distributions; do not treat metrics from the old 59-work pool and a new pool as same-pool performance.",
        "A serious expansion is followed (in order) by text repair, embedding coverage checks, clustering, a zero–bridge-weight ranking run, bridge diagnostics, and fresh labels before comparing bridge head distinctness.",
        "This preview is not an ingest or policy decision by itself; apply topic gates, exclusions, and source policy from CorpusPolicy in code before committing a snapshot.",
        "Preview counts and samples are from OpenAlex list endpoints; deduplication, retraction, and final inclusion rules may reduce realized corpus size.",
    ]

    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "mailto": mailto,
        "policy_reference": {
            "name": policy.name,
            "policy_hash": policy.policy_hash,
            "min_year": policy.min_year,
            "not_mutated": True,
        },
        "buckets": buckets,
        "recommendation": recommendation,
        "versioning_implications": versioning,
        "caveats": caveats,
        "openalex_mode": openalex_mode,
        "per_bucket_sample_size": per_bucket_sample,
    }


def _markdown_escape(s: str) -> str:
    return s.replace("|", r"\|")


def render_corpus_expansion_markdown(preview: Mapping[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Corpus expansion preview")
    lines.append("")
    lines.append(f"Generated: `{preview.get('generated_at')}`")
    lines.append(f"OpenAlex mode: **{preview.get('openalex_mode')}**  ")
    lines.append(
        f"**Suggested target size (works):** {preview.get('recommendation', {}).get('suggested_target_corpus_size_range', [])}  "
    )
    if isinstance(preview.get("recommendation"), Mapping):
        rec = preview["recommendation"]
        if rec.get("expand_before_next_bridge_weight_experiment") is not None:
            e = rec.get("expand_before_next_bridge_weight_experiment")
            lines.append(f"**expand_before_next_bridge_weight_experiment:** {e}")
        if rec.get("keep_current_corpus_for_smoke_only") is not None:
            lines.append(f"**keep_current_corpus_for_smoke_only:** {rec.get('keep_current_corpus_for_smoke_only')}")
    lines.append("")

    lines.append("## Recommendation (non-binding)")
    lines.append(
        f"- First-pass bucket ids: {preview.get('recommendation', {}).get('recommended_first_pass_bucket_ids', [])}"
    )
    lines.append(
        f"- Defer (noise-prone) bucket ids: {preview.get('recommendation', {}).get('buckets_likely_defer_due_to_noise', [])}"
    )
    lines.append(
        "- Stricter bridge eligibility on next zero-weight run: **"
        + str(
            preview.get("recommendation", {}).get("stricter_bridge_eligibility_recommended_for_next_zero_weight_run")
        )
        + "**"
    )
    lines.append("")

    lines.append("## Versioning implications")
    vi = preview.get("versioning_implications") or {}
    if isinstance(vi, dict):
        for k, v in vi.items():
            lines.append(f"- **{k}:** {v}")
    lines.append("")

    lines.append("## Caveats")
    for c in list(preview.get("caveats") or ()):
        lines.append(f"- {_markdown_escape(str(c))}")
    lines.append("")
    lines.append(
        "This document is a planning note only. It is **not** a scientific validation, benchmark result, or ingest commitment."
    )
    lines.append("")

    lines.append("## Buckets (summary)")
    for b in list(preview.get("buckets") or []):
        if not isinstance(b, Mapping):
            continue
        bid = b.get("bucket_id", "")
        n = b.get("estimated_candidate_count")
        lines.append(f"### {bid}")
        lines.append(f"- **Estimated count (OpenAlex list meta):** {n if n is not None else 'n/a'}")
        n_sample = len(list(b.get("sample_works") or []))
        lines.append(f"- **Sample size this run:** {n_sample} works (preview only)")
    lines.append("")
    return "\n".join(lines)


def resolve_corpus_expansion_preview_mailto(*, mailto: str, mock_openalex: bool) -> str:
    """
    User-Agent contact for OpenAlex. Live mode requires a real address via --mailto or
    OPENALEX_MAILTO; mock mode may use a placeholder if neither is set.
    """
    cli = (mailto or "").strip()
    env = (os.environ.get("OPENALEX_MAILTO") or "").strip()
    if mock_openalex:
        return cli or env or "research-radar-dev@local.invalid"
    if not cli and not env:
        raise ValueError("live_corpus_expansion_preview_requires_mailto")
    return cli or env


def write_corpus_expansion_artifacts(
    preview: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path | None,
) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(preview, f, indent=2, ensure_ascii=False)
        f.write("\n")
    if markdown_path is not None:
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        text = render_corpus_expansion_markdown(preview)
        markdown_path.write_text(text, encoding="utf-8")


def run_corpus_expansion_preview_from_cli(
    *,
    output: Path,
    markdown_output: Path,
    mailto: str,
    per_bucket_sample: int,
    mock_openalex: bool,
) -> None:
    policy = CorpusPolicy()
    mode = "mock" if mock_openalex else "live"
    try:
        m = resolve_corpus_expansion_preview_mailto(mailto=mailto, mock_openalex=mock_openalex)
    except ValueError:
        print(
            "corpus-expansion-preview: live mode requires a real contact: pass --mailto or set "
            "OPENALEX_MAILTO in the environment (identifies you to OpenAlex in the User-Agent).",
            file=sys.stderr,
        )
        raise SystemExit(2) from None
    out = run_corpus_expansion_preview(
        policy=policy,
        mailto=m,
        per_bucket_sample=per_bucket_sample,
        openalex_mode=mode,
    )
    for k in REQUIRED_PREVIEW_TOP_LEVEL_KEYS:
        if k not in out:
            raise RuntimeError(f"internal: missing preview key {k!r}")
    write_corpus_expansion_artifacts(out, json_path=output, markdown_path=markdown_output)

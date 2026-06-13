"""ISMIR expansion ingest preview: OpenAlex-only dry run, no Postgres.

Answers, before any ingest, how an ISMIR tranche would actually be sourced:

- works found under the dedicated OpenAlex ISMIR source id (coverage is
  known to be partial),
- works found by a title/abstract search for the acronym,
- the overlap between the two,
- how many pass the deterministic corpus-v2 filters,
- which search hits can be *attributed* to the ISMIR venue through any
  location's source or raw source name (many ISMIR papers are indexed under
  Zenodo/arXiv repository sources), and which are unattributed MIR-context
  noise.

The recommended approved set (attributed + passing filters) is emitted in the
same artifact shape `corpus-v2-ingest-from-plan` consumes, so approving the
plan does not require new ingest code.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from pipeline.corpus_expansion_preview import (
    _base_filters,  # noqa: SLF001
    _works_list_url,  # noqa: SLF001
    resolve_corpus_expansion_preview_mailto,
)
from pipeline.corpus_v2_candidate_plan import (
    FETCH_PAGE_SIZE,
    _DedupTracker,  # noqa: SLF001
    _work_to_candidate_row,  # noqa: SLF001
    evaluate_v2_candidate,
)
from pipeline.openalex import DEFAULT_SELECT_FIELDS
from pipeline.openalex_client import (
    compute_contact_provenance,
    compute_openalex_auth_artifact_fields,
    fetch_openalex_json,
    openalex_api_key_from_env,
)
from pipeline.policy import CorpusPolicy

PreviewFetch = Callable[[str], Mapping[str, Any]]

ISMIR_SLUG = "ismir"
SEARCH_QUERY = "ISMIR"
# Reuse the deterministic corpus-v2 filter rules for this venue family.
EVAL_BUCKET_ID = "ismir_proceedings_or_mir_conference"
SOURCE_BUCKET_ID = "ismir_source_id"
SEARCH_BUCKET_ID = "ismir_title_abstract_search"
SELECT_FIELDS: tuple[str, ...] = DEFAULT_SELECT_FIELDS + ("locations",)
MAX_REJECTED_EXAMPLES = 10
MAX_UNATTRIBUTED_EXAMPLES = 10


class IsmirIngestPreviewError(RuntimeError):
    pass


def _ismir_source_policy(policy: CorpusPolicy):
    source = policy.source_for_slug(ISMIR_SLUG)
    if source is None or not source.openalex_source_id:
        raise IsmirIngestPreviewError(
            "CorpusPolicy has no resolved 'ismir' source policy; add it before running this preview"
        )
    return source


def _norm(name: Any) -> str:
    return " ".join(str(name or "").split()).casefold()


def ismir_name_match(name: Any) -> bool:
    """Venue-name attribution for ISMIR proceedings indexed under other sources.

    Matches ordinal variants like 'Proceedings of the 24th International
    Society for Music Information Retrieval Conference' while rejecting the
    TISMIR journal ('Transactions of ...').
    """
    n = _norm(name)
    if not n:
        return False
    if "transactions" in n:
        return False
    if n == "ismir" or n.startswith("ismir ") or " ismir" in f" {n}":
        return True
    if "music information retrieval" in n and any(
        token in n for token in ("society", "symposium", "conference")
    ):
        return True
    return False


def _location_names(work: Mapping[str, Any]) -> list[tuple[str, Any]]:
    """(field, value) pairs of every source/raw name across all locations."""
    out: list[tuple[str, Any]] = []
    locations: list[Any] = []
    pl = work.get("primary_location")
    if isinstance(pl, Mapping):
        locations.append(pl)
    extra = work.get("locations")
    if isinstance(extra, list):
        locations.extend(loc for loc in extra if isinstance(loc, Mapping))
    for loc in locations:
        source = loc.get("source")
        if isinstance(source, Mapping):
            out.append(("source.display_name", source.get("display_name")))
        out.append(("raw_source_name", loc.get("raw_source_name")))
    return out


def attribute_ismir(work: Mapping[str, Any], *, ismir_source_id: str) -> str | None:
    """Return 'source_id', 'location_name', or None."""
    pl = work.get("primary_location")
    sources: list[Any] = []
    if isinstance(pl, Mapping) and isinstance(pl.get("source"), Mapping):
        sources.append(pl["source"].get("id"))
    extra = work.get("locations")
    if isinstance(extra, list):
        for loc in extra:
            if isinstance(loc, Mapping) and isinstance(loc.get("source"), Mapping):
                sources.append(loc["source"].get("id"))
    if any(str(s or "") == ismir_source_id for s in sources):
        return "source_id"
    if any(ismir_name_match(value) for _field, value in _location_names(work)):
        return "location_name"
    return None


def _fetch_bucket(
    fetch_fn: PreviewFetch,
    *,
    params: dict[str, str],
    max_works: int,
) -> tuple[int | None, list[Mapping[str, Any]]]:
    works: list[Mapping[str, Any]] = []
    meta_count: int | None = None
    cursor = "*"
    page_params = dict(params)
    page_params["per-page"] = str(min(FETCH_PAGE_SIZE, max_works))
    while len(works) < max_works:
        p = dict(page_params)
        p["cursor"] = cursor
        payload = fetch_fn(_works_list_url(p, SELECT_FIELDS))
        meta = payload.get("meta") or {}
        if meta_count is None and isinstance(meta.get("count"), int):
            meta_count = int(meta["count"])
        results = list(payload.get("results") or [])
        for work in results:
            if isinstance(work, Mapping) and len(works) < max_works:
                works.append(work)
        next_cursor = meta.get("next_cursor")
        if not results or not next_cursor:
            break
        cursor = str(next_cursor)
    return meta_count, works


def run_ismir_ingest_preview(
    *,
    policy: CorpusPolicy,
    mailto: str,
    contact_mode: str,
    contact_provided: bool,
    max_works_per_bucket: int = 400,
    target_min: int = 1,
    target_max: int = 500,
    fetch: PreviewFetch | None = None,
    mock_openalex: bool = False,
) -> dict[str, Any]:
    if max_works_per_bucket < 1 or max_works_per_bucket > 2000:
        raise ValueError("max_works_per_bucket must be between 1 and 2000")
    if target_min < 1 or target_max < target_min:
        raise ValueError("invalid target_min/target_max")
    ismir = _ismir_source_policy(policy)
    ismir_source_id = str(ismir.openalex_source_id)

    if fetch is not None:
        fetch_fn = fetch
    elif mock_openalex:
        fetch_fn = lambda _u: {"meta": {"count": 0, "next_cursor": None}, "results": []}
    else:
        fetch_fn = lambda u: fetch_openalex_json(u, mailto=mailto, timeout_sec=90.0)

    base = _base_filters(policy)
    source_params = {
        "filter": ",".join([*base, f"primary_location.source.id:{ismir_source_id}"]),
        "sort": "publication_date:desc",
        "cursor": "*",
    }
    search_params = {
        "filter": ",".join(base),
        "search": SEARCH_QUERY,
        "sort": "publication_date:desc",
        "cursor": "*",
    }

    source_count, source_works = _fetch_bucket(
        fetch_fn, params=source_params, max_works=max_works_per_bucket
    )
    search_count, search_works = _fetch_bucket(
        fetch_fn, params=search_params, max_works=max_works_per_bucket
    )

    source_ids = {str(w.get("id") or "") for w in source_works} - {""}
    search_ids = {str(w.get("id") or "") for w in search_works} - {""}
    overlap_ids = source_ids & search_ids

    dedup = _DedupTracker()
    selected: list[dict[str, Any]] = []
    bucket_summaries: list[dict[str, Any]] = []
    rejected_examples: dict[str, list[dict[str, Any]]] = {
        SOURCE_BUCKET_ID: [],
        SEARCH_BUCKET_ID: [],
    }
    attribution = {
        "by_source_id": 0,
        "by_location_name": 0,
        "unattributed_passing_filters": 0,
    }
    unattributed_examples: list[dict[str, Any]] = []

    for bucket_id, works, meta_count in (
        (SOURCE_BUCKET_ID, source_works, source_count),
        (SEARCH_BUCKET_ID, search_works, search_count),
    ):
        passed_filter = 0
        attributed_count = 0
        selected_count = 0
        for work in works:
            ev = evaluate_v2_candidate(work, policy=policy, bucket_id=EVAL_BUCKET_ID)
            if not ev["included"]:
                if len(rejected_examples[bucket_id]) < MAX_REJECTED_EXAMPLES:
                    rejected_examples[bucket_id].append(
                        {
                            "openalex_id": str(work.get("id") or ""),
                            "title": work.get("title"),
                            "exclusion_reason": ev["exclusion_reason"],
                        }
                    )
                continue
            passed_filter += 1
            attributed = attribute_ismir(work, ismir_source_id=ismir_source_id)
            if attributed is None:
                attribution["unattributed_passing_filters"] += 1
                if len(unattributed_examples) < MAX_UNATTRIBUTED_EXAMPLES:
                    unattributed_examples.append(
                        {
                            "openalex_id": str(work.get("id") or ""),
                            "title": work.get("title"),
                            "source_display_name": next(
                                (str(v) for _f, v in _location_names(work) if v),
                                None,
                            ),
                        }
                    )
                continue
            attributed_count += 1
            if len(selected) >= target_max or not dedup.try_add(work):
                continue
            attribution[f"by_{attributed}"] += 1
            selected.append(_work_to_candidate_row(work, bucket_id=bucket_id, ev=ev))
            selected_count += 1
        bucket_summaries.append(
            {
                "bucket_id": bucket_id,
                "estimated_count_from_meta": meta_count,
                "raw_candidates_fetched": len(works),
                "passed_filter_count": passed_filter,
                "ismir_attributed_count": attributed_count,
                "selected_count_after_dedup": selected_count,
            }
        )

    selected_total = len(selected)
    caveats = [
        "Dry-run only: no Postgres writes, no snapshot, no embeddings, clustering, or ranking.",
        "Candidate list is not a benchmark and does not validate retrieval or bridge quality.",
        "The approved ingest set is works attributed to the ISMIR venue (source id or location name) that pass "
        "the deterministic corpus-v2 filters; unattributed search hits are reported but excluded.",
    ]
    if selected_total < target_min:
        caveats.append(
            f"selected_total ({selected_total}) is below target_min ({target_min}); "
            "widen max_works_per_bucket or revisit attribution before ingest."
        )

    api_key_provided, auth_mode = compute_openalex_auth_artifact_fields(mock_openalex=mock_openalex)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "contact_provided": contact_provided,
        "contact_mode": contact_mode,
        "api_key_provided": api_key_provided,
        "auth_mode": auth_mode,
        "policy_reference": {
            "name": policy.name,
            "policy_hash": policy.policy_hash,
            "ismir_openalex_source_id": ismir_source_id,
        },
        "target_min": target_min,
        "target_max": target_max,
        "max_works_per_bucket": max_works_per_bucket,
        "selected_total": selected_total,
        "bucket_summaries": bucket_summaries,
        "bucket_overlap": {
            "in_both_buckets": len(overlap_ids),
            "source_id_only": len(source_ids - search_ids),
            "search_only": len(search_ids - source_ids),
        },
        "attribution_statistics": attribution,
        "unattributed_passing_examples": unattributed_examples,
        "selected_candidates": selected,
        "rejected_noise_examples_by_bucket": rejected_examples,
        "dedup_statistics": {
            "drops_by_openalex_id": dedup.drops_by_oa,
            "drops_by_doi": dedup.drops_by_doi,
            "drops_by_normalized_title": dedup.drops_by_title,
            "unique_openalex_ids_kept": len(dedup.seen_oa),
        },
        "recommended_ingest_scope": (
            "Approve this artifact (or a reviewed subset) as the ISMIR expansion tranche and import it with "
            "corpus-v2-ingest-from-plan under a new source_snapshot_version, then hydrate, re-embed, re-cluster, "
            "and run a zero-bridge-weight ranking before any bridge-weight comparison."
        ),
        "versioning_implications": {
            "new_corpus_snapshot_version": "required_before_ingest",
            "new_embedding_version": "required_after_snapshot",
            "new_cluster_version": "required_after_embeddings",
            "new_zero_bridge_ranking_version": "required_before_bridge_weight_experiments",
        },
        "caveats": caveats,
    }


def render_ismir_ingest_preview_markdown(plan: Mapping[str, Any]) -> str:
    lines = [
        "# ISMIR expansion ingest preview (dry-run)",
        "",
        "Planning output only: **no database writes**, no benchmark or retrieval-quality claims.",
        "",
        "## Totals",
        "",
        f"- **selected_total (approved ingest set):** `{plan.get('selected_total')}`",
        f"- **contact_mode:** `{plan.get('contact_mode')}` (raw mailto is not stored)",
        f"- **auth_mode:** `{plan.get('auth_mode')}` · **api_key_provided:** `{plan.get('api_key_provided')}`",
        "",
        "## Buckets",
        "",
    ]
    for b in plan.get("bucket_summaries") or []:
        lines.append(
            f"- **{b.get('bucket_id')}:** OpenAlex meta count `{b.get('estimated_count_from_meta')}`, "
            f"fetched `{b.get('raw_candidates_fetched')}`, passed filters `{b.get('passed_filter_count')}`, "
            f"ISMIR-attributed `{b.get('ismir_attributed_count')}`, selected `{b.get('selected_count_after_dedup')}`"
        )
    overlap = plan.get("bucket_overlap") or {}
    lines.extend(
        [
            "",
            "## Overlap (raw fetched ids)",
            "",
            f"- in both buckets: `{overlap.get('in_both_buckets')}`",
            f"- source-id bucket only: `{overlap.get('source_id_only')}`",
            f"- search bucket only: `{overlap.get('search_only')}`",
            "",
            "## Attribution of filter-passing works",
            "",
        ]
    )
    attr = plan.get("attribution_statistics") or {}
    lines.append(f"- attributed via dedicated source id: `{attr.get('by_source_id')}`")
    lines.append(f"- attributed via location/raw source name: `{attr.get('by_location_name')}`")
    lines.append(
        f"- passing filters but **not** attributable to ISMIR (excluded): "
        f"`{attr.get('unattributed_passing_filters')}`"
    )
    lines.extend(["", "### Unattributed examples (excluded from approved set)", ""])
    unattributed = list(plan.get("unattributed_passing_examples") or [])
    if unattributed:
        for ex in unattributed:
            lines.append(
                f"- `{ex.get('openalex_id')}` — {ex.get('title')!r} (source: {ex.get('source_display_name')})"
            )
    else:
        lines.append("- None")
    lines.extend(["", "## Rejected examples (filter false positives)", ""])
    for bucket_id, examples in (plan.get("rejected_noise_examples_by_bucket") or {}).items():
        if not examples:
            continue
        lines.append(f"### {bucket_id}")
        for ex in examples:
            lines.append(
                f"- `{ex.get('openalex_id')}` — {ex.get('title')!r} — **{ex.get('exclusion_reason')}**"
            )
        lines.append("")
    lines.extend(["## Recommended ingest scope", "", str(plan.get("recommended_ingest_scope") or ""), ""])
    lines.extend(["## Caveats", ""])
    for caveat in plan.get("caveats") or []:
        lines.append(f"- {caveat}")
    return "\n".join(lines).rstrip() + "\n"


def run_ismir_ingest_preview_from_cli(
    *,
    output: Path,
    markdown_output: Path,
    mailto: str,
    max_works_per_bucket: int,
    target_min: int,
    target_max: int,
    mock_openalex: bool,
) -> None:
    policy = CorpusPolicy()
    mailto_raw = (mailto or "").strip()
    has_env_mailto = bool((os.environ.get("OPENALEX_MAILTO") or "").strip())
    if not mock_openalex and not mailto_raw and not has_env_mailto and not openalex_api_key_from_env():
        print(
            "ismir-ingest-preview: live mode requires OPENALEX_API_KEY and/or contact: "
            "set OPENALEX_API_KEY, or pass --mailto, or set OPENALEX_MAILTO",
            file=sys.stderr,
        )
        raise SystemExit(2)
    contact_mode, contact_provided = compute_contact_provenance(
        mailto_cli=mailto or "", mock_openalex=mock_openalex
    )
    resolved_mailto = resolve_corpus_expansion_preview_mailto(mailto=mailto, mock_openalex=mock_openalex)
    plan = run_ismir_ingest_preview(
        policy=policy,
        mailto=resolved_mailto,
        contact_mode=contact_mode,
        contact_provided=contact_provided,
        max_works_per_bucket=max_works_per_bucket,
        target_min=target_min,
        target_max=target_max,
        mock_openalex=mock_openalex,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(render_ismir_ingest_preview_markdown(plan), encoding="utf-8", newline="\n")


__all__ = [
    "attribute_ismir",
    "ismir_name_match",
    "render_ismir_ingest_preview_markdown",
    "run_ismir_ingest_preview",
    "run_ismir_ingest_preview_from_cli",
]

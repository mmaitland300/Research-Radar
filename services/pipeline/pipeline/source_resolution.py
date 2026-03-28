from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlencode

from pipeline.openalex import OPENALEX_SOURCES_URL
from pipeline.openalex_client import fetch_openalex_json
from pipeline.policy import CorpusPolicy, SourcePolicy


def _norm_name(s: str) -> str:
    return " ".join(str(s).split()).casefold()


class SourceResolutionError(RuntimeError):
    """Raised when an OpenAlex source cannot be matched authoritatively to policy."""


@dataclass(frozen=True)
class SourceResolutionOutcome:
    source_slug: str
    openalex_source_id: str
    matched_display_name: str
    search_query: str


def resolve_source_openalex_id(source: SourcePolicy, *, mailto: str | None) -> SourceResolutionOutcome:
    """
    Resolve a single venue to a canonical OpenAlex source id using /sources search
    and exact normalized display_name match against policy names (display_name + aliases).
    """
    if source.openalex_source_id:
        return SourceResolutionOutcome(
            source_slug=source.slug,
            openalex_source_id=source.openalex_source_id,
            matched_display_name=source.display_name,
            search_query="",
        )

    allowed = {_norm_name(n) for n in source.names_for_resolution()}
    last_results: list[Mapping[str, Any]] = []

    for candidate in source.names_for_resolution():
        params = {"search": candidate, "per-page": "25"}
        url = f"{OPENALEX_SOURCES_URL}?{urlencode(params)}"
        data = fetch_openalex_json(url, mailto=mailto)
        results = data.get("results") if isinstance(data, Mapping) else None
        if not isinstance(results, list):
            results = []
        last_results = [r for r in results if isinstance(r, Mapping)]

        for row in last_results:
            dn = row.get("display_name")
            rid = row.get("id")
            if not isinstance(dn, str) or not rid:
                continue
            if _norm_name(dn) in allowed:
                return SourceResolutionOutcome(
                    source_slug=source.slug,
                    openalex_source_id=str(rid),
                    matched_display_name=dn,
                    search_query=candidate,
                )

    preview = [
        (r.get("id"), r.get("display_name"))
        for r in last_results[:8]
        if isinstance(r, Mapping)
    ]
    raise SourceResolutionError(
        f"No authoritative OpenAlex source match for slug={source.slug!r}; "
        f"allowed policy names={sorted(allowed)!r}; last search preview={preview!r}"
    )


def resolve_all_sources(policy: CorpusPolicy, *, mailto: str | None) -> tuple[SourceResolutionOutcome, ...]:
    return tuple(resolve_source_openalex_id(s, mailto=mailto) for s in policy.source_policies)


def slug_to_openalex_id_map(outcomes: tuple[SourceResolutionOutcome, ...]) -> dict[str, str]:
    return {o.source_slug: o.openalex_source_id for o in outcomes}

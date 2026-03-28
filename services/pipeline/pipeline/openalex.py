from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from urllib.parse import urlencode

from pipeline.policy import CorpusPolicy, SourcePolicy


OPENALEX_SOURCES_URL = "https://api.openalex.org/sources"
OPENALEX_WORKS_URL = "https://api.openalex.org/works"
DEFAULT_PER_PAGE = 100
# `has_abstract` must not appear here: OpenAlex returns HTTP 400 ("not a valid select field").
# Abstract presence is enforced via filter `has_abstract:true` plus `abstract_inverted_index` below.
DEFAULT_SELECT_FIELDS = (
    "id",
    "doi",
    "title",
    "type",
    "language",
    "publication_year",
    "publication_date",
    "cited_by_count",
    "is_retracted",
    "primary_location",
    "authorships",
    "topics",
    "referenced_works",
    "updated_date",
    "abstract_inverted_index",
)


@dataclass(frozen=True)
class SourceResolutionPlan:
    source_slug: str
    candidate_name: str
    url: str


@dataclass(frozen=True)
class WorksPagePlan:
    source_slug: str
    source_display_name: str
    params: dict[str, str]
    select_fields: tuple[str, ...] = DEFAULT_SELECT_FIELDS

    def url(self) -> str:
        query = dict(self.params)
        query["select"] = ",".join(self.select_fields)
        return f"{OPENALEX_WORKS_URL}?{urlencode(query)}"

    def next_page(self, cursor: str) -> "WorksPagePlan":
        next_params = dict(self.params)
        next_params["cursor"] = cursor
        return WorksPagePlan(
            source_slug=self.source_slug,
            source_display_name=self.source_display_name,
            params=next_params,
            select_fields=self.select_fields,
        )


def build_source_resolution_plans(policy: CorpusPolicy) -> list[SourceResolutionPlan]:
    plans: list[SourceResolutionPlan] = []
    for source in policy.source_policies:
        if source.openalex_source_id:
            continue
        for candidate in source.names_for_resolution():
            params = {"search": candidate, "per-page": "25"}
            plans.append(
                SourceResolutionPlan(
                    source_slug=source.slug,
                    candidate_name=candidate,
                    url=f"{OPENALEX_SOURCES_URL}?{urlencode(params)}",
                )
            )
    return plans


def build_bootstrap_work_plans(policy: CorpusPolicy) -> list[WorksPagePlan]:
    return [build_source_work_plan(policy, source) for source in policy.source_policies]


def build_source_work_plan(policy: CorpusPolicy, source: SourcePolicy) -> WorksPagePlan:
    filters = [
        f"from_publication_date:{policy.min_year}-01-01",
        f"language:{policy.language}",
        f"type:{'|'.join(policy.include_document_types)}",
        "is_retracted:false",
        "has_abstract:true",
    ]
    if not source.openalex_source_id:
        raise ValueError(
            f"Source {source.slug!r} has no canonical openalex_source_id; "
            "resolve sources (API + DB) before building work plans."
        )
    filters.append(f"primary_location.source.id:{source.openalex_source_id}")

    params = {
        "filter": ",".join(filters),
        "cursor": "*",
        "per-page": str(DEFAULT_PER_PAGE),
        "sort": "publication_date:desc",
    }
    return WorksPagePlan(
        source_slug=source.slug,
        source_display_name=source.display_name,
        params=params,
    )


def build_work_select_clause(extra_fields: Iterable[str] = ()) -> tuple[str, ...]:
    merged = list(DEFAULT_SELECT_FIELDS)
    for field in extra_fields:
        if field not in merged:
            merged.append(field)
    return tuple(merged)

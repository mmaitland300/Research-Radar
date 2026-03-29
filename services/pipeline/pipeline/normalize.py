from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from pipeline.openalex_text import abstract_plain_text, clean_openalex_text
from pipeline.policy import CorpusPolicy, PolicyDecision


@dataclass(frozen=True)
class NormalizedWork:
    openalex_id: str
    title: str
    abstract: str | None
    year: int
    doi: str | None
    work_type: str
    language: str
    publication_date: str | None
    updated_date: str | None
    citation_count: int
    source_openalex_id: str | None
    source_display_name: str | None
    inclusion_status: str
    exclusion_reason: str | None
    is_core_corpus: bool
    matched_keywords: tuple[str, ...]


@dataclass(frozen=True)
class AuthorLink:
    author_openalex_id: str
    display_name: str
    author_position: int


@dataclass(frozen=True)
class TopicLink:
    topic_openalex_id: str
    display_name: str
    level: int
    score: float


@dataclass(frozen=True)
class CitationLink:
    cited_openalex_id: str


@dataclass(frozen=True)
class HydratedWorkRecord:
    work: NormalizedWork
    policy_decision: PolicyDecision
    authors: tuple[AuthorLink, ...]
    topics: tuple[TopicLink, ...]
    citations: tuple[CitationLink, ...]


def hydrate_work_record(work: Mapping[str, Any], policy: CorpusPolicy) -> HydratedWorkRecord:
    decision = policy.evaluate_work(work)
    source = work.get("primary_location", {}).get("source", {}) if isinstance(work.get("primary_location"), Mapping) else {}
    normalized = NormalizedWork(
        openalex_id=str(work.get("id") or ""),
        title=clean_openalex_text(str(work.get("title") or "Untitled work")),
        abstract=_abstract_text(work),
        year=int(work.get("publication_year") or 0),
        doi=work.get("doi"),
        work_type=str(work.get("type") or ""),
        language=str(work.get("language") or ""),
        publication_date=work.get("publication_date"),
        updated_date=work.get("updated_date"),
        citation_count=int(work.get("cited_by_count") or 0),
        source_openalex_id=source.get("id") if isinstance(source, Mapping) else None,
        source_display_name=clean_openalex_text(source.get("display_name")) if isinstance(source, Mapping) else None,
        inclusion_status="included" if decision.included else "excluded",
        exclusion_reason=None if decision.included else decision.reason,
        is_core_corpus=decision.is_core_corpus,
        matched_keywords=decision.matched_keywords,
    )
    return HydratedWorkRecord(
        work=normalized,
        policy_decision=decision,
        authors=extract_authors(work),
        topics=extract_topics(work),
        citations=extract_citations(work),
    )


def extract_authors(work: Mapping[str, Any]) -> tuple[AuthorLink, ...]:
    authorships = work.get("authorships")
    if not isinstance(authorships, list):
        return ()

    author_links: list[AuthorLink] = []
    for position, authorship in enumerate(authorships, start=1):
        if not isinstance(authorship, Mapping):
            continue
        author = authorship.get("author")
        if not isinstance(author, Mapping):
            continue
        author_id = author.get("id")
        display_name = author.get("display_name")
        if not author_id or not display_name:
            continue
        author_links.append(
            AuthorLink(
                author_openalex_id=str(author_id),
                display_name=clean_openalex_text(str(display_name)),
                author_position=position,
            )
        )
    return tuple(author_links)


def _topic_level_from_payload(topic: Mapping[str, Any]) -> int:
    """Work-embedded topic objects may omit `level` (unlike full Topic entities)."""
    raw = topic.get("level")
    if raw is not None:
        return int(raw)
    return 0


def extract_topics(work: Mapping[str, Any]) -> tuple[TopicLink, ...]:
    raw_topics = work.get("topics")
    if not isinstance(raw_topics, list):
        return ()

    topics: list[TopicLink] = []
    for topic in raw_topics:
        if not isinstance(topic, Mapping):
            continue
        topic_id = topic.get("id")
        display_name = topic.get("display_name")
        score = topic.get("score")
        if topic_id is None or display_name is None or score is None:
            continue
        topics.append(
            TopicLink(
                topic_openalex_id=str(topic_id),
                display_name=clean_openalex_text(str(display_name)),
                level=_topic_level_from_payload(topic),
                score=float(score),
            )
        )
    return tuple(topics)


def extract_citations(work: Mapping[str, Any]) -> tuple[CitationLink, ...]:
    referenced_works = work.get("referenced_works")
    if not isinstance(referenced_works, list):
        return ()
    return tuple(CitationLink(cited_openalex_id=str(item)) for item in referenced_works if item)


def _abstract_text(work: Mapping[str, Any]) -> str | None:
    text = abstract_plain_text(work)
    return text if text.strip() else None

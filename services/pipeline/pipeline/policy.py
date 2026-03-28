from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Mapping

from pipeline.openalex_text import abstract_plain_text


@dataclass(frozen=True)
class SourcePolicy:
    slug: str
    display_name: str
    venue_class: str
    rationale: str
    openalex_source_id: str | None = None
    aliases: tuple[str, ...] = ()

    def names_for_resolution(self) -> tuple[str, ...]:
        ordered = [self.display_name, *self.aliases]
        seen: set[str] = set()
        unique: list[str] = []
        for item in ordered:
            key = item.casefold()
            if key not in seen:
                seen.add(key)
                unique.append(item)
        return tuple(unique)


@dataclass(frozen=True)
class PolicyDecision:
    included: bool
    reason: str
    venue_class: str
    matched_keywords: tuple[str, ...] = ()
    matched_exclusions: tuple[str, ...] = ()
    is_core_corpus: bool = False


@dataclass(frozen=True)
class CorpusPolicy:
    name: str = "research-radar-v1"
    min_year: int = 2016
    citation_context_start_year: int = 2000
    language: str = "en"
    include_document_types: tuple[str, ...] = ("article", "proceedings-article", "preprint")
    require_abstract: bool = True
    exclude_retracted: bool = True
    source_policies: tuple[SourcePolicy, ...] = (
        SourcePolicy(
            slug="ismir",
            display_name="International Society for Music Information Retrieval Conference",
            venue_class="core",
            rationale="Core MIR proceedings venue for the corpus.",
            aliases=("ISMIR",),
        ),
        SourcePolicy(
            slug="tismir",
            display_name="Transactions of the International Society for Music Information Retrieval",
            venue_class="core",
            rationale="Core MIR journal venue for the corpus.",
            aliases=("TISMIR",),
        ),
        SourcePolicy(
            slug="dafx",
            display_name="International Conference on Digital Audio Effects",
            venue_class="core",
            rationale="Adjacent peer-reviewed audio effects venue with direct technical overlap.",
            aliases=("DAFx",),
        ),
        SourcePolicy(
            slug="jaes",
            display_name="Journal of the Audio Engineering Society",
            venue_class="core",
            rationale="Peer-reviewed audio technology journal with relevant MIR and audio-ML work.",
            aliases=("JAES", "AES Journal"),
        ),
        SourcePolicy(
            slug="icassp",
            display_name="IEEE International Conference on Acoustics, Speech, and Signal Processing",
            venue_class="core",
            rationale="Flagship signal-processing venue where adjacent audio work appears.",
            aliases=("ICASSP",),
        ),
    )
    strong_topic_signals: tuple[str, ...] = (
        "music information retrieval",
        "audio representation learning",
        "music tagging",
        "music transcription",
        "source separation",
        "beat tracking",
        "onset detection",
        "music similarity",
        "mir evaluation",
        "self-supervised audio",
        "contrastive audio",
        "audio embeddings",
    )
    controlled_edge_terms: tuple[str, ...] = (
        "neural audio effects",
        "differentiable dsp",
        "music generation",
        "audio generation",
    )
    exclusion_terms: tuple[str, ...] = (
        "speech recognition",
        "speaker verification",
        "biomedical audio",
        "medical audio",
    )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def policy_hash(self) -> str:
        payload = json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    def source_for_slug(self, slug: str) -> SourcePolicy | None:
        for source in self.source_policies:
            if source.slug == slug:
                return source
        return None

    def unresolved_sources(self) -> tuple[SourcePolicy, ...]:
        return tuple(source for source in self.source_policies if source.openalex_source_id is None)

    def classify_source(self, source_id: str | None, source_name: str | None) -> SourcePolicy | None:
        normalized_name = _norm_display_name(source_name or "")
        for source in self.source_policies:
            if source_id and source.openalex_source_id and source.openalex_source_id == source_id:
                return source
            if normalized_name and any(_norm_display_name(c) == normalized_name for c in source.names_for_resolution()):
                return source
        return None

    def evaluate_work(self, work: Mapping[str, Any]) -> PolicyDecision:
        year = int(work.get("publication_year") or work.get("year") or 0)
        if year < self.min_year:
            return PolicyDecision(False, "before_year_floor", "excluded")

        language = (work.get("language") or "").casefold()
        if language != self.language:
            return PolicyDecision(False, "language_mismatch", "excluded")

        work_type = (work.get("type") or "").casefold()
        if work_type not in self.include_document_types:
            return PolicyDecision(False, "document_type_excluded", "excluded")

        if self.exclude_retracted and bool(work.get("is_retracted")):
            return PolicyDecision(False, "retracted", "excluded")

        plain_abstract = abstract_plain_text(work)
        has_abstract = bool(work.get("has_abstract")) or bool(plain_abstract.strip())
        if self.require_abstract and not has_abstract:
            return PolicyDecision(False, "missing_abstract", "excluded")

        source = self.classify_source(
            source_id=_dig(work, "primary_location", "source", "id"),
            source_name=_dig(work, "primary_location", "source", "display_name"),
        )

        text_blob = _build_text_blob(work)
        matched_exclusions = tuple(term for term in self.exclusion_terms if term in text_blob)
        if matched_exclusions:
            return PolicyDecision(False, "explicit_exclusion_term", "excluded", matched_exclusions=matched_exclusions)

        matched_keywords = tuple(term for term in self.strong_topic_signals if term in text_blob)
        matched_edge = tuple(term for term in self.controlled_edge_terms if term in text_blob)

        if source is None:
            if matched_keywords and bool(work.get("is_curated_edge")):
                return PolicyDecision(True, "curated_edge_allowlist", "edge", matched_keywords=matched_keywords)
            if matched_keywords and bool(work.get("is_connected_to_core")):
                return PolicyDecision(True, "citation_connected_edge", "edge", matched_keywords=matched_keywords)
            return PolicyDecision(False, "source_not_allowed", "excluded", matched_keywords=matched_keywords)

        if not matched_keywords:
            if source.slug == "icassp":
                return PolicyDecision(False, "topic_gate_failed", source.venue_class)
            if matched_edge:
                return PolicyDecision(False, "edge_term_without_core_signal", source.venue_class, matched_keywords=matched_edge)
            return PolicyDecision(False, "topic_gate_failed", source.venue_class)

        return PolicyDecision(
            included=True,
            reason="core_source_topic_match",
            venue_class=source.venue_class,
            matched_keywords=matched_keywords,
            is_core_corpus=source.venue_class == "core",
        )


def _dig(payload: Mapping[str, Any], *keys: str) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _norm_display_name(name: str) -> str:
    return " ".join(str(name).split()).casefold()


def _build_text_blob(work: Mapping[str, Any]) -> str:
    title = str(work.get("title") or "")
    abstract = abstract_plain_text(work)
    topics = work.get("topics") or ()
    topic_names = []
    for topic in topics:
        if isinstance(topic, Mapping):
            name = topic.get("display_name") or topic.get("name")
            if name:
                topic_names.append(str(name))
    return " ".join([title, abstract, *topic_names]).casefold()


def corpus_policy_with_openalex_source_ids(policy: CorpusPolicy, slug_to_id: Mapping[str, str]) -> CorpusPolicy:
    """Return a copy of policy with `openalex_source_id` set for every source (authoritative ingest)."""
    resolved: list[SourcePolicy] = []
    for s in policy.source_policies:
        oid = s.openalex_source_id or slug_to_id.get(s.slug)
        if not oid:
            raise ValueError(f"Missing OpenAlex source id for source slug {s.slug!r}")
        resolved.append(replace(s, openalex_source_id=oid))
    return replace(policy, source_policies=tuple(resolved))

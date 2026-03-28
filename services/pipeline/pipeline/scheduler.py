from dataclasses import dataclass


@dataclass(frozen=True)
class ScheduledJob:
    name: str
    cadence: str
    purpose: str


DEFAULT_JOBS: tuple[ScheduledJob, ...] = (
    ScheduledJob(
        name="corpus_snapshot",
        cadence="weekly",
        purpose="Refresh the curated OpenAlex slice and emit a new corpus snapshot version.",
    ),
    ScheduledJob(
        name="embedding_refresh",
        cadence="weekly",
        purpose="Generate embeddings for newly admitted papers and persist an embedding version.",
    ),
    ScheduledJob(
        name="ranking_refresh",
        cadence="weekly",
        purpose="Materialize recommendation families and persist a ranking version.",
    ),
)

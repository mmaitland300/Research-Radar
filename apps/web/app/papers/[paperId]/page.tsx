import Link from "next/link";

const FAMILIES = ["emerging", "bridge", "undercited"] as const;
type Family = (typeof FAMILIES)[number];

const FAMILY_LABEL: Record<Family, string> = {
  emerging: "Emerging",
  bridge: "Bridge",
  undercited: "Under-cited"
};

type PaperDetail = {
  paper_id: string;
  title: string;
  abstract: string;
  venue: string | null;
  year: number;
  citation_count: number;
  source_slug: string | null;
  is_core_corpus: boolean;
  authors: string[];
  topics: string[];
};

type SimilarPaperItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  topics: string[];
  similarity: number;
};

type SimilarPapersResponse = {
  paper_id: string;
  embedding_version: string;
  total: number;
  items: SimilarPaperItem[];
};

type RankedSignals = {
  semantic: number | null;
  citation_velocity: number | null;
  topic_growth: number | null;
  bridge: number | null;
  diversity_penalty: number | null;
};

type RankedSignalExplanation = {
  key: string;
  label: string;
  role: "used" | "measured" | "experimental" | "penalty" | "not_computed";
  value: number | null;
  contribution: number | null;
  summary: string;
};

type PaperRankingFamilyItem = {
  family: Family;
  present: boolean;
  in_top_n: boolean;
  rank: number | null;
  final_score: number | null;
  reason_short: string | null;
  signals: RankedSignals | null;
  signal_explanations: RankedSignalExplanation[];
  bridge_eligible: boolean | null;
};

type PaperRankingResponse = {
  paper_id: string;
  ranking_run_id: string;
  ranking_version: string;
  corpus_snapshot_version: string;
  top_n: number;
  rank_scope: string;
  families: PaperRankingFamilyItem[];
};

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

const EMBEDDING_VERSION =
  process.env.NEXT_PUBLIC_EMBEDDING_VERSION?.trim() || undefined;
const RANKING_VERSION =
  process.env.NEXT_PUBLIC_RANKING_VERSION?.trim() || undefined;

type SimilarPapersState =
  | { kind: "disabled" }
  | { kind: "ok"; data: SimilarPapersResponse }
  | { kind: "not_found"; message: string }
  | { kind: "error"; message: string };

type PaperRankingState =
  | { kind: "ok"; data: PaperRankingResponse }
  | { kind: "not_available"; message: string }
  | { kind: "error"; message: string };

async function fetchPaperDetail(
  paperId: string
): Promise<{ paper: PaperDetail | null; error: string | null }> {
  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/papers/${encodeURIComponent(paperId)}`,
      { cache: "no-store" }
    );

    if (response.status === 404) {
      return { paper: null, error: "Paper not found." };
    }
    if (!response.ok) {
      return { paper: null, error: `API responded with ${response.status}.` };
    }

    const paper = (await response.json()) as PaperDetail;
    return { paper, error: null };
  } catch {
    return {
      paper: null,
      error: "Could not reach the API. Start apps/api and Postgres, then refresh."
    };
  }
}

async function fetchSimilarPapers(paperId: string): Promise<SimilarPapersState> {
  if (!EMBEDDING_VERSION) {
    return { kind: "disabled" };
  }

  try {
    const params = new URLSearchParams({
      embedding_version: EMBEDDING_VERSION,
      limit: "6"
    });
    const response = await fetch(
      `${API_BASE_URL}/api/v1/papers/${encodeURIComponent(paperId)}/similar?${params.toString()}`,
      { cache: "no-store" }
    );

    if (response.status === 404) {
      return {
        kind: "not_found",
        message:
          "No embedding-backed neighbors available for this paper/version yet."
      };
    }

    if (!response.ok) {
      return {
        kind: "error",
        message: `Similar papers could not be loaded (API ${response.status}).`
      };
    }

    const data = (await response.json()) as SimilarPapersResponse;
    return { kind: "ok", data };
  } catch {
    return {
      kind: "error",
      message:
        "Could not load similar papers. The API may be unreachable."
    };
  }
}

function barWidthPercent(value: number | null, role: RankedSignalExplanation["role"]): number {
  if (value == null || role === "not_computed") return 0;
  return Math.min(100, Math.round(Math.max(0, value) * 100));
}

function barFillClass(role: RankedSignalExplanation["role"]): string {
  if (role === "used") return "ranking-bar-fill ranking-bar-used";
  if (role === "measured" || role === "experimental") {
    return "ranking-bar-fill ranking-bar-measured";
  }
  if (role === "penalty") return "ranking-bar-fill ranking-bar-penalty";
  return "ranking-bar-fill ranking-bar-none";
}

function explanationSummary(explanations: RankedSignalExplanation[]): string {
  const count = (role: RankedSignalExplanation["role"]) =>
    explanations.filter((e) => e.role === role).length;
  const parts: string[] = [];
  const used = count("used");
  const measured = count("measured");
  const experimental = count("experimental");
  const penalty = count("penalty");
  const notComputed = count("not_computed");
  if (used) parts.push(`${used} used`);
  if (measured) parts.push(`${measured} measured`);
  if (experimental) parts.push(`${experimental} experimental`);
  if (penalty) parts.push(`${penalty} penalty`);
  if (notComputed) parts.push(`${notComputed} not computed`);
  return parts.length > 0 ? parts.join(" | ") : "No signal breakdown";
}

function formatSignals(signals: RankedSignals | null): string {
  if (!signals) return "n/a";
  const entries: [string, number][] = [];
  if (signals.semantic != null) entries.push(["semantic", signals.semantic]);
  if (signals.citation_velocity != null) {
    entries.push(["citation_velocity", signals.citation_velocity]);
  }
  if (signals.topic_growth != null) entries.push(["topic_growth", signals.topic_growth]);
  if (signals.bridge != null) entries.push(["bridge", signals.bridge]);
  if (signals.diversity_penalty != null) {
    entries.push(["diversity_penalty", signals.diversity_penalty]);
  }
  if (entries.length === 0) return "n/a";
  return entries.map(([k, v]) => `${k}=${Number(v).toFixed(4)}`).join(", ");
}

async function fetchPaperRanking(
  paperId: string,
  options?: { rankingRunId?: string }
): Promise<PaperRankingState> {
  const params = new URLSearchParams({
    top_n: "50"
  });
  if (options?.rankingRunId) {
    params.set("ranking_run_id", options.rankingRunId);
  } else if (RANKING_VERSION) {
    params.set("ranking_version", RANKING_VERSION);
  }

  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/papers/${encodeURIComponent(paperId)}/ranking?${params.toString()}`,
      { cache: "no-store" }
    );

    if (response.status === 404) {
      const body = (await response.json().catch(() => ({}))) as { detail?: unknown };
      const detail =
        typeof body.detail === "string"
          ? body.detail
          : "No succeeded ranking run found for the given filters.";
      return {
        kind: "not_available",
        message: detail
      };
    }

    if (!response.ok) {
      return {
        kind: "error",
        message: `Ranking details could not be loaded (API ${response.status}).`
      };
    }

    const data = (await response.json()) as PaperRankingResponse;
    return { kind: "ok", data };
  } catch {
    return {
      kind: "error",
      message: "Could not load ranking details. The API may be unreachable."
    };
  }
}

function buildRecommendedHref(args: {
  family: Family;
  paperId: string;
  rankingRunId?: string;
  limit?: number;
}): string {
  const params = new URLSearchParams({ family: args.family, paper: args.paperId });
  if (args.rankingRunId) params.set("ranking_run_id", args.rankingRunId);
  if (args.limit != null) params.set("limit", String(args.limit));
  return `/recommended?${params.toString()}`;
}

function buildEvaluationHref(args: {
  family: Family;
  paperId: string;
  rankingRunId?: string;
  limit?: number;
}): string {
  const params = new URLSearchParams({ family: args.family, paper: args.paperId });
  if (args.rankingRunId) params.set("ranking_run_id", args.rankingRunId);
  if (args.limit != null) params.set("limit", String(args.limit));
  return `/evaluation?${params.toString()}`;
}

function buildPaperHref(args: { paperId: string; rankingRunId?: string }): string {
  const base = `/papers/${encodeURIComponent(args.paperId)}`;
  if (!args.rankingRunId) return base;
  const params = new URLSearchParams({ ranking_run_id: args.rankingRunId });
  return `${base}?${params.toString()}`;
}

function formatRankScope(rankScope: string): string {
  return rankScope.replaceAll("_", " ");
}

function PaperWhySurfaced({ explanations }: { explanations: RankedSignalExplanation[] }) {
  return (
    <details className="ranking-why-details">
      <summary>Why this surfaced | {explanationSummary(explanations)}</summary>
      {explanations.map((e) => (
        <div key={e.key} className="ranking-signal-row">
          <div className="ranking-signal-label">
            <span>{e.label}</span>
            <span className="ranking-signal-role">{e.role.replace("_", " ")}</span>
          </div>
          <div className="ranking-bar-track" aria-hidden>
            <div
              className={barFillClass(e.role)}
              style={{ width: `${barWidthPercent(e.value, e.role)}%` }}
            />
          </div>
          <p className="result-breakdown" style={{ marginTop: 4 }}>
            {e.summary}
            {e.contribution != null && e.role !== "not_computed" ? (
              <>
                {" "}
                (contribution to score: {e.contribution.toFixed(4)})
              </>
            ) : null}
          </p>
        </div>
      ))}
    </details>
  );
}

export default async function PaperDetailPage({
  params,
  searchParams
}: {
  params: { paperId: string };
  searchParams?: Record<string, string | string[] | undefined>;
}) {
  const canonicalPaperId = decodeURIComponent(params.paperId);
  const rankingRunIdParam = Array.isArray(searchParams?.ranking_run_id)
    ? searchParams?.ranking_run_id[0]
    : searchParams?.ranking_run_id;
  const { paper, error } = await fetchPaperDetail(canonicalPaperId);

  const similar: SimilarPapersState | null =
    paper && !error ? await fetchSimilarPapers(canonicalPaperId) : null;
  const ranking: PaperRankingState | null =
    paper && !error
      ? await fetchPaperRanking(canonicalPaperId, {
          rankingRunId: rankingRunIdParam?.trim() || undefined
        })
      : null;
  const similarCount = similar?.kind === "ok" ? similar.data.items.length : 0;
  const rankingPresentCount =
    ranking?.kind === "ok" ? ranking.data.families.filter((family) => family.present).length : 0;
  const rankingTopNCount =
    ranking?.kind === "ok" ? ranking.data.families.filter((family) => family.in_top_n).length : 0;
  const emergingFocusHref =
    ranking?.kind === "ok"
      ? buildRecommendedHref({
          family: "emerging",
          paperId: canonicalPaperId,
          rankingRunId: ranking.data.ranking_run_id,
          limit: Math.max(
            15,
            ranking.data.families.find((family) => family.family === "emerging")?.rank ?? 15
          )
        })
      : "/recommended?family=emerging";
  const bridgeFocusHref =
    ranking?.kind === "ok"
      ? buildRecommendedHref({
          family: "bridge",
          paperId: canonicalPaperId,
          rankingRunId: ranking.data.ranking_run_id,
          limit: Math.max(
            15,
            ranking.data.families.find((family) => family.family === "bridge")?.rank ??
              ranking.data.top_n
          )
        })
      : "/recommended?family=bridge";
  const undercitedFocusHref =
    ranking?.kind === "ok"
      ? buildRecommendedHref({
          family: "undercited",
          paperId: canonicalPaperId,
          rankingRunId: ranking.data.ranking_run_id,
          limit: Math.max(
            15,
            ranking.data.families.find((family) => family.family === "undercited")?.rank ??
              ranking.data.top_n
          )
        })
      : "/recommended?family=undercited";
  const evaluationFocusHref =
    ranking?.kind === "ok"
      ? buildEvaluationHref({
          family: "emerging",
          paperId: canonicalPaperId,
          rankingRunId: ranking.data.ranking_run_id,
          limit: Math.min(
            50,
            Math.max(
              12,
              ranking.data.families.find((family) => family.family === "emerging")?.rank ?? 12
            )
          )
        })
      : "/evaluation?family=emerging";

  return (
    <main className="page">
      <section className="panel page-hero family-hero family-hero-emerging">
        <div className="family-hero-grid">
          <div>
            <div className="panel-header">
              <div>
                <p className="eyebrow family-emerging">Paper dossier</p>
                <h1 className="paper-dossier-hero-title">{paper?.title ?? "Paper detail"}</h1>
              </div>
              <div className="stamp-row">
                <span className="stamp">Detail view</span>
                <span className="stamp">Similarity handoff</span>
              </div>
            </div>
            {error ? <p>{error}</p> : null}
            {!error && !paper ? <p>No detail was returned for this paper id.</p> : null}
            {paper ? (
              <>
                <p className="hero-lead">
                  Review source metadata, abstract, authors, topics, and local similarity context
                  before moving into explanation and ranking views.
                </p>
                <div className="hero-metrics" aria-label="Paper summary">
                  <article className="metric-card">
                    <p className="metric-label">Paper year</p>
                    <p className="metric-value">{paper.year}</p>
                  </article>
                  <article className="metric-card">
                    <p className="metric-label">Citations</p>
                    <p className="metric-value">{paper.citation_count}</p>
                  </article>
                  <article className="metric-card">
                    <p className="metric-label">Authors</p>
                    <p className="metric-value">{paper.authors.length}</p>
                  </article>
                  <article className="metric-card">
                    <p className="metric-label">Topic labels</p>
                    <p className="metric-value">{paper.topics.length}</p>
                  </article>
                </div>
                <div className="meta">
                  <span>Paper ID: {canonicalPaperId}</span>
                  <span>{paper.is_core_corpus ? "core corpus" : "edge slice"}</span>
                  <span>{paper.source_slug ?? "unknown source slug"}</span>
                </div>
                <div className="action-row" aria-label="Paper workflow handoff">
                  <Link className="action-link" href={emergingFocusHref}>
                    Check emerging feed
                  </Link>
                  <Link className="action-link" href={bridgeFocusHref}>
                    Check bridge preview
                  </Link>
                  <Link className="action-link" href="/trends">
                    Inspect topic momentum
                  </Link>
                </div>
              </>
            ) : null}
          </div>
          <aside className="family-brief">
            <div className="family-brief-diagram" aria-hidden="true">
              <span className="family-ring family-ring-a" />
              <span className="family-ring family-ring-b" />
              <span className="family-ring family-ring-c" />
              <span className="family-sweep family-sweep-emerging" />
              <span className="family-node family-node-emerging family-node-1" />
              <span className="family-node family-node-emerging family-node-2" />
              <span className="family-node family-node-emerging family-node-3" />
            </div>
            <div className="family-brief-copy">
              <p className="eyebrow family-emerging">Reading guide</p>
              <h2>Use this page as a paper dossier</h2>
              <ul className="measure-list">
                <li>Confirm the source and abstract before judging similarity.</li>
                <li>Use topic labels as coarse navigation hints for neighborhood placement.</li>
                <li>Hand off from this page into ranked or similar-paper surfaces.</li>
              </ul>
            </div>
          </aside>
        </div>
      </section>

      {paper ? (
        <section className="panel section-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow eyebrow-muted">Source readout</p>
              <h2>Source and corpus status</h2>
            </div>
          </div>
          <div className="info-grid">
            <article className="brief-card">
              <h3>Venue</h3>
              <p>{paper.venue ?? "Unknown venue"}</p>
            </article>
            <article className="brief-card">
              <h3>Source slug</h3>
              <p className="metric-value metric-value-mono">{paper.source_slug ?? "unknown"}</p>
            </article>
            <article className="brief-card">
              <h3>Corpus placement</h3>
              <p>{paper.is_core_corpus ? "Core corpus" : "Controlled edge slice"}</p>
            </article>
            <article className="brief-card">
              <h3>Similarity rows</h3>
              <p>{similarCount || "Not available yet"}</p>
            </article>
          </div>
        </section>
      ) : null}

      {paper ? (
        <section className="panel section-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow eyebrow-muted">Ranking readout</p>
              <h2>Where this paper lands in the current run</h2>
            </div>
            {ranking?.kind === "ok" ? (
              <div className="stamp-row">
                <span className="stamp">Run {ranking.data.ranking_version}</span>
                <span className="stamp">Top {ranking.data.top_n} surfaced</span>
              </div>
            ) : null}
          </div>
          {ranking?.kind === "ok" ? (
            <>
              <p className="hero-lead dossier-ranking-lead">
                This block uses the same resolved ranking run as Recommended. Family rank is
                global within each family, but rank is only shown when this paper lands inside the
                surfaced top {ranking.data.top_n}.
              </p>
              <div className="hero-metrics hero-metrics-compact" aria-label="Ranking dossier summary">
                <article className="metric-card">
                  <p className="metric-label">Families present</p>
                  <p className="metric-value">{rankingPresentCount}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Top {ranking.data.top_n}</p>
                  <p className="metric-value">{rankingTopNCount}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Run label</p>
                  <p className="metric-value metric-value-mono">{ranking.data.ranking_version}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Snapshot</p>
                  <p className="metric-value metric-value-mono">
                    {ranking.data.corpus_snapshot_version}
                  </p>
                </article>
              </div>
              <p className="muted-inline">
                Scope: {formatRankScope(ranking.data.rank_scope)} | run{" "}
                <code>{ranking.data.ranking_run_id}</code>
              </p>
              <div className="dossier-ranking-grid">
                {ranking.data.families.map((family) => (
                  <article
                    key={family.family}
                    className={`result-item dossier-ranking-card result-item-${family.family}`}
                  >
                    <div className="result-heading">
                      <div>
                        <p className="result-title">{FAMILY_LABEL[family.family]}</p>
                        <p className="family-rank-line">
                          {family.present
                            ? family.in_top_n && family.rank != null
                              ? `In top ${ranking.data.top_n} at rank ${family.rank}`
                              : `Present in run, outside top ${ranking.data.top_n}`
                            : "No materialized row for this family in the resolved run"}
                        </p>
                      </div>
                      <span className={`result-score result-score-${family.family}`}>
                        {family.final_score != null ? family.final_score.toFixed(3) : "n/a"}
                      </span>
                    </div>
                    {family.reason_short ? (
                      <p className="family-rank-summary">{family.reason_short}</p>
                    ) : (
                      <p className="family-rank-summary">
                        This paper did not surface into the current materialized family row set.
                      </p>
                    )}
                    {family.family === "bridge" && family.bridge_eligible != null ? (
                      <p className="result-breakdown">
                        Bridge eligibility: {family.bridge_eligible ? "eligible" : "not eligible"}
                      </p>
                    ) : null}
                    {family.present ? (
                      <>
                        <p className="result-breakdown">Signals: {formatSignals(family.signals)}</p>
                        {family.signal_explanations.length > 0 ? (
                          <PaperWhySurfaced explanations={family.signal_explanations} />
                        ) : null}
                      </>
                    ) : null}
                    <div className="action-row" aria-label={`${FAMILY_LABEL[family.family]} handoff`}>
                      <Link
                        className="action-link"
                        href={buildRecommendedHref({
                          family: family.family,
                          paperId: canonicalPaperId,
                          rankingRunId: ranking.data.ranking_run_id,
                          limit: Math.max(15, family.rank ?? ranking.data.top_n)
                        })}
                      >
                        Open {FAMILY_LABEL[family.family]} feed
                      </Link>
                      <Link
                        className="action-link"
                        href={buildEvaluationHref({
                          family: family.family,
                          paperId: canonicalPaperId,
                          rankingRunId: ranking.data.ranking_run_id,
                          limit: Math.min(50, Math.max(12, family.rank ?? ranking.data.top_n))
                        })}
                      >
                        Compare baseline
                      </Link>
                    </div>
                  </article>
                ))}
              </div>
            </>
          ) : null}
          {ranking?.kind === "not_available" ? (
            <p className="muted-inline">{ranking.message}</p>
          ) : null}
          {ranking?.kind === "error" ? (
            <p className="muted-inline">{ranking.message}</p>
          ) : null}
        </section>
      ) : null}

      <section className="split detail-split">
        <article className="panel instrument-panel">
          <h2>Abstract</h2>
          <p className="detail-abstract">{paper?.abstract || "No abstract available."}</p>
        </article>
        <article className="panel instrument-panel">
          <h2>Authors</h2>
          {paper && paper.authors.length > 0 ? (
            <ul className="author-list">
              {paper.authors.map((author) => (
                <li key={author} className="author-chip">
                  {author}
                </li>
              ))}
            </ul>
          ) : (
            <p>No authors available.</p>
          )}
        </article>
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Neighborhood labels</p>
            <h2>Topics</h2>
          </div>
          {paper ? (
            <div className="stamp-row">
              <span className="stamp">{paper.topics.length} labels</span>
            </div>
          ) : null}
        </div>
        <p className="muted-inline">
          Topic labels are imported metadata and can be noisy; use them as coarse navigation hints,
          not authoritative classifications.
        </p>
        {paper && paper.topics.length > 0 ? (
          <p className="chip-row" aria-label="Topics">
            {paper.topics.map((topic) => (
              <span key={topic} className="chip">
                {topic}
              </span>
            ))}
          </p>
        ) : null}
      </section>

      {similar ? (
        <section className="panel section-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow eyebrow-muted">Neighbor surface</p>
              <h2>Similar papers</h2>
            </div>
            {similar.kind === "ok" ? (
              <div className="stamp-row">
                <span className="stamp">{similar.data.total} total neighbors</span>
                <span className="stamp">
                  Embedding {similar.data.embedding_version}
                </span>
              </div>
            ) : null}
          </div>
          {similar.kind === "disabled" ? (
            <p className="muted-inline">
              Configure an active embedding version to enable embedding-backed
              similar papers (for example,{" "}
              <code>v1-title-abstract-1536</code>).
            </p>
          ) : null}
          {similar.kind === "not_found" ? (
            <p className="muted-inline">{similar.message}</p>
          ) : null}
          {similar.kind === "error" ? (
            <p className="muted-inline">{similar.message}</p>
          ) : null}
          {similar.kind === "ok" && similar.data.total === 0 ? (
            <p className="muted-inline">
              No similar papers found for this version.
            </p>
          ) : null}
          {similar.kind === "ok" && similar.data.items.length > 0 ? (
            <ul className="result-list">
              {similar.data.items.map((item) => (
                <li key={item.paper_id} className="result-item result-item-emerging">
                  <div className="result-heading">
                    <p className="result-title">
                      <Link
                        href={buildPaperHref({
                          paperId: item.paper_id,
                          rankingRunId:
                            ranking?.kind === "ok" ? ranking.data.ranking_run_id : undefined
                        })}
                      >
                        {item.title}
                      </Link>
                    </p>
                    <span className="result-score result-score-emerging">
                      {item.similarity.toFixed(3)}
                    </span>
                  </div>
                  <p className="result-meta">
                    {item.year} | cites: {item.citation_count} |{" "}
                    {item.source_slug ?? "unknown venue"}
                  </p>
                  {item.topics.length > 0 ? (
                    <div className="chip-row" aria-label="Top topics">
                      {item.topics.map((t) => (
                        <span key={t} className="chip">
                          {t}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  <div className="action-row" aria-label="Neighbor handoff">
                    <Link
                      className="action-link"
                      href={buildPaperHref({
                        paperId: item.paper_id,
                        rankingRunId:
                          ranking?.kind === "ok" ? ranking.data.ranking_run_id : undefined
                      })}
                    >
                      Open neighbor dossier
                    </Link>
                    <Link
                      className="action-link"
                      href={
                        ranking?.kind === "ok"
                          ? buildRecommendedHref({
                              family: "emerging",
                              paperId: item.paper_id,
                              rankingRunId: ranking.data.ranking_run_id
                            })
                          : "/recommended?family=emerging"
                      }
                    >
                      Compare in feed
                    </Link>
                  </div>
                </li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Next handoff</p>
            <h2>Best next moves from here</h2>
          </div>
        </div>
        <div className="workflow-grid">
          <article className="workflow-card">
            <p className="workflow-step">01</p>
            <h3>Check recommendation families</h3>
            <p>
              Use Recommended to see whether this paper behaves like an emerging or undercited
              signal in the current ranked feed, or how it appears on the bridge preview /
              diagnostics view.
            </p>
            <div className="action-row">
              <Link className="action-link" href={emergingFocusHref}>
                Emerging
              </Link>
              <Link className="action-link" href={bridgeFocusHref}>
                Bridge preview
              </Link>
              <Link className="action-link" href={undercitedFocusHref}>
                Under-cited
              </Link>
            </div>
          </article>
          <article className="workflow-card">
            <p className="workflow-step">02</p>
            <h3>Inspect nearby topics</h3>
            <p>
              Use Trends to understand whether its attached labels are heating up or cooling down
              inside the curated corpus.
            </p>
            <div className="action-row">
              <Link className="action-link" href="/trends">
                Open trends
              </Link>
            </div>
          </article>
          <article className="workflow-card">
            <p className="workflow-step">03</p>
            <h3>Cross-check evaluation baselines</h3>
            <p>
              Use Evaluation to compare the dossier readout against citation and recency baselines
              for the same resolved family run.
            </p>
            <div className="action-row">
              <Link className="action-link" href={evaluationFocusHref}>
                Open evaluation
              </Link>
            </div>
          </article>
        </div>
      </section>
    </main>
  );
}

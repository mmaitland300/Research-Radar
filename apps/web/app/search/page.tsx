import Link from "next/link";

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

const FAMILIES = ["emerging", "bridge", "undercited"] as const;
type FamilyHint = (typeof FAMILIES)[number];

/** Lexical sample queries for the empty state; MIR / audio-ML phrasing aligned with the curated corpus. */
const REVIEWER_SAMPLE_QUERIES = [
  "audio embeddings",
  "music information retrieval",
  "source separation",
  "piano transcription",
  "self-supervised audio"
] as const;

const INCLUDED_SCOPES = ["all_included", "core"] as const;
type IncludedScope = (typeof INCLUDED_SCOPES)[number];

type SearchMatchMetadata = {
  matched_fields: string[];
  highlight_fragments: string[];
  lexical_rank: number;
};

type SearchResultItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  source_label: string | null;
  is_core_corpus: boolean;
  topics: string[];
  preview: string | null;
  match: SearchMatchMetadata;
};

type SearchResolvedFilters = {
  q: string;
  limit: number;
  offset: number;
  year_from: number | null;
  year_to: number | null;
  included_scope: IncludedScope;
  source_slug: string | null;
  topic: string | null;
  family_hint: FamilyHint | null;
  ranking_run_id?: string | null;
  ranking_version?: string | null;
};

type SearchResponse = {
  total: number;
  ordering: string;
  resolved_filters: SearchResolvedFilters;
  items: SearchResultItem[];
  resolved_ranking_run_id?: string | null;
  resolved_ranking_version?: string | null;
  resolved_corpus_snapshot_version?: string | null;
};

type PageProps = {
  searchParams: Record<string, string | string[] | undefined>;
};

function parseSingleParam(raw: string | string[] | undefined): string | undefined {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function parseIntegerParam(
  raw: string | string[] | undefined,
  fallback: number,
  options: { min: number; max: number }
): number {
  const value = parseSingleParam(raw);
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(options.min, Math.min(options.max, Math.trunc(parsed)));
}

function parseOptionalYear(raw: string | string[] | undefined): number | undefined {
  const value = parseSingleParam(raw);
  if (!value) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return undefined;
  const year = Math.trunc(parsed);
  if (year < 1900 || year > 2100) return undefined;
  return year;
}

function parseIncludedScope(raw: string | string[] | undefined): IncludedScope {
  const value = parseSingleParam(raw);
  if (value && INCLUDED_SCOPES.includes(value as IncludedScope)) {
    return value as IncludedScope;
  }
  return "all_included";
}

function parseFamilyHint(raw: string | string[] | undefined): FamilyHint | undefined {
  const value = parseSingleParam(raw);
  if (value && FAMILIES.includes(value as FamilyHint)) {
    return value as FamilyHint;
  }
  return undefined;
}

function normalizePayload(json: unknown): SearchResponse | null {
  if (!json || typeof json !== "object") return null;
  const raw = json as Record<string, unknown>;
  if (!Array.isArray(raw.items) || !raw.resolved_filters || typeof raw.ordering !== "string") {
    return null;
  }

  const resolvedRaw = raw.resolved_filters as Record<string, unknown>;
  const items: SearchResultItem[] = raw.items.map((row: unknown) => {
    const item = row as Record<string, unknown>;
    const matchRaw = item.match as Record<string, unknown>;
    return {
      paper_id: String(item.paper_id ?? ""),
      title: String(item.title ?? ""),
      year: Number(item.year ?? 0),
      citation_count: Number(item.citation_count ?? 0),
      source_slug: item.source_slug == null ? null : String(item.source_slug),
      source_label: item.source_label == null ? null : String(item.source_label),
      is_core_corpus: Boolean(item.is_core_corpus),
      topics: Array.isArray(item.topics)
        ? item.topics.filter((topic): topic is string => typeof topic === "string")
        : [],
      preview: item.preview == null ? null : String(item.preview),
      match: {
        matched_fields: Array.isArray(matchRaw?.matched_fields)
          ? matchRaw.matched_fields.filter((field): field is string => typeof field === "string")
          : [],
        highlight_fragments: Array.isArray(matchRaw?.highlight_fragments)
          ? matchRaw.highlight_fragments.filter(
              (fragment): fragment is string => typeof fragment === "string"
            )
          : [],
        lexical_rank: Number(matchRaw?.lexical_rank ?? 0)
      }
    };
  });

  return {
    total: Number(raw.total ?? items.length),
    ordering: raw.ordering,
    resolved_filters: {
      q: String(resolvedRaw.q ?? ""),
      limit: Number(resolvedRaw.limit ?? 15),
      offset: Number(resolvedRaw.offset ?? 0),
      year_from: resolvedRaw.year_from == null ? null : Number(resolvedRaw.year_from),
      year_to: resolvedRaw.year_to == null ? null : Number(resolvedRaw.year_to),
      included_scope:
        resolvedRaw.included_scope === "core" ? "core" : "all_included",
      source_slug: resolvedRaw.source_slug == null ? null : String(resolvedRaw.source_slug),
      topic: resolvedRaw.topic == null ? null : String(resolvedRaw.topic),
      family_hint:
        resolvedRaw.family_hint && FAMILIES.includes(String(resolvedRaw.family_hint) as FamilyHint)
          ? (String(resolvedRaw.family_hint) as FamilyHint)
          : null,
      ranking_run_id:
        resolvedRaw.ranking_run_id == null ? null : String(resolvedRaw.ranking_run_id),
      ranking_version:
        resolvedRaw.ranking_version == null ? null : String(resolvedRaw.ranking_version)
    },
    items,
    resolved_ranking_run_id:
      raw.resolved_ranking_run_id == null ? null : String(raw.resolved_ranking_run_id),
    resolved_ranking_version:
      raw.resolved_ranking_version == null ? null : String(raw.resolved_ranking_version),
    resolved_corpus_snapshot_version:
      raw.resolved_corpus_snapshot_version == null
        ? null
        : String(raw.resolved_corpus_snapshot_version)
  };
}

async function fetchSearch(params: {
  q: string;
  limit: number;
  offset: number;
  yearFrom?: number;
  yearTo?: number;
  includedScope: IncludedScope;
  sourceSlug?: string;
  topic?: string;
  familyHint?: FamilyHint;
  rankingRunId?: string;
  rankingVersion?: string;
}): Promise<{
  data: SearchResponse | null;
  error: string | null;
  status: number | null;
}> {
  const query = new URLSearchParams({
    q: params.q,
    limit: String(params.limit),
    offset: String(params.offset),
    included_scope: params.includedScope
  });
  if (params.yearFrom != null) query.set("year_from", String(params.yearFrom));
  if (params.yearTo != null) query.set("year_to", String(params.yearTo));
  if (params.sourceSlug) query.set("source_slug", params.sourceSlug);
  if (params.topic) query.set("topic", params.topic);
  if (params.familyHint) query.set("family_hint", params.familyHint);
  if (params.familyHint && params.rankingRunId) query.set("ranking_run_id", params.rankingRunId);
  if (params.familyHint && params.rankingVersion) {
    query.set("ranking_version", params.rankingVersion);
  }

  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/search?${query.toString()}`, {
      cache: "no-store"
    });
    if (!response.ok) {
      let detail = "";
      try {
        const body = (await response.json()) as { detail?: unknown };
        if (typeof body.detail === "string") detail = ` ${body.detail}`;
      } catch {
        /* ignore non-JSON response bodies */
      }
      return {
        data: null,
        error: `API responded with ${response.status}.${detail}`,
        status: response.status
      };
    }
    const raw: unknown = await response.json();
    const normalized = normalizePayload(raw);
    if (!normalized) {
      return {
        data: null,
        error: "API returned search results in an unexpected shape.",
        status: 200
      };
    }
    return { data: normalized, error: null, status: 200 };
  } catch {
    return {
      data: null,
      error: "Could not reach the API. Start apps/api and Postgres, then refresh.",
      status: null
    };
  }
}

type SearchState = {
  q?: string;
  limit: number;
  offset: number;
  yearFrom?: number;
  yearTo?: number;
  includedScope: IncludedScope;
  sourceSlug?: string;
  topic?: string;
  familyHint?: FamilyHint;
  rankingRunId?: string;
  rankingVersion?: string;
};

function buildSearchHref(state: SearchState, overrides: Partial<SearchState>): string {
  const next: SearchState = {
    ...state,
    ...overrides
  };
  const params = new URLSearchParams();
  if (next.q) params.set("q", next.q);
  params.set("limit", String(next.limit));
  params.set("offset", String(next.offset));
  params.set("included_scope", next.includedScope);
  if (next.yearFrom != null) params.set("year_from", String(next.yearFrom));
  if (next.yearTo != null) params.set("year_to", String(next.yearTo));
  if (next.sourceSlug) params.set("source_slug", next.sourceSlug);
  if (next.topic) params.set("topic", next.topic);
  if (next.familyHint) params.set("family_hint", next.familyHint);
  if (next.familyHint && next.rankingRunId) params.set("ranking_run_id", next.rankingRunId);
  if (next.familyHint && next.rankingVersion) {
    params.set("ranking_version", next.rankingVersion);
  }
  return `/search?${params.toString()}`;
}

function buildRankingViewHref(
  pathname: "/recommended" | "/evaluation",
  family: FamilyHint,
  resolvedRankingRunId?: string | null
): string {
  const params = new URLSearchParams({ family });
  if (resolvedRankingRunId) params.set("ranking_run_id", resolvedRankingRunId);
  return `${pathname}?${params.toString()}`;
}

function buildPaperHref(paperId: string, resolvedRankingRunId?: string | null): string {
  const base = `/papers/${encodeURIComponent(paperId)}`;
  if (!resolvedRankingRunId) return base;
  const params = new URLSearchParams({ ranking_run_id: resolvedRankingRunId });
  return `${base}?${params.toString()}`;
}

function familyLabel(family: FamilyHint): string {
  if (family === "undercited") return "Under-cited";
  return family.charAt(0).toUpperCase() + family.slice(1);
}

export default async function SearchPage({ searchParams }: PageProps) {
  const q = parseSingleParam(searchParams.q);
  const limit = parseIntegerParam(searchParams.limit, 15, { min: 1, max: 100 });
  const offset = parseIntegerParam(searchParams.offset, 0, { min: 0, max: 10_000 });
  const yearFrom = parseOptionalYear(searchParams.year_from);
  const yearTo = parseOptionalYear(searchParams.year_to);
  const includedScope = parseIncludedScope(searchParams.included_scope);
  const sourceSlug = parseSingleParam(searchParams.source_slug);
  const topic = parseSingleParam(searchParams.topic);
  const familyHint = parseFamilyHint(searchParams.family_hint);
  const rankingRunId = parseSingleParam(searchParams.ranking_run_id);
  const rankingVersion = parseSingleParam(searchParams.ranking_version);

  const currentState: SearchState = {
    q,
    limit,
    offset,
    yearFrom,
    yearTo,
    includedScope,
    sourceSlug,
    topic,
    familyHint,
    rankingRunId: familyHint ? rankingRunId : undefined,
    rankingVersion: familyHint ? rankingVersion : undefined
  };

  const searchResult = q
    ? await fetchSearch({
        q,
        limit,
        offset,
        yearFrom,
        yearTo,
        includedScope,
        sourceSlug,
        topic,
        familyHint,
        rankingRunId: familyHint ? rankingRunId : undefined,
        rankingVersion: familyHint ? rankingVersion : undefined
      })
    : { data: null, error: null, status: null };

  const data = searchResult.data;
  const coreCount = data?.items.filter((paper) => paper.is_core_corpus).length ?? 0;
  const topicTaggedCount = data?.items.filter((paper) => paper.topics.length > 0).length ?? 0;
  const visibleStart = data && data.total > 0 ? data.resolved_filters.offset + 1 : 0;
  const visibleEnd = data ? data.resolved_filters.offset + data.items.length : 0;
  const canPrev = Boolean(data && data.resolved_filters.offset > 0);
  const canNext = Boolean(data && visibleEnd < data.total);
  const recommendedFamily = data?.resolved_filters.family_hint ?? familyHint ?? "emerging";
  const resolvedRankingRunId = data?.resolved_ranking_run_id ?? null;
  const resolvedRankingVersion = data?.resolved_ranking_version ?? null;
  const resolvedSnapshotVersion = data?.resolved_corpus_snapshot_version ?? null;
  const controlsKey = [
    q ?? "",
    includedScope,
    yearFrom ?? "",
    yearTo ?? "",
    sourceSlug ?? "",
    topic ?? "",
    familyHint ?? "",
    rankingVersion ?? "",
    rankingRunId ?? "",
    limit,
  ].join("|");

  return (
    <main className="page">
      <section className="panel page-hero family-hero family-hero-bridge">
        <div className="family-hero-grid">
          <div>
            <div className="panel-header">
              <div>
                <p className="eyebrow family-bridge">Search</p>
                <h1>Search the curated corpus with lexical retrieval first.</h1>
              </div>
              <div className="stamp-row">
                <span className="stamp">Title + abstract lexical search</span>
                <span className="stamp">Deterministic ordering</span>
              </div>
            </div>
            <p className="hero-lead">
              Search v1 is intentionally narrow: lexical retrieval over titles and abstracts, plus
              practical filters for narrowing the curated slice. Semantic assist can come later,
              but it is not part of this branch&apos;s contract.
            </p>
            {data ? (
              <div className="hero-metrics" aria-label="Search surface summary">
                <article className="metric-card">
                  <p className="metric-label">Total matches</p>
                  <p className="metric-value">{data.total}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Visible window</p>
                  <p className="metric-value">
                    {visibleStart}-{visibleEnd}
                  </p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Core papers shown</p>
                  <p className="metric-value">{coreCount}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Rows with topics</p>
                  <p className="metric-value">{topicTaggedCount}</p>
                </article>
              </div>
            ) : null}
            <p className="muted-inline">
              Search branch contract: dedicated <code>/api/v1/search</code>, lexical ordering, real
              filters, and clean handoff into dossier and ranking views. When ranking family
              filtering is active, the API resolves and returns one explicit run context.
            </p>
          </div>
          <aside className="family-brief">
            <div className="family-brief-diagram" aria-hidden="true">
              <span className="family-ring family-ring-a" />
              <span className="family-ring family-ring-b" />
              <span className="family-ring family-ring-c" />
              <span className="family-sweep family-sweep-bridge" />
              <span className="family-node family-node-bridge family-node-1" />
              <span className="family-node family-node-bridge family-node-2" />
              <span className="family-node family-node-bridge family-node-3" />
            </div>
            <div className="family-brief-copy">
              <p className="eyebrow family-bridge">Search contract</p>
              <h2>Lexical now, semantic later</h2>
              <ul className="measure-list">
                <li>Search title and abstract with deterministic lexical ordering.</li>
                <li>Use filters to narrow the curated slice before ranking handoff.</li>
                <li>Ranking family filtering resolves against one explicit succeeded run.</li>
                <li>When both run fields are supplied, exact run id wins over version label.</li>
                <li>Keep explanation honest: no hybrid claims until hybrid retrieval exists.</li>
              </ul>
            </div>
          </aside>
        </div>
      </section>

      <section className="split">
        <article className="panel instrument-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow eyebrow-muted">Query surface</p>
              <h2>Search controls</h2>
            </div>
          </div>
          <form key={controlsKey} className="search-form" method="get">
            <div className="search-filter-grid">
              <label className="search-field">
                <span>Query</span>
                <input
                  key={`query-${q ?? "empty"}`}
                  type="text"
                  name="q"
                  defaultValue={q ?? ""}
                  placeholder="audio embeddings, music retrieval, MIR surveys"
                />
              </label>
              <label className="search-field">
                <span>Included scope</span>
                <select name="included_scope" defaultValue={includedScope}>
                  <option value="all_included">All included</option>
                  <option value="core">Core only</option>
                </select>
              </label>
              <label className="search-field">
                <span>Year from</span>
                <input type="number" name="year_from" defaultValue={yearFrom ?? ""} />
              </label>
              <label className="search-field">
                <span>Year to</span>
                <input type="number" name="year_to" defaultValue={yearTo ?? ""} />
              </label>
              <label className="search-field">
                <span>Source slug</span>
                <input type="text" name="source_slug" defaultValue={sourceSlug ?? ""} />
              </label>
              <label className="search-field">
                <span>Topic label</span>
                <input type="text" name="topic" defaultValue={topic ?? ""} />
              </label>
              <label className="search-field">
                <span>Ranking family filter</span>
                <select name="family_hint" defaultValue={familyHint ?? ""}>
                  <option value="">None</option>
                  <option value="emerging">Emerging</option>
                  <option value="bridge">Bridge</option>
                  <option value="undercited">Under-cited</option>
                </select>
              </label>
              <label className="search-field">
                <span>Ranking version</span>
                <input type="text" name="ranking_version" defaultValue={rankingVersion ?? ""} />
              </label>
              <label className="search-field">
                <span>Ranking run id</span>
                <input type="text" name="ranking_run_id" defaultValue={rankingRunId ?? ""} />
              </label>
              <label className="search-field">
                <span>Limit</span>
                <input type="number" name="limit" min={1} max={100} defaultValue={limit} />
              </label>
            </div>
            <input type="hidden" name="offset" value="0" />
            <div className="action-row">
              <button className="action-link action-link-button" type="submit">
                Run lexical search
              </button>
              <Link className="action-link" href="/search">
                Reset filters
              </Link>
            </div>
            <p className="muted-inline">
              If both <code>ranking_run_id</code> and <code>ranking_version</code> are set, the
              exact run id takes precedence.
            </p>
          </form>
        </article>
        <article className="panel instrument-panel">
          <h2>What this branch ships</h2>
          <ul className="measure-list">
            <li>Lexical retrieval over <code>title + abstract</code>.</li>
            <li>Filters for year, scope, venue/source, topic label, and ranking family filter.</li>
            <li>Stable ordering: lexical rank, then year, citations, and work id.</li>
            <li>Run metadata appears only when the search depended on ranking state.</li>
          </ul>
        </article>
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Workflow map</p>
            <h2>How search moves through Research Radar</h2>
          </div>
        </div>
        <div className="workflow-grid">
          <article className="workflow-card">
            <p className="workflow-step">01</p>
            <h3>Find the right slice</h3>
            <p>
              Start with lexical retrieval and narrow with year, venue, topic, and scope filters
              until the candidate pool feels right.
            </p>
          </article>
          <article className="workflow-card">
            <p className="workflow-step">02</p>
            <h3>Inspect the dossier</h3>
            <p>
              Open the paper dossier to review abstract, metadata, ranking presence, and adjacent
              papers without losing the corpus framing.
            </p>
          </article>
          <article className="workflow-card">
            <p className="workflow-step">03</p>
            <h3>Move into signals</h3>
            <p>
              Use Recommended and Evaluation to understand why a result matters now and how it sits
              inside the exact resolved ranking run when family filtering is active.
            </p>
          </article>
        </div>
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Lexical results</p>
            <h2>{q ? `Results for “${q}”` : "Run a query to search the corpus"}</h2>
          </div>
          {data ? (
            <div className="stamp-row">
              <span className="stamp">Order: {data.ordering}</span>
              <span className="stamp">Total matches: {data.total}</span>
            </div>
          ) : null}
        </div>

        {searchResult.error ? <p>{searchResult.error}</p> : null}
        {!q ? (
          <>
            <p>
              Start with a concrete phrase from the literature, then narrow by topic or venue if
              the first pass is too broad. Search v1 only claims lexical retrieval today, so the
              surfaced rows should be interpretable from their text.
            </p>
            <p className="muted-inline">
              Retrieval is <strong>lexical</strong> over each paper&apos;s stored{" "}
              <strong>title and abstract</strong> (word/phrase match, deterministic ordering). It is
              not semantic vector search, hybrid retrieval, or LLM-ranked results.
            </p>
            <p className="muted-inline">Try a sample query (same filters as the form above):</p>
            <ul className="action-row search-sample-query-list" aria-label="Sample lexical searches">
              {REVIEWER_SAMPLE_QUERIES.map((sample) => (
                <li key={sample}>
                  <Link
                    className="action-link"
                    href={buildSearchHref(currentState, { q: sample, offset: 0 })}
                  >
                    {sample}
                  </Link>
                </li>
              ))}
            </ul>
          </>
        ) : null}
        {data ? (
          <>
            <p className="muted-inline">
              Resolved filters: scope <code>{data.resolved_filters.included_scope}</code>
              {data.resolved_filters.year_from != null ? (
                <>
                  {" "}
                  | year from <code>{data.resolved_filters.year_from}</code>
                </>
              ) : null}
              {data.resolved_filters.year_to != null ? (
                <>
                  {" "}
                  | year to <code>{data.resolved_filters.year_to}</code>
                </>
              ) : null}
              {data.resolved_filters.source_slug ? (
                <>
                  {" "}
                  | source <code>{data.resolved_filters.source_slug}</code>
                </>
              ) : null}
              {data.resolved_filters.topic ? (
                <>
                  {" "}
                  | topic <code>{data.resolved_filters.topic}</code>
                </>
              ) : null}
              {data.resolved_filters.family_hint ? (
                <>
                  {" "}
                  | ranking family filter <code>{data.resolved_filters.family_hint}</code>
                </>
              ) : null}
              {resolvedRankingRunId ? (
                <>
                  {" "}
                  | run <code>{resolvedRankingRunId}</code>
                </>
              ) : null}
              {resolvedRankingVersion ? (
                <>
                  {" "}
                  | version <code>{resolvedRankingVersion}</code>
                </>
              ) : null}
              {resolvedSnapshotVersion ? (
                <>
                  {" "}
                  | snapshot <code>{resolvedSnapshotVersion}</code>
                </>
              ) : null}
            </p>

            {data.items.length === 0 ? <p>No included papers matched this lexical query.</p> : null}

            {data.items.length > 0 ? (
              <ul className="result-list">
                {data.items.map((paper) => (
                  <li key={paper.paper_id} className="result-item result-item-bridge">
                    <div className="result-heading">
                      <p className="result-title">
                        <Link href={buildPaperHref(paper.paper_id, resolvedRankingRunId)}>
                          {paper.title}
                        </Link>
                      </p>
                      <span className="result-score result-score-bridge">
                        lex {paper.match.lexical_rank.toFixed(3)}
                      </span>
                    </div>
                    <p className="result-meta">
                      {paper.year} | cites: {paper.citation_count} |{" "}
                      {paper.source_label ?? paper.source_slug ?? "unknown venue"}
                    </p>
                    <div className="stamp-row stamp-row-inline">
                      <span className="stamp">{paper.is_core_corpus ? "core" : "included"}</span>
                      <span className="stamp">
                        matched: {paper.match.matched_fields.join(", ") || "n/a"}
                      </span>
                    </div>
                    {paper.preview ? <p className="search-result-preview">{paper.preview}</p> : null}
                    {paper.match.highlight_fragments.length > 0 ? (
                      <ul className="measure-list search-highlights">
                        {paper.match.highlight_fragments.map((fragment) => (
                          <li key={fragment}>{fragment}</li>
                        ))}
                      </ul>
                    ) : null}
                    {paper.topics.length > 0 ? (
                      <div className="chip-row" aria-label="Topics">
                        {paper.topics.map((resultTopic) => (
                          <span key={resultTopic} className="chip">
                            {resultTopic}
                          </span>
                        ))}
                      </div>
                    ) : null}
                    <div className="action-row" aria-label="Related views">
                      <Link
                        className="action-link"
                        href={buildPaperHref(paper.paper_id, resolvedRankingRunId)}
                      >
                        Open dossier
                      </Link>
                      <Link
                        className="action-link"
                        href={buildRankingViewHref(
                          "/recommended",
                          recommendedFamily,
                          resolvedRankingRunId
                        )}
                      >
                        {familyLabel(recommendedFamily)} feed
                      </Link>
                      <Link
                        className="action-link"
                        href={buildRankingViewHref(
                          "/evaluation",
                          recommendedFamily,
                          resolvedRankingRunId
                        )}
                      >
                        Compare in evaluation
                      </Link>
                    </div>
                  </li>
                ))}
              </ul>
            ) : null}

            {data.total > data.resolved_filters.limit ? (
              <div className="search-pagination">
                <span className="muted-inline">
                  Showing {visibleStart}-{visibleEnd} of {data.total}
                </span>
                <div className="action-row">
                  {canPrev ? (
                    <Link
                      className="action-link"
                      href={buildSearchHref(currentState, {
                        offset: Math.max(0, offset - limit)
                      })}
                    >
                      Previous page
                    </Link>
                  ) : null}
                  {canNext ? (
                    <Link
                      className="action-link"
                      href={buildSearchHref(currentState, {
                        offset: offset + limit
                      })}
                    >
                      Next page
                    </Link>
                  ) : null}
                </div>
              </div>
            ) : null}
          </>
        ) : null}
      </section>
    </main>
  );
}

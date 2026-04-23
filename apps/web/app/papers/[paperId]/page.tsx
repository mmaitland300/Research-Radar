import Link from "next/link";

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

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

const EMBEDDING_VERSION =
  process.env.NEXT_PUBLIC_EMBEDDING_VERSION?.trim() || undefined;

type SimilarPapersState =
  | { kind: "disabled" }
  | { kind: "ok"; data: SimilarPapersResponse }
  | { kind: "not_found"; message: string }
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

export default async function PaperDetailPage({
  params
}: {
  params: { paperId: string };
}) {
  const canonicalPaperId = decodeURIComponent(params.paperId);
  const { paper, error } = await fetchPaperDetail(canonicalPaperId);

  const similar: SimilarPapersState | null =
    paper && !error ? await fetchSimilarPapers(canonicalPaperId) : null;
  const similarCount = similar?.kind === "ok" ? similar.data.items.length : 0;

  return (
    <main className="page">
      <section className="panel page-hero family-hero family-hero-emerging">
        <div className="family-hero-grid">
          <div>
            <div className="panel-header">
              <div>
                <p className="eyebrow family-emerging">Paper dossier</p>
                <h1>{paper?.title ?? "Paper detail"}</h1>
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
                  <Link className="action-link" href="/recommended?family=emerging">
                    Check emerging feed
                  </Link>
                  <Link className="action-link" href="/recommended?family=bridge">
                    Check bridge feed
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
                <li>Use topic labels to understand neighborhood placement.</li>
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
              Set <code>NEXT_PUBLIC_EMBEDDING_VERSION</code> to enable
              embedding-backed similar papers (e.g.{" "}
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
                        href={`/papers/${encodeURIComponent(item.paper_id)}`}
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
                    <Link className="action-link" href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                      Open neighbor dossier
                    </Link>
                    <Link className="action-link" href="/recommended?family=emerging">
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
              Use Recommended to see whether this paper behaves like an emerging, bridge, or
              undercited signal in the current ranked feed.
            </p>
            <div className="action-row">
              <Link className="action-link" href="/recommended?family=emerging">
                Emerging
              </Link>
              <Link className="action-link" href="/recommended?family=bridge">
                Bridge
              </Link>
              <Link className="action-link" href="/recommended?family=undercited">
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
            <h3>Add signal explanations</h3>
            <p>
              This page is now ready for a deeper explainability pass with run-specific score
              contributions and family-level annotations.
            </p>
            <div className="action-row">
              <Link className="action-link" href="/evaluation?family=emerging">
                Open evaluation
              </Link>
            </div>
          </article>
        </div>
      </section>
    </main>
  );
}

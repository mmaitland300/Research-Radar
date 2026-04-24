import Link from "next/link";

type PaperListItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  source_label: string | null;
  is_core_corpus: boolean;
  topics: string[];
};

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

async function fetchPapers(): Promise<{
  items: PaperListItem[];
  error: string | null;
}> {
  try {
    const response = await fetch(`${API_BASE_URL}/api/v1/papers?limit=10`, {
      cache: "no-store"
    });
    if (!response.ok) {
      return {
        items: [],
        error: `API responded with ${response.status}.`
      };
    }
    const payload = (await response.json()) as { items?: PaperListItem[] };
    return { items: payload.items ?? [], error: null };
  } catch {
    return {
      items: [],
      error: "Could not reach the API. Start apps/api and Postgres, then refresh."
    };
  }
}

export default async function SearchPage() {
  const { items, error } = await fetchPapers();
  const coreCount = items.filter((paper) => paper.is_core_corpus).length;
  const topicTaggedCount = items.filter((paper) => paper.topics.length > 0).length;
  const newestYear = items.reduce((max, paper) => Math.max(max, paper.year), 0);

  return (
    <main className="page">
      <section className="panel page-hero family-hero family-hero-bridge">
        <div className="family-hero-grid">
          <div>
            <div className="panel-header">
              <div>
                <p className="eyebrow family-bridge">Search</p>
                <h1>Query papers, authors, topics, and clusters.</h1>
              </div>
              <div className="stamp-row">
                <span className="stamp">Lexical + embedding retrieval</span>
                <span className="stamp">Curated slice first</span>
              </div>
            </div>
            <p className="hero-lead">
              The first searchable surface will prioritize relevance and filter
              control over breadth. Users should be able to narrow by year, venue,
              cluster, and score family without feeling like they are browsing a
              generic scholar clone.
            </p>
            {items.length > 0 ? (
              <div className="hero-metrics" aria-label="Search surface summary">
                <article className="metric-card">
                  <p className="metric-label">Rows loaded</p>
                  <p className="metric-value">{items.length}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Core corpus rows</p>
                  <p className="metric-value">{coreCount}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Tagged with topics</p>
                  <p className="metric-value">{topicTaggedCount}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Newest year</p>
                  <p className="metric-value">{newestYear || "n/a"}</p>
                </article>
              </div>
            ) : null}
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
              <h2>Search should hand off, not dead-end</h2>
              <ul className="measure-list">
                <li>Move from retrieval into recommendation families.</li>
                <li>Keep cluster and topic labels visible as navigational hints.</li>
                <li>Use detail pages as the bridge into similarity and explainability.</li>
              </ul>
            </div>
          </aside>
        </div>
      </section>

      <section className="split">
        <article className="panel instrument-panel">
          <h2>Primary filters</h2>
          <ul className="measure-list">
            <li>Year range</li>
            <li>Venue class and venue</li>
            <li>Cluster and topic labels</li>
            <li>Recommendation family</li>
          </ul>
        </article>
        <article className="panel instrument-panel">
          <h2>Search contract</h2>
          <p>
            Search should use lexical matching plus embedding retrieval, then
            hand off to ranking views instead of trying to solve every use case
            in a single results page.
          </p>
        </article>
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Workflow map</p>
            <h2>How search should move through the product</h2>
          </div>
        </div>
        <div className="workflow-grid">
          <article className="workflow-card">
            <p className="workflow-step">01</p>
            <h3>Find the slice</h3>
            <p>
              Use venue, year, cluster, and topic filters to get the candidate pool into the right
              neighborhood before ranking takes over.
            </p>
          </article>
          <article className="workflow-card">
            <p className="workflow-step">02</p>
            <h3>Open the dossier</h3>
            <p>
              Jump into paper detail to inspect source, abstract, authors, and similarity context
              without losing the curated-corpus framing.
            </p>
          </article>
          <article className="workflow-card">
            <p className="workflow-step">03</p>
            <h3>Move into signals</h3>
            <p>
              Continue into Recommended, Trends, and Evaluation to understand why the paper matters
              now and where it fits in the corpus.
            </p>
          </article>
        </div>
      </section>

      <section className="panel section-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow eyebrow-muted">Corpus readout</p>
            <h2>Latest corpus papers (DB-backed)</h2>
          </div>
          <div className="stamp-row">
            <span className="stamp">Live API surface</span>
            <span className="stamp">10-row sample</span>
          </div>
        </div>
        {error ? <p>{error}</p> : null}
        {!error && items.length === 0 ? <p>No included papers found yet.</p> : null}
        {items.length > 0 ? (
          <ul className="result-list">
            {items.map((paper) => (
              <li key={paper.paper_id} className="result-item result-item-bridge">
                <div className="result-heading">
                  <p className="result-title">
                    <Link href={`/papers/${encodeURIComponent(paper.paper_id)}`}>
                      {paper.title}
                    </Link>
                  </p>
                  <span className={`result-score ${paper.is_core_corpus ? "result-score-bridge" : "result-score-neutral"}`}>
                    {paper.is_core_corpus ? "core" : "edge"}
                  </span>
                </div>
                <p className="result-meta">
                  {paper.year} | cites: {paper.citation_count} |{" "}
                  {paper.source_label ?? paper.source_slug ?? "unknown venue"}
                </p>
                <div className="stamp-row stamp-row-inline">
                  <span className="stamp">Paper detail</span>
                  <span className="stamp">Similarity path</span>
                </div>
                <p className="result-reason">
                  <Link href={`/papers/${encodeURIComponent(paper.paper_id)}`}>
                    Open detail view and move from retrieval into recommendation
                    and similarity context.
                  </Link>
                </p>
                <div className="action-row" aria-label="Related views">
                  <Link className="action-link" href={`/papers/${encodeURIComponent(paper.paper_id)}`}>
                    Open dossier
                  </Link>
                  <Link className="action-link" href="/recommended?family=emerging">
                    Emerging feed
                  </Link>
                  <Link className="action-link" href="/trends">
                    Topic momentum
                  </Link>
                </div>
                {paper.topics.length > 0 ? (
                  <p className="chip-row" aria-label="Topics">
                    {paper.topics.map((topic) => (
                      <span key={topic} className="chip">
                        {topic}
                      </span>
                    ))}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        ) : null}
      </section>
    </main>
  );
}

type PaperListItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  is_core_corpus: boolean;
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

  return (
    <main className="page">
      <section className="panel">
        <p className="accent-2">Search</p>
        <h1>Query papers, authors, topics, and clusters.</h1>
        <p>
          The first searchable surface will prioritize relevance and filter
          control over breadth. Users should be able to narrow by year, venue,
          cluster, and score family without feeling like they are browsing a
          generic scholar clone.
        </p>
      </section>

      <section className="split">
        <article className="panel">
          <h2>Primary filters</h2>
          <ul>
            <li>Year range</li>
            <li>Venue class and venue</li>
            <li>Cluster and topic labels</li>
            <li>Recommendation family</li>
          </ul>
        </article>
        <article className="panel">
          <h2>Search contract</h2>
          <p>
            Search should use lexical matching plus embedding retrieval, then
            hand off to ranking views instead of trying to solve every use case
            in a single results page.
          </p>
        </article>
      </section>

      <section className="panel">
        <h2>Latest corpus papers (DB-backed)</h2>
        {error ? <p>{error}</p> : null}
        {!error && items.length === 0 ? <p>No included papers found yet.</p> : null}
        {items.length > 0 ? (
          <ul className="result-list">
            {items.map((paper) => (
              <li key={paper.paper_id} className="result-item">
                <p className="result-title">{paper.title}</p>
                <p className="result-meta">
                  {paper.year} | cites: {paper.citation_count} |{" "}
                  {paper.source_slug ?? "unknown venue"} |{" "}
                  {paper.is_core_corpus ? "core" : "edge"}
                </p>
              </li>
            ))}
          </ul>
        ) : null}
      </section>
    </main>
  );
}

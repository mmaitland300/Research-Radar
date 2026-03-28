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

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

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

export default async function PaperDetailPage({
  params
}: {
  params: { paperId: string };
}) {
  const canonicalPaperId = decodeURIComponent(params.paperId);
  const { paper, error } = await fetchPaperDetail(canonicalPaperId);

  return (
    <main className="page">
      <section className="panel">
        <p className="accent">Paper Detail</p>
        <h1>{paper?.title ?? "Paper detail"}</h1>
        {error ? <p>{error}</p> : null}
        {!error && !paper ? <p>No detail was returned for this paper id.</p> : null}
        <div className="meta">
          <span>Paper ID: {canonicalPaperId}</span>
          {paper ? <span>{paper.year}</span> : null}
          {paper ? <span>Citations: {paper.citation_count}</span> : null}
          {paper ? <span>{paper.is_core_corpus ? "core" : "edge"}</span> : null}
        </div>
        {paper ? (
          <div className="source-block">
            <h2 className="source-block-heading">Source</h2>
            <p className="source-block-primary">{paper.venue ?? "Unknown venue"}</p>
            <p className="source-block-secondary">
              Slug: <strong>{paper.source_slug ?? "unknown"}</strong>
            </p>
          </div>
        ) : null}
      </section>

      <section className="split">
        <article className="panel">
          <h2>Abstract</h2>
          <p>{paper?.abstract || "No abstract available."}</p>
        </article>
        <article className="panel">
          <h2>Authors</h2>
          {paper && paper.authors.length > 0 ? (
            <ul>
              {paper.authors.map((author) => (
                <li key={author}>{author}</li>
              ))}
            </ul>
          ) : (
            <p>No authors available.</p>
          )}
        </article>
      </section>

      <section className="panel">
        <h2>Topics</h2>
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

      <section className="panel">
        <h2>Next explainability step</h2>
        <p>
          This page now serves real metadata from Postgres. Next, attach ranking
          run context and per-signal contributions.
        </p>
      </section>
    </main>
  );
}

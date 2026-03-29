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

      {similar ? (
        <section className="panel">
          <h2>Similar papers</h2>
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
                <li key={item.paper_id} className="result-item">
                  <p className="result-title">
                    <Link
                      href={`/papers/${encodeURIComponent(item.paper_id)}`}
                    >
                      {item.title}
                    </Link>
                  </p>
                  <p className="result-meta">
                    {item.year} | cites: {item.citation_count} |{" "}
                    {item.source_slug ?? "unknown venue"} | similarity:{" "}
                    {item.similarity.toFixed(4)}
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
                </li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}

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

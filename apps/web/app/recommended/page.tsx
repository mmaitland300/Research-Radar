import Link from "next/link";

type UndercitedItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  reason: string;
  signal_breakdown: Record<string, number>;
};

type UndercitedResponse = {
  heuristic_label: string;
  heuristic_version: string;
  description: string;
  total: number;
  items: UndercitedItem[];
};

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

async function fetchUndercited(): Promise<{
  data: UndercitedResponse | null;
  error: string | null;
}> {
  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/recommendations/undercited?limit=12`,
      { cache: "no-store" }
    );
    if (!response.ok) {
      return {
        data: null,
        error: `API responded with ${response.status}.`
      };
    }
    const data = (await response.json()) as UndercitedResponse;
    return { data, error: null };
  } catch {
    return {
      data: null,
      error: "Could not reach the API. Start apps/api and Postgres, then refresh."
    };
  }
}

export default async function RecommendedPage() {
  const { data, error } = await fetchUndercited();

  return (
    <main className="page">
      <section className="panel">
        <p className="accent">Recommended</p>
        <h1>Heuristic v0: under-cited core papers</h1>
        <p>
          This feed is a deliberate <strong>rule-based baseline</strong>, not a trained
          ranking model. It surfaces recent included core-corpus work with low citation
          counts and minimum metadata quality so you can demo recommendations without
          overclaiming.
        </p>
        {data ? (
          <p className="muted-inline">
            <strong>{data.heuristic_label}</strong> ({data.heuristic_version}) ·{" "}
            {data.total} match{data.total === 1 ? "" : "es"}
          </p>
        ) : null}
      </section>

      {error ? (
        <section className="panel">
          <p>{error}</p>
        </section>
      ) : null}

      {data && !error ? (
        <section className="panel">
          <h2>Live results</h2>
          <p>{data.description}</p>
          {data.items.length === 0 ? (
            <p>No papers matched the heuristic filters yet.</p>
          ) : (
            <ul className="result-list">
              {data.items.map((item) => (
                <li key={item.paper_id} className="result-item">
                  <p className="result-title">
                    <Link href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                      {item.title}
                    </Link>
                  </p>
                  <p className="result-meta">
                    {item.year} | cites: {item.citation_count} |{" "}
                    {item.source_slug ?? "unknown venue"}
                  </p>
                  <p className="result-reason">{item.reason}</p>
                  <p className="result-breakdown">
                    Signals:{" "}
                    {Object.entries(item.signal_breakdown)
                      .map(([k, v]) => `${k}=${v}`)
                      .join(", ")}
                  </p>
                </li>
              ))}
            </ul>
          )}
        </section>
      ) : null}

      <section className="grid">
        <article className="card">
          <h2>Emerging</h2>
          <p>Planned: growth and velocity signals once ranking runs write paper_scores.</p>
        </article>
        <article className="card">
          <h2>Bridge</h2>
          <p>Planned: cluster-aware bridge score from embeddings and graph context.</p>
        </article>
        <article className="card">
          <h2>Under-cited (this page)</h2>
          <p>
            Shipped as heuristic v0 above. Next step: recency-normalized citation velocity,
            then combined signals into paper_scores.
          </p>
        </article>
      </section>
    </main>
  );
}

import Link from "next/link";

const FAMILIES = ["emerging", "bridge", "undercited"] as const;
type Family = (typeof FAMILIES)[number];

const FAMILY_LABEL: Record<Family, string> = {
  emerging: "Emerging",
  bridge: "Bridge",
  undercited: "Under-cited"
};

const API_BASE_URL =
  process.env.API_BASE_URL ?? process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

const RANKING_VERSION = process.env.NEXT_PUBLIC_RANKING_VERSION?.trim() || undefined;

type EvalPaper = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  topics: string[];
  final_score: number | null;
};

type EvalArm = {
  arm_label: string;
  arm_description: string;
  ordering_description: string;
  items: EvalPaper[];
  recency: {
    mean_year: number;
    min_year: number;
    max_year: number;
    share_in_latest_two_years: number;
  };
  citations: {
    mean: number;
    median: number;
    min_val: number;
    max_val: number;
  };
  topics: {
    unique_topic_labels: number;
    top_topics: string[];
  };
};

type EvalCompareResponse = {
  disclaimer: { headline: string; bullets: string[] };
  ranking_run_id: string;
  ranking_version: string;
  corpus_snapshot_version: string;
  embedding_version: string;
  family: string;
  pool_definition: string;
  pool_size: number;
  low_cite_min_year: number | null;
  low_cite_max_citations: number | null;
  candidate_pool_doc_revision: string | null;
  topic_overlap_note: string;
  ranked: EvalArm;
  citation_baseline: EvalArm;
  date_baseline: EvalArm;
  topic_overlap: {
    jaccard_ranked_vs_citation_baseline: number;
    jaccard_ranked_vs_date_baseline: number;
    jaccard_citation_vs_date_baseline: number;
  };
  generated_at: string;
};

function fmtFixed(value: number, digits: number): string {
  return value.toFixed(digits);
}

function fmtPercent01(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function parseFamily(raw: string | string[] | undefined): Family {
  const v = Array.isArray(raw) ? raw[0] : raw;
  if (v && (FAMILIES as readonly string[]).includes(v)) {
    return v as Family;
  }
  return "emerging";
}

async function fetchCompare(family: Family): Promise<{
  data: EvalCompareResponse | null;
  error: string | null;
  status: number | null;
}> {
  const params = new URLSearchParams({ family, limit: "12" });
  if (RANKING_VERSION) params.set("ranking_version", RANKING_VERSION);

  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/evaluation/compare?${params.toString()}`,
      { cache: "no-store" }
    );
    if (response.status === 404) {
      return {
        data: null,
        error:
          "No succeeded ranking run found. Run the pipeline ranking job, or set NEXT_PUBLIC_RANKING_VERSION to match a run.",
        status: 404
      };
    }
    if (!response.ok) {
      return {
        data: null,
        error: `API returned ${response.status} for /api/v1/evaluation/compare`,
        status: response.status
      };
    }
    const data = (await response.json()) as EvalCompareResponse;
    return { data, error: null, status: 200 };
  } catch (e) {
    return {
      data: null,
      error: e instanceof Error ? e.message : "Unknown error",
      status: null
    };
  }
}

function ArmProxyStats({ arm }: { arm: EvalArm }) {
  return (
    <div className="eval-proxy">
      <p className="eval-proxy-title">Proxy stats (list-only; not relevance)</p>
      <dl className="eval-dl">
        <dt>Recency</dt>
        <dd>
          mean year {fmtFixed(arm.recency.mean_year, 2)}; min-max {arm.recency.min_year}-
          {arm.recency.max_year}; share in latest two years{" "}
          {fmtPercent01(arm.recency.share_in_latest_two_years)}
        </dd>
        <dt>Citations</dt>
        <dd>
          mean {fmtFixed(arm.citations.mean, 2)}; median {fmtFixed(arm.citations.median, 2)};
          range {arm.citations.min_val}-{arm.citations.max_val}
        </dd>
        <dt>Topic mix</dt>
        <dd>
          {arm.topics.unique_topic_labels} unique labels in list; top:{" "}
          {arm.topics.top_topics.length ? arm.topics.top_topics.join(", ") : "-"}
        </dd>
      </dl>
    </div>
  );
}

function ArmColumn({ title, arm }: { title: string; arm: EvalArm }) {
  return (
    <article className="panel eval-arm instrument-panel">
      <div className="result-heading">
        <h2>{title}</h2>
        <span className="stamp">List size {arm.items.length}</span>
      </div>
      <p className="muted-inline">{arm.arm_description}</p>
      <p className="muted-inline">
        <strong>Order:</strong> {arm.ordering_description}
      </p>
      <div className="hero-metrics hero-metrics-compact" aria-label={`${title} proxy summary`}>
        <article className="metric-card">
          <p className="metric-label">Mean year</p>
          <p className="metric-value">{fmtFixed(arm.recency.mean_year, 1)}</p>
        </article>
        <article className="metric-card">
          <p className="metric-label">Median cites</p>
          <p className="metric-value">{fmtFixed(arm.citations.median, 1)}</p>
        </article>
        <article className="metric-card">
          <p className="metric-label">Unique topics</p>
          <p className="metric-value">{arm.topics.unique_topic_labels}</p>
        </article>
      </div>
      <ArmProxyStats arm={arm} />
      <ul className="result-list">
        {arm.items.length === 0 ? (
          <li className="result-item">No papers in this slice.</li>
        ) : (
          arm.items.map((item) => (
            <li key={item.paper_id} className="result-item result-item-bridge">
              <div className="result-heading">
                <p className="result-title">
                  <Link href={`/papers/${encodeURIComponent(item.paper_id)}`}>{item.title}</Link>
                </p>
                {item.final_score != null ? (
                  <span className="result-score result-score-bridge">
                    {item.final_score.toFixed(3)}
                  </span>
                ) : null}
              </div>
              <p className="result-meta">
                {item.year} | cites: {item.citation_count} | {item.source_slug ?? "-"}
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
          ))
        )}
      </ul>
    </article>
  );
}

type PageProps = {
  searchParams: Record<string, string | string[] | undefined>;
};

export default async function EvaluationPage({ searchParams }: PageProps) {
  const family = parseFamily(searchParams.family);
  const { data, error, status } = await fetchCompare(family);

  return (
    <main className={`page page-family page-family-${family}`}>
      <section className="panel page-hero">
        <div className="panel-header">
          <div>
            <p className={`eyebrow family-${family}`}>Evaluation</p>
            <h1>Evidence v0: ranked feed vs naive baselines</h1>
          </div>
          <div className="stamp-row">
            <span className={`stamp stamp-family stamp-family-${family}`}>
              {FAMILY_LABEL[family]} family
            </span>
            <span className="stamp">Distributional checks only</span>
          </div>
        </div>
        <p className="hero-lead">
          This page calls <code>GET /api/v1/evaluation/compare</code> so you can inspect the same
          candidate pool under three orderings: materialized ranking, citation-sorted, and
          date-sorted. Nothing here claims human relevance - only distributional checks on short
          lists.
        </p>
        {data ? (
          <div className="hero-metrics" aria-label="Evaluation summary">
            <article className="metric-card">
              <p className="metric-label">Pool size</p>
              <p className="metric-value">{data.pool_size}</p>
            </article>
            <article className="metric-card">
              <p className="metric-label">Run label</p>
              <p className="metric-value metric-value-mono">{data.ranking_version}</p>
            </article>
            <article className="metric-card">
              <p className="metric-label">Embedding</p>
              <p className="metric-value metric-value-mono">{data.embedding_version}</p>
            </article>
            <article className="metric-card">
              <p className="metric-label">Generated at</p>
              <p className="metric-value metric-value-mono">{data.generated_at.slice(0, 10)}</p>
            </article>
          </div>
        ) : null}
        <nav className="tabs" aria-label="Recommendation family">
          {FAMILIES.map((f) => (
            <Link
              key={f}
              href={`/evaluation?family=${f}`}
              aria-current={f === family ? "page" : undefined}
            >
              {FAMILY_LABEL[f]}
            </Link>
          ))}
        </nav>
        {RANKING_VERSION ? (
          <p className="muted-inline">
            Pin: <code>NEXT_PUBLIC_RANKING_VERSION={RANKING_VERSION}</code>
          </p>
        ) : (
          <p className="muted-inline">
            Using latest succeeded run for the default snapshot (set{" "}
            <code>NEXT_PUBLIC_RANKING_VERSION</code> to pin).
          </p>
        )}
      </section>

      {error ? (
        <section className="panel instrument-panel">
          <h2>Compare unavailable</h2>
          <p>{error}</p>
          {status === 404 ? (
            <p className="muted-inline">
              Run <code>ranking-run</code> against Postgres, or align{" "}
              <code>NEXT_PUBLIC_RANKING_VERSION</code> with an existing label.
            </p>
          ) : null}
        </section>
      ) : null}

      {data ? (
        <>
          <section className="panel eval-disclaimer section-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow eyebrow-muted">Interpretation guardrails</p>
                <h2>Disclaimer</h2>
              </div>
            </div>
            <p className="result-title">{data.disclaimer.headline}</p>
            <ul>
              {data.disclaimer.bullets.map((b, i) => (
                <li key={i}>{b}</li>
              ))}
            </ul>
          </section>

          <section className="panel section-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow eyebrow-muted">Run context</p>
                <h2>Run and pool</h2>
              </div>
            </div>
            <p className="muted-inline">
              Run <code>{data.ranking_version}</code> |{" "}
              <code>{data.ranking_run_id}</code> | snapshot <code>{data.corpus_snapshot_version}</code>{" "}
              | embedding <code>{data.embedding_version}</code> | pool size <strong>{data.pool_size}</strong>
            </p>
            <p className="muted-inline">{data.pool_definition}</p>
            {data.family === "undercited" ? (
              <p className="muted-inline">
                Low-cite gate from run config: year at least {data.low_cite_min_year}, citations at most
                {data.low_cite_max_citations} (revision {data.candidate_pool_doc_revision ?? "v0"}).
                Frozen definition: <code>docs/candidate-pool-low-cite.md</code>.
              </p>
            ) : null}
          </section>

          <section className="panel section-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow eyebrow-muted">List overlap</p>
                <h2>Topic label overlap between lists</h2>
              </div>
            </div>
            <p className="muted-inline">{data.topic_overlap_note}</p>
            <dl className="eval-dl">
              <dt>Ranked vs citation baseline</dt>
              <dd>{fmtFixed(data.topic_overlap.jaccard_ranked_vs_citation_baseline, 4)}</dd>
              <dt>Ranked vs date baseline</dt>
              <dd>{fmtFixed(data.topic_overlap.jaccard_ranked_vs_date_baseline, 4)}</dd>
              <dt>Citation vs date baseline</dt>
              <dd>{fmtFixed(data.topic_overlap.jaccard_citation_vs_date_baseline, 4)}</dd>
            </dl>
          </section>

          <div className="eval-arms">
            <ArmColumn title="Ranked (family)" arm={data.ranked} />
            <ArmColumn title="Citation baseline" arm={data.citation_baseline} />
            <ArmColumn title="Date baseline" arm={data.date_baseline} />
          </div>

          <p className="muted-inline">
            Generated at {data.generated_at} | Later work: labeled benchmarks, P@k, freeze-at-T
            backtests - see product checklist in API <code>/api/v1/evaluation/summary</code>.
          </p>
        </>
      ) : null}
    </main>
  );
}

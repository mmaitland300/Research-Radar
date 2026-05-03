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

type BridgeDistinctnessNextStep =
  | "inspect_cluster_quality_first"
  | "eligible_filter_not_distinct_enough"
  | "candidate_for_small_weight_experiment"
  | "insufficient_bridge_signal_coverage";

type BridgeDistinctnessOverlapMetrics = {
  overlap_count: number;
  jaccard: number;
};

type BridgeDistinctnessDecisionSupport = {
  eligible_head_differs_from_full: boolean;
  eligible_head_less_emerging_like_than_full: boolean;
  suggested_next_step: BridgeDistinctnessNextStep;
};

/** Response from GET /api/v1/evaluation/bridge-distinctness */
type BridgeDistinctnessResponse = {
  ranking_run_id: string;
  ranking_version: string;
  corpus_snapshot_version: string;
  embedding_version: string;
  cluster_version: string | null;
  k: number;
  full_bridge_top_k_ids: string[];
  eligible_bridge_top_k_ids: string[];
  emerging_top_k_ids: string[];
  full_bridge_vs_eligible_bridge: BridgeDistinctnessOverlapMetrics;
  full_bridge_vs_emerging: BridgeDistinctnessOverlapMetrics;
  eligible_bridge_vs_emerging: BridgeDistinctnessOverlapMetrics;
  bridge_family_row_count: number;
  bridge_score_nonnull_count: number;
  bridge_score_null_count: number;
  bridge_eligible_true_count: number;
  bridge_eligible_false_count: number;
  bridge_eligible_null_count: number;
  bridge_signal_json_present_count: number;
  bridge_signal_json_missing_count: number;
  decision_support: BridgeDistinctnessDecisionSupport;
  generated_at: string;
};

type BridgeDistinctnessFetchResult =
  | { kind: "ok"; data: BridgeDistinctnessResponse }
  | { kind: "error"; message: string; status: number | null; detail: string | null };

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

function parseSingleParam(raw: string | string[] | undefined): string | undefined {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function parseLimit(raw: string | string[] | undefined, fallback: number, max: number): number {
  const value = parseSingleParam(raw);
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(max, Math.trunc(parsed)));
}

function paperAnchorId(paperId: string): string {
  return `paper-${encodeURIComponent(paperId)}`;
}

function armFocusRowId(armKey: string, paperId: string): string {
  return `${armKey}-${paperAnchorId(paperId)}`;
}

function buildEvaluationFamilyHref(
  family: Family,
  options: {
    focusPaperId?: string;
    rankingRunId?: string;
    limit?: number;
  }
): string {
  const params = new URLSearchParams({ family });
  if (options.focusPaperId) params.set("paper", options.focusPaperId);
  if (options.rankingRunId) params.set("ranking_run_id", options.rankingRunId);
  if (options.limit != null) params.set("limit", String(options.limit));
  return `/evaluation?${params.toString()}`;
}

async function parseResponseErrorDetail(response: Response): Promise<string | null> {
  try {
    const text = await response.text();
    if (!text) return null;
    const j = JSON.parse(text) as { detail?: unknown };
    if (typeof j.detail === "string") return j.detail;
    if (Array.isArray(j.detail)) {
      return j.detail
        .map((x: { msg?: string } | string) => (typeof x === "object" && x && "msg" in x ? x.msg : String(x)))
        .filter(Boolean)
        .join("; ");
    }
  } catch {
    /* ignore */
  }
  return null;
}

/**
 * Pinned to a concrete ranking_run_id only. Do not call with ranking_version or env-only resolution.
 */
async function fetchBridgeDistinctness(
  rankingRunId: string,
  k: number
): Promise<BridgeDistinctnessFetchResult> {
  const params = new URLSearchParams({
    ranking_run_id: rankingRunId,
    k: String(k)
  });
  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/evaluation/bridge-distinctness?${params.toString()}`,
      { cache: "no-store" }
    );
    if (!response.ok) {
      const detail = await parseResponseErrorDetail(response);
      return {
        kind: "error",
        message: `API returned ${response.status} for /api/v1/evaluation/bridge-distinctness`,
        status: response.status,
        detail
      };
    }
    const payload = (await response.json()) as BridgeDistinctnessResponse;
    return { kind: "ok", data: payload };
  } catch (e) {
    return {
      kind: "error",
      message: e instanceof Error ? e.message : "Unknown error",
      status: null,
      detail: null
    };
  }
}

function boolLabel(v: boolean): string {
  return v ? "Yes" : "No";
}

function machineLabel(value: string): string {
  return value.replaceAll("_", " ");
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

function ArmColumn({
  title,
  arm,
  armKey,
  focusedPaperId
}: {
  title: string;
  arm: EvalArm;
  armKey: string;
  focusedPaperId?: string;
}) {
  const focusedItem = focusedPaperId
    ? arm.items.find((item) => item.paper_id === focusedPaperId) ?? null
    : null;
  return (
    <article className="panel eval-arm instrument-panel">
      <div className="result-heading">
        <h2>{title}</h2>
        <span className="stamp">List size {arm.items.length}</span>
      </div>
      {focusedPaperId ? (
        <p className="muted-inline">
          Focus paper: <code>{focusedPaperId}</code>
          {focusedItem ? " appears in this arm." : " is not visible in this arm."}
        </p>
      ) : null}
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
            <li
              key={item.paper_id}
              id={focusedPaperId === item.paper_id ? armFocusRowId(armKey, item.paper_id) : undefined}
              className={`result-item result-item-bridge${
                focusedPaperId === item.paper_id ? " result-item-focus" : ""
              }`}
            >
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
              {focusedPaperId === item.paper_id ? (
                <div className="stamp-row stamp-row-inline">
                  <span className="stamp">Focus paper</span>
                </div>
              ) : null}
              <div className="action-row" aria-label="Evaluation handoff">
                <Link className="action-link" href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                  Open dossier
                </Link>
                <Link className="action-link" href="/trends">
                  Topic momentum
                </Link>
              </div>
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
  const focusPaperId = parseSingleParam(searchParams.paper);
  const rankingRunId = parseSingleParam(searchParams.ranking_run_id);
  const limit = parseLimit(searchParams.limit, 12, 50);
  const { data, error, status } = await (async () => {
    const params = new URLSearchParams({ family, limit: String(limit) });
    if (RANKING_VERSION) params.set("ranking_version", RANKING_VERSION);
    if (rankingRunId) params.set("ranking_run_id", rankingRunId);

    try {
      const response = await fetch(
        `${API_BASE_URL}/api/v1/evaluation/compare?${params.toString()}`,
        { cache: "no-store" }
      );
      if (response.status === 404) {
        return {
          data: null,
          error:
            "No succeeded ranking run found. Align the configured run label with an existing run, or use an explicit ranking_run_id.",
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
  })();

  const bridgeDistinctness: BridgeDistinctnessFetchResult | null =
    data && family === "bridge"
      ? await fetchBridgeDistinctness(data.ranking_run_id, limit)
      : null;

  const focusPresence = data
    ? {
        ranked: data.ranked.items.some((item) => item.paper_id === focusPaperId),
        citation: data.citation_baseline.items.some((item) => item.paper_id === focusPaperId),
        date: data.date_baseline.items.some((item) => item.paper_id === focusPaperId)
      }
    : null;

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
              href={buildEvaluationFamilyHref(f, {
                focusPaperId,
                rankingRunId,
                limit
              })}
              aria-current={f === family ? "page" : undefined}
              scroll={false}
            >
              {FAMILY_LABEL[f]}
            </Link>
          ))}
        </nav>
        {RANKING_VERSION ? (
          <p className="muted-inline">
            Run label filter: <code>{RANKING_VERSION}</code>
          </p>
        ) : (
          <p className="muted-inline">
            Using latest succeeded run for the default snapshot. Use an explicit run label or run id
            for stable reviewer checks.
          </p>
        )}
        {focusPaperId ? (
          <p className="muted-inline">
            Focus paper: <code>{focusPaperId}</code> | compare limit {limit}
            {focusPresence ? (
              <>
                {" "}
                {focusPresence.ranked || focusPresence.citation || focusPresence.date
                  ? "Visible in:"
                  : "Not visible in the current compare window."}{" "}
                {focusPresence.ranked ? (
                  <Link href={`#${armFocusRowId("ranked", focusPaperId)}`}>ranked</Link>
                ) : null}
                {focusPresence.ranked && (focusPresence.citation || focusPresence.date) ? ", " : null}
                {focusPresence.citation ? (
                  <Link href={`#${armFocusRowId("citation", focusPaperId)}`}>citation baseline</Link>
                ) : null}
                {focusPresence.citation && focusPresence.date ? ", " : null}
                {focusPresence.date ? (
                  <Link href={`#${armFocusRowId("date", focusPaperId)}`}>date baseline</Link>
                ) : null}
                {!focusPresence.ranked && !focusPresence.citation && !focusPresence.date
                  ? " The run context is still pinned while you switch families."
                  : "."}
              </>
            ) : null}
          </p>
        ) : null}
      </section>

      {error ? (
        <section className="panel instrument-panel">
          <h2>Compare unavailable</h2>
          <p>{error}</p>
          {status === 404 ? (
            <p className="muted-inline">
              Run <code>ranking-run</code> against Postgres, or align the configured run label with
              an existing label.
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
              </p>
            ) : null}
            <p className="muted-inline">
              Topic labels are imported metadata and can be noisy; use them as coarse navigation hints,
              not authoritative classifications.
            </p>
          </section>

          {family === "bridge" && bridgeDistinctness ? (
            <section className="panel section-panel instrument-panel eval-bridge-distinctness">
              <div className="panel-header">
                <div>
                  <p className="eyebrow eyebrow-muted">Bridge family</p>
                  <h2>Bridge distinctness diagnostics</h2>
                </div>
              </div>
              <ul className="eval-bridge-guardrails muted-inline" aria-label="Bridge diagnostics scope">
                <li>
                  <strong>Diagnostic only.</strong>
                </li>
                <li>Not a human relevance benchmark.</li>
                <li>Used to inspect whether this bridge review arm is worth further evaluation.</li>
                <li>
                  Bridge remains a diagnostic review surface until labeled review, proxy evaluation, and
                  product policy support stronger recommender claims.
                </li>
              </ul>
              {bridgeDistinctness.kind === "error" ? (
                <div className="eval-bridge-distinctness-error">
                  <p className="result-title">Bridge diagnostics unavailable for this resolved run.</p>
                  <p className="muted-inline">
                    {bridgeDistinctness.message}
                    {bridgeDistinctness.status != null ? ` (HTTP ${bridgeDistinctness.status})` : ""}
                    {bridgeDistinctness.detail ? (
                      <>
                        {" "}
                        <span className="metric-value-mono">— {bridgeDistinctness.detail}</span>
                      </>
                    ) : null}
                  </p>
                  <p className="muted-inline">
                    Suggested operator action: run a bridge-v2 zero-weight ranking with neighbor_mix_v1, then
                    reload this page.
                  </p>
                </div>
              ) : (
                <>
                  <p className="muted-inline">
                    Pinned to the same ranking run as the compare response above. Overlap and
                    coverage are structural checks on short heads, not relevance judgments.
                  </p>
                  <h3 className="eval-proxy-title">Head overlap (Jaccard)</h3>
                  <dl className="eval-dl">
                    <dt>Full bridge vs eligible-only bridge</dt>
                    <dd>
                      overlap {bridgeDistinctness.data.full_bridge_vs_eligible_bridge.overlap_count}; Jaccard{" "}
                      {fmtFixed(bridgeDistinctness.data.full_bridge_vs_eligible_bridge.jaccard, 4)}
                    </dd>
                    <dt>Full bridge vs emerging</dt>
                    <dd>
                      overlap {bridgeDistinctness.data.full_bridge_vs_emerging.overlap_count}; Jaccard{" "}
                      {fmtFixed(bridgeDistinctness.data.full_bridge_vs_emerging.jaccard, 4)}
                    </dd>
                    <dt>Eligible-only bridge vs emerging</dt>
                    <dd>
                      overlap {bridgeDistinctness.data.eligible_bridge_vs_emerging.overlap_count}; Jaccard{" "}
                      {fmtFixed(bridgeDistinctness.data.eligible_bridge_vs_emerging.jaccard, 4)}
                    </dd>
                  </dl>
                  <h3 className="eval-proxy-title">Eligibility (bridge rows)</h3>
                  <dl className="eval-dl">
                    <dt>True / false / null</dt>
                    <dd>
                      {bridgeDistinctness.data.bridge_eligible_true_count} /{" "}
                      {bridgeDistinctness.data.bridge_eligible_false_count} /{" "}
                      {bridgeDistinctness.data.bridge_eligible_null_count}
                    </dd>
                  </dl>
                  <h3 className="eval-proxy-title">Score and signal coverage</h3>
                  <dl className="eval-dl">
                    <dt>Bridge score (non-null / null)</dt>
                    <dd>
                      {bridgeDistinctness.data.bridge_score_nonnull_count} /{" "}
                      {bridgeDistinctness.data.bridge_score_null_count}
                    </dd>
                    <dt>Bridge signal payload (present / missing)</dt>
                    <dd>
                      {bridgeDistinctness.data.bridge_signal_json_present_count} /{" "}
                      {bridgeDistinctness.data.bridge_signal_json_missing_count}
                    </dd>
                    <dt>Bridge family row count</dt>
                    <dd>{bridgeDistinctness.data.bridge_family_row_count}</dd>
                  </dl>
                  <h3 className="eval-proxy-title">Decision support (heuristic)</h3>
                  <dl className="eval-dl">
                    <dt>Suggested next step</dt>
                    <dd>
                      {machineLabel(bridgeDistinctness.data.decision_support.suggested_next_step)}
                    </dd>
                    <dt>Eligible head differs from full bridge</dt>
                    <dd>
                      {boolLabel(bridgeDistinctness.data.decision_support.eligible_head_differs_from_full)}
                    </dd>
                    <dt>Eligible head is less emerging-like than full bridge</dt>
                    <dd>
                      {boolLabel(
                        bridgeDistinctness.data.decision_support.eligible_head_less_emerging_like_than_full
                      )}
                    </dd>
                  </dl>
                  <h3 className="eval-proxy-title">Provenance</h3>
                  <dl className="eval-dl">
                    <dt>Active ranking run</dt>
                    <dd>
                      <code>{bridgeDistinctness.data.ranking_run_id}</code>
                    </dd>
                    <dt>Active ranking version</dt>
                    <dd>
                      <code>{bridgeDistinctness.data.ranking_version}</code>
                    </dd>
                    <dt>Corpus snapshot</dt>
                    <dd>
                      <code>{bridgeDistinctness.data.corpus_snapshot_version}</code>
                    </dd>
                    <dt>Embedding version</dt>
                    <dd>
                      <code>{bridgeDistinctness.data.embedding_version}</code>
                    </dd>
                    <dt>Cluster version</dt>
                    <dd>
                      {bridgeDistinctness.data.cluster_version != null ? (
                        <code>{bridgeDistinctness.data.cluster_version}</code>
                      ) : (
                        <>not recorded</>
                      )}
                    </dd>
                    <dt>Head size</dt>
                    <dd>{bridgeDistinctness.data.k}</dd>
                    <dt>Generated at</dt>
                    <dd className="metric-value-mono">{bridgeDistinctness.data.generated_at}</dd>
                  </dl>
                </>
              )}
            </section>
          ) : null}

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
            <ArmColumn
              title="Ranked (family)"
              arm={data.ranked}
              armKey="ranked"
              focusedPaperId={focusPaperId}
            />
            <ArmColumn
              title="Citation baseline"
              arm={data.citation_baseline}
              armKey="citation"
              focusedPaperId={focusPaperId}
            />
            <ArmColumn
              title="Date baseline"
              arm={data.date_baseline}
              armKey="date"
              focusedPaperId={focusPaperId}
            />
          </div>

          <p className="muted-inline">
            Generated at {data.generated_at} | Current view: proxy evaluation and citation/date baseline
            comparison. Later work could add labeled sets, P@k, or freeze-at-T studies if/when labels exist—see
            product checklist in API <code>/api/v1/evaluation/summary</code>.
          </p>
        </>
      ) : null}
    </main>
  );
}

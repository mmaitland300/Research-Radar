import Link from "next/link";

const FAMILIES = ["emerging", "bridge", "undercited"] as const;
type Family = (typeof FAMILIES)[number];

const FAMILY_LABEL: Record<Family, string> = {
  emerging: "Emerging",
  bridge: "Bridge",
  undercited: "Under-cited"
};

const FAMILY_SUMMARY: Record<Family, string> = {
  emerging: "Momentum-weighted papers gaining relevance inside your curated slice.",
  bridge:
    "Candidate bridge papers with measured cross-cluster signal. In this public run, bridge signal is visible for inspection but is not weighted into final_score yet.",
  undercited: "Low-cite candidates that appear stronger than their current attention level."
};

const FAMILY_NOTES: Record<Family, string[]> = {
  emerging: [
    "Topic-growth and citation-velocity signals should dominate the list.",
    "General semantic relevance is not treated as a default quality score. Some pinned runs use embedding slice-fit as one bounded ranking feature, and the UI labels when that feature is used.",
    "The goal is early importance, not raw popularity."
  ],
  bridge: [
    "Bridge signal is currently measured for inspection, not weighted into final_score.",
    "Use this page as a diagnostics surface for cross-cluster candidates, not a validated bridge recommender.",
    "Bridge should get first-class recommender framing only after the weighting and evaluation story match the label."
  ],
  undercited: [
    "These rows are judged against a low-cite candidate pool, not the whole corpus.",
    "The family should surface overlooked strength before attention catches up.",
    "Snapshot scope matters because the pool is frozen to a corpus version."
  ]
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

type RankedListExplanation = {
  family: string;
  headline: string;
  bullets: string[];
  used_in_ordering: string[];
  measured_only: string[];
  experimental: string[];
};

type RankedItem = {
  paper_id: string;
  title: string;
  year: number;
  citation_count: number;
  source_slug: string | null;
  topics: string[];
  signals: RankedSignals;
  final_score: number;
  reason_short: string;
  signal_explanations: RankedSignalExplanation[];
  bridge_eligible: boolean | null;
};

type RankedResponse = {
  ranking_run_id: string;
  ranking_version: string;
  corpus_snapshot_version: string;
  family: string;
  total: number;
  list_explanation: RankedListExplanation;
  items: RankedItem[];
};

const EMPTY_SIGNALS: RankedSignals = {
  semantic: null,
  citation_velocity: null,
  topic_growth: null,
  bridge: null,
  diversity_penalty: null
};

const SIGNAL_ROLES: RankedSignalExplanation["role"][] = [
  "used",
  "measured",
  "experimental",
  "penalty",
  "not_computed"
];

function coerceSignalExplanation(e: Record<string, unknown>): RankedSignalExplanation {
  const roleRaw = e.role;
  const role = SIGNAL_ROLES.includes(roleRaw as RankedSignalExplanation["role"])
    ? (roleRaw as RankedSignalExplanation["role"])
    : "not_computed";
  const v = e.value;
  const value = typeof v === "number" && Number.isFinite(v) ? v : v == null ? null : Number(v);
  const c = e.contribution;
  const contribution =
    typeof c === "number" && Number.isFinite(c) ? c : c == null ? null : Number(c);
  return {
    key: String(e.key ?? ""),
    label: String(e.label ?? ""),
    role,
    value: value != null && Number.isFinite(value) ? value : null,
    contribution: contribution != null && Number.isFinite(contribution) ? contribution : null,
    summary: String(e.summary ?? "")
  };
}

/** Tolerate older API payloads (no explanation fields) so SSR does not throw. */
function normalizeRankedPayload(json: unknown, family: Family): RankedResponse | null {
  if (!json || typeof json !== "object") return null;
  const raw = json as Record<string, unknown>;
  if (!Array.isArray(raw.items)) return null;

  const defaultListExplanation: RankedListExplanation = {
    family: typeof raw.family === "string" ? raw.family : family,
    headline: "How this list is ranked",
    bullets: [
      "List-level explanations were not returned by this API (often an older deploy). Redeploy apps/api from current main, or use Evaluation for the same run."
    ],
    used_in_ordering: [],
    measured_only: [],
    experimental: []
  };

  let list_explanation: RankedListExplanation = defaultListExplanation;
  const le = raw.list_explanation;
  if (le && typeof le === "object") {
    const o = le as Record<string, unknown>;
    if (typeof o.headline === "string" && Array.isArray(o.bullets)) {
      list_explanation = {
        family: typeof o.family === "string" ? o.family : defaultListExplanation.family,
        headline: o.headline,
        bullets: o.bullets.filter((b): b is string => typeof b === "string"),
        used_in_ordering: Array.isArray(o.used_in_ordering)
          ? o.used_in_ordering.filter((x): x is string => typeof x === "string")
          : [],
        measured_only: Array.isArray(o.measured_only)
          ? o.measured_only.filter((x): x is string => typeof x === "string")
          : [],
        experimental: Array.isArray(o.experimental)
          ? o.experimental.filter((x): x is string => typeof x === "string")
          : []
      };
    }
  }

  const items: RankedItem[] = raw.items.map((row: unknown) => {
    const r = row as Record<string, unknown>;
    const sig = r.signals;
    const signals: RankedSignals =
      sig && typeof sig === "object"
        ? {
            semantic: (sig as RankedSignals).semantic ?? null,
            citation_velocity: (sig as RankedSignals).citation_velocity ?? null,
            topic_growth: (sig as RankedSignals).topic_growth ?? null,
            bridge: (sig as RankedSignals).bridge ?? null,
            diversity_penalty: (sig as RankedSignals).diversity_penalty ?? null
          }
        : { ...EMPTY_SIGNALS };

    const fs = r.final_score;
    const finalNum = typeof fs === "number" ? fs : Number(fs);

    const explRaw = r.signal_explanations;
    const signal_explanations: RankedSignalExplanation[] = Array.isArray(explRaw)
      ? explRaw
          .filter((e): e is Record<string, unknown> => e != null && typeof e === "object")
          .map((e) => coerceSignalExplanation(e))
      : [];

    const topicsRaw = r.topics;
    const topics = Array.isArray(topicsRaw)
      ? topicsRaw.filter((t): t is string => typeof t === "string")
      : [];

    return {
      paper_id: String(r.paper_id ?? ""),
      title: String(r.title ?? ""),
      year: Number(r.year ?? 0),
      citation_count: Number(r.citation_count ?? 0),
      source_slug: r.source_slug == null ? null : String(r.source_slug),
      topics,
      signals,
      final_score: Number.isFinite(finalNum) ? finalNum : 0,
      reason_short: String(r.reason_short ?? ""),
      signal_explanations,
      bridge_eligible:
        typeof r.bridge_eligible === "boolean"
          ? r.bridge_eligible
          : r.bridge_eligible == null
            ? null
            : Boolean(r.bridge_eligible)
    };
  });

  return {
    ranking_run_id: String(raw.ranking_run_id ?? ""),
    ranking_version: String(raw.ranking_version ?? ""),
    corpus_snapshot_version: String(raw.corpus_snapshot_version ?? ""),
    family: typeof raw.family === "string" ? raw.family : family,
    total: typeof raw.total === "number" ? raw.total : items.length,
    list_explanation,
    items
  };
}

const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

const RANKING_VERSION =
  process.env.NEXT_PUBLIC_RANKING_VERSION?.trim() || undefined;

/** Evidence-backed reviewed 0.05 eligible-only bridge arm (manual audit / delta review). Not a default. */
const REVIEWED_ELIGIBLE_BRIDGE_RUN_ID = "rank-bc1123e00c";

function parseFamily(raw: string | string[] | undefined): Family {
  const v = Array.isArray(raw) ? raw[0] : raw;
  if (v && (FAMILIES as readonly string[]).includes(v)) {
    return v as Family;
  }
  return "emerging";
}

function formatSignals(signals: RankedSignals): string {
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
  return parts.length > 0 ? parts.join(" · ") : "No signal breakdown";
}

function EmergingHowPanel({ expl, rankingVersion }: { expl: RankedListExplanation; rankingVersion: string }) {
  return (
    <div className="ranking-how-panel">
      <h3>{expl.headline}</h3>
      <ul>
        {expl.bullets.map((b) => (
          <li key={b}>{b}</li>
        ))}
      </ul>
      <p className="ranking-how-meta">
        <strong>Used in ordering:</strong> {expl.used_in_ordering.join(", ") || "—"}
        <br />
        <strong>Measured only (transparency):</strong> {expl.measured_only.join(", ") || "—"}
        {expl.experimental.length > 0 ? (
          <>
            <br />
            <strong>Experimental:</strong> {expl.experimental.join(", ")}
          </>
        ) : null}
        <br />
        <span className="muted-inline">Pinned run label: {rankingVersion}</span>
      </p>
    </div>
  );
}

function bridgeEligibilityLabel(bridgeEligible: boolean | null): string {
  if (bridgeEligible === true) return "Bridge eligible";
  if (bridgeEligible === false) return "Not bridge eligible";
  return "Bridge eligibility not recorded";
}

function bridgeSignalOrderingLine(explanations: RankedSignalExplanation[]): string | null {
  const bridge = explanations.find((e) => e.key === "bridge");
  if (!bridge) return null;
  if (bridge.role === "used") {
    return "Cross-cluster (bridge) signal is used in final ordering for this run.";
  }
  if (bridge.role === "measured" || bridge.role === "experimental") {
    return "Cross-cluster (bridge) signal is measured for this run but is not used in final ordering.";
  }
  return "Cross-cluster (bridge) signal is not computed for this row.";
}

function BridgeSignalOrderingParagraph({ explanations }: { explanations: RankedSignalExplanation[] }) {
  const line = bridgeSignalOrderingLine(explanations);
  return line ? <p className="result-breakdown">{line}</p> : null;
}

function EmergingWhySurfaced({ explanations }: { explanations: RankedSignalExplanation[] }) {
  return (
    <details className="ranking-why-details">
      <summary>Why this surfaced · {explanationSummary(explanations)}</summary>
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

async function fetchRanked(
  family: Family,
  options: {
    limit: number;
    rankingRunId: string | undefined;
    bridgeEligibleOnly: boolean;
  }
): Promise<{
  data: RankedResponse | null;
  error: string | null;
  status: number | null;
}> {
  const params = new URLSearchParams({
    family,
    limit: String(options.limit)
  });
  if (RANKING_VERSION) params.set("ranking_version", RANKING_VERSION);
  if (options.rankingRunId) params.set("ranking_run_id", options.rankingRunId);
  if (family === "bridge" && options.bridgeEligibleOnly) {
    params.set("bridge_eligible_only", "true");
  }

  try {
    const response = await fetch(
      `${API_BASE_URL}/api/v1/recommendations/ranked?${params.toString()}`,
      { cache: "no-store" }
    );
    if (response.status === 404) {
      return {
        data: null,
        error:
          "No succeeded ranking run found. Run the pipeline ranking job against your DB, or set NEXT_PUBLIC_RANKING_VERSION to match an existing run.",
        status: 404
      };
    }
    if (!response.ok) {
      let detail = "";
      try {
        const errBody = (await response.json()) as { detail?: unknown };
        if (typeof errBody.detail === "string") {
          detail = ` ${errBody.detail}`;
        }
      } catch {
        /* ignore non-JSON error bodies */
      }
      return {
        data: null,
        error: `API responded with ${response.status}.${detail}`,
        status: response.status
      };
    }
    const rawJson: unknown = await response.json();
    const data = normalizeRankedPayload(rawJson, family);
    if (!data) {
      return {
        data: null,
        error: "API returned ranked data in an unexpected shape.",
        status: 200
      };
    }
    return { data, error: null, status: 200 };
  } catch {
    return {
      data: null,
      error: "Could not reach the API. Start apps/api and Postgres, then refresh.",
      status: null
    };
  }
}

type PageProps = {
  searchParams: Record<string, string | string[] | undefined>;
};

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

function parseBooleanParam(raw: string | string[] | undefined): boolean {
  const value = parseSingleParam(raw);
  return value === "true" || value === "1" || value === "yes";
}

function paperAnchorId(paperId: string): string {
  return `paper-${encodeURIComponent(paperId)}`;
}

function buildRecommendedFamilyHref(
  family: Family,
  options: {
    focusPaperId?: string;
    rankingRunId?: string;
    limit?: number;
    bridgeEligibleOnly?: boolean;
  }
): string {
  const params = new URLSearchParams({ family });
  if (options.focusPaperId) params.set("paper", options.focusPaperId);
  if (options.rankingRunId) params.set("ranking_run_id", options.rankingRunId);
  if (options.limit != null) params.set("limit", String(options.limit));
  if (family === "bridge" && options.bridgeEligibleOnly) {
    params.set("bridge_eligible_only", "true");
  }
  return `/recommended?${params.toString()}`;
}

export default async function RecommendedPage({ searchParams }: PageProps) {
  const family = parseFamily(searchParams.family);
  const focusPaperId = parseSingleParam(searchParams.paper);
  const rankingRunId = parseSingleParam(searchParams.ranking_run_id);
  const limit = parseLimit(searchParams.limit, 15, 100);
  const bridgeEligibleOnly = family === "bridge" && parseBooleanParam(searchParams.bridge_eligible_only);
  const viewingReviewedEligibleBridgeArm =
    family === "bridge" &&
    bridgeEligibleOnly &&
    rankingRunId === REVIEWED_ELIGIBLE_BRIDGE_RUN_ID;
  const usingUnpinnedLatestRun = !rankingRunId && !RANKING_VERSION;
  const { data, error, status } = await fetchRanked(family, {
    limit,
    rankingRunId,
    bridgeEligibleOnly
  });
  const topScore = data?.items[0]?.final_score ?? null;
  const surfacedWithTopics = data?.items.filter((item) => item.topics.length > 0).length ?? 0;
  const focusItem = focusPaperId
    ? data?.items.find((item) => item.paper_id === focusPaperId) ?? null
    : null;

  return (
    <main className={`page page-family page-family-${family}`}>
      <section className={`panel page-hero family-hero family-hero-${family}`}>
        <div className="family-hero-grid">
          <div>
            <div className="panel-header">
              <div>
                <p className={`eyebrow family-${family}`}>Recommended</p>
                <h1>Ranked recommendations</h1>
              </div>
              <div className="stamp-row">
                <span className={`stamp stamp-family stamp-family-${family}`}>
                  {family === "bridge" && bridgeEligibleOnly
                    ? "Eligible-only bridge view"
                    : family === "bridge"
                      ? "Bridge preview"
                      : `${FAMILY_LABEL[family]} feed`}
                </span>
                <span className="stamp">Materialized ranking run</span>
              </div>
            </div>
            <p className="hero-lead">{FAMILY_SUMMARY[family]}</p>
            {usingUnpinnedLatestRun ? (
              <div className="ranking-how-panel" role="status">
                <h3>Run pin warning</h3>
                <p className="muted-inline">
                  Using latest succeeded run. For reviewed bridge evidence, pin a{" "}
                  <code>ranking_run_id</code> or <code>NEXT_PUBLIC_RANKING_VERSION</code>.
                </p>
              </div>
            ) : null}
            {family === "bridge" ? (
              <p className="muted-inline">
                <strong>Diagnostics:</strong> bridge signal is <strong>measured</strong> and visible in this
                run for inspection. Depending on the pinned run, it may be measured-only or experimental,
                so this page is a <strong>preview / diagnostics</strong>{" "}
                surface—not a validated bridge recommender.
              </p>
            ) : null}
            {family === "bridge" ? (
              <p className="muted-inline">
                Bridge evidence is experimental and run-specific. Use pinned runs for reviewed behavior.
              </p>
            ) : null}
            {family === "bridge" && viewingReviewedEligibleBridgeArm ? (
              <div className="ranking-how-panel" role="status">
                <h3>Reviewed 0.05 eligible bridge arm</h3>
                <p>
                  <strong>Viewing reviewed 0.05 eligible bridge arm.</strong>
                </p>
                <p className="muted-inline">
                  Pinned to reviewed experimental run <code>{REVIEWED_ELIGIBLE_BRIDGE_RUN_ID}</code>. Not
                  default. Not validation.
                </p>
              </div>
            ) : null}
            {family === "bridge" && !viewingReviewedEligibleBridgeArm ? (
              <div className="ranking-how-panel">
                <h3>Reviewed experimental arm</h3>
                <p>
                  <Link
                    className="action-link"
                    href={buildRecommendedFamilyHref("bridge", {
                      focusPaperId,
                      rankingRunId: REVIEWED_ELIGIBLE_BRIDGE_RUN_ID,
                      bridgeEligibleOnly: true
                    })}
                    scroll={false}
                  >
                    Open reviewed 0.05 eligible bridge arm
                  </Link>
                </p>
                <p className="muted-inline">
                  Pinned to reviewed experimental run <code>{REVIEWED_ELIGIBLE_BRIDGE_RUN_ID}</code>. Not
                  default. Not validation.
                </p>
              </div>
            ) : null}
            {family === "bridge" && bridgeEligibleOnly && !viewingReviewedEligibleBridgeArm ? (
              <div className="ranking-how-panel">
                <h3>Eligible-only bridge view</h3>
                <p className="muted-inline">
                  Experimental bridge arm; not a default or validation claim.
                </p>
                <p className="muted-inline">
                  Current evidence supports 0.05 as a plausible experimental bridge-weight arm,
                  not default. 0.10 showed no additional eligible top-20 membership movement.
                </p>
              </div>
            ) : null}
            <p>
              Papers come from a <strong>materialized ranking run</strong> (<code>paper_scores</code> per
              family). The API derives plain-language explanations from the same weights stored on the
              run. The <strong>undercited</strong> family only scores works in the frozen low-cite
              candidate pool (<code>docs/candidate-pool-low-cite.md</code> v0), scoped to your corpus
              snapshot.
            </p>
            <nav className="tabs" aria-label="Recommendation family">
              {FAMILIES.map((f) => (
                <Link
                  key={f}
                  href={buildRecommendedFamilyHref(f, {
                    focusPaperId,
                    rankingRunId,
                    limit,
                    bridgeEligibleOnly: f === "bridge" ? bridgeEligibleOnly : false
                  })}
                  aria-current={f === family ? "page" : undefined}
                  scroll={false}
                >
                  {FAMILY_LABEL[f]}
                </Link>
              ))}
            </nav>
            {family === "bridge" ? (
              <nav className="tabs" aria-label="Bridge feed view">
                <Link
                  href={buildRecommendedFamilyHref("bridge", {
                    focusPaperId,
                    rankingRunId,
                    limit,
                    bridgeEligibleOnly: false
                  })}
                  aria-current={!bridgeEligibleOnly ? "page" : undefined}
                  scroll={false}
                >
                  Full bridge feed
                </Link>
                <Link
                  href={buildRecommendedFamilyHref("bridge", {
                    focusPaperId,
                    rankingRunId,
                    limit,
                    bridgeEligibleOnly: true
                  })}
                  aria-current={bridgeEligibleOnly ? "page" : undefined}
                  scroll={false}
                >
                  Eligible-only bridge feed
                </Link>
              </nav>
            ) : null}
            {data ? (
              <div className="hero-metrics" aria-label="Ranking run summary">
                <article className="metric-card">
                  <p className="metric-label">Run label</p>
                  <p className="metric-value metric-value-mono">{data.ranking_version}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Rows surfaced</p>
                  <p className="metric-value">{data.total}</p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Top score</p>
                  <p className="metric-value">
                    {topScore != null ? topScore.toFixed(3) : "n/a"}
                  </p>
                </article>
                <article className="metric-card">
                  <p className="metric-label">Rows with topic labels</p>
                  <p className="metric-value">{surfacedWithTopics}</p>
                </article>
              </div>
            ) : null}
            {data ? (
              <p className="muted-inline">
                <strong>{data.ranking_version}</strong> | run{" "}
                <code>{data.ranking_run_id}</code> | snapshot{" "}
                <code>{data.corpus_snapshot_version}</code> | {data.total}{" "}
                {data.total === 1 ? "paper" : "papers"}
              </p>
            ) : null}
            {focusPaperId ? (
              <p className="muted-inline">
                Focus paper: <code>{focusPaperId}</code>
                {focusItem
                  ? (
                    <>
                      {` is visible in this ${FAMILY_LABEL[family].toLowerCase()} slice. `}
                      <Link href={`#${paperAnchorId(focusPaperId)}`}>Jump to focused row</Link>.
                    </>
                  )
                  : ` is not in the current top ${limit} rows for this slice, but the run context is still pinned while you switch families.`}
              </p>
            ) : null}
            {RANKING_VERSION ? (
              <p className="muted-inline">
                Web is filtering runs with <code>NEXT_PUBLIC_RANKING_VERSION={RANKING_VERSION}</code>.
              </p>
            ) : (
              <p className="muted-inline">
                Using the latest succeeded run for the corpus snapshot (set{" "}
                <code>NEXT_PUBLIC_RANKING_VERSION</code> to pin a label).
              </p>
            )}
          </div>
          <aside className="family-brief">
            <div className="family-brief-diagram" aria-hidden="true">
              <span className="family-ring family-ring-a" />
              <span className="family-ring family-ring-b" />
              <span className="family-ring family-ring-c" />
              <span className={`family-sweep family-sweep-${family}`} />
              <span className={`family-node family-node-${family} family-node-1`} />
              <span className={`family-node family-node-${family} family-node-2`} />
              <span className={`family-node family-node-${family} family-node-3`} />
            </div>
            <div className="family-brief-copy">
              <p className={`eyebrow family-${family}`}>Signal lens</p>
              <h2>
                {family === "bridge"
                  ? "Bridge preview reading guide"
                  : `${FAMILY_LABEL[family]} reading guide`}
              </h2>
              <ul className="measure-list">
                {FAMILY_NOTES[family].map((note) => (
                  <li key={note}>{note}</li>
                ))}
              </ul>
            </div>
          </aside>
        </div>
      </section>

      {error ? (
        <section className="panel instrument-panel">
          <p>{error}</p>
          {status === 404 ? (
            <p className="muted-inline">
              Example:{" "}
              <code>NEXT_PUBLIC_RANKING_VERSION=v0-heuristic-no-embeddings-step3</code>
            </p>
          ) : null}
        </section>
      ) : null}

      {data && !error ? (
        <section className="panel section-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow eyebrow-muted">Live ranking surface</p>
              <h2>
                {family === "bridge"
                  ? "Bridge preview results"
                  : `${FAMILY_LABEL[family]} results`}
              </h2>
            </div>
            <div className="stamp-row">
              <span className={`stamp stamp-family stamp-family-${family}`}>
                Family: {data.family}
              </span>
              {family === "bridge" && bridgeEligibleOnly ? (
                <span className="stamp">Eligible only</span>
              ) : null}
              <span className="stamp">Order: final_score desc, work_id asc</span>
              <span className="stamp">Limit: {limit}</span>
            </div>
          </div>
          {family === "emerging" || family === "bridge" ? (
            <EmergingHowPanel expl={data.list_explanation} rankingVersion={data.ranking_version} />
          ) : null}
          {data.items.length === 0 ? (
            <p>No rows for this family in the selected run.</p>
          ) : (
            <ul className="result-list">
              {data.items.map((item) => (
                <li
                  key={item.paper_id}
                  id={focusPaperId === item.paper_id ? paperAnchorId(item.paper_id) : undefined}
                  className={`result-item result-item-${family}${
                    focusPaperId === item.paper_id ? " result-item-focus" : ""
                  }`}
                >
                  <div className="result-heading">
                    <p className="result-title">
                      <Link href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                        {item.title}
                      </Link>
                    </p>
                    <span className={`result-score result-score-${family}`}>
                      {item.final_score.toFixed(3)}
                    </span>
                  </div>
                  <p className="result-meta">
                    {item.year} | cites: {item.citation_count} |{" "}
                    {item.source_slug ?? "unknown venue"}
                  </p>
                  <div className="stamp-row stamp-row-inline">
                    <span className={`stamp stamp-family stamp-family-${family}`}>
                      {FAMILY_LABEL[family]}
                    </span>
                    {family === "bridge" ? (
                      <span className="stamp">{bridgeEligibilityLabel(item.bridge_eligible)}</span>
                    ) : null}
                    {focusPaperId === item.paper_id ? <span className="stamp">Focus paper</span> : null}
                    <span className="stamp">{item.topics.length} topic label{item.topics.length === 1 ? "" : "s"}</span>
                  </div>
                  {item.topics.length > 0 ? (
                    <div className="chip-row" aria-label="Top topics">
                      {item.topics.map((t) => (
                        <span key={t} className="chip">
                          {t}
                        </span>
                      ))}
                    </div>
                  ) : null}
                  <p className="result-reason">{item.reason_short}</p>
                  {family === "bridge" && bridgeEligibleOnly ? (
                    <p className="result-breakdown">
                      This row passed the bridge eligibility gate for the resolved run.
                    </p>
                  ) : null}
                  {family === "bridge" ? (
                    <BridgeSignalOrderingParagraph explanations={item.signal_explanations} />
                  ) : null}
                  {family === "emerging" && item.signal_explanations?.length ? (
                    <EmergingWhySurfaced explanations={item.signal_explanations} />
                  ) : null}
                  {family === "bridge" && item.signal_explanations?.length ? (
                    <EmergingWhySurfaced explanations={item.signal_explanations} />
                  ) : null}
                  {family === "bridge" ? (
                    <p className="result-breakdown">Signals: {formatSignals(item.signals)}</p>
                  ) : family === "emerging" && item.signal_explanations?.length ? null : (
                    <p className="result-breakdown">Signals: {formatSignals(item.signals)}</p>
                  )}
                  <div className="action-row" aria-label="Related views">
                    <Link className="action-link" href={`/papers/${encodeURIComponent(item.paper_id)}`}>
                      Open dossier
                    </Link>
                    <Link className="action-link" href={`/evaluation?family=${family}`}>
                      Compare in evaluation
                    </Link>
                    <Link className="action-link" href="/trends">
                      Check topic momentum
                    </Link>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>
      ) : null}

      <section className="grid">
        <article className="card">
          <h2>Current ML boundary</h2>
          <p>
            ML milestone 1 delivers retrieval for similar papers. Writing{" "}
            <code>semantic_score</code> into ranked families stays gated until a defined relevance target;
            bridge-style scores remain diagnostics until weighting and proxy evaluation justify recommender
            framing.
          </p>
        </article>
        <article className="card">
          <h2>Heuristic baseline</h2>
          <p>
            The rule-only undercited list (<code>/api/v1/recommendations/undercited</code>) uses the same
            pool definition but is not tied to a corpus snapshot. For snapshot-scoped A/B against the
            ranked undercited family, use <Link href="/evaluation?family=undercited">Evaluation</Link>.
          </p>
        </article>
      </section>
    </main>
  );
}

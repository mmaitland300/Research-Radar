import type {
  Family,
  RankedItem,
  RankedListExplanation,
  RankedResponse,
  RankedSignalExplanation,
  RankedSignals,
  RankingMode
} from "./types";

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

export const API_BASE_URL =
  process.env.API_BASE_URL ??
  process.env.NEXT_PUBLIC_API_BASE_URL ??
  "http://localhost:8000";

export const PRODUCT_RANKING_VERSION =
  process.env.NEXT_PUBLIC_RANKING_VERSION?.trim() ||
  "shadow-generalization-product-candidate-ranking-v1";
export const BRIDGE_RANKING_RUN_ID =
  process.env.NEXT_PUBLIC_BRIDGE_RANKING_RUN_ID?.trim() || "rank-5a7efa5ca3";

function envFlagEnabled(raw: string | undefined): boolean {
  const value = raw?.trim().toLowerCase();
  return value === "true" || value === "1" || value === "yes" || value === "on";
}

export const ENABLE_EXPERIMENTAL_BRIDGE_VIEW = envFlagEnabled(
  process.env.NEXT_PUBLIC_ENABLE_EXPERIMENTAL_BRIDGE_VIEW
);

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
export function normalizeRankedPayload(json: unknown, family: Family): RankedResponse | null {
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

  const modeRaw = raw.ranking_mode;
  const ranking_mode: RankingMode =
    modeRaw === "bounded_ml_scorer" || modeRaw === "bounded_bridge_ml_scorer"
      ? modeRaw
      : "materialized_heuristic";

  return {
    ranking_run_id: String(raw.ranking_run_id ?? ""),
    ranking_version: String(raw.ranking_version ?? ""),
    corpus_snapshot_version: String(raw.corpus_snapshot_version ?? ""),
    family: typeof raw.family === "string" ? raw.family : family,
    ranking_mode,
    ranking_mode_detail:
      typeof raw.ranking_mode_detail === "string" ? raw.ranking_mode_detail : null,
    scorer_surface: typeof raw.scorer_surface === "string" ? raw.scorer_surface : null,
    bridge_recommendations_ml_served:
      typeof raw.bridge_recommendations_ml_served === "boolean"
        ? raw.bridge_recommendations_ml_served
        : null,
    bridge_rank_pct_hybrid_alpha:
      typeof raw.bridge_rank_pct_hybrid_alpha === "number"
        ? raw.bridge_rank_pct_hybrid_alpha
        : null,
    bridge_rank_pct_scope:
      typeof raw.bridge_rank_pct_scope === "string" ? raw.bridge_rank_pct_scope : null,
    emitted_to_public_users:
      typeof raw.emitted_to_public_users === "boolean" ? raw.emitted_to_public_users : null,
    total: typeof raw.total === "number" ? raw.total : items.length,
    list_explanation,
    items
  };
}

export type FetchRankedResult = {
  data: RankedResponse | null;
  error: string | null;
  status: number | null;
};

export async function fetchRanked(
  family: Family,
  options: {
    limit: number;
    rankingRunId: string | undefined;
    bridgeEligibleOnly: boolean;
  }
): Promise<FetchRankedResult> {
  const params = new URLSearchParams({
    family,
    limit: String(options.limit)
  });
  if (family === "bridge") {
    params.set("ranking_run_id", options.rankingRunId ?? BRIDGE_RANKING_RUN_ID);
  } else {
    if (PRODUCT_RANKING_VERSION) params.set("ranking_version", PRODUCT_RANKING_VERSION);
    if (options.rankingRunId) params.set("ranking_run_id", options.rankingRunId);
  }
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
          "No succeeded ranking run found. Align the configured run label with an existing run, or use an explicit ranking_run_id.",
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
      error:
        "The Research Radar API is unavailable. Try again later, or check the repository setup docs if you are running it locally.",
      status: null
    };
  }
}

import { FAMILIES } from "./types";
import type { Family } from "./types";

export type SearchParams = Record<string, string | string[] | undefined>;

export type RecommendedUrlState = {
  family: Family;
  focusPaperId: string | undefined;
  requestedRankingRunId: string | undefined;
  limit: number;
  bridgeEligibleOnlyRequested: boolean;
};

export function parseFamily(raw: string | string[] | undefined): Family {
  const v = Array.isArray(raw) ? raw[0] : raw;
  if (v && (FAMILIES as readonly string[]).includes(v)) {
    return v as Family;
  }
  return "emerging";
}

export function parseSingleParam(raw: string | string[] | undefined): string | undefined {
  const value = Array.isArray(raw) ? raw[0] : raw;
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

export function parseLimit(raw: string | string[] | undefined, fallback: number, max: number): number {
  const value = parseSingleParam(raw);
  if (!value) return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(max, Math.trunc(parsed)));
}

export function parseBooleanParam(raw: string | string[] | undefined): boolean {
  const value = parseSingleParam(raw);
  return value === "true" || value === "1" || value === "yes";
}

export function parseRecommendedUrlState(params: SearchParams): RecommendedUrlState {
  const family = parseFamily(params.family);
  const defaultLimit = family === "emerging" || family === "bridge" ? 20 : 15;

  return {
    family,
    focusPaperId: parseSingleParam(params.paper),
    requestedRankingRunId: parseSingleParam(params.ranking_run_id),
    limit: parseLimit(params.limit, defaultLimit, 100),
    bridgeEligibleOnlyRequested:
      family === "bridge" && parseBooleanParam(params.bridge_eligible_only)
  };
}

export function paperAnchorId(paperId: string): string {
  return `paper-${encodeURIComponent(paperId)}`;
}

export function buildRecommendedFamilyHref(
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
  if (family !== "bridge" && options.rankingRunId) {
    params.set("ranking_run_id", options.rankingRunId);
  }
  if (options.limit != null) params.set("limit", String(options.limit));
  if (family === "bridge" && options.bridgeEligibleOnly) {
    params.set("bridge_eligible_only", "true");
  }
  return `/recommended?${params.toString()}`;
}

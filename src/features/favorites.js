import { fetchJsonFromCandidates } from "../data/fetch-json.js";

function toText(value) {
  return String(value || "").trim();
}

function normalizeBaseUrl(baseUrl) {
  return toText(baseUrl).replace(/\/$/, "");
}

function buildFavoritesReadCandidates(baseUrl, paths) {
  const normalizedBaseUrl = normalizeBaseUrl(baseUrl);
  return (Array.isArray(paths) ? paths : [paths]).map((path) => `${normalizedBaseUrl}${path}`);
}

export async function sendFavoriteVote(baseUrl, payload) {
  const response = await fetch(`${normalizeBaseUrl(baseUrl)}/favorites/vote`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  const isJson = (response.headers.get("content-type") || "").toLowerCase().includes("application/json");
  const body = isJson ? await response.json().catch(() => null) : null;
  const duplicate = response.status === 409
    || body?.code === "duplicate"
    || body?.status === "duplicate"
    || body?.duplicate === true;

  if (response.ok || duplicate) {
    return { ok: true, duplicate, status: response.status, body };
  }

  throw new Error(`HTTP ${response.status}`);
}

export function fetchHallOfFame(baseUrl) {
  return fetchJsonFromCandidates(
    buildFavoritesReadCandidates(baseUrl, ["/favorites/aggregates/hall_of_fame.json", "/favorites/hall_of_fame.json"]),
    { targetName: "favorites aggregate(hall)" },
  );
}

export function fetchRecentRecommendations(baseUrl) {
  return fetchJsonFromCandidates(
    buildFavoritesReadCandidates(baseUrl, ["/favorites/aggregates/recent_recommendations.json", "/favorites/recent_recommendations.json"]),
    { targetName: "favorites aggregate(recent)" },
  );
}

export function fetchFavoriteRanking(baseUrl) {
  return fetchJsonFromCandidates(
    buildFavoritesReadCandidates(baseUrl, ["/favorites/exports/current_ranking.json", "/favorites/current_ranking.json"]),
    { targetName: "favorites aggregate(ranking)" },
  );
}

export function toggleFavorite(setLike, id) {
  if (setLike.has(id)) {
    setLike.delete(id);
    return false;
  }
  setLike.add(id);
  return true;
}

export function isFavorite(setLike, id) {
  return setLike.has(id);
}

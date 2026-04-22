function toText(value) {
  return String(value || "").trim();
}

function normalizeBaseUrl(baseUrl) {
  return toText(baseUrl).replace(/\/$/, "");
}

async function fetchJsonFromPaths(baseUrl, paths, errorMessage) {
  const normalizedBaseUrl = normalizeBaseUrl(baseUrl);
  const pathCandidates = Array.isArray(paths) ? paths : [paths];
  let lastStatus = "";

  for (const path of pathCandidates) {
    const response = await fetch(`${normalizedBaseUrl}${path}`);
    if (response.ok) {
      return response.json();
    }
    lastStatus = `HTTP ${response.status}`;
  }

  throw new Error(errorMessage || lastStatus || "favorites JSON の取得に失敗しました。");
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
  return fetchJsonFromPaths(
    baseUrl,
    ["/favorites/aggregates/hall_of_fame.json", "/favorites/hall_of_fame.json"],
    "hall_of_fame.json の取得に失敗しました。",
  );
}

export function fetchRecentRecommendations(baseUrl) {
  return fetchJsonFromPaths(
    baseUrl,
    ["/favorites/aggregates/recent_recommendations.json", "/favorites/recent_recommendations.json"],
    "recent_recommendations.json の取得に失敗しました。",
  );
}

export function fetchFavoriteRanking(baseUrl) {
  return fetchJsonFromPaths(
    baseUrl,
    ["/favorites/exports/current_ranking.json", "/favorites/current_ranking.json"],
    "current_ranking.json の取得に失敗しました。",
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

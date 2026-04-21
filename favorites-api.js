function toText(value) {
  return String(value || "").trim();
}

export async function sendFavoriteVote(baseUrl, payload) {
  const response = await fetch(`${toText(baseUrl).replace(/\/$/, "")}/favorites/vote`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  return response.json();
}

export async function fetchHallOfFame(baseUrl) {
  const response = await fetch(`${toText(baseUrl).replace(/\/$/, "")}/favorites/hall_of_fame.json`);
  if (!response.ok) throw new Error("hall_of_fame.json の取得に失敗しました。");
  return response.json();
}

export async function fetchRecentRecommendations(baseUrl) {
  const response = await fetch(`${toText(baseUrl).replace(/\/$/, "")}/favorites/recent_recommendations.json`);
  if (!response.ok) throw new Error("recent_recommendations.json の取得に失敗しました。");
  return response.json();
}

export async function fetchFavoriteRanking(baseUrl) {
  const response = await fetch(`${toText(baseUrl).replace(/\/$/, "")}/favorites/current_ranking.json`);
  if (!response.ok) throw new Error("current_ranking.json の取得に失敗しました。");
  return response.json();
}

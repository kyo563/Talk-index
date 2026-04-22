function normalizeFailureReason(error) {
  const message = error instanceof Error ? error.message : String(error || "");
  if (/^HTTP\s+\d{3}/.test(message)) return message;
  if (message === "JSONの解析に失敗しました") return message;
  if (message === "ネットワークエラー") return message;
  if (error instanceof SyntaxError) return "JSONの解析に失敗しました";
  if (error instanceof TypeError) return "ネットワークエラー";
  return "不明なエラー";
}

export async function fetchJson(url, options = {}) {
  let response;
  try {
    response = await fetch(url, { cache: "no-store", ...options });
  } catch {
    throw new Error("ネットワークエラー");
  }
  if (!response.ok) throw new Error(`HTTP ${response.status}`);

  try {
    return await response.json();
  } catch {
    throw new Error("JSONの解析に失敗しました");
  }
}

export async function fetchJsonFromCandidates(candidates, config = {}) {
  const { targetName = "JSON", fetchOptions = {} } = config || {};
  let lastReason = "不明なエラー";

  for (const candidate of (Array.isArray(candidates) ? candidates : [])) {
    try {
      return await fetchJson(candidate, fetchOptions);
    } catch (error) {
      lastReason = normalizeFailureReason(error);
    }
  }

  throw new Error(`${targetName} の取得に失敗しました: ${lastReason}`);
}

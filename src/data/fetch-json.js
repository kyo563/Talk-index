export async function fetchJson(url, options = {}) {
  const response = await fetch(url, { cache: "no-store", ...options });
  if (!response.ok) {
    throw new Error(`JSON取得失敗: ${response.status} ${response.statusText} (${url})`);
  }
  return response.json();
}

export async function fetchJsonFromCandidates(candidates, options = {}) {
  const errors = [];
  for (const candidate of candidates) {
    try {
      return await fetchJson(candidate, options);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
    }
  }
  throw new Error(`JSON取得候補をすべて試行しましたが失敗しました: ${errors.join(" | ")}`);
}

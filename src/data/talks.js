import { fetchJsonFromCandidates } from "./fetch-json.js";

export async function fetchTalks(candidates) {
  const data = await fetchJsonFromCandidates(candidates);
  return Array.isArray(data) ? data : [];
}

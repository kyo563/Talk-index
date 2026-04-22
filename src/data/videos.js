import { fetchJsonFromCandidates } from "./fetch-json.js";

export async function fetchVideos(candidates) {
  const data = await fetchJsonFromCandidates(candidates);
  return Array.isArray(data) ? data : [];
}

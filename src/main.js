import { createInitialState, createRefs } from "./core/state.js";
import { fetchJson, fetchJsonFromCandidates } from "./data/fetch-json.js";
import { fetchVideos } from "./data/videos.js";
import { fetchTalks } from "./data/talks.js";
import { filterBySearch } from "./features/search.js";
import { toggleFavorite, isFavorite } from "./features/favorites.js";
import { renderStatus } from "./ui/render-status.js";
import { renderResults } from "./ui/render-results.js";

void {
  createInitialState,
  createRefs,
  fetchJson,
  fetchJsonFromCandidates,
  fetchVideos,
  fetchTalks,
  filterBySearch,
  toggleFavorite,
  isFavorite,
  renderStatus,
  renderResults,
};

await import("../app.js");

import { createInvalidJsonShapeError, createTargetFetchError, fetchJsonFromCandidates } from "./src/data/fetch-json.js";
import { fetchHallOfFame, fetchRecentRecommendations, sendFavoriteVote as sendFavoriteVoteRequest } from "./src/features/favorites.js";
import { getModeMessage } from "./src/ui/render-messages.js";

const configuredDataUrl = text(window.TALK_INDEX_DATA_URL);
const DATA_URL_CANDIDATES = configuredDataUrl
  ? [configuredDataUrl]
  : ["index/latest.json", "./index/latest.json", "/index/latest.json", "latest.json"];

const SEARCH_INDEX_URL_CANDIDATES = DATA_URL_CANDIDATES
  .map((url) => String(url || "").replace(/latest\.json$/, "search_index.json"))
  .filter((url, index, self) => url && self.indexOf(url) === index);
const TALKS_URL_CANDIDATES = DATA_URL_CANDIDATES
  .map((url) => String(url || "").replace(/latest\.json$/, "talks.json"))
  .filter((url, index, self) => url && self.indexOf(url) === index);
const FAVORITES_BASE_URL_CANDIDATES = DATA_URL_CANDIDATES
  .map((url) => String(url || "").replace(/\/index\/latest\.json$/, ""))
  .filter((url, index, self) => url && self.indexOf(url) === index);
const FAVORITES_READ_BASE_URL_CANDIDATES = [
  text(window.__TALK_INDEX_FAVORITES_READ_BASE_URL__),
  text(window.__TALK_INDEX_FAVORITES_BASE_URL__),
  ...FAVORITES_BASE_URL_CANDIDATES,
  text(location.origin),
].filter((url, index, self) => url && self.indexOf(url) === index);
const FAVORITES_WRITE_BASE_URL_CANDIDATES = [
  text(window.__TALK_INDEX_FAVORITES_WRITE_BASE_URL__),
  text(window.__TALK_INDEX_FAVORITES_API_BASE_URL__),
  text(window.__TALK_INDEX_FAVORITES_BASE_URL__),
  text(location.origin),
].filter((url, index, self) => url && self.indexOf(url) === index);
const state = {
  search: "",
  videos: [],
  talks: [],
  recommendation: null,
  searchIndexStatus: "idle",
  searchIndexError: "",
  searchIndexPromise: null,
  talksStatus: "idle",
  talksError: "",
  talksPromise: null,
  talksFallbackActive: false,
  skippedRows: 0,
  openVideoKeys: new Set(),
  openTalkKeys: new Set(),
  isVideoExpandLock: false,
  videoAutoCollapseAnchor: null,
  viewMode: "video",
  randomSection: "",
  randomTalkKeys: null,
  newVideoHighlightKeys: new Set(),
  isNewVideoHighlightVisible: true,
  videoDetailsCache: new Map(),
  videoDetailsPromises: new Map(),
  talkRecommendationCache: new Map(),
  talkSearchDocuments: null,
  lastFallbackTalkKey: "",
  favoritesClientId: "",
  favoritedHeadingIds: new Set(),
  alreadyVotedHeadingIds: new Set(),
  unsyncedFavoriteHeadingIds: new Set(),
  favoritePanelOpenKeys: new Set(),
  favoritesRecent: null,
  favoritesHall: null,
  favoritesDataStatus: "idle",
  favoritesDataError: "",
};

const RECOMMEND_LIMIT = 3;
const NEW_VIDEO_HIGHLIGHT_COUNT = 1;
const NEW_VIDEO_HIGHLIGHT_SCROLL_SCREENS = 2;
const VIDEO_AUTO_COLLAPSE_PASSED_COUNT = 2;
const SONG_DB_URL = "https://performancerecord.github.io/uni-uta-db/";
const GENERIC_TAG_RATIO_THRESHOLD = 1;
const HIDDEN_DISPLAY_TAGS = new Set(["Vtuber", "雲丹ゐくら", "個人Vtuber", "バーチャルYOUTUBER"]);

const TOKEN_STOP_WORDS = new Set([
  "の",
  "こと",
  "です",
  "ます",
  "する",
  "した",
  "いる",
  "ある",
  "なる",
  "よう",
  "ため",
  "話",
  "の話",
  "配信の話",
  "雑談の話",
  "について",
  "そして",
]);


const TALK_RECOMMENDATION_WORD_COUNT = 2;
const TALK_RECOMMENDATION_MIN_SCORE = 3;
const TALK_RECOMMENDATION_STOP_WORDS = new Set([
  "話", "こと", "これ", "それ", "もの", "やつ", "ほんま", "まじ", "やばい",
  "すごい", "無理", "なるほど", "感じ", "みたい", "とき", "時", "自分", "相手", "返事",
  "今回", "前回", "最近", "雑談", "配信", "トーク", "について", "の話",
]);

const refs = {
  search: document.getElementById("search"),
  notice: document.getElementById("notice"),
  error: document.getElementById("error"),
  results: document.getElementById("results"),
  serverStatus: document.getElementById("server-status"),
  toggleAll: document.getElementById("toggle-all"),
  clearSearch: document.getElementById("clear-search"),
  randomSection: document.getElementById("random-section"),
  tabVideo: document.getElementById("tab-video"),
  tabTalk: document.getElementById("tab-talk"),
  tabFavorites: document.getElementById("tab-favorites"),
  topButton: document.getElementById("top-button"),
  bubbleLayer: document.getElementById("bubble-layer"),
  starLayer: document.getElementById("star-layer"),
};

const FAVORITES_STORAGE_KEYS = {
  clientId: "talk_index:favorites:client_id",
  favoriteIds: "talk_index:favorites:heading_ids",
  votedIds: "talk_index:favorites:voted_heading_ids",
  unsyncedIds: "talk_index:favorites:unsynced_heading_ids",
};

const AMBIENT_BUBBLE_COUNT = window.innerWidth < 700 ? 16 : 24;
const AMBIENT_STAR_COUNT = window.innerWidth < 700 ? 32 : 48;
const ambientScene = {
  bubbles: [],
  stars: [],
  width: window.innerWidth,
  height: window.innerHeight,
  lastTick: 0,
  rafId: 0,
  reducedMotion: window.matchMedia("(prefers-reduced-motion: reduce)").matches,
};

function text(value) {
  return String(value || "").trim();
}

function toJsonArray(value) {
  try {
    const parsed = JSON.parse(String(value || "[]"));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function normalizeIdSet(values) {
  return new Set((Array.isArray(values) ? values : []).map((v) => text(v)).filter(Boolean));
}

function saveFavoritesToStorage() {
  localStorage.setItem(FAVORITES_STORAGE_KEYS.clientId, state.favoritesClientId);
  localStorage.setItem(FAVORITES_STORAGE_KEYS.favoriteIds, JSON.stringify(Array.from(state.favoritedHeadingIds)));
  localStorage.setItem(FAVORITES_STORAGE_KEYS.votedIds, JSON.stringify(Array.from(state.alreadyVotedHeadingIds)));
  localStorage.setItem(FAVORITES_STORAGE_KEYS.unsyncedIds, JSON.stringify(Array.from(state.unsyncedFavoriteHeadingIds)));
}

function ensureFavoritesClientId() {
  if (state.favoritesClientId) return state.favoritesClientId;
  const existing = text(localStorage.getItem(FAVORITES_STORAGE_KEYS.clientId));
  if (existing) {
    state.favoritesClientId = existing;
    return existing;
  }
  const generated = (globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(16).slice(2)}`).replace(/\s+/g, "");
  state.favoritesClientId = generated;
  return generated;
}

function restoreFavoritesFromStorage() {
  state.favoritesClientId = text(localStorage.getItem(FAVORITES_STORAGE_KEYS.clientId));
  state.favoritedHeadingIds = normalizeIdSet(toJsonArray(localStorage.getItem(FAVORITES_STORAGE_KEYS.favoriteIds)));
  state.alreadyVotedHeadingIds = normalizeIdSet(toJsonArray(localStorage.getItem(FAVORITES_STORAGE_KEYS.votedIds)));
  state.unsyncedFavoriteHeadingIds = normalizeIdSet(toJsonArray(localStorage.getItem(FAVORITES_STORAGE_KEYS.unsyncedIds)));
  ensureFavoritesClientId();
  state.unsyncedFavoriteHeadingIds.forEach((headingId) => {
    if (state.alreadyVotedHeadingIds.has(headingId)) state.unsyncedFavoriteHeadingIds.delete(headingId);
  });
  saveFavoritesToStorage();
}

function normalizeTag(tag) {
  return text(tag).replace(/^#+/, "").trim();
}

function splitTags(raw) {
  return text(raw)
    .split(",")
    .map((item) => normalizeTag(item))
    .filter(Boolean);
}

function parseDateValue(raw) {
  const v = text(raw);
  if (!v) return "";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return "";
  return d.toISOString().slice(0, 10);
}

function compareDateDesc(a, b) {
  const aDate = parseDateValue(a);
  const bDate = parseDateValue(b);
  if (!aDate && !bDate) return 0;
  if (!aDate) return 1;
  if (!bDate) return -1;
  return bDate.localeCompare(aDate);
}

function normalizeToken(rawToken) {
  const token = text(rawToken).toLowerCase();
  if (!token) return "";
  if (TOKEN_STOP_WORDS.has(token)) return "";
  if (token.endsWith("の話")) return normalizeToken(token.slice(0, -2));
  if (token.length <= 1 && !/^\d+$/.test(token)) return "";
  return token;
}

function tokenizeText(raw) {
  const src = text(raw);
  if (!src) return [];
  const words = src.match(/[一-龠ぁ-んァ-ヶーa-zA-Z0-9]+/g) || [];
  const unique = new Set();
  words.forEach((word) => {
    const token = normalizeToken(word);
    if (token) unique.add(token);
  });
  return Array.from(unique);
}

function pushTokenSet(tokenSet, raw) {
  tokenizeText(raw).forEach((token) => tokenSet.add(token));
}

function normalizeRow(row) {
  const tags = Array.isArray(row?.tags)
    ? row.tags.map((tag) => normalizeTag(tag)).filter(Boolean)
    : splitTags(row["自動検出タグ"]);

  return {
    id: text(row.id) || text(row["URL"]) || `${text(row["タイトル"])}::${text(row["日付"])}` ,
    title: text(row.title || row["タイトル"]),
    date: text(row.date || row["日付"]),
    url: text(row.url || row["URL"]),
    section: text(row.section || row["大見出し"]),
    sectionUrl: text(row.section_url || row.sectionUrl || row["大見出しURL"]),
    subsection: text(row.subsection || row["小見出し"]),
    tags,
  };
}

function isValidHttpUrl(value) {
  const v = text(value);
  if (!v) return false;
  try {
    const u = new URL(v);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

function extractYoutubeVideoId(url) {
  if (!isValidHttpUrl(url)) return "";
  try {
    const u = new URL(url);
    if (u.hostname.includes("youtu.be")) return u.pathname.replace("/", "");
    if (u.searchParams.get("v")) return u.searchParams.get("v");
    if (u.pathname.startsWith("/shorts/")) return u.pathname.replace("/shorts/", "");
    return "";
  } catch {
    return "";
  }
}

function thumbnailUrl(url) {
  const id = extractYoutubeVideoId(url);
  return id ? `https://i.ytimg.com/vi/${id}/mqdefault.jpg` : "";
}

function toSafeDetailId(raw, fallbackKey = "") {
  const base = text(raw) || text(fallbackKey) || "unknown";
  const normalized = base.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 120);
  return normalized || "unknown";
}

function normalizeVideoSummary(video) {
  const url = text(video?.url);
  const title = text(video?.title) || "タイトルなし";
  const date = text(video?.date);
  const id = text(video?.id) || extractYoutubeVideoId(url) || toSafeDetailId(`${title}::${date}`);
  const tags = Array.isArray(video?.tags)
    ? video.tags.map((tag) => normalizeTag(tag)).filter(Boolean)
    : [];
  return {
    key: text(video?.key) || id,
    id,
    detailId: text(video?.detail_id) || id,
    title,
    date,
    url,
    tags,
    sectionCount: Number(video?.section_count) || 0,
    thumb: text(video?.thumb) || thumbnailUrl(url),
    sections: Array.isArray(video?.sections) ? video.sections : null,
    detailError: "",
    detailLoading: false,
  };
}

function groupVideos(rows) {
  const byVideo = new Map();

  rows.forEach((row) => {
    const key = row.url || `${row.title}::${row.date}`;
    if (!key) return;

    if (!byVideo.has(key)) {
      byVideo.set(key, {
        key,
        title: row.title || "タイトルなし",
        date: row.date,
        url: row.url,
        sections: new Map(),
        tags: new Set(),
      });
    }

    const video = byVideo.get(key);
    row.tags.forEach((tag) => video.tags.add(tag));

    if (!row.section) return;

    if (!video.sections.has(row.section)) {
      video.sections.set(row.section, {
        name: row.section,
        sectionUrl: row.sectionUrl,
        subsections: [],
      });
    }

    if (row.subsection) {
      video.sections.get(row.section).subsections.push({ name: row.subsection });
    }
  });

  return Array.from(byVideo.values()).map((video) => ({
    ...video,
    sections: Array.from(video.sections.values()),
    tags: Array.from(video.tags),
    thumb: thumbnailUrl(video.url),
  }));
}

function buildGenericTagSet(videos) {
  const total = videos.length;
  if (!total) return new Set();

  const counts = new Map();
  videos.forEach((video) => {
    const uniqueTags = new Set(video.tags.map((tag) => normalizeTag(tag)).filter(Boolean));
    uniqueTags.forEach((tag) => {
      counts.set(tag, (counts.get(tag) || 0) + 1);
    });
  });

  const generic = new Set();
  counts.forEach((count, tag) => {
    if (count / total > GENERIC_TAG_RATIO_THRESHOLD) {
      generic.add(tag);
    }
  });
  return generic;
}

function attachDisplayTags(videos) {
  const genericTags = buildGenericTagSet(videos);
  return videos.map((video) => ({
    ...video,
    displayTags: video.tags.filter((tag) => {
      const normalized = normalizeTag(tag);
      return !genericTags.has(normalized) && !HIDDEN_DISPLAY_TAGS.has(normalized);
    }),
  }));
}

function isTalkSectionVisible(name) {
  return name !== "【オープニングトーク】" && name !== "【エンディングトーク】" && name !== "【開場】";
}

function ensureTalkSection(bySection, row) {
  if (!bySection.has(row.section)) {
    bySection.set(row.section, {
      key: row.section,
      name: row.section,
      sectionUrl: row.sectionUrl,
      date: "",
      subsections: [],
      thumb: "",
      hasSingingVideo: false,
    });
  }
  return bySection.get(row.section);
}

function hasSingingTag(tags) {
  return tags.some((tag) => tag === "歌枠" || tag === "#歌枠");
}

function isSingingVideoRow(row) {
  return text(row.title).includes("#歌枠") || hasSingingTag(row.tags);
}


function normalizeRecommendationText(raw) {
  return text(raw)
    .toLowerCase()
    .replace(/[\s\u3000]+/g, " ")
    .replace(/[wｗ笑]+/g, " ")
    .replace(/[!！?？。\.、,，・~〜]+/g, " ")
    .replace(/[「」『』【】()（）\[\]<>＜＞]/g, " ")
    .replace(/(について|に関して|の話|という話)$/g, "")
    .trim();
}

function normalizeRecommendationToken(rawToken) {
  let token = normalizeRecommendationText(rawToken);
  if (!token) return "";
  token = token.replace(/(について|に関して|の話|という話)$/g, "");
  if (TOKEN_STOP_WORDS.has(token) || TALK_RECOMMENDATION_STOP_WORDS.has(token)) return "";
  if (token.length <= 1 && !/^\d+$/.test(token)) return "";
  if (/^\d+$/.test(token)) return "";
  return token;
}

function extractRecommendationTokens(raw) {
  const src = normalizeRecommendationText(raw);
  if (!src) return [];
  const words = src.match(/[一-龠ぁ-んァ-ヶーa-zA-Z0-9]+/g) || [];
  const unique = new Set();
  words.forEach((word) => {
    const token = normalizeRecommendationToken(word);
    if (token) unique.add(token);
  });
  return Array.from(unique);
}

function buildTalkCandidatePool(talk) {
  const scoreMap = new Map();
  const subCounts = new Map();

  const majorTokens = extractRecommendationTokens(talk.name);
  majorTokens.forEach((token) => {
    scoreMap.set(token, (scoreMap.get(token) || 0) + 6);
  });

  const subsectionTokens = [];
  talk.subsections.forEach((sub) => {
    const tokens = extractRecommendationTokens(sub.name);
    subsectionTokens.push(...tokens);
    const seen = new Set(tokens);
    seen.forEach((token) => {
      subCounts.set(token, (subCounts.get(token) || 0) + 1);
      scoreMap.set(token, (scoreMap.get(token) || 0) + 3);
    });
  });

  subCounts.forEach((count, token) => {
    if (count >= 2) scoreMap.set(token, (scoreMap.get(token) || 0) + (count - 1));
  });

  const scored = Array.from(scoreMap.entries())
    .map(([token, score]) => ({ token, score }))
    .sort((a, b) => b.score - a.score || a.token.localeCompare(b.token));

  return {
    majorTokens,
    subsectionTokens: Array.from(new Set(subsectionTokens)),
    scored,
  };
}

function pickQueryWordFromPool(candidates, usedWords) {
  const filtered = candidates.filter((item) => !usedWords.has(item.token));
  if (!filtered.length) return "";
  const topScore = filtered[0].score;
  const strongPool = filtered.filter((item) => item.score >= Math.max(1, topScore - 2)).slice(0, 5);
  if (!strongPool.length) return "";
  const picked = strongPool[Math.floor(Math.random() * strongPool.length)];
  return picked?.token || "";
}

function chooseTalkRecommendationQueryWords(talk) {
  const pool = buildTalkCandidatePool(talk);
  const usedWords = new Set();
  const words = [];

  const majorRanked = pool.scored.filter((item) => pool.majorTokens.includes(item.token));
  const majorWord = pickQueryWordFromPool(majorRanked, usedWords);
  if (majorWord) {
    words.push(majorWord);
    usedWords.add(majorWord);
  }

  const subRanked = pool.scored.filter((item) => pool.subsectionTokens.includes(item.token));
  const subWord = pickQueryWordFromPool(subRanked, usedWords);
  if (subWord) {
    words.push(subWord);
    usedWords.add(subWord);
  }

  while (words.length < TALK_RECOMMENDATION_WORD_COUNT) {
    const fallbackWord = pickQueryWordFromPool(pool.scored, usedWords);
    if (!fallbackWord) break;
    words.push(fallbackWord);
    usedWords.add(fallbackWord);
  }

  return words.slice(0, TALK_RECOMMENDATION_WORD_COUNT);
}

function buildTalkSearchDocumentsIfNeeded() {
  if (state.talkSearchDocuments) return state.talkSearchDocuments;
  state.talkSearchDocuments = state.talks.map((talk) => {
    const sourceTitle = text(talk.subsections?.[0]?.videoTitle);
    return {
      key: talk.key,
      talk,
      date: parseDateValue(talk.date),
      headingText: normalizeRecommendationText(talk.name),
      subsectionText: normalizeRecommendationText((talk.subsections || []).map((sub) => sub.name).join(" ")),
      sourceTitleText: normalizeRecommendationText(sourceTitle),
      sourceTitle,
      subsectionCount: Array.isArray(talk.subsections) ? talk.subsections.length : 0,
    };
  });
  return state.talkSearchDocuments;
}

function searchBestTalkByWord(word, currentTalkKey, usedTalkKeys) {
  const documents = buildTalkSearchDocumentsIfNeeded();
  const normalizedWord = normalizeRecommendationToken(word);
  if (!normalizedWord) return null;

  const scored = [];
  documents.forEach((doc) => {
    if (doc.key === currentTalkKey || usedTalkKeys.has(doc.key)) return;
    let score = 0;
    if (doc.headingText.includes(normalizedWord)) score += 5;
    if (doc.subsectionText.includes(normalizedWord)) score += 3;
    if (doc.sourceTitleText.includes(normalizedWord)) score += 1;
    if (score < TALK_RECOMMENDATION_MIN_SCORE) return;
    scored.push({ doc, score });
  });

  if (!scored.length) return null;
  scored.sort((a, b) => b.score - a.score);
  const topScore = scored[0].score;
  const top = scored.filter((item) => item.score === topScore);
  top.sort((a, b) => {
    if (a.doc.date && b.doc.date && a.doc.date !== b.doc.date) return a.doc.date.localeCompare(b.doc.date);
    if (a.doc.date && !b.doc.date) return -1;
    if (!a.doc.date && b.doc.date) return 1;
    return a.doc.key.localeCompare(b.doc.key);
  });

  return top[0]?.doc || null;
}

function pickFallbackTalk(currentTalk, usedTalkKeys) {
  const currentDate = parseDateValue(currentTalk.date);
  const documents = buildTalkSearchDocumentsIfNeeded().filter((doc) => {
    if (doc.key === currentTalk.key || usedTalkKeys.has(doc.key)) return false;
    if (currentDate && doc.date && doc.date > currentDate) return false;
    return true;
  });

  if (!documents.length) return null;

  documents.sort((a, b) => {
    if (state.lastFallbackTalkKey) {
      if (a.key === state.lastFallbackTalkKey) return 1;
      if (b.key === state.lastFallbackTalkKey) return -1;
    }
    if (a.subsectionCount !== b.subsectionCount) return b.subsectionCount - a.subsectionCount;
    if (a.date && b.date && a.date !== b.date) return a.date.localeCompare(b.date);
    if (a.date && !b.date) return -1;
    if (!a.date && b.date) return 1;
    return a.key.localeCompare(b.key);
  });

  const picked = documents[0] || null;
  if (picked) state.lastFallbackTalkKey = picked.key;
  return picked;
}

function formatTalkRecommendationSubtitle(doc) {
  const dateLabel = doc.date || text(doc.talk?.date);
  const source = doc.sourceTitle || "元動画情報なし";
  return dateLabel ? `${source} (${dateLabel})` : source;
}

function buildTalkRecommendationsForTalk(talk) {
  const cached = state.talkRecommendationCache.get(talk.key);
  if (cached) return cached;

  const usedTalkKeys = new Set();
  const recommendations = [];
  const words = chooseTalkRecommendationQueryWords(talk);

  words.forEach((word) => {
    const hit = searchBestTalkByWord(word, talk.key, usedTalkKeys);
    if (!hit) return;
    usedTalkKeys.add(hit.key);
    recommendations.push({
      id: hit.key,
      title: hit.talk.name || "タイトルなし",
      subtitle: formatTalkRecommendationSubtitle(hit),
      reason: `${word}といえば……`,
      searchQuery: text(talk.name),
    });
  });

  const targetCount = Math.min(2, Math.max(0, state.talks.length - 1));
  while (recommendations.length < targetCount) {
    const fallback = pickFallbackTalk(talk, usedTalkKeys);
    if (!fallback) break;
    usedTalkKeys.add(fallback.key);
    recommendations.push({
      id: fallback.key,
      title: fallback.talk.name || "タイトルなし",
      subtitle: formatTalkRecommendationSubtitle(fallback),
      reason: "こんな話題もおすすめ",
      searchQuery: text(talk.name),
    });
  }

  const finalItems = recommendations.slice(0, RECOMMEND_LIMIT);
  state.talkRecommendationCache.set(talk.key, finalItems);
  return finalItems;
}

function groupTalks(rows) {
  const bySection = new Map();

  rows.forEach((row) => {
    if (!row.section || !isTalkSectionVisible(row.section)) return;
    const talk = ensureTalkSection(bySection, row);
    if (compareDateDesc(row.date, talk.date) < 0) talk.date = row.date;
    if (!talk.thumb && row.url) talk.thumb = thumbnailUrl(row.url);
    if (isSingingVideoRow(row)) talk.hasSingingVideo = true;

    if (row.subsection) {
      talk.subsections.push({
        name: row.subsection,
        videoTitle: row.title || "タイトルなし",
        videoUrl: row.url,
      });
    }
  });

  return Array.from(bySection.values()).map((talk) => ({
    key: talk.key,
    name: talk.name,
    sectionUrl: talk.sectionUrl,
    date: talk.date,
    thumb: talk.thumb,
    hasSingingVideo: talk.hasSingingVideo,
    subsections: talk.subsections,
  }));
}

function scoreRecommendations(store, currentId) {
  const current = store.entryMap.get(currentId);
  if (!current) return [];

  const scored = new Map();

  current.tokens.forEach((token) => {
    const ids = store.tokenMap.get(token) || [];
    ids.forEach((id) => {
      if (id === currentId) return;
      if (!scored.has(id)) {
        scored.set(id, { id, score: 0, overlap: new Set(), reason: "" });
      }
      const item = scored.get(id);
      item.score += 2;
      item.overlap.add(token);
    });
  });

  scored.forEach((item) => {
    const candidate = store.entryMap.get(item.id);
    if (!candidate) return;
    if (current.date && candidate.date && current.date === candidate.date) {
      item.score += 0.5;
      item.reason = "同時期";
    }
    if (!item.reason) item.reason = `共通トークン ${item.overlap.size}件`;
    if (item.overlap.size >= 2) item.reason = "同じタグ/話題";

    // 将来拡張:
    // score に「同一配信」などの軽い加点を増やせる。
    // ただし重くなるため、全件比較や高度NLPは入れないこと。
    item.title = candidate.title;
    item.subtitle = candidate.subtitle;
    item.overlapCount = item.overlap.size;
  });

  const ranked = Array.from(scored.values())
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score || b.overlapCount - a.overlapCount);

  const related = ranked.slice(0, Math.max(0, RECOMMEND_LIMIT - 1));
  const selectedIds = new Set(related.map((item) => item.id));
  const currentTokens = new Set(current.tokens);
  const unrelatedPool = Array.from(store.entryMap.values()).filter((entry) => {
    return entry.id !== currentId && !selectedIds.has(entry.id);
  });
  const unrelatedCandidates = unrelatedPool.map((entry) => ({
    entry,
    overlap: entry.tokens.filter((token) => currentTokens.has(token)).length,
  }));
  unrelatedCandidates.sort((a, b) => a.overlap - b.overlap);
  const minOverlap = unrelatedCandidates[0]?.overlap;
  const lowestOverlapPool = unrelatedCandidates
    .filter((item) => item.overlap === minOverlap)
    .map((item) => item.entry);
  const randomIndex = lowestOverlapPool.length > 0
    ? Math.floor(Math.random() * lowestOverlapPool.length)
    : -1;
  const unrelated = randomIndex >= 0 ? lowestOverlapPool[randomIndex] : null;
  if (unrelated) {
    related.push({
      id: unrelated.id,
      title: unrelated.title,
      subtitle: unrelated.subtitle,
      reason: "まったく別の話題",
      score: -1,
      overlapCount: 0,
    });
  }
  return related.slice(0, RECOMMEND_LIMIT);
}


function buildStoreByMode(modeData) {
  const entries = Array.isArray(modeData?.entries) ? modeData.entries : [];
  const inverted = modeData?.inverted_index && typeof modeData.inverted_index === "object"
    ? modeData.inverted_index
    : {};

  const entryMap = new Map();
  entries.forEach((entry) => {
    const id = text(entry?.id);
    if (!id) return;
    entryMap.set(id, {
      id,
      title: text(entry?.title) || "タイトルなし",
      subtitle: text(entry?.subtitle),
      date: parseDateValue(entry?.date),
      tokens: Array.isArray(entry?.tokens) ? entry.tokens.map((t) => normalizeToken(t)).filter(Boolean) : [],
    });
  });

  const tokenMap = new Map();
  Object.entries(inverted).forEach(([token, ids]) => {
    const normalizedToken = normalizeToken(token);
    if (!normalizedToken || !Array.isArray(ids)) return;
    const filteredIds = ids.map((id) => text(id)).filter((id) => entryMap.has(id));
    if (filteredIds.length) tokenMap.set(normalizedToken, filteredIds);
  });

  return { tokenMap, entryMap };
}

function buildRecommendationStoreFromSearchIndex(data) {
  return {
    video: buildStoreByMode(data?.video),
    talk: buildStoreByMode(data?.talk),
  };
}

async function loadSearchIndexIfNeeded() {
  if (state.recommendation) return state.recommendation;
  if (state.searchIndexPromise) return state.searchIndexPromise;

  state.searchIndexStatus = "loading";
  state.searchIndexError = "";

  state.searchIndexPromise = (async () => {
    try {
      const data = await fetchJsonFromCandidates(SEARCH_INDEX_URL_CANDIDATES, { targetName: "search_index" });
      state.recommendation = buildRecommendationStoreFromSearchIndex(data);
      state.searchIndexStatus = "ready";
      return state.recommendation;
    } catch (error) {
      state.searchIndexStatus = "error";
      state.searchIndexError = error instanceof Error ? error.message : String(error);
      return null;
    }
  })();

  try {
    return await state.searchIndexPromise;
  } finally {
    state.searchIndexPromise = null;
  }
}

function parseSearch(raw) {
  const q = text(raw).toLowerCase();
  if (!q) return { mode: "none", keyword: "" };
  if (q.startsWith("#")) return { mode: "tag", keyword: q.slice(1).trim() };
  return { mode: "normal", keyword: q };
}

function includesKeyword(value, keyword) {
  return String(value || "").toLowerCase().includes(keyword);
}

function canSkipSearch(search) {
  return search.mode === "none" || !search.keyword;
}

function collectMatchedIdsFromStore(store, search) {
  if (!store || canSkipSearch(search) || search.mode === "tag") return null;
  const tokenMap = store.tokenMap instanceof Map ? store.tokenMap : new Map();
  if (!tokenMap.size) return new Set();

  const words = tokenizeText(search.keyword);
  const tokens = words.length ? words : [normalizeToken(search.keyword)].filter(Boolean);
  if (!tokens.length) return new Set();

  const matched = new Set();
  tokens.forEach((token) => {
    const ids = tokenMap.get(token);
    if (Array.isArray(ids)) ids.forEach((id) => matched.add(id));
  });

  if (matched.size) return matched;

  for (const [token, ids] of tokenMap.entries()) {
    if (!token.includes(search.keyword) || !Array.isArray(ids)) continue;
    ids.forEach((id) => matched.add(id));
  }
  return matched;
}

function hitVideoBySearchIndex(video, matchedIds) {
  if (!(matchedIds instanceof Set) || !matchedIds.size) return false;
  return [
    text(video?.id),
    text(video?.key),
    text(video?.detailId),
    text(video?.url),
  ].some((id) => id && matchedIds.has(id));
}

function hitTalkBySearchIndex(talk, matchedIds) {
  if (!(matchedIds instanceof Set) || !matchedIds.size) return false;
  const key = text(talk?.key);
  return !!key && matchedIds.has(key);
}

function hitVideo(video, search) {
  if (canSkipSearch(search)) return true;
  if (search.mode === "tag") {
    return video.tags.some((tag) => includesKeyword(tag, search.keyword));
  }

  if (includesKeyword(video.title, search.keyword)) return true;
  const sections = Array.isArray(video.sections) ? video.sections : [];
  return sections.some((sec) => {
    if (includesKeyword(sec.name, search.keyword)) return true;
    return sec.subsections.some((sub) => includesKeyword(sub.name, search.keyword));
  });
}

function hitTalk(talk, search) {
  if (canSkipSearch(search)) return true;
  if (search.mode === "tag") return false;
  if (includesKeyword(talk.name, search.keyword)) return true;
  return talk.subsections.some((sub) => includesKeyword(sub.name, search.keyword));
}

function createAnchor(href, label) {
  const a = document.createElement("a");
  a.href = href;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.textContent = label;
  return a;
}

function buildHeadingFormattedFragment(raw) {
  const fragment = document.createDocumentFragment();
  const value = String(raw || "");
  const pattern = /(\*\*[^*]+\*\*|\*[^*]+\*|--[^-]+--|-[^-]+-)/g;
  let lastIndex = 0;

  value.replace(pattern, (matched, _unused, offset) => {
    if (offset > lastIndex) {
      fragment.appendChild(document.createTextNode(value.slice(lastIndex, offset)));
    }

    let content = matched;
    let node = document.createTextNode(content);
    if (matched.startsWith("**") && matched.endsWith("**")) {
      content = matched.slice(2, -2);
      const strong = document.createElement("strong");
      strong.appendChild(document.createTextNode(content));
      node = strong;
    } else if (matched.startsWith("*") && matched.endsWith("*")) {
      content = matched.slice(1, -1);
      const strong = document.createElement("strong");
      strong.appendChild(document.createTextNode(content));
      node = strong;
    } else if (matched.startsWith("--") && matched.endsWith("--")) {
      content = matched.slice(2, -2);
      const s = document.createElement("s");
      s.appendChild(document.createTextNode(content));
      node = s;
    } else if (matched.startsWith("-") && matched.endsWith("-")) {
      content = matched.slice(1, -1);
      const s = document.createElement("s");
      s.appendChild(document.createTextNode(content));
      node = s;
    }

    fragment.appendChild(node);
    lastIndex = offset + matched.length;
    return matched;
  });

  if (lastIndex < value.length) {
    fragment.appendChild(document.createTextNode(value.slice(lastIndex)));
  }

  return fragment;
}


function lerpColorHex(fromHex, toHex, ratio) {
  const r = Math.min(1, Math.max(0, ratio));
  const from = fromHex.replace("#", "");
  const to = toHex.replace("#", "");
  const fromR = parseInt(from.slice(0, 2), 16);
  const fromG = parseInt(from.slice(2, 4), 16);
  const fromB = parseInt(from.slice(4, 6), 16);
  const toR = parseInt(to.slice(0, 2), 16);
  const toG = parseInt(to.slice(2, 4), 16);
  const toB = parseInt(to.slice(4, 6), 16);

  const mix = (a, b) => Math.round(a + (b - a) * r);
  const next = [mix(fromR, toR), mix(fromG, toG), mix(fromB, toB)]
    .map((v) => v.toString(16).padStart(2, "0"))
    .join("");
  return `#${next}`;
}

function updateScrollGradient() {
  const scrollTop = window.scrollY || document.documentElement.scrollTop || 0;
  const maxScroll = Math.max(document.documentElement.scrollHeight - window.innerHeight, 1);
  const ratio = Math.min(scrollTop / maxScroll, 1);
  const color = lerpColorHex("#87ceeb", "#045a8d", ratio);
  document.documentElement.style.setProperty("--scroll-marine", color);
}

function updateAmbientTransitionByCards() {
  if (!refs.bubbleLayer || !refs.starLayer) return;

  const cards = refs.results?.querySelectorAll?.(".card") || [];
  if (!cards.length) return;

  const firstCard = cards[0];
  const halfIndex = Math.floor((cards.length - 1) * 0.5);
  const targetCard = cards[Math.min(halfIndex, cards.length - 1)];
  if (!firstCard || !targetCard) return;

  const scrollY = window.scrollY || document.documentElement.scrollTop || 0;
  const firstCardY = firstCard.getBoundingClientRect().top + scrollY;
  const targetCardY = targetCard.getBoundingClientRect().top + scrollY;

  const startY = firstCardY - (window.innerHeight * 0.2);
  const endY = targetCardY;
  const distance = Math.max(endY - startY, 1);
  const rawProgress = (scrollY - startY) / distance;
  const progress = Math.min(Math.max(rawProgress, 0), 1);

  document.documentElement.style.setProperty("--ambient-cosmos-progress", progress.toFixed(3));

  if (progress >= 0.999) {
    refs.bubbleLayer.style.opacity = "0";
    refs.bubbleLayer.style.visibility = "hidden";
    refs.starLayer.style.opacity = "1";
    refs.starLayer.style.visibility = "visible";
    return;
  }

  refs.bubbleLayer.style.opacity = (1 - progress).toFixed(3);
  refs.bubbleLayer.style.visibility = "visible";
  refs.starLayer.style.opacity = progress.toFixed(3);
  refs.starLayer.style.visibility = "visible";
}

function randomBetween(min, max) {
  return min + (max - min) * Math.random();
}

function resetBubble(bubble, randomY = false) {
  const width = ambientScene.width;
  const height = ambientScene.height;
  bubble.x = randomBetween(-20, width + 20);
  bubble.y = randomY ? randomBetween(-height, height + 40) : height + randomBetween(20, 180);
  bubble.baseX = bubble.x;
  bubble.wobbleOffset = randomBetween(0, Math.PI * 2);
}

function createAmbientBubble() {
  const node = document.createElement("span");
  node.className = "ambient-bubble";
  const size = randomBetween(6, 16);
  node.style.width = `${size}px`;
  node.style.height = `${size}px`;
  refs.bubbleLayer.appendChild(node);

  const bubble = {
    node,
    size,
    x: 0,
    y: 0,
    baseX: 0,
    speed: randomBetween(18, 48),
    wobbleAmp: randomBetween(4, 16),
    wobbleSpeed: randomBetween(0.7, 1.8),
    wobbleOffset: randomBetween(0, Math.PI * 2),
  };
  resetBubble(bubble, true);
  return bubble;
}

function createAmbientStar() {
  const node = document.createElement("span");
  node.className = "ambient-star";
  const size = randomBetween(10, 18);
  node.textContent = "☆";
  node.style.fontSize = `${size}px`;
  node.style.left = `${randomBetween(0, 100)}%`;
  node.style.top = `${randomBetween(3, 97)}%`;
  node.style.setProperty("--twinkle-duration", `${randomBetween(4.5, 9)}s`);
  node.style.setProperty("--twinkle-delay", `${randomBetween(-8, 0)}s`);
  refs.starLayer.appendChild(node);
  return node;
}

function updateAmbientBubbles(deltaSec, elapsedSec) {
  if (!ambientScene.bubbles.length) return;
  ambientScene.bubbles.forEach((bubble) => {
    bubble.y -= bubble.speed * deltaSec;
    if (bubble.y < -bubble.size - 16) {
      resetBubble(bubble, false);
    }
    const sway = Math.sin((elapsedSec * bubble.wobbleSpeed) + bubble.wobbleOffset) * bubble.wobbleAmp;
    bubble.x = bubble.baseX + sway;
    bubble.node.style.transform = `translate3d(${bubble.x}px, ${bubble.y}px, 0)`;
  });
}

function animateAmbientScene(timestamp) {
  if (ambientScene.reducedMotion) return;
  if (!ambientScene.lastTick) ambientScene.lastTick = timestamp;
  const deltaSec = Math.min((timestamp - ambientScene.lastTick) / 1000, 0.05);
  ambientScene.lastTick = timestamp;
  updateAmbientBubbles(deltaSec, timestamp / 1000);
  ambientScene.rafId = window.requestAnimationFrame(animateAmbientScene);
}

function refreshAmbientViewport() {
  ambientScene.width = window.innerWidth;
  ambientScene.height = window.innerHeight;
}

function initAmbientScene() {
  if (!refs.bubbleLayer || !refs.starLayer) return;
  refs.bubbleLayer.innerHTML = "";
  refs.starLayer.innerHTML = "";

  ambientScene.bubbles = Array.from({ length: AMBIENT_BUBBLE_COUNT }, () => createAmbientBubble());
  ambientScene.stars = Array.from({ length: AMBIENT_STAR_COUNT }, () => createAmbientStar());

  if (ambientScene.reducedMotion) {
    ambientScene.bubbles.forEach((bubble) => {
      bubble.node.style.transform = `translate3d(${bubble.x}px, ${bubble.y}px, 0)`;
    });
    return;
  }
  ambientScene.rafId = window.requestAnimationFrame(animateAmbientScene);
}

function createHeadingFormattedSpan(raw) {
  const span = document.createElement("span");
  span.appendChild(buildHeadingFormattedFragment(raw));
  return span;
}

function createHeadingFormattedAnchor(href, label) {
  const a = document.createElement("a");
  a.href = href;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.appendChild(buildHeadingFormattedFragment(label));
  return a;
}

function getHeadingIdFromObject(obj, fallback = "") {
  return text(obj?.headingId || obj?.heading_id || obj?.id || obj?.key || fallback);
}

function findTalkByHeadingId(headingId) {
  const normalized = text(headingId);
  if (!normalized) return null;
  return state.talks.find((talk) => getHeadingIdFromObject(talk, talk.name) === normalized)
    || state.talks.find((talk) => text(talk.name) === normalized)
    || null;
}

function isFavoritedHeading(headingId) {
  return state.favoritedHeadingIds.has(text(headingId));
}

function createFavoriteButton(headingId, extraClass = "", onToggle = null) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = `favorite-toggle${extraClass ? ` ${extraClass}` : ""}`;
  const normalized = text(headingId);
  const active = isFavoritedHeading(normalized);
  button.textContent = active ? "★" : "☆";
  button.setAttribute("aria-label", active ? "お気に入り解除" : "お気に入り登録");
  button.setAttribute("aria-pressed", active ? "true" : "false");
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    event.preventDefault();
    if (onToggle) {
      void onToggle(normalized);
      return;
    }
    void toggleFavoriteHeading(normalized);
  });
  return button;
}

async function sendFavoriteVote(payload) {
  let lastError = "";
  for (const baseUrl of FAVORITES_WRITE_BASE_URL_CANDIDATES) {
    try {
      const result = await sendFavoriteVoteRequest(baseUrl, payload);
      return { ...result, baseUrl };
    } catch (error) {
      lastError = `${baseUrl}: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  throw new Error(lastError || "favorites vote 送信に失敗しました");
}

async function fetchFavoritesAggregate(kind) {
  const loaders = {
    recent: fetchRecentRecommendations,
    hall: fetchHallOfFame,
  };
  const load = loaders[kind];
  if (!load) throw new Error(`unsupported favorites aggregate kind: ${kind}`);

  let lastError = null;
  for (const baseUrl of FAVORITES_READ_BASE_URL_CANDIDATES) {
    try {
      return await load(baseUrl);
    } catch (error) {
      lastError = error;
      console.warn(`[favorites] ${kind} aggregate fetch failed`, { kind, baseUrl, error });
    }
  }

  throw (lastError instanceof Error ? lastError : new Error(`favorites aggregate(${kind}) の取得に失敗しました。`));
}

async function syncFavoriteVote(headingId, sourceTalk = null) {
  const normalized = text(headingId);
  if (!normalized || !state.favoritedHeadingIds.has(normalized)) return false;
  if (state.alreadyVotedHeadingIds.has(normalized)) {
    state.unsyncedFavoriteHeadingIds.delete(normalized);
    saveFavoritesToStorage();
    return true;
  }
  const talk = sourceTalk || findTalkByHeadingId(normalized);
  try {
    await sendFavoriteVote({
      headingId: normalized,
      headingTitle: text(talk?.name || talk?.headingTitle || normalized),
      videoId: text(talk?.videoId),
      videoTitle: text(talk?.subsections?.[0]?.videoTitle || talk?.videoTitle),
      sourceMode: state.viewMode,
      clientId: ensureFavoritesClientId(),
      timestamp: new Date().toISOString(),
    });
    state.alreadyVotedHeadingIds.add(normalized);
    state.unsyncedFavoriteHeadingIds.delete(normalized);
    saveFavoritesToStorage();
    if (state.unsyncedFavoriteHeadingIds.size === 0 && refs.notice.textContent.includes("投票は未同期")) {
      refs.notice.textContent = "";
    }
    return true;
  } catch (error) {
    state.unsyncedFavoriteHeadingIds.add(normalized);
    saveFavoritesToStorage();
    console.warn("[favorites] vote sync failed", error);
    return false;
  }
}

async function retryUnsyncedFavoriteVotes() {
  const targets = Array.from(state.unsyncedFavoriteHeadingIds).filter((headingId) => state.favoritedHeadingIds.has(headingId));
  if (targets.length === 0) return;
  for (const headingId of targets) {
    await syncFavoriteVote(headingId);
  }
}

async function loadFavoritesDataIfNeeded() {
  if (state.favoritesDataStatus === "ready") return;
  if (state.favoritesDataStatus === "loading") return;
  state.favoritesDataStatus = "loading";
  state.favoritesDataError = "";
  render();
  try {
    const [recent, hall] = await Promise.all([
      fetchFavoritesAggregate("recent"),
      fetchFavoritesAggregate("hall"),
    ]);
    state.favoritesRecent = recent;
    state.favoritesHall = hall;
    state.favoritesDataStatus = "ready";
  } catch (error) {
    console.warn("[favorites] aggregate load failed", error);
    state.favoritesDataStatus = "error";
  }
}

async function toggleFavoriteHeading(headingId, sourceTalk = null) {
  const normalized = text(headingId);
  if (!normalized) return;
  const alreadyFavorite = state.favoritedHeadingIds.has(normalized);
  if (alreadyFavorite) {
    state.favoritedHeadingIds.delete(normalized);
    saveFavoritesToStorage();
    render();
    return;
  }

  state.favoritedHeadingIds.add(normalized);
  state.unsyncedFavoriteHeadingIds.add(normalized);
  saveFavoritesToStorage();
  render();
  await syncFavoriteVote(normalized, sourceTalk);
}

function getFilteredVideos(search = parseSearch(state.search)) {
  const matchedIds = collectMatchedIdsFromStore(state.recommendation?.video, search);
  if (matchedIds instanceof Set) {
    return state.videos
      .filter((video) => hitVideoBySearchIndex(video, matchedIds))
      .slice()
      .sort((a, b) => compareDateDesc(a.date, b.date));
  }
  return state.videos
    .filter((video) => hitVideo(video, search))
    .slice()
    .sort((a, b) => compareDateDesc(a.date, b.date));
}

function getFilteredTalks(search = parseSearch(state.search)) {
  const matchedIds = collectMatchedIdsFromStore(state.recommendation?.talk, search);
  if (matchedIds instanceof Set) {
    let filteredByIndex = state.talks
      .filter((talk) => hitTalkBySearchIndex(talk, matchedIds))
      .slice()
      .sort((a, b) => compareDateDesc(a.date, b.date));
    if (state.randomTalkKeys) {
      filteredByIndex = filteredByIndex.filter((talk) => state.randomTalkKeys.has(talk.key));
    }
    return filteredByIndex;
  }
  let filtered = state.talks
    .filter((talk) => hitTalk(talk, search))
    .slice()
    .sort((a, b) => compareDateDesc(a.date, b.date));

  if (state.randomTalkKeys) {
    filtered = filtered.filter((talk) => state.randomTalkKeys.has(talk.key));
  }
  return filtered;
}

function getDisplayedVideoOpenKeys(videos) {
  if (state.isVideoExpandLock) {
    return new Set(videos.map((video) => video.key));
  }
  return state.openVideoKeys;
}

function updateToggleAllButton() {
  const search = parseSearch(state.search);
  let total = 0;
  let openCount = 0;
  if (state.viewMode === "video") {
    const videos = getFilteredVideos(search);
    total = videos.length;
    openCount = getDisplayedVideoOpenKeys(videos).size;
  } else if (state.viewMode === "talk") {
    const talks = getFilteredTalks(search);
    total = talks.length;
    openCount = state.openTalkKeys.size;
  } else {
    refs.toggleAll.textContent = "全て展開";
    refs.toggleAll.disabled = true;
    return;
  }
  refs.toggleAll.disabled = false;
  const allOpen = total > 0 && openCount === total;
  refs.toggleAll.textContent = allOpen ? "全て折り畳み" : "全て展開";
}

function updateTabs() {
  const isVideo = state.viewMode === "video";
  const isTalk = state.viewMode === "talk";
  const isFavorites = state.viewMode === "favorites";
  refs.tabVideo.classList.toggle("is-active", isVideo);
  refs.tabTalk.classList.toggle("is-active", isTalk);
  refs.tabFavorites.classList.toggle("is-active", isFavorites);
  refs.tabVideo.setAttribute("aria-selected", isVideo ? "true" : "false");
  refs.tabTalk.setAttribute("aria-selected", isTalk ? "true" : "false");
  refs.tabFavorites.setAttribute("aria-selected", isFavorites ? "true" : "false");
}

function updateServerStatus(mode, shownCount = 0) {
  refs.serverStatus.classList.remove("server-status--loading", "server-status--error", "server-status--ok");
  if (mode === "loading") {
    refs.serverStatus.textContent = "読込中…";
    refs.serverStatus.classList.add("server-status--loading");
    return;
  }
  if (mode === "error") {
    refs.serverStatus.textContent = "❕エラー";
    refs.serverStatus.classList.add("server-status--error");
    return;
  }
  refs.serverStatus.textContent = `⚡稼働中(全${shownCount}件)`;
  refs.serverStatus.classList.add("server-status--ok");
}

function renderNoResult() {
  refs.results.innerHTML = "";

  const message = document.createElement("p");
  message.textContent = getModeMessage(state.viewMode, "noResult");

  const fallback = document.createElement("a");
  fallback.className = "no-results-fallback";
  fallback.href = SONG_DB_URL;
  fallback.target = "_blank";
  fallback.rel = "noopener noreferrer";
  fallback.textContent = "歌枠DBも見る";

  refs.results.append(message, fallback);
}

function pickAmbientTone(values) {
  const joined = values.map((v) => text(v)).join(" ").toLowerCase();
  if (/爆笑|笑い|神回|ハイテンション/.test(joined)) return "lively";
  if (/深夜|チル|まったり|海|波|ラジオ/.test(joined)) return "night";
  if (/しんみり|悩み|相談/.test(joined)) return "calm";
  return "base";
}

function createRecommendationBlock(items, mode) {
  const wrap = document.createElement("section");
  wrap.className = "recommend";

  const title = document.createElement("h4");
  title.className = "recommend-title";
  title.textContent = "おすすめ🫵";
  wrap.appendChild(title);

  if (!items.length) {
    const empty = document.createElement("p");
    empty.className = "recommend-empty";
    empty.textContent = "関連候補は準備中です";
    wrap.appendChild(empty);
    return wrap;
  }

  const list = document.createElement("ul");
  list.className = "recommend-list";
  items.forEach((item, index) => {
    const li = document.createElement("li");
    li.className = "recommend-item";
    li.style.animationDelay = `${index * 60}ms`;

    const button = document.createElement("button");
    button.type = "button";
    button.className = "recommend-button";

    const main = document.createElement("span");
    main.className = "recommend-main";
    main.textContent = item.title;

    const sub = document.createElement("span");
    sub.className = "recommend-sub";
    sub.textContent = item.subtitle;

    const reason = document.createElement("span");
    reason.className = "recommend-reason";
    reason.textContent = item.reason;

    button.append(main, sub, reason);
    button.addEventListener("click", () => {
      void applyRecommendationSearch(item);
    });

    li.appendChild(button);
    list.appendChild(li);
  });
  wrap.appendChild(list);
  return wrap;
}

async function applyRecommendationSearch(item) {
  const searchQuery = text(item?.searchQuery) || text(item?.title);
  if (!searchQuery) return;

  state.viewMode = "video";
  state.search = searchQuery;
  refs.search.value = searchQuery;
  state.randomTalkKeys = null;
  state.randomSection = "";
  state.openTalkKeys = new Set();
  state.openVideoKeys = new Set();
  state.isVideoExpandLock = false;
  state.videoAutoCollapseAnchor = null;

  if (state.searchIndexStatus === "idle") {
    await loadSearchIndexIfNeeded();
  }

  render();
  requestAnimationFrame(() => {
    window.scrollTo({ top: 0, behavior: "smooth" });
    refs.search.focus();
  });
}

function openCardFromRecommendation(mode, key) {
  if (mode === "video") {
    state.viewMode = "video";
    state.openVideoKeys.add(key);
  } else {
    state.viewMode = "talk";
    state.openTalkKeys.add(key);
  }
  state.randomTalkKeys = null;
  state.randomSection = "";
  render();
  requestAnimationFrame(() => {
    const card = refs.results.querySelector(`.card[data-key="${CSS.escape(key)}"]`);
    if (card) card.scrollIntoView({ behavior: "smooth", block: "center" });
  });
}

function updateNewVideoHighlightVisibility() {
  const threshold = window.innerHeight * NEW_VIDEO_HIGHLIGHT_SCROLL_SCREENS;
  state.isNewVideoHighlightVisible = window.scrollY <= threshold;
}

function pickNewVideoHighlightKeys(videos) {
  const latestVideo = videos
    .map((video) => ({ ...video, parsedDate: parseDateValue(video.date) }))
    .filter((video) => video.parsedDate)
    .sort((a, b) => b.parsedDate.localeCompare(a.parsedDate))[0];

  if (!latestVideo) return new Set();
  return new Set([latestVideo.key].slice(0, NEW_VIDEO_HIGHLIGHT_COUNT));
}

function renderCards(videos) {
  if (!videos.length) return renderNoResult();

  refs.results.innerHTML = "";
  const openKeys = getDisplayedVideoOpenKeys(videos);
  const lockClass = state.isVideoExpandLock ? " is-expand-locked" : "";

  videos.forEach((video, index) => {
    const card = document.createElement("article");
    card.className = `card${lockClass}`;
    card.dataset.key = video.key;
    const sectionsForTone = Array.isArray(video.sections) ? video.sections : [];
    card.dataset.tone = pickAmbientTone([video.title, ...video.tags, ...sectionsForTone.map((sec) => sec.name)]);
    if (openKeys.has(video.key)) card.classList.add("is-open");
    if (state.isNewVideoHighlightVisible && state.newVideoHighlightKeys.has(video.key)) card.classList.add("is-new-highlight");

    const summary = document.createElement("div");
    summary.className = "card-summary";

    const main = document.createElement("div");
    main.className = "card-main";

    const titleRow = document.createElement("div");
    titleRow.className = "card-title-row";
    if (isValidHttpUrl(video.url)) {
      titleRow.appendChild(createAnchor(video.url, video.title));
    } else {
      titleRow.textContent = video.title;
    }

    const metaRow = document.createElement("div");
    metaRow.className = "card-meta-row";
    metaRow.textContent = `${video.sectionCount || 0}件`;

    main.append(titleRow, metaRow);

    const side = document.createElement("div");
    side.className = "card-side";

    const thumb = document.createElement("img");
    thumb.className = "thumbnail";
    thumb.alt = "サムネイル";
    thumb.loading = "lazy";
    if (video.thumb) {
      thumb.src = video.thumb;
    }

    const dateCorner = document.createElement("div");
    dateCorner.className = "card-date-corner";
    dateCorner.textContent = video.date || "日付なし";

    side.append(thumb, dateCorner);
    summary.append(main, side);

    const detail = document.createElement("div");
    detail.className = "card-detail";

    const sectionList = document.createElement("div");
    sectionList.className = "section-list";

    if (video.detailLoading) {
      const loading = document.createElement("p");
      loading.className = "detail-status";
      loading.textContent = "詳細を読込中…";
      sectionList.appendChild(loading);
    } else if (Array.isArray(video.sections)) {
      video.sections.forEach((sec) => {
        const item = document.createElement("div");
        item.className = "section-item";

        const head = document.createElement("div");
        head.className = "section-head";

        const hasSubsections = sec.subsections.length > 0;
        let toggle;
        if (hasSubsections) {
          toggle = document.createElement("button");
          toggle.className = "section-toggle";
          toggle.type = "button";
          toggle.setAttribute("aria-expanded", "false");
          toggle.textContent = "▶";
        } else {
          toggle = document.createElement("span");
          toggle.className = "section-toggle-placeholder";
        }

        const label = sec.sectionUrl && isValidHttpUrl(sec.sectionUrl)
          ? createHeadingFormattedAnchor(sec.sectionUrl, sec.name)
          : createHeadingFormattedSpan(sec.name);
        label.classList.add("section-link");
        const headingId = getHeadingIdFromObject(sec, sec.name);
        const sourceTalk = findTalkByHeadingId(headingId);
        const favoriteButton = createFavoriteButton(headingId, "favorite-toggle--section", (id) => toggleFavoriteHeading(id, sourceTalk));
        head.append(toggle, label, favoriteButton);

        const subList = document.createElement("ul");
        subList.className = "sub-list";
        sec.subsections.forEach((sub) => {
          const li = document.createElement("li");
          li.appendChild(document.createTextNode("- "));
          li.appendChild(buildHeadingFormattedFragment(sub.name));
          subList.appendChild(li);
        });

        if (hasSubsections) {
          toggle.addEventListener("click", (event) => {
            event.stopPropagation();
            const open = subList.classList.toggle("is-open");
            toggle.textContent = open ? "▼" : "▶";
            toggle.setAttribute("aria-expanded", open ? "true" : "false");
          });
        }

        item.append(head, subList);
        sectionList.appendChild(item);
      });
    } else {
      const placeholder = document.createElement("p");
      placeholder.className = "detail-status";
      placeholder.textContent = "クリックで詳細を読み込みます";
      sectionList.appendChild(placeholder);
    }

    const tags = document.createElement("div");
    tags.className = "tags";
    const tagsForDisplay = Array.isArray(video.displayTags) ? video.displayTags : video.tags;
    if (tagsForDisplay.length) {
      tagsForDisplay.forEach((tag) => {
        const el = document.createElement("span");
        el.className = "tag";
        el.textContent = `#${tag}`;
        tags.appendChild(el);
      });
    } else {
      const empty = document.createElement("span");
      empty.className = "tag";
      empty.textContent = "#タグなし";
      tags.appendChild(empty);
    }

    detail.append(sectionList, tags);
    summary.addEventListener("click", async () => {
      if (state.isVideoExpandLock) return;
      if (state.openVideoKeys.has(video.key)) {
        state.openVideoKeys.delete(video.key);
        if (state.videoAutoCollapseAnchor?.key === video.key) {
          state.videoAutoCollapseAnchor = null;
        }
      } else {
        state.openVideoKeys.add(video.key);
        state.videoAutoCollapseAnchor = { key: video.key, index };
        if (!Array.isArray(video.sections)) {
          render();
          await ensureVideoDetailsLoaded(video);
        }
      }
      render();
    });

    card.append(summary, detail);
    refs.results.appendChild(card);
  });
}

function renderTalkCards(talks, options = {}) {
  if (!talks.length) return renderNoResult();

  const {
    appendToResults = true,
    showRecommendations = true,
    showFavoriteButton = true,
  } = options;

  if (appendToResults) refs.results.innerHTML = "";
  const container = appendToResults ? refs.results : document.createElement("div");
  if (!appendToResults) {
    container.className = "results favorite-panel-results";
  }

  talks.forEach((talk) => {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.key = talk.key;
    card.dataset.tone = pickAmbientTone([talk.name, ...talk.subsections.map((sub) => sub.name)]);
    if (state.openTalkKeys.has(talk.key)) card.classList.add("is-open");

    const summary = document.createElement("div");
    summary.className = "card-summary";

    const main = document.createElement("div");
    main.className = "card-main";

    const titleRow = document.createElement("div");
    titleRow.className = "card-title-row";
    const titleLabel = document.createElement("span");
    if (talk.sectionUrl && isValidHttpUrl(talk.sectionUrl)) {
      titleLabel.appendChild(createHeadingFormattedAnchor(talk.sectionUrl, talk.name));
    } else {
      titleLabel.appendChild(createHeadingFormattedSpan(talk.name));
    }
    titleRow.appendChild(titleLabel);
    if (showFavoriteButton) {
      titleRow.appendChild(
        createFavoriteButton(getHeadingIdFromObject(talk, talk.name), "favorite-toggle--row", (id) => toggleFavoriteHeading(id, talk)),
      );
    }

    const metaRow = document.createElement("div");
    metaRow.className = "card-meta-row";
    metaRow.textContent = `小見出し ${talk.subsections.length}件`;

    main.append(titleRow, metaRow);

    summary.append(main);

    const detail = document.createElement("div");
    detail.className = "card-detail";

    const subList = document.createElement("ul");
    subList.className = "sub-list is-open";
    talk.subsections.forEach((sub) => {
      const li = document.createElement("li");
      const subsectionText = document.createElement("div");
      subsectionText.appendChild(document.createTextNode("- "));
      subsectionText.appendChild(buildHeadingFormattedFragment(sub.name));
      li.appendChild(subsectionText);

      subList.appendChild(li);
    });

    const firstSub = talk.subsections[0];
    if (firstSub) {
      const videoTitle = document.createElement("li");
      videoTitle.className = "talk-video-title";
      videoTitle.appendChild(document.createTextNode("元動画: "));
      if (isValidHttpUrl(firstSub.videoUrl)) {
        videoTitle.appendChild(createAnchor(firstSub.videoUrl, firstSub.videoTitle));
      } else {
        videoTitle.appendChild(document.createTextNode(firstSub.videoTitle));
      }
      subList.appendChild(videoTitle);
    }

    detail.appendChild(subList);
    if (showRecommendations && state.openTalkKeys.has(talk.key)) {
      const recommendations = buildTalkRecommendationsForTalk(talk);
      detail.appendChild(createRecommendationBlock(recommendations, "talk"));
    }

    summary.addEventListener("click", () => {
      if (state.openTalkKeys.has(talk.key)) {
        state.openTalkKeys.delete(talk.key);
      } else {
        state.openTalkKeys.add(talk.key);
      }
      card.classList.toggle("is-open");
      updateToggleAllButton();
    });

    card.append(summary, detail);
    container.appendChild(card);
  });

  return container;
}

function createFavoritePanel(title, key, talks, options = {}) {
  const { meta = "", showMeta = true } = options;
  const wrap = document.createElement("article");
  wrap.className = "favorite-panel";

  const head = document.createElement("button");
  head.type = "button";
  head.className = "favorite-panel-head";
  const isOpen = state.favoritePanelOpenKeys.has(key);
  head.setAttribute("aria-expanded", isOpen ? "true" : "false");

  const titleEl = document.createElement("span");
  titleEl.className = "favorite-panel-title";
  titleEl.textContent = title;

  if (!showMeta) {
    head.classList.add("favorite-panel-head--no-meta");
  }

  const metaEl = showMeta ? document.createElement("span") : null;
  if (metaEl) {
    metaEl.className = "favorite-panel-meta";
    metaEl.textContent = meta || `${talks.length}件`;
  }
  const marker = document.createElement("span");
  marker.className = "favorite-panel-marker";
  marker.textContent = isOpen ? "▼" : "▶";
  head.append(titleEl);
  if (metaEl) head.append(metaEl);
  head.append(marker);

  const body = document.createElement("div");
  body.className = "favorite-panel-body";
  if (isOpen) {
    if (talks.length) {
      body.appendChild(renderTalkCards(talks, { appendToResults: false, showRecommendations: false, showFavoriteButton: true }));
    } else {
      const empty = document.createElement("p");
      empty.className = "favorite-panel-empty";
      empty.textContent = "データがありません";
      body.appendChild(empty);
    }
  }

  head.addEventListener("click", () => {
    if (state.favoritePanelOpenKeys.has(key)) state.favoritePanelOpenKeys.delete(key);
    else state.favoritePanelOpenKeys.add(key);
    render();
  });

  wrap.append(head, body);
  return wrap;
}

function renderFavoritesTab() {
  refs.results.innerHTML = "";
  const favoriteTalks = Array.from(state.favoritedHeadingIds)
    .map((headingId) => findTalkByHeadingId(headingId))
    .filter(Boolean);

  const recentItems = Array.isArray(state.favoritesRecent?.items) ? state.favoritesRecent.items : [];
  const hallItems = Array.isArray(state.favoritesHall?.items) ? state.favoritesHall.items : [];
  const recentTalks = recentItems.map((item) => findTalkByHeadingId(getHeadingIdFromObject(item))).filter(Boolean);
  const hallTalks = hallItems.map((item) => findTalkByHeadingId(getHeadingIdFromObject(item))).filter(Boolean);

  const favoriteMeta = favoriteTalks.length ? `${favoriteTalks.length}件` : "0件";
  refs.results.appendChild(createFavoritePanel("お気に入りリスト", "mine", favoriteTalks, { meta: favoriteMeta, showMeta: true }));

  refs.results.appendChild(createFavoritePanel("最近のおすすめ", "recent", recentTalks, { showMeta: false }));

  const hallDisplayTalks = hallTalks.slice(0, 5);
  refs.results.appendChild(createFavoritePanel("殿堂入り", "hall", hallDisplayTalks, { showMeta: false }));

}

function render() {
  const search = parseSearch(state.search);
  const isVideo = state.viewMode === "video";
  const isTalkLike = state.viewMode === "talk" || state.viewMode === "favorites";
  if (isTalkLike && state.talksStatus === "loading") {
    const loadingMessage = getModeMessage("talk", "loading");
    refs.notice.textContent = loadingMessage;
    refs.results.innerHTML = `<p>${loadingMessage}</p>`;
    updateTabs();
    updateServerStatus("ok", 0);
    updateToggleAllButton();
    return;
  }
  if (isTalkLike && state.talksStatus === "error") {
    refs.notice.textContent = "";
    refs.results.innerHTML = `<p>${getModeMessage("talk", "loadFailed")}</p>`;
    updateTabs();
    updateServerStatus("ok", 0);
    updateToggleAllButton();
    return;
  }
  const filtered = isVideo ? getFilteredVideos(search) : getFilteredTalks(search);

  refs.notice.textContent = state.talksFallbackActive && isTalkLike
    ? getModeMessage("talk", "fallback")
    : "";

  updateTabs();
  updateServerStatus("ok", state.viewMode === "favorites" ? state.favoritedHeadingIds.size : filtered.length);
  updateToggleAllButton();
  if (isVideo) {
    renderCards(filtered);
  } else if (state.viewMode === "talk") {
    renderTalkCards(filtered);
  } else {
    renderFavoritesTab();
  }

  window.requestAnimationFrame(() => {
    updateAmbientTransitionByCards();
  });
}

async function fetchInitialVideos() {
  updateServerStatus("loading");
  const data = await fetchJsonFromCandidates(DATA_URL_CANDIDATES, { targetName: "latest" });
  const videos = Array.isArray(data?.videos) ? data.videos : null;
  if (Array.isArray(videos)) {
    return videos.map((video) => normalizeVideoSummary(video));
  }

  const rows = Array.isArray(data) ? data : (Array.isArray(data.items) ? data.items : data.rows);
  if (!Array.isArray(rows)) throw createInvalidJsonShapeError();
  const normalized = [];
  rows.forEach((raw) => {
    const row = normalizeRow(raw || {});
    if (!row.title || !row.section) {
      state.skippedRows += 1;
      return;
    }
    normalized.push(row);
  });
  return attachDisplayTags(groupVideos(normalized)).map((video) => ({
    ...normalizeVideoSummary({
      id: video.key,
      key: video.key,
      title: video.title,
      date: video.date,
      url: video.url,
      tags: video.tags,
      section_count: video.sections.length,
      thumb: video.thumb,
      sections: video.sections,
    }),
    displayTags: video.displayTags,
  }));
}

function buildDetailUrlCandidates(detailId) {
  const safeId = toSafeDetailId(detailId);
  return DATA_URL_CANDIDATES
    .map((url) => String(url || "").replace(/latest\.json$/, `video-details/${safeId}.json`))
    .filter((url, index, self) => url && self.indexOf(url) === index);
}

async function ensureVideoDetailsLoaded(video) {
  if (!video || Array.isArray(video.sections)) return;
  const detailId = toSafeDetailId(video.detailId || video.id || video.key);
  if (state.videoDetailsCache.has(detailId)) {
    video.sections = state.videoDetailsCache.get(detailId);
    video.sectionCount = video.sections.length;
    return;
  }
  if (state.videoDetailsPromises.has(detailId)) {
    await state.videoDetailsPromises.get(detailId);
    return;
  }

  video.detailLoading = true;
  video.detailError = "";
  const promise = (async () => {
    try {
      const data = await fetchJsonFromCandidates(buildDetailUrlCandidates(detailId), { targetName: "video-details" });
      const sections = Array.isArray(data?.sections) ? data.sections : [];
      state.videoDetailsCache.set(detailId, sections);
      video.sections = sections;
      video.sectionCount = sections.length;
      video.detailLoading = false;
    } catch (error) {
      video.detailLoading = false;
      video.detailError = "詳細の読込に失敗しました";
      console.warn("[video-details] load failed", { detailId, error });
    }
  })();

  state.videoDetailsPromises.set(detailId, promise);
  try {
    await promise;
  } finally {
    state.videoDetailsPromises.delete(detailId);
  }
}

async function loadTalksFromLegacyLatest() {
  const data = await fetchJsonFromCandidates(DATA_URL_CANDIDATES, { targetName: "latest" });
  const rows = Array.isArray(data) ? data : (Array.isArray(data?.items) ? data.items : data?.rows);
  if (!Array.isArray(rows)) throw createInvalidJsonShapeError();

  const normalized = [];
  rows.forEach((raw) => {
    const row = normalizeRow(raw || {});
    if (!row.title || !row.section) return;
    normalized.push(row);
  });
  if (!normalized.length) throw createInvalidJsonShapeError();
  return groupTalks(normalized);
}

async function loadTalksIfNeeded() {
  if (state.talksStatus === "ready") return state.talks;
  if (state.talksPromise) return state.talksPromise;

  state.talksStatus = "loading";
  state.talksError = "";
  state.talksFallbackActive = false;
  render();
  state.talksPromise = (async () => {
    let talksFetchError = null;
    try {
      const data = await fetchJsonFromCandidates(TALKS_URL_CANDIDATES, { targetName: "talks" });
      const talks = Array.isArray(data?.talks)
        ? data.talks
        : (Array.isArray(data) ? data : (() => { throw createInvalidJsonShapeError(); })());
      state.talks = talks;
      state.talkRecommendationCache = new Map();
      state.talkSearchDocuments = null;
      state.talksStatus = "ready";
      state.talksError = "";
      state.talksFallbackActive = false;
      return state.talks;
    } catch (error) {
      talksFetchError = error;
      console.warn("[talks] talks fetch failed", error);
    }
    try {
      const fallbackTalks = await loadTalksFromLegacyLatest();
      if (Array.isArray(fallbackTalks) && fallbackTalks.length) {
        state.talks = fallbackTalks;
        state.talkRecommendationCache = new Map();
        state.talkSearchDocuments = null;
        state.talksStatus = "ready";
        state.talksError = "";
        state.talksFallbackActive = true;
        console.warn("[talks] fallback to latest.json");
        return state.talks;
      }
    } catch (fallbackError) {
      console.warn("[talks] fallback latest fetch failed", fallbackError);
    }

    state.talksStatus = "error";
    state.talksError = createTargetFetchError("talks", talksFetchError).message;
    state.talksFallbackActive = false;
    return [];
  })();

  try {
    return await state.talksPromise;
  } finally {
    state.talksPromise = null;
  }
}

async function pickRandomSection() {
  await loadTalksIfNeeded();
  if (!state.talks.length) {
    state.randomSection = "候補なし";
    render();
    return;
  }

  const pool = [...state.talks];
  for (let i = pool.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [pool[i], pool[j]] = [pool[j], pool[i]];
  }

  const picked = pool.slice(0, 3);
  state.viewMode = "talk";
  state.search = "";
  refs.search.value = "";
  state.randomTalkKeys = new Set(picked.map((item) => item.key));
  state.randomSection = `トーク見出しを${picked.length}件表示中`;
  render();
}

function toggleAllByMode() {
  if (state.viewMode === "video") {
    const videos = getFilteredVideos();
    const allOpen = videos.length > 0 && getDisplayedVideoOpenKeys(videos).size === videos.length;
    if (allOpen) {
      state.openVideoKeys = new Set();
      state.isVideoExpandLock = false;
      state.videoAutoCollapseAnchor = null;
      return;
    }
    state.openVideoKeys = new Set(videos.map((video) => video.key));
    state.isVideoExpandLock = true;
    state.videoAutoCollapseAnchor = null;
    return;
  }
  if (state.viewMode === "favorites") return;

  const allOpen = state.talks.length > 0 && state.openTalkKeys.size === state.talks.length;
  state.openTalkKeys = allOpen ? new Set() : new Set(state.talks.map((talk) => talk.key));
}

async function switchViewMode(mode) {
  if (mode === "talk" || mode === "favorites") {
    await loadTalksIfNeeded();
    if (mode === "favorites") {
      await retryUnsyncedFavoriteVotes();
      await loadFavoritesDataIfNeeded();
    }
    state.isVideoExpandLock = false;
    state.videoAutoCollapseAnchor = null;
  }
  state.viewMode = mode;
  state.randomTalkKeys = null;
  state.randomSection = "";
  render();
}

function getHeaderBottomOffset() {
  const fixedHeader = document.querySelector(".view-tabs-row");
  if (!fixedHeader) return 0;
  const rect = fixedHeader.getBoundingClientRect();
  return Math.max(rect.bottom, 0);
}

function handleVideoAutoCollapseByCardPass() {
  if (state.viewMode !== "video") return;
  if (state.isVideoExpandLock) return;
  if (!state.videoAutoCollapseAnchor?.key) return;

  const cards = Array.from(refs.results?.querySelectorAll?.(".card") || []);
  if (!cards.length) return;

  const anchor = state.videoAutoCollapseAnchor;
  const anchorIndexByKey = cards.findIndex((card) => card.dataset.key === anchor.key);
  const anchorIndex = anchorIndexByKey >= 0 ? anchorIndexByKey : anchor.index;
  const anchorCard = cards[anchorIndex];
  if (!anchorCard || anchorIndex < 0) {
    state.videoAutoCollapseAnchor = null;
    return;
  }

  const headerBottom = getHeaderBottomOffset();
  const passedCount = cards.slice(anchorIndex + 1).reduce((count, card) => {
    const summary = card.querySelector(".card-summary");
    if (!summary) return count;
    return summary.getBoundingClientRect().top <= headerBottom ? count + 1 : count;
  }, 0);
  const anchorBottom = anchorCard.getBoundingClientRect().bottom;
  const hasPassedEnoughCards = passedCount >= VIDEO_AUTO_COLLAPSE_PASSED_COUNT;
  const isAnchorOutOfView = anchorBottom <= headerBottom;
  if (!hasPassedEnoughCards || !isAnchorOutOfView) return;

  state.openVideoKeys.delete(anchor.key);
  state.videoAutoCollapseAnchor = null;
  render();
}

function bindAmbientReactions() {
  const setToneFromElement = (target) => {
    const card = target?.closest?.(".card");
    document.body.dataset.ambientTone = card?.dataset?.tone || "base";
  };

  refs.results.addEventListener("mouseover", (event) => setToneFromElement(event.target));
  refs.results.addEventListener("focusin", (event) => setToneFromElement(event.target));
  refs.results.addEventListener("mouseout", () => {
    document.body.dataset.ambientTone = "base";
  });
  refs.results.addEventListener("focusout", () => {
    document.body.dataset.ambientTone = "base";
  });
}

function bindMobileScrollLock() {
  const viewport = document.querySelector('meta[name="viewport"]');
  if (viewport) {
    viewport.setAttribute("content", "width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no, viewport-fit=cover");
  }

  const preventIfZoom = (event) => {
    if (event.ctrlKey) event.preventDefault();
  };
  const preventPinch = (event) => {
    if (event.touches && event.touches.length > 1) event.preventDefault();
  };

  document.addEventListener("touchmove", preventPinch, { passive: false });
  window.addEventListener("wheel", preventIfZoom, { passive: false });
  window.addEventListener("gesturestart", (event) => event.preventDefault(), { passive: false });
}

async function init() {
  refreshAmbientViewport();
  initAmbientScene();
  const SEARCH_INPUT_DELAY_MS = 300;
  let searchInputTimer = null;
  let isComposing = false;

  const applySearchInput = (value) => {
    state.search = text(value);
    state.randomTalkKeys = null;
    if (state.search && state.searchIndexStatus === "idle") {
      void loadSearchIndexIfNeeded().then(() => render());
    }
    render();
  };

  const scheduleSearchInput = (value) => {
    if (searchInputTimer) clearTimeout(searchInputTimer);
    searchInputTimer = setTimeout(() => {
      searchInputTimer = null;
      applySearchInput(value);
    }, SEARCH_INPUT_DELAY_MS);
  };

  refs.search.addEventListener("focus", () => {
    if (state.searchIndexStatus === "idle") {
      void loadSearchIndexIfNeeded().then(() => render());
    }
  }, { once: false });

  refs.search.addEventListener("compositionstart", () => {
    isComposing = true;
  });

  refs.search.addEventListener("compositionend", (event) => {
    isComposing = false;
    scheduleSearchInput(event.target.value);
  });

  refs.search.addEventListener("input", (event) => {
    if (isComposing) return;
    scheduleSearchInput(event.target.value);
  });

  refs.clearSearch.addEventListener("click", () => {
    if (searchInputTimer) {
      clearTimeout(searchInputTimer);
      searchInputTimer = null;
    }
    state.search = "";
    refs.search.value = "";
    state.randomTalkKeys = null;
    render();
    refs.search.focus();
  });

  refs.toggleAll.addEventListener("click", () => {
    toggleAllByMode();
    render();
  });

  refs.randomSection.addEventListener("click", () => {
    void pickRandomSection();
  });

  refs.tabVideo.addEventListener("click", () => {
    void switchViewMode("video");
  });

  refs.tabTalk.addEventListener("click", () => {
    void switchViewMode("talk");
  });
  refs.tabFavorites.addEventListener("click", () => {
    void switchViewMode("favorites");
  });

  refs.topButton.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });

  window.addEventListener("scroll", () => {
    updateScrollGradient();
    updateAmbientTransitionByCards();
    handleVideoAutoCollapseByCardPass();
    const before = state.isNewVideoHighlightVisible;
    updateNewVideoHighlightVisibility();
    if (before !== state.isNewVideoHighlightVisible && state.viewMode === "video") {
      render();
    }
  }, { passive: true });

  window.addEventListener("resize", () => {
    refreshAmbientViewport();
    updateAmbientTransitionByCards();
  }, { passive: true });
  updateScrollGradient();
  updateAmbientTransitionByCards();
  updateNewVideoHighlightVisibility();
  bindAmbientReactions();
  bindMobileScrollLock();
  restoreFavoritesFromStorage();
  await retryUnsyncedFavoriteVotes();

  try {
    state.videos = await fetchInitialVideos();
    state.videos = attachDisplayTags(state.videos);
    state.newVideoHighlightKeys = pickNewVideoHighlightKeys(state.videos);
    render();
  } catch (error) {
    refs.error.textContent = "";
    console.error("[latest] initial video load failed", error);
    updateServerStatus("error");
  }
}

init();

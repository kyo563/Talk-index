const configuredDataUrl = text(window.TALK_INDEX_DATA_URL);
const DATA_URL_CANDIDATES = configuredDataUrl
  ? [configuredDataUrl]
  : ["index/latest.json", "./index/latest.json", "/index/latest.json", "latest.json"];

const SEARCH_INDEX_URL_CANDIDATES = DATA_URL_CANDIDATES
  .map((url) => String(url || "").replace(/latest\.json$/, "search_index.json"))
  .filter((url, index, self) => url && self.indexOf(url) === index);

const state = {
  search: "",
  videos: [],
  talks: [],
  recommendation: null,
  searchIndexStatus: "idle",
  searchIndexError: "",
  searchIndexPromise: null,
  skippedRows: 0,
  openVideoKeys: new Set(),
  openTalkKeys: new Set(),
  viewMode: "video",
  randomSection: "",
  randomTalkKeys: null,
  newVideoHighlightKeys: new Set(),
  isNewVideoHighlightVisible: true,
};

const RECOMMEND_LIMIT = 3;
const NEW_VIDEO_HIGHLIGHT_COUNT = 1;
const NEW_VIDEO_HIGHLIGHT_SCROLL_SCREENS = 2;
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
  topButton: document.getElementById("top-button"),
};

function text(value) {
  return String(value || "").trim();
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

function groupTalks(rows) {
  const bySection = new Map();

  rows.forEach((row) => {
    if (!row.section || !isTalkSectionVisible(row.section)) return;
    const talk = ensureTalkSection(bySection, row);
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
  unrelatedPool.sort((a, b) => {
    const aOverlap = a.tokens.filter((token) => currentTokens.has(token)).length;
    const bOverlap = b.tokens.filter((token) => currentTokens.has(token)).length;
    return aOverlap - bOverlap;
  });
  const unrelated = unrelatedPool[0];
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
    let lastError = "";
    for (const url of SEARCH_INDEX_URL_CANDIDATES) {
      try {
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const data = await res.json();
        state.recommendation = buildRecommendationStoreFromSearchIndex(data);
        state.searchIndexStatus = "ready";
        return state.recommendation;
      } catch (error) {
        lastError = `${url}: ${error instanceof Error ? error.message : String(error)}`;
      }
    }

    state.searchIndexStatus = "error";
    state.searchIndexError = lastError || "search_index.json の読込に失敗しました";
    return null;
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

function hitVideo(video, search) {
  if (canSkipSearch(search)) return true;
  if (search.mode === "tag") {
    return video.tags.some((tag) => includesKeyword(tag, search.keyword));
  }

  if (includesKeyword(video.title, search.keyword)) return true;
  return video.sections.some((sec) => {
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

function buildFormattedFragment(raw) {
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

function createFormattedSpan(raw) {
  const span = document.createElement("span");
  span.appendChild(buildFormattedFragment(raw));
  return span;
}

function createFormattedAnchor(href, label) {
  const a = document.createElement("a");
  a.href = href;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.appendChild(buildFormattedFragment(label));
  return a;
}

function updateToggleAllButton() {
  const total = state.viewMode === "video" ? state.videos.length : state.talks.length;
  const openCount = state.viewMode === "video" ? state.openVideoKeys.size : state.openTalkKeys.size;
  const allOpen = total > 0 && openCount === total;
  refs.toggleAll.textContent = allOpen ? "全て折り畳み" : "全て展開";
}

function updateTabs() {
  const isVideo = state.viewMode === "video";
  refs.tabVideo.classList.toggle("is-active", isVideo);
  refs.tabTalk.classList.toggle("is-active", !isVideo);
  refs.tabVideo.setAttribute("aria-selected", isVideo ? "true" : "false");
  refs.tabTalk.setAttribute("aria-selected", isVideo ? "false" : "true");
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
  message.textContent = "条件に一致する動画がありません";

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
    button.innerHTML = `<span class="recommend-main">${item.title}</span><span class="recommend-sub">${item.subtitle}</span><span class="recommend-reason">${item.reason}</span>`;
    button.addEventListener("click", () => {
      openCardFromRecommendation(mode, item.id);
    });

    li.appendChild(button);
    list.appendChild(li);
  });
  wrap.appendChild(list);
  return wrap;
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

  videos.forEach((video) => {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.key = video.key;
    card.dataset.tone = pickAmbientTone([video.title, ...video.tags, ...video.sections.map((sec) => sec.name)]);
    if (state.openVideoKeys.has(video.key)) card.classList.add("is-open");
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
    metaRow.textContent = `${video.sections.length}件`;

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
        ? createFormattedAnchor(sec.sectionUrl, sec.name)
        : createFormattedSpan(sec.name);
      label.classList.add("section-link");
      head.append(toggle, label);

      const subList = document.createElement("ul");
      subList.className = "sub-list";
      sec.subsections.forEach((sub) => {
        const li = document.createElement("li");
        li.appendChild(document.createTextNode("- "));
        li.appendChild(buildFormattedFragment(sub.name));
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
    summary.addEventListener("click", () => {
      if (state.openVideoKeys.has(video.key)) {
        state.openVideoKeys.delete(video.key);
      } else {
        state.openVideoKeys.add(video.key);
      }
      card.classList.toggle("is-open");
      updateToggleAllButton();
    });

    card.append(summary, detail);
    refs.results.appendChild(card);
  });
}

function renderTalkCards(talks) {
  if (!talks.length) return renderNoResult();

  refs.results.innerHTML = "";

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
    if (talk.sectionUrl && isValidHttpUrl(talk.sectionUrl)) {
      titleRow.appendChild(createFormattedAnchor(talk.sectionUrl, talk.name));
    } else {
      titleRow.appendChild(createFormattedSpan(talk.name));
    }

    const metaRow = document.createElement("div");
    metaRow.className = "card-meta-row";
    metaRow.textContent = `小見出し ${talk.subsections.length}件`;

    main.append(titleRow, metaRow);

    const side = document.createElement("div");
    side.className = "card-side";

    const thumb = document.createElement("img");
    thumb.className = "thumbnail";
    thumb.alt = "サムネイル";
    thumb.loading = "lazy";
    if (talk.thumb) {
      thumb.src = talk.thumb;
    }

    side.appendChild(thumb);
    summary.append(main, side);

    const detail = document.createElement("div");
    detail.className = "card-detail";

    const subList = document.createElement("ul");
    subList.className = "sub-list is-open";
    talk.subsections.forEach((sub) => {
      const li = document.createElement("li");
      const subsectionText = document.createElement("div");
      subsectionText.appendChild(document.createTextNode("- "));
      subsectionText.appendChild(buildFormattedFragment(sub.name));
      li.appendChild(subsectionText);

      const videoTitle = document.createElement("div");
      videoTitle.className = "talk-video-title";
      videoTitle.appendChild(document.createTextNode("元動画: "));
      if (isValidHttpUrl(sub.videoUrl)) {
        videoTitle.appendChild(createAnchor(sub.videoUrl, sub.videoTitle));
      } else {
        videoTitle.appendChild(document.createTextNode(sub.videoTitle));
      }
      li.appendChild(videoTitle);

      subList.appendChild(li);
    });
    detail.appendChild(subList);
    if (state.recommendation) {
      const recommendations = scoreRecommendations(state.recommendation.talk, talk.key);
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
    refs.results.appendChild(card);
  });
}

function render() {
  const search = parseSearch(state.search);
  const isVideo = state.viewMode === "video";
  let filtered = isVideo
    ? state.videos.filter((video) => hitVideo(video, search))
    : state.talks.filter((talk) => hitTalk(talk, search));

  if (!isVideo && state.randomTalkKeys) {
    filtered = filtered.filter((talk) => state.randomTalkKeys.has(talk.key));
  }

  refs.notice.textContent = "";

  updateTabs();
  updateServerStatus("ok", filtered.length);
  updateToggleAllButton();
  if (isVideo) {
    renderCards(filtered);
  } else {
    renderTalkCards(filtered);
  }
}

async function fetchRows() {
  let lastError = "";
  updateServerStatus("loading");

  for (const url of DATA_URL_CANDIDATES) {
    try {
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const rows = Array.isArray(data) ? data : (Array.isArray(data.items) ? data.items : data.rows);
      if (!Array.isArray(rows)) throw new Error("JSON形式が不正です");
      return rows;
    } catch (error) {
      lastError = `${url}: ${error instanceof Error ? error.message : String(error)}`;
    }
  }

  throw new Error(`データ取得に失敗しました。${lastError}`);
}

function pickRandomSection() {
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
    const allOpen = state.videos.length > 0 && state.openVideoKeys.size === state.videos.length;
    state.openVideoKeys = allOpen ? new Set() : new Set(state.videos.map((v) => v.key));
    return;
  }

  const allOpen = state.talks.length > 0 && state.openTalkKeys.size === state.talks.length;
  state.openTalkKeys = allOpen ? new Set() : new Set(state.talks.map((talk) => talk.key));
}

function switchViewMode(mode) {
  state.viewMode = mode;
  state.randomTalkKeys = null;
  state.randomSection = "";
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
  refs.search.addEventListener("focus", () => {
    if (state.searchIndexStatus === "idle") {
      void loadSearchIndexIfNeeded();
    }
  }, { once: false });

  refs.search.addEventListener("input", (event) => {
    state.search = text(event.target.value);
    state.randomTalkKeys = null;
    render();
  });

  refs.clearSearch.addEventListener("click", () => {
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
    pickRandomSection();
  });

  refs.tabVideo.addEventListener("click", () => {
    switchViewMode("video");
  });

  refs.tabTalk.addEventListener("click", () => {
    switchViewMode("talk");
  });

  refs.topButton.addEventListener("click", () => {
    window.scrollTo({ top: 0, behavior: "smooth" });
  });

  window.addEventListener("scroll", () => {
    updateScrollGradient();
    const before = state.isNewVideoHighlightVisible;
    updateNewVideoHighlightVisibility();
    if (before !== state.isNewVideoHighlightVisible && state.viewMode === "video") {
      render();
    }
  }, { passive: true });
  updateScrollGradient();
  updateNewVideoHighlightVisibility();
  bindAmbientReactions();
  bindMobileScrollLock();

  try {
    const rows = await fetchRows();
    const normalized = [];

    rows.forEach((raw) => {
      const row = normalizeRow(raw || {});
      if (!row.title || !row.section) {
        state.skippedRows += 1;
        return;
      }
      normalized.push(row);
    });

    state.videos = attachDisplayTags(groupVideos(normalized));
    state.newVideoHighlightKeys = pickNewVideoHighlightKeys(state.videos);
    state.talks = groupTalks(normalized);
    render();
  } catch (error) {
    refs.error.textContent = error instanceof Error ? error.message : String(error);
    updateServerStatus("error");
  }
}

init();

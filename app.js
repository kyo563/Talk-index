const configuredDataUrl = text(window.TALK_INDEX_DATA_URL);
const DATA_URL_CANDIDATES = configuredDataUrl
  ? [configuredDataUrl]
  : ["index/latest.json", "./index/latest.json", "/index/latest.json", "latest.json"];

const state = {
  search: "",
  videos: [],
  talks: [],
  skippedRows: 0,
  openVideoKeys: new Set(),
  openTalkKeys: new Set(),
  viewMode: "video",
  randomSection: "",
  randomTalkKeys: null,
};

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

function normalizeRow(row) {
  return {
    title: text(row["タイトル"]),
    date: text(row["日付"]),
    url: text(row["URL"]),
    section: text(row["大見出し"]),
    sectionUrl: text(row["大見出しURL"]),
    subsection: text(row["小見出し"]),
    tags: splitTags(row["自動検出タグ"]),
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

function isTalkSectionVisible(name) {
  return name !== "【オープニングトーク】" && name !== "【エンディングトーク】";
}

function groupTalks(rows) {
  const bySection = new Map();

  rows.forEach((row) => {
    if (!row.section || !isTalkSectionVisible(row.section)) return;

    if (!bySection.has(row.section)) {
      bySection.set(row.section, {
        key: row.section,
        name: row.section,
        sectionUrl: row.sectionUrl,
        subsections: new Set(),
      });
    }

    if (row.subsection) {
      bySection.get(row.section).subsections.add(row.subsection);
    }
  });

  return Array.from(bySection.values()).map((talk) => ({
    key: talk.key,
    name: talk.name,
    sectionUrl: talk.sectionUrl,
    subsections: Array.from(talk.subsections),
  }));
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

function hitVideo(video, search) {
  if (search.mode === "none") return true;
  if (!search.keyword) return true;

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
  if (search.mode === "none") return true;
  if (!search.keyword) return true;
  if (search.mode === "tag") return false;
  if (includesKeyword(talk.name, search.keyword)) return true;
  return talk.subsections.some((sub) => includesKeyword(sub, search.keyword));
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
  const tokens = String(raw || "").split(/(\*\*|--)/);
  let isBold = false;
  let isStrike = false;

  tokens.forEach((token) => {
    if (token === "**") {
      isBold = !isBold;
      return;
    }
    if (token === "--") {
      isStrike = !isStrike;
      return;
    }
    if (!token) return;

    let node = document.createTextNode(token);
    if (isBold) {
      const strong = document.createElement("strong");
      strong.appendChild(node);
      node = strong;
    }
    if (isStrike) {
      const s = document.createElement("s");
      s.appendChild(node);
      node = s;
    }
    fragment.appendChild(node);
  });

  return fragment;
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
  if (mode === "loading") {
    refs.serverStatus.textContent = "通信状態: 読込中";
    return;
  }
  if (mode === "error") {
    refs.serverStatus.textContent = "通信状態: サーバーエラー";
    return;
  }
  refs.serverStatus.textContent = `通信状態: 正常稼働中（${shownCount}件表示）`;
}

function renderNoResult() {
  refs.results.innerHTML = "<p>条件に一致する動画がありません</p>";
}

function renderCards(videos) {
  if (!videos.length) return renderNoResult();

  refs.results.innerHTML = "";

  videos.forEach((video) => {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.key = video.key;
    if (state.openVideoKeys.has(video.key)) card.classList.add("is-open");

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
    metaRow.textContent = `${video.date || "日付なし"} / ${video.sections.length}件`;

    main.append(titleRow, metaRow);

    const thumb = document.createElement("img");
    thumb.className = "thumbnail";
    thumb.alt = "サムネイル";
    thumb.loading = "lazy";
    if (video.thumb) {
      thumb.src = video.thumb;
    }

    summary.append(main, thumb);

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
    if (video.tags.length) {
      video.tags.forEach((tag) => {
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
    summary.appendChild(main);

    const detail = document.createElement("div");
    detail.className = "card-detail";

    const subList = document.createElement("ul");
    subList.className = "sub-list is-open";
    talk.subsections.forEach((sub) => {
      const li = document.createElement("li");
      li.appendChild(document.createTextNode("- "));
      li.appendChild(buildFormattedFragment(sub));
      subList.appendChild(li);
    });
    detail.appendChild(subList);

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

  const notes = [];
  if (state.skippedRows > 0) notes.push(`${state.skippedRows}件の不正データをスキップしました`);
  if (state.randomSection) notes.push(`ランダムおすすめ: ${state.randomSection}`);
  notes.push(isVideo ? "動画単位モード" : "トーク単位モード");
  if (search.mode === "tag") notes.push("タグ検索中（#付き）");
  if (search.mode === "normal") notes.push("通常検索中（タイトル/大見出し/小見出し）");
  refs.notice.textContent = notes.join(" / ");

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
      const rows = Array.isArray(data) ? data : data.rows;
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

  const picked = pool.slice(0, 4);
  state.viewMode = "talk";
  state.search = "";
  refs.search.value = "";
  state.randomTalkKeys = new Set(picked.map((item) => item.key));
  state.randomSection = `トーク見出しを${picked.length}件表示中`;
  render();
}

async function init() {
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
    if (state.viewMode === "video") {
      const allOpen = state.videos.length > 0 && state.openVideoKeys.size === state.videos.length;
      if (allOpen) {
        state.openVideoKeys.clear();
      } else {
        state.openVideoKeys = new Set(state.videos.map((v) => v.key));
      }
    } else {
      const allOpen = state.talks.length > 0 && state.openTalkKeys.size === state.talks.length;
      if (allOpen) {
        state.openTalkKeys.clear();
      } else {
        state.openTalkKeys = new Set(state.talks.map((talk) => talk.key));
      }
    }
    render();
  });

  refs.randomSection.addEventListener("click", () => {
    pickRandomSection();
  });

  refs.tabVideo.addEventListener("click", () => {
    state.viewMode = "video";
    state.randomTalkKeys = null;
    state.randomSection = "";
    render();
  });

  refs.tabTalk.addEventListener("click", () => {
    state.viewMode = "talk";
    state.randomTalkKeys = null;
    state.randomSection = "";
    render();
  });

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

    state.videos = groupVideos(normalized);
    state.talks = groupTalks(normalized);
    render();
  } catch (error) {
    refs.error.textContent = error instanceof Error ? error.message : String(error);
    updateServerStatus("error");
  }
}

init();

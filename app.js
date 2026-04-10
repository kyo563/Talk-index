const configuredDataUrl = text(window.TALK_INDEX_DATA_URL);
const DATA_URL_CANDIDATES = configuredDataUrl
  ? [configuredDataUrl]
  : ["index/latest.json", "./index/latest.json", "/index/latest.json", "latest.json"];

const state = {
  search: "",
  videos: [],
  skippedRows: 0,
  openVideoKeys: new Set(),
  lastScrollY: 0,
};

const refs = {
  search: document.getElementById("search"),
  resultMeta: document.getElementById("result-meta"),
  notice: document.getElementById("notice"),
  error: document.getElementById("error"),
  results: document.getElementById("results"),
  serverStatus: document.getElementById("server-status"),
  expandAll: document.getElementById("expand-all"),
  collapseAll: document.getElementById("collapse-all"),
};

function text(value) {
  return String(value || "").trim();
}

function splitTags(raw) {
  return text(raw)
    .split(",")
    .map((item) => item.trim())
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

function createAnchor(href, label) {
  const a = document.createElement("a");
  a.href = href;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.textContent = label;
  return a;
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
        ? createAnchor(sec.sectionUrl, sec.name)
        : document.createTextNode(sec.name);

      if (label instanceof Node && label.nodeType === Node.TEXT_NODE) {
        const span = document.createElement("span");
        span.textContent = label.textContent || "";
        head.append(toggle, span);
      } else {
        label.classList.add("section-link");
        head.append(toggle, label);
      }

      const subList = document.createElement("ul");
      subList.className = "sub-list";
      sec.subsections.forEach((sub) => {
        const li = document.createElement("li");
        li.textContent = `- ${sub.name}`;
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
    });

    card.append(summary, detail);
    refs.results.appendChild(card);
  });
}

function render() {
  const search = parseSearch(state.search);
  const filtered = state.videos.filter((video) => hitVideo(video, search));

  refs.resultMeta.textContent = `${filtered.length}件表示 / 全${state.videos.length}件`;
  refs.notice.textContent =
    search.mode === "tag"
      ? "タグ検索中（#付き）"
      : search.mode === "normal"
        ? "通常検索中（タイトル/大見出し/小見出し）"
        : "";

  renderCards(filtered);
}

async function fetchRows() {
  let lastError = "";
  for (const url of DATA_URL_CANDIDATES) {
    try {
      refs.serverStatus.textContent = `通信状態: 取得中 (${url})`;
      const res = await fetch(url, { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const rows = Array.isArray(data) ? data : data.rows;
      if (!Array.isArray(rows)) throw new Error("JSON形式が不正です");
      refs.serverStatus.textContent = `通信状態: 正常 (${url})`;
      return rows;
    } catch (error) {
      lastError = `${url}: ${error instanceof Error ? error.message : String(error)}`;
    }
  }
  throw new Error(`データ取得に失敗しました。${lastError}`);
}

async function init() {
  refs.search.addEventListener("input", (event) => {
    state.search = text(event.target.value);
    render();
  });

  refs.expandAll.addEventListener("click", () => {
    state.openVideoKeys = new Set(state.videos.map((v) => v.key));
    render();
  });

  refs.collapseAll.addEventListener("click", () => {
    state.openVideoKeys.clear();
    render();
  });

  window.addEventListener("scroll", () => {
    const nowY = window.scrollY;
    if (Math.abs(nowY - state.lastScrollY) >= window.innerHeight) {
      if (state.openVideoKeys.size > 0) {
        state.openVideoKeys.clear();
        render();
      }
      state.lastScrollY = nowY;
    }
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

    if (state.skippedRows > 0) {
      refs.notice.textContent = `${state.skippedRows}件の不正データをスキップしました`;
    }

    render();
  } catch (error) {
    refs.error.textContent = error instanceof Error ? error.message : String(error);
    refs.serverStatus.textContent = "通信状態: 異常";
  }
}

init();

const configuredDataUrl = text(window.TALK_INDEX_DATA_URL);
const DATA_URL_CANDIDATES = configuredDataUrl
  ? [configuredDataUrl]
  : ["index/latest.json", "./index/latest.json", "/index/latest.json", "latest.json"];

const state = {
  view: "video",
  search: "",
  sortVideo: "newest",
  sortTheme: "count_desc",
  rawRows: [],
  videos: [],
  themes: [],
  skippedRows: 0,
};

const sortOptions = {
  video: [
    { value: "newest", label: "新しい順" },
    { value: "oldest", label: "古い順" },
    { value: "title_asc", label: "タイトル昇順" },
    { value: "title_desc", label: "タイトル降順" },
  ],
  theme: [
    { value: "count_desc", label: "出現数が多い順" },
    { value: "count_asc", label: "出現数が少ない順" },
    { value: "name_asc", label: "文字列昇順" },
    { value: "name_desc", label: "文字列降順" },
  ],
};

const refs = {
  tabVideo: document.getElementById("tab-video"),
  tabTheme: document.getElementById("tab-theme"),
  search: document.getElementById("search"),
  sort: document.getElementById("sort"),
  sortLabel: document.getElementById("sort-label"),
  resultMeta: document.getElementById("result-meta"),
  notice: document.getElementById("notice"),
  error: document.getElementById("error"),
  results: document.getElementById("results"),
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

function toDateValue(rawDate) {
  const value = Date.parse(text(rawDate));
  return Number.isNaN(value) ? 0 : value;
}

function normalizeRow(row) {
  return {
    title: text(row["タイトル"]),
    date: text(row["日付"]),
    url: text(row["URL"]),
    section: text(row["大見出し"]),
    sectionUrl: text(row["大見出しURL"]),
    subsection: text(row["小見出し"]),
    subsectionUrl: text(row["小見出しURL"]),
    tags: splitTags(row["自動検出タグ"]),
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
        tags: new Set(),
        sections: new Map(),
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

    if (row.subsection && row.subsectionUrl) {
      video.sections.get(row.section).subsections.push({
        name: row.subsection,
        url: row.subsectionUrl,
      });
    }
  });

  return Array.from(byVideo.values()).map((video) => ({
    ...video,
    tags: Array.from(video.tags),
    sections: Array.from(video.sections.values()),
    dateValue: toDateValue(video.date),
  }));
}

function groupThemes(videos) {
  const byTheme = new Map();

  videos.forEach((video) => {
    video.sections.forEach((section) => {
      if (!byTheme.has(section.name)) {
        byTheme.set(section.name, {
          name: section.name,
          appearances: 0,
          subsectionCount: 0,
          videos: [],
        });
      }

      const theme = byTheme.get(section.name);
      theme.appearances += 1;
      theme.subsectionCount += section.subsections.length;
      theme.videos.push({
        title: video.title,
        videoUrl: video.url,
        sectionUrl: section.sectionUrl,
        subsections: section.subsections,
      });
    });
  });

  return Array.from(byTheme.values());
}

function containsAnyTarget(video, keyword) {
  if (!keyword) return true;
  const base = [video.title, ...video.tags].join(" ").toLowerCase();
  if (base.includes(keyword)) return true;

  return video.sections.some((section) => {
    if (section.name.toLowerCase().includes(keyword)) return true;
    return section.subsections.some((sub) => sub.name.toLowerCase().includes(keyword));
  });
}

function getFilteredVideos() {
  const keyword = state.search.toLowerCase();
  const filtered = state.videos.filter((video) => containsAnyTarget(video, keyword));

  return filtered.sort((a, b) => {
    switch (state.sortVideo) {
      case "oldest":
        return a.dateValue - b.dateValue;
      case "title_asc":
        return a.title.localeCompare(b.title, "ja");
      case "title_desc":
        return b.title.localeCompare(a.title, "ja");
      case "newest":
      default:
        return b.dateValue - a.dateValue;
    }
  });
}

function getFilteredThemes() {
  const keyword = state.search.toLowerCase();
  const filtered = state.themes.filter((theme) => {
    if (!keyword) return true;
    if (theme.name.toLowerCase().includes(keyword)) return true;

    return theme.videos.some((video) => {
      const textBody = [video.title, ...video.subsections.map((s) => s.name)].join(" ").toLowerCase();
      return textBody.includes(keyword);
    });
  });

  return filtered.sort((a, b) => {
    switch (state.sortTheme) {
      case "count_asc":
        return a.appearances - b.appearances;
      case "name_asc":
        return a.name.localeCompare(b.name, "ja");
      case "name_desc":
        return b.name.localeCompare(a.name, "ja");
      case "count_desc":
      default:
        return b.appearances - a.appearances;
    }
  });
}

function renderSortOptions() {
  const view = state.view;
  refs.sort.innerHTML = "";
  sortOptions[view].forEach((opt) => {
    const option = document.createElement("option");
    option.value = opt.value;
    option.textContent = opt.label;
    refs.sort.appendChild(option);
  });

  if (view === "video") {
    refs.sort.value = state.sortVideo;
    refs.sortLabel.textContent = "並べ替え（動画単位）";
  } else {
    refs.sort.value = state.sortTheme;
    refs.sortLabel.textContent = "並べ替え（大見出し単位）";
  }
}

function renderNoResult() {
  refs.results.innerHTML = "<p>条件に一致するトークがありません</p>";
}

function createAnchor(href, label) {
  const a = document.createElement("a");
  a.href = href;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.textContent = label;
  return a;
}

function renderVideoCards(videos) {
  if (!videos.length) return renderNoResult();

  refs.results.innerHTML = "";

  videos.forEach((video) => {
    const card = document.createElement("article");
    card.className = "card";

    const title = document.createElement("h3");
    if (video.url && isValidHttpUrl(video.url)) {
      title.appendChild(createAnchor(video.url, video.title));
    } else {
      title.textContent = video.title;
    }

    const meta = document.createElement("p");
    meta.className = "meta";
    meta.textContent = `日付: ${video.date || "不明"} / 大見出し: ${video.sections.length}件`;

    card.appendChild(title);
    card.appendChild(meta);

    if (video.tags.length) {
      const tags = document.createElement("div");
      tags.className = "tag-list";
      video.tags.forEach((tag) => {
        const span = document.createElement("span");
        span.className = "tag";
        span.textContent = tag;
        tags.appendChild(span);
      });
      card.appendChild(tags);
    }

    video.sections.forEach((section) => {
      const group = document.createElement("details");
      group.className = "group";

      const summary = document.createElement("summary");
      summary.textContent = section.name;

      const list = document.createElement("ul");
      section.subsections.forEach((sub) => {
        const li = document.createElement("li");
        if (isValidHttpUrl(sub.url)) {
          li.appendChild(createAnchor(sub.url, sub.name));
        } else {
          li.textContent = sub.name;
        }
        list.appendChild(li);
      });

      group.appendChild(summary);
      if (section.subsections.length) group.appendChild(list);
      card.appendChild(group);
    });

    refs.results.appendChild(card);
  });
}

function renderThemeCards(themes) {
  if (!themes.length) return renderNoResult();

  refs.results.innerHTML = "";

  themes.forEach((theme) => {
    const card = document.createElement("article");
    card.className = "card";

    const title = document.createElement("h3");
    title.textContent = theme.name;

    const meta = document.createElement("p");
    meta.className = "meta";
    meta.textContent = `該当動画: ${theme.appearances}件 / 小見出し: ${theme.subsectionCount}件`;

    card.appendChild(title);
    card.appendChild(meta);

    theme.videos.forEach((video) => {
      const group = document.createElement("details");
      group.className = "group";

      const summary = document.createElement("summary");
      if (video.videoUrl && isValidHttpUrl(video.videoUrl)) {
        summary.appendChild(createAnchor(video.videoUrl, video.title));
      } else {
        summary.textContent = video.title;
      }

      const list = document.createElement("ul");
      video.subsections.forEach((sub) => {
        const li = document.createElement("li");
        if (isValidHttpUrl(sub.url)) {
          li.appendChild(createAnchor(sub.url, sub.name));
        } else {
          li.textContent = sub.name;
        }
        list.appendChild(li);
      });

      if (video.sectionUrl && isValidHttpUrl(video.sectionUrl)) {
        const jump = document.createElement("p");
        jump.appendChild(createAnchor(video.sectionUrl, "大見出しへ移動"));
        group.appendChild(jump);
      }

      group.appendChild(summary);
      if (video.subsections.length) group.appendChild(list);
      card.appendChild(group);
    });

    refs.results.appendChild(card);
  });
}

function renderNotice() {
  refs.notice.textContent = state.skippedRows
    ? `不正データを ${state.skippedRows} 件スキップしました。`
    : "";
}

function renderResultMeta(count) {
  const label = state.view === "video" ? "動画" : "テーマ";
  refs.resultMeta.textContent = `${label}の表示件数: ${count}件`;
}

function render() {
  renderSortOptions();
  renderNotice();
  refs.error.textContent = "";

  if (state.view === "video") {
    const videos = getFilteredVideos();
    renderVideoCards(videos);
    renderResultMeta(videos.length);
    refs.tabVideo.classList.add("is-active");
    refs.tabTheme.classList.remove("is-active");
    refs.tabVideo.setAttribute("aria-selected", "true");
    refs.tabTheme.setAttribute("aria-selected", "false");
  } else {
    const themes = getFilteredThemes();
    renderThemeCards(themes);
    renderResultMeta(themes.length);
    refs.tabTheme.classList.add("is-active");
    refs.tabVideo.classList.remove("is-active");
    refs.tabTheme.setAttribute("aria-selected", "true");
    refs.tabVideo.setAttribute("aria-selected", "false");
  }
}

function setError(message) {
  refs.error.textContent = message;
}

function setupEvents() {
  refs.search.addEventListener("input", (e) => {
    state.search = text(e.target.value);
    render();
  });

  refs.sort.addEventListener("change", (e) => {
    if (state.view === "video") {
      state.sortVideo = e.target.value;
    } else {
      state.sortTheme = e.target.value;
    }
    render();
  });

  refs.tabVideo.addEventListener("click", () => {
    state.view = "video";
    render();
  });

  refs.tabTheme.addEventListener("click", () => {
    state.view = "theme";
    render();
  });
}

async function tryFetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`JSON取得に失敗しました (HTTP ${response.status})`);
  }
  return response.json();
}

async function loadData() {
  refs.resultMeta.textContent = "データ読み込み中...";

  let rows = null;
  const errors = [];

  for (const url of DATA_URL_CANDIDATES) {
    try {
      rows = await tryFetchJson(url);
      break;
    } catch (error) {
      errors.push(`${url}: ${error.message}`);
    }
  }

  if (!rows) {
    const checked = DATA_URL_CANDIDATES.join(", ");
    const details = errors.join(" / ");
    const isFileProtocol = window.location.protocol === "file:";
    const hint = isFileProtocol
      ? "ヒント: file:// 直開きでは fetch 制限が出ることがあります。`python -m http.server 8000` で起動し、`http://localhost:8000` から開いてください。"
      : "ヒント: JSONのURLをブラウザで直接開けても、fetch では CORS 設定が必要です。配信側で Access-Control-Allow-Origin を確認してください。";
    setError(`データ取得に失敗しました。確認URL: ${checked}。詳細: ${details}。${hint}`);
    refs.resultMeta.textContent = "";
    return;
  }

  if (!Array.isArray(rows)) {
    setError("JSON形式が不正です。配列データを想定しています。");
    refs.resultMeta.textContent = "";
    return;
  }

  state.rawRows = [];
  state.skippedRows = 0;

  rows.forEach((row) => {
    const normalized = normalizeRow(row);

    const urlOk = isValidHttpUrl(normalized.url);
    const hasTopic = Boolean(normalized.section || normalized.subsection);

    if (!urlOk || !hasTopic) {
      state.skippedRows += 1;
      return;
    }

    state.rawRows.push(normalized);
  });

  state.videos = groupVideos(state.rawRows);
  state.themes = groupThemes(state.videos);

  render();
}

setupEvents();
loadData();

const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
};

function text(value) {
  return String(value || "").trim();
}

function utf8(input) {
  return new TextEncoder().encode(input);
}

function canonicalServerTimestamp(now = new Date()) {
  return now.toISOString();
}

function jstDateFromIso(iso) {
  const raw = text(iso);
  const date = new Date(raw || Date.now());
  const jstMs = date.getTime() + 9 * 60 * 60 * 1000;
  return new Date(jstMs).toISOString().slice(0, 10);
}

function weekKeyJstFromIso(iso) {
  const date = new Date(`${jstDateFromIso(iso)}T00:00:00.000Z`);
  const day = date.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  date.setUTCDate(date.getUTCDate() + diff);
  return date.toISOString().slice(0, 10);
}

function previousCompletedWeekKeyFromIso(iso) {
  const currentWeekKey = weekKeyJstFromIso(iso);
  const date = new Date(`${currentWeekKey}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() - 7);
  return date.toISOString().slice(0, 10);
}


function parseEventMs(value) {
  const ms = Date.parse(text(value));
  return Number.isFinite(ms) ? ms : null;
}

function normalizeTitle(value) {
  return text(value)
    .normalize("NFKC")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .replace(/\s+/g, "")
    .toLowerCase();
}

function canonicalizeYouTubeUrl(value) {
  const videoId = extractYouTubeVideoId(value);
  return videoId ? `https://www.youtube.com/watch?v=${videoId}` : "";
}

function extractYouTubeVideoId(url) {
  const raw = text(url);
  if (!raw) return "";
  if (/^[a-zA-Z0-9_-]{11}$/.test(raw)) return raw;
  try {
    const parsed = new URL(raw);
    const host = parsed.hostname.toLowerCase().replace(/^www\./, "");
    if (host === "youtu.be") {
      return text(parsed.pathname.split("/").filter(Boolean)[0]);
    }
    if (host.endsWith("youtube.com")) {
      if (parsed.pathname.startsWith("/shorts/")) {
        return text(parsed.pathname.replace("/shorts/", "").split("/")[0]);
      }
      return text(parsed.searchParams.get("v"));
    }
  } catch {
    // noop
  }
  return "";
}

function isValidYouTubeVideoId(value) {
  return /^[a-zA-Z0-9_-]{11}$/.test(text(value));
}

function pickFirstValidDate(...candidates) {
  for (const candidate of candidates) {
    const raw = text(candidate);
    if (!raw) continue;
    if (parsePublishedAt(raw) !== null) return raw;
  }
  return "";
}

function normalizeVotePayload(vote) {
  const normalized = {
    ...vote,
    headingId: text(vote?.headingId),
    videoId: text(vote?.videoId),
    headingTitle: text(vote?.headingTitle),
    videoTitle: text(vote?.videoTitle),
    sourceVideoTitle: text(vote?.sourceVideoTitle),
    sourceVideoUrl: text(vote?.sourceVideoUrl) || text(vote?.videoUrl),
    headingStart: text(vote?.headingStart),
    publishedAt: text(vote?.publishedAt),
    videoDate: text(vote?.videoDate),
    sourceMode: text(vote?.sourceMode) || "unknown",
  };
  const extractedVideoId = extractYouTubeVideoId(normalized.sourceVideoUrl);
  const canonicalVideoId = isValidYouTubeVideoId(normalized.videoId)
    ? normalized.videoId
    : (isValidYouTubeVideoId(extractedVideoId) ? extractedVideoId : "");
  const canonicalSourceVideoUrl = canonicalVideoId ? canonicalizeYouTubeUrl(canonicalVideoId) : "";
  const canonicalPublishedAt = pickFirstValidDate(normalized.publishedAt, normalized.videoDate);
  const canonicalVideoDate = pickFirstValidDate(normalized.videoDate, normalized.publishedAt);
  const canonicalVideoTitle = normalized.videoTitle || normalized.sourceVideoTitle;
  const canonicalSourceVideoTitle = normalized.sourceVideoTitle || normalized.videoTitle;
  return {
    ...normalized,
    videoId: canonicalVideoId,
    sourceVideoUrl: canonicalSourceVideoUrl,
    publishedAt: canonicalPublishedAt,
    videoDate: canonicalVideoDate,
    videoTitle: canonicalVideoTitle,
    sourceVideoTitle: canonicalSourceVideoTitle,
  };
}

function collectMetadataValidationReasons(result, rawInput = {}, maps = null) {
  const reasons = [];
  const rawVideoId = text(rawInput?.videoId);
  if (!rawVideoId) {
    if (!text(result.videoId)) reasons.push("missing_video_id");
  } else if (!isValidYouTubeVideoId(rawVideoId)) {
    reasons.push("invalid_video_id");
  } else if (!text(result.videoId)) {
    reasons.push("missing_video_id");
  }

  const rawPublishedAt = text(rawInput?.publishedAt) || text(rawInput?.videoDate);
  if (!rawPublishedAt) {
    if (!(text(result.publishedAt) || text(result.videoDate))) reasons.push("missing_published_at");
  } else if (!parsePublishedAt(rawPublishedAt)) {
    reasons.push("invalid_published_at");
  } else if (!parsePublishedAt(text(result.publishedAt) || text(result.videoDate))) {
    reasons.push("invalid_published_at");
  }

  if (!(text(result.sourceVideoTitle) || text(result.videoTitle))) reasons.push("missing_video_title");

  const rawSourceUrl = text(rawInput?.sourceVideoUrl) || text(rawInput?.videoUrl);
  if (!rawSourceUrl) {
    if (!text(result.sourceVideoUrl)) reasons.push("missing_source_video_url");
  } else if (!extractYouTubeVideoId(rawSourceUrl)) {
    reasons.push("url_unparseable");
  } else if (!text(result.sourceVideoUrl)) {
    reasons.push("missing_source_video_url");
  }

  if (maps) {
    const sourceTitleKey = normalizeTitle(text(rawInput?.sourceVideoTitle));
    const videoTitleKey = normalizeTitle(text(rawInput?.videoTitle));
    if (sourceTitleKey && maps.ambiguousTitles.has(sourceTitleKey)) reasons.push("title_ambiguous");
    if (videoTitleKey && maps.ambiguousTitles.has(videoTitleKey)) reasons.push("title_ambiguous");
    const headingTitleKey = normalizeTitle(text(rawInput?.headingTitle));
    if (headingTitleKey && maps.ambiguousHeadingTitles.has(headingTitleKey)) reasons.push("heading_title_ambiguous");
  }

  return Array.from(new Set(reasons));
}

function buildVideoMetadataMaps(talksPayload, latestPayload) {
  const byVideoId = new Map();
  const byCanonicalUrl = new Map();
  const byHeadingId = new Map();
  const byTitleHits = new Map();
  const byHeadingTitleHits = new Map();
  const ambiguousTitles = new Set();
  const ambiguousHeadingTitles = new Set();

  function registerTitleHit(target, rawTitle, metadata) {
    const normalized = normalizeTitle(rawTitle);
    if (!normalized) return;
    const existing = target.get(normalized);
    if (!existing) {
      target.set(normalized, metadata);
      return;
    }
    if (existing.videoId !== metadata.videoId) {
      target.set(normalized, null);
      if (target === byTitleHits) ambiguousTitles.add(normalized);
      if (target === byHeadingTitleHits) ambiguousHeadingTitles.add(normalized);
    }
  }

  function registerMetadata(partial, context = {}) {
    const videoId = text(partial.videoId) || extractYouTubeVideoId(partial.url);
    if (!videoId) return;
    const url = canonicalizeYouTubeUrl(partial.url) || canonicalizeYouTubeUrl(videoId);
    const next = {
      videoId,
      title: text(partial.title),
      url,
      publishedAt: text(partial.publishedAt),
      videoDate: text(partial.videoDate) || text(partial.publishedAt),
    };
    const existing = byVideoId.get(videoId) || {};
    const merged = {
      videoId,
      title: text(next.title) || text(existing.title),
      url: text(next.url) || text(existing.url),
      publishedAt: pickFirstValidDate(next.publishedAt, existing.publishedAt),
      videoDate: pickFirstValidDate(next.videoDate, existing.videoDate, next.publishedAt, existing.publishedAt),
    };
    byVideoId.set(videoId, merged);
    if (merged.url) byCanonicalUrl.set(merged.url, merged);
    registerTitleHit(byTitleHits, merged.title, merged);

    const headingId = text(context.headingId);
    if (headingId) byHeadingId.set(headingId, merged);
    registerTitleHit(byHeadingTitleHits, context.headingTitle, merged);
  }

  const talks = Array.isArray(talksPayload?.talks) ? talksPayload.talks : [];
  for (const talk of talks) {
    if (!talk || typeof talk !== "object") continue;
    const talkDate = text(talk.date);
    const headingId = text(talk.headingId) || text(talk.id);
    const headingTitle = text(talk.name) || text(talk.headingTitle);
    const subsections = Array.isArray(talk.subsections) ? talk.subsections : [];
    for (const subsection of subsections) {
      if (!subsection || typeof subsection !== "object") continue;
      const url = text(subsection.videoUrl);
      registerMetadata({
        videoId: text(subsection.videoId),
        title: text(subsection.videoTitle),
        url,
        publishedAt: talkDate,
      }, { headingId, headingTitle });
    }
  }

  let latestItems = [];
  if (Array.isArray(latestPayload)) {
    latestItems = latestPayload;
  } else if (latestPayload && typeof latestPayload === "object") {
    for (const key of ["videos", "items", "data"]) {
      if (Array.isArray(latestPayload[key])) {
        latestItems = latestPayload[key];
        break;
      }
    }
  }

  for (const item of latestItems) {
    if (!item || typeof item !== "object") continue;
    registerMetadata({
      videoId: text(item.id) || text(item.videoId),
      title: text(item.title),
      url: text(item.url),
      publishedAt: text(item.date) || text(item.publishedAt),
    });
  }

  const byNormalizedTitle = new Map();
  for (const [key, value] of byTitleHits.entries()) {
    if (value) byNormalizedTitle.set(key, value);
  }
  const byNormalizedHeadingTitle = new Map();
  for (const [key, value] of byHeadingTitleHits.entries()) {
    if (value) byNormalizedHeadingTitle.set(key, value);
  }

  return {
    byVideoId,
    byCanonicalUrl,
    byNormalizedTitle,
    byHeadingId,
    byNormalizedHeadingTitle,
    ambiguousTitles,
    ambiguousHeadingTitles,
  };
}

function buildVideoMetadataMap(talksPayload, latestPayload) {
  return buildVideoMetadataMaps(talksPayload, latestPayload).byVideoId;
}

function ensureVideoMetadataMaps(mapsOrMap) {
  if (mapsOrMap && typeof mapsOrMap === "object" && mapsOrMap.byVideoId instanceof Map) {
    return mapsOrMap;
  }
  const byVideoId = mapsOrMap instanceof Map ? mapsOrMap : new Map();
  return {
    byVideoId,
    byCanonicalUrl: new Map(),
    byNormalizedTitle: new Map(),
    byHeadingId: new Map(),
    byNormalizedHeadingTitle: new Map(),
    ambiguousTitles: new Set(),
    ambiguousHeadingTitles: new Set(),
  };
}

function resolveVoteMetadata(vote, mapsOrMap, rawInput = vote) {
  const maps = ensureVideoMetadataMaps(mapsOrMap);
  const normalizedVote = normalizeVotePayload(vote);

  let resolvedMeta = null;
  const byId = maps.byVideoId.get(normalizedVote.videoId);
  if (byId) resolvedMeta = byId;

  const extractedFromUrl = extractYouTubeVideoId(text(rawInput?.sourceVideoUrl) || text(rawInput?.videoUrl));
  if (!resolvedMeta && extractedFromUrl && maps.byVideoId.get(extractedFromUrl)) {
    resolvedMeta = maps.byVideoId.get(extractedFromUrl);
  }
  if (!resolvedMeta) {
    const canonicalUrl = canonicalizeYouTubeUrl(normalizedVote.sourceVideoUrl);
    if (canonicalUrl && maps.byCanonicalUrl.get(canonicalUrl)) resolvedMeta = maps.byCanonicalUrl.get(canonicalUrl);
  }
  if (!resolvedMeta) {
    const bySourceTitle = maps.byNormalizedTitle.get(normalizeTitle(normalizedVote.sourceVideoTitle));
    if (bySourceTitle) resolvedMeta = bySourceTitle;
  }
  if (!resolvedMeta) {
    const byVideoTitle = maps.byNormalizedTitle.get(normalizeTitle(normalizedVote.videoTitle));
    if (byVideoTitle) resolvedMeta = byVideoTitle;
  }
  if (!resolvedMeta && normalizedVote.headingId && maps.byHeadingId.get(normalizedVote.headingId)) {
    resolvedMeta = maps.byHeadingId.get(normalizedVote.headingId);
  }
  const meta = resolvedMeta || {};
  const resolvedVideoId = normalizedVote.videoId || (isValidYouTubeVideoId(extractedFromUrl) ? extractedFromUrl : "") || text(meta.videoId);
  const resolvedUrl = canonicalizeYouTubeUrl(resolvedVideoId) || text(meta.url);
  const resolvedPublishedAt = pickFirstValidDate(
    normalizedVote.publishedAt,
    normalizedVote.videoDate,
    meta.publishedAt,
    meta.videoDate,
  );
  const resolvedVideoDate = pickFirstValidDate(
    normalizedVote.videoDate,
    normalizedVote.publishedAt,
    meta.videoDate,
    meta.publishedAt,
  );
  const resolvedVideoTitle = normalizedVote.videoTitle || text(meta.title);
  const resolvedSourceVideoTitle = normalizedVote.sourceVideoTitle || resolvedVideoTitle || text(meta.title);

  const result = {
    ...vote,
    ...normalizedVote,
    videoId: resolvedVideoId,
    videoTitle: resolvedVideoTitle,
    sourceVideoTitle: resolvedSourceVideoTitle,
    sourceVideoUrl: resolvedUrl,
    publishedAt: resolvedPublishedAt,
    videoDate: resolvedVideoDate,
  };

  const uniqueReasons = collectMetadataValidationReasons(result, rawInput, maps);
  const hasMinimum = uniqueReasons.length === 0;
  result.metadataIncomplete = !hasMinimum;
  result.metadataIncompleteReason = uniqueReasons;
  return result;
}

function createAggregateItem(vote) {
  return {
    headingId: text(vote.headingId),
    videoId: text(vote.videoId),
    headingTitle: text(vote.headingTitle) || text(vote.headingId),
    videoTitle: text(vote.videoTitle),
    sourceVideoTitle: text(vote.sourceVideoTitle) || text(vote.videoTitle),
    sourceVideoUrl: text(vote.sourceVideoUrl) || text(vote.videoUrl),
    headingStart: text(vote.headingStart),
    publishedAt: text(vote.publishedAt),
    videoDate: text(vote.videoDate),
    sourceMode: text(vote.sourceMode) || "unknown",
    voteCount: 0,
    firstVotedAt: text(vote.firstVotedAt),
    lastVotedAt: text(vote.firstVotedAt),
  };
}

function backfillAggregateMetadata(item, vote = {}, meta = {}) {
  if (!text(item.videoId) && text(vote.videoId)) item.videoId = text(vote.videoId);
  if (!text(item.videoId) && text(meta.videoId)) item.videoId = text(meta.videoId);
  if (!text(item.headingTitle) && text(vote.headingTitle)) item.headingTitle = text(vote.headingTitle);
  if (!text(item.videoTitle) && text(vote.videoTitle)) item.videoTitle = text(vote.videoTitle);
  if (!text(item.videoTitle) && text(meta.title)) item.videoTitle = text(meta.title);
  if (!text(item.sourceVideoTitle) && text(vote.sourceVideoTitle)) item.sourceVideoTitle = text(vote.sourceVideoTitle);
  if (!text(item.sourceVideoTitle) && text(vote.videoTitle)) item.sourceVideoTitle = text(vote.videoTitle);
  if (!text(item.sourceVideoTitle) && text(meta.title)) item.sourceVideoTitle = text(meta.title);
  if (!text(item.sourceVideoUrl) && text(vote.sourceVideoUrl)) item.sourceVideoUrl = text(vote.sourceVideoUrl);
  if (!text(item.sourceVideoUrl) && text(vote.videoUrl)) item.sourceVideoUrl = text(vote.videoUrl);
  if (!text(item.sourceVideoUrl) && text(meta.url)) item.sourceVideoUrl = text(meta.url);
  if (!text(item.headingStart) && text(vote.headingStart)) item.headingStart = text(vote.headingStart);
  item.publishedAt = pickFirstValidDate(item.publishedAt, vote.publishedAt, vote.videoDate, meta.publishedAt, meta.videoDate);
  item.videoDate = pickFirstValidDate(item.videoDate, vote.videoDate, vote.publishedAt, meta.videoDate, meta.publishedAt);
  if (!text(item.sourceMode) && text(vote.sourceMode)) item.sourceMode = text(vote.sourceMode);
}

function buildRecentRecommendations(votes, generatedAtIso, windowHours = 240, videoMetadataMap = new Map()) {
  const maps = ensureVideoMetadataMaps(videoMetadataMap);
  const generatedAtMs = Date.parse(text(generatedAtIso));
  if (!Number.isFinite(generatedAtMs)) return [];
  const windowStartMs = generatedAtMs - windowHours * 60 * 60 * 1000;

  const recentMap = new Map();
  for (const vote of votes) {
    const resolvedVote = resolveVoteMetadata(vote, maps);
    const eventAt = text(resolvedVote.firstVotedAt);
    const eventMs = Date.parse(eventAt);
    if (!Number.isFinite(eventMs)) continue;
    if (eventMs < windowStartMs || eventMs > generatedAtMs) continue;

    const headingId = text(resolvedVote.headingId);
    if (!headingId) continue;

    if (!recentMap.has(headingId)) recentMap.set(headingId, createAggregateItem(resolvedVote));

    const item = recentMap.get(headingId);
    backfillAggregateMetadata(item, resolvedVote, maps.byVideoId.get(text(item.videoId)) || {});
    item.voteCount += 1;
    if (eventAt < text(item.firstVotedAt)) item.firstVotedAt = eventAt;
    if (eventAt > text(item.lastVotedAt)) item.lastVotedAt = eventAt;
  }

  return Array.from(recentMap.values()).sort((a, b) => {
    if (b.voteCount !== a.voteCount) return b.voteCount - a.voteCount;
    const aLast = parseEventMs(a.lastVotedAt) ?? -Infinity;
    const bLast = parseEventMs(b.lastVotedAt) ?? -Infinity;
    if (bLast !== aLast) return bLast - aLast;
    const aPublished = parsePublishedAt(text(a.publishedAt) || text(a.videoDate)) ?? Number.MAX_SAFE_INTEGER;
    const bPublished = parsePublishedAt(text(b.publishedAt) || text(b.videoDate)) ?? Number.MAX_SAFE_INTEGER;
    if (aPublished !== bPublished) return aPublished - bPublished;
    const videoCmp = text(a.videoId).localeCompare(text(b.videoId));
    if (videoCmp !== 0) return videoCmp;
    return text(a.headingId).localeCompare(text(b.headingId));
  });
}

function parsePublishedAt(value) {
  const raw = text(value);
  if (!raw) return null;
  const withTime = raw.includes("T") ? raw : `${raw}T00:00:00Z`;
  const ms = Date.parse(withTime);
  if (!Number.isFinite(ms)) return null;
  return ms;
}

function toJstDateString(value) {
  const raw = text(value);
  if (!raw) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  return jstDateFromIso(raw);
}

function shiftDateString(dateString, diffDays) {
  const raw = text(dateString);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return "";
  const date = new Date(`${raw}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + diffDays);
  return date.toISOString().slice(0, 10);
}

function buildRecentUploadRecommendations(rankingItems, generatedAtIso) {
  const generatedAtJstDate = toJstDateString(generatedAtIso);
  if (!generatedAtJstDate) return [];
  const windowStartDate = shiftDateString(generatedAtJstDate, -6);
  if (!windowStartDate) return [];

  const items = (Array.isArray(rankingItems) ? rankingItems : [])
    .map((item) => {
      const publishedAt = text(item?.publishedAt) || text(item?.videoDate);
      const publishedAtJstDate = toJstDateString(publishedAt);
      if (!publishedAtJstDate) return null;
      if (publishedAtJstDate < windowStartDate || publishedAtJstDate > generatedAtJstDate) return null;
      const publishedAtMs = parsePublishedAt(publishedAtJstDate);
      return { ...item, publishedAt, publishedAtMs, publishedAtJstDate };
    })
    .filter(Boolean);

  items.sort((a, b) => {
    if (Number(b.voteCount || 0) !== Number(a.voteCount || 0)) return Number(b.voteCount || 0) - Number(a.voteCount || 0);
    if (a.publishedAtMs !== b.publishedAtMs) return a.publishedAtMs - b.publishedAtMs;
    const videoCmp = text(a.videoId).localeCompare(text(b.videoId));
    if (videoCmp !== 0) return videoCmp;
    return text(a.headingId).localeCompare(text(b.headingId));
  });

  return items.map(({ publishedAtMs, publishedAtJstDate, ...item }) => item);
}

function buildAggregatesFromVotes(votes, generatedAt, videoMetadataMap = new Map()) {
  const maps = ensureVideoMetadataMaps(videoMetadataMap);
  const ranking = new Map();
  const weekly = new Map();

  for (const vote of votes) {
    const resolvedVote = resolveVoteMetadata(vote, maps);
    const headingId = text(resolvedVote.headingId);
    if (!headingId) continue;

    if (!ranking.has(headingId)) ranking.set(headingId, createAggregateItem(resolvedVote));
    const item = ranking.get(headingId);
    backfillAggregateMetadata(item, resolvedVote, maps.byVideoId.get(text(item.videoId)) || {});
    item.voteCount += 1;
    if (text(resolvedVote.firstVotedAt) < text(item.firstVotedAt)) item.firstVotedAt = text(resolvedVote.firstVotedAt);
    if (text(resolvedVote.firstVotedAt) > text(item.lastVotedAt)) item.lastVotedAt = text(resolvedVote.firstVotedAt);

    const weekKey = text(resolvedVote.weekKey) || weekKeyJstFromIso(text(resolvedVote.firstVotedAt));
    if (!weekly.has(weekKey)) weekly.set(weekKey, new Map());
    const weekMap = weekly.get(weekKey);
    if (!weekMap.has(headingId)) weekMap.set(headingId, createAggregateItem(resolvedVote));
    const weekItem = weekMap.get(headingId);
    backfillAggregateMetadata(weekItem, resolvedVote, maps.byVideoId.get(text(weekItem.videoId)) || {});
    weekItem.voteCount += 1;
    if (text(resolvedVote.firstVotedAt) < text(weekItem.firstVotedAt)) weekItem.firstVotedAt = text(resolvedVote.firstVotedAt);
    if (text(resolvedVote.firstVotedAt) > text(weekItem.lastVotedAt)) weekItem.lastVotedAt = text(resolvedVote.firstVotedAt);
  }

  for (const item of ranking.values()) {
    backfillAggregateMetadata(item, {}, maps.byVideoId.get(text(item.videoId)) || {});
  }
  for (const map of weekly.values()) {
    for (const item of map.values()) {
      backfillAggregateMetadata(item, {}, maps.byVideoId.get(text(item.videoId)) || {});
    }
  }

  const sorted = Array.from(ranking.values()).sort((a, b) => {
    if (b.voteCount !== a.voteCount) return b.voteCount - a.voteCount;
    const aPublished = parsePublishedAt(text(a.publishedAt) || text(a.videoDate)) ?? Number.MAX_SAFE_INTEGER;
    const bPublished = parsePublishedAt(text(b.publishedAt) || text(b.videoDate)) ?? Number.MAX_SAFE_INTEGER;
    if (aPublished !== bPublished) return aPublished - bPublished;
    const videoCmp = text(a.videoId).localeCompare(text(b.videoId));
    if (videoCmp !== 0) return videoCmp;
    return text(a.headingId).localeCompare(text(b.headingId));
  });

  const recentItems = buildRecentRecommendations(votes, generatedAt, 240, maps);
  const recentUploadItems = buildRecentUploadRecommendations(sorted, generatedAt);
  return { sorted, weekly, recentItems, recentUploadItems };
}

function canonicalVoteMetadata(receivedAtIso, payloadTimestamp) {
  const firstVotedAt = text(receivedAtIso);
  const metadata = {
    firstVotedAt,
    weekKey: weekKeyJstFromIso(firstVotedAt),
  };
  const clientTimestamp = text(payloadTimestamp);
  if (clientTimestamp) metadata.clientTimestamp = clientTimestamp;
  return metadata;
}

function hasCompleteMetadata(payload, rawInput = payload) {
  return collectMetadataValidationReasons(payload, rawInput).length === 0;
}

async function hmacSha256Hex(secret, message) {
  const key = await crypto.subtle.importKey(
    "raw",
    utf8(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const digest = await crypto.subtle.sign("HMAC", key, utf8(message));
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function hashWithSecret(secret, scope, value) {
  const normalizedSecret = text(secret);
  if (!normalizedSecret) {
    throw new Error("FAVORITES_HASH_SECRET is required");
  }
  const normalized = text(value);
  if (!normalized) return "";
  return hmacSha256Hex(normalizedSecret, `${scope}:${normalized}`);
}


function parseAllowedOrigins(value) {
  return text(value)
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean);
}

function resolveAllowedOrigin(request, env) {
  const origin = text(request.headers.get("Origin"));
  if (!origin) return "";
  const allowedOrigins = parseAllowedOrigins(env.FAVORITES_ALLOWED_ORIGINS);
  if (!allowedOrigins.length) return "";
  if (allowedOrigins.includes("*")) return origin;
  return allowedOrigins.includes(origin) ? origin : "";
}

function withCorsHeaders(headers, request, env) {
  const next = new Headers(headers || {});
  const allowOrigin = resolveAllowedOrigin(request, env);
  if (allowOrigin) {
    next.set("Access-Control-Allow-Origin", allowOrigin);
  }
  next.set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  next.set("Access-Control-Allow-Headers", "Content-Type, X-Favorites-Admin-Token");
  next.set("Access-Control-Max-Age", "86400");
  next.set("Vary", "Origin");
  return next;
}

function withCorsResponse(response, request, env) {
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: withCorsHeaders(response.headers, request, env),
  });
}

function jsonResponse(payload, status = 200, headers = {}, request = null, env = null) {
  const response = new Response(JSON.stringify(payload), {
    status,
    headers: { ...JSON_HEADERS, ...headers },
  });
  if (!request || !env) return response;
  return withCorsResponse(response, request, env);
}

async function readJsonObject(bucket, key) {
  const object = await bucket.get(key);
  if (!object) return null;
  return JSON.parse(await object.text());
}

async function writeVote(request, env) {
  const payload = await request.json().catch(() => null);
  const headingId = text(payload?.headingId);
  const clientId = text(payload?.clientId);
  if (!headingId || !clientId) {
    return jsonResponse({ status: "error", message: "headingId と clientId は必須です。" }, 400, {}, request, env);
  }

  const receivedAt = canonicalServerTimestamp();
  const clientHash = await hashWithSecret(env.FAVORITES_HASH_SECRET, "client", clientId);
  const ip = text(request.headers.get("CF-Connecting-IP"));
  const ua = text(request.headers.get("User-Agent"));
  const ipHash = ip ? await hashWithSecret(env.FAVORITES_HASH_SECRET, "ip", ip) : "";
  const uaHash = ua ? await hashWithSecret(env.FAVORITES_HASH_SECRET, "ua", ua) : "";

  const key = `favorites/unique/${encodeURIComponent(headingId)}/${clientHash}.json`;
  const existing = await env.FAVORITES_BUCKET.get(key);
  if (existing) {
    return jsonResponse({ status: "duplicate", accepted: false, key }, 200, {}, request, env);
  }

  const voteMeta = canonicalVoteMetadata(receivedAt, payload?.timestamp);
  const rawPayload = {
    headingId,
    videoId: text(payload?.videoId),
    headingTitle: text(payload?.headingTitle),
    videoTitle: text(payload?.videoTitle),
    sourceVideoTitle: text(payload?.sourceVideoTitle),
    sourceVideoUrl: text(payload?.sourceVideoUrl) || text(payload?.videoUrl),
    headingStart: text(payload?.headingStart),
    sourceMode: text(payload?.sourceMode) || "unknown",
    publishedAt: text(payload?.publishedAt),
    videoDate: text(payload?.videoDate),
  };
  const normalizedPayload = normalizeVotePayload(rawPayload);

  let resolvedPayload = {
    ...normalizedPayload,
    metadataIncomplete: false,
    metadataIncompleteReason: [],
  };
  if (!hasCompleteMetadata(normalizedPayload, rawPayload)) {
    const talksPayload = await readJsonObject(env.FAVORITES_BUCKET, "index/talks.json");
    const latestPayload = await readJsonObject(env.FAVORITES_BUCKET, "index/latest.json");
    const metadataMaps = buildVideoMetadataMaps(talksPayload || {}, latestPayload || {});
    resolvedPayload = resolveVoteMetadata(normalizedPayload, metadataMaps, rawPayload);
  } else {
    const reasons = collectMetadataValidationReasons(normalizedPayload, rawPayload);
    resolvedPayload.metadataIncomplete = reasons.length > 0;
    resolvedPayload.metadataIncompleteReason = reasons;
  }

  const body = {
    headingId: resolvedPayload.headingId,
    clientHash,
    videoId: resolvedPayload.videoId,
    headingTitle: resolvedPayload.headingTitle,
    videoTitle: resolvedPayload.videoTitle,
    headingStart: resolvedPayload.headingStart,
    sourceMode: resolvedPayload.sourceMode,
    sourceVideoUrl: resolvedPayload.sourceVideoUrl,
    sourceVideoTitle: resolvedPayload.sourceVideoTitle,
    publishedAt: resolvedPayload.publishedAt,
    videoDate: resolvedPayload.videoDate,
    firstVotedAt: voteMeta.firstVotedAt,
    weekKey: voteMeta.weekKey,
    ipHash,
    uaHash,
  };
  if (resolvedPayload.metadataIncomplete) {
    body.metadataIncomplete = true;
    body.metadataIncompleteReason = Array.isArray(resolvedPayload.metadataIncompleteReason)
      ? resolvedPayload.metadataIncompleteReason
      : [];
    console.warn("[favorites] metadata incomplete vote saved", {
      headingId: body.headingId,
      reasons: body.metadataIncompleteReason,
    });
  }
  if (voteMeta.clientTimestamp) body.clientTimestamp = voteMeta.clientTimestamp;

  await env.FAVORITES_BUCKET.put(key, JSON.stringify(body), {
    httpMetadata: {
      contentType: "application/json; charset=utf-8",
      cacheControl: "no-store",
    },
  });

  return jsonResponse({ status: "accepted", accepted: true, key }, 200, {}, request, env);
}

async function rebuildAggregates(request, env) {
  const adminToken = text(request.headers.get("x-favorites-admin-token"));
  if (!adminToken || adminToken !== text(env.FAVORITES_ADMIN_TOKEN)) {
    return jsonResponse({ status: "error", message: "unauthorized" }, 401, {}, request, env);
  }

  const votes = [];
  let cursor;
  do {
    const listed = await env.FAVORITES_BUCKET.list({ prefix: "favorites/unique/", cursor });
    cursor = listed.truncated ? listed.cursor : undefined;
    for (const object of listed.objects) {
      if (!object.key.endsWith(".json")) continue;
      const row = await readJsonObject(env.FAVORITES_BUCKET, object.key);
      if (row) votes.push(row);
    }
  } while (cursor);

  const generatedAt = new Date().toISOString();
  const talksPayload = await readJsonObject(env.FAVORITES_BUCKET, "index/talks.json");
  const latestPayload = await readJsonObject(env.FAVORITES_BUCKET, "index/latest.json");
  const videoMetadataMap = buildVideoMetadataMaps(talksPayload || {}, latestPayload || {});
  const { sorted, weekly, recentItems, recentUploadItems } = buildAggregatesFromVotes(votes, generatedAt, videoMetadataMap);

  const allTime = { generatedAt, source: "favorites/unique", items: sorted };
  const hall = { generatedAt, source: "favorites/unique", items: sorted };
  const recent = { generatedAt, source: "favorites/unique", items: recentItems };
  const recentUpload = { generatedAt, source: "favorites/unique", items: recentUploadItems };
  const currentRanking = { generatedAt, source: "favorites/unique", items: sorted };
  const snapshotDate = jstDateFromIso(generatedAt);
  const dailySnapshot = { generatedAt, source: "favorites/unique", snapshotDate, items: sorted };

  await Promise.all([
    env.FAVORITES_BUCKET.put("favorites/aggregates/all_time.json", JSON.stringify(allTime)),
    env.FAVORITES_BUCKET.put("favorites/aggregates/hall_of_fame.json", JSON.stringify(hall)),
    env.FAVORITES_BUCKET.put("favorites/aggregates/recent_recommendations.json", JSON.stringify(recent)),
    env.FAVORITES_BUCKET.put("favorites/aggregates/recent_upload_recommendations.json", JSON.stringify(recentUpload)),
    env.FAVORITES_BUCKET.put("favorites/exports/current_ranking.json", JSON.stringify(currentRanking)),
    env.FAVORITES_BUCKET.put(`favorites/exports/daily_snapshot/${snapshotDate}.json`, JSON.stringify(dailySnapshot)),
  ]);

  for (const [weekKey, map] of weekly.entries()) {
    const items = Array.from(map.values()).sort((a, b) => {
      if (b.voteCount !== a.voteCount) return b.voteCount - a.voteCount;
      const aPublished = parsePublishedAt(text(a.publishedAt) || text(a.videoDate)) ?? Number.MAX_SAFE_INTEGER;
      const bPublished = parsePublishedAt(text(b.publishedAt) || text(b.videoDate)) ?? Number.MAX_SAFE_INTEGER;
      if (aPublished !== bPublished) return aPublished - bPublished;
      const videoCmp = text(a.videoId).localeCompare(text(b.videoId));
      if (videoCmp !== 0) return videoCmp;
      return text(a.headingId).localeCompare(text(b.headingId));
    });
    await env.FAVORITES_BUCKET.put(
      `favorites/aggregates/weekly/${weekKey}.json`,
      JSON.stringify({ generatedAt, source: "favorites/unique", weekKey, items }),
    );
  }

  return jsonResponse({ status: "ok", uniqueVotes: votes.length }, 200, {}, request, env);
}

async function readAggregate(request, env, key) {
  const object = await env.FAVORITES_BUCKET.get(key);
  if (!object) return jsonResponse({ status: "error", message: "not found" }, 404, { "cache-control": "no-store" }, request, env);
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("cache-control", "public, max-age=60");
  return withCorsResponse(new Response(object.body, { status: 200, headers }), request, env);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: withCorsHeaders({}, request, env) });
    }

    if (request.method === "POST" && url.pathname === "/favorites/vote") {
      return writeVote(request, env);
    }
    if (request.method === "POST" && url.pathname === "/favorites/admin/rebuild") {
      return rebuildAggregates(request, env);
    }
    if (request.method === "GET" && url.pathname === "/favorites/hall_of_fame.json") {
      return readAggregate(request, env, "favorites/aggregates/hall_of_fame.json");
    }
    if (request.method === "GET" && url.pathname === "/favorites/recent_recommendations.json") {
      return readAggregate(request, env, "favorites/aggregates/recent_recommendations.json");
    }
    if (request.method === "GET" && url.pathname === "/favorites/current_ranking.json") {
      return readAggregate(request, env, "favorites/exports/current_ranking.json");
    }
    if (request.method === "GET" && url.pathname === "/favorites/recent_upload_recommendations.json") {
      return readAggregate(request, env, "favorites/aggregates/recent_upload_recommendations.json");
    }

    return jsonResponse({ status: "error", message: "not found" }, 404, {}, request, env);
  },
};

export {
  canonicalServerTimestamp,
  jstDateFromIso,
  weekKeyJstFromIso,
  previousCompletedWeekKeyFromIso,
  buildRecentRecommendations,
  buildRecentUploadRecommendations,
  normalizeTitle,
  extractYouTubeVideoId,
  canonicalizeYouTubeUrl,
  normalizeVotePayload,
  buildVideoMetadataMaps,
  buildVideoMetadataMap,
  resolveVoteMetadata,
  buildAggregatesFromVotes,
  hashWithSecret,
  canonicalVoteMetadata,
};

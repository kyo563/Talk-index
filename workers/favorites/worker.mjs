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

function buildVideoMetadataMap(talksPayload, latestPayload) {
  const map = new Map();

  const talks = Array.isArray(talksPayload?.talks) ? talksPayload.talks : [];
  for (const talk of talks) {
    if (!talk || typeof talk !== "object") continue;
    const talkDate = text(talk.date);
    const subsections = Array.isArray(talk.subsections) ? talk.subsections : [];
    for (const subsection of subsections) {
      if (!subsection || typeof subsection !== "object") continue;
      const url = text(subsection.videoUrl);
      const videoId = text(subsection.videoId) || extractVideoId(url);
      if (!videoId) continue;
      map.set(videoId, {
        title: text(subsection.videoTitle),
        url,
        publishedAt: talkDate,
      });
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
    const videoId = text(item.id) || text(item.videoId) || extractVideoId(text(item.url));
    if (!videoId) continue;
    const existing = map.get(videoId) || {};
    map.set(videoId, {
      title: text(item.title) || text(existing.title),
      url: text(item.url) || text(existing.url),
      publishedAt: text(item.date) || text(item.publishedAt) || text(existing.publishedAt),
    });
  }

  return map;
}

function extractVideoId(url) {
  const raw = text(url);
  if (!raw) return "";
  try {
    const parsed = new URL(raw);
    if (parsed.hostname.includes("youtu.be")) {
      return text(parsed.pathname.split("/").filter(Boolean)[0]);
    }
    if (parsed.hostname.includes("youtube.com")) {
      return text(parsed.searchParams.get("v"));
    }
  } catch {
    // noop
  }
  return "";
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
  if (!text(item.publishedAt) && text(vote.publishedAt)) item.publishedAt = text(vote.publishedAt);
  if (!text(item.publishedAt) && text(vote.videoDate)) item.publishedAt = text(vote.videoDate);
  if (!text(item.publishedAt) && text(meta.publishedAt)) item.publishedAt = text(meta.publishedAt);
  if (!text(item.videoDate) && text(vote.videoDate)) item.videoDate = text(vote.videoDate);
  if (!text(item.videoDate) && text(vote.publishedAt)) item.videoDate = text(vote.publishedAt);
  if (!text(item.videoDate) && text(meta.publishedAt)) item.videoDate = text(meta.publishedAt);
  if (!text(item.sourceMode) && text(vote.sourceMode)) item.sourceMode = text(vote.sourceMode);
}

function buildRecentRecommendations(votes, generatedAtIso, windowHours = 240, videoMetadataMap = new Map()) {
  const generatedAtMs = Date.parse(text(generatedAtIso));
  if (!Number.isFinite(generatedAtMs)) return [];
  const windowStartMs = generatedAtMs - windowHours * 60 * 60 * 1000;

  const recentMap = new Map();
  for (const vote of votes) {
    const eventAt = text(vote.firstVotedAt);
    const eventMs = Date.parse(eventAt);
    if (!Number.isFinite(eventMs)) continue;
    if (eventMs < windowStartMs || eventMs > generatedAtMs) continue;

    const headingId = text(vote.headingId);
    if (!headingId) continue;

    if (!recentMap.has(headingId)) recentMap.set(headingId, createAggregateItem(vote));

    const item = recentMap.get(headingId);
    backfillAggregateMetadata(item, vote, videoMetadataMap.get(text(item.videoId)) || {});
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
  const ranking = new Map();
  const weekly = new Map();

  for (const vote of votes) {
    const headingId = text(vote.headingId);
    if (!headingId) continue;

    if (!ranking.has(headingId)) ranking.set(headingId, createAggregateItem(vote));
    const item = ranking.get(headingId);
    backfillAggregateMetadata(item, vote, videoMetadataMap.get(text(item.videoId)) || {});
    item.voteCount += 1;
    if (text(vote.firstVotedAt) < text(item.firstVotedAt)) item.firstVotedAt = text(vote.firstVotedAt);
    if (text(vote.firstVotedAt) > text(item.lastVotedAt)) item.lastVotedAt = text(vote.firstVotedAt);

    const weekKey = text(vote.weekKey) || weekKeyJstFromIso(text(vote.firstVotedAt));
    if (!weekly.has(weekKey)) weekly.set(weekKey, new Map());
    const weekMap = weekly.get(weekKey);
    if (!weekMap.has(headingId)) weekMap.set(headingId, createAggregateItem(vote));
    const weekItem = weekMap.get(headingId);
    backfillAggregateMetadata(weekItem, vote, videoMetadataMap.get(text(weekItem.videoId)) || {});
    weekItem.voteCount += 1;
    if (text(vote.firstVotedAt) < text(weekItem.firstVotedAt)) weekItem.firstVotedAt = text(vote.firstVotedAt);
    if (text(vote.firstVotedAt) > text(weekItem.lastVotedAt)) weekItem.lastVotedAt = text(vote.firstVotedAt);
  }

  for (const item of ranking.values()) {
    backfillAggregateMetadata(item, {}, videoMetadataMap.get(text(item.videoId)) || {});
  }
  for (const map of weekly.values()) {
    for (const item of map.values()) {
      backfillAggregateMetadata(item, {}, videoMetadataMap.get(text(item.videoId)) || {});
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

  const recentItems = buildRecentRecommendations(votes, generatedAt, 240, videoMetadataMap);
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
  const body = {
    headingId,
    clientHash,
    videoId: text(payload?.videoId),
    headingTitle: text(payload?.headingTitle),
    videoTitle: text(payload?.videoTitle),
    headingStart: text(payload?.headingStart),
    sourceMode: text(payload?.sourceMode) || "unknown",
    sourceVideoUrl: text(payload?.sourceVideoUrl) || text(payload?.videoUrl),
    sourceVideoTitle: text(payload?.sourceVideoTitle),
    publishedAt: text(payload?.publishedAt),
    firstVotedAt: voteMeta.firstVotedAt,
    weekKey: voteMeta.weekKey,
    ipHash,
    uaHash,
  };
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
  const videoMetadataMap = buildVideoMetadataMap(talksPayload || {}, latestPayload || {});
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
  buildVideoMetadataMap,
  buildAggregatesFromVotes,
  hashWithSecret,
  canonicalVoteMetadata,
};

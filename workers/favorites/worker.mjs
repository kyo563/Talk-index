const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
};

function text(value) {
  return String(value || "").trim();
}

async function sha256Hex(input) {
  const bytes = new TextEncoder().encode(input);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function hashWithSecret(secret, scope, value) {
  const normalizedSecret = text(secret);
  if (!normalizedSecret) {
    throw new Error("FAVORITES_HASH_SECRET is required");
  }
  const normalized = text(value);
  if (!normalized) return "";
  return sha256Hex(`${scope}:${normalizedSecret}:${normalized}`);
}

function jsonResponse(payload, status = 200, headers = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { ...JSON_HEADERS, ...headers },
  });
}

function weekKeyJst(iso) {
  const raw = text(iso);
  const date = new Date(raw || Date.now());
  const jstMs = date.getTime() + 9 * 60 * 60 * 1000;
  const jst = new Date(jstMs);
  const day = jst.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  jst.setUTCDate(jst.getUTCDate() + diff);
  return jst.toISOString().slice(0, 10);
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
    return jsonResponse({ status: "error", message: "headingId と clientId は必須です。" }, 400);
  }

  const now = new Date().toISOString();
  const clientHash = await hashWithSecret(env.FAVORITES_HASH_SECRET, "client", clientId);
  const ip = text(request.headers.get("CF-Connecting-IP"));
  const ua = text(request.headers.get("User-Agent"));
  const ipHash = ip ? await hashWithSecret(env.FAVORITES_HASH_SECRET, "ip", ip) : "";
  const uaHash = ua ? await hashWithSecret(env.FAVORITES_HASH_SECRET, "ua", ua) : "";

  const key = `favorites/unique/${encodeURIComponent(headingId)}/${clientHash}.json`;
  const existing = await env.FAVORITES_BUCKET.get(key);
  if (existing) {
    return jsonResponse({ status: "duplicate", accepted: false, key });
  }

  const firstVotedAt = text(payload?.timestamp) || now;
  const body = {
    headingId,
    clientHash,
    videoId: text(payload?.videoId),
    headingTitle: text(payload?.headingTitle),
    videoTitle: text(payload?.videoTitle),
    headingStart: text(payload?.headingStart),
    sourceMode: text(payload?.sourceMode) || "unknown",
    firstVotedAt,
    weekKey: weekKeyJst(firstVotedAt),
    ipHash,
    uaHash,
  };

  await env.FAVORITES_BUCKET.put(key, JSON.stringify(body), {
    httpMetadata: {
      contentType: "application/json; charset=utf-8",
      cacheControl: "no-store",
    },
  });

  return jsonResponse({ status: "accepted", accepted: true, key });
}

async function rebuildAggregates(request, env) {
  const adminToken = text(request.headers.get("x-favorites-admin-token"));
  if (!adminToken || adminToken !== text(env.FAVORITES_ADMIN_TOKEN)) {
    return jsonResponse({ status: "error", message: "unauthorized" }, 401);
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

  const ranking = new Map();
  const weekly = new Map();

  for (const vote of votes) {
    const headingId = text(vote.headingId);
    if (!headingId) continue;
    if (!ranking.has(headingId)) {
      ranking.set(headingId, {
        headingId,
        videoId: text(vote.videoId),
        headingTitle: text(vote.headingTitle) || headingId,
        videoTitle: text(vote.videoTitle),
        headingStart: text(vote.headingStart),
        sourceMode: text(vote.sourceMode) || "unknown",
        voteCount: 0,
        firstVotedAt: text(vote.firstVotedAt),
        lastVotedAt: text(vote.firstVotedAt),
      });
    }
    const item = ranking.get(headingId);
    item.voteCount += 1;
    if (text(vote.firstVotedAt) < text(item.firstVotedAt)) item.firstVotedAt = text(vote.firstVotedAt);
    if (text(vote.firstVotedAt) > text(item.lastVotedAt)) item.lastVotedAt = text(vote.firstVotedAt);

    const weekKey = text(vote.weekKey) || weekKeyJst(text(vote.firstVotedAt));
    if (!weekly.has(weekKey)) weekly.set(weekKey, new Map());
    const weekMap = weekly.get(weekKey);
    if (!weekMap.has(headingId)) {
      weekMap.set(headingId, { ...item, voteCount: 0 });
    }
    weekMap.get(headingId).voteCount += 1;
  }

  const sorted = Array.from(ranking.values()).sort((a, b) => {
    if (b.voteCount !== a.voteCount) return b.voteCount - a.voteCount;
    if (a.firstVotedAt !== b.firstVotedAt) return a.firstVotedAt.localeCompare(b.firstVotedAt);
    return a.headingId.localeCompare(b.headingId);
  });

  const generatedAt = new Date().toISOString();
  const currentWeekKey = weekKeyJst(generatedAt);
  const weeklyCurrent = Array.from((weekly.get(currentWeekKey) || new Map()).values()).sort((a, b) => {
    if (b.voteCount !== a.voteCount) return b.voteCount - a.voteCount;
    if (a.firstVotedAt !== b.firstVotedAt) return a.firstVotedAt.localeCompare(b.firstVotedAt);
    return a.headingId.localeCompare(b.headingId);
  });

  const allTime = { generatedAt, source: "favorites/unique", items: sorted };
  const hall = { generatedAt, source: "favorites/unique", items: sorted.slice(0, 3) };
  const recent = { generatedAt, source: "favorites/unique", weekKey: currentWeekKey, items: weeklyCurrent.slice(0, 5) };
  const currentRanking = { generatedAt, source: "favorites/unique", items: sorted };
  const snapshotDate = weekKeyJst(generatedAt);
  const dailySnapshot = { generatedAt, source: "favorites/unique", snapshotDate, items: sorted };

  await Promise.all([
    env.FAVORITES_BUCKET.put("favorites/aggregates/all_time.json", JSON.stringify(allTime)),
    env.FAVORITES_BUCKET.put("favorites/aggregates/hall_of_fame.json", JSON.stringify(hall)),
    env.FAVORITES_BUCKET.put("favorites/aggregates/recent_recommendations.json", JSON.stringify(recent)),
    env.FAVORITES_BUCKET.put("favorites/exports/current_ranking.json", JSON.stringify(currentRanking)),
    env.FAVORITES_BUCKET.put(`favorites/exports/daily_snapshot/${snapshotDate}.json`, JSON.stringify(dailySnapshot)),
  ]);

  for (const [weekKey, map] of weekly.entries()) {
    const items = Array.from(map.values()).sort((a, b) => {
      if (b.voteCount !== a.voteCount) return b.voteCount - a.voteCount;
      if (a.firstVotedAt !== b.firstVotedAt) return a.firstVotedAt.localeCompare(b.firstVotedAt);
      return a.headingId.localeCompare(b.headingId);
    });
    await env.FAVORITES_BUCKET.put(
      `favorites/aggregates/weekly/${weekKey}.json`,
      JSON.stringify({ generatedAt, source: "favorites/unique", weekKey, items }),
    );
  }

  return jsonResponse({ status: "ok", uniqueVotes: votes.length, weekKey: currentWeekKey });
}

async function readAggregate(env, key) {
  const object = await env.FAVORITES_BUCKET.get(key);
  if (!object) return jsonResponse({ status: "error", message: "not found" }, 404, { "cache-control": "no-store" });
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("cache-control", "public, max-age=60");
  return new Response(object.body, { status: 200, headers });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/favorites/vote") {
      return writeVote(request, env);
    }
    if (request.method === "POST" && url.pathname === "/favorites/admin/rebuild") {
      return rebuildAggregates(request, env);
    }
    if (request.method === "GET" && url.pathname === "/favorites/hall_of_fame.json") {
      return readAggregate(env, "favorites/aggregates/hall_of_fame.json");
    }
    if (request.method === "GET" && url.pathname === "/favorites/recent_recommendations.json") {
      return readAggregate(env, "favorites/aggregates/recent_recommendations.json");
    }
    if (request.method === "GET" && url.pathname === "/favorites/current_ranking.json") {
      return readAggregate(env, "favorites/exports/current_ranking.json");
    }

    return jsonResponse({ status: "error", message: "not found" }, 404);
  },
};

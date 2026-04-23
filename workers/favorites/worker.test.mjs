import test from 'node:test';
import assert from 'node:assert/strict';

import {
  jstDateFromIso,
  weekKeyJstFromIso,
  buildRecentRecommendations,
  buildRecentUploadRecommendations,
  buildVideoMetadataMaps,
  buildVideoMetadataMap,
  resolveVoteMetadata,
  canonicalizeYouTubeUrl,
  buildAggregatesFromVotes,
  normalizeVotePayload,
  hashWithSecret,
  canonicalVoteMetadata,
  default as worker,
} from './worker.mjs';

test('recent_recommendations は generatedAt 基準の直近240時間投票で再集計する', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const votes = [
    {
      headingId: 'h-old-active',
      firstVotedAt: '2026-04-21T03:00:00Z',
      headingTitle: 'Old but active',
      videoId: 'v1',
      sourceMode: 'talk',
    },
    {
      headingId: 'h-old-active',
      firstVotedAt: '2026-04-14T12:00:00Z',
      headingTitle: 'Old but active',
      videoId: 'v1',
      sourceMode: 'talk',
    },
    {
      headingId: 'h-out',
      firstVotedAt: '2026-04-12T11:59:59Z',
      headingTitle: 'too old',
      videoId: 'v2',
      sourceMode: 'talk',
    },
  ];

  const items = buildRecentRecommendations(votes, generatedAt, 240);
  assert.equal(items.length, 1);
  assert.equal(items[0].headingId, 'h-old-active');
  assert.equal(items[0].voteCount, 2);
  assert.equal(items[0].firstVotedAt, '2026-04-14T12:00:00Z');
  assert.equal(items[0].lastVotedAt, '2026-04-21T03:00:00Z');
});

test('recent_recommendations の tie-break は lastVotedAt desc → publishedAt asc(古い優先) → headingId', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const votes = [
    { headingId: 'newer', videoId: 'newer000001', firstVotedAt: '2026-04-20T02:00:00Z', publishedAt: '2026-04-21', sourceVideoUrl: 'https://youtu.be/newer000001?t=10', sourceVideoTitle: 'new' },
    { headingId: 'older', videoId: 'older000001', firstVotedAt: '2026-04-20T02:00:00Z', publishedAt: '2026-04-20', sourceVideoUrl: 'https://www.youtube.com/watch?v=older000001&list=x', sourceVideoTitle: 'old' },
  ];

  const items = buildRecentRecommendations(votes, generatedAt, 240);
  assert.deepEqual(items.map((item) => item.headingId), ['older', 'newer']);
  assert.equal(items[0].publishedAt, '2026-04-20');
  assert.equal(items[0].sourceVideoUrl, 'https://www.youtube.com/watch?v=older000001');
  assert.equal(items[0].sourceVideoTitle, 'old');
});

test('recent_upload_recommendations は JST 更新日を含む直近7日間の公開動画のみ対象', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const items = buildRecentUploadRecommendations([
    { headingId: 'in', voteCount: 3, publishedAt: '2026-04-16' },
    { headingId: 'out', voteCount: 9, publishedAt: '2026-04-01' },
  ], generatedAt);
  assert.deepEqual(items.map((item) => item.headingId), ['in']);
});

test('recent_upload_recommendations の tie-break は publishedAt asc(古い優先) → headingId', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const items = buildRecentUploadRecommendations([
    { headingId: 'b-id', voteCount: 2, publishedAt: '2026-04-20' },
    { headingId: 'a-id', voteCount: 2, publishedAt: '2026-04-20' },
    { headingId: 'newer', voteCount: 2, publishedAt: '2026-04-21' },
  ], generatedAt);
  assert.deepEqual(items.map((item) => item.headingId), ['a-id', 'b-id', 'newer']);
});

test('recent_upload_recommendations は 2026-04-23 JST 集計で 2026-04-17 を含む', () => {
  const generatedAt = '2026-04-23T00:00:00+09:00';
  const items = buildRecentUploadRecommendations([
    { headingId: 'in-boundary', voteCount: 1, publishedAt: '2026-04-17' },
  ], generatedAt);
  assert.deepEqual(items.map((item) => item.headingId), ['in-boundary']);
});

test('recent_upload_recommendations は 2026-04-23 JST 集計で 2026-04-16 を除外', () => {
  const generatedAt = '2026-04-23T00:00:00+09:00';
  const items = buildRecentUploadRecommendations([
    { headingId: 'out-boundary', voteCount: 10, publishedAt: '2026-04-16' },
  ], generatedAt);
  assert.deepEqual(items.map((item) => item.headingId), []);
});

test('recent_upload_recommendations は date-only の境界日が時刻差で落ちない', () => {
  const generatedAt = '2026-04-23T00:30:00+09:00';
  const items = buildRecentUploadRecommendations([
    { headingId: 'date-only-boundary', voteCount: 2, publishedAt: '2026-04-17' },
  ], generatedAt);
  assert.deepEqual(items.map((item) => item.headingId), ['date-only-boundary']);
});

test('recent_upload_recommendations は ISO datetime を JST 日付に変換して判定する', () => {
  const generatedAt = '2026-04-23T00:00:00+09:00';
  const items = buildRecentUploadRecommendations([
    { headingId: 'in-iso', voteCount: 5, publishedAt: '2026-04-16T16:00:00Z' }, // JST 2026-04-17
    { headingId: 'out-iso', voteCount: 6, publishedAt: '2026-04-16T14:59:59Z' }, // JST 2026-04-16
  ], generatedAt);
  assert.deepEqual(items.map((item) => item.headingId), ['in-iso']);
});

test('aggregate は後続 vote の metadata で backfill する', () => {
  const votes = [
    {
      headingId: 'h1',
      firstVotedAt: '2026-04-20T00:00:00Z',
      weekKey: '2026-04-20',
      videoId: 'video00001a',
    },
    {
      headingId: 'h1',
      firstVotedAt: '2026-04-21T00:00:00Z',
      weekKey: '2026-04-20',
      videoId: 'video00001a',
      videoTitle: 'Video One',
      sourceVideoTitle: 'Source One',
      sourceVideoUrl: 'https://youtube.com/watch?v=video00001a',
      publishedAt: '2026-04-19',
    },
  ];

  const { sorted } = buildAggregatesFromVotes(votes, '2026-04-22T00:00:00Z');
  assert.equal(sorted[0].videoTitle, 'Video One');
  assert.equal(sorted[0].sourceVideoTitle, 'Source One');
  assert.equal(sorted[0].sourceVideoUrl, 'https://www.youtube.com/watch?v=video00001a');
  assert.equal(sorted[0].publishedAt, '2026-04-19');
});

test('ranking / hall / current 向け item に sourceVideoTitle/sourceVideoUrl が残る', () => {
  const votes = [
    {
      headingId: 'h2',
      firstVotedAt: '2026-04-21T00:00:00Z',
      weekKey: '2026-04-20',
      sourceVideoTitle: 'S2',
      sourceVideoUrl: 'https://youtu.be/rank0000001?t=5',
    },
  ];
  const { sorted } = buildAggregatesFromVotes(votes, '2026-04-22T00:00:00Z');
  assert.equal(sorted[0].sourceVideoTitle, 'S2');
  assert.equal(sorted[0].sourceVideoUrl, 'https://www.youtube.com/watch?v=rank0000001');
});

test('recent_upload_recommendations は後続 vote の publishedAt 補完で落ちない', () => {
  const votes = [
    { headingId: 'h3', videoId: 'vid-3', firstVotedAt: '2026-04-21T00:00:00Z', weekKey: '2026-04-20' },
    { headingId: 'h3', videoId: 'vid-3', firstVotedAt: '2026-04-21T01:00:00Z', weekKey: '2026-04-20', publishedAt: '2026-04-20' },
  ];
  const { recentUploadItems } = buildAggregatesFromVotes(votes, '2026-04-22T12:00:00Z');
  assert.deepEqual(recentUploadItems.map((item) => item.headingId), ['h3']);
});

test('legacy vote でも talks/latest metadata map から復元できる', () => {
  const talksPayload = {
    talks: [
      {
        date: '2026-04-20',
        subsections: [
          { videoUrl: 'https://www.youtube.com/watch?v=legacy00001', videoTitle: 'Legacy Title' },
        ],
      },
    ],
  };
  const latestPayload = { items: [] };
  const metadataMap = buildVideoMetadataMap(talksPayload, latestPayload);
  const votes = [
    {
      headingId: 'h-legacy',
      videoId: 'legacy00001',
      firstVotedAt: '2026-04-21T00:00:00Z',
      weekKey: '2026-04-20',
    },
  ];
  const { sorted, recentUploadItems } = buildAggregatesFromVotes(votes, '2026-04-22T12:00:00Z', metadataMap);
  assert.equal(sorted[0].videoTitle, 'Legacy Title');
  assert.equal(sorted[0].sourceVideoTitle, 'Legacy Title');
  assert.equal(sorted[0].sourceVideoUrl, 'https://www.youtube.com/watch?v=legacy00001');
  assert.equal(sorted[0].publishedAt, '2026-04-20');
  assert.equal(recentUploadItems.length, 1);
});

test('rebuild: videoId 欠落でも sourceVideoUrl から復元できる', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-20', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=abc123def45', videoTitle: 'From URL' }] }] },
    {},
  );
  const resolved = resolveVoteMetadata({ headingId: 'h-url', sourceVideoUrl: 'https://youtu.be/abc123def45?t=10' }, maps);
  assert.equal(resolved.videoId, 'abc123def45');
  assert.equal(resolved.publishedAt, '2026-04-20');
  assert.equal(resolved.sourceVideoUrl, canonicalizeYouTubeUrl('abc123def45'));
});

test('rebuild: sourceVideoTitle から復元できる', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-19', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=title000001', videoTitle: '  タイトル　A  ' }] }] },
    {},
  );
  const resolved = resolveVoteMetadata({ headingId: 'h-title', sourceVideoTitle: 'タイトルA' }, maps);
  assert.equal(resolved.videoId, 'title000001');
  assert.equal(resolved.publishedAt, '2026-04-19');
});

test('rebuild: videoTitle から復元できる', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-18', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=title000002', videoTitle: 'Video Match' }] }] },
    {},
  );
  const resolved = resolveVoteMetadata({ headingId: 'h-video-title', videoTitle: 'Video Match' }, maps);
  assert.equal(resolved.videoId, 'title000002');
  assert.equal(resolved.publishedAt, '2026-04-18');
});

test('rebuild: 曖昧一致をせず重複 title は解決しない', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-18', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=dup00000001', videoTitle: 'SameTitle' }, { videoUrl: 'https://www.youtube.com/watch?v=dup00000002', videoTitle: 'SameTitle' }] }] },
    {},
  );
  const resolved = resolveVoteMetadata({ headingId: 'h-dup', sourceVideoTitle: 'SameTitle' }, maps);
  assert.equal(resolved.videoId, '');
  assert.equal(resolved.metadataIncomplete, true);
  assert.deepEqual(resolved.metadataIncompleteReason, ['missing_video_id', 'missing_published_at', 'missing_source_video_url', 'title_ambiguous']);
});

test('rebuild: headingTitle 単独一致では解決しない', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ headingId: 'h-1', name: '雑談', date: '2026-04-18', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=solohead001', videoTitle: 'A' }] }] },
    {},
  );
  const resolved = resolveVoteMetadata({ headingId: 'h-x', headingTitle: '雑談' }, maps);
  assert.equal(resolved.videoId, '');
  assert.equal(resolved.metadataIncomplete, true);
});

test('rebuild: 補完後に recent_upload_recommendations に残る', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-22', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=up000000001', videoTitle: 'Upload Target' }] }] },
    {},
  );
  const votes = [
    { headingId: 'h-up', firstVotedAt: '2026-04-23T00:00:00+09:00', sourceVideoUrl: 'https://youtube.com/watch?v=up000000001&list=x' },
  ];
  const { recentUploadItems } = buildAggregatesFromVotes(votes, '2026-04-23T00:00:00+09:00', maps);
  assert.deepEqual(recentUploadItems.map((item) => item.headingId), ['h-up']);
});

test('resolveVoteMetadata: raw publishedAt が invalid でも meta の valid date を優先する', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-20', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=meta0000001', videoTitle: 'Meta Date' }] }] },
    {},
  );
  const resolved = resolveVoteMetadata(
    { headingId: 'h-meta-date', videoId: 'meta0000001', publishedAt: 'not-a-date' },
    maps,
    { headingId: 'h-meta-date', videoId: 'meta0000001', publishedAt: 'not-a-date' },
  );
  assert.equal(resolved.publishedAt, '2026-04-20');
  assert.equal(resolved.metadataIncompleteReason.includes('invalid_published_at'), true);
});

test('normalizeVotePayload: publishedAt invalid / videoDate valid なら videoDate を採用する', () => {
  const normalized = normalizeVotePayload({
    headingId: 'h-norm-date',
    publishedAt: 'not-a-date',
    videoDate: '2026-04-20',
  });
  assert.equal(normalized.publishedAt, '2026-04-20');
  assert.equal(normalized.videoDate, '2026-04-20');
});

test('rebuild: 既存 vote の invalid publishedAt は metadata map で修復される', () => {
  const maps = buildVideoMetadataMaps(
    { talks: [{ date: '2026-04-22', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=fixdate0001', videoTitle: 'Fix Date' }] }] },
    {},
  );
  const votes = [
    {
      headingId: 'h-fix-date',
      videoId: 'fixdate0001',
      firstVotedAt: '2026-04-23T00:00:00+09:00',
      publishedAt: 'not-a-date',
    },
  ];
  const { sorted, recentUploadItems } = buildAggregatesFromVotes(votes, '2026-04-23T00:00:00+09:00', maps);
  assert.equal(sorted[0].publishedAt, '2026-04-22');
  assert.deepEqual(recentUploadItems.map((item) => item.headingId), ['h-fix-date']);
});

function createMemoryBucket(seed = {}) {
  const store = new Map(Object.entries(seed));
  return {
    async get(key) {
      if (!store.has(key)) return null;
      const value = store.get(key);
      return {
        async text() { return value; },
        body: value,
        writeHttpMetadata() {},
      };
    },
    async put(key, value) {
      store.set(key, String(value));
    },
    async list({ prefix = '' } = {}) {
      const objects = Array.from(store.keys())
        .filter((key) => key.startsWith(prefix))
        .map((key) => ({ key }));
      return { objects, truncated: false, cursor: undefined };
    },
    dump() {
      return store;
    },
  };
}

async function readStoredVote(bucket, headingId) {
  const listed = await bucket.list({ prefix: `favorites/unique/${encodeURIComponent(headingId)}/` });
  const key = listed.objects[0]?.key;
  const object = key ? await bucket.get(key) : null;
  return object ? JSON.parse(await object.text()) : null;
}

test('write: talk mode で videoId / URL / publishedAt を保存する', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({
      talks: [{ headingId: 'h-talk', name: 'Talk Heading', date: '2026-04-20', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=talk0000001', videoTitle: 'Talk Video' }] }],
    }),
    'index/latest.json': JSON.stringify({ items: [] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ headingId: 'h-talk', clientId: 'c1', sourceMode: 'talk', headingTitle: 'Talk Heading' }),
  });
  const res = await worker.fetch(req, env);
  assert.equal(res.status, 200);
  const saved = await readStoredVote(bucket, 'h-talk');
  assert.equal(saved.videoId, 'talk0000001');
  assert.equal(saved.sourceVideoUrl, 'https://www.youtube.com/watch?v=talk0000001');
  assert.equal(saved.publishedAt, '2026-04-20');
});

test('write: video mode でも videoId / URL / publishedAt を保存する', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({ talks: [] }),
    'index/latest.json': JSON.stringify({ items: [{ id: 'vid00000001', title: 'Video Mode', date: '2026-04-21', url: 'https://youtu.be/vid00000001' }] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ headingId: 'h-video', clientId: 'c2', sourceMode: 'video', sourceVideoTitle: 'Video Mode' }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-video');
  assert.equal(saved.videoId, 'vid00000001');
  assert.equal(saved.sourceVideoUrl, 'https://www.youtube.com/watch?v=vid00000001');
  assert.equal(saved.publishedAt, '2026-04-21');
});

test('write: 欠損 payload でも URL から worker 側補完できる', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({ talks: [{ date: '2026-04-22', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=fill0000001', videoTitle: 'Fill' }] }] }),
    'index/latest.json': JSON.stringify({ items: [] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ headingId: 'h-fill', clientId: 'c3', sourceVideoUrl: 'https://youtu.be/fill0000001?t=5' }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-fill');
  assert.equal(saved.videoId, 'fill0000001');
  assert.equal(saved.publishedAt, '2026-04-22');
});

test('write: 解決不能な payload は metadataIncomplete=true で保存する', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({ talks: [] }),
    'index/latest.json': JSON.stringify({ items: [] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ headingId: 'h-incomplete', clientId: 'c4', sourceMode: 'talk' }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-incomplete');
  assert.equal(saved.metadataIncomplete, true);
  assert.deepEqual(saved.metadataIncompleteReason, ['missing_video_id', 'missing_published_at', 'missing_video_title', 'missing_source_video_url']);
});

test('write: payload が十分なら index を読まず保存する', async () => {
  const bucket = createMemoryBucket();
  const originalGet = bucket.get;
  bucket.get = async (key) => {
    if (key === 'index/talks.json' || key === 'index/latest.json') {
      throw new Error('index should not be read');
    }
    return originalGet.call(bucket, key);
  };
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      headingId: 'h-direct',
      clientId: 'c5',
      videoId: 'direct00001',
      sourceVideoUrl: 'https://youtu.be/direct00001?t=1',
      sourceVideoTitle: 'Direct Vote',
      publishedAt: '2026-04-20',
      sourceMode: 'talk',
    }),
  });
  const res = await worker.fetch(req, env);
  assert.equal(res.status, 200);
  const saved = await readStoredVote(bucket, 'h-direct');
  assert.equal(saved.metadataIncomplete, undefined);
  assert.equal(saved.sourceVideoUrl, 'https://www.youtube.com/watch?v=direct00001');
});

test('write: invalid videoId は complete 扱いで素通ししない', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({ talks: [] }),
    'index/latest.json': JSON.stringify({ items: [] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      headingId: 'h-invalid-id',
      clientId: 'c6',
      videoId: 'invalid',
      sourceVideoUrl: 'https://example.com/not-youtube',
      sourceVideoTitle: 'X',
      publishedAt: '2026-04-20',
    }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-invalid-id');
  assert.equal(saved.metadataIncomplete, true);
  assert.equal(saved.videoId, '');
  assert.equal(saved.sourceVideoUrl, '');
  assert.deepEqual(saved.metadataIncompleteReason, ['invalid_video_id', 'url_unparseable']);
});

test('write: invalid publishedAt + metadata あり なら保存値は valid date に補正する', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({
      talks: [{ date: '2026-04-20', subsections: [{ videoUrl: 'https://www.youtube.com/watch?v=abc123def45', videoTitle: 'Date NG' }] }],
    }),
    'index/latest.json': JSON.stringify({ items: [] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      headingId: 'h-invalid-date',
      clientId: 'c7',
      videoId: 'abc123def45',
      sourceVideoUrl: 'https://www.youtube.com/watch?v=abc123def45',
      sourceVideoTitle: 'Date NG',
      publishedAt: 'not-a-date',
    }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-invalid-date');
  assert.equal(saved.metadataIncomplete, true);
  assert.equal(saved.publishedAt, '2026-04-20');
  assert.equal(saved.metadataIncompleteReason.includes('invalid_published_at'), true);
});

test('write: publishedAt/videoDate も invalid かつ meta 無しなら修復不能のまま保存される', async () => {
  const bucket = createMemoryBucket({
    'index/talks.json': JSON.stringify({ talks: [] }),
    'index/latest.json': JSON.stringify({ items: [] }),
  });
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      headingId: 'h-invalid-date-unrecoverable',
      clientId: 'c7b',
      videoId: 'abc123def45',
      sourceVideoUrl: 'https://www.youtube.com/watch?v=abc123def45',
      sourceVideoTitle: 'Date NG2',
      publishedAt: 'not-a-date',
      videoDate: 'still-not-a-date',
    }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-invalid-date-unrecoverable');
  assert.equal(saved.metadataIncomplete, true);
  assert.equal(saved.publishedAt, '');
  assert.equal(saved.videoDate, '');
  assert.equal(saved.metadataIncompleteReason.includes('invalid_published_at'), true);
});

test('write: videoTitle のみでも保存時に sourceVideoTitle を補完する', async () => {
  const bucket = createMemoryBucket();
  const originalGet = bucket.get;
  bucket.get = async (key) => {
    if (key === 'index/talks.json' || key === 'index/latest.json') {
      throw new Error('index should not be read');
    }
    return originalGet.call(bucket, key);
  };
  const env = { FAVORITES_BUCKET: bucket, FAVORITES_HASH_SECRET: 'secret', FAVORITES_ADMIN_TOKEN: 'admin' };
  const req = new Request('https://example.com/favorites/vote', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      headingId: 'h-title-fill',
      clientId: 'c8',
      videoId: 'titlefill01',
      sourceVideoUrl: 'https://www.youtube.com/watch?v=titlefill01',
      videoTitle: 'Title Only',
      publishedAt: '2026-04-20',
    }),
  });
  await worker.fetch(req, env);
  const saved = await readStoredVote(bucket, 'h-title-fill');
  assert.equal(saved.videoTitle, 'Title Only');
  assert.equal(saved.sourceVideoTitle, 'Title Only');
});

test('resolveVoteMetadata: URL はあるが YouTube ID 抽出不能なら url_unparseable を返す', () => {
  const maps = buildVideoMetadataMaps({ talks: [] }, { items: [] });
  const resolved = resolveVoteMetadata(
    { headingId: 'h-url-ng', sourceVideoUrl: 'https://example.com/nope', sourceVideoTitle: 'ng' },
    maps,
  );
  assert.equal(resolved.metadataIncomplete, true);
  assert.equal(resolved.metadataIncompleteReason.includes('url_unparseable'), true);
});

test('daily snapshot の JST 日付', () => {
  assert.equal(jstDateFromIso('2026-04-22T02:30:00Z'), '2026-04-22');
  assert.equal(jstDateFromIso('2026-04-21T23:30:00Z'), '2026-04-22');
});

test('weekKey は JST 週の月曜', () => {
  assert.equal(weekKeyJstFromIso('2026-04-19T15:30:00Z'), '2026-04-20');
});

test('HMAC-SHA256 固定ベクタ', async () => {
  const digest = await hashWithSecret('test-secret', 'client', 'abc123');
  assert.equal(digest, '5b2c6164ccbf4b5c088c39c8a31cd4c2c5e009a6773d86ba89a92115f69a47ab');
});

test('payload.timestamp を改ざんしても canonical 時刻と週は不変', () => {
  const receivedAt = '2026-04-21T00:00:00Z';
  const tampered = '2099-01-01T00:00:00Z';
  const meta = canonicalVoteMetadata(receivedAt, tampered);
  assert.equal(meta.firstVotedAt, receivedAt);
  assert.equal(meta.weekKey, '2026-04-20');
  assert.equal(meta.clientTimestamp, tampered);
});

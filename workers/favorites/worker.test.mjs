import test from 'node:test';
import assert from 'node:assert/strict';

import {
  jstDateFromIso,
  weekKeyJstFromIso,
  buildRecentRecommendations,
  buildRecentUploadRecommendations,
  hashWithSecret,
  canonicalVoteMetadata,
} from './worker.mjs';

test('recent_recommendations は generatedAt 基準の直近10日投票で再集計する', () => {
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

  const items = buildRecentRecommendations(votes, generatedAt, 10);
  assert.equal(items.length, 1);
  assert.equal(items[0].headingId, 'h-old-active');
  assert.equal(items[0].voteCount, 2);
  assert.equal(items[0].firstVotedAt, '2026-04-14T12:00:00Z');
  assert.equal(items[0].lastVotedAt, '2026-04-21T03:00:00Z');
});

test('recent_recommendations の tie-break は firstVotedAt → headingId', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const votes = [
    { headingId: 'b-id', firstVotedAt: '2026-04-20T01:00:00Z' },
    { headingId: 'a-id', firstVotedAt: '2026-04-20T01:00:00Z' },
  ];

  const items = buildRecentRecommendations(votes, generatedAt, 10);
  assert.deepEqual(items.map((item) => item.headingId), ['a-id', 'b-id']);
});

test('recent_upload_recommendations は1週間以内の公開動画のみ対象', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const items = buildRecentUploadRecommendations([
    { headingId: 'in', voteCount: 3, publishedAt: '2026-04-18' },
    { headingId: 'out', voteCount: 9, publishedAt: '2026-04-01' },
  ], generatedAt, 7);
  assert.deepEqual(items.map((item) => item.headingId), ['in']);
});

test('recent_upload_recommendations の tie-break は publishedAt desc → headingId', () => {
  const generatedAt = '2026-04-22T12:00:00Z';
  const items = buildRecentUploadRecommendations([
    { headingId: 'b-id', voteCount: 2, publishedAt: '2026-04-20' },
    { headingId: 'a-id', voteCount: 2, publishedAt: '2026-04-20' },
    { headingId: 'newer', voteCount: 2, publishedAt: '2026-04-21' },
  ], generatedAt, 7);
  assert.deepEqual(items.map((item) => item.headingId), ['newer', 'a-id', 'b-id']);
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

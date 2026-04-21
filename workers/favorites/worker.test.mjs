import test from 'node:test';
import assert from 'node:assert/strict';

import {
  jstDateFromIso,
  weekKeyJstFromIso,
  previousCompletedWeekKeyFromIso,
  hashWithSecret,
  canonicalVoteMetadata,
} from './worker.mjs';

test('recent_recommendations 用 weekKey は先週', () => {
  const nowIso = '2026-04-21T00:00:00Z';
  assert.equal(previousCompletedWeekKeyFromIso(nowIso), '2026-04-13');
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

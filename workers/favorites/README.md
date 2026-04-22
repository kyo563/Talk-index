# favorites worker

お気に入り投票の書き込み口と、集計JSONの読み取り口を分離する Worker です。

## Routes

- `POST /favorites/vote`
  - body: `headingId`, `clientId`, optional `videoId`, `headingTitle`, `videoTitle`, `headingStart`, `sourceMode`, `timestamp`
  - result: `accepted` / `duplicate` / `error`
- `POST /favorites/admin/rebuild`
  - header: `x-favorites-admin-token`
  - R2 の `favorites/unique/` を再集計して read model を更新
- `GET /favorites/hall_of_fame.json`
- `GET /favorites/recent_recommendations.json`
- `GET /favorites/recent_upload_recommendations.json`
- `GET /favorites/current_ranking.json`

## Bindings / Secrets

- R2 binding: `FAVORITES_BUCKET`
- secret: `FAVORITES_HASH_SECRET`
- secret: `FAVORITES_ADMIN_TOKEN`

## Notes

- 集計で使う canonical 時刻は Worker サーバ受信時刻です（`payload.timestamp` は集計に使いません）。
- `headingId + clientId` は secret hash 化して deterministic key を作成します（重複判定もこれのみ）。
- hash 方式は `HMAC-SHA256(secret, "${scope}:${value}")` です。
- `ipHash` / `uaHash` は保存のみで、重複判定には使いません。
- raw `clientId` / raw IP は保存しません。
- 集計正本は `favorites/unique/`、表示向けは `favorites/aggregates/` と `favorites/exports/` です。

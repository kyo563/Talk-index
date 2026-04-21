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
- `GET /favorites/current_ranking.json`

## Bindings / Secrets

- R2 binding: `FAVORITES_BUCKET`
- secret: `FAVORITES_HASH_SECRET`
- secret: `FAVORITES_ADMIN_TOKEN`

## Notes

- `headingId + clientId` は secret hash 化して deterministic key を作成します。
- raw `clientId` / raw IP は保存しません。
- 集計正本は `favorites/unique/`、表示向けは `favorites/aggregates/` と `favorites/exports/` です。

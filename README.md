# Talk-index

Talk-index は、YouTube の配信情報を **収集 → スプレッドシート蓄積 → JSON 生成 → Cloudflare R2 配信 → 静的フロントで閲覧** するための運用リポジトリです。

現状は、Python クローラー・JSON エクスポーター・静的フロント・GitHub Actions が連携して稼働する構成です。

## 全体フロー（現行）

1. `crawler/jobs/daily_crawl.py` が YouTube から動画情報を取得し、Google スプレッドシートへ追記
2. `exporter/sheet_to_json_and_upload_r2.py` がシートを JSON 化して R2 へアップロード
3. `index.html` + `src/main.js` が R2 の JSON を読み、ブラウザで一覧/検索表示

## 現在の構成（分割後）

```text
.
├─ src/                              # フロント本体（分割済み）
│  ├─ main.js                        # 開発時のフロント入口
│  ├─ core/state.js                  # 画面状態
│  ├─ data/                          # JSON取得・整形
│  ├─ features/                      # favorites / search
│  └─ ui/                            # 描画処理
├─ workers/favorites/                # favorites API（vote/read/rebuild）
├─ crawler/                          # 収集・シート操作
│  ├─ jobs/daily_crawl.py            # 日次クロール入口
│  └─ services/                      # youtube / spreadsheet / favorites集計補助
├─ exporter/                         # JSON出力・R2連携・favorites再集計
│  ├─ sheet_to_json_and_upload_r2.py # index JSON更新入口
│  ├─ rebuild_favorites_aggregates.py
│  └─ favorites_r2_to_sheet.py
├─ scripts/                          # build契約
├─ tests/
├─ index.html                        # フロントHTML入口
├─ app.js                            # 旧集約実装（build互換のため保持）
├─ styles.css
├─ package.json
└─ requirements.txt
```

## 主要ファイル（開発時の入口）

- フロント開発の入口: `index.html` + `src/main.js`
- フロント機能追加の主編集先:
  - 描画: `src/ui/render-results.js`, `src/ui/render-status.js`, `src/ui/render-messages.js`
  - 機能: `src/features/favorites.js`, `src/features/search.js`
  - データ取得: `src/data/fetch-json.js`, `src/data/videos.js`, `src/data/talks.js`
- 日次クロール入口: `crawler/jobs/daily_crawl.py`
- JSON/R2 更新入口: `exporter/sheet_to_json_and_upload_r2.py`
- favorites 再集計入口: `exporter/rebuild_favorites_aggregates.py`
- favorites ミラー同期入口: `exporter/favorites_r2_to_sheet.py`
- favorites API 実装: `workers/favorites/worker.mjs`
- build 契約: `scripts/validate-build-env.mjs` / `scripts/build-static.mjs` / `scripts/preview-check.mjs`

## ローカル実行

### 1) Python 依存の導入

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

必要な環境変数は `.env.example` を参考に設定してください（例: `YOUTUBE_API_KEY`）。

### 2) クローラー画面（MVP）

```bash
streamlit run crawler/app.py
```

### 3) 手動記帳画面（talk-indexDB）

```bash
streamlit run crawler/db_app.py
```

### 4) 静的フロント確認

`index.html` は `file://` 直開きだと JSON fetch が CORS 制限で失敗することがあります。ローカルサーバーで確認してください。

```bash
python -m http.server 8000
```

`http://localhost:8000` を開きます。

### 5) ビルド方法（Cloudflare Build と同等）

```bash
npm ci
npm run ci:build
```

- `validate:build-env`: 必須ファイルと `TALK_INDEX_DATA_URL` を検証
- `build`: `dist/` に成果物を出力
- `preview:check`: 出力契約を検証

## GitHub Actions / 自動運用

- `daily_crawl.yml`  
  毎日 `22:00 UTC`（`07:00 JST`）に `python -m crawler.jobs.daily_crawl` を実行。
- `upload_index_json_to_r2.yml`  
  毎日 `22:10 UTC`（`07:10 JST`）に `python exporter/sheet_to_json_and_upload_r2.py` を実行。
- `cloudflare_build_contract.yml`  
  main への push と pull request 時に `npm ci` → build 契約チェックを実行。

### 主な Secrets（用途別）

- クロール系:  
  `YOUTUBE_API_KEY`, `YOUTUBE_CHANNEL_ID`, `SPREADSHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `DAILY_MAX_RESULTS`  
  （任意）`SPREADSHEET_WORKSHEET_NAME`, `TITLE_LIST_WORKSHEET_NAME`, `DAILY_NEW_VIDEO_LIMIT`, `DAILY_RECHECK_LIMIT`
- R2 アップロード系:  
  `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`  
  + `SPREADSHEET_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON` などシート参照に必要な値
- favorites ミラー系（追加）:
  `PUBLIC_FAVORITES_SPREADSHEET_ID`（公開用 favorites ミラー出力先）

## R2 / JSON 出力

エクスポーターは R2 に次を出力します。

- `index/latest.json` : 動画カード一覧の軽量サマリー
- `index/video-details/*.json` : 各動画のセクション詳細
- `index/talks.json` : トーク単位表示用データ
- `index/search_index.json` : 検索用インデックス


## お気に入り投票（R2正本, R2 read model）

今回追加したお気に入り基盤は、**R2を正本**として次を分離しています。

- 書き込み（投票）: Worker `POST /favorites/vote`
- 読み取り（表示）: Worker `GET /favorites/*.json`
- 集計更新: `python exporter/rebuild_favorites_aggregates.py`（R2上の原票から再集計）

### 保存構造（R2）

- 原票（ユニーク投票）
  - `favorites/unique/<headingId>/<clientHash>.json`
- 集計済みJSON（read model）
  - `favorites/aggregates/all_time.json`
  - `favorites/aggregates/hall_of_fame.json`
  - `favorites/aggregates/recent_recommendations.json`
  - `favorites/aggregates/recent_upload_recommendations.json`
  - `favorites/aggregates/weekly/<weekKey>.json`
- エクスポート（シート同期しやすい形式）
  - `favorites/exports/current_ranking.json`
  - `favorites/exports/daily_snapshot/<YYYY-MM-DD>.json`

### 重複投票抑止

- 主判定: `headingId + clientId`
- Worker側で `clientId` を secret hash 化し、`favorites/unique/.../<clientHash>.json` を**決定的キー**として保存
- 同キーが既存なら duplicate/no-op を返し、集計を増やしません
- `ipHash` / `uaHash` は保存のみで、重複判定には使いません
- raw `clientId` / raw IP は保存しません（`clientHash`, optional `ipHash`, `uaHash` のみ）

### favorites 時刻・ハッシュ規則

- 集計正本の時刻（`firstVotedAt`, `weekKey`, `daily_snapshot`）は **サーバ受信時刻** を使います
- `payload.timestamp` は集計に使わず、必要時のみ `clientTimestamp` として保持します
- hash 方式は Python / Worker 共通で `HMAC-SHA256(secret, "${scope}:${value}")` です
- `recent_recommendations` は **generatedAt基準の直近10日間** の投票イベントを再集計して参照します
- `recent_upload_recommendations` は **generatedAt基準の過去7日以内に公開された動画**に属する大見出しを、累計票で上位化します

### favorites の実装位置

- フロント送受信: `src/features/favorites.js`
- フロント描画反映: `src/ui/render-results.js`（お気に入りタブ/カード）
- 状態管理: `src/core/state.js`
- 投票API/集計JSON配信: `workers/favorites/worker.mjs`
- 再集計バッチ: `exporter/rebuild_favorites_aggregates.py`
- R2→Spreadsheetミラー: `exporter/favorites_r2_to_sheet.py` / `crawler/services/favorites_mirror.py`

### フロント接続インターフェース

`src/features/favorites.js` に通信関数を集約しています。

- `sendFavoriteVote(baseUrl, payload)`
- `fetchHallOfFame(baseUrl)`
- `fetchRecentRecommendations(baseUrl)`
- `fetchRecentUploadRecommendations(baseUrl)`
- `fetchFavoriteRanking(baseUrl)`

JSON取得の共通基盤は `src/data/fetch-json.js`（`fetchJsonFromCandidates` など）を使い、favorites 側で独自の汎用JSON fetch helperは持たない方針です。

### フロントUI（最小構成）

- タブ: `動画単位 / トーク単位 / お気に入り`
- ☆/★トグル: 大見出し行の右端に配置（即時反映）
- localStorage 保存:
  - `talk_index:favorites:client_id`
  - `talk_index:favorites:heading_ids`
  - `talk_index:favorites:voted_heading_ids`
  - `talk_index:favorites:unsynced_heading_ids`
- vote送信ルール:
  - UIの☆→★は即時反映（ローカル保存を先行）
  - `alreadyVotedHeadingIds` は `POST /favorites/vote` の成功（2xx）または duplicate（409）後にだけ確定
  - 通信失敗時は `unsyncedFavoriteHeadingIds` に残し、将来再送する
  - 解除時は vote 取消APIは呼ばない
  - 既にサーバ成功済みの heading は再送しない
- お気に入りタブの3カード:
  - お気に入りリスト（localStorage基準）
  - 直近の動画のおすすめ（`/favorites/recent_upload_recommendations.json`、上位3件）
  - 殿堂入り（`/favorites/hall_of_fame.json`、上位3件）


### favorites ミラー同期（R2 → Spreadsheet）

- favorites の**正本は引き続き R2 / Worker** です。
- スプレッドシートは管理者向けの**参照ミラー**です（正本にしません）。
- 同期ジョブ: `python exporter/favorites_r2_to_sheet.py`
- 同期元JSON:
  - `favorites/exports/current_ranking.json`
  - `favorites/aggregates/hall_of_fame.json`
  - `favorites/aggregates/recent_recommendations.json`
  - `favorites/aggregates/recent_upload_recommendations.json`
  - `favorites/exports/daily_snapshot/latest.json` または `favorites/exports/daily_snapshot/YYYY-MM-DD.json`

#### 反映先シート

- `favorites_current_ranking`（毎回全置換）
- `favorites_hall_of_fame`（毎回全置換、上位3件）
- `favorites_recent_recommendations`（毎回全置換、generatedAt基準の直近10日間上位5件）
- `favorites_recent_upload_recommendations`（毎回全置換、generatedAt基準の過去7日以内公開動画を累計票で上位5件）
- `favorites_daily_snapshots`（`snapshotDate + headingId` をキーに upsert / 履歴保持）
- 公開用別スプレッドシート（`PUBLIC_FAVORITES_SPREADSHEET_ID`）:
  - `殿堂入りトーク`（毎回全置換）
  - `10日間のおすすめトーク`（毎回全置換）
  - `直近の動画のおすすめ`（毎回全置換）

#### 列

- 必須: `snapshotDate`, `weekKey`, `headingId`, `headingText`, `videoId`, `sourceVideoTitle`, `voteCount`, `rank`, `firstVotedAt`, `lastVotedAt`, `aggregateType`
- 追加: `generatedAt`, `sourceJsonUrl`, `note`
- 公開用シート列（日本語固定）: `動画投稿日`, `動画タイトル`（リンク付き）, `大見出し`, `得票数`

#### 実行方法

- 手動: GitHub Actions `Mirror favorites aggregates to Spreadsheet` を `workflow_dispatch` で実行
- 日次: 同ワークフローの `schedule`（毎日 09:00 UTC / 18:00 JST）

### favorites 読み取り/書き込み URL 設定

- favorites は READ（aggregate JSON 取得）と WRITE（vote API送信）を分離可能です。
- aggregate JSON と vote API は別ホスト構成でも動作します。
- READ 候補順:
  1. `window.__TALK_INDEX_FAVORITES_READ_BASE_URL__`
  2. `window.__TALK_INDEX_FAVORITES_BASE_URL__`
  3. `TALK_INDEX_DATA_URL` から `/index/latest.json` を除いたURL
  4. `location.origin`
- WRITE 候補順:
  1. `window.__TALK_INDEX_FAVORITES_WRITE_BASE_URL__`
  2. `window.__TALK_INDEX_FAVORITES_API_BASE_URL__`
  3. `window.__TALK_INDEX_FAVORITES_BASE_URL__`
  4. `location.origin`
- 未同期票の再送トリガー:
  - アプリ起動時
  - お気に入りタブ遷移時
  - 再お気に入り時（toggle直後）

### favorites 有効化に必要な設定（管理者向け）

- `index.html` に `window.__TALK_INDEX_FAVORITES_BASE_URL__` を設定（vote 送信先 Worker のベースURL）
- Worker 環境変数 `FAVORITES_ALLOWED_ORIGINS` を設定（許可する Origin をカンマ区切りで列挙）
- Worker 環境変数 `FAVORITES_HASH_SECRET`
- Worker 環境変数 `FAVORITES_ADMIN_TOKEN`
- Worker の R2 binding: `FAVORITES_BUCKET`

### 集計ジョブ

GitHub Actions `rebuild_favorites_aggregates.yml` を追加しています。
R2内の原票から read model を再生成し、R2に上書き保存します。


### favorites 手動確認手順（管理者向け）

1. ブラウザで ★ を押したときの送信先が Worker URL（`__TALK_INDEX_FAVORITES_BASE_URL__`）になっていることを確認
2. Worker に対する `OPTIONS /favorites/vote` が `204` を返すことを確認
3. `POST /favorites/vote` が `accepted` または `duplicate` を返すことを確認
4. R2 に `favorites/unique/...`（`favorites/unique/<headingId>/<clientHash>.json`）が作成されることを確認
5. GitHub Actions `Rebuild favorites aggregates` を実行
6. GitHub Actions `Mirror favorites aggregates to Spreadsheet` を実行

## ビルド契約（静的フロント）

- 前提 Node: **20.x**（`package.json` / `.nvmrc`）
- CI の基本コマンド:

```bash
npm ci
npm run ci:build
```

- 生成物: `dist/`（`dist/index.html`, `dist/app.js`, `dist/styles.css`）
- `TALK_INDEX_DATA_URL` を環境変数で渡すと、build 時に `dist/index.html` のデータ URL を差し替えます。

## 運用手順（最小）

### 初期設定

1. Google スプレッドシートを用意
2. GitHub Secrets を登録
3. 手動で Actions を 1 回実行して疎通確認

### 日次運用

- `daily_crawl.yml` がシート更新
- `upload_index_json_to_r2.yml` が JSON を R2 へ反映

### 失敗時確認

1. Actions ログで失敗ステップ確認
2. Secrets の空値・タイポ確認
3. スプレッドシート権限と R2 権限を確認

## 補足ドキュメント

- R2 の詳細運用: `docs/r2_operation_guide.md`

## 日次クロールの運用ルール（2026-04 更新）

- 追加シート `運用状態` は使いません。
- 巡回状態は `タイトルリスト!F1:G3` に保存します。
  - `F2/G2`: `refresh_cursor`
  - `F3/G3`: `updated_at`
- `タイトルリスト` の列定義（A:C）は次で固定です。
  - `A列`: `日付`
  - `B列`: `タイトル`
  - `C列`: `動画固有ID`
  - `F1:G3`: 状態セル（`key/value`, `refresh_cursor`, `updated_at`）
- `タイトルリスト` は並び順を固定で運用してください（並び替えしない）。
- 1回の daily crawl で、**新規2件 + 再評価5件** を処理します（環境変数で変更可）。
- 再評価は「公開後72時間以内の動画」を優先し、残りを `refresh_cursor` 巡回で補完します（`DAILY_RECENT_RECHECK_HOURS` で変更可）。
- タイムスタンプ抽出は **トップコメント + 返信コメントを優先** し、概要欄は「コメントにない時刻のみ」を補完して索引へ書き込みます（重複はトップ→返信→概要欄の順で1件採用）。
- コメント抽出のトップコメント取得数上限は `TIMESTAMP_COMMENT_THREAD_LIMIT` で調整できます（既定 300）。


## 未使用ファイル整理結果（2026-04-22 時点）

- `app.js` は現状 `index.html` の実行入口ではありません（入口は `src/main.js`）。
- ただし build 契約（`scripts/*.mjs`）が `app.js` の存在を前提にしているため、**まだ削除していません**。
- 直近は「削除候補の明確化」まで完了、実削除は build 契約更新と同時に行う方針です。

## Changelog（削除済みファイル/機能）

- 2026-04-22: `favorites-api.js` を削除。
  - 理由: favorites 通信処理を `src/features/favorites.js` に一本化し、重複実装を解消するため。
- 2026-04-22: 「favorites API helper の二重管理」を廃止。
  - 理由: fetch ポリシーを `src/data/fetch-json.js` 経由に統一し、エラー処理と再利用性を揃えるため。

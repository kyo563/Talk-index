# Talk-index

Talk-index は、YouTube の配信情報を **収集 → スプレッドシート蓄積 → JSON 生成 → Cloudflare R2 配信 → 静的フロントで閲覧** するための運用リポジトリです。

現状は、Python クローラー・JSON エクスポーター・静的フロント・GitHub Actions が連携して稼働する構成です。

## 全体フロー（現行）

1. `crawler/jobs/daily_crawl.py` が YouTube から動画情報を取得し、Google スプレッドシートへ追記
2. `exporter/sheet_to_json_and_upload_r2.py` がシートを JSON 化して R2 へアップロード
3. `index.html` + `app.js` が R2 の JSON を読み、ブラウザで一覧/検索表示

## 現在の構成

```text
.
├─ crawler/
│  ├─ app.py                         # Streamlit: 取得確認・JSON/CSVダウンロード
│  ├─ db_app.py                      # Streamlit: 手動記帳ページ（talk-indexDB）
│  ├─ config.py                      # 環境変数設定
│  ├─ models.py                      # VideoItem モデル
│  ├─ utils.py                       # URL補助・JSON/CSV変換
│  ├─ services/
│  │  ├─ youtube.py                  # YouTube Data API 呼び出し
│  │  └─ spreadsheet.py              # スプレッドシート入出力
│  └─ jobs/
│     └─ daily_crawl.py              # 日次クロール実行
├─ exporter/
│  └─ sheet_to_json_and_upload_r2.py # Sheet→JSON→R2
├─ scripts/
│  ├─ build-static.mjs               # dist 生成
│  ├─ validate-build-env.mjs         # build前提チェック
│  └─ preview-check.mjs              # build成果物チェック
├─ .github/workflows/
│  ├─ daily_crawl.yml
│  ├─ upload_index_json_to_r2.yml
│  └─ cloudflare_build_contract.yml
├─ docs/
│  └─ r2_operation_guide.md
├─ index.html
├─ app.js
├─ styles.css
├─ package.json
└─ requirements.txt
```

## 主要コンポーネント

- `crawler/app.py`  
  チャンネル ID/URL から動画を取得して画面表示し、JSON/CSV をダウンロードできます。
- `crawler/db_app.py`  
  `talk-indexDB` ページ。手動でコメント候補確認・単発記帳・一括取り込みができます。
- `crawler/jobs/daily_crawl.py`  
  日次運用ジョブ本体。新規追加（既定2件）と再評価更新（既定5件）を同時実行し、タイトルリスト右側セルに巡回状態を保存します。
- `exporter/sheet_to_json_and_upload_r2.py`  
  シート内容を `latest.json` / `talks.json` / `search_index.json` / `video-details/*.json` に変換し、R2 に配置します。
- `index.html` / `app.js` / `styles.css`  
  JSON を段階読み込みして表示する静的フロントです。
- `scripts/*.mjs`  
  Cloudflare Build で使う静的 build 契約（前提確認→build→出力検証）です。
- `.github/workflows/*.yml`  
  日次クロール、R2 反映、静的 build 契約チェックを自動実行します。

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

## R2 / JSON 出力

エクスポーターは R2 に次を出力します。

- `index/latest.json` : 動画カード一覧の軽量サマリー
- `index/video-details/*.json` : 各動画のセクション詳細
- `index/talks.json` : トーク単位表示用データ
- `index/search_index.json` : 検索用インデックス

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
- `タイトルリスト` は並び順を固定で運用してください（並び替えしない）。
- 1回の daily crawl で、**新規2件 + 再評価5件** を処理します（環境変数で変更可）。
- タイムスタンプ抽出は **概要欄 + トップコメント + 返信コメント** を統合し、重複を抑えて索引へ書き込みます。

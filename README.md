# Talk-index

YouTubeチャンネルの動画情報を取得し、将来的に検索しやすいインデックスへ育てるプロジェクトです。

## 現在の実装フェーズ

現在は **Phase 1: Streamlit ベースの YouTube動画取得ツール (MVP)** を実装しています。  
この段階では、まず「動画一覧を安定して取得・確認できること」を優先します。

---

## このMVPでできること

- チャンネルID または チャンネルURL を入力
- YouTube Data API v3 で動画一覧を取得（通常動画を優先）
- 取得項目を表で表示
- JSON / CSV でダウンロード
- エラー時に原因を表示

### 取得項目

- `video_id`
- `title`
- `url`
- `published_at`
- `thumbnail_url`
- `tags`（取得できる場合）
- `timestamp_comment`（コメント1ページ目からタイムスタンプを含む代表コメント）

---

## ディレクトリ構成（現時点）

```text
/
├─ crawler/
│  ├─ app.py                 # Streamlit エントリポイント
│  ├─ config.py              # 環境変数・設定値
│  ├─ models.py              # データモデル
│  ├─ utils.py               # 補助処理
│  └─ services/
│     └─ youtube.py          # YouTube API 呼び出し
├─ requirements.txt
├─ .env.example
└─ README.md
```

---

## セットアップ

### 1) 依存関係インストール

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) 環境変数設定

```bash
cp .env.example .env
```

`.env` に YouTube API キーを設定してください。

```env
YOUTUBE_API_KEY=your_api_key_here
DEFAULT_MAX_RESULTS=20
MAX_ALLOWED_RESULTS=200
```

### 3) 起動

```bash
streamlit run crawler/app.py
```

ブラウザで表示された画面から、チャンネルIDまたはURLを入力して実行します。

---

## 使い方（最短）

1. `チャンネルID または URL` を入力
2. `取得件数上限` を指定
3. `取得する` を押す
4. 結果を確認し、必要なら JSON / CSV をダウンロード

---

## 既知の制約

- APIクォータ制限により、大量取得は失敗する場合があります
- URL形式によってはチャンネル解決に失敗する場合があります
- ライブ配信や配信予定は除外（通常動画優先）

---

## 将来構想（Roadmap）

以下はこのMVPの次フェーズとして予定している内容です（まだ未実装）。

1. Googleスプレッドシート蓄積
2. JSON生成バッチ
3. Cloudflare R2 への配信
4. 静的 HTML/JavaScript で検索・閲覧
5. GitHub Actions で定期実行

### 将来の想定ディレクトリ（planned）

```text
/
├─ crawler/                  # Python: 収集・解析
├─ exporter/                 # Python: Sheet→JSON
├─ frontend/                 # 静的HTML/CSS/JS
├─ gas/                      # 補助用GAS（必要時のみ）
├─ .github/workflows/        # 定期実行
└─ docs/                     # 設計・運用メモ
```

---

## 設計メモ（将来向け）

- 差分更新、再試行、バックフィル戦略は維持
- ただし現在フェーズでは、まず動画一覧取得の信頼性を優先
- 未実装要素は段階的に追加

---


### 手動データ読み込みページ（talk-indexDB）

定刻の GitHub Actions とは別に、手動で差分取り込みを実行できます。

```bash
streamlit run crawler/db_app.py
```

- ページタイトル: `talk-indexDB`
- 「読み込み実行」を押すたびに 1件（または指定件数）を取り込み
- 実行ごとに成否（成功/失敗）を画面に表示

## GitHub Actions で毎日9時に自動実行（JST）

このリポジトリは、毎日 9:00（JST）にクローラーを実行し、
YouTube動画情報を Google スプレッドシートへ追記します。

- workflow: `.github/workflows/daily_crawl.yml`
- 実行スクリプト: `python -m crawler.jobs.daily_crawl`

### 必要な GitHub Secrets（daily crawl）

必須:
- `YOUTUBE_API_KEY`
- `YOUTUBE_CHANNEL_ID`
- `SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `DAILY_MAX_RESULTS`（運用中は `1` 推奨）

任意:
- `TITLE_LIST_WORKSHEET_NAME`（未指定時: `タイトルリスト`）
- `SPREADSHEET_WORKSHEET_NAME`（未指定時: `索引`）

### 差分抽出ルール（現行仕様）

- `TITLE_LIST_WORKSHEET_NAME` シートの2行目以降を参照し、**ID列またはURL列**から動画IDを抽出します。
- URL列が入っている場合は、URLから動画IDへ変換して扱います。
- タイトルリストに1件以上IDがある場合は、**そのIDに一致する動画だけ**を対象にします。
- `SPREADSHEET_WORKSHEET_NAME`（索引シート）に既存の動画IDがある場合は重複追加しません。
- タイトルリストが空の場合は、チャンネル最新動画から既存IDを除外して追記します。

### タイトルリストシートへの書き込み

`TITLE_LIST_WORKSHEET_NAME`（タイトルリスト）には、2行目以降へ次の順で追記します。

1. 動画投稿日付（JST日付）
2. 動画タイトル
3. 動画固有ID

### コメントのタイムスタンプ抽出

- 対象: 各動画のコメント1ページ目（topLevelComment）
- `0:32` / `12:05` / `1:02:33` のようなタイムスタンプを含むコメントのみ候補化
- 代表コメントは次の優先順で1件選択
  1. タイムスタンプ種類数が多い
  2. いいね数が多い
  3. コメント本文が長い
- 代表コメント内のタイムスタンプ行を索引シートへ展開
  - 大見出し: タイムスタンプ除去後の文字列
  - 小見出し: `┝` / `└` とタイムスタンプ除去後の文字列
- タイムスタンプが無い動画は、大見出し/小見出しを空欄で追記

### 索引シートへの書き込み列

`SPREADSHEET_WORKSHEET_NAME`（索引）には、2行目以降へ次の列順で追記します。

1. タイトル
2. 日付（JST日付）
3. URL
4. 大見出し
5. 大見出しURL（`?t=` 付き）
6. 小見出し
7. 小見出しURL（`?t=` 付き）
8. 自動検出タグ（YouTubeタグを `,` 連結し各タグ先頭に `#`）


## フロントエンド（静的HTML）

R2 の `index/latest.json` を読み込んで、トーク索引を閲覧できます。

- 画面: `index.html`
- ロジック: `app.js`
- スタイル: `styles.css`

### 使い方

`index.html` をブラウザで開いてください。

> メモ: JSON の参照先は `index.html` の `window.TALK_INDEX_DATA_URL`（未設定時は `index/latest.json` など）です。
>
> `file://` で `index.html` を直接開くと、ブラウザ制限（CORS）で `Failed to fetch` になる場合があります。  
> その場合はローカルサーバーで開いてください。
>
> ```bash
> python -m http.server 8000
> ```
>
> その後、`http://localhost:8000` にアクセスします。

### 軽量レコメンドの仕組み（2026-04 追加）

- 詳細を開いたときだけ「次に見そうな話題」を 2〜4件表示します。
- 各エントリで `タイトル / 大見出し / 小見出し / タグ` から簡易 token を作ります。
- 初回ロード時に `token -> エントリID一覧` の inverted index を1回だけ作ります。
- レコメンド時は、開いているエントリの token に一致する候補だけをスコア化します。
  - 一致 token 数で加点
  - 同時期なら小さく加点
- 重い類似検索や外部APIは使いません（クライアント内で完結）。

### 背景演出の調整ポイント（軽量）

- 背景は CSS 主体です（超低速グラデーション / 波 / ON AIR）。
- 反応はカード hover/focus 時のクラス切り替えだけで、JS再計算を最小化しています。
- 演出を強くしたい場合も、次の値を少しだけ調整してください。
  - `styles.css` の `--ambient-wave-opacity`
  - `@keyframes wave-slide` の duration
  - `@keyframes on-air-blink` の opacity 幅
- 可読性優先のため、明滅強化・高速化は避けてください。

## 索引シートJSONをR2へアップロード

索引シートをJSON化し、Cloudflare R2 の `index/latest.json` に配置します。

- workflow: `.github/workflows/upload_index_json_to_r2.yml`
- 実行スクリプト: `python exporter/sheet_to_json_and_upload_r2.py`
- 実行タイミング: 手動 (`workflow_dispatch`) / 毎日定期 (`schedule`)

### 必要な GitHub Secrets（R2）

必須:
- `SPREADSHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET_NAME`

任意:
- `SPREADSHEET_WORKSHEET_NAME`（未指定時: `索引`）

### 生成されるJSON

- 対象: `SPREADSHEET_WORKSHEET_NAME`（既定: `索引`）の2行目以降
- 形式: 配列（1行 = 1オブジェクト）
- キー名: シート1行目の見出しをそのまま使用
- 空行: 自動除外

## 運用手順（READMEだけで運用するための最短手順）

### 1. 初期設定（最初に1回だけ）

1. GitHub Secrets を登録（daily crawl 用 + R2 用）。
2. スプレッドシートを作成し、`GOOGLE_SERVICE_ACCOUNT_JSON` の `client_email` を編集者で共有。
3. 必要ならワークシート名を Secrets に設定（未設定なら既定値を使用）。

### 2. 日次運用の実行順

1. `daily_crawl.yml` を実行（定期実行または手動）。
2. 正常終了を確認後、`upload_index_json_to_r2.yml` を実行（または定期実行を待つ）。
3. R2 の `index/latest.json` 更新を確認。

### 3. 毎回の確認手順

1. GitHub Actions の実行結果が `Success` か確認。
2. 索引シートに新規行が増えているか確認。
3. R2 の `index/latest.json` の更新時刻が新しいか確認。

### 4. 失敗時の確認（チェックリスト）

- **環境変数エラー**: エラーログに出た不足Secret名を追加/修正。
- **シートアクセス失敗**: `client_email` 共有漏れ、`SPREADSHEET_ID`、ワークシート名を確認。
- **R2アップロード失敗**: `R2_ACCOUNT_ID` / キー / バケット名 / 書き込み権限を確認。
- **差分が入らない**: タイトルリストの ID列またはURL列に値があるか確認。

### 5. 補足

- 必須環境変数が未設定の場合は、不足している変数名を明示して fail します。
- 例外は握りつぶさず、原因がわかるメッセージで fail します。


### R2運用ガイド

R2 の初回構築・公開・キャッシュ・ロールバックは、以下のガイドを参照してください。

- `docs/r2_operation_guide.md`

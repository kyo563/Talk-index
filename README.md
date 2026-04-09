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

このリポジトリには、毎日 9:00（JST）にクローラーを実行し、
YouTube動画情報を Google スプレッドシートへ追記する workflow を追加しています。

- workflow: `.github/workflows/daily_crawl.yml`
- 実行スクリプト: `python -m crawler.jobs.daily_crawl`

### 必要な GitHub Secrets

- `YOUTUBE_API_KEY`: YouTube Data API キー
- `YOUTUBE_CHANNEL_ID`: 対象チャンネルID
- `SPREADSHEET_ID`: 書き込み先スプレッドシートID
- `GOOGLE_SERVICE_ACCOUNT_JSON`: サービスアカウントJSON（1行文字列）
- `TITLE_LIST_WORKSHEET_NAME`: タイトルリストのシート名（未指定時: `タイトルリスト`）

任意:
- `SPREADSHEET_WORKSHEET_NAME`（未指定時: `索引`）
- `DAILY_MAX_RESULTS`（必須。現在は `1` を設定して運用）

### 差分抽出ルール（更新）

- `TITLE_LIST_WORKSHEET_NAME` シートの **C2以降（動画固有ID）** から動画IDを抽出
- `TITLE_LIST_WORKSHEET_NAME` にIDが1件以上ある場合は、**そのIDに含まれる動画のみ** を対象化
- `SPREADSHEET_WORKSHEET_NAME`（索引シート）に既存の動画IDがあれば除外
- `TITLE_LIST_WORKSHEET_NAME` が空の場合は、チャンネル最新動画から既存IDを除外して追記

### タイトルリストシートへの書き込み（更新）

`TITLE_LIST_WORKSHEET_NAME`（タイトルリストシート）には、2行目以降へ次の列順で追記します。

1. 動画投稿日付（JST日付）
2. 動画タイトル
3. 動画固有ID

### コメントのタイムスタンプ抽出（更新）

- 対象は各動画の **コメント1ページ目（topLevelComment）**
- タイムスタンプ形式（例: `0:32`, `12:05`, `1:02:33`）を含むコメントだけを候補化
- 候補の中で以下の順で代表コメントを1件選択し、`timestamp_comment` に保存
  1. タイムスタンプの種類数が多い
  2. いいね数が多い
  3. コメント本文が長い
- 代表コメント内のタイムスタンプ行を **すべて** 索引シートへ追記
  - 大見出し: タイムスタンプ（`hh:mm:ss` など）を除去した文字情報のみ
  - 小見出し: `┝` / `└` とタイムスタンプ（`h:mm:ss` など）を除去した文字情報のみ
  - 大見出しごとに、対応する小見出しを紐づけて追記
- タイムスタンプが存在しない動画は、大見出し/小見出し列を空欄にして、その他列のみ追記

### 索引シートへの書き込み列（更新）

`SPREADSHEET_WORKSHEET_NAME`（索引シート）には、2行目以降へ次の列順で追記します。

1. タイトル
2. 日付（JST日付）
3. URL
4. 大見出し（タイムスタンプ除去後の文字情報）
5. 大見出しURL（`?t=` 付き動画URL）
6. 小見出し（記号・タイムスタンプ除去後の文字情報）
7. 小見出しURL（`?t=` 付き動画URL）
8. 自動検出タグ（YouTubeタグを `,` 連結し、各タグ先頭に `#` を付与）

### スプレッドシートの注意

- サービスアカウントの `client_email` を対象スプレッドシートに共有してください（編集者）。
- 初回実行時、指定ワークシートがなければ自動で作成されます。

## 索引シートJSONをR2へアップロード（新規）

索引シートをJSON化し、Cloudflare R2 の `index/latest.json` に配置する workflow を追加しました。

- workflow: `.github/workflows/upload_index_json_to_r2.yml`
- 実行スクリプト: `python exporter/sheet_to_json_and_upload_r2.py`
- 実行タイミング: 手動実行 (`workflow_dispatch`) / 毎日定期実行 (`schedule`)

### 必要な GitHub Secrets（R2アップロード用）

- `SPREADSHEET_ID`
- `SPREADSHEET_WORKSHEET_NAME`（未指定時は `索引`）
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `R2_ACCOUNT_ID`
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_BUCKET_NAME`

### 生成されるJSON

- 対象: `SPREADSHEET_WORKSHEET_NAME`（既定: `索引`）の2行目以降
- 形式: 配列（各要素は1行分のオブジェクト）
- キー名: シート1行目の見出し文字列をそのまま使用
- 空行: 自動で除外

### 失敗時の挙動

- 必須環境変数が未設定なら、どの変数が不足かを明示して fail
- シート読み取り失敗時は、設定値/権限確認を促すメッセージで fail
- R2アップロード失敗時は、認証情報・バケット名・権限確認を促すメッセージで fail

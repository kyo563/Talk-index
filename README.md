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

1. コメント欄からのタイムスタンプ抽出（main/sub構造）
2. Googleスプレッドシート蓄積
3. JSON生成バッチ
4. Cloudflare R2 への配信
5. 静的 HTML/JavaScript で検索・閲覧
6. GitHub Actions で定期実行

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

任意:
- `SPREADSHEET_WORKSHEET_NAME`（未指定時: `videos`）
- `TITLE_LIST_WORKSHEET_NAME`（未指定時: `タイトルリスト`）
- `DAILY_MAX_RESULTS`（未指定時: `50`）

### 差分抽出ルール（更新）

- `TITLE_LIST_WORKSHEET_NAME` シートの **A2以降URL** から動画IDを抽出
- `SPREADSHEET_WORKSHEET_NAME`（索引シート）に既存の動画IDがあれば除外
- 上記2条件を満たす（=どちらにも無い）動画のみ `SPREADSHEET_WORKSHEET_NAME` へ追記

### スプレッドシートの注意

- サービスアカウントの `client_email` を対象スプレッドシートに共有してください（編集者）。
- 初回実行時、指定ワークシートがなければ自動で作成されます。

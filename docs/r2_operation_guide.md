# R2運用ガイド（Talk-index）

このドキュメントは、`シート → JSON → R2` 配信を安全に運用するための手順です。

---

## 1. 非公開バケット作成手順

対象バケット名: `Talk-indexdepo`

1. Cloudflare Dashboard → **R2** → **Create bucket** を開く
2. Bucket name に `Talk-indexdepo` を入力して作成
3. **Public access は有効化しない（非公開のまま）**
4. R2 API Tokens で、次の最小権限トークンを作成
   - 権限: Object Read / Object Write（対象バケットのみ）

> 理由: 公開制御を Worker 側に集約し、誤公開リスクを減らすため。

---

## 2. Worker経由公開手順

### 2-1. Worker を作成

1. Cloudflare Dashboard → **Workers & Pages** → **Create**
2. HTTP Worker を作成し、`/index/latest.json` を返す用途にする

### 2-2. Worker の環境変数（Secrets）

- `R2_BUCKET_NAME=Talk-indexdepo`
- `R2_OBJECT_KEY=index/latest.json`

### 2-3. R2 バインディング

Worker に R2 binding を追加:

- Binding name: `INDEX_BUCKET`
- Bucket: `Talk-indexdepo`

### 2-4. Worker サンプル（最小）

```js
export default {
  async fetch(request, env) {
    const object = await env.INDEX_BUCKET.get(env.R2_OBJECT_KEY || "index/latest.json");
    if (!object) {
      return new Response(JSON.stringify({ error: "not found" }), {
        status: 404,
        headers: { "content-type": "application/json; charset=utf-8" },
      });
    }

    const headers = new Headers();
    object.writeHttpMetadata(headers);
    headers.set("content-type", "application/json; charset=utf-8");
    headers.set("cache-control", "public, max-age=60");

    return new Response(object.body, { status: 200, headers });
  },
};
```

---

## 3. Cache-Control / CORS 推奨値

### Cache-Control（推奨）

- 初期値: `public, max-age=60`
- 更新頻度が低い場合: `max-age=300` まで延長可

### CORS（ブラウザから別ドメイン取得する場合のみ）

- Allow-Origin: フロント配信ドメインのみ
- Allow-Methods: `GET, HEAD`
- Allow-Headers: 必要最小限（通常は空で可）

> 理由: まずは短めキャッシュで鮮度優先。CORSは最小許可で安全性を保つ。

---

## 4. ロールバック手順（前回JSON再配置）

### 前提

アップロード時に、最新だけでなくバックアップを保存しておく。

- `index/latest.json`（本番参照）
- `index/backup/YYYYMMDD-HHMMSS.json`（履歴）

### ロールバック手順

1. 障害発生時、最後に正常だったバックアップを選ぶ
2. そのファイル内容を `index/latest.json` に上書き配置
3. Worker URL で JSON 取得できることを確認
4. 失敗原因を修正後、次回定期処理を再開

---

## 5. 完了条件（初回構築者チェックリスト）

- [ ] `Talk-indexdepo` が非公開で作成されている
- [ ] GitHub Secrets に R2認証情報が設定されている
- [ ] Action 実行で `index/latest.json` が更新される
- [ ] Worker URL で JSON が 200 で返る
- [ ] `cache-control` が意図どおり付いている
- [ ] ロールバックを1回試し、復旧できる


from __future__ import annotations

import json
import os

import boto3

from crawler.services.spreadsheet import build_gspread_client


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} が未設定です。GitHub Secrets を確認してください。")
    return value


def main() -> None:
    spreadsheet_id = _require_env("SPREADSHEET_ID")
    worksheet_name = os.getenv("SPREADSHEET_WORKSHEET_NAME", "索引").strip() or "索引"
    service_account_json = _require_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    r2_account_id = _require_env("R2_ACCOUNT_ID")
    r2_access_key_id = _require_env("R2_ACCESS_KEY_ID")
    r2_secret_access_key = _require_env("R2_SECRET_ACCESS_KEY")
    r2_bucket_name = _require_env("R2_BUCKET_NAME")

    endpoint_url = f"https://{r2_account_id}.r2.cloudflarestorage.com"

    try:
        gspread_client = build_gspread_client(service_account_json)
        sheet = gspread_client.open_by_key(spreadsheet_id).worksheet(worksheet_name)
        values = sheet.get_all_values()
    except Exception as exc:
        raise RuntimeError("Googleスプレッドシートの読み取りに失敗しました。設定値と権限を確認してください。") from exc

    if not values:
        rows: list[dict[str, str]] = []
    else:
        headers = [h.strip() for h in values[0]]
        rows = []
        for raw in values[1:]:
            if not any((cell or "").strip() for cell in raw):
                continue
            row: dict[str, str] = {}
            for i, header in enumerate(headers):
                if not header:
                    continue
                row[header] = raw[i] if i < len(raw) else ""
            rows.append(row)

    payload = json.dumps(rows, ensure_ascii=False, indent=2)

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=r2_access_key_id,
            aws_secret_access_key=r2_secret_access_key,
            region_name="auto",
        )
        s3.put_object(
            Bucket=r2_bucket_name,
            Key="index/latest.json",
            Body=payload.encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
    except Exception as exc:
        raise RuntimeError("R2 へのアップロードに失敗しました。認証情報・バケット名・権限を確認してください。") from exc

    print(
        "done: "
        f"worksheet={worksheet_name}, "
        f"rows={len(rows)}, "
        f"bucket={r2_bucket_name}, "
        "key=index/latest.json"
    )


if __name__ == "__main__":
    main()

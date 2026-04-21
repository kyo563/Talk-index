from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import boto3

from crawler.services.favorites_mirror import (
    FAVORITES_CURRENT_RANKING_SHEET,
    FAVORITES_DAILY_SNAPSHOTS_SHEET,
    FAVORITES_HALL_OF_FAME_SHEET,
    FAVORITES_RECENT_RECOMMENDATIONS_SHEET,
    build_heading_video_title_map,
    build_sheet_rows_from_items,
    previous_week_key_jst,
    replace_sheet_rows,
    upsert_daily_snapshot_rows,
)
from crawler.services.spreadsheet import build_gspread_client, normalize_spreadsheet_id

CURRENT_RANKING_KEY = "favorites/exports/current_ranking.json"
HALL_OF_FAME_KEY = "favorites/aggregates/hall_of_fame.json"
RECENT_RECOMMENDATIONS_KEY = "favorites/aggregates/recent_recommendations.json"
DAILY_SNAPSHOT_PREFIX = "favorites/exports/daily_snapshot/"
DAILY_SNAPSHOT_LATEST_KEY = f"{DAILY_SNAPSHOT_PREFIX}latest.json"
TALKS_JSON_KEY = "index/talks.json"


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} が未設定です。GitHub Secrets を確認してください。")
    return value


def _load_json_required(s3, bucket: str, key: str) -> dict[str, object]:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:
        raise RuntimeError(f"必須JSONの取得に失敗しました: {key}") from exc
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"JSONの形式が不正です: {key}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"JSONオブジェクトではありません: {key}")
    return payload


def _load_json_optional(s3, bucket: str, key: str) -> dict[str, object] | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception:
        print(f"warning: 取得できませんでした key={key}")
        return None

    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        print(f"warning: JSON形式が不正なためスキップ key={key}")
        return None
    if not isinstance(payload, dict):
        print(f"warning: JSONオブジェクトではないためスキップ key={key}")
        return None
    return payload


def _load_daily_snapshots(s3, bucket: str) -> list[tuple[str, dict[str, object]]]:
    snapshots: list[tuple[str, dict[str, object]]] = []

    latest_payload = _load_json_optional(s3, bucket, DAILY_SNAPSHOT_LATEST_KEY)
    if latest_payload:
        snapshots.append((DAILY_SNAPSHOT_LATEST_KEY, latest_payload))
    else:
        print(f"warning: latest snapshot が見つかりません key={DAILY_SNAPSHOT_LATEST_KEY}")

    token: str | None = None
    listed_keys: list[str] = []
    while True:
        kwargs = {"Bucket": bucket, "Prefix": DAILY_SNAPSHOT_PREFIX, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        response = s3.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith(".json"):
                continue
            if key == DAILY_SNAPSHOT_LATEST_KEY:
                continue
            listed_keys.append(key)

        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")

    for key in sorted(set(listed_keys)):
        payload = _load_json_optional(s3, bucket, key)
        if payload:
            snapshots.append((key, payload))

    deduped: dict[str, tuple[str, dict[str, object]]] = {}
    for key, payload in snapshots:
        snapshot_date = str(payload.get("snapshotDate") or "").strip()
        if not snapshot_date:
            print(f"warning: snapshotDate が無いためスキップ key={key}")
            continue
        if snapshot_date not in deduped:
            deduped[snapshot_date] = (key, payload)
            continue
        current_is_latest = key.endswith("/latest.json")
        previous_is_latest = deduped[snapshot_date][0].endswith("/latest.json")
        if not previous_is_latest and current_is_latest:
            deduped[snapshot_date] = (key, payload)

    return [deduped[date] for date in sorted(deduped.keys())]


def main() -> None:
    account_id = _require_env("R2_ACCOUNT_ID")
    access_key_id = _require_env("R2_ACCESS_KEY_ID")
    secret_access_key = _require_env("R2_SECRET_ACCESS_KEY")
    bucket = _require_env("R2_BUCKET_NAME")

    spreadsheet_id = normalize_spreadsheet_id(_require_env("SPREADSHEET_ID"))
    service_account_json = _require_env("GOOGLE_SERVICE_ACCOUNT_JSON")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )
    gspread_client = build_gspread_client(service_account_json)

    current_payload = _load_json_required(s3, bucket, CURRENT_RANKING_KEY)
    hall_payload = _load_json_required(s3, bucket, HALL_OF_FAME_KEY)
    recent_payload = _load_json_required(s3, bucket, RECENT_RECOMMENDATIONS_KEY)
    talks_payload = _load_json_optional(s3, bucket, TALKS_JSON_KEY) or {}

    heading_title_map = build_heading_video_title_map(talks_payload)

    current_rows = build_sheet_rows_from_items(
        payload=current_payload,
        aggregate_type="current_ranking",
        source_json_url=CURRENT_RANKING_KEY,
        heading_title_map=heading_title_map,
    )
    hall_rows = build_sheet_rows_from_items(
        payload=hall_payload,
        aggregate_type="hall_of_fame",
        source_json_url=HALL_OF_FAME_KEY,
        heading_title_map=heading_title_map,
    )

    recent_default_week = previous_week_key_jst(datetime.now(UTC))
    recent_rows = build_sheet_rows_from_items(
        payload=recent_payload,
        aggregate_type="recent_recommendations",
        source_json_url=RECENT_RECOMMENDATIONS_KEY,
        heading_title_map=heading_title_map,
        default_week_key=recent_default_week,
    )

    replace_sheet_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=FAVORITES_CURRENT_RANKING_SHEET,
        rows=current_rows,
    )
    replace_sheet_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=FAVORITES_HALL_OF_FAME_SHEET,
        rows=hall_rows,
    )
    replace_sheet_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=FAVORITES_RECENT_RECOMMENDATIONS_SHEET,
        rows=recent_rows,
    )

    daily_payloads = _load_daily_snapshots(s3, bucket)
    daily_rows: list[list[str]] = []
    for key, payload in daily_payloads:
        snapshot_date = str(payload.get("snapshotDate") or "").strip()
        if not snapshot_date:
            print(f"warning: snapshotDate 不足 key={key}")
            continue
        rows = build_sheet_rows_from_items(
            payload=payload,
            aggregate_type="daily_snapshot",
            source_json_url=key,
            heading_title_map=heading_title_map,
            default_snapshot_date=snapshot_date,
        )
        daily_rows.extend(rows)

    updated_count, appended_count = upsert_daily_snapshot_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=FAVORITES_DAILY_SNAPSHOTS_SHEET,
        rows=daily_rows,
    )

    print(
        "favorites mirror done: "
        f"current={len(current_rows)}, hall={len(hall_rows)}, recent={len(recent_rows)}, "
        f"daily_rows={len(daily_rows)}, daily_updated={updated_count}, daily_appended={appended_count}"
    )


if __name__ == "__main__":
    main()

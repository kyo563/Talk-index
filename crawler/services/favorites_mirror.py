from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import boto3

from crawler.services.spreadsheet import replace_sheet_records, upsert_sheet_records

FAVORITES_EXPORT_PREFIX = "favorites/exports"
FAVORITES_AGGREGATE_KEYS = {
    "current_ranking": f"{FAVORITES_EXPORT_PREFIX}/current_ranking.json",
    "hall_of_fame": "favorites/aggregates/hall_of_fame.json",
    "recent_recommendations": "favorites/aggregates/recent_recommendations.json",
}
FAVORITES_DAILY_SNAPSHOT_PREFIX = f"{FAVORITES_EXPORT_PREFIX}/daily_snapshot/"
FAVORITES_DAILY_SNAPSHOT_LATEST_KEY = f"{FAVORITES_DAILY_SNAPSHOT_PREFIX}latest.json"
INDEX_TALKS_KEY = "index/talks.json"

CURRENT_RANKING_SHEET = "favorites_current_ranking"
HALL_OF_FAME_SHEET = "favorites_hall_of_fame"
RECENT_RECOMMENDATIONS_SHEET = "favorites_recent_recommendations"
DAILY_SNAPSHOTS_SHEET = "favorites_daily_snapshots"

FAVORITES_MIRROR_COLUMNS = [
    "snapshotDate",
    "weekKey",
    "headingId",
    "headingText",
    "videoId",
    "sourceVideoTitle",
    "voteCount",
    "rank",
    "firstVotedAt",
    "lastVotedAt",
    "aggregateType",
    "generatedAt",
    "sourceJsonUrl",
    "note",
]

DAILY_SNAPSHOT_KEY_COLUMNS = ["snapshotDate", "headingId"]
JST = timezone(timedelta(hours=9))


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} が未設定です。GitHub Secrets または環境変数を確認してください。")
    return value


def build_r2_client() -> tuple[Any, str]:
    account_id = require_env("R2_ACCOUNT_ID")
    access_key_id = require_env("R2_ACCESS_KEY_ID")
    secret_access_key = require_env("R2_SECRET_ACCESS_KEY")
    bucket = require_env("R2_BUCKET_NAME")
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )
    return s3, bucket


def fetch_json_object(s3: Any, bucket: str, key: str) -> dict[str, Any]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{key} のJSONトップレベルが object ではありません。")
    return payload


def list_daily_snapshot_keys(s3: Any, bucket: str) -> list[str]:
    keys: list[str] = []
    token: str | None = None

    while True:
        kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": FAVORITES_DAILY_SNAPSHOT_PREFIX,
            "MaxKeys": 1000,
        }
        if token:
            kwargs["ContinuationToken"] = token
        response = s3.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith(".json"):
                continue
            keys.append(key)
        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")

    return sorted(set(keys))


def load_required_aggregate_payloads(
    fetcher: Callable[[str], dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for aggregate_type, key in FAVORITES_AGGREGATE_KEYS.items():
        payloads[aggregate_type] = fetcher(key)
    return payloads


def load_daily_snapshot_payloads(
    list_keys: Callable[[], list[str]],
    fetcher: Callable[[str], dict[str, Any]],
    *,
    logger: Callable[[str], None] = print,
) -> list[tuple[str, dict[str, Any]]]:
    try:
        keys = list_keys()
    except Exception as exc:
        logger(f"[favorites mirror] daily snapshot key list failed: {exc}")
        keys = []

    dated_keys = [
        key
        for key in keys
        if key.startswith(FAVORITES_DAILY_SNAPSHOT_PREFIX)
        and key.endswith(".json")
        and not key.endswith("/latest.json")
    ]

    payloads: list[tuple[str, dict[str, Any]]] = []

    if dated_keys:
        for key in dated_keys:
            try:
                payloads.append((key, fetcher(key)))
            except Exception as exc:
                logger(f"[favorites mirror] missing or unreadable daily snapshot: {key} ({exc})")
    else:
        logger("[favorites mirror] no dated daily snapshots were listed. latest.json fallback will be attempted.")

    if not payloads:
        try:
            payloads.append((FAVORITES_DAILY_SNAPSHOT_LATEST_KEY, fetcher(FAVORITES_DAILY_SNAPSHOT_LATEST_KEY)))
            logger("[favorites mirror] using daily snapshot latest.json fallback")
        except Exception as exc:
            logger(f"[favorites mirror] latest daily snapshot fallback failed: {exc}")
            raise RuntimeError("daily_snapshot を1件も取得できませんでした。") from exc

    deduped: dict[str, tuple[str, dict[str, Any]]] = {}
    for key, payload in payloads:
        snapshot_date = str(payload.get("snapshotDate") or "").strip()
        if not snapshot_date:
            logger(f"[favorites mirror] snapshotDate missing: {key}")
            continue
        previous = deduped.get(snapshot_date)
        if previous is None or previous[0].endswith("latest.json"):
            deduped[snapshot_date] = (key, payload)

    return [deduped[snapshot_date] for snapshot_date in sorted(deduped)]


def load_talks_payload(
    fetcher: Callable[[str], dict[str, Any]],
    *,
    logger: Callable[[str], None] = print,
) -> dict[str, Any] | None:
    try:
        return fetcher(INDEX_TALKS_KEY)
    except Exception as exc:
        logger(f"[favorites mirror] talks.json lookup skipped: {exc}")
        return None


def build_public_json_url(r2_key: str, public_base_url: str | None) -> str:
    base = (public_base_url or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/{r2_key}"


def _extract_unique_video_titles(talk: dict[str, Any]) -> list[str]:
    titles: list[str] = []
    for subsection in talk.get("subsections", []) or []:
        if not isinstance(subsection, dict):
            continue
        title = str(subsection.get("videoTitle") or "").strip()
        if title and title not in titles:
            titles.append(title)
    return titles


def resolve_source_video_title(
    talks_payload: dict[str, Any] | None,
    heading_id: str,
    *,
    fallback_video_title: str = "",
) -> tuple[str, str]:
    """
    headingId は現行実装では大見出しテキスト相当で、動画を跨いで重複し得ます。
    そのため talks.json から複数動画が見つかった場合は、単一カラムに収める都合上、
    talks.json 内の先頭（= 既存エクスポート順で最も優先されるもの）の videoTitle を代表値として返します。
    talks.json で引けない場合のみ、集計JSONに残っている videoTitle を補助値として使い、
    note に fallback であることを明示します。
    """
    normalized_heading_id = str(heading_id or "").strip()
    if talks_payload and normalized_heading_id:
        talks = talks_payload.get("talks", []) or []
        for talk in talks:
            if not isinstance(talk, dict):
                continue
            candidates = {str(talk.get("key") or "").strip(), str(talk.get("name") or "").strip()}
            if normalized_heading_id not in candidates:
                continue
            titles = _extract_unique_video_titles(talk)
            if not titles:
                break
            if len(titles) == 1:
                return titles[0], ""
            return titles[0], "talks.json 上で複数の元動画が見つかったため、先頭の動画タイトルを表示"

    fallback = str(fallback_video_title or "").strip()
    if fallback:
        return fallback, "talks.json から引けなかったため aggregate JSON の videoTitle を補助表示"
    return "", "sourceVideoTitle を talks.json から復元できませんでした"


def build_sheet_row(
    item: dict[str, Any],
    *,
    aggregate_type: str,
    rank: int,
    generated_at: str,
    source_json_url: str,
    week_key: str = "",
    snapshot_date: str = "",
    talks_payload: dict[str, Any] | None = None,
) -> dict[str, str]:
    heading_id = str(item.get("headingId") or "").strip()
    aggregate_video_title = str(item.get("videoTitle") or "").strip()
    source_video_title, note = resolve_source_video_title(
        talks_payload,
        heading_id,
        fallback_video_title=aggregate_video_title,
    )
    return {
        "snapshotDate": snapshot_date,
        "weekKey": week_key,
        "headingId": heading_id,
        "headingText": str(item.get("headingTitle") or heading_id).strip(),
        "videoId": str(item.get("videoId") or "").strip(),
        "sourceVideoTitle": source_video_title,
        "voteCount": str(item.get("voteCount") or 0),
        "rank": str(rank),
        "firstVotedAt": str(item.get("firstVotedAt") or "").strip(),
        "lastVotedAt": str(item.get("lastVotedAt") or "").strip(),
        "aggregateType": aggregate_type,
        "generatedAt": generated_at,
        "sourceJsonUrl": source_json_url,
        "note": note,
    }


def build_sheet_rows_from_payload(
    payload: dict[str, Any],
    *,
    aggregate_type: str,
    source_json_url: str,
    talks_payload: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    generated_at = str(payload.get("generatedAt") or "").strip()
    week_key = str(payload.get("weekKey") or "").strip()
    snapshot_date = str(payload.get("snapshotDate") or "").strip()
    rows: list[dict[str, str]] = []
    for rank, item in enumerate(payload.get("items", []) or [], start=1):
        if not isinstance(item, dict):
            continue
        rows.append(
            build_sheet_row(
                item,
                aggregate_type=aggregate_type,
                rank=rank,
                generated_at=generated_at,
                source_json_url=source_json_url,
                week_key=week_key,
                snapshot_date=snapshot_date,
                talks_payload=talks_payload,
            )
        )
    return rows


def build_daily_snapshot_upsert_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("snapshotDate") or "").strip(),
        str(row.get("headingId") or "").strip(),
    )


def compute_previous_week_key_jst(reference_utc: datetime) -> str:
    jst_now = reference_utc.astimezone(JST)
    current_week_monday = jst_now.date() - timedelta(days=jst_now.weekday())
    previous_week_monday = current_week_monday - timedelta(days=7)
    return previous_week_monday.isoformat()


def sync_favorites_exports_to_spreadsheet(
    *,
    gc: Any,
    spreadsheet_id: str,
    s3: Any,
    bucket: str,
    public_base_url: str | None = None,
    logger: Callable[[str], None] = print,
) -> dict[str, Any]:
    fetcher = lambda key: fetch_json_object(s3, bucket, key)
    talks_payload = load_talks_payload(fetcher, logger=logger)
    aggregate_payloads = load_required_aggregate_payloads(fetcher)
    daily_payloads = load_daily_snapshot_payloads(
        lambda: list_daily_snapshot_keys(s3, bucket),
        fetcher,
        logger=logger,
    )

    current_rows = build_sheet_rows_from_payload(
        aggregate_payloads["current_ranking"],
        aggregate_type="current_ranking",
        source_json_url=build_public_json_url(FAVORITES_AGGREGATE_KEYS["current_ranking"], public_base_url),
        talks_payload=talks_payload,
    )
    hall_rows = build_sheet_rows_from_payload(
        aggregate_payloads["hall_of_fame"],
        aggregate_type="hall_of_fame",
        source_json_url=build_public_json_url(FAVORITES_AGGREGATE_KEYS["hall_of_fame"], public_base_url),
        talks_payload=talks_payload,
    )
    recent_payload = aggregate_payloads["recent_recommendations"]
    recent_rows = build_sheet_rows_from_payload(
        recent_payload,
        aggregate_type="recent_recommendations",
        source_json_url=build_public_json_url(FAVORITES_AGGREGATE_KEYS["recent_recommendations"], public_base_url),
        talks_payload=talks_payload,
    )

    expected_previous_week_key = compute_previous_week_key_jst(datetime.now(UTC))
    actual_week_key = str(recent_payload.get("weekKey") or "").strip()
    if actual_week_key and actual_week_key != expected_previous_week_key:
        logger(
            "[favorites mirror] warning: recent_recommendations weekKey "
            f"({actual_week_key}) does not match computed previous week ({expected_previous_week_key})"
        )

    daily_rows: list[dict[str, str]] = []
    for key, payload in daily_payloads:
        daily_rows.extend(
            build_sheet_rows_from_payload(
                payload,
                aggregate_type="daily_snapshot",
                source_json_url=build_public_json_url(key, public_base_url),
                talks_payload=talks_payload,
            )
        )

    current_result = replace_sheet_records(
        gc,
        spreadsheet_id,
        CURRENT_RANKING_SHEET,
        FAVORITES_MIRROR_COLUMNS,
        current_rows,
    )
    hall_result = replace_sheet_records(
        gc,
        spreadsheet_id,
        HALL_OF_FAME_SHEET,
        FAVORITES_MIRROR_COLUMNS,
        hall_rows,
    )
    recent_result = replace_sheet_records(
        gc,
        spreadsheet_id,
        RECENT_RECOMMENDATIONS_SHEET,
        FAVORITES_MIRROR_COLUMNS,
        recent_rows,
    )
    daily_result = upsert_sheet_records(
        gc,
        spreadsheet_id,
        DAILY_SNAPSHOTS_SHEET,
        FAVORITES_MIRROR_COLUMNS,
        daily_rows,
        key_columns=DAILY_SNAPSHOT_KEY_COLUMNS,
    )

    logger(
        "[favorites mirror] synced "
        f"current={current_result['written']} "
        f"hall={hall_result['written']} "
        f"recent={recent_result['written']} "
        f"daily_appended={daily_result['appended']} "
        f"daily_updated={daily_result['updated']}"
    )

    return {
        "current_ranking": current_result,
        "hall_of_fame": hall_result,
        "recent_recommendations": recent_result,
        "daily_snapshots": daily_result,
        "daily_snapshot_sources": [key for key, _ in daily_payloads],
    }

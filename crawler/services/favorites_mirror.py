from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import gspread

from crawler.services.spreadsheet import _get_or_create_sheet, extract_video_id_from_url

FAVORITES_CURRENT_RANKING_SHEET = "favorites_current_ranking"
FAVORITES_HALL_OF_FAME_SHEET = "favorites_hall_of_fame"
FAVORITES_RECENT_RECOMMENDATIONS_SHEET = "favorites_recent_recommendations"
FAVORITES_DAILY_SNAPSHOTS_SHEET = "favorites_daily_snapshots"

FAVORITES_SHEET_HEADERS = [
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

HeadingVideoTitleMap = dict[str, dict[Any, Any]]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any) -> int:
    raw = _text(value)
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def _column_name(index: int) -> str:
    if index < 1:
        raise ValueError("index must be >= 1")
    out = ""
    n = index
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(ord("A") + r) + out
    return out


def previous_week_key_jst(now_utc: datetime) -> str:
    jst_now = now_utc.astimezone(UTC) + timedelta(hours=9)
    current_week_monday = jst_now - timedelta(days=jst_now.weekday())
    previous_week_monday = current_week_monday - timedelta(days=7)
    return previous_week_monday.strftime("%Y-%m-%d")


def build_heading_video_title_map(talks_payload: dict[str, Any]) -> HeadingVideoTitleMap:
    out: HeadingVideoTitleMap = {"by_heading_and_video": {}, "by_heading_candidates": {}}
    by_heading_and_video: dict[tuple[str, str], str] = out["by_heading_and_video"]
    by_heading_candidates: dict[str, set[str]] = out["by_heading_candidates"]
    talks = talks_payload.get("talks") if isinstance(talks_payload, dict) else []
    if not isinstance(talks, list):
        return out

    for talk in talks:
        if not isinstance(talk, dict):
            continue
        key = _text(talk.get("key"))
        name = _text(talk.get("name"))
        subsections = talk.get("subsections") if isinstance(talk.get("subsections"), list) else []
        for subsection in subsections:
            if not isinstance(subsection, dict):
                continue
            title = _text(subsection.get("videoTitle"))
            if not title:
                continue
            subsection_video_id = extract_video_id_from_url(_text(subsection.get("videoUrl")))
            for heading in (key, name):
                if not heading:
                    continue
                candidates = by_heading_candidates.setdefault(heading, set())
                candidates.add(title)
                if subsection_video_id:
                    by_heading_and_video[(heading, subsection_video_id)] = title
    return out


def _resolve_source_video_title(item: dict[str, Any], heading_title_map: HeadingVideoTitleMap) -> str:
    direct = _text(item.get("sourceVideoTitle")) or _text(item.get("videoTitle"))
    if direct:
        return direct

    by_heading_and_video: dict[tuple[str, str], str] = heading_title_map.get("by_heading_and_video", {})
    by_heading_candidates: dict[str, set[str]] = heading_title_map.get("by_heading_candidates", {})

    heading_id = _text(item.get("headingId"))
    heading_title = _text(item.get("headingTitle"))
    video_id = _text(item.get("videoId"))

    if video_id:
        if heading_id:
            matched = by_heading_and_video.get((heading_id, video_id))
            if matched:
                return matched
        if heading_title:
            matched = by_heading_and_video.get((heading_title, video_id))
            if matched:
                return matched

    for heading in (heading_id, heading_title):
        if not heading:
            continue
        candidates = by_heading_candidates.get(heading, set())
        if len(candidates) == 1:
            return next(iter(candidates))

    # heading または videoId だけでは候補が特定できない曖昧ケースは誤復元を避けて空欄にする。
    return ""


def build_sheet_rows_from_items(
    *,
    payload: dict[str, Any],
    aggregate_type: str,
    source_json_url: str,
    heading_title_map: HeadingVideoTitleMap,
    default_snapshot_date: str = "",
    default_week_key: str = "",
) -> list[list[str]]:
    generated_at = _text(payload.get("generatedAt"))
    snapshot_date = _text(payload.get("snapshotDate")) or default_snapshot_date
    week_key = _text(payload.get("weekKey")) or default_week_key

    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    rows: list[list[str]] = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        row = [
            snapshot_date,
            week_key,
            _text(item.get("headingId")),
            _text(item.get("headingTitle")) or _text(item.get("headingId")),
            _text(item.get("videoId")),
            _resolve_source_video_title(item, heading_title_map),
            str(_to_int(item.get("voteCount"))),
            str(index),
            _text(item.get("firstVotedAt")),
            _text(item.get("lastVotedAt")),
            aggregate_type,
            generated_at,
            source_json_url,
            "",
        ]
        rows.append(row)
    return rows


def replace_sheet_rows(
    *,
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    rows: list[list[str]],
) -> None:
    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    _ensure_favorites_sheet_columns(sheet)
    sheet.clear()
    matrix = [FAVORITES_SHEET_HEADERS, *rows]
    sheet.update("A1", matrix, value_input_option="RAW")


def upsert_daily_snapshot_rows(
    *,
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    rows: list[list[str]],
) -> tuple[int, int]:
    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    _ensure_favorites_sheet_columns(sheet)

    values = sheet.get_all_values()
    if not values:
        sheet.update("A1", [FAVORITES_SHEET_HEADERS], value_input_option="RAW")
        values = [FAVORITES_SHEET_HEADERS]

    header = values[0]
    if header != FAVORITES_SHEET_HEADERS:
        sheet.clear()
        sheet.update("A1", [FAVORITES_SHEET_HEADERS], value_input_option="RAW")
        values = [FAVORITES_SHEET_HEADERS]

    key_index: dict[tuple[str, str], int] = {}
    for row_no, row in enumerate(values[1:], start=2):
        snapshot = _text(row[0] if len(row) > 0 else "")
        heading_id = _text(row[2] if len(row) > 2 else "")
        if snapshot and heading_id:
            key_index[(snapshot, heading_id)] = row_no

    updates: list[dict[str, Any]] = []
    appends: list[list[str]] = []

    for row in rows:
        snapshot = _text(row[0] if len(row) > 0 else "")
        heading_id = _text(row[2] if len(row) > 2 else "")
        if not snapshot or not heading_id:
            continue
        matched_row = key_index.get((snapshot, heading_id))
        if matched_row:
            col = _column_name(len(FAVORITES_SHEET_HEADERS))
            updates.append({"range": f"A{matched_row}:{col}{matched_row}", "values": [row]})
        else:
            appends.append(row)

    if updates:
        sheet.batch_update(updates, value_input_option="RAW")
    if appends:
        sheet.append_rows(appends, value_input_option="RAW")

    return len(updates), len(appends)


def _ensure_favorites_sheet_columns(sheet: gspread.Worksheet) -> None:
    required_cols = len(FAVORITES_SHEET_HEADERS)
    current_cols = getattr(sheet, "col_count", 0) or 0
    if current_cols >= required_cols:
        return
    sheet.add_cols(required_cols - current_cols)

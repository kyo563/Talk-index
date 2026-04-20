from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import parse_qs, urlparse

import gspread

from crawler.models import VideoItem
from crawler.services.timestamps import build_timestamp_rows


class SpreadsheetServiceError(RuntimeError):
    pass


TIMESTAMP_WITH_LABEL_PATTERN = re.compile(
    r"(?P<ts>(?:\d{1,2}:)?\d{1,2}:\d{2})\s*(?P<label>[^\n\r]*)"
)
TIMESTAMP_TOKEN_PATTERN = re.compile(r"\b(?:\d{1,2}:)?\d{1,2}:\d{2}\b")
MARKER_TOKEN_PATTERN = re.compile(r"[└┝├]")
MAJOR_LINE_PATTERN = re.compile(r"^\s*(?P<ts>\d{2}:\d{2}:\d{2})\s*(?P<label>.*)$")
MINOR_LINE_PATTERN = re.compile(
    r"^\s*(?P<marker>[┝└├])\s*(?P<ts>\d{1,2}:\d{2}:\d{2})\s*(?P<label>.*)$"
)
SPREADSHEET_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9-_]{20,}$")

TITLE_LIST_STATE_RANGE = "F1:G3"
TITLE_LIST_STATE_HEADER = ["key", "value"]
TITLE_LIST_STATE_REFRESH_CURSOR_KEY = "refresh_cursor"
TITLE_LIST_STATE_UPDATED_AT_KEY = "updated_at"
TITLE_LIST_HEADER = ["タイトル", "日付", "URL"]


def build_gspread_client(service_account_json: str) -> gspread.Client:
    if not service_account_json.strip():
        raise SpreadsheetServiceError("GOOGLE_SERVICE_ACCOUNT_JSON が未設定です。")

    try:
        account_info = json.loads(service_account_json)
    except json.JSONDecodeError as exc:
        raise SpreadsheetServiceError("GOOGLE_SERVICE_ACCOUNT_JSON のJSON形式が不正です。") from exc

    return gspread.service_account_from_dict(account_info)


def extract_video_id_from_url(url: str) -> str:
    value = (url or "").strip()
    if not value:
        return ""

    if re.fullmatch(r"[A-Za-z0-9_-]{11}", value):
        return value

    if "youtu.be/" in value:
        path = urlparse(value).path.strip("/")
        return path.split("/")[0] if path else ""

    parsed = urlparse(value)
    query_video_id = parse_qs(parsed.query).get("v", [""])[0]
    if query_video_id:
        return query_video_id

    path_parts = [p for p in parsed.path.split("/") if p]
    if len(path_parts) >= 2 and path_parts[0] in {"shorts", "live"}:
        return path_parts[1]

    return ""


def normalize_spreadsheet_id(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""

    if SPREADSHEET_KEY_PATTERN.fullmatch(value):
        return value

    parsed = urlparse(value)
    if parsed.netloc not in {"docs.google.com", "drive.google.com"}:
        return value

    path_parts = [p for p in parsed.path.split("/") if p]
    if len(path_parts) >= 3 and path_parts[0] == "spreadsheets" and path_parts[1] == "d":
        spreadsheet_id = path_parts[2]
        if SPREADSHEET_KEY_PATTERN.fullmatch(spreadsheet_id):
            return spreadsheet_id

    return value


def _normalize_header_name(value: str) -> str:
    return (value or "").strip().lower().replace(" ", "")


def extract_video_id_from_row(row: list[str], header: list[str] | None = None) -> str:
    if not row:
        return ""

    normalized_header = [_normalize_header_name(h) for h in (header or [])]
    header_index_map: dict[str, list[int]] = {}
    for idx, key in enumerate(normalized_header):
        header_index_map.setdefault(key, []).append(idx)

    prioritized_url_keys = ["url", "動画url", "youtube_url", "大見出しurl", "小見出しurl"]
    for key in prioritized_url_keys:
        for idx in header_index_map.get(key, []):
            if len(row) <= idx:
                continue
            video_id = extract_video_id_from_url(row[idx])
            if video_id:
                return video_id

    prioritized_id_keys = ["video_id", "動画固有id", "id"]
    for key in prioritized_id_keys:
        for idx in header_index_map.get(key, []):
            if len(row) <= idx:
                continue
            video_id = extract_video_id_from_url(row[idx])
            if video_id:
                return video_id

    for cell in row:
        video_id = extract_video_id_from_url(cell)
        if video_id:
            return video_id

    return ""


def _get_or_create_sheet(book: gspread.Spreadsheet, worksheet_name: str) -> gspread.Worksheet:
    try:
        return book.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        return book.add_worksheet(title=worksheet_name, rows=1000, cols=12)


def ensure_title_list_state_cells(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
) -> None:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    existing = sheet.get(TITLE_LIST_STATE_RANGE)

    needs_reset = False
    if len(existing) < 3:
        needs_reset = True
    else:
        row1 = existing[0] if len(existing) > 0 else []
        row2 = existing[1] if len(existing) > 1 else []
        row3 = existing[2] if len(existing) > 2 else []
        key1 = (row1[0] if len(row1) > 0 else "").strip()
        value1 = (row1[1] if len(row1) > 1 else "").strip()
        key2 = (row2[0] if len(row2) > 0 else "").strip()
        key3 = (row3[0] if len(row3) > 0 else "").strip()
        if key1 != "key" or value1 != "value" or key2 != TITLE_LIST_STATE_REFRESH_CURSOR_KEY or key3 != TITLE_LIST_STATE_UPDATED_AT_KEY:
            needs_reset = True

    if needs_reset:
        sheet.update(
            TITLE_LIST_STATE_RANGE,
            [
                TITLE_LIST_STATE_HEADER,
                [TITLE_LIST_STATE_REFRESH_CURSOR_KEY, "0"],
                [TITLE_LIST_STATE_UPDATED_AT_KEY, ""],
            ],
            value_input_option="RAW",
        )


def read_title_list_refresh_state(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
) -> dict[str, str | int]:
    ensure_title_list_state_cells(client, spreadsheet_id, worksheet_name)

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    values = sheet.get(TITLE_LIST_STATE_RANGE)

    raw_cursor = "0"
    raw_updated_at = ""
    if len(values) >= 2 and len(values[1]) >= 2:
        raw_cursor = (values[1][1] or "").strip()
    if len(values) >= 3 and len(values[2]) >= 2:
        raw_updated_at = (values[2][1] or "").strip()

    try:
        cursor = int(raw_cursor)
    except ValueError:
        cursor = 0

    if cursor < 0:
        cursor = 0

    return {
        "refresh_cursor": cursor,
        "updated_at": raw_updated_at,
    }


def write_title_list_refresh_state(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    refresh_cursor: int,
    updated_at: str,
) -> None:
    ensure_title_list_state_cells(client, spreadsheet_id, worksheet_name)

    safe_cursor = refresh_cursor if refresh_cursor >= 0 else 0
    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    sheet.update(
        "G2:G3",
        [[str(safe_cursor)], [(updated_at or "").strip()]],
        value_input_option="RAW",
    )


def read_ordered_video_ids_from_title_list(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
) -> list[str]:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)

    values = sheet.col_values(3)
    if len(values) < 2:
        return []

    seen: set[str] = set()
    ordered_ids: list[str] = []
    for raw in values[1:]:
        video_id = extract_video_id_from_url(raw)
        if not video_id:
            continue
        if video_id in seen:
            continue
        seen.add(video_id)
        ordered_ids.append(video_id)

    return ordered_ids


def read_video_ids_from_url_column(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    start_row: int = 2,
    column_index: int = 1,
) -> set[str]:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)

    values = sheet.col_values(column_index)
    if len(values) < start_row:
        return set()

    video_ids: set[str] = set()
    for raw in values[start_row - 1 :]:
        video_id = extract_video_id_from_url(raw)
        if video_id:
            video_ids.add(video_id)

    return video_ids


def read_existing_video_ids(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
) -> set[str]:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)

    values = sheet.get_all_values()
    if len(values) <= 1:
        return set()

    header = values[0]
    rows = values[1:]

    existing_ids: set[str] = set()
    for row in rows:
        video_id = extract_video_id_from_row(row=row, header=header)
        if video_id:
            existing_ids.add(video_id)

    return existing_ids


def read_video_ids_from_sheet_rows(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
) -> set[str]:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    values = sheet.get_all_values()
    if len(values) <= 1:
        return set()

    header = values[0]
    rows = values[1:]

    video_ids: set[str] = set()
    for row in rows:
        video_id = extract_video_id_from_row(row=row, header=header)
        if video_id:
            video_ids.add(video_id)

    return video_ids


def append_videos(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    channel_id: str,
    videos: Iterable[VideoItem],
) -> int:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)

    header = [
        "タイトル",
        "日付",
        "URL",
        "大見出し",
        "大見出しURL",
        "小見出し",
        "小見出しURL",
        "自動検出タグ",
    ]

    if not sheet.get_all_values():
        sheet.append_row(header, value_input_option="RAW")

    rows = build_rows_for_videos(videos)

    if rows:
        sheet.append_rows(rows, value_input_option="RAW")

    return len(rows)


def build_rows_for_videos(videos: Iterable[VideoItem]) -> list[list[str]]:
    rows: list[list[str]] = []
    for video in videos:
        timestamp_rows = build_timestamp_rows(
            video_url=video.url,
            description=video.description,
            timestamp_sources=video.timestamp_sources,
            fallback_text=video.timestamp_comment,
        )
        if not timestamp_rows:
            timestamp_rows = [("", "", "", "")]

        for major, major_url, minor, minor_url in timestamp_rows:
            rows.append(
                [
                    video.title,
                    _to_jst_date(video.published_at),
                    video.url,
                    major,
                    major_url,
                    minor,
                    minor_url,
                    _format_tags(video.tags),
                ]
            )
    return rows


def upsert_videos_by_video_id(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    videos: Iterable[VideoItem],
) -> int:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    target_videos = [video for video in videos if video.video_id.strip()]
    if not target_videos:
        return 0

    target_ids = {video.video_id.strip() for video in target_videos}
    replacement_rows = build_rows_for_videos(target_videos)

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)
    values = sheet.get_all_values()

    header = [
        "タイトル",
        "日付",
        "URL",
        "大見出し",
        "大見出しURL",
        "小見出し",
        "小見出しURL",
        "自動検出タグ",
    ]
    existing_body_rows: list[list[str]] = []

    if values:
        header = values[0] if values[0] else header
        for row in values[1:]:
            video_id = extract_video_id_from_row(row=row, header=header)
            if video_id and video_id in target_ids:
                continue
            existing_body_rows.append(row)

    max_cols = max(len(header), 8)
    final_rows = [_pad_row(header, max_cols)]
    final_rows.extend(_pad_row(row, max_cols) for row in existing_body_rows)
    final_rows.extend(_pad_row(row, max_cols) for row in replacement_rows)

    last_row = len(values)
    if last_row > 1:
        sheet.batch_clear([f"A2:H{last_row}"])
    if final_rows[1:]:
        sheet.update("A2", final_rows[1:], value_input_option="RAW")
    return len(replacement_rows)


def append_title_list_rows(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    videos: Iterable[VideoItem],
) -> int:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)

    existing_header = sheet.get("A1:C1")
    header_values = existing_header[0] if existing_header else []
    normalized_header = [(cell or "").strip() for cell in header_values]
    if normalized_header != TITLE_LIST_HEADER:
        sheet.update("A1:C1", [TITLE_LIST_HEADER], value_input_option="RAW")

    existing_ids = read_video_ids_from_url_column(
        client=client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
        start_row=2,
        column_index=3,
    )

    rows: list[list[str]] = []
    for video in videos:
        if video.video_id in existing_ids:
            continue
        rows.append(
            [
                video.title,
                _to_jst_date(video.published_at),
                video.url,
            ]
        )

    if rows:
        existing_values = sheet.get_all_values()
        next_row = _next_data_row_for_title_list(existing_values)
        sheet.update(f"A{next_row}:C{next_row + len(rows) - 1}", rows, value_input_option="RAW")

    return len(rows)


def upsert_title_list_rows(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    videos: Iterable[VideoItem],
) -> tuple[int, int]:
    if not spreadsheet_id.strip():
        raise SpreadsheetServiceError("SPREADSHEET_ID が未設定です。")

    target_videos = [video for video in videos if video.video_id.strip()]
    if not target_videos:
        return 0, 0

    book = client.open_by_key(spreadsheet_id)
    sheet = _get_or_create_sheet(book, worksheet_name)

    existing_header = sheet.get("A1:C1")
    header_values = existing_header[0] if existing_header else []
    normalized_header = [(cell or "").strip() for cell in header_values]
    if normalized_header != TITLE_LIST_HEADER:
        sheet.update("A1:C1", [TITLE_LIST_HEADER], value_input_option="RAW")

    rows = sheet.get_all_values()
    id_to_row_index: dict[str, int] = {}
    for idx, row in enumerate(rows[1:], start=2):
        video_id = extract_video_id_from_url(row[2] if len(row) >= 3 else "")
        if not video_id:
            continue
        if video_id not in id_to_row_index:
            id_to_row_index[video_id] = idx

    updates: list[dict[str, str | list[list[str]]]] = []
    append_rows: list[list[str]] = []
    updated = 0
    appended = 0
    for video in target_videos:
        row_values = [video.title, _to_jst_date(video.published_at), video.url]
        target_row = id_to_row_index.get(video.video_id)
        if target_row:
            updates.append(
                {
                    "range": f"A{target_row}:C{target_row}",
                    "values": [row_values],
                }
            )
            updated += 1
            continue
        append_rows.append(row_values)
        appended += 1

    if updates:
        sheet.batch_update(updates, value_input_option="RAW")
    if append_rows:
        next_row = _next_data_row_for_title_list(rows)
        sheet.update(
            f"A{next_row}:C{next_row + len(append_rows) - 1}",
            append_rows,
            value_input_option="RAW",
        )

    return updated, appended


def _next_data_row_for_title_list(values: list[list[str]]) -> int:
    last_data_row = 1
    for idx, row in enumerate(values[1:], start=2):
        a_to_c = row[:3]
        if any((cell or "").strip() for cell in a_to_c):
            last_data_row = idx
    return last_data_row + 1


# 互換のため残す（主経路では未使用）
def _extract_timestamp_rows(video_url: str, timestamp_comment: str) -> list[tuple[str, str, str, str]]:
    text = (timestamp_comment or "").strip()
    if not text:
        return []

    rows: list[tuple[str, str, str, str]] = []
    current_major_text = ""
    current_major_url = ""
    has_minor_for_current_major = False

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        major_match = MAJOR_LINE_PATTERN.match(line)
        if major_match:
            if current_major_text and not has_minor_for_current_major:
                rows.append((current_major_text, current_major_url, "", ""))

            major_ts = _normalize_major_timestamp(major_match.group("ts") or "")
            if major_ts:
                label = _clean_heading_text(major_match.group("label") or "")
                current_major_text = label
                current_major_url = _build_timestamp_url(video_url, major_ts)
                has_minor_for_current_major = False
            continue

        minor_match = MINOR_LINE_PATTERN.match(line)
        if minor_match and current_major_text:
            minor_raw_ts = (minor_match.group("ts") or "").strip()
            minor_ts = _normalize_minor_timestamp(minor_raw_ts)
            if minor_ts:
                label = _clean_heading_text(minor_match.group("label") or "")
                minor_text = label
                rows.append(
                    (
                        current_major_text,
                        current_major_url,
                        minor_text,
                        _build_timestamp_url(video_url, minor_raw_ts),
                    )
                )
                has_minor_for_current_major = True
            continue

    if current_major_text and not has_minor_for_current_major:
        rows.append((current_major_text, current_major_url, "", ""))

    if rows:
        return rows

    for match in TIMESTAMP_WITH_LABEL_PATTERN.finditer(text):
        ts = (match.group("ts") or "").strip()
        label = (match.group("label") or "").strip()
        normalized = _normalize_major_timestamp(ts)
        if not normalized:
            continue
        major_text = _clean_heading_text(label)
        rows.append((major_text, _build_timestamp_url(video_url, normalized), "", ""))

    return rows


def _clean_heading_text(text: str) -> str:
    value = (text or "").strip()
    if not value:
        return ""
    value = MARKER_TOKEN_PATTERN.sub(" ", value)
    value = TIMESTAMP_TOKEN_PATTERN.sub(" ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _format_tags(tags: Iterable[str]) -> str:
    normalized: list[str] = []
    for tag in tags:
        value = (tag or "").strip()
        if not value:
            continue
        if not value.startswith("#"):
            value = f"#{value}"
        normalized.append(value)
    return ",".join(normalized)


def _normalize_major_timestamp(timestamp: str) -> str:
    total_seconds = _timestamp_to_seconds(timestamp)
    if total_seconds < 0:
        return ""
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _normalize_minor_timestamp(timestamp: str) -> str:
    total_seconds = _timestamp_to_seconds(timestamp)
    if total_seconds < 0:
        return ""
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours}:{minutes:02d}:{seconds:02d}"


def _build_timestamp_url(video_url: str, timestamp: str) -> str:
    seconds = _timestamp_to_seconds(timestamp)
    if seconds <= 0:
        return video_url
    separator = "&" if "?" in video_url else "?"
    return f"{video_url}{separator}t={seconds}s"


def _timestamp_to_seconds(timestamp: str) -> int:
    raw = timestamp.strip()
    if not raw:
        return -1
    parts = raw.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return -1
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    return -1


def _to_jst_date(published_at: str) -> str:
    value = (published_at or "").strip()
    if not value:
        return ""
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    jst = timezone(timedelta(hours=9))
    return dt.astimezone(jst).date().isoformat()


def _pad_row(row: list[str], size: int) -> list[str]:
    if len(row) >= size:
        return row[:size]
    return row + [""] * (size - len(row))

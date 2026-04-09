from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import parse_qs, urlparse

import gspread

from crawler.models import VideoItem


class SpreadsheetServiceError(RuntimeError):
    pass


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


def _get_or_create_sheet(book: gspread.Spreadsheet, worksheet_name: str) -> gspread.Worksheet:
    try:
        return book.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        return book.add_worksheet(title=worksheet_name, rows=1000, cols=12)


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

    video_id_idx = header.index("video_id") if "video_id" in header else None
    url_idx = header.index("url") if "url" in header else None

    existing_ids: set[str] = set()
    for row in rows:
        video_id = ""
        if video_id_idx is not None and len(row) > video_id_idx:
            video_id = row[video_id_idx].strip()

        if not video_id and url_idx is not None and len(row) > url_idx:
            video_id = extract_video_id_from_url(row[url_idx])

        if video_id:
            existing_ids.add(video_id)

    return existing_ids


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
        "fetched_at_utc",
        "channel_id",
        "video_id",
        "title",
        "url",
        "published_at",
        "thumbnail_url",
        "tags",
    ]

    if not sheet.get_all_values():
        sheet.append_row(header, value_input_option="RAW")

    rows: list[list[str]] = []
    fetched_at_utc = datetime.now(timezone.utc).isoformat()
    for video in videos:
        rows.append(
            [
                fetched_at_utc,
                channel_id,
                video.video_id,
                video.title,
                video.url,
                video.published_at,
                video.thumbnail_url,
                "|".join(video.tags),
            ]
        )

    if rows:
        sheet.append_rows(rows, value_input_option="RAW")

    return len(rows)

from __future__ import annotations

import json
from typing import Iterable

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
    try:
        sheet = book.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        sheet = book.add_worksheet(title=worksheet_name, rows=1000, cols=12)

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
    from datetime import datetime, timezone

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

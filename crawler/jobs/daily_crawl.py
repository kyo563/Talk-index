from __future__ import annotations

import os

from crawler.services.spreadsheet import (
    append_title_list_rows,
    append_videos,
    build_gspread_client,
    normalize_spreadsheet_id,
    read_existing_video_ids,
    read_video_ids_from_url_column,
)
from crawler.services.youtube import build_youtube_client, fetch_channel_videos


def main() -> None:
    youtube_api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    channel_id = os.getenv("YOUTUBE_CHANNEL_ID", "").strip()
    spreadsheet_id = normalize_spreadsheet_id(os.getenv("SPREADSHEET_ID", ""))
    worksheet_name = os.getenv("SPREADSHEET_WORKSHEET_NAME", "索引").strip() or "索引"
    title_list_worksheet = os.getenv("TITLE_LIST_WORKSHEET_NAME", "タイトルリスト").strip() or "タイトルリスト"
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    max_results_env = os.getenv("DAILY_MAX_RESULTS", "").strip()
    if not max_results_env:
        raise RuntimeError("DAILY_MAX_RESULTS が未設定です。GitHub Secrets を確認してください。")

    try:
        max_results = int(max_results_env)
    except ValueError as exc:
        raise RuntimeError("DAILY_MAX_RESULTS は整数で指定してください。") from exc

    if max_results <= 0:
        raise RuntimeError("DAILY_MAX_RESULTS は1以上で指定してください。")

    if not channel_id:
        raise RuntimeError("YOUTUBE_CHANNEL_ID が未設定です。")

    youtube = build_youtube_client(youtube_api_key)
    gspread_client = build_gspread_client(service_account_json)

    title_list_ids = read_video_ids_from_url_column(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
        start_row=2,
        column_index=3,
    )
    existing_ids = read_existing_video_ids(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
    )

    skip_ids = title_list_ids | existing_ids
    videos = fetch_channel_videos(
        youtube,
        channel_id,
        max_results=max_results,
        exclude_video_ids=skip_ids,
    )

    title_list_appended = append_title_list_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
        videos=videos,
    )

    added_count = append_videos(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
        channel_id=channel_id,
        videos=videos,
    )

    print(
        "done: "
        f"channel_id={channel_id}, "
        f"fetched={len(videos)}, "
        f"title_list_appended={title_list_appended}, "
        f"title_list_ids={len(title_list_ids)}, "
        f"existing_ids={len(existing_ids)}, "
        f"target={len(videos)}, "
        f"appended={added_count}"
    )


if __name__ == "__main__":
    main()

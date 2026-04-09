from __future__ import annotations

import os

from crawler.services.spreadsheet import (
    append_videos,
    build_gspread_client,
    read_existing_video_ids,
    read_video_ids_from_url_column,
)
from crawler.services.youtube import build_youtube_client, fetch_channel_videos


def main() -> None:
    youtube_api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    channel_id = os.getenv("YOUTUBE_CHANNEL_ID", "").strip()
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("SPREADSHEET_WORKSHEET_NAME", "videos").strip() or "videos"
    title_list_worksheet = os.getenv("TITLE_LIST_WORKSHEET_NAME", "タイトルリスト").strip() or "タイトルリスト"
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    max_results = int(os.getenv("DAILY_MAX_RESULTS", "50"))

    if not channel_id:
        raise RuntimeError("YOUTUBE_CHANNEL_ID が未設定です。")

    youtube = build_youtube_client(youtube_api_key)
    videos = fetch_channel_videos(youtube, channel_id, max_results=max_results)

    gspread_client = build_gspread_client(service_account_json)

    title_list_ids = read_video_ids_from_url_column(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
        start_row=2,
        column_index=1,
    )
    existing_ids = read_existing_video_ids(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
    )

    filtered_videos = [
        video
        for video in videos
        if video.video_id not in title_list_ids and video.video_id not in existing_ids
    ]

    added_count = append_videos(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
        channel_id=channel_id,
        videos=filtered_videos,
    )

    print(
        "done: "
        f"channel_id={channel_id}, "
        f"fetched={len(videos)}, "
        f"title_list_ids={len(title_list_ids)}, "
        f"existing_ids={len(existing_ids)}, "
        f"target={len(filtered_videos)}, "
        f"appended={added_count}"
    )


if __name__ == "__main__":
    main()

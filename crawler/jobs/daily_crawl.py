from __future__ import annotations

import os

from crawler.services.spreadsheet import append_videos, build_gspread_client
from crawler.services.youtube import build_youtube_client, fetch_channel_videos


def main() -> None:
    youtube_api_key = os.getenv("YOUTUBE_API_KEY", "").strip()
    channel_id = os.getenv("YOUTUBE_CHANNEL_ID", "").strip()
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "").strip()
    worksheet_name = os.getenv("SPREADSHEET_WORKSHEET_NAME", "videos").strip() or "videos"
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    max_results = int(os.getenv("DAILY_MAX_RESULTS", "50"))

    if not channel_id:
        raise RuntimeError("YOUTUBE_CHANNEL_ID が未設定です。")

    youtube = build_youtube_client(youtube_api_key)
    videos = fetch_channel_videos(youtube, channel_id, max_results=max_results)

    gspread_client = build_gspread_client(service_account_json)
    added_count = append_videos(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
        channel_id=channel_id,
        videos=videos,
    )

    print(f"done: channel_id={channel_id}, fetched={len(videos)}, appended={added_count}")


if __name__ == "__main__":
    main()

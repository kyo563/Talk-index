from __future__ import annotations

import os
from datetime import datetime, timezone

import streamlit as st

from crawler.services.spreadsheet import (
    SpreadsheetServiceError,
    append_videos,
    build_gspread_client,
    read_existing_video_ids,
    read_video_ids_from_url_column,
)
from crawler.services.youtube import YouTubeServiceError, build_youtube_client, fetch_channel_videos


st.set_page_config(page_title="talk-indexDB", page_icon="🗂️", layout="wide")
st.title("talk-indexDB")
st.caption("定刻の自動実行と同じ処理を、手動でも実行できます。")

if "manual_logs" not in st.session_state:
    st.session_state.manual_logs = []


def load_settings() -> dict[str, str]:
    return {
        "youtube_api_key": os.getenv("YOUTUBE_API_KEY", "").strip(),
        "channel_id": os.getenv("YOUTUBE_CHANNEL_ID", "").strip(),
        "spreadsheet_id": os.getenv("SPREADSHEET_ID", "").strip(),
        "worksheet_name": os.getenv("SPREADSHEET_WORKSHEET_NAME", "videos").strip() or "videos",
        "title_list_worksheet": os.getenv("TITLE_LIST_WORKSHEET_NAME", "タイトルリスト").strip() or "タイトルリスト",
        "service_account_json": os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip(),
    }


def run_manual_load(request_count: int) -> dict:
    settings = load_settings()

    if not settings["channel_id"]:
        raise RuntimeError("YOUTUBE_CHANNEL_ID が未設定です。")

    fetch_pool_size = max(50, request_count * 5)

    youtube = build_youtube_client(settings["youtube_api_key"])
    videos = fetch_channel_videos(youtube, settings["channel_id"], max_results=fetch_pool_size)

    gspread_client = build_gspread_client(settings["service_account_json"])
    title_list_ids = read_video_ids_from_url_column(
        client=gspread_client,
        spreadsheet_id=settings["spreadsheet_id"],
        worksheet_name=settings["title_list_worksheet"],
        start_row=2,
        column_index=1,
    )
    existing_ids = read_existing_video_ids(
        client=gspread_client,
        spreadsheet_id=settings["spreadsheet_id"],
        worksheet_name=settings["worksheet_name"],
    )

    candidates = [
        video
        for video in videos
        if video.video_id not in title_list_ids and video.video_id not in existing_ids
    ]
    targets = candidates[:request_count]

    appended = append_videos(
        client=gspread_client,
        spreadsheet_id=settings["spreadsheet_id"],
        worksheet_name=settings["worksheet_name"],
        channel_id=settings["channel_id"],
        videos=targets,
    )

    return {
        "requested": request_count,
        "fetched": len(videos),
        "candidates": len(candidates),
        "appended": appended,
        "status": "success" if appended == len(targets) else "partial",
        "rows": [
            {
                "video_id": video.video_id,
                "title": video.title,
                "url": video.url,
                "result": "追加成功" if i < appended else "未追加",
            }
            for i, video in enumerate(targets)
        ],
    }


settings_preview = load_settings()
missing = [
    key
    for key in ["YOUTUBE_CHANNEL_ID", "SPREADSHEET_ID", "GOOGLE_SERVICE_ACCOUNT_JSON"]
    if not os.getenv(key, "").strip()
]
if missing:
    st.warning(f"未設定の環境変数があります: {', '.join(missing)}")

count = st.number_input("1回で読み込む件数", min_value=1, max_value=100, value=1, step=1)
run = st.button("読み込み実行", type="primary", use_container_width=True)

if run:
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        result = run_manual_load(int(count))
        st.session_state.manual_logs.append(
            {
                "at_utc": started_at,
                **result,
                "message": "OK",
            }
        )
        if result["appended"] == 0:
            st.info("追加対象がありませんでした。")
        else:
            st.success(f"{result['appended']}件を追加しました。")

        st.write(
            f"要求: {result['requested']}件 / 候補: {result['candidates']}件 / 取得: {result['fetched']}件"
        )
        if result["rows"]:
            st.dataframe(result["rows"], use_container_width=True)

    except (RuntimeError, SpreadsheetServiceError, YouTubeServiceError) as exc:
        st.session_state.manual_logs.append(
            {
                "at_utc": started_at,
                "requested": int(count),
                "fetched": 0,
                "candidates": 0,
                "appended": 0,
                "status": "failed",
                "message": str(exc),
            }
        )
        st.error(f"失敗: {exc}")
    except Exception as exc:
        st.session_state.manual_logs.append(
            {
                "at_utc": started_at,
                "requested": int(count),
                "fetched": 0,
                "candidates": 0,
                "appended": 0,
                "status": "failed",
                "message": f"予期しないエラー: {exc}",
            }
        )
        st.error(f"予期しないエラー: {exc}")

st.subheader("実行結果")
if st.session_state.manual_logs:
    st.dataframe(list(reversed(st.session_state.manual_logs)), use_container_width=True)
else:
    st.caption("まだ実行されていません。")

with st.expander("現在の設定値（マスク済み）"):
    st.write(
        {
            "YOUTUBE_CHANNEL_ID": settings_preview["channel_id"],
            "SPREADSHEET_ID": settings_preview["spreadsheet_id"],
            "SPREADSHEET_WORKSHEET_NAME": settings_preview["worksheet_name"],
            "TITLE_LIST_WORKSHEET_NAME": settings_preview["title_list_worksheet"],
            "YOUTUBE_API_KEY": "設定済み" if settings_preview["youtube_api_key"] else "未設定",
            "GOOGLE_SERVICE_ACCOUNT_JSON": "設定済み" if settings_preview["service_account_json"] else "未設定",
        }
    )

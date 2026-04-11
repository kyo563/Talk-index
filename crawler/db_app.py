from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime, timezone

import streamlit as st

if __package__ is None or __package__ == "":
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from crawler.models import VideoItem
from crawler.services.spreadsheet import (
    SpreadsheetServiceError,
    append_videos,
    extract_video_id_from_url,
    build_gspread_client,
    read_existing_video_ids,
    read_video_ids_from_sheet_rows,
)
from crawler.services.youtube import (
    YouTubeServiceError,
    build_youtube_client,
    fetch_channel_videos,
    fetch_video_item,
    list_timestamp_comments,
)


st.set_page_config(page_title="talk-indexDB", page_icon="🗂️", layout="wide")
st.title("talk-indexDB")
st.caption("定刻の自動実行と同じ処理を、手動でも実行できます。")

if "manual_logs" not in st.session_state:
    st.session_state.manual_logs = []
if "manual_comment_candidates" not in st.session_state:
    st.session_state.manual_comment_candidates = []


def load_settings() -> dict[str, str]:
    return {
        "youtube_api_key": os.getenv("YOUTUBE_API_KEY", "").strip(),
        "channel_id": os.getenv("YOUTUBE_CHANNEL_ID", "").strip(),
        "spreadsheet_id": os.getenv("SPREADSHEET_ID", "").strip(),
        "worksheet_name": os.getenv("SPREADSHEET_WORKSHEET_NAME", "索引").strip() or "索引",
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
    title_list_ids = read_video_ids_from_sheet_rows(
        client=gspread_client,
        spreadsheet_id=settings["spreadsheet_id"],
        worksheet_name=settings["title_list_worksheet"],
    )
    existing_ids = read_existing_video_ids(
        client=gspread_client,
        spreadsheet_id=settings["spreadsheet_id"],
        worksheet_name=settings["worksheet_name"],
    )

    if title_list_ids:
        candidates = [
            video
            for video in videos
            if video.video_id in title_list_ids and video.video_id not in existing_ids
        ]
    else:
        candidates = [
            video
            for video in videos
            if video.video_id not in existing_ids
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

st.subheader("1件手動記帳（URL指定）")
manual_api_key = st.text_input(
    "YouTube APIキー（未入力時は環境変数YOUTUBE_API_KEY）",
    value="",
    type="password",
)
manual_video_url = st.text_input("対象動画URL")
load_comments = st.button("コメント候補を取得", use_container_width=True)

if load_comments:
    api_key = manual_api_key.strip() or settings_preview["youtube_api_key"]
    video_id = extract_video_id_from_url(manual_video_url.strip())

    if not api_key:
        st.error("YouTube APIキーを入力してください。")
    elif not video_id:
        st.error("動画URLが不正です。YouTube URLを確認してください。")
    else:
        try:
            youtube = build_youtube_client(api_key)
            video_item = fetch_video_item(youtube, video_id)
            comments = list_timestamp_comments(youtube, video_id)
            st.session_state.manual_comment_candidates = comments
            st.session_state.manual_video_item = video_item
            if not comments:
                st.info("タイムスタンプ付きコメントが見つかりませんでした。")
            else:
                st.success(f"{len(comments)}件の候補を取得しました。")
        except (RuntimeError, SpreadsheetServiceError, YouTubeServiceError) as exc:
            st.error(f"取得失敗: {exc}")
        except Exception as exc:
            st.error(f"予期しないエラー: {exc}")

if st.session_state.manual_comment_candidates and "manual_video_item" in st.session_state:
    video_item = st.session_state.manual_video_item
    st.caption(f"動画ID: {video_item.video_id}")
    edited_title = st.text_input("動画タイトル（編集可）", value=video_item.title)

    comment_options = []
    for idx, row in enumerate(st.session_state.manual_comment_candidates):
        preview = str(row["text"]).replace("\n", " ")
        if len(preview) > 80:
            preview = f"{preview[:80]}..."
        comment_options.append(
            f"{idx + 1}. タイムスタンプ{row['timestamp_count']}個 / 👍{row['like_count']} / {preview}"
        )

    selected_idx = st.selectbox(
        "記帳に使うコメント",
        options=list(range(len(comment_options))),
        format_func=lambda i: comment_options[i],
    )

    write_manual = st.button("選択コメントで記帳", type="primary", use_container_width=True)
    if write_manual:
        settings = load_settings()
        if not settings["spreadsheet_id"]:
            st.error("SPREADSHEET_ID が未設定です。")
        elif not settings["service_account_json"]:
            st.error("GOOGLE_SERVICE_ACCOUNT_JSON が未設定です。")
        else:
            try:
                gspread_client = build_gspread_client(settings["service_account_json"])
                append_videos(
                    client=gspread_client,
                    spreadsheet_id=settings["spreadsheet_id"],
                    worksheet_name=settings["worksheet_name"],
                    channel_id=settings["channel_id"],
                    videos=[
                        VideoItem(
                            video_id=video_item.video_id,
                            title=edited_title.strip() or video_item.title,
                            url=video_item.url,
                            published_at=video_item.published_at,
                            thumbnail_url=video_item.thumbnail_url,
                            tags=video_item.tags,
                            timestamp_comment=str(
                                st.session_state.manual_comment_candidates[selected_idx]["text"]
                            ),
                        )
                    ],
                )
                st.success("スプレッドシートに記帳しました。")
            except (RuntimeError, SpreadsheetServiceError, YouTubeServiceError) as exc:
                st.error(f"記帳失敗: {exc}")
            except Exception as exc:
                st.error(f"予期しないエラー: {exc}")

st.divider()
st.subheader("従来の一括取り込み")
count = st.number_input("1回で読み込む件数", min_value=1, max_value=100, value=1, step=1)
run = st.button("読み込み実行", use_container_width=True)

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

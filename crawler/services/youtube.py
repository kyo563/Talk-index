from __future__ import annotations

import re
from typing import Callable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from crawler.models import VideoItem
from crawler.utils import extract_channel_hint, looks_like_channel_id

Logger = Callable[[str], None]
TIMESTAMP_PATTERN = re.compile(r"\b(?:\d{1,2}:)?\d{1,2}:\d{2}\b")


class YouTubeServiceError(RuntimeError):
    pass


def build_youtube_client(api_key: str):
    if not api_key:
        raise YouTubeServiceError("YOUTUBE_API_KEY が未設定です。")
    return build("youtube", "v3", developerKey=api_key)


def resolve_channel_id(youtube, channel_input: str, log: Logger | None = None) -> str:
    hint = extract_channel_hint(channel_input)
    if not hint:
        raise YouTubeServiceError("チャンネルIDまたはURLを入力してください。")

    if looks_like_channel_id(hint):
        return hint

    if log:
        log(f"チャンネル解決中: {hint}")

    query = hint.lstrip("@")
    response = (
        youtube.search()
        .list(part="snippet", q=query, type="channel", maxResults=1)
        .execute()
    )

    items = response.get("items", [])
    if not items:
        raise YouTubeServiceError("チャンネルが見つかりませんでした。入力値を確認してください。")

    channel_id = items[0].get("snippet", {}).get("channelId", "")
    if not channel_id:
        raise YouTubeServiceError("チャンネルIDの解決に失敗しました。")

    return channel_id


def fetch_channel_videos(
    youtube,
    channel_id: str,
    max_results: int,
    log: Logger | None = None,
    exclude_video_ids: set[str] | None = None,
) -> list[VideoItem]:
    try:
        channel_res = (
            youtube.channels()
            .list(part="contentDetails", id=channel_id, maxResults=1)
            .execute()
        )
    except HttpError as exc:
        raise YouTubeServiceError(f"channels.list でエラー: {exc}") from exc

    channel_items = channel_res.get("items", [])
    if not channel_items:
        raise YouTubeServiceError("指定チャンネルが見つかりませんでした。")

    uploads_playlist_id = (
        channel_items[0]
        .get("contentDetails", {})
        .get("relatedPlaylists", {})
        .get("uploads")
    )
    if not uploads_playlist_id:
        raise YouTubeServiceError("uploads プレイリストIDを取得できませんでした。")

    if log:
        log("動画IDを収集中...")

    excluded_ids = {v.strip() for v in (exclude_video_ids or set()) if v.strip()}
    videos: list[VideoItem] = []
    page_token = None

    while len(videos) < max_results:
        per_page = 50
        try:
            playlist_res = (
                youtube.playlistItems()
                .list(
                    part="contentDetails",
                    playlistId=uploads_playlist_id,
                    maxResults=per_page,
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as exc:
            raise YouTubeServiceError(f"playlistItems.list でエラー: {exc}") from exc

        playlist_items = playlist_res.get("items", [])
        if not playlist_items:
            break

        page_video_ids: list[str] = []
        for item in playlist_items:
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                page_video_ids.append(video_id)

        if page_video_ids:
            try:
                videos_res = (
                    youtube.videos()
                    .list(
                        part="snippet,liveStreamingDetails",
                        id=",".join(page_video_ids),
                        maxResults=len(page_video_ids),
                    )
                    .execute()
                )
            except HttpError as exc:
                raise YouTubeServiceError(f"videos.list でエラー: {exc}") from exc

            items_by_id = {item.get("id", ""): item for item in videos_res.get("items", [])}
            for video_id in page_video_ids:
                if len(videos) >= max_results:
                    break
                if video_id in excluded_ids:
                    continue

                item = items_by_id.get(video_id)
                if not item:
                    continue

                snippet = item.get("snippet", {})

                # 配信予定/ライブ中は除外
                if snippet.get("liveBroadcastContent") != "none":
                    continue

                # ライブ配信アーカイブのみ対象（開始/終了時刻があるもの）
                live_details = item.get("liveStreamingDetails", {})
                if not live_details.get("actualStartTime") or not live_details.get("actualEndTime"):
                    continue

                thumbs = snippet.get("thumbnails", {})
                thumb_url = (
                    thumbs.get("high", {}).get("url")
                    or thumbs.get("medium", {}).get("url")
                    or thumbs.get("default", {}).get("url")
                    or ""
                )

                timestamp_comment = ""
                try:
                    timestamp_comment = extract_timestamp_comment(youtube, video_id)
                except YouTubeServiceError as exc:
                    if log:
                        log(f"コメント抽出スキップ: video_id={video_id}, reason={exc}")

                videos.append(
                    VideoItem(
                        video_id=video_id,
                        title=snippet.get("title", ""),
                        url=f"https://www.youtube.com/watch?v={video_id}",
                        published_at=snippet.get("publishedAt", ""),
                        thumbnail_url=thumb_url,
                        tags=snippet.get("tags", []),
                        timestamp_comment=timestamp_comment,
                    )
                )

        page_token = playlist_res.get("nextPageToken")
        if not page_token:
            break

    return videos


def extract_timestamp_comment(youtube, video_id: str) -> str:
    if not video_id.strip():
        return ""

    try:
        response = (
            youtube.commentThreads()
            .list(
                part="snippet,replies",
                videoId=video_id,
                order="relevance",
                textFormat="plainText",
                maxResults=100,
            )
            .execute()
        )
    except HttpError as exc:
        raise YouTubeServiceError(f"commentThreads.list でエラー: video_id={video_id}, detail={exc}") from exc

    best_text = ""
    best_score = (-1, -1, -1)  # timestamp_count, like_count, text_length
    for row in _collect_timestamp_comment_rows(response):
        score = (
            int(row["timestamp_count"]),
            int(row["like_count"]),
            len(str(row["text"])),
        )
        if score > best_score:
            best_score = score
            best_text = str(row["text"])

    return best_text


def list_timestamp_comments(
    youtube,
    video_id: str,
    max_results: int = 100,
) -> list[dict[str, str | int]]:
    if not video_id.strip():
        raise YouTubeServiceError("動画IDが空です。URLを確認してください。")

    try:
        response = (
            youtube.commentThreads()
            .list(
                part="snippet,replies",
                videoId=video_id,
                order="relevance",
                textFormat="plainText",
                maxResults=max_results,
            )
            .execute()
        )
    except HttpError as exc:
        raise YouTubeServiceError(f"commentThreads.list でエラー: video_id={video_id}, detail={exc}") from exc

    results = _collect_timestamp_comment_rows(response)

    results.sort(
        key=lambda row: (
            int(row["timestamp_count"]),
            int(row["like_count"]),
            len(str(row["text"])),
        ),
        reverse=True,
    )
    return results


def _collect_timestamp_comment_rows(response: dict) -> list[dict[str, str | int]]:
    results: list[dict[str, str | int]] = []
    for item in response.get("items", []):
        top_level_snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        _append_timestamp_comment_row(results, top_level_snippet)

        reply_items = item.get("replies", {}).get("comments", [])
        for reply in reply_items:
            reply_snippet = reply.get("snippet", {})
            _append_timestamp_comment_row(results, reply_snippet)

    return results


def _append_timestamp_comment_row(
    results: list[dict[str, str | int]],
    snippet: dict,
) -> None:
    text = (snippet.get("textOriginal", "") or "").strip()
    if not text:
        return

    timestamps = TIMESTAMP_PATTERN.findall(text)
    if not timestamps:
        return

    results.append(
        {
            "text": text,
            "timestamp_count": len(set(timestamps)),
            "like_count": int(snippet.get("likeCount", 0) or 0),
        }
    )


def fetch_video_item(youtube, video_id: str) -> VideoItem:
    value = video_id.strip()
    if not value:
        raise YouTubeServiceError("動画IDが空です。URLを確認してください。")

    try:
        response = (
            youtube.videos()
            .list(
                part="snippet,liveStreamingDetails",
                id=value,
                maxResults=1,
            )
            .execute()
        )
    except HttpError as exc:
        raise YouTubeServiceError(f"videos.list でエラー: video_id={value}, detail={exc}") from exc

    items = response.get("items", [])
    if not items:
        raise YouTubeServiceError("動画情報を取得できませんでした。URLを確認してください。")

    item = items[0]
    snippet = item.get("snippet", {})
    thumbs = snippet.get("thumbnails", {})
    thumb_url = (
        thumbs.get("high", {}).get("url")
        or thumbs.get("medium", {}).get("url")
        or thumbs.get("default", {}).get("url")
        or ""
    )

    return VideoItem(
        video_id=value,
        title=snippet.get("title", ""),
        url=f"https://www.youtube.com/watch?v={value}",
        published_at=snippet.get("publishedAt", ""),
        thumbnail_url=thumb_url,
        tags=snippet.get("tags", []),
        timestamp_comment="",
    )

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

    video_ids: list[str] = []
    page_token = None

    while len(video_ids) < max_results:
        per_page = min(50, max_results - len(video_ids))
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

        items = playlist_res.get("items", [])
        if not items:
            break

        for item in items:
            video_id = item.get("contentDetails", {}).get("videoId")
            if video_id:
                video_ids.append(video_id)

        page_token = playlist_res.get("nextPageToken")
        if not page_token:
            break

    if not video_ids:
        return []

    if log:
        log(f"動画詳細を取得中... ({len(video_ids)}件)")

    videos: list[VideoItem] = []
    for i in range(0, len(video_ids), 50):
        chunk_ids = video_ids[i : i + 50]
        try:
            videos_res = (
                youtube.videos()
                .list(part="snippet", id=",".join(chunk_ids), maxResults=len(chunk_ids))
                .execute()
            )
        except HttpError as exc:
            raise YouTubeServiceError(f"videos.list でエラー: {exc}") from exc

        for item in videos_res.get("items", []):
            snippet = item.get("snippet", {})

            # 通常動画を優先（配信予定/ライブ中を除外）
            if snippet.get("liveBroadcastContent") != "none":
                continue

            thumbs = snippet.get("thumbnails", {})
            thumb_url = (
                thumbs.get("high", {}).get("url")
                or thumbs.get("medium", {}).get("url")
                or thumbs.get("default", {}).get("url")
                or ""
            )

            video_id = item.get("id", "")
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

    videos.sort(key=lambda x: x.published_at, reverse=True)
    return videos[:max_results]


def extract_timestamp_comment(youtube, video_id: str) -> str:
    if not video_id.strip():
        return ""

    try:
        response = (
            youtube.commentThreads()
            .list(
                part="snippet",
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
    for item in response.get("items", []):
        snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        text = (snippet.get("textOriginal", "") or "").strip()
        if not text:
            continue

        timestamps = TIMESTAMP_PATTERN.findall(text)
        if not timestamps:
            continue

        unique_timestamps = len(set(timestamps))
        like_count = int(snippet.get("likeCount", 0) or 0)
        text_length = len(text)
        score = (unique_timestamps, like_count, text_length)
        if score > best_score:
            best_score = score
            best_text = text

    return best_text

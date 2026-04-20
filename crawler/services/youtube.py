from __future__ import annotations

import os
import re
from typing import Callable

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from crawler.models import TimestampSource, VideoItem
from crawler.utils import extract_channel_hint, looks_like_channel_id

Logger = Callable[[str], None]
TIMESTAMP_PATTERN = re.compile(r"\b(?:\d{1,2}:)?\d{1,2}:\d{2}\b")
DEFAULT_TIMESTAMP_COMMENT_THREAD_LIMIT = 300


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

                if snippet.get("liveBroadcastContent") != "none":
                    continue

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

                description = (snippet.get("description", "") or "").strip()
                timestamp_sources: list[TimestampSource] = []
                timestamp_comment = ""
                try:
                    timestamp_sources = fetch_timestamp_sources(
                        youtube,
                        video_id,
                        description=description,
                    )
                    timestamp_comment = _choose_best_comment_source_text(timestamp_sources)
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
                        description=description,
                        timestamp_sources=timestamp_sources,
                    )
                )

        page_token = playlist_res.get("nextPageToken")
        if not page_token:
            break

    return videos


def fetch_timestamp_sources(
    youtube,
    video_id: str,
    description: str | None = None,
) -> list[TimestampSource]:
    value = (video_id or "").strip()
    if not value:
        return []

    results: list[TimestampSource] = []
    normalized_description = (description or "").strip()
    if normalized_description:
        ts_count = _count_timestamps(normalized_description)
        if ts_count > 0:
            results.append(
                TimestampSource(
                    source_type="description",
                    text=normalized_description,
                    like_count=0,
                    timestamp_count=ts_count,
                    source_id=f"description:{value}",
                )
            )

    thread_limit = _load_comment_thread_limit()
    thread_items = _fetch_comment_threads(youtube, value, thread_limit)
    for item in thread_items:
        thread_snippet = item.get("snippet", {})
        top_level = thread_snippet.get("topLevelComment", {})
        top_level_id = (top_level.get("id") or item.get("id") or "").strip()

        top_level_snippet = top_level.get("snippet", {})
        top_source = _build_comment_source(
            snippet=top_level_snippet,
            source_type="top",
            source_id=top_level_id,
            parent_id="",
        )
        if top_source:
            results.append(top_source)

        replies = _fetch_all_replies(youtube, item, top_level_id=top_level_id)
        for reply in replies:
            reply_id = (reply.get("id") or "").strip()
            reply_source = _build_comment_source(
                snippet=reply.get("snippet", {}),
                source_type="reply",
                source_id=reply_id,
                parent_id=top_level_id,
            )
            if reply_source:
                results.append(reply_source)

    results.sort(
        key=lambda src: (
            1 if src.source_type == "description" else 0,
            int(src.timestamp_count),
            int(src.like_count),
            len(src.text),
        ),
        reverse=True,
    )
    return results


def _load_comment_thread_limit() -> int:
    raw = os.getenv("TIMESTAMP_COMMENT_THREAD_LIMIT", "").strip()
    if not raw:
        return DEFAULT_TIMESTAMP_COMMENT_THREAD_LIMIT
    try:
        value = int(raw)
    except ValueError as exc:
        raise YouTubeServiceError("TIMESTAMP_COMMENT_THREAD_LIMIT は整数で指定してください。") from exc
    if value <= 0:
        raise YouTubeServiceError("TIMESTAMP_COMMENT_THREAD_LIMIT は1以上で指定してください。")
    return value


def _fetch_comment_threads(youtube, video_id: str, thread_limit: int) -> list[dict]:
    items: list[dict] = []
    page_token = None

    while len(items) < thread_limit:
        per_page = min(100, thread_limit - len(items))
        try:
            response = (
                youtube.commentThreads()
                .list(
                    part="snippet,replies",
                    videoId=video_id,
                    # 手動運用との整合のため relevance を維持しつつ、複数ページを回収する。
                    order="relevance",
                    textFormat="plainText",
                    maxResults=per_page,
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as exc:
            raise YouTubeServiceError(
                f"commentThreads.list でエラー: video_id={video_id}, detail={exc}"
            ) from exc

        page_items = response.get("items", [])
        if not page_items:
            break

        items.extend(page_items)
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return items


def extract_timestamp_comment(youtube, video_id: str) -> str:
    if not video_id.strip():
        return ""

    sources = fetch_timestamp_sources(youtube, video_id)
    return _choose_best_comment_source_text(sources)


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

    results = _collect_timestamp_comment_rows(youtube, response)

    results.sort(
        key=lambda row: (
            int(row["timestamp_count"]),
            int(row["like_count"]),
            len(str(row["text"])),
        ),
        reverse=True,
    )
    return results


def _collect_timestamp_comment_rows(youtube, response: dict) -> list[dict[str, str | int]]:
    results: list[dict[str, str | int]] = []
    for item in response.get("items", []):
        top_level_snippet = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
        _append_timestamp_comment_row(results, top_level_snippet, comment_type="top")

        thread_snippet = item.get("snippet", {})
        top_level_id = (
            thread_snippet.get("topLevelComment", {}).get("id")
            or item.get("id")
            or ""
        )
        reply_items = _fetch_all_replies(youtube, item, top_level_id=top_level_id)
        for reply in reply_items:
            reply_snippet = reply.get("snippet", {})
            _append_timestamp_comment_row(results, reply_snippet, comment_type="reply")

    return results


def _fetch_all_replies(youtube, thread_item: dict, top_level_id: str) -> list[dict]:
    embedded_replies = thread_item.get("replies", {}).get("comments", [])
    total_reply_count = int(thread_item.get("snippet", {}).get("totalReplyCount", 0) or 0)
    if total_reply_count <= len(embedded_replies):
        return embedded_replies

    if not top_level_id:
        return embedded_replies

    replies: list[dict] = []
    page_token = None
    while True:
        try:
            response = (
                youtube.comments()
                .list(
                    part="snippet",
                    parentId=top_level_id,
                    textFormat="plainText",
                    maxResults=100,
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as exc:
            raise YouTubeServiceError(
                f"comments.list で返信取得に失敗: parent_id={top_level_id}, detail={exc}"
            ) from exc

        replies.extend(response.get("items", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return replies or embedded_replies


def _append_timestamp_comment_row(
    results: list[dict[str, str | int]],
    snippet: dict,
    comment_type: str,
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
            "comment_type": comment_type,
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
    description = (snippet.get("description", "") or "").strip()
    timestamp_sources = fetch_timestamp_sources(youtube, value, description=description)

    return VideoItem(
        video_id=value,
        title=snippet.get("title", ""),
        url=f"https://www.youtube.com/watch?v={value}",
        published_at=snippet.get("publishedAt", ""),
        thumbnail_url=thumb_url,
        tags=snippet.get("tags", []),
        timestamp_comment=_choose_best_comment_source_text(timestamp_sources),
        description=description,
        timestamp_sources=timestamp_sources,
    )


def fetch_video_metadata_map(youtube, video_ids: list[str]) -> dict[str, VideoItem]:
    normalized_ids = [video_id.strip() for video_id in video_ids if video_id.strip()]
    if not normalized_ids:
        return {}

    result: dict[str, VideoItem] = {}
    for i in range(0, len(normalized_ids), 50):
        chunk_ids = normalized_ids[i : i + 50]
        try:
            response = (
                youtube.videos()
                .list(
                    part="snippet",
                    id=",".join(chunk_ids),
                    maxResults=len(chunk_ids),
                )
                .execute()
            )
        except HttpError as exc:
            raise YouTubeServiceError(
                f"videos.list でメタデータ取得に失敗: count={len(chunk_ids)}, detail={exc}"
            ) from exc

        for item in response.get("items", []):
            video_id = (item.get("id") or "").strip()
            if not video_id:
                continue
            snippet = item.get("snippet", {})
            result[video_id] = VideoItem(
                video_id=video_id,
                title=(snippet.get("title", "") or "").strip(),
                url=f"https://www.youtube.com/watch?v={video_id}",
                published_at=(snippet.get("publishedAt", "") or "").strip(),
                thumbnail_url="",
                tags=[],
                timestamp_comment="",
                description="",
                timestamp_sources=[],
            )

    return result


def _count_timestamps(text: str) -> int:
    timestamps = TIMESTAMP_PATTERN.findall(text or "")
    return len(set(timestamps)) if timestamps else 0


def _build_comment_source(
    snippet: dict,
    source_type: str,
    source_id: str,
    parent_id: str,
) -> TimestampSource | None:
    text = (snippet.get("textOriginal", "") or "").strip()
    if not text:
        return None

    ts_count = _count_timestamps(text)
    if ts_count <= 0:
        return None

    return TimestampSource(
        source_type=source_type,  # type: ignore[arg-type]
        text=text,
        like_count=int(snippet.get("likeCount", 0) or 0),
        timestamp_count=ts_count,
        source_id=source_id,
        parent_id=parent_id,
        author=(snippet.get("authorDisplayName", "") or "").strip(),
    )


def _choose_best_comment_source_text(sources: list[TimestampSource]) -> str:
    comment_sources = [s for s in sources if s.source_type in {"top", "reply"}]
    if not comment_sources:
        return ""

    comment_sources.sort(
        key=lambda src: (src.timestamp_count, src.like_count, len(src.text)),
        reverse=True,
    )
    return comment_sources[0].text

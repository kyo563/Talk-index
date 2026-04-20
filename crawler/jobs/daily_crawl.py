from __future__ import annotations

import os
from datetime import datetime, timezone

from crawler.services.spreadsheet import (
    append_title_list_rows,
    append_videos,
    build_gspread_client,
    ensure_title_list_state_cells,
    normalize_spreadsheet_id,
    read_existing_video_ids,
    read_ordered_video_ids_from_title_list,
    read_title_list_refresh_state,
    upsert_videos_by_video_id,
    upsert_title_list_rows,
    write_title_list_refresh_state,
)
from crawler.services.youtube import (
    build_youtube_client,
    fetch_channel_videos,
    fetch_video_item,
    fetch_video_metadata_map,
)


def _load_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default

    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} は整数で指定してください。") from exc

    if value < 0:
        raise RuntimeError(f"{name} は0以上で指定してください。")

    return value


def _select_cyclic_targets(ordered_video_ids: list[str], current_cursor: int, limit: int) -> tuple[list[str], int]:
    if limit <= 0 or not ordered_video_ids:
        return [], 0

    total = len(ordered_video_ids)
    start_cursor = current_cursor % total
    count = min(limit, total)
    selected: list[str] = []

    for i in range(count):
        idx = (start_cursor + i) % total
        selected.append(ordered_video_ids[idx])

    next_cursor = (start_cursor + count) % total
    return selected, next_cursor


def _parse_iso_datetime(raw: str) -> datetime | None:
    value = (raw or "").strip()
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _select_recheck_ids(
    ordered_video_ids: list[str],
    current_cursor: int,
    limit: int,
    recent_hours: int,
    videos_by_id: dict[str, object],
) -> tuple[list[str], int]:
    if limit <= 0 or not ordered_video_ids:
        return [], current_cursor if current_cursor >= 0 else 0

    now = datetime.now(timezone.utc)
    threshold = now.timestamp() - (recent_hours * 3600)

    recent_candidates: list[tuple[float, str]] = []
    for video_id in ordered_video_ids:
        video = videos_by_id.get(video_id)
        published_at = getattr(video, "published_at", "")
        published = _parse_iso_datetime(str(published_at))
        if not published:
            continue
        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)
        published_ts = published.timestamp()
        if published_ts < threshold:
            continue
        recent_candidates.append((published_ts, video_id))

    recent_candidates.sort(key=lambda x: x[0], reverse=True)
    selected: list[str] = []
    selected_set: set[str] = set()
    for _, video_id in recent_candidates:
        if len(selected) >= limit:
            break
        selected.append(video_id)
        selected_set.add(video_id)

    remaining = limit - len(selected)
    cyclic_selected, next_cursor = _select_cyclic_targets(ordered_video_ids, current_cursor, remaining)
    for video_id in cyclic_selected:
        if len(selected) >= limit:
            break
        if video_id in selected_set:
            continue
        selected.append(video_id)
        selected_set.add(video_id)

    return selected, next_cursor


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

    daily_new_video_limit = _load_positive_int_env("DAILY_NEW_VIDEO_LIMIT", 2)
    daily_recheck_limit = _load_positive_int_env("DAILY_RECHECK_LIMIT", 5)
    daily_recent_recheck_hours = _load_positive_int_env("DAILY_RECENT_RECHECK_HOURS", 72)

    youtube = build_youtube_client(youtube_api_key)
    gspread_client = build_gspread_client(service_account_json)

    ensure_title_list_state_cells(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
    )

    title_list_ids = set(
        read_ordered_video_ids_from_title_list(
            client=gspread_client,
            spreadsheet_id=spreadsheet_id,
            worksheet_name=title_list_worksheet,
        )
    )
    existing_ids = read_existing_video_ids(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
    )

    skip_ids = title_list_ids | existing_ids
    fetched_candidates = fetch_channel_videos(
        youtube,
        channel_id,
        max_results=max_results,
        exclude_video_ids=skip_ids,
    )
    new_videos = fetched_candidates[:daily_new_video_limit]

    title_list_appended = append_title_list_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
        videos=new_videos,
    )

    appended_count = append_videos(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
        channel_id=channel_id,
        videos=new_videos,
    )

    ordered_title_list_ids = read_ordered_video_ids_from_title_list(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
    )
    refresh_state = read_title_list_refresh_state(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
    )
    current_cursor = int(refresh_state.get("refresh_cursor", 0) or 0)

    videos_by_id = fetch_video_metadata_map(youtube, ordered_title_list_ids)
    for video in fetched_candidates:
        videos_by_id[video.video_id] = video

    recheck_ids, next_cursor = _select_recheck_ids(
        ordered_video_ids=ordered_title_list_ids,
        current_cursor=current_cursor,
        limit=daily_recheck_limit,
        recent_hours=daily_recent_recheck_hours,
        videos_by_id=videos_by_id,
    )

    recheck_videos = []
    for video_id in recheck_ids:
        try:
            recheck_videos.append(fetch_video_item(youtube, video_id))
        except Exception as exc:
            print(f"warning: 再評価取得スキップ video_id={video_id}, reason={exc}")

    rechecked_count = upsert_videos_by_video_id(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
        videos=recheck_videos,
    )
    title_updated_count, title_appended_count = upsert_title_list_rows(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
        videos=recheck_videos,
    )

    if not ordered_title_list_ids:
        next_cursor = 0

    updated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    write_title_list_refresh_state(
        client=gspread_client,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=title_list_worksheet,
        refresh_cursor=next_cursor,
        updated_at=updated_at,
    )

    print(
        "done: "
        f"channel_id={channel_id}, "
        f"fetched_candidates={len(fetched_candidates)}, "
        f"new_video_limit={daily_new_video_limit}, "
        f"new_selected={len(new_videos)}, "
        f"title_list_appended={title_list_appended}, "
        f"appended={appended_count}, "
        f"ordered_title_list_count={len(ordered_title_list_ids)}, "
        f"recheck_limit={daily_recheck_limit}, "
        f"recent_recheck_hours={daily_recent_recheck_hours}, "
        f"recheck_selected_count={len(recheck_ids)}, "
        f"recheck_upserted_rows={rechecked_count}, "
        f"title_recheck_updated={title_updated_count}, "
        f"title_recheck_appended={title_appended_count}, "
        f"current_cursor={current_cursor}, "
        f"next_cursor={next_cursor}, "
        f"updated_at={updated_at}"
    )


if __name__ == "__main__":
    main()

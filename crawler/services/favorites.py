from __future__ import annotations

import hashlib
import hmac
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

JST_OFFSET = timedelta(hours=9)


@dataclass(frozen=True)
class FavoriteVote:
    heading_id: str
    video_id: str
    heading_title: str
    video_title: str
    heading_start: str
    source_mode: str
    first_voted_at: str
    week_key: str
    source_video_url: str
    published_at: str


def to_text(value: Any) -> str:
    return str(value or "").strip()


def hash_with_secret(secret: str, value: str, *, scope: str) -> str:
    normalized_secret = to_text(secret)
    normalized_value = to_text(value)
    if not normalized_secret:
        raise RuntimeError("FAVORITES_HASH_SECRET が未設定です。GitHub Secrets を確認してください。")
    if not normalized_value:
        return ""
    digest = hmac.new(
        normalized_secret.encode("utf-8"),
        f"{scope}:{normalized_value}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return digest


def parse_iso_datetime(value: str) -> datetime:
    raw = to_text(value)
    if not raw:
        raise ValueError("timestamp が空です")
    normalized = raw.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_iso_datetime_optional(value: str) -> datetime | None:
    raw = to_text(value)
    if not raw:
        return None
    try:
        return parse_iso_datetime(raw)
    except ValueError:
        return None


def to_week_key_jst(value: str) -> str:
    utc_dt = parse_iso_datetime(value)
    jst_dt = utc_dt.astimezone(UTC) + JST_OFFSET
    monday = jst_dt - timedelta(days=jst_dt.weekday())
    return monday.strftime("%Y-%m-%d")


def normalize_vote_record(record: dict[str, Any]) -> FavoriteVote | None:
    heading_id = to_text(record.get("headingId"))
    client_hash = to_text(record.get("clientHash"))
    first_voted_at = to_text(record.get("firstVotedAt"))
    if not heading_id or not client_hash or not first_voted_at:
        return None

    week_key = to_text(record.get("weekKey")) or to_week_key_jst(first_voted_at)

    return FavoriteVote(
        heading_id=heading_id,
        video_id=to_text(record.get("videoId")),
        heading_title=to_text(record.get("headingTitle")) or heading_id,
        video_title=to_text(record.get("videoTitle")),
        heading_start=to_text(record.get("headingStart")),
        source_mode=to_text(record.get("sourceMode")) or "unknown",
        first_voted_at=first_voted_at,
        week_key=week_key,
        source_video_url=to_text(record.get("sourceVideoUrl")) or to_text(record.get("videoUrl")),
        published_at=to_text(record.get("publishedAt")) or to_text(record.get("videoDate")),
    )


def build_video_metadata_map(talks_payload: dict[str, Any], latest_payload: dict[str, Any]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    talks = talks_payload.get("talks") if isinstance(talks_payload, dict) else []
    if isinstance(talks, list):
        for talk in talks:
            if not isinstance(talk, dict):
                continue
            talk_date = to_text(talk.get("date"))
            subsections = talk.get("subsections") if isinstance(talk.get("subsections"), list) else []
            for subsection in subsections:
                if not isinstance(subsection, dict):
                    continue
                url = to_text(subsection.get("videoUrl"))
                video_id = to_text(subsection.get("videoId"))
                if not video_id:
                    from crawler.services.spreadsheet import extract_video_id_from_url
                    video_id = extract_video_id_from_url(url)
                if not video_id:
                    continue
                out[video_id] = {
                    "title": to_text(subsection.get("videoTitle")),
                    "url": url,
                    "published_at": talk_date,
                }

    latest_items: list[Any] = []
    if isinstance(latest_payload, dict):
        for key in ("videos", "items", "data"):
            values = latest_payload.get(key)
            if isinstance(values, list):
                latest_items = values
                break
    elif isinstance(latest_payload, list):
        latest_items = latest_payload

    for item in latest_items:
        if not isinstance(item, dict):
            continue
        video_id = to_text(item.get("id")) or to_text(item.get("videoId"))
        if not video_id:
            from crawler.services.spreadsheet import extract_video_id_from_url
            video_id = extract_video_id_from_url(to_text(item.get("url")))
        if not video_id:
            continue
        existing = out.get(video_id, {})
        out[video_id] = {
            "title": to_text(item.get("title")) or to_text(existing.get("title")),
            "url": to_text(item.get("url")) or to_text(existing.get("url")),
            "published_at": to_text(item.get("date")) or to_text(item.get("publishedAt")) or to_text(existing.get("published_at")),
        }
    return out


def stable_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(item.get("voteCount", 0)),
        to_text(item.get("firstVotedAt")),
        to_text(item.get("headingId")),
    )


def build_aggregates(
    votes: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
    video_metadata_map: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    ranking: dict[str, dict[str, Any]] = {}
    weekly_counts: defaultdict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    for raw in votes:
        vote = normalize_vote_record(raw)
        if vote is None:
            continue

        if vote.heading_id not in ranking:
            ranking[vote.heading_id] = {
                "headingId": vote.heading_id,
                "videoId": vote.video_id,
                "headingTitle": vote.heading_title,
                "videoTitle": vote.video_title,
                "headingStart": vote.heading_start,
                "sourceMode": vote.source_mode,
                "voteCount": 0,
                "firstVotedAt": vote.first_voted_at,
                "lastVotedAt": vote.first_voted_at,
                "sourceVideoTitle": vote.video_title,
                "sourceVideoUrl": vote.source_video_url,
                "publishedAt": vote.published_at,
            }
        item = ranking[vote.heading_id]
        item["voteCount"] += 1
        if vote.first_voted_at and vote.first_voted_at < to_text(item.get("firstVotedAt")):
            item["firstVotedAt"] = vote.first_voted_at
        if vote.first_voted_at and vote.first_voted_at > to_text(item.get("lastVotedAt")):
            item["lastVotedAt"] = vote.first_voted_at
        if not to_text(item.get("sourceVideoTitle")) and vote.video_title:
            item["sourceVideoTitle"] = vote.video_title
        if not to_text(item.get("sourceVideoUrl")) and vote.source_video_url:
            item["sourceVideoUrl"] = vote.source_video_url
        if not to_text(item.get("publishedAt")) and vote.published_at:
            item["publishedAt"] = vote.published_at

        week_group = weekly_counts[vote.week_key]
        if vote.heading_id not in week_group:
            week_group[vote.heading_id] = {
                "headingId": vote.heading_id,
                "videoId": vote.video_id,
                "headingTitle": vote.heading_title,
                "videoTitle": vote.video_title,
                "headingStart": vote.heading_start,
                "sourceMode": vote.source_mode,
                "voteCount": 0,
                "firstVotedAt": vote.first_voted_at,
                "lastVotedAt": vote.first_voted_at,
            }
        w_item = week_group[vote.heading_id]
        w_item["voteCount"] += 1
        if vote.first_voted_at < to_text(w_item.get("firstVotedAt")):
            w_item["firstVotedAt"] = vote.first_voted_at
        if vote.first_voted_at > to_text(w_item.get("lastVotedAt")):
            w_item["lastVotedAt"] = vote.first_voted_at

    all_ranking = sorted(ranking.values(), key=stable_sort_key)
    now_base = now_utc or datetime.now(UTC)
    jst_now = now_base + JST_OFFSET
    current_week_key = (jst_now - timedelta(days=jst_now.weekday())).strftime("%Y-%m-%d")
    previous_week_key = (datetime.fromisoformat(current_week_key) - timedelta(days=7)).strftime("%Y-%m-%d")

    weekly_items = sorted(weekly_counts.get(previous_week_key, {}).values(), key=stable_sort_key)

    metadata_map = video_metadata_map or {}
    for item in all_ranking:
        video_id = to_text(item.get("videoId"))
        if not video_id:
            continue
        meta = metadata_map.get(video_id, {})
        if not to_text(item.get("sourceVideoTitle")) and to_text(meta.get("title")):
            item["sourceVideoTitle"] = to_text(meta.get("title"))
        if not to_text(item.get("sourceVideoUrl")) and to_text(meta.get("url")):
            item["sourceVideoUrl"] = to_text(meta.get("url"))
        if not to_text(item.get("publishedAt")) and to_text(meta.get("published_at")):
            item["publishedAt"] = to_text(meta.get("published_at"))

    recent_upload_window_start = now_base - timedelta(days=7)
    recent_upload_items: list[tuple[dict[str, Any], float]] = []
    for item in all_ranking:
        published_at = to_text(item.get("publishedAt"))
        published_dt = parse_iso_datetime_optional(published_at)
        if published_dt is None and published_at:
            published_dt = parse_iso_datetime_optional(f"{published_at}T00:00:00Z")
        if published_dt is None:
            continue
        if not (recent_upload_window_start <= published_dt <= now_base):
            continue
        recent_upload_items.append((item, published_dt.timestamp()))

    recent_upload_items.sort(
        key=lambda entry: (
            -int(entry[0].get("voteCount", 0)),
            -entry[1],
            to_text(entry[0].get("headingId")),
        )
    )

    base_payload = {
        "generatedAt": now_base.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source": "favorites/unique",
    }

    weekly_payloads = {
        week_key: {
            **base_payload,
            "weekKey": week_key,
            "items": sorted(items.values(), key=stable_sort_key),
        }
        for week_key, items in weekly_counts.items()
    }

    return {
        "all_time": {
            **base_payload,
            "items": all_ranking,
        },
        "hall_of_fame": {
            **base_payload,
            "items": all_ranking[:3],
        },
        "recent_recommendations": {
            **base_payload,
            "weekKey": previous_week_key,
            "items": weekly_items[:5],
        },
        "recent_upload_recommendations": {
            **base_payload,
            "items": [entry[0] for entry in recent_upload_items[:5]],
        },
        "current_ranking": {
            **base_payload,
            "items": all_ranking,
        },
        "weekly": weekly_payloads,
        "daily_snapshot": {
            **base_payload,
            "snapshotDate": jst_now.strftime("%Y-%m-%d"),
            "items": all_ranking,
        },
    }


def dump_json(data: dict[str, Any]) -> bytes:
    return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

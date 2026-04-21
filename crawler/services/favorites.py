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
    )


def stable_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -int(item.get("voteCount", 0)),
        to_text(item.get("firstVotedAt")),
        to_text(item.get("headingId")),
    )


def build_aggregates(votes: list[dict[str, Any]], *, now_utc: datetime | None = None) -> dict[str, Any]:
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
            }
        item = ranking[vote.heading_id]
        item["voteCount"] += 1
        if vote.first_voted_at and vote.first_voted_at < to_text(item.get("firstVotedAt")):
            item["firstVotedAt"] = vote.first_voted_at
        if vote.first_voted_at and vote.first_voted_at > to_text(item.get("lastVotedAt")):
            item["lastVotedAt"] = vote.first_voted_at

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

    weekly_items = sorted(weekly_counts.get(current_week_key, {}).values(), key=stable_sort_key)

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
            "weekKey": current_week_key,
            "items": weekly_items[:5],
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

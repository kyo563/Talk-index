from __future__ import annotations

import re
from dataclasses import dataclass, field

from crawler.models import TimestampSource

HMS_PATTERN = r"(?:\d{1,2}):[0-5]\d:[0-5]\d"
LINE_END_PAREN_TS_PATTERN = re.compile(rf"^(?P<label>.*?)[\(（]\s*(?P<ts>{HMS_PATTERN})\s*[\)）]\s*$")
LEADING_TS_PATTERN = re.compile(rf"^(?P<ts>{HMS_PATTERN})\s*(?P<label>.*)$")
TREE_PREFIX_PATTERN = re.compile(r"^\s*(?P<prefix>[├┝└┗┣┠┡┢│┃\s]+)?(?P<body>.*)$")
NOISE_PATTERN = re.compile(r"^[\s\-:：|／/、。・･]+|[\s\-:：|／/、。・･]+$")


@dataclass
class ParsedTimestampEntry:
    seconds: int
    label: str
    is_minor: bool
    source_type: str
    source_priority: int


@dataclass
class GroupedTimeline:
    major_seconds: int
    major_label: str
    minor_items: list[ParsedTimestampEntry] = field(default_factory=list)


def build_timestamp_rows(
    video_url: str,
    description: str = "",
    timestamp_sources: list[TimestampSource] | None = None,
    fallback_text: str = "",
) -> list[tuple[str, str, str, str]]:
    sources = _build_effective_sources(description, timestamp_sources, fallback_text)
    deduped = _merge_and_dedup_entries(sources)
    grouped = _group_entries(deduped)

    rows: list[tuple[str, str, str, str]] = []
    for group in grouped:
        major_label = group.major_label or _format_time(group.major_seconds)
        major_url = _build_timestamp_url(video_url, group.major_seconds)

        if not group.minor_items:
            rows.append((major_label, major_url, "", ""))
            continue

        for minor in group.minor_items:
            minor_label = minor.label or _format_time(minor.seconds)
            minor_url = _build_timestamp_url(video_url, minor.seconds)
            rows.append((major_label, major_url, minor_label, minor_url))

    return rows


def _build_effective_sources(
    description: str,
    timestamp_sources: list[TimestampSource] | None,
    fallback_text: str,
) -> list[TimestampSource]:
    items: list[TimestampSource] = []

    for src in timestamp_sources or []:
        text = (src.text or "").strip()
        if not text:
            continue
        items.append(
            TimestampSource(
                source_type=src.source_type,
                text=text,
                like_count=src.like_count,
                timestamp_count=src.timestamp_count,
                source_id=src.source_id,
                parent_id=src.parent_id,
                author=src.author,
            )
        )

    if description.strip():
        items.append(TimestampSource(source_type="description", text=description.strip()))

    if not items and fallback_text.strip():
        items.append(TimestampSource(source_type="top", text=fallback_text.strip()))

    return items


def _merge_and_dedup_entries(sources: list[TimestampSource]) -> list[ParsedTimestampEntry]:
    indexed_entries: list[tuple[int, ParsedTimestampEntry]] = []
    order = 0
    for source in sources:
        priority = _source_priority(source.source_type)
        for raw_line in source.text.splitlines():
            parsed = _parse_line(raw_line, source.source_type, priority)
            if not parsed:
                continue
            for entry in parsed:
                indexed_entries.append((order, entry))
                order += 1

    comment_entries = [(idx, ent) for idx, ent in indexed_entries if ent.source_type != "description"]
    occupied_seconds = {ent.seconds for _, ent in comment_entries}
    description_entries = [
        (idx, ent)
        for idx, ent in indexed_entries
        if ent.source_type == "description" and ent.seconds not in occupied_seconds
    ]

    merged = comment_entries + description_entries
    exact_merged: dict[tuple[int, str], tuple[int, ParsedTimestampEntry]] = {}
    for idx, entry in merged:
        key = (entry.seconds, _normalize_dedup_label(entry.label))
        existing = exact_merged.get(key)
        if not existing:
            exact_merged[key] = (idx, entry)
            continue

        existing_idx, existing_entry = existing
        if entry.source_priority > existing_entry.source_priority:
            exact_merged[key] = (idx, entry)
            continue
        if entry.source_priority == existing_entry.source_priority:
            if len(entry.label) > len(existing_entry.label):
                exact_merged[key] = (idx, entry)
                continue
            if len(entry.label) == len(existing_entry.label) and idx < existing_idx:
                exact_merged[key] = (idx, entry)

    merged = sorted(exact_merged.values(), key=lambda item: item[0])
    accepted: list[tuple[int, ParsedTimestampEntry]] = []

    for idx, entry in merged:
        duplicate_indexes = [
            i
            for i, (accepted_idx, accepted_entry) in enumerate(accepted)
            if _is_duplicate_candidate(entry, accepted_entry)
        ]
        if not duplicate_indexes:
            accepted.append((idx, entry))
            continue

        contenders = [(idx, entry)] + [accepted[i] for i in duplicate_indexes]
        winner_idx, winner_entry = min(
            contenders,
            key=lambda item: (-item[1].source_priority, item[0]),
        )
        if winner_idx != idx:
            continue

        for i in reversed(duplicate_indexes):
            accepted.pop(i)
        accepted.append((idx, winner_entry))

    return [entry for _, entry in sorted(accepted, key=lambda item: item[1].seconds)]


def _group_entries(entries: list[ParsedTimestampEntry]) -> list[GroupedTimeline]:
    if not entries:
        return []

    groups: list[GroupedTimeline] = []
    current_major: GroupedTimeline | None = None

    for entry in entries:
        if _is_major_entry(entry):
            if current_major and not current_major.major_label:
                current_major.major_label = _format_time(current_major.major_seconds)
            current_major = GroupedTimeline(major_seconds=entry.seconds, major_label=entry.label)
            groups.append(current_major)
            continue

        if current_major is None:
            continue

        current_major.minor_items.append(entry)

    return groups


def _parse_line(line: str, source_type: str, source_priority: int) -> list[ParsedTimestampEntry]:
    normalized = _normalize_line(line)
    if not normalized:
        return []

    prefix_match = TREE_PREFIX_PATTERN.match(normalized)
    body = (prefix_match.group("body") if prefix_match else normalized).strip()
    prefix = (prefix_match.group("prefix") if prefix_match else "") or ""
    has_tree_prefix = any(ch in "├┝└┗┣┠┡┢│┃" for ch in prefix)

    ts = ""
    label_text = ""

    lead_match = LEADING_TS_PATTERN.match(body)
    if lead_match:
        ts = (lead_match.group("ts") or "").strip()
        label_text = (lead_match.group("label") or "").strip()
    else:
        end_match = LINE_END_PAREN_TS_PATTERN.match(body)
        if not end_match:
            return []
        ts = (end_match.group("ts") or "").strip()
        label_text = (end_match.group("label") or "").strip()

    seconds = _timestamp_to_seconds(ts)
    if seconds < 0:
        return []

    label = _clean_label(label_text)
    if not label:
        return []

    return [
        ParsedTimestampEntry(
            seconds=seconds,
            label=label,
            is_minor=has_tree_prefix,
            source_type=source_type,
            source_priority=source_priority,
        )
    ]


def _is_major_entry(entry: ParsedTimestampEntry) -> bool:
    return not entry.is_minor


def _is_duplicate_candidate(entry: ParsedTimestampEntry, existing: ParsedTimestampEntry) -> bool:
    if entry.source_type == existing.source_type:
        return False

    if abs(entry.seconds - existing.seconds) <= 10:
        return True

    return False


def _normalize_line(line: str) -> str:
    value = (line or "").replace("\u3000", " ").replace("\t", " ").strip()
    if not value:
        return ""
    value = value.replace("｜", "|")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _clean_label(label: str) -> str:
    value = (label or "").strip()
    if not value:
        return ""
    value = re.sub(rf"{HMS_PATTERN}", " ", value)
    value = re.sub(r"[()（）]", " ", value)
    value = NOISE_PATTERN.sub("", value)
    value = re.sub(r"\s+", " ", value).strip()
    if value in {")", "）", "(", "（"}:
        return ""
    return value


def _normalize_dedup_label(label: str) -> str:
    value = _clean_label(label).lower()
    value = re.sub(r"\s+", "", value)
    return value


def _source_priority(source_type: str) -> int:
    if source_type == "top":
        return 3
    if source_type == "reply":
        return 2
    return 1


def _timestamp_to_seconds(timestamp: str) -> int:
    raw = (timestamp or "").strip()
    if not raw:
        return -1

    parts = raw.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return -1

    if len(nums) == 3:
        hh, mm, ss = nums
        if hh < 0 or mm < 0 or ss < 0 or mm >= 60 or ss >= 60:
            return -1
        return hh * 3600 + mm * 60 + ss

    return -1


def _format_time(seconds: int) -> str:
    if seconds < 0:
        return ""
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _build_timestamp_url(video_url: str, seconds: int) -> str:
    if seconds <= 0:
        return video_url
    separator = "&" if "?" in video_url else "?"
    return f"{video_url}{separator}t={seconds}s"

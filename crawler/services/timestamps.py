from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable
from urllib.parse import parse_qs, urlparse

from crawler.models import TimestampSource

Logger = Callable[[str], None]
HMS_PATTERN = r"(?:\d{1,2}):[0-5]\d:[0-5]\d"
LINE_END_PAREN_TS_PATTERN = re.compile(rf"^(?P<label>.*?)[\(（]\s*(?P<ts>{HMS_PATTERN})\s*[\)）]\s*$")
LEADING_TS_PATTERN = re.compile(rf"^(?P<ts>{HMS_PATTERN})\s*(?P<label>.*)$")
TREE_PREFIX_PATTERN = re.compile(r"^\s*(?P<prefix>[├┝└┗┣┠┡┢│┃\s]+)?(?P<body>.*)$")
NOISE_PATTERN = re.compile(r"^[\s\-:：|／/、。・･]+|[\s\-:：|／/、。・･]+$")
GENERIC_SHORT_WORDS = {"ここ", "好き", "最高", "神", "草", "笑", "www", "やばい", "神回"}


@dataclass
class ParsedTimestampEntry:
    video_id: str
    start_seconds: int
    timestamp_text: str
    title: str
    kind: str
    source_comment_id: str
    source_parent_comment_id: str
    is_reply: bool
    published_at: str
    author_channel_id: str
    is_video_owner: bool
    is_pinned: bool | None
    source_type: str


@dataclass
class GroupedTimeline:
    heading: ParsedTimestampEntry
    children: list[ParsedTimestampEntry] = field(default_factory=list)


def build_timestamp_rows(
    video_url: str,
    description: str = "",
    timestamp_sources: list[TimestampSource] | None = None,
    fallback_text: str = "",
    log: Logger | None = None,
) -> list[tuple[str, str, str, str]]:
    sources = _build_effective_sources(description, timestamp_sources, fallback_text)
    comment_sources = [src for src in sources if src.source_type in {"top", "reply"}]
    comment_sources.sort(key=lambda src: ((src.published_at or "9999-99-99T99:99:99Z"), src.source_id))

    checked_top_count = sum(1 for src in comment_sources if src.source_type == "top")
    checked_reply_count = sum(1 for src in comment_sources if src.source_type == "reply")

    per_comment_entries: dict[str, list[ParsedTimestampEntry]] = {}
    for src in comment_sources:
        entries = _extract_entries_from_source(src, video_url)
        per_comment_entries[src.source_id] = entries

    extracted_count = sum(len(items) for items in per_comment_entries.values())

    primary_source_ids: set[str] = set()
    rejected_single_timestamp_comments_count = 0
    for src in comment_sources:
        entries = per_comment_entries.get(src.source_id, [])
        count = len(entries)
        if count <= 0:
            continue
        if src.is_video_owner or src.is_pinned is True:
            primary_source_ids.add(src.source_id)
            continue
        if count >= 2:
            primary_source_ids.add(src.source_id)
            continue
        rejected_single_timestamp_comments_count += 1

    has_primary = bool(primary_source_ids)
    selected: list[ParsedTimestampEntry] = []

    for src in comment_sources:
        entries = per_comment_entries.get(src.source_id, [])
        if not entries:
            continue

        if src.source_id in primary_source_ids:
            selected.extend(entries)
            continue

        if not has_primary:
            continue

        if len(entries) == 1 and _is_valid_single_supplement(entries[0]):
            selected.extend(entries)

    deduped = _dedupe_entries(selected)
    deduped.sort(key=lambda ent: ent.start_seconds)

    grouped = _group_entries(deduped)

    _log(log, f"checked top-level comments count: {checked_top_count}")
    _log(log, f"checked reply comments count: {checked_reply_count}")
    _log(log, f"extracted timestamp entries count: {extracted_count}")
    _log(log, f"deduped timestamp entries count: {len(deduped)}")
    _log(log, f"generated headings count: {len(grouped)}")
    _log(log, f"generated children count: {sum(len(g.children) for g in grouped)}")
    _log(log, f"rejected single timestamp comments count: {rejected_single_timestamp_comments_count}")

    rows: list[tuple[str, str, str, str]] = []
    for group in grouped:
        major_label = group.heading.title or _format_time(group.heading.start_seconds)
        major_url = _build_timestamp_url(video_url, group.heading.start_seconds)
        if not group.children:
            rows.append((major_label, major_url, "", ""))
            continue

        for child in group.children:
            rows.append(
                (
                    major_label,
                    major_url,
                    child.title or _format_time(child.start_seconds),
                    _build_timestamp_url(video_url, child.start_seconds),
                )
            )

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
        items.append(src)

    if description.strip():
        items.append(TimestampSource(source_type="description", text=description.strip()))

    if not items and fallback_text.strip():
        items.append(TimestampSource(source_type="top", text=fallback_text.strip()))

    return items


def _extract_entries_from_source(src: TimestampSource, video_url: str) -> list[ParsedTimestampEntry]:
    video_id = _extract_video_id(video_url)
    entries: list[ParsedTimestampEntry] = []
    for line in src.text.splitlines():
        parsed = _parse_line(line)
        if not parsed:
            continue
        timestamp_text, seconds, title, kind = parsed
        entries.append(
            ParsedTimestampEntry(
                video_id=video_id,
                start_seconds=seconds,
                timestamp_text=timestamp_text,
                title=title,
                kind=kind,
                source_comment_id=src.source_id,
                source_parent_comment_id=src.parent_id,
                is_reply=src.is_reply,
                published_at=src.published_at,
                author_channel_id=src.author_channel_id,
                is_video_owner=src.is_video_owner,
                is_pinned=src.is_pinned,
                source_type=src.source_type,
            )
        )
    return entries


def _parse_line(line: str) -> tuple[str, int, str, str] | None:
    normalized = _normalize_line(line)
    if not normalized:
        return None

    prefix_match = TREE_PREFIX_PATTERN.match(normalized)
    body = (prefix_match.group("body") if prefix_match else normalized).strip()
    prefix = (prefix_match.group("prefix") if prefix_match else "") or ""
    has_tree_prefix = any(ch in "├┝└┗┣┠┡┢│┃" for ch in prefix)
    kind = "child" if has_tree_prefix else "heading"

    ts = ""
    label_text = ""
    lead_match = LEADING_TS_PATTERN.match(body)
    if lead_match:
        ts = (lead_match.group("ts") or "").strip()
        label_text = (lead_match.group("label") or "").strip()
    else:
        end_match = LINE_END_PAREN_TS_PATTERN.match(body)
        if not end_match:
            return None
        ts = (end_match.group("ts") or "").strip()
        label_text = (end_match.group("label") or "").strip()

    seconds = _timestamp_to_seconds(ts)
    if seconds < 0:
        return None

    title = _clean_label(label_text)
    if not title:
        return None

    return (ts, seconds, title, kind)


def _dedupe_entries(entries: list[ParsedTimestampEntry]) -> list[ParsedTimestampEntry]:
    deduped: dict[tuple[str, int, str, str], ParsedTimestampEntry] = {}
    for entry in entries:
        normalized_title = _normalize_title(entry.title)
        if not normalized_title:
            continue
        key = (entry.video_id, entry.start_seconds, normalized_title, entry.kind)
        existing = deduped.get(key)
        if not existing:
            deduped[key] = entry
            continue
        if (entry.published_at or "") < (existing.published_at or ""):
            deduped[key] = entry
    return list(deduped.values())


def _group_entries(entries: list[ParsedTimestampEntry]) -> list[GroupedTimeline]:
    groups: list[GroupedTimeline] = []
    latest_heading: GroupedTimeline | None = None

    for entry in entries:
        if entry.kind == "heading":
            latest_heading = GroupedTimeline(heading=entry)
            groups.append(latest_heading)
            continue

        if latest_heading is None:
            continue

        if latest_heading.heading.start_seconds <= entry.start_seconds:
            latest_heading.children.append(entry)

    for group in groups:
        group.children.sort(key=lambda item: item.start_seconds)

    return groups


def _is_valid_single_supplement(entry: ParsedTimestampEntry) -> bool:
    title = _normalize_title(entry.title)
    if not title:
        return False
    if title in {")", "）", "()", "（）"}:
        return False
    if title in GENERIC_SHORT_WORDS:
        return False
    if re.fullmatch(rf"{HMS_PATTERN}", entry.title):
        return False
    if len(title) <= 1:
        return False
    return True


def _normalize_title(title: str) -> str:
    value = (title or "").replace("\t", " ").replace("\u3000", " ").strip()
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"[()（）]", "", value)
    value = value.strip()
    if value in {"", ")", "）", "()", "（）"}:
        return ""
    return value.lower()


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
    if value in {")", "）", "(", "（", "()", "（）"}:
        return ""
    return value


def _timestamp_to_seconds(ts: str) -> int:
    parts = ts.strip().split(":")
    if len(parts) != 3:
        return -1
    try:
        h, m, s = [int(x) for x in parts]
    except ValueError:
        return -1
    if m < 0 or m >= 60 or s < 0 or s >= 60:
        return -1
    return h * 3600 + m * 60 + s


def _format_time(seconds: int) -> str:
    h = seconds // 3600
    rem = seconds % 3600
    m = rem // 60
    s = rem % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def _build_timestamp_url(video_url: str, seconds: int) -> str:
    if seconds <= 0:
        return video_url
    sep = "&" if "?" in video_url else "?"
    return f"{video_url}{sep}t={seconds}s"


def _extract_video_id(video_url: str) -> str:
    text = (video_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    if parsed.hostname == "youtu.be":
        return parsed.path.strip("/")
    if parsed.query:
        v = parse_qs(parsed.query).get("v", [""])[0].strip()
        if v:
            return v
    return ""


def _log(log: Logger | None, message: str) -> None:
    if log:
        log(message)

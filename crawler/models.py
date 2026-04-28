from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


TimestampSourceType = Literal["description", "top", "reply"]


@dataclass
class TimestampSource:
    source_type: TimestampSourceType
    text: str
    like_count: int = 0
    timestamp_count: int = 0
    source_id: str = ""
    parent_id: str = ""
    author: str = ""
    published_at: str = ""
    author_channel_id: str = ""
    is_video_owner: bool = False
    is_reply: bool = False
    is_pinned: bool | None = None


@dataclass
class VideoItem:
    video_id: str
    title: str
    url: str
    published_at: str
    thumbnail_url: str
    tags: list[str] = field(default_factory=list)
    timestamp_comment: str = ""
    description: str = ""
    timestamp_sources: list[TimestampSource] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

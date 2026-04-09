from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class VideoItem:
    video_id: str
    title: str
    url: str
    published_at: str
    thumbnail_url: str
    tags: list[str] = field(default_factory=list)
    timestamp_comment: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

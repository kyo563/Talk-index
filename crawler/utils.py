from __future__ import annotations

import io
import json
from urllib.parse import parse_qs, urlparse

import pandas as pd


CHANNEL_ID_PREFIX = "UC"


def looks_like_channel_id(value: str) -> bool:
    value = value.strip()
    return value.startswith(CHANNEL_ID_PREFIX) and len(value) >= 24


def extract_channel_hint(input_text: str) -> str:
    value = input_text.strip()
    if not value:
        return ""

    if looks_like_channel_id(value):
        return value

    if value.startswith("@"):
        return value

    if not value.startswith("http://") and not value.startswith("https://"):
        return value

    parsed = urlparse(value)
    path_parts = [p for p in parsed.path.split("/") if p]

    if "channel" in path_parts:
        idx = path_parts.index("channel")
        if idx + 1 < len(path_parts):
            return path_parts[idx + 1]

    if path_parts:
        first = path_parts[0]
        if first.startswith("@"):
            return first

    query = parse_qs(parsed.query)
    if "channel_id" in query and query["channel_id"]:
        return query["channel_id"][0]

    return value


def to_json_bytes(rows: list[dict]) -> bytes:
    return json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8")


def to_csv_bytes(rows: list[dict]) -> bytes:
    output = io.StringIO()
    pd.DataFrame(rows).to_csv(output, index=False)
    return output.getvalue().encode("utf-8")

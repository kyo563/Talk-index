from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    youtube_api_key: str
    default_max_results: int = 20
    max_allowed_results: int = 200


def get_settings() -> Settings:
    return Settings(
        youtube_api_key=os.getenv("YOUTUBE_API_KEY", "").strip(),
        default_max_results=int(os.getenv("DEFAULT_MAX_RESULTS", "20")),
        max_allowed_results=int(os.getenv("MAX_ALLOWED_RESULTS", "200")),
    )

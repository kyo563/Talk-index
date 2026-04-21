from __future__ import annotations

import json
import os

from crawler.services.favorites_mirror import build_r2_client, sync_favorites_exports_to_spreadsheet
from crawler.services.spreadsheet import build_gspread_client


def main() -> None:
    spreadsheet_id = os.getenv("FAVORITES_MIRROR_SPREADSHEET_ID", "").strip() or os.getenv("SPREADSHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("FAVORITES_MIRROR_SPREADSHEET_ID または SPREADSHEET_ID が必要です。")

    gc = build_gspread_client()
    s3, bucket = build_r2_client()
    public_base_url = os.getenv("FAVORITES_PUBLIC_BASE_URL", "").strip()

    result = sync_favorites_exports_to_spreadsheet(
        gc=gc,
        spreadsheet_id=spreadsheet_id,
        s3=s3,
        bucket=bucket,
        public_base_url=public_base_url,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

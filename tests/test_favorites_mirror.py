import unittest
from datetime import UTC, datetime

from crawler.services.favorites_mirror import (
    FAVORITES_SHEET_HEADERS,
    build_heading_video_title_map,
    build_sheet_rows_from_items,
    previous_week_key_jst,
    upsert_daily_snapshot_rows,
)
from exporter import favorites_r2_to_sheet


class FakeWorksheet:
    def __init__(self):
        self.values: list[list[str]] = []

    def get_all_values(self):
        return [row[:] for row in self.values]

    def update(self, a1, values, value_input_option="RAW"):
        if a1 == "A1" and values:
            if len(values) == 1 and values[0] == FAVORITES_SHEET_HEADERS:
                if not self.values:
                    self.values = [FAVORITES_SHEET_HEADERS[:]]
                else:
                    self.values[0] = FAVORITES_SHEET_HEADERS[:]
                return
            self.values = [row[:] for row in values]
            return

        if not self.values:
            self.values = [FAVORITES_SHEET_HEADERS[:]]
        row_no = int(a1[1:].split(":")[0])
        while len(self.values) < row_no:
            self.values.append([])
        self.values[row_no - 1] = values[0][:]

    def clear(self):
        self.values = []

    def batch_update(self, updates, value_input_option="RAW"):
        for item in updates:
            self.update(item["range"], item["values"], value_input_option=value_input_option)

    def append_rows(self, rows, value_input_option="RAW"):
        if not self.values:
            self.values = [FAVORITES_SHEET_HEADERS[:]]
        self.values.extend([row[:] for row in rows])


class FakeBook:
    def __init__(self, sheet):
        self.sheet = sheet

    def worksheet(self, _name):
        return self.sheet


class FakeClient:
    def __init__(self, sheet):
        self.sheet = sheet

    def open_by_key(self, _key):
        return FakeBook(self.sheet)


class FavoritesMirrorTests(unittest.TestCase):
    def test_build_rows_with_source_video_title_resolution(self):
        talks_payload = {
            "talks": [
                {
                    "key": "h1",
                    "name": "見出し1",
                    "subsections": [{"videoTitle": "元動画A"}],
                }
            ]
        }
        title_map = build_heading_video_title_map(talks_payload)
        payload = {
            "generatedAt": "2026-04-21T00:00:00Z",
            "items": [
                {
                    "headingId": "h1",
                    "headingTitle": "見出し1",
                    "videoId": "v1",
                    "voteCount": 3,
                    "firstVotedAt": "2026-04-20T00:00:00Z",
                    "lastVotedAt": "2026-04-21T00:00:00Z",
                }
            ],
        }

        rows = build_sheet_rows_from_items(
            payload=payload,
            aggregate_type="current_ranking",
            source_json_url="favorites/exports/current_ranking.json",
            heading_title_map=title_map,
        )

        self.assertEqual(rows[0][5], "元動画A")
        self.assertEqual(rows[0][10], "current_ranking")

    def test_daily_upsert_key_snapshot_date_plus_heading_id(self):
        sheet = FakeWorksheet()
        sheet.values = [
            FAVORITES_SHEET_HEADERS[:],
            ["2026-04-20", "", "h1", "old", "v1", "", "1", "1", "", "", "daily_snapshot", "", "", ""],
        ]
        client = FakeClient(sheet)

        rows = [
            ["2026-04-20", "", "h1", "new", "v1", "", "2", "1", "", "", "daily_snapshot", "", "", ""],
            ["2026-04-21", "", "h2", "add", "v2", "", "1", "1", "", "", "daily_snapshot", "", "", ""],
        ]

        updated, appended = upsert_daily_snapshot_rows(
            client=client,
            spreadsheet_id="dummy",
            worksheet_name="favorites_daily_snapshots",
            rows=rows,
        )

        self.assertEqual((updated, appended), (1, 1))
        self.assertEqual(sheet.values[1][3], "new")
        self.assertEqual(sheet.values[2][2], "h2")

    def test_recent_recommendations_uses_previous_week_when_missing(self):
        week_key = previous_week_key_jst(datetime(2026, 4, 21, tzinfo=UTC))
        rows = build_sheet_rows_from_items(
            payload={"generatedAt": "2026-04-21T00:00:00Z", "items": [{"headingId": "h1", "voteCount": 1}]},
            aggregate_type="recent_recommendations",
            source_json_url="favorites/aggregates/recent_recommendations.json",
            heading_title_map={},
            default_week_key=week_key,
        )
        self.assertEqual(rows[0][1], "2026-04-13")

    def test_required_json_fetch_error_has_clear_message(self):
        class S3Fail:
            def get_object(self, Bucket, Key):
                raise RuntimeError("boom")

        with self.assertRaises(RuntimeError) as ctx:
            favorites_r2_to_sheet._load_json_required(S3Fail(), "bucket", "favorites/exports/current_ranking.json")
        self.assertIn("favorites/exports/current_ranking.json", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

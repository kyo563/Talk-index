import unittest
from datetime import UTC, datetime

from crawler.services.favorites_mirror import (
    FAVORITES_SHEET_HEADERS,
    PUBLIC_FAVORITES_SHEET_HEADERS,
    PUBLIC_FAVORITES_RECENT_RECOMMENDATIONS_SHEET,
    PUBLIC_FAVORITES_RECENT_UPLOAD_RECOMMENDATIONS_SHEET,
    build_heading_video_candidates_map,
    build_heading_video_title_map,
    build_public_sheet_rows_from_items,
    build_sheet_rows_from_items,
    build_video_metadata_map,
    previous_week_key_jst,
    replace_public_sheet_rows,
    upsert_daily_snapshot_rows,
)
from exporter import favorites_r2_to_sheet


class FakeWorksheet:
    def __init__(self, col_count: int = 14):
        self.values: list[list[str]] = []
        self.col_count = col_count

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

    def add_cols(self, cols):
        self.col_count += cols


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
    def test_public_headers_are_japanese(self):
        self.assertEqual(PUBLIC_FAVORITES_SHEET_HEADERS, ["動画投稿日", "動画タイトル", "大見出し", "得票数"])

    def test_public_sheet_names_include_recent_upload(self):
        self.assertEqual(PUBLIC_FAVORITES_RECENT_RECOMMENDATIONS_SHEET, "10日間のおすすめトーク")
        self.assertEqual(PUBLIC_FAVORITES_RECENT_UPLOAD_RECOMMENDATIONS_SHEET, "直近の動画のおすすめ")

    def test_build_video_metadata_map_resolves_from_talks_and_latest(self):
        talks_payload = {
            "talks": [
                {
                    "date": "2026-04-20",
                    "subsections": [
                        {"videoTitle": "talk-title", "videoUrl": "https://www.youtube.com/watch?v=AAAAAAAAAAA"}
                    ],
                }
            ]
        }
        latest_payload = {
            "videos": [
                {
                    "id": "AAAAAAAAAAA",
                    "title": "latest-title",
                    "url": "https://youtu.be/AAAAAAAAAAA",
                    "date": "2026-04-21",
                }
            ]
        }
        meta = build_video_metadata_map(talks_payload, latest_payload)
        self.assertEqual(meta["AAAAAAAAAAA"]["title"], "latest-title")
        self.assertEqual(meta["AAAAAAAAAAA"]["published_date"], "2026-04-21")

    def test_public_rows_include_only_4_columns_with_hyperlink_and_sort(self):
        meta = {
            "BBBBBBBBBBB": {
                "title": '動画"2',
                "url": "https://www.youtube.com/watch?v=BBBBBBBBBBB",
                "published_date": "2026-04-20",
            },
            "AAAAAAAAAAA": {
                "title": "動画1",
                "url": "https://www.youtube.com/watch?v=AAAAAAAAAAA",
                "published_date": "2026-04-21",
            },
        }
        payload = {
            "items": [
                {"videoId": "BBBBBBBBBBB", "headingTitle": "見出しB", "voteCount": 2},
                {"videoId": "AAAAAAAAAAA", "headingTitle": "見出しA", "voteCount": 2},
            ]
        }
        rows = build_public_sheet_rows_from_items(payload=payload, video_metadata_map=meta)
        self.assertEqual(len(rows[0]), 4)
        self.assertEqual(rows[0][0], "2026-04-21")
        self.assertTrue(rows[0][1].startswith('=HYPERLINK("https://www.youtube.com/watch?v=AAAAAAAAAAA"'))
        self.assertIn('動画""2', rows[1][1])

    def test_public_rows_fallback_by_heading_id_when_video_id_missing(self):
        talks_payload = {
            "talks": [
                {
                    "key": "h1",
                    "name": "見出し1",
                    "date": "2026-04-20",
                    "subsections": [{"videoUrl": "https://www.youtube.com/watch?v=AAAAAAAAAAA"}],
                }
            ]
        }
        meta = {
            "AAAAAAAAAAA": {
                "title": "動画A",
                "url": "https://www.youtube.com/watch?v=AAAAAAAAAAA",
                "published_date": "2026-04-20",
            }
        }
        payload = {"items": [{"headingId": "h1", "headingTitle": "見出し1", "voteCount": 1}]}

        rows = build_public_sheet_rows_from_items(
            payload=payload,
            video_metadata_map=meta,
            heading_video_candidates_map=build_heading_video_candidates_map(talks_payload),
        )
        self.assertEqual(rows[0][0], "2026-04-20")
        self.assertTrue(rows[0][1].startswith('=HYPERLINK("https://www.youtube.com/watch?v=AAAAAAAAAAA"'))

    def test_public_rows_ambiguous_heading_does_not_link(self):
        talks_payload = {
            "talks": [
                {
                    "key": "h1",
                    "name": "見出し1",
                    "subsections": [
                        {"videoUrl": "https://www.youtube.com/watch?v=AAAAAAAAAAA"},
                        {"videoUrl": "https://www.youtube.com/watch?v=BBBBBBBBBBB"},
                    ],
                }
            ]
        }
        meta = {
            "AAAAAAAAAAA": {"title": "動画A", "url": "https://www.youtube.com/watch?v=AAAAAAAAAAA", "published_date": "2026-04-20"},
            "BBBBBBBBBBB": {"title": "動画B", "url": "https://www.youtube.com/watch?v=BBBBBBBBBBB", "published_date": "2026-04-21"},
        }
        payload = {"items": [{"headingId": "h1", "headingTitle": "見出し1", "videoTitle": "候補あり", "voteCount": 1}]}

        rows = build_public_sheet_rows_from_items(
            payload=payload,
            video_metadata_map=meta,
            heading_video_candidates_map=build_heading_video_candidates_map(talks_payload),
        )
        self.assertEqual(rows[0][0], "")
        self.assertEqual(rows[0][1], "候補あり")

    def test_replace_public_sheet_rows_uses_user_entered(self):
        class PublicSheet(FakeWorksheet):
            def __init__(self):
                super().__init__()
                self.last_value_input_option = ""

            def update(self, a1, values, value_input_option="RAW"):
                self.last_value_input_option = value_input_option
                super().update(a1, values, value_input_option=value_input_option)

        sheet = PublicSheet()
        client = FakeClient(sheet)
        replace_public_sheet_rows(
            client=client,
            spreadsheet_id="dummy",
            worksheet_name="殿堂入りトーク",
            rows=[["2026-04-21", '=HYPERLINK("https://example.com","タイトル")', "見出し", "3"]],
        )
        self.assertEqual(sheet.last_value_input_option, "USER_ENTERED")
        self.assertEqual(sheet.values[0], PUBLIC_FAVORITES_SHEET_HEADERS)
        self.assertEqual(len(sheet.values[1]), 4)

    def test_build_rows_with_source_video_title_resolution_by_video_id(self):
        talks_payload = {
            "talks": [
                {
                    "key": "h1",
                    "name": "見出し1",
                    "subsections": [
                        {"videoTitle": "元動画A", "videoUrl": "https://www.youtube.com/watch?v=AAAAAAAAAAA"},
                        {"videoTitle": "元動画B", "videoUrl": "https://www.youtube.com/watch?v=BBBBBBBBBBB"},
                    ],
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
                    "videoId": "BBBBBBBBBBB",
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

        self.assertEqual(rows[0][5], "元動画B")
        self.assertEqual(rows[0][10], "current_ranking")

    def test_build_rows_with_source_video_title_resolution_unique_heading_fallback(self):
        talks_payload = {
            "talks": [
                {
                    "key": "h1",
                    "name": "見出し1",
                    "subsections": [{"videoTitle": "元動画A", "videoUrl": ""}],
                }
            ]
        }
        title_map = build_heading_video_title_map(talks_payload)
        rows = build_sheet_rows_from_items(
            payload={"generatedAt": "2026-04-21T00:00:00Z", "items": [{"headingId": "h1", "voteCount": 1}]},
            aggregate_type="current_ranking",
            source_json_url="favorites/exports/current_ranking.json",
            heading_title_map=title_map,
        )
        self.assertEqual(rows[0][5], "元動画A")

    def test_build_rows_with_source_video_title_resolution_ambiguous_heading_returns_blank(self):
        talks_payload = {
            "talks": [
                {
                    "key": "h1",
                    "name": "見出し1",
                    "subsections": [{"videoTitle": "元動画A", "videoUrl": ""}, {"videoTitle": "元動画B", "videoUrl": ""}],
                }
            ]
        }
        title_map = build_heading_video_title_map(talks_payload)
        rows = build_sheet_rows_from_items(
            payload={"generatedAt": "2026-04-21T00:00:00Z", "items": [{"headingId": "h1", "voteCount": 1}]},
            aggregate_type="current_ranking",
            source_json_url="favorites/exports/current_ranking.json",
            heading_title_map=title_map,
        )
        self.assertEqual(rows[0][5], "")

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

    def test_daily_upsert_ensures_required_columns_for_new_favorites_sheet(self):
        sheet = FakeWorksheet(col_count=12)
        client = FakeClient(sheet)
        rows = [["2026-04-21", "", "h2", "add", "v2", "", "1", "1", "", "", "daily_snapshot", "", "", ""]]

        upsert_daily_snapshot_rows(
            client=client,
            spreadsheet_id="dummy",
            worksheet_name="favorites_daily_snapshots",
            rows=rows,
        )

        self.assertGreaterEqual(sheet.col_count, len(FAVORITES_SHEET_HEADERS))

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

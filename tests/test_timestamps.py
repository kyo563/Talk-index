import unittest

from crawler.models import TimestampSource
from crawler.services.timestamps import build_timestamp_rows


class TimestampTests(unittest.TestCase):
    def test_top_level_with_heading_and_children(self):
        source = TimestampSource(
            source_type="top",
            source_id="c1",
            published_at="2026-01-01T00:00:00Z",
            text="\n".join([
                "00:00:10 大見出しA",
                "├00:00:20 小見出しA-1",
                "└補足A (00:00:30)",
            ]),
        )
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=[source])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "大見出しA")
        self.assertEqual(rows[0][2], "小見出しA-1")

    def test_reply_child_is_reflected(self):
        sources = [
            TimestampSource(
                source_type="top",
                source_id="c1",
                published_at="2026-01-01T00:00:00Z",
                text="00:00:10 大見出しA\n00:01:00 大見出しB",
            ),
            TimestampSource(
                source_type="reply",
                source_id="r1",
                parent_id="c1",
                is_reply=True,
                published_at="2026-01-01T00:00:10Z",
                text="├補足B (00:01:10)",
            ),
        ]
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=sources)
        self.assertIn("補足B", [row[2] for row in rows])

    def test_single_supplement_from_other_comment_is_reflected(self):
        sources = [
            TimestampSource(
                source_type="top",
                source_id="main",
                published_at="2026-01-01T00:00:00Z",
                text="00:00:10 大見出しA\n00:01:00 大見出しB",
            ),
            TimestampSource(
                source_type="top",
                source_id="extra",
                published_at="2026-01-01T00:00:05Z",
                text="├補足B (00:01:20)",
            ),
        ]
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=sources)
        self.assertIn("補足B", [row[2] for row in rows])

    def test_no_primary_no_timeline_from_single_comment_only(self):
        sources = [
            TimestampSource(
                source_type="top",
                source_id="single",
                published_at="2026-01-01T00:00:00Z",
                text="00:01:20 ここ好き",
            ),
        ]
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=sources)
        self.assertEqual(rows, [])

    def test_video_owner_single_timestamp_is_accepted(self):
        sources = [
            TimestampSource(
                source_type="top",
                source_id="owner",
                published_at="2026-01-01T00:00:00Z",
                is_video_owner=True,
                text="00:00:20 投稿者コメント",
            ),
        ]
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=sources)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "投稿者コメント")

    def test_html_order_is_by_start_seconds_not_comment_order(self):
        sources = [
            TimestampSource(
                source_type="top",
                source_id="late",
                published_at="2026-01-01T00:10:00Z",
                text="00:10:00 大見出しC\n00:20:00 大見出しD",
            ),
            TimestampSource(
                source_type="top",
                source_id="early",
                published_at="2026-01-01T00:00:00Z",
                text="00:00:10 大見出しA\n00:01:00 大見出しB",
            ),
        ]
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=sources)
        self.assertEqual([row[0] for row in rows], ["大見出しA", "大見出しB", "大見出しC", "大見出しD"])

    def test_duplicate_timestamp_and_title_is_deduped(self):
        sources = [
            TimestampSource(
                source_type="top",
                source_id="c1",
                published_at="2026-01-01T00:00:00Z",
                text="00:10:00 同じ見出し\n00:20:00 大見出しB",
            ),
            TimestampSource(
                source_type="reply",
                source_id="r1",
                parent_id="c1",
                is_reply=True,
                published_at="2026-01-01T00:00:01Z",
                text="00:10:00 同じ見出し",
            ),
        ]
        rows = build_timestamp_rows("https://www.youtube.com/watch?v=abc123def45", timestamp_sources=sources)
        self.assertEqual([row[0] for row in rows].count("同じ見出し"), 1)


if __name__ == "__main__":
    unittest.main()

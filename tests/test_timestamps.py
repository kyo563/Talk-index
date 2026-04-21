import unittest

from crawler.models import TimestampSource
from crawler.services.timestamps import build_timestamp_rows


class TimestampTests(unittest.TestCase):
    def test_hhmmss_major_three_lines(self):
        text = "\n".join([
            "0:12:34 大見出しA",
            "1:23:45 大見出しB",
            "2:10:00 大見出しC",
        ])
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            timestamp_sources=[TimestampSource(source_type="description", text=text)],
        )
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0][0], "大見出しA")
        self.assertEqual(rows[1][0], "大見出しB")
        self.assertEqual(rows[2][0], "大見出しC")

    def test_line_end_parenthesis_minor(self):
        text = "\n".join([
            "0:00:30 大見出し",
            "小見出し情報(1:23)",
            "小見出し情報（1:23）",
        ])
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            timestamp_sources=[TimestampSource(source_type="description", text=text)],
        )
        minors = [r[2] for r in rows if r[2]]
        self.assertEqual(minors.count("小見出し情報"), 1)

    def test_mmss_major_and_minor_grouping_from_comment(self):
        comment = "\n".join([
            "00:00 大見出し①",
            "├0:10 小見出し1",
            "├0:20 小見出し2",
            "30:00 大見出し②",
            "├30:10 小見出し2-1",
            "├30:20 小見出し2-2",
        ])
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            timestamp_sources=[TimestampSource(source_type="top", text=comment)],
        )

        self.assertIn(("大見出し①", "https://www.youtube.com/watch?v=abc123def45", "小見出し1", "https://www.youtube.com/watch?v=abc123def45&t=10s"), rows)
        self.assertIn(("大見出し②", "https://www.youtube.com/watch?v=abc123def45&t=1800s", "小見出し2-1", "https://www.youtube.com/watch?v=abc123def45&t=1810s"), rows)
        majors_for_late_minors = {row[0] for row in rows if row[2] in {"小見出し2-1", "小見出し2-2"}}
        self.assertEqual(majors_for_late_minors, {"大見出し②"})

    def test_comment_priority_and_description_complement(self):
        comment = "\n".join(["00:00 大見出し①", "30:00 大見出し②"])
        description = "\n".join(["0:00 オープニング", "15:00 中間チャプター", "30:00 後半"])
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            description=description,
            timestamp_sources=[TimestampSource(source_type="top", text=comment)],
        )

        major_labels = [row[0] for row in rows]
        self.assertIn("大見出し①", major_labels)
        self.assertIn("大見出し②", major_labels)
        self.assertIn("中間チャプター", major_labels)
        self.assertNotIn("オープニング", major_labels)
        self.assertNotIn("後半", major_labels)

    def test_mmss_major_are_separated(self):
        comment = "\n".join(["05:00 トークA", "10:00 トークB", "15:00 トークC"])
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            timestamp_sources=[TimestampSource(source_type="top", text=comment)],
        )
        self.assertEqual([row[0] for row in rows], ["トークA", "トークB", "トークC"])

    def test_same_label_far_apart_can_coexist(self):
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            description="",
            timestamp_sources=[
                TimestampSource(source_type="top", text="05:00 雑談"),
                TimestampSource(source_type="reply", text="45:00 雑談"),
            ],
        )
        self.assertEqual([row[0] for row in rows], ["雑談", "雑談"])
        self.assertEqual(
            [row[1] for row in rows],
            [
                "https://www.youtube.com/watch?v=abc123def45&t=300s",
                "https://www.youtube.com/watch?v=abc123def45&t=2700s",
            ],
        )

    def test_duplicate_within_ten_seconds_keeps_top(self):
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            description="20:09 後半開始",
            timestamp_sources=[
                TimestampSource(source_type="top", text="20:00 本題"),
                TimestampSource(source_type="reply", text="20:07 メイントーク"),
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "本題")
        self.assertEqual(rows[0][1], "https://www.youtube.com/watch?v=abc123def45&t=1200s")

    def test_duplicate_at_exactly_ten_seconds_keeps_top(self):
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            timestamp_sources=[
                TimestampSource(source_type="top", text="20:00 本題"),
                TimestampSource(source_type="reply", text="20:10 補足"),
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "本題")
        self.assertEqual(rows[0][1], "https://www.youtube.com/watch?v=abc123def45&t=1200s")

    def test_same_source_within_ten_seconds_can_coexist(self):
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            timestamp_sources=[
                TimestampSource(source_type="top", text="10:00 話題A\n10:06 話題B"),
            ],
        )
        self.assertEqual([row[0] for row in rows], ["話題A", "話題B"])

    def test_over_ten_seconds_can_coexist(self):
        rows = build_timestamp_rows(
            video_url="https://www.youtube.com/watch?v=abc123def45",
            description="40:30 話題C",
            timestamp_sources=[
                TimestampSource(source_type="top", text="40:00 話題A"),
                TimestampSource(source_type="reply", text="40:12 話題B"),
            ],
        )
        self.assertEqual([row[0] for row in rows], ["話題A", "話題B", "話題C"])


if __name__ == "__main__":
    unittest.main()

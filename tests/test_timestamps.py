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


if __name__ == "__main__":
    unittest.main()

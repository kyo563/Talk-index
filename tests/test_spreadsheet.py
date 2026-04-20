import unittest

from crawler.models import VideoItem
from crawler.services import spreadsheet


class FakeWorksheet:
    def __init__(self):
        self.grid = {}
        self.appended_rows = []
        self.cleared_ranges = []

    def _set_cell(self, row: int, col: int, value: str):
        if value == "":
            self.grid.pop((row, col), None)
        else:
            self.grid[(row, col)] = value

    def _get_cell(self, row: int, col: int) -> str:
        return self.grid.get((row, col), "")

    def _parse_a1(self, a1: str):
        left, right = a1.split(":") if ":" in a1 else (a1, a1)

        def parse_cell(cell: str):
            letters = ""
            digits = ""
            for ch in cell:
                if ch.isalpha():
                    letters += ch
                elif ch.isdigit():
                    digits += ch
            col = ord(letters.upper()) - ord("A") + 1
            row = int(digits)
            return row, col

        r1, c1 = parse_cell(left)
        r2, c2 = parse_cell(right)
        return r1, c1, r2, c2

    def get(self, a1: str):
        r1, c1, r2, c2 = self._parse_a1(a1)
        out = []
        for r in range(r1, r2 + 1):
            row = []
            for c in range(c1, c2 + 1):
                row.append(self._get_cell(r, c))
            out.append(row)
        return out

    def update(self, a1: str, values, value_input_option="RAW"):
        r1, c1, _, _ = self._parse_a1(a1)
        for r_off, row_values in enumerate(values):
            for c_off, value in enumerate(row_values):
                self._set_cell(r1 + r_off, c1 + c_off, value)

    def append_row(self, values, value_input_option="RAW"):
        self.append_rows([values], value_input_option=value_input_option)

    def append_rows(self, rows, value_input_option="RAW"):
        start = len(self.get_all_values()) + 1
        if start < 1:
            start = 1
        for r_off, row_values in enumerate(rows):
            for c_off, value in enumerate(row_values):
                self._set_cell(start + r_off, c_off + 1, value)
        self.appended_rows.extend(rows)

    def get_all_values(self):
        if not self.grid:
            return []
        max_row = max(r for r, _ in self.grid.keys())
        max_col = max(c for _, c in self.grid.keys())
        values = []
        for r in range(1, max_row + 1):
            row = [self._get_cell(r, c) for c in range(1, max_col + 1)]
            while row and row[-1] == "":
                row.pop()
            values.append(row)
        while values and not values[-1]:
            values.pop()
        return values

    def col_values(self, col):
        values = self.get_all_values()
        out = []
        for row in values:
            out.append(row[col - 1] if len(row) >= col else "")
        return out

    def batch_clear(self, ranges):
        self.cleared_ranges.extend(ranges)

    def batch_update(self, updates, value_input_option="RAW"):
        for item in updates:
            self.update(item["range"], item["values"], value_input_option=value_input_option)


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


class SpreadsheetTests(unittest.TestCase):
    def _video(self, vid: str, title: str = "title") -> VideoItem:
        return VideoItem(
            video_id=vid,
            title=title,
            url=f"https://www.youtube.com/watch?v={vid}",
            published_at="2026-04-19T00:00:00Z",
            thumbnail_url="",
        )

    def test_title_list_header_init_with_state_cells(self):
        sheet = FakeWorksheet()
        sheet.update("F1:G3", [["key", "value"], ["refresh_cursor", "0"], ["updated_at", ""]])
        client = FakeClient(sheet)

        count = spreadsheet.append_title_list_rows(
            client=client,
            spreadsheet_id="dummy",
            worksheet_name="タイトルリスト",
            videos=[self._video("abc123def45", "new title")],
        )

        self.assertEqual(count, 1)
        self.assertEqual(sheet.get("A1:C1")[0], ["タイトル", "日付", "URL"])
        self.assertEqual(sheet.get("A2:C2")[0][0], "new title")
        self.assertEqual(sheet.get("F1:G3")[0], ["key", "value"])

    def test_upsert_videos_no_sheet_clear(self):
        sheet = FakeWorksheet()
        sheet.update("A1:H2", [["タイトル", "日付", "URL", "大見出し", "大見出しURL", "小見出し", "小見出しURL", "自動検出タグ"], ["old", "2026-04-01", "https://www.youtube.com/watch?v=abc123def45", "M", "u", "", "", ""]])
        client = FakeClient(sheet)

        count = spreadsheet.upsert_videos_by_video_id(
            client=client,
            spreadsheet_id="dummy",
            worksheet_name="索引",
            videos=[self._video("abc123def45", "new")],
        )

        self.assertGreaterEqual(count, 1)
        self.assertIn("A2:H2", sheet.cleared_ranges)

    def test_upsert_title_list_rows_updates_existing(self):
        sheet = FakeWorksheet()
        sheet.update("A1:C2", [["タイトル", "日付", "URL"], ["old", "2026-04-01", "https://www.youtube.com/watch?v=abc123def45"]])
        client = FakeClient(sheet)

        updated, appended = spreadsheet.upsert_title_list_rows(
            client=client,
            spreadsheet_id="dummy",
            worksheet_name="タイトルリスト",
            videos=[self._video("abc123def45", "new")],
        )

        self.assertEqual((updated, appended), (1, 0))
        self.assertEqual(sheet.get("A2:C2")[0][0], "new")


if __name__ == "__main__":
    unittest.main()

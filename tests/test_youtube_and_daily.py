import unittest
from datetime import datetime, timedelta, timezone

from crawler.jobs.daily_crawl import _select_recheck_ids
from crawler.models import VideoItem
from crawler.services.youtube import fetch_timestamp_sources


class _Exec:
    def __init__(self, payload):
        self.payload = payload

    def execute(self):
        return self.payload


class _CommentThreadsAPI:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def list(self, **kwargs):
        token = kwargs.get("pageToken")
        self.calls.append(kwargs)
        payload = self.pages.get(token, {"items": []})
        return _Exec(payload)


class _RepliesAPI:
    def list(self, **kwargs):
        return _Exec({"items": []})


class _YoutubeMock:
    def __init__(self, pages):
        self._threads = _CommentThreadsAPI(pages)
        self._replies = _RepliesAPI()

    def commentThreads(self):
        return self._threads

    def comments(self):
        return self._replies


class YoutubeAndDailyTests(unittest.TestCase):
    def test_comment_threads_multi_page(self):
        pages = {
            None: {
                "items": [
                    {
                        "id": "t1",
                        "snippet": {
                            "topLevelComment": {
                                "id": "c1",
                                "snippet": {"textOriginal": "00:10 foo", "likeCount": 1},
                            },
                            "totalReplyCount": 0,
                        },
                    }
                ],
                "nextPageToken": "p2",
            },
            "p2": {
                "items": [
                    {
                        "id": "t2",
                        "snippet": {
                            "topLevelComment": {
                                "id": "c2",
                                "snippet": {"textOriginal": "00:20 bar", "likeCount": 1},
                            },
                            "totalReplyCount": 0,
                        },
                    }
                ]
            },
        }
        youtube = _YoutubeMock(pages)

        sources = fetch_timestamp_sources(youtube, "abc123def45", description="")

        tops = [s for s in sources if s.source_type == "top"]
        self.assertEqual(len(tops), 2)
        self.assertEqual(len(youtube._threads.calls), 2)

    def test_select_recheck_recent_first_and_cursor_fill(self):
        now = datetime.now(timezone.utc)
        ordered = ["old1", "old2", "new1", "new2"]

        def make_video(vid: str, hours_ago: int) -> VideoItem:
            return VideoItem(
                video_id=vid,
                title=vid,
                url=f"https://www.youtube.com/watch?v={vid}",
                published_at=(now - timedelta(hours=hours_ago)).isoformat().replace("+00:00", "Z"),
                thumbnail_url="",
            )

        videos_by_id = {
            "old1": make_video("old1", 200),
            "old2": make_video("old2", 150),
            "new1": make_video("new1", 10),
            "new2": make_video("new2", 5),
        }

        selected, next_cursor = _select_recheck_ids(
            ordered_video_ids=ordered,
            current_cursor=0,
            limit=3,
            recent_hours=72,
            videos_by_id=videos_by_id,
        )

        self.assertEqual(selected[:2], ["new2", "new1"])
        self.assertEqual(len(selected), 3)
        self.assertEqual(next_cursor, 1)


if __name__ == "__main__":
    unittest.main()

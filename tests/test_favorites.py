import unittest
from datetime import UTC, datetime

from crawler.services.favorites import build_aggregates, hash_with_secret, to_week_key_jst


class FavoritesTests(unittest.TestCase):
    def test_hash_with_secret(self):
        one = hash_with_secret("secret", "abc", scope="client")
        two = hash_with_secret("secret", "abc", scope="client")
        three = hash_with_secret("secret", "abc", scope="ip")
        self.assertEqual(one, two)
        self.assertNotEqual(one, three)

    def test_week_key_jst(self):
        self.assertEqual(to_week_key_jst("2026-04-20T00:30:00Z"), "2026-04-20")
        self.assertEqual(to_week_key_jst("2026-04-19T23:59:59Z"), "2026-04-20")

    def test_build_aggregates(self):
        votes = [
            {
                "headingId": "h1",
                "clientHash": "c1",
                "videoId": "v1",
                "headingTitle": "見出し1",
                "videoTitle": "動画1",
                "firstVotedAt": "2026-04-20T01:00:00Z",
                "weekKey": "2026-04-20",
            },
            {
                "headingId": "h1",
                "clientHash": "c2",
                "videoId": "v1",
                "headingTitle": "見出し1",
                "videoTitle": "動画1",
                "firstVotedAt": "2026-04-20T02:00:00Z",
                "weekKey": "2026-04-20",
            },
            {
                "headingId": "h2",
                "clientHash": "c3",
                "videoId": "v2",
                "headingTitle": "見出し2",
                "videoTitle": "動画2",
                "firstVotedAt": "2026-04-20T03:00:00Z",
                "weekKey": "2026-04-20",
            },
            {
                "headingId": "h1",
                "clientHash": "c4",
                "videoId": "v1",
                "headingTitle": "見出し1",
                "videoTitle": "動画1",
                "firstVotedAt": "2026-04-14T04:00:00Z",
                "weekKey": "2026-04-13",
            },
            {
                "headingId": "h1",
                "clientHash": "c5",
                "videoId": "v1",
                "headingTitle": "見出し1",
                "videoTitle": "動画1",
                "firstVotedAt": "2026-04-19T04:00:00Z",
                "weekKey": "2026-04-13",
            },
        ]

        aggregates = build_aggregates(votes, now_utc=datetime(2026, 4, 21, tzinfo=UTC))
        previous_week = aggregates["recent_recommendations"]
        weekly_0413 = aggregates["weekly"]["2026-04-13"]["items"][0]
        weekly_0420 = aggregates["weekly"]["2026-04-20"]["items"][0]

        self.assertEqual(aggregates["hall_of_fame"]["items"][0]["headingId"], "h1")
        self.assertEqual(aggregates["hall_of_fame"]["items"][0]["voteCount"], 4)
        self.assertEqual(previous_week["weekKey"], "2026-04-13")
        self.assertEqual(len(previous_week["items"]), 1)
        self.assertEqual(aggregates["daily_snapshot"]["snapshotDate"], "2026-04-21")
        self.assertEqual(weekly_0413["firstVotedAt"], "2026-04-14T04:00:00Z")
        self.assertEqual(weekly_0413["lastVotedAt"], "2026-04-19T04:00:00Z")
        self.assertEqual(weekly_0420["firstVotedAt"], "2026-04-20T01:00:00Z")
        self.assertEqual(weekly_0420["lastVotedAt"], "2026-04-20T02:00:00Z")


if __name__ == "__main__":
    unittest.main()

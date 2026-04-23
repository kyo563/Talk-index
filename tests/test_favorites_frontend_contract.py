import unittest
from pathlib import Path


class FavoritesFrontendContractTests(unittest.TestCase):
    def test_html_does_not_render_recent_recommendations_feed(self):
        app_js = Path('app.js').read_text(encoding='utf-8')
        self.assertIn('fetchFavoritesAggregate("recentUpload")', app_js)
        self.assertNotIn('fetchFavoritesAggregate("recent")', app_js)

    def test_video_vote_payload_uses_video_context_metadata(self):
        app_js = Path('app.js').read_text(encoding='utf-8')
        self.assertIn('function resolveVideoFavoriteContext', app_js)
        self.assertIn('sec?.headingTitle || sec?.heading_title || sec?.name || sec?.title', app_js)
        self.assertIn('headingId: canonicalHeadingId', app_js)
        self.assertIn('videoId,', app_js)
        self.assertIn('videoTitle,', app_js)
        self.assertIn('sourceVideoUrl,', app_js)
        self.assertIn('sourceVideoTitle,', app_js)
        self.assertIn('publishedAt: canonicalPublishedAt', app_js)
        self.assertIn('videoDate: canonicalVideoDate', app_js)
        self.assertIn('headingStart,', app_js)
        self.assertIn('sourceMode: isVideoMode ? "video" : state.viewMode', app_js)

    def test_video_vote_fail_closed_guard_exists(self):
        app_js = Path('app.js').read_text(encoding='utf-8')
        self.assertIn('video mode vote skipped due to missing metadata', app_js)
        self.assertIn('missing.push("videoId")', app_js)
        self.assertIn('missing.push("headingTitle")', app_js)
        self.assertIn('missing.push("sourceVideoUrl_or_publishedAt")', app_js)
        self.assertIn('state.unsyncedFavoriteHeadingIds.add(normalized);', app_js)

    def test_vote_marked_only_on_success(self):
        app_js = Path('app.js').read_text(encoding='utf-8')
        self.assertIn('await sendFavoriteVote({', app_js)
        self.assertIn('state.alreadyVotedHeadingIds.add(normalized);', app_js)
        self.assertIn('if (state.alreadyVotedHeadingIds.has(normalized) && !state.unsyncedFavoriteHeadingIds.has(normalized)) {', app_js)

    def test_toggle_off_clears_unsynced_and_voted_to_avoid_resend(self):
        app_js = Path('app.js').read_text(encoding='utf-8')
        self.assertIn('state.unsyncedFavoriteHeadingIds.delete(normalized);', app_js)
        self.assertIn('state.alreadyVotedHeadingIds.delete(normalized);', app_js)


if __name__ == '__main__':
    unittest.main()

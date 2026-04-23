import unittest
from pathlib import Path


class FavoritesFrontendContractTests(unittest.TestCase):
    def test_html_does_not_render_recent_recommendations_feed(self):
        app_js = Path('app.js').read_text(encoding='utf-8')
        self.assertIn('fetchFavoritesAggregate("recentUpload")', app_js)
        self.assertNotIn('fetchFavoritesAggregate("recent")', app_js)


if __name__ == '__main__':
    unittest.main()

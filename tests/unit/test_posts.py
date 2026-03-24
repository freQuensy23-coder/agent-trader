from datetime import datetime, timezone

from agent_trader.data.posts import _strip_html, _parse_datetime, load_posts

PARQUET_PATH = "data/posts/trump_posts.parquet"


class TestHtmlStripping:
    def test_html_entities_stripped(self):
        assert _strip_html("&amp; &lt; &gt; &#39;") == "& < > '"

    def test_nested_html_tags(self):
        assert _strip_html("<p><strong>text</strong></p>") == "text"

    def test_br_becomes_newline(self):
        assert _strip_html("hello<br>world") == "hello\nworld"

    def test_script_stripped(self):
        assert _strip_html("before<script>evil()</script>after") == "beforeafter"

    def test_empty_string(self):
        assert _strip_html("") == ""


class TestDatetimeParsing:
    def test_z_suffix(self):
        dt = _parse_datetime("2025-06-15T14:00:00.000Z")
        assert dt.tzinfo is not None
        assert dt == datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)

    def test_plus_offset(self):
        dt = _parse_datetime("2025-06-15T14:00:00+00:00")
        assert dt == datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)

    def test_nonzero_offset_converted_to_utc(self):
        dt = _parse_datetime("2025-06-15T17:00:00+03:00")
        assert dt == datetime(2025, 6, 15, 14, 0, 0, tzinfo=timezone.utc)


class TestLoadPostsParquet:
    def test_loads_all(self):
        posts = load_posts(PARQUET_PATH)
        assert len(posts) > 6000

    def test_sorted_chronologically(self):
        posts = load_posts(PARQUET_PATH)
        for i in range(1, len(posts)):
            assert posts[i].created_at >= posts[i - 1].created_at

    def test_dedup_by_id(self):
        posts = load_posts(PARQUET_PATH)
        ids = [p.id for p in posts]
        assert len(ids) == len(set(ids))

    def test_date_filter(self):
        start = datetime(2025, 6, 1, tzinfo=timezone.utc)
        end = datetime(2025, 6, 30, tzinfo=timezone.utc)
        posts = load_posts(PARQUET_PATH, start=start, end=end)
        assert len(posts) > 0
        for p in posts:
            assert p.created_at >= start
            assert p.created_at <= end

    def test_reposts_detected(self):
        posts = load_posts(PARQUET_PATH)
        reposts = [p for p in posts if p.is_repost]
        assert len(reposts) > 100

    def test_media_posts_detected(self):
        posts = load_posts(PARQUET_PATH)
        media = [p for p in posts if p.has_media]
        assert len(media) > 500

    def test_engagement_non_negative(self):
        posts = load_posts(PARQUET_PATH)
        for p in posts:
            assert p.engagement >= 0

    def test_created_at_ms_consistent(self):
        posts = load_posts(PARQUET_PATH)
        for p in posts[:100]:
            expected_ms = int(p.created_at.timestamp() * 1000)
            assert p.created_at_ms == expected_ms

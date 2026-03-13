"""
TDD tests for src/harvesters/news_harvester.py

All HTTP calls are mocked — no network access required.
"""
from unittest.mock import MagicMock, patch

import pytest

from src.harvesters.news_harvester import (
    Article,
    fetch_articles,
    _normalize,
    _name_tokens,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RSS = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>MMA Junkie</title>
    <link>https://mmajunkie.usatoday.com</link>
    <item>
      <title>Jon Jones vs Stipe Miocic: Full breakdown</title>
      <link>https://mmajunkie.usatoday.com/jones-miocic</link>
      <pubDate>Wed, 12 Mar 2025 10:00:00 +0000</pubDate>
      <description>Jon Jones is set to fight Stipe Miocic at UFC 309 in a heavyweight title bout.</description>
    </item>
    <item>
      <title>Islam Makhachev defends lightweight title</title>
      <link>https://mmajunkie.usatoday.com/makhachev-defense</link>
      <pubDate>Wed, 12 Mar 2025 09:00:00 +0000</pubDate>
      <description>Islam Makhachev successfully defended his lightweight title with a dominant performance.</description>
    </item>
  </channel>
</rss>"""

EMPTY_RSS = """\
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Empty Feed</title>
  </channel>
</rss>"""


def _mock_response(text: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFetchArticles:
    @patch("src.harvesters.news_harvester.httpx.get")
    def test_returns_list_of_article_objects(self, mock_get):
        """A successful fetch returns Article instances."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)

        results = fetch_articles("Jon Jones")

        assert isinstance(results, list)
        assert len(results) > 0
        assert all(isinstance(a, Article) for a in results)

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_article_has_required_fields(self, mock_get):
        """Each Article carries title, url, published_date, source, raw_text."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)

        results = fetch_articles("Jon Jones")
        article = results[0]

        assert article.title
        assert article.url
        assert article.published_date
        assert article.source
        assert article.raw_text

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_filters_to_articles_mentioning_fighter(self, mock_get):
        """Only articles that mention the fighter name (case-insensitive) are returned."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)

        results = fetch_articles("Jon Jones")

        titles = [a.title for a in results]
        assert any("Jon Jones" in t for t in titles)
        assert all("Makhachev" not in t for t in titles)

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_filtering_is_case_insensitive(self, mock_get):
        """Fighter name match is not case-sensitive."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)

        upper = fetch_articles("JON JONES")
        lower = fetch_articles("jon jones")

        assert len(upper) == len(lower)

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_no_match_returns_empty_list(self, mock_get):
        """A fighter not mentioned in any article yields an empty list."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)

        results = fetch_articles("Conor McGregor")

        assert results == []

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_dead_feed_is_skipped_gracefully(self, mock_get):
        """A feed that raises on HTTP request does not crash the harvester."""
        mock_get.side_effect = Exception("connection refused")

        results = fetch_articles("Jon Jones")

        assert isinstance(results, list)  # no exception raised

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_http_error_feed_is_skipped_gracefully(self, mock_get):
        """A feed returning a non-2xx status is skipped without crashing."""
        import httpx

        mock_get.return_value = _mock_response("", status_code=503)
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "503", request=MagicMock(), response=MagicMock()
        )

        results = fetch_articles("Jon Jones")

        assert isinstance(results, list)

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_source_name_is_set_correctly(self, mock_get):
        """Article.source reflects the named feed it came from."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)

        results = fetch_articles("Jon Jones")

        sources = {a.source for a in results}
        assert sources <= {"MMA Weekly", "MMA Fighting", "Sherdog"}

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_queries_all_three_feeds(self, mock_get):
        """httpx.get is called once per configured feed."""
        mock_get.return_value = _mock_response(EMPTY_RSS)

        fetch_articles("Jon Jones")

        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# RSS builder helper for new tests
# ---------------------------------------------------------------------------

def _make_rss(*items: tuple[str, str]) -> str:
    """Build a minimal RSS feed from (title, description) pairs."""
    item_xml = ""
    for title, desc in items:
        slug = title.replace(" ", "-")[:30]
        item_xml += f"""
    <item>
      <title>{title}</title>
      <link>https://example.com/{slug}</link>
      <pubDate>Thu, 12 Mar 2026 10:00:00 +0000</pubDate>
      <description>{desc}</description>
    </item>"""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel>'
        f"<title>MMA News</title><link>https://example.com</link>"
        f"{item_xml}"
        "</channel></rss>"
    )


# ---------------------------------------------------------------------------
# Unit tests for _normalize and _name_tokens
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_strips_diacritics(self):
        assert _normalize("Procházka") == "prochazka"

    def test_strips_multiple_diacritics(self):
        assert _normalize("Jiří Procházka") == "jiri prochazka"

    def test_lowercases(self):
        assert _normalize("BLAYDES") == "blaydes"

    def test_plain_ascii_unchanged(self):
        assert _normalize("Jon Jones") == "jon jones"


class TestNameTokens:
    def test_includes_full_normalized_name(self):
        tokens = _name_tokens("Curtis Blaydes")
        assert "curtis blaydes" in tokens

    def test_includes_last_name(self):
        tokens = _name_tokens("Curtis Blaydes")
        assert "blaydes" in tokens

    def test_includes_first_name_when_requested(self):
        tokens = _name_tokens("Curtis Blaydes", include_first=True)
        assert "curtis" in tokens

    def test_excludes_first_name_by_default(self):
        tokens = _name_tokens("Curtis Blaydes")
        assert "curtis" not in tokens

    def test_normalizes_diacritics_in_tokens(self):
        tokens = _name_tokens("Jiří Procházka")
        assert "prochazka" in tokens
        assert "jiri prochazka" in tokens

    def test_single_word_name_no_error(self):
        tokens = _name_tokens("Ngannou")
        assert "ngannou" in tokens


# ---------------------------------------------------------------------------
# Loosened matching — last name and diacritics
# ---------------------------------------------------------------------------


class TestLoosenedMatching:
    @patch("src.harvesters.news_harvester.httpx.get")
    def test_matches_by_last_name_only(self, mock_get):
        """Article mentioning only 'Blaydes' matches fighter 'Curtis Blaydes'."""
        rss = _make_rss(
            ("Blaydes dominates in UFC 327 co-main", "Curtis Blaydes lands heavy shots"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Curtis Blaydes")
        assert len(results) >= 1  # same RSS returned by all 3 mocked feeds

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_diacritics_normalized_for_matching(self, mock_get):
        """'Prochazka' (ASCII) in article text matches 'Jiří Procházka'."""
        rss = _make_rss(
            ("Prochazka vs Ulberg title fight preview", "Jiri Prochazka defends his belt"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Jiří Procházka")
        assert len(results) >= 1

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_last_name_match_is_case_insensitive(self, mock_get):
        """Last-name matching is not case-sensitive."""
        rss = _make_rss(
            ("GASTELUM vs LUQUE set for prelims", "KELVIN GASTELUM returns to action"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Kelvin Gastelum")
        assert len(results) >= 1

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_costa_last_name_matches(self, mock_get):
        """'Costa' in text matches 'Paulo Costa'."""
        rss = _make_rss(
            ("Costa vs Murzakanov added to UFC 327", "Paulo Costa looks to rebound"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Paulo Costa")
        assert len(results) >= 1

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_unrelated_fighter_still_returns_empty(self, mock_get):
        """Fighter not mentioned anywhere still returns empty — no false positives."""
        rss = _make_rss(
            ("Prochazka vs Ulberg main event", "Blaydes fights on the main card"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Conor McGregor")
        assert results == []

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_full_name_match_still_works(self, mock_get):
        """Original full-name substring matching still functions correctly."""
        mock_get.return_value = _mock_response(SAMPLE_RSS)
        results = fetch_articles("Jon Jones")
        assert len(results) >= 1


# ---------------------------------------------------------------------------
# Fallback sweep — first-name matching when last-name yields nothing
# ---------------------------------------------------------------------------


class TestFallbackSweep:
    @patch("src.harvesters.news_harvester.httpx.get")
    def test_fallback_matches_first_name_when_last_name_absent(self, mock_get):
        """Article has 'Kevin' but not 'Holland' → zero primary hits → fallback fires."""
        rss = _make_rss(
            ("Kevin makes stunning return at UFC 327", "Kevin looked great in camp"),
        )
        mock_get.return_value = _mock_response(rss)
        # Primary tokens: {"kevin holland", "holland"} → no match in any feed
        # Fallback tokens add "kevin" → matches (3 feeds × 1 article = up to 3)
        results = fetch_articles("Kevin Holland")
        assert len(results) >= 1

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_fallback_not_triggered_when_primary_succeeds(self, mock_get):
        """Articles matched in primary are not duplicated by the fallback pass."""
        rss = _make_rss(
            ("Holland stuns Randy Brown at UFC 327", "Kevin Holland delivers KO"),
        )
        mock_get.return_value = _mock_response(rss)
        primary_results = fetch_articles("Kevin Holland")
        # 3 feeds × 1 matching article = 3 from primary.
        # If fallback incorrectly ran, count would double to 6.
        assert len(primary_results) <= 3

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_truly_absent_fighter_empty_even_after_fallback(self, mock_get):
        """No name token at all → empty even after fallback sweep."""
        rss = _make_rss(
            ("Ilia Topuria training update", "Max Holloway looks sharp in sparring"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Conor McGregor")
        assert results == []

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_fallback_respects_twenty_entry_limit_per_feed(self, mock_get):
        """Fallback only considers the first 20 entries per feed.

        Feed has 25 entries; entry 21 (0-indexed) contains the first name
        'Paulo' — beyond the 20-entry limit — so fallback returns nothing.
        """
        items = []
        for i in range(25):
            if i == 20:
                items.append(("Paulo fights at UFC 327", "Paulo checks in at weight"))
            else:
                items.append((f"UFC 327 preview part {i}", "General MMA news"))
        rss = _make_rss(*items)
        mock_get.return_value = _mock_response(rss)
        # "costa" / "paulo costa" not in any of the 20 first entries
        # "paulo" only at entry 21 — excluded by limit
        results = fetch_articles("Paulo Costa")
        assert results == []

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_httpx_called_only_three_times_total(self, mock_get):
        """Feeds are fetched once — fallback reuses cached entries, no extra calls."""
        mock_get.return_value = _mock_response(EMPTY_RSS)
        fetch_articles("Kevin Holland")
        assert mock_get.call_count == 3


# ---------------------------------------------------------------------------
# Emmett last-name matching — regression tests
# ---------------------------------------------------------------------------


class TestEmmettMatching:
    def test_emmett_is_in_primary_tokens(self):
        """'emmett' must be in the primary search tokens for 'Josh Emmett'."""
        tokens = _name_tokens("Josh Emmett")
        assert "emmett" in tokens

    def test_josh_emmett_full_name_in_primary_tokens(self):
        """Full normalised name 'josh emmett' must be in primary tokens."""
        tokens = _name_tokens("Josh Emmett")
        assert "josh emmett" in tokens

    def test_emmett_not_in_tokens_as_first_name_by_default(self):
        """First name 'josh' is not included in primary tokens."""
        tokens = _name_tokens("Josh Emmett")
        assert "josh" not in tokens

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_article_mentioning_only_emmett_matches_josh_emmett(self, mock_get):
        """Article title/body with only 'Emmett' (last name) matches 'Josh Emmett'."""
        rss = _make_rss(
            ("Emmett stops Vallejos in featherweight main event", "Emmett lands TKO"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Josh Emmett")
        assert len(results) >= 1

    @patch("src.harvesters.news_harvester.httpx.get")
    def test_emmett_match_is_case_insensitive(self, mock_get):
        """'EMMETT' in text still matches 'Josh Emmett'."""
        rss = _make_rss(
            ("EMMETT vs VALLEJOS UFC Fight Night preview", "EMMETT trains hard"),
        )
        mock_get.return_value = _mock_response(rss)
        results = fetch_articles("Josh Emmett")
        assert len(results) >= 1

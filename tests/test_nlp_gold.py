"""Pruebas puras de normalización NLP y agregaciones Gold."""
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from rss_lake.gold import _aggregate
from rss_lake.nlp import analysis_text, cache_key


class NlpGoldTests(unittest.TestCase):
    def test_analyzed_text_is_title_newline_description(self):
        row = {"title": "  Título   limpio ", "description": " Descripción   limpia "}
        self.assertEqual("Título limpio\nDescripción limpia", analysis_text(row))

    def test_cache_key_changes_only_when_text_changes(self):
        self.assertEqual(cache_key("texto"), cache_key("texto"))
        self.assertNotEqual(cache_key("texto"), cache_key("texto modificado"))

    def test_gold_counts_distinct_news_per_dimension(self):
        news = [{"news_id": "n1", "pub_date_utc": "2026-11-04T10:00:00+00:00",
                 "medio": "M", "section_primary": "S"}]
        sources = [{"news_id": "n1", "medio": "M", "seccion": "S"}]
        entities = [
            {"news_id": "n1", "entity_type": "ORGANIZATION", "entity_normalized": "aws"},
            {"news_id": "n1", "entity_type": "ORGANIZATION", "entity_normalized": "aws"},
        ]
        phrases = [
            {"news_id": "n1", "phrase_normalized": "data lake"},
            {"news_id": "n1", "phrase_normalized": "data lake"},
        ]
        daily, entity_daily, phrase_daily = _aggregate(news, sources, entities, phrases)
        self.assertEqual(1, daily[0]["news_count"])
        self.assertEqual(1, entity_daily[0]["news_count"])
        self.assertEqual(1, phrase_daily[0]["news_count"])


if __name__ == "__main__":
    unittest.main()

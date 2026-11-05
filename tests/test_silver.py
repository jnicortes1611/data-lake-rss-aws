"""Pruebas Silver sin AWS, internet ni pyarrow."""
import copy
from io import BytesIO
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from rss_lake.silver import canonical_url, clean_text, normalized_date, process_silver, verify_silver


class StorageError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.fail_key_contains = None

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise StorageError("NoSuchKey")
        obj = copy.deepcopy(self.objects[Key])
        obj["Body"] = BytesIO(obj["Body"])
        return obj

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail_key_contains and self.fail_key_contains in Key:
            raise StorageError("ServiceUnavailable")
        if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
            raise StorageError("PreconditionFailed")
        self.objects[Key] = {"Body": bytes(Body), "Metadata": kwargs.get("Metadata", {})}
        return {}


def encode(rows, fields):
    return json.dumps([{field: row.get(field) for field in fields} for row in rows],
                      sort_keys=True).encode()


def decode(raw):
    return json.loads(raw)


def rss(title="Noticia uno", description="<b>Texto</b>", link="https://www.eltiempo.com/a?utm_source=x"):
    return f"""<rss><channel><item><guid>g1</guid><title>{title}</title>
    <link>{link.replace('&', '&amp;')}</link><description><![CDATA[{description}]]></description>
    <pubDate>Sun, 01 Nov 2026 10:00:00 -0500</pubDate></item></channel></rss>""".encode()


class SilverTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()

    def add_batch(self, batch_id, feeds):
        sources = []
        for source_id, section, raw in feeds:
            key = f"bronze/source={source_id}/ingestion_date=2026-11-01/run_id={batch_id}/feed.xml"
            self.s3.objects[key] = {"Body": raw, "Metadata": {}}
            sources.append({
                "source_id": source_id, "medio": "El Tiempo", "seccion": section,
                "bronze_s3_uri": f"s3://bucket/{key}", "status": "OK",
            })
        manifest = {"stage": "BRONZE", "status": "COMPLETE", "batch_id": batch_id, "sources": sources}
        key = f"control/batches/{batch_id}/manifest.json"
        self.s3.objects[key] = {"Body": json.dumps(manifest).encode(), "Metadata": {}}

    def test_text_url_and_date_normalization(self):
        self.assertEqual(clean_text("<b> Hola </b> &amp; mundo"), "Hola & mundo")
        self.assertEqual(canonical_url("http://WWW.ELTIEMPO.COM/a/?utm_source=x#parte"),
                         "https://www.eltiempo.com/a")
        original, utc = normalized_date("Sun, 01 Nov 2026 10:00:00 -0500")
        self.assertEqual(original, "Sun, 01 Nov 2026 10:00:00 -0500")
        self.assertEqual(utc, "2026-11-01T15:00:00+00:00")

    def test_deduplicates_across_sources_and_quarantines_invalid(self):
        invalid = b"<rss><channel><item><title></title><link>sin-url</link></item></channel></rss>"
        self.add_batch("b1", [
            ("colombia", "Colombia", rss()),
            ("bogota", "Bogota", rss()),
            ("mundo", "Mundo", invalid),
        ])
        result = process_silver(self.s3, "bucket", "b1", encode, decode)
        self.assertEqual(result["new"], 1)
        self.assertEqual(result["duplicates_in_batch"], 1)
        self.assertEqual(result["quarantined"], 1)
        self.assertEqual(result["news_current"], 1)
        self.assertEqual(result["source_associations"], 2)
        verified = verify_silver(self.s3, "bucket", result["release_id"], decode)
        self.assertEqual(verified["status"], "VERIFIED")

    def test_same_batch_reuses_release_without_duplicates(self):
        self.add_batch("b1", [("colombia", "Colombia", rss())])
        first = process_silver(self.s3, "bucket", "b1", encode, decode)
        second = process_silver(self.s3, "bucket", "b1", encode, decode)
        self.assertTrue(second["reused_release"])
        self.assertEqual(first["release_id"], second["release_id"])
        self.assertEqual(second["news_current"], 1)

    def test_new_batch_updates_existing_news_version(self):
        self.add_batch("b1", [("colombia", "Colombia", rss(description="Texto inicial"))])
        process_silver(self.s3, "bucket", "b1", encode, decode)
        self.add_batch("b2", [("colombia", "Colombia", rss(description="Texto corregido"))])
        second = process_silver(self.s3, "bucket", "b2", encode, decode)
        self.assertEqual(second["updated"], 1)
        pointer = json.loads(self.s3.objects["control/silver/current.json"]["Body"])
        news = decode(self.s3.objects[pointer["objects"]["news"]]["Body"])
        self.assertEqual(news[0]["version"], 2)
        self.assertEqual(news[0]["description"], "Texto corregido")

    def test_failed_release_does_not_replace_current_pointer(self):
        self.add_batch("b1", [("colombia", "Colombia", rss())])
        first = process_silver(self.s3, "bucket", "b1", encode, decode)
        before = self.s3.objects["control/silver/current.json"]["Body"]
        self.add_batch("b2", [("colombia", "Colombia", rss(description="Cambio"))])
        self.s3.fail_key_contains = "quarantine/silver/batch_id=b2"
        with self.assertRaises(StorageError):
            process_silver(self.s3, "bucket", "b2", encode, decode)
        self.assertEqual(before, self.s3.objects["control/silver/current.json"]["Body"])
        self.assertEqual(first["release_id"], json.loads(before)["release_id"])


if __name__ == "__main__":
    unittest.main()

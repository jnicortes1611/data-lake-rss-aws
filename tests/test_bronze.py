"""Pruebas sin AWS/HTTP. Ejecutar: python -m unittest discover -s tests -v."""
import copy
from io import BytesIO
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from rss_lake.bronze import FeedFailure, ingest_batch, inspect_xml, load_sources, verify_batch

RSS = b'<rss version="2.0"><channel><item><guid>1</guid><title>Noticia de prueba</title><link>https://www.eltiempo.com/prueba</link></item></channel></rss>'
EMPTY = b'<rss version="2.0"><channel/></rss>'


class StorageError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}
        super().__init__(code)


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.fail_metadata_once = False

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise StorageError("NoSuchKey")
        result = copy.deepcopy(self.objects[Key])
        result["Body"] = BytesIO(result["Body"])
        return result

    def put_object(self, Bucket, Key, Body, **kwargs):
        if self.fail_metadata_once and Key.endswith("metadata.json"):
            self.fail_metadata_once = False
            raise StorageError("ServiceUnavailable")
        if kwargs.get("IfNoneMatch") == "*" and Key in self.objects:
            raise StorageError("PreconditionFailed")
        self.objects[Key] = {"Body": bytes(Body), "Metadata": kwargs.get("Metadata", {})}
        return {}


class BronzeTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()
        self.sources = load_sources()
        self.calls = []

    def fetch(self, url):
        self.calls.append(url)
        return RSS, 200, url

    def run_batch(self, run="run-1", reprocess="", fetch=None):
        return ingest_batch(self.s3, "bucket", run, self.sources, reprocess, fetch or self.fetch)

    def manifest(self, result):
        return json.loads(self.s3.objects[result["manifest_key"]]["Body"])

    def originals(self):
        return {k: v["Body"] for k, v in self.s3.objects.items() if k.endswith("feed.xml")}

    def test_initial_load_is_original_and_verifiable(self):
        result = self.run_batch()
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len(self.originals()), 3)
        self.assertTrue(all(body == RSS for body in self.originals().values()))
        self.assertEqual(verify_batch(self.s3, "bucket", result["manifest_key"])["verified_records"], 3)

    def test_repeated_attempt_never_calls_http_or_changes_originals(self):
        first = self.run_batch()
        original = self.originals()
        again = self.run_batch()
        self.assertEqual(first["batch_id"], again["batch_id"])
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(original, self.originals())
        self.assertTrue(all(s["reused_bronze"] for s in self.manifest(again)["sources"]))

    def test_reprocess_uses_existing_batch_without_network(self):
        first = self.run_batch()
        original = self.originals()
        def no_network(url):
            self.fail("El reproceso no debe consultar RSS")
        second = self.run_batch("new-airflow-run", first["batch_id"], no_network)
        self.assertEqual(second["batch_id"], first["batch_id"])
        self.assertEqual(self.manifest(second)["mode"], "reprocess")
        self.assertEqual(original, self.originals())

    def test_new_run_is_a_separate_extraction(self):
        first = self.run_batch()
        second = self.run_batch("run-2")
        self.assertNotEqual(first["batch_id"], second["batch_id"])
        self.assertEqual(len(self.calls), 6)
        self.assertEqual(len(self.originals()), 6)

    def test_valid_empty_feed_is_success_with_zero_records(self):
        result = self.run_batch(fetch=lambda url: (EMPTY, 200, url))
        self.assertEqual(result["status"], "COMPLETE")
        self.assertEqual(result["records_received"], 0)
        self.assertTrue(all(s["status"] == "EMPTY_FEED" for s in self.manifest(result)["sources"]))

    def test_partial_failure_retries_only_missing_source(self):
        failing = self.sources[1]["url"]
        def partial(url):
            if url == failing:
                raise FeedFailure("HTTP_ERROR", "HTTP 503", 503)
            return self.fetch(url)
        failed = self.run_batch(fetch=partial)
        self.assertEqual(failed["status"], "PARTIAL")
        self.assertEqual(result_source := self.manifest(failed)["sources"][1]["http_status"], 503)
        with self.assertRaises(RuntimeError):
            verify_batch(self.s3, "bucket", failed["manifest_key"])
        recovered = self.run_batch()
        self.assertEqual(recovered["status"], "COMPLETE")
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(len(self.originals()), 3)

    def test_crash_between_xml_and_json_does_not_refetch(self):
        self.s3.fail_metadata_once = True
        first = self.run_batch()
        self.assertEqual(first["status"], "PARTIAL")
        self.assertEqual(len(self.originals()), 3)
        recovered = self.run_batch()
        self.assertEqual(recovered["status"], "COMPLETE")
        self.assertEqual(len(self.calls), 3)
        verify_batch(self.s3, "bucket", recovered["manifest_key"])

    def test_invalid_responses_never_become_bronze_xml(self):
        for content, expected in [(b"", "EMPTY_BODY"), (b"<rss>", "MALFORMED_XML"), (b"<html/>", "NOT_RSS"), (b"<!DOCTYPE rss><rss><channel/></rss>", "UNSAFE_XML_DECLARATION")]:
            with self.subTest(expected=expected):
                self.s3 = MemoryS3()
                result = self.run_batch(fetch=lambda url: (content, 200, url))
                self.assertEqual(result["status"], "FAILED")
                self.assertEqual(self.originals(), {})
                self.assertTrue(all(s["status"] == expected for s in self.manifest(result)["sources"]))

    def test_corrupted_original_is_detected(self):
        first = self.run_batch()
        key = next(iter(self.originals()))
        self.s3.objects[key]["Body"] = EMPTY
        with self.assertRaises(ValueError):
            verify_batch(self.s3, "bucket", first["manifest_key"])

    def test_missing_bronze_reprocess_never_downloads(self):
        first = self.run_batch()
        del self.s3.objects[next(iter(self.originals()))]
        second = self.run_batch("replay", first["batch_id"])
        self.assertEqual(second["status"], "PARTIAL")
        self.assertEqual(len(self.calls), 3)
        self.assertIn("MISSING_BRONZE", [s["status"] for s in self.manifest(second)["sources"]])

    def test_unknown_reprocess_batch_does_not_create_a_batch(self):
        with self.assertRaises(ValueError):
            self.run_batch("replay", "does-not-exist")
        self.assertEqual(self.s3.objects, {})
        self.assertEqual(self.calls, [])

    def test_access_denied_is_not_treated_as_missing(self):
        def denied(**kwargs):
            raise StorageError("AccessDenied")
        self.s3.get_object = denied
        with self.assertRaises(StorageError):
            self.run_batch()
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()

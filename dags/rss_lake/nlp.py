"""Silver NLP v2: cache Comprehend y tablas normalizadas particionadas."""
from __future__ import annotations

import hashlib
import io
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone

CACHE_VERSION = "v1"
BUILD_VERSION = "nlp-v3"
MAX_TEXT_BYTES = 90_000


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _code(exc):
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


def _get_optional(s3, bucket, key):
    try:
        return s3.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if _code(exc) in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise


def _read_json(s3, bucket, key):
    obj = _get_optional(s3, bucket, key)
    return None if obj is None else json.loads(obj["Body"].read())


def _clean(value):
    return re.sub(r"\s+", " ", value or "").strip()


def _normalized(value):
    return _clean(unicodedata.normalize("NFKC", value or "").casefold())


def analysis_text(row):
    title, description = _clean(row.get("title")), _clean(row.get("description"))
    return "\n".join(part for part in (title, description) if part)


def cache_key(text):
    digest = hashlib.sha256(text.encode()).hexdigest()
    return f"cache/comprehend/{CACHE_VERSION}/sha256={digest}.json"


def _detect(comprehend, text):
    languages = comprehend.detect_dominant_language(Text=text)["Languages"]
    language = max(languages, key=lambda item: item["Score"])["LanguageCode"]
    result = {"schema_version": 2, "language": language, "languages": languages,
              "entities": [], "key_phrases": [], "operation_count": 1}
    if language != "es":
        result["analysis_status"] = "UNSUPPORTED_LANGUAGE"
        return result
    result["entities"] = comprehend.detect_entities(Text=text, LanguageCode=language)["Entities"]
    result["key_phrases"] = comprehend.detect_key_phrases(Text=text, LanguageCode=language)["KeyPhrases"]
    result.update({"analysis_status": "COMPLETE", "operation_count": 3})
    return result


def analyze_cached(s3, comprehend, bucket, text):
    key = cache_key(text)
    cached = _read_json(s3, bucket, key)
    if cached is not None:
        cached.setdefault("analysis_status", "COMPLETE")
        cached.setdefault("operation_count", 3)
        return cached, True, key
    result = _detect(comprehend, text)
    result.update({"cache_key": key, "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                   "created_at_utc": datetime.now(timezone.utc).isoformat()})
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=_json_bytes(result),
                      ContentType="application/json", IfNoneMatch="*")
    except Exception as exc:
        if _code(exc) not in {"PreconditionFailed", "412"}:
            raise
        result = _read_json(s3, bucket, key)
    return result, False, key


def _read_parquet(raw):
    import pyarrow.parquet as pq
    return pq.read_table(io.BytesIO(raw)).to_pylist()


def _parquet(rows, fields, doubles=(), integers=()):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([pa.field(name, pa.float64() if name in doubles else
        pa.int64() if name in integers else pa.string(), nullable=True) for name in fields])
    table = pa.Table.from_pylist([{name: row.get(name) for name in fields} for row in rows], schema=schema)
    output = io.BytesIO()
    pq.write_table(table, output, compression="snappy")
    return output.getvalue()


def _put_immutable(s3, bucket, key, body):
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body,
                      ContentType="application/vnd.apache.parquet", IfNoneMatch="*")
    except Exception as exc:
        if _code(exc) not in {"PreconditionFailed", "412"}:
            raise
        if s3.get_object(Bucket=bucket, Key=key)["Body"].read() != body:
            raise RuntimeError(f"Objeto inmutable distinto: {key}")


def _publish_partitioned(s3, bucket, release_id, table, rows, fields, doubles=(), integers=()):
    partitions = []
    grouped = defaultdict(list)
    for row in rows:
        grouped[row["publication_date"]].append(row)
    parquet_fields = [field for field in fields if field != "publication_date"]
    for date in sorted(grouped):
        key = (f"silver/nlp/releases/release_id={release_id}/{table}/"
               f"publication_date={date}/part-000.parquet")
        _put_immutable(s3, bucket, key, _parquet(grouped[date], parquet_fields, doubles, integers))
        partitions.append({"publication_date": date, "key": key, "rows": len(grouped[date])})
    return {"base_prefix": f"silver/nlp/releases/release_id={release_id}/{table}/",
            "partitions": partitions, "rows": len(rows)}


def process_nlp(s3, comprehend, bucket, max_records=1000):
    silver = _read_json(s3, bucket, "control/silver/current.json")
    if not silver or silver.get("stage") != "SILVER":
        raise RuntimeError("No existe una version Silver publicada")
    previous = _read_json(s3, bucket, "control/nlp/current.json")
    if (previous and previous.get("silver_release_id") == silver["release_id"]
            and previous.get("build_version") == BUILD_VERSION):
        return dict(previous["metrics"], reused_release=True, release_id=previous["release_id"])

    news = _read_parquet(s3.get_object(Bucket=bucket, Key=silver["objects"]["news"])["Body"].read())
    news = news[:int(max_records)]
    sources = _read_parquet(
        s3.get_object(Bucket=bucket, Key=silver["objects"]["news_sources"])["Body"].read())
    enriched, entities, phrases = [], [], []
    hits = misses = calls = 0
    for row in news:
        text = analysis_text(row)
        date = row.get("pub_date_utc", "")[:10] or "unknown"
        text_hash = hashlib.sha256(text.encode()).hexdigest() if text else None
        if not text:
            result, hit, key = {"analysis_status": "EMPTY_TEXT", "language": None,
                                "entities": [], "key_phrases": [], "operation_count": 0}, False, None
        elif len(text.encode()) > MAX_TEXT_BYTES:
            result, hit, key = {"analysis_status": "TEXT_TOO_LONG", "language": None,
                                "entities": [], "key_phrases": [], "operation_count": 0}, False, None
        else:
            result, hit, key = analyze_cached(s3, comprehend, bucket, text)
            hits += int(hit)
            misses += int(not hit)
            calls += 0 if hit else int(result.get("operation_count", 3))
        analyzed_at = result.get("created_at_utc") or datetime.now(timezone.utc).isoformat()
        enriched.append({"news_id": row["news_id"], "content_hash": row["content_hash"],
                         "text_hash": text_hash, "analyzed_text": text,
                         "analysis_status": result["analysis_status"], "language": result.get("language"),
                         "entities_json": json.dumps(result["entities"], ensure_ascii=False),
                         "key_phrases_json": json.dumps(result["key_phrases"], ensure_ascii=False),
                         "cache_key": key, "analyzed_at_utc": analyzed_at})
        entity_best = {}
        for item in result["entities"]:
            normalized, kind = _normalized(item.get("Text")), item.get("Type")
            if normalized and kind:
                current = entity_best.get((kind, normalized))
                if current is None or float(item.get("Score", 0)) > float(current.get("Score", 0)):
                    entity_best[(kind, normalized)] = item
        for (kind, normalized), item in entity_best.items():
            entities.append({"publication_date": date, "news_id": row["news_id"],
                "entity_type": kind, "entity_text": item.get("Text"), "entity_normalized": normalized,
                "confidence": float(item.get("Score", 0)), "language": result.get("language"),
                "text_hash": text_hash, "analyzed_at_utc": analyzed_at})
        phrase_best = {}
        for item in result["key_phrases"]:
            normalized = _normalized(item.get("Text"))
            if normalized and (normalized not in phrase_best or
                    float(item.get("Score", 0)) > float(phrase_best[normalized].get("Score", 0))):
                phrase_best[normalized] = item
        for normalized, item in phrase_best.items():
            phrases.append({"publication_date": date, "news_id": row["news_id"],
                "phrase_text": item.get("Text"), "phrase_normalized": normalized,
                "confidence": float(item.get("Score", 0)), "language": result.get("language"),
                "text_hash": text_hash, "analyzed_at_utc": analyzed_at})

    release_id = hashlib.sha256(f"{silver['release_id']}:{BUILD_VERSION}:{max_records}".encode()).hexdigest()[:20]
    legacy_key = f"silver/nlp/releases/release_id={release_id}/news_nlp/news_nlp.parquet"
    legacy_fields = ["news_id", "content_hash", "text_hash", "analyzed_text", "analysis_status",
                     "language", "entities_json", "key_phrases_json", "cache_key", "analyzed_at_utc"]
    _put_immutable(s3, bucket, legacy_key, _parquet(enriched, legacy_fields))
    news_dates = {row["news_id"]: (row.get("pub_date_utc", "")[:10] or "unknown") for row in news}
    news_partitioned = [dict(row, publication_date=news_dates[row["news_id"]]) for row in news]
    source_partitioned = [dict(row, publication_date=news_dates[row["news_id"]]) for row in sources
                          if row["news_id"] in news_dates]
    tables = {
        "news": _publish_partitioned(s3, bucket, release_id, "news", news_partitioned,
            ["publication_date", "news_id", "canonical_url", "title", "description",
             "pub_date_original", "pub_date_utc", "content_hash", "version", "first_seen_at_utc",
             "last_seen_at_utc", "last_batch_id", "last_source_id", "medio", "section_primary"],
            integers={"version"}),
        "news_sources": _publish_partitioned(s3, bucket, release_id, "news_sources", source_partitioned,
            ["publication_date", "news_id", "source_id", "medio", "seccion", "first_seen_batch_id",
             "last_seen_batch_id", "first_seen_at_utc", "last_seen_at_utc"]),
        "news_entities": _publish_partitioned(s3, bucket, release_id, "news_entities", entities,
            ["publication_date", "news_id", "entity_type", "entity_text", "entity_normalized",
             "confidence", "language", "text_hash", "analyzed_at_utc"], {"confidence"}),
        "news_key_phrases": _publish_partitioned(s3, bucket, release_id, "news_key_phrases", phrases,
            ["publication_date", "news_id", "phrase_text", "phrase_normalized", "confidence",
             "language", "text_hash", "analyzed_at_utc"], {"confidence"}),
    }
    metrics = {"records": len(enriched), "source_rows": len(source_partitioned),
               "entity_rows": len(entities), "key_phrase_rows": len(phrases),
               "cache_hits": hits, "cache_misses": misses, "comprehend_calls": calls,
               "skipped": sum(row["analysis_status"] not in {"COMPLETE", "CACHE_HIT"} for row in enriched)}
    pointer = {"schema_version": 2, "build_version": BUILD_VERSION, "stage": "NLP",
               "release_id": release_id, "silver_release_id": silver["release_id"],
               "published_at_utc": datetime.now(timezone.utc).isoformat(),
               "objects": {"news_nlp": legacy_key}, "tables": tables, "metrics": metrics}
    body = _json_bytes(pointer)
    s3.put_object(Bucket=bucket, Key="control/nlp/current.json", Body=body, ContentType="application/json")
    s3.put_object(Bucket=bucket, Key=f"control/nlp/releases/{release_id}.json", Body=body,
                  ContentType="application/json")
    return dict(metrics, reused_release=False, release_id=release_id)


"""Construccion reproducible de agregados Gold desde Silver y NLP publicados."""
from __future__ import annotations

import hashlib
import io
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def _read_parquet(s3, bucket, key):
    import pyarrow.parquet as pq
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(raw)).to_pylist()


def _json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _normalized(value):
    value = unicodedata.normalize("NFKC", value or "").casefold()
    return re.sub(r"\s+", " ", value).strip()


def _publication_date(row):
    value = row.get("pub_date_utc")
    return value[:10] if value and len(value) >= 10 else None


def _parquet_bytes(rows, fields, integer_fields=()):
    import pyarrow as pa
    import pyarrow.parquet as pq
    schema = pa.schema([
        pa.field(name, pa.int64() if name in integer_fields else pa.string(), nullable=False)
        for name in fields
    ])
    table = pa.Table.from_pylist(
        [{name: row[name] for name in fields} for row in rows], schema=schema
    )
    output = io.BytesIO()
    pq.write_table(table, output, compression="snappy")
    return output.getvalue()


def _put_immutable(s3, bucket, key, body, content_type):
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") not in {"PreconditionFailed", "412"}:
            raise
        current = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if current != body:
            raise RuntimeError(f"El objeto Gold inmutable ya existe con otro contenido: {key}")


def _aggregate(news, sources, entities, phrases):
    news_by_id = {row["news_id"]: row for row in news}
    sections = defaultdict(set)
    for row in sources:
        sections[row["news_id"]].add((row["medio"], row["seccion"]))

    daily_sets = defaultdict(set)
    for news_id, row in news_by_id.items():
        date = _publication_date(row)
        if not date:
            continue
        for medio, seccion in sections.get(news_id, {(row["medio"], row["section_primary"])}):
            daily_sets[(date, medio, seccion)].add(news_id)

    entity_sets, phrase_sets = defaultdict(set), defaultdict(set)
    for entity in entities:
        news_id = entity["news_id"]
        row = news_by_id.get(news_id)
        if not row or not _publication_date(row):
            continue
        date = _publication_date(row)
        medios = {medio for medio, _ in sections.get(news_id, {(row["medio"], row["section_primary"])})}
        kind, normalized = entity["entity_type"], entity["entity_normalized"]
        if kind in {"PERSON", "ORGANIZATION", "LOCATION"} and normalized:
            for medio in medios:
                entity_sets[(date, medio, kind, normalized)].add(news_id)

    for phrase in phrases:
        news_id = phrase["news_id"]
        row = news_by_id.get(news_id)
        if not row or not _publication_date(row):
            continue
        date, normalized = _publication_date(row), phrase["phrase_normalized"]
        for medio, seccion in sections.get(news_id, {(row["medio"], row["section_primary"])}):
            if normalized:
                phrase_sets[(date, medio, seccion, normalized)].add(news_id)

    news_daily = [
        {"publication_date": key[0], "medio": key[1], "seccion": key[2], "news_count": len(ids)}
        for key, ids in sorted(daily_sets.items())
    ]
    entity_daily = [
        {"publication_date": key[0], "medio": key[1], "entity_type": key[2],
         "entity_normalized": key[3], "news_count": len(ids)}
        for key, ids in sorted(entity_sets.items())
    ]
    phrase_daily = [
        {"publication_date": key[0], "medio": key[1], "seccion": key[2],
         "phrase_normalized": key[3], "news_count": len(ids)}
        for key, ids in sorted(phrase_sets.items())
    ]
    return news_daily, entity_daily, phrase_daily


def _publish_table(s3, bucket, release_id, table_name, rows, fields):
    partitions = []
    dates = sorted({row["publication_date"] for row in rows})
    # Athena obtiene publication_date de la particion Hive; no se duplica en Parquet.
    parquet_fields = [field for field in fields if field != "publication_date"]
    for date in dates:
        selected = [row for row in rows if row["publication_date"] == date]
        key = (f"gold/releases/release_id={release_id}/{table_name}/"
               f"publication_date={date}/part-000.parquet")
        body = _parquet_bytes(selected, parquet_fields, integer_fields={"news_count"})
        _put_immutable(s3, bucket, key, body, "application/vnd.apache.parquet")
        partitions.append({"publication_date": date, "key": key, "rows": len(selected)})
    return {"base_prefix": f"gold/releases/release_id={release_id}/{table_name}/",
            "partitions": partitions, "rows": len(rows)}


def _read_partitioned(s3, bucket, table):
    rows = []
    for partition in table.get("partitions", []):
        part_rows = _read_parquet(s3, bucket, partition["key"])
        for row in part_rows:
            row["publication_date"] = partition["publication_date"]
        rows.extend(part_rows)
    return rows


def process_gold(s3, bucket):
    silver = _read_json(s3, bucket, "control/silver/current.json")
    nlp_pointer = _read_json(s3, bucket, "control/nlp/current.json")
    if nlp_pointer.get("silver_release_id") != silver.get("release_id"):
        raise RuntimeError("NLP y Silver no pertenecen a la misma release")

    previous = None
    try:
        previous = _read_json(s3, bucket, "control/gold/current.json")
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") not in {"NoSuchKey", "404"}:
            raise
    if (previous and previous.get("nlp_release_id") == nlp_pointer["release_id"]
            and previous.get("build_version") == "gold-v3"):
        return dict(previous["metrics"], reused_release=True, release_id=previous["release_id"])

    news = _read_parquet(s3, bucket, silver["objects"]["news"])
    sources = _read_parquet(s3, bucket, silver["objects"]["news_sources"])
    entities = _read_partitioned(s3, bucket, nlp_pointer["tables"]["news_entities"])
    phrases = _read_partitioned(s3, bucket, nlp_pointer["tables"]["news_key_phrases"])
    news_daily, entity_daily, phrase_daily = _aggregate(news, sources, entities, phrases)

    release_id = hashlib.sha256(
        f"{silver['release_id']}:{nlp_pointer['release_id']}:gold-v3".encode()
    ).hexdigest()[:20]
    tables = {
        "news_daily": _publish_table(s3, bucket, release_id, "news_daily", news_daily,
            ["publication_date", "medio", "seccion", "news_count"]),
        "entity_daily": _publish_table(s3, bucket, release_id, "entity_daily", entity_daily,
            ["publication_date", "medio", "entity_type", "entity_normalized", "news_count"]),
        "key_phrase_section_daily": _publish_table(
            s3, bucket, release_id, "key_phrase_section_daily", phrase_daily,
            ["publication_date", "medio", "seccion", "phrase_normalized", "news_count"]),
    }
    metrics = {"news_daily_rows": len(news_daily), "entity_daily_rows": len(entity_daily),
               "key_phrase_daily_rows": len(phrase_daily),
               "undated_news": sum(_publication_date(row) is None for row in news)}
    pointer = {
        "schema_version": 1, "build_version": "gold-v3", "stage": "GOLD", "release_id": release_id,
        "silver_release_id": silver["release_id"], "nlp_release_id": nlp_pointer["release_id"],
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "tables": tables, "metrics": metrics,
    }
    body = _json_bytes(pointer)
    _put_immutable(s3, bucket, f"control/gold/releases/{release_id}.json", body, "application/json")
    s3.put_object(Bucket=bucket, Key="control/gold/current.json", Body=body, ContentType="application/json")
    release = {"schema_version": 1, "silver": silver, "nlp": nlp_pointer, "gold": pointer}
    s3.put_object(Bucket=bucket, Key="control/current_release.json",
                  Body=_json_bytes(release), ContentType="application/json")
    return dict(metrics, reused_release=False, release_id=release_id)


def verify_gold(s3, bucket, release_id):
    pointer = _read_json(s3, bucket, "control/gold/current.json")
    if pointer["release_id"] != release_id:
        raise RuntimeError("Gold publicado no coincide con la release construida")
    for name, table in pointer["tables"].items():
        actual = sum(len(_read_parquet(s3, bucket, part["key"])) for part in table["partitions"])
        if actual != table["rows"]:
            raise RuntimeError(f"Conteo inconsistente en {name}")
    return {"status": "VERIFIED", "release_id": release_id, **pointer["metrics"]}


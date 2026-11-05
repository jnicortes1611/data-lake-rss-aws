"""Transformacion Bronze -> Silver con snapshots publicados de forma atomica."""
from __future__ import annotations

import hashlib
import html
import io
import json
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

from rss_lake.bronze import get_optional, json_bytes, now_utc, put_new, save_control


class _TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.ignored = 0

    def handle_starttag(self, tag, attrs):
        if tag.lower() in {"script", "style"}:
            self.ignored += 1

    def handle_endtag(self, tag):
        if tag.lower() in {"script", "style"} and self.ignored:
            self.ignored -= 1

    def handle_data(self, data):
        if not self.ignored:
            self.parts.append(data)


def clean_text(value):
    parser = _TextExtractor()
    parser.feed(value or "")
    parser.close()
    return re.sub(r"\s+", " ", html.unescape(" ".join(parser.parts))).strip()


def canonical_url(value):
    value = (value or "").strip()
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""
    discarded = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "output"}
    query = [(k, v) for k, v in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
             if k.lower() not in discarded]
    path = re.sub(r"/+", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urllib.parse.urlunsplit(("https", parsed.hostname.lower(), path, urllib.parse.urlencode(sorted(query)), ""))


def normalized_date(value):
    original = clean_text(value)
    if not original:
        return None, None
    try:
        parsed = parsedate_to_datetime(original)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return original, parsed.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError, OverflowError):
        try:
            parsed = datetime.fromisoformat(original.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return original, parsed.astimezone(timezone.utc).isoformat()
        except ValueError:
            return original, None


def _local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


def _field(item, *names):
    wanted = {name.lower() for name in names}
    for child in item:
        if _local_name(child.tag) in wanted:
            return "".join(child.itertext())
    return ""


def parse_feed(raw, source, batch_id, observed_at):
    root = ET.fromstring(raw)
    valid, rejected = [], []
    for index, item in enumerate(root.findall("./channel/item")):
        title = clean_text(_field(item, "title"))
        description = clean_text(_field(item, "description", "encoded"))
        link_raw = clean_text(_field(item, "link"))
        link = canonical_url(link_raw)
        guid = clean_text(_field(item, "guid"))
        date_original, date_utc = normalized_date(_field(item, "pubDate", "date"))
        reasons = []
        if not title:
            reasons.append("MISSING_TITLE")
        if not link:
            reasons.append("INVALID_OR_MISSING_LINK")
        if date_original and not date_utc:
            reasons.append("INVALID_PUBLICATION_DATE")
        if reasons:
            rejected.append({
                "batch_id": batch_id, "source_id": source["source_id"], "item_index": index,
                "reason": "|".join(reasons), "raw_guid": guid, "raw_link": link_raw,
                "raw_title": title, "quarantined_at_utc": observed_at,
            })
            continue
        news_id = hashlib.sha256(link.encode("utf-8")).hexdigest()
        content_basis = json.dumps([title, description, date_utc], ensure_ascii=False, separators=(",", ":"))
        valid.append({
            "news_id": news_id, "canonical_url": link, "title": title, "description": description,
            "pub_date_original": date_original, "pub_date_utc": date_utc,
            "content_hash": hashlib.sha256(content_basis.encode("utf-8")).hexdigest(),
            "source_id": source["source_id"], "medio": source["medio"], "seccion": source["seccion"],
            "batch_id": batch_id, "observed_at_utc": observed_at,
        })
    return valid, rejected


NEWS_FIELDS = [
    "news_id", "canonical_url", "title", "description", "pub_date_original", "pub_date_utc",
    "content_hash", "version", "first_seen_at_utc", "last_seen_at_utc", "last_batch_id",
    "last_source_id", "medio", "section_primary",
]
SOURCE_FIELDS = [
    "news_id", "source_id", "medio", "seccion", "first_seen_batch_id", "last_seen_batch_id",
    "first_seen_at_utc", "last_seen_at_utc",
]
QUARANTINE_FIELDS = [
    "batch_id", "source_id", "item_index", "reason", "raw_guid", "raw_link", "raw_title",
    "quarantined_at_utc",
]


def write_parquet(rows, fields):
    import pyarrow as pa
    import pyarrow.parquet as pq
    normalized = [{field: row.get(field) for field in fields} for row in rows]
    integer_fields = {"version", "item_index"}
    schema = pa.schema([
        pa.field(field, pa.int64() if field in integer_fields else pa.string(), nullable=True)
        for field in fields
    ])
    # El esquema es explicito para que un lote sin cuarentena no cambie los tipos en Glue.
    table = pa.Table.from_pylist(normalized, schema=schema)
    output = io.BytesIO()
    pq.write_table(table, output, compression="snappy")
    return output.getvalue()


def read_parquet(raw):
    import pyarrow.parquet as pq
    return pq.read_table(io.BytesIO(raw)).to_pylist()


def _read_json(s3, bucket, key):
    obj = get_optional(s3, bucket, key)
    return None if obj is None else json.loads(obj["Body"].read())


def _read_rows(s3, bucket, key, decoder):
    obj = get_optional(s3, bucket, key)
    if obj is None:
        raise RuntimeError(f"Falta objeto Silver publicado: {key}")
    return decoder(obj["Body"].read())


def _put_immutable(s3, bucket, key, body, content_type):
    if put_new(s3, bucket, key, body, content_type):
        return
    previous = get_optional(s3, bucket, key)["Body"].read()
    if previous != body:
        raise RuntimeError(f"La version Silver existente no coincide: {key}")


def process_silver(s3, bucket, batch_id, encoder=write_parquet, decoder=read_parquet):
    manifest_key = f"control/batches/{batch_id}/manifest.json"
    manifest = _read_json(s3, bucket, manifest_key)
    if not manifest or manifest.get("stage") != "BRONZE" or manifest.get("status") != "COMPLETE":
        raise ValueError("Se requiere un manifiesto Bronze COMPLETE")

    pointer_key = "control/silver/current.json"
    previous_pointer = _read_json(s3, bucket, pointer_key)
    if previous_pointer and batch_id in previous_pointer.get("processed_batches", []):
        return dict(previous_pointer["metrics"], reused_release=True,
                    release_id=previous_pointer["release_id"], pointer_key=pointer_key)

    previous_news, previous_sources = [], []
    processed_batches = []
    previous_release = "initial"
    if previous_pointer:
        previous_release = previous_pointer["release_id"]
        processed_batches = list(previous_pointer.get("processed_batches", []))
        previous_news = _read_rows(s3, bucket, previous_pointer["objects"]["news"], decoder)
        previous_sources = _read_rows(s3, bucket, previous_pointer["objects"]["news_sources"], decoder)

    observed_at = now_utc()
    incoming, rejected = [], []
    prefix = f"s3://{bucket}/"
    for source in manifest["sources"]:
        uri = source.get("bronze_s3_uri", "")
        if not uri.startswith(prefix):
            raise ValueError("Referencia Bronze fuera del bucket")
        raw = get_optional(s3, bucket, uri[len(prefix):])
        if raw is None:
            raise RuntimeError(f"Falta original Bronze de {source['source_id']}")
        rows, bad = parse_feed(raw["Body"].read(), source, batch_id, observed_at)
        incoming.extend(rows)
        rejected.extend(bad)

    current_news = {row["news_id"]: row for row in previous_news}
    current_sources = {(row["news_id"], row["source_id"]): row for row in previous_sources}
    metrics = {"items_read": len(incoming) + len(rejected), "new": 0, "updated": 0,
               "unchanged": 0, "duplicates_in_batch": 0, "quarantined": len(rejected)}
    seen_in_batch = set()
    for row in incoming:
        existing = current_news.get(row["news_id"])
        if row["news_id"] in seen_in_batch:
            metrics["duplicates_in_batch"] += 1
        else:
            seen_in_batch.add(row["news_id"])
            if existing is None:
                metrics["new"] += 1
            elif existing["content_hash"] != row["content_hash"]:
                metrics["updated"] += 1
            else:
                metrics["unchanged"] += 1

        if existing is None:
            current_news[row["news_id"]] = {
                "news_id": row["news_id"], "canonical_url": row["canonical_url"], "title": row["title"],
                "description": row["description"], "pub_date_original": row["pub_date_original"],
                "pub_date_utc": row["pub_date_utc"], "content_hash": row["content_hash"], "version": 1,
                "first_seen_at_utc": observed_at, "last_seen_at_utc": observed_at,
                "last_batch_id": batch_id, "last_source_id": row["source_id"], "medio": row["medio"],
                "section_primary": row["seccion"],
            }
        elif existing["content_hash"] != row["content_hash"]:
            existing.update({
                "title": row["title"], "description": row["description"],
                "pub_date_original": row["pub_date_original"], "pub_date_utc": row["pub_date_utc"],
                "content_hash": row["content_hash"], "version": int(existing["version"]) + 1,
                "last_seen_at_utc": observed_at, "last_batch_id": batch_id,
                "last_source_id": row["source_id"],
            })
        else:
            existing.update({"last_seen_at_utc": observed_at, "last_batch_id": batch_id,
                             "last_source_id": row["source_id"]})

        association_key = (row["news_id"], row["source_id"])
        association = current_sources.get(association_key)
        if association is None:
            current_sources[association_key] = {
                "news_id": row["news_id"], "source_id": row["source_id"], "medio": row["medio"],
                "seccion": row["seccion"], "first_seen_batch_id": batch_id, "last_seen_batch_id": batch_id,
                "first_seen_at_utc": observed_at, "last_seen_at_utc": observed_at,
            }
        else:
            association.update({"last_seen_batch_id": batch_id, "last_seen_at_utc": observed_at})

    news_rows = sorted(current_news.values(), key=lambda row: row["news_id"])
    source_rows = sorted(current_sources.values(), key=lambda row: (row["news_id"], row["source_id"]))
    release_id = hashlib.sha256(f"{previous_release}:{batch_id}".encode()).hexdigest()[:20]
    base = f"silver/releases/release_id={release_id}"
    objects = {
        "news": f"{base}/news/news.parquet",
        "news_sources": f"{base}/news_sources/news_sources.parquet",
        "quarantine": f"quarantine/silver/batch_id={batch_id}/rejected.parquet",
    }
    encoded = {
        "news": encoder(news_rows, NEWS_FIELDS),
        "news_sources": encoder(source_rows, SOURCE_FIELDS),
        "quarantine": encoder(rejected, QUARANTINE_FIELDS),
    }
    for name, key in objects.items():
        _put_immutable(s3, bucket, key, encoded[name], "application/vnd.apache.parquet")

    metrics.update({"news_current": len(news_rows), "source_associations": len(source_rows)})
    pointer = {
        "schema_version": 1, "stage": "SILVER", "release_id": release_id,
        "published_at_utc": observed_at, "last_batch_id": batch_id,
        "processed_batches": processed_batches + [batch_id], "objects": objects, "metrics": metrics,
    }
    # El puntero se actualiza al final: una version incompleta nunca queda publicada.
    save_control(s3, bucket, pointer_key, pointer)
    save_control(s3, bucket, f"control/silver/releases/{release_id}.json", pointer)
    return dict(metrics, reused_release=False, release_id=release_id, pointer_key=pointer_key)


def verify_silver(s3, bucket, release_id, decoder=read_parquet):
    pointer = _read_json(s3, bucket, "control/silver/current.json")
    if not pointer or pointer["release_id"] != release_id:
        raise RuntimeError("La version Silver no es la version publicada")
    news = _read_rows(s3, bucket, pointer["objects"]["news"], decoder)
    sources = _read_rows(s3, bucket, pointer["objects"]["news_sources"], decoder)
    if len({row["news_id"] for row in news}) != len(news):
        raise RuntimeError("Silver news contiene duplicados")
    if any(row["news_id"] not in {item["news_id"] for item in news} for row in sources):
        raise RuntimeError("news_sources contiene referencias sin noticia")
    if len(news) != pointer["metrics"]["news_current"]:
        raise RuntimeError("Conteo Silver inconsistente")
    return {"release_id": release_id, "status": "VERIFIED", "news_current": len(news),
            "source_associations": len(sources)}

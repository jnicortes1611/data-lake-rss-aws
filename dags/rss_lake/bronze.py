"""Ingesta RSS a Bronze, con originales inmutables y reproceso sin HTTP.

Dependencia externa: boto3 (se importa solo al ejecutar, no al leer el DAG).
El manifiesto describe exclusivamente la etapa Bronze, no todo el data lake.
"""
import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

LOG = logging.getLogger(__name__)
MAX_BYTES = 10 * 1024 * 1024
VALID_STATUSES = {"OK", "EMPTY_FEED"}
IDENTIFIER = re.compile(r"[a-zA-Z0-9_-]{1,100}\Z")


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def error_code(error):
    return str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))


def json_bytes(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, indent=2).encode("utf-8")


def get_optional(s3, bucket, key):
    try:
        return s3.get_object(Bucket=bucket, Key=key)
    except Exception as error:
        if error_code(error) in {"NoSuchKey", "404", "NotFound"}:
            return None
        # AccessDenied no se trata como archivo inexistente.
        raise


def put_new(s3, bucket, key, body, content_type, metadata=None):
    """Escritura condicional: devuelve False si otro intento ya guardo el objeto."""
    try:
        s3.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType=content_type,
            ServerSideEncryption="AES256", Metadata=metadata or {}, IfNoneMatch="*",
        )
        return True
    except Exception as error:
        if error_code(error) in {"PreconditionFailed", "412"}:
            return False
        raise


def save_control(s3, bucket, key, value):
    s3.put_object(
        Bucket=bucket, Key=key, Body=json_bytes(value),
        ContentType="application/json", ServerSideEncryption="AES256",
    )


def inspect_xml(raw):
    if not raw.strip():
        raise ValueError("EMPTY_BODY")
    if len(raw) > MAX_BYTES:
        raise ValueError("RESPONSE_TOO_LARGE")
    declarations = raw.replace(b"\x00", b"").upper()
    if b"<!DOCTYPE" in declarations or b"<!ENTITY" in declarations:
        raise ValueError("UNSAFE_XML_DECLARATION")
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as error:
        raise ValueError("MALFORMED_XML") from error
    if root.tag != "rss" or root.find("channel") is None:
        raise ValueError("NOT_RSS")
    count = len(root.findall("./channel/item"))
    return {"record_count": count, "status": "OK" if count else "EMPTY_FEED"}


def load_sources():
    sources = json.loads(Path(__file__).with_name("sources.json").read_text(encoding="utf-8"))
    ids = [source["source_id"] for source in sources]
    if len(ids) != 3 or len(set(ids)) != 3:
        raise ValueError("Se requieren tres fuentes con identificadores distintos")
    for source in sources:
        if not IDENTIFIER.fullmatch(source["source_id"]):
            raise ValueError("source_id invalido")
        parsed = urllib.parse.urlsplit(source["url"])
        if parsed.scheme != "https" or parsed.hostname != "www.eltiempo.com":
            raise ValueError("Se esperan RSS HTTPS de El Tiempo")
        if not source.get("medio") or not source.get("seccion"):
            raise ValueError("Falta medio o seccion")
    return sources


class FeedFailure(Exception):
    def __init__(self, status, message, http_status=None):
        super().__init__(message)
        self.status = status
        self.http_status = http_status


def fetch_feed(url):
    """Tres intentos para timeouts/429/5xx. Nunca descarga los articulos."""
    for attempt in range(3):
        try:
            request = urllib.request.Request(url, headers={
                "User-Agent": "EIA-RSS-Academic-Lab/1.0",
                "Accept": "application/rss+xml, application/xml, text/xml",
            })
            with urllib.request.urlopen(request, timeout=30) as response:
                status = response.status
                if status != 200:
                    raise FeedFailure("HTTP_ERROR", f"HTTP inesperado: {status}", status)
                raw = response.read(MAX_BYTES + 1)
                final_url = response.geturl()
            return raw, status, final_url
        except urllib.error.HTTPError as error:
            transient = error.code in {408, 429, 500, 502, 503, 504}
            if not transient or attempt == 2:
                raise FeedFailure("HTTP_ERROR", f"HTTP {error.code}", error.code) from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            if attempt == 2:
                raise FeedFailure("NETWORK_ERROR", str(error)) from error
        time.sleep(2 ** (attempt + 1))
    raise RuntimeError("Reintentos agotados")


def make_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-2"),
        config=Config(retries={"mode": "standard", "max_attempts": 3}, connect_timeout=10, read_timeout=30),
    )


def validate_environment():
    bucket = os.environ["DATA_LAKE_BUCKET"]
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
    s3 = make_client()
    actual = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint") or "us-east-1"
    if actual != region:
        raise ValueError(f"Region del bucket {actual}; region configurada {region}")
    load_sources()
    LOG.info("Configuracion valida bucket=%s region=%s", bucket, region)
    return {"bucket": bucket, "region": region}


def prepare_request(s3, bucket, run_id, sources, reprocess_batch_id):
    batch_id = reprocess_batch_id or "rss-" + digest(run_id.encode())[:24]
    if not IDENTIFIER.fullmatch(batch_id):
        raise ValueError("batch_id invalido")
    request_key = f"control/batches/{batch_id}/request.json"
    existing = get_optional(s3, bucket, request_key)
    if existing is None:
        if reprocess_batch_id:
            raise ValueError("El lote solicitado para reproceso no existe")
        created_at = now_utc()
        candidate = {
            "batch_id": batch_id, "original_run_id": run_id,
            "created_at_utc": created_at, "ingestion_date": created_at[:10],
            "sources": sources, "schema_version": 1,
        }
        put_new(s3, bucket, request_key, json_bytes(candidate), "application/json")
        existing = get_optional(s3, bucket, request_key)
    request = json.loads(existing["Body"].read())
    if request["batch_id"] != batch_id:
        raise ValueError("Manifiesto de solicitud inconsistente")
    return request


def restore_raw(s3, bucket, key, source, batch_id, obj):
    raw = obj["Body"].read()
    meta = json.loads(obj["Metadata"]["ingestion"])
    if meta["batch_id"] != batch_id or meta["source_id"] != source["source_id"]:
        raise ValueError("Referencia Bronze inconsistente")
    if digest(raw) != meta["content_sha256"]:
        raise ValueError("Hash Bronze inconsistente")
    inspected = inspect_xml(raw)
    if inspected["record_count"] != meta["record_count"] or inspected["status"] != meta["status"]:
        raise ValueError("Conteo Bronze inconsistente")
    metadata_key = key.rsplit("/", 1)[0] + "/metadata.json"
    expected = json_bytes(meta)
    if not put_new(s3, bucket, metadata_key, expected, "application/json"):
        previous = get_optional(s3, bucket, metadata_key)["Body"].read()
        if previous != expected:
            raise ValueError("Archivo metadata.json inconsistente")
    return dict(meta, metadata_s3_uri=f"s3://{bucket}/{metadata_key}")


def ingest_source(s3, bucket, request, source, reprocess, fetch):
    batch_id = request["batch_id"]
    key = (
        f"bronze/source={source['source_id']}/ingestion_date={request['ingestion_date']}/"
        f"run_id={batch_id}/feed.xml"
    )
    obj = get_optional(s3, bucket, key)
    reused = obj is not None
    if obj is None:
        if reprocess:
            return dict(source, status="MISSING_BRONZE", record_count=None, http_status=None,
                        error="Fuente sin archivo original en el lote; no se consulto el RSS")
        http_status = None
        try:
            raw, http_status, final_url = fetch(source["url"])
            if http_status != 200:
                raise FeedFailure("HTTP_ERROR", f"HTTP inesperado: {http_status}", http_status)
            inspected = inspect_xml(raw)
        except (FeedFailure, ValueError) as error:
            return dict(source, status=getattr(error, "status", str(error)), error=str(error),
                        record_count=None, http_status=getattr(error, "http_status", http_status),
                        attempted_at_utc=now_utc())
        meta = dict(
            source, **inspected, batch_id=batch_id, original_run_id=request["original_run_id"],
            ingested_at_utc=now_utc(), http_status=http_status, final_url=final_url,
            content_sha256=digest(raw), byte_count=len(raw), bronze_s3_uri=f"s3://{bucket}/{key}",
        )
        created = put_new(s3, bucket, key, raw, "application/rss+xml", {
            "ingestion": json.dumps(meta, ensure_ascii=True, separators=(",", ":")),
        })
        reused = not created
        obj = get_optional(s3, bucket, key)
    # Recuperable si el proceso fallo despues del XML y antes del JSON lateral.
    meta = restore_raw(s3, bucket, key, source, batch_id, obj)
    LOG.info("source=%s batch_id=%s status=%s records=%s reused=%s",
             source["source_id"], batch_id, meta["status"], meta["record_count"], reused)
    return dict(meta, reused_bronze=reused)


def ingest_batch(s3, bucket, run_id, sources, reprocess_batch_id="", fetch=fetch_feed):
    request = prepare_request(s3, bucket, run_id, sources, reprocess_batch_id)
    results = []
    for source in request["sources"]:
        try:
            result = ingest_source(s3, bucket, request, source, bool(reprocess_batch_id), fetch)
        except Exception as error:
            # Aun si una fuente falla en almacenamiento, intentar las restantes y auditar.
            LOG.exception("Fallo de procesamiento source=%s batch_id=%s", source["source_id"], request["batch_id"])
            result = dict(source, status="PROCESSING_ERROR", record_count=None,
                          error=f"{type(error).__name__}: {error}", http_status=None)
        LOG.info("Resultado source=%s batch_id=%s status=%s", source["source_id"], request["batch_id"], result["status"])
        results.append(result)
    successes = sum(r["status"] in VALID_STATUSES for r in results)
    status = "COMPLETE" if successes == len(results) else "PARTIAL" if successes else "FAILED"
    report = {
        "schema_version": 1, "stage": "BRONZE", "batch_id": request["batch_id"],
        "execution_id": str(uuid4()), "airflow_run_id": run_id,
        "mode": "reprocess" if reprocess_batch_id else "ingest",
        "completed_at_utc": now_utc(), "status": status,
        "sources_expected": len(results), "sources_succeeded": successes,
        "records_received": sum(r["record_count"] for r in results if r["status"] in VALID_STATUSES),
        "sources": results,
    }
    execution_key = f"control/executions/{report['execution_id']}.json"
    manifest_key = f"control/batches/{request['batch_id']}/manifest.json"
    # Historico de intentos inmutable + estado mas reciente de la etapa Bronze.
    put_new(s3, bucket, execution_key, json_bytes(report), "application/json")
    save_control(s3, bucket, manifest_key, report)
    return {
        "batch_id": request["batch_id"], "status": status,
        "manifest_key": manifest_key, "manifest_s3_uri": f"s3://{bucket}/{manifest_key}",
        "execution_s3_uri": f"s3://{bucket}/{execution_key}",
        "records_received": report["records_received"],
    }


def verify_batch(s3, bucket, manifest_key):
    obj = get_optional(s3, bucket, manifest_key)
    if obj is None:
        raise RuntimeError("No existe el manifiesto Bronze")
    report = json.loads(obj["Body"].read())
    failures = [f"{s['source_id']}={s['status']}" for s in report["sources"] if s["status"] not in VALID_STATUSES]
    if report["status"] != "COMPLETE" or failures:
        raise RuntimeError(f"Lote {report['batch_id']} {report['status']}: " + ", ".join(failures))
    total = 0
    for source in report["sources"]:
        prefix = f"s3://{bucket}/"
        if not source["bronze_s3_uri"].startswith(prefix):
            raise RuntimeError("Bucket inesperado en referencia Bronze")
        key = source["bronze_s3_uri"][len(prefix):]
        original = get_optional(s3, bucket, key)
        if original is None:
            raise RuntimeError(f"Falta original: {key}")
        meta = restore_raw(s3, bucket, key, source, report["batch_id"], original)
        total += meta["record_count"]
    if total != report["records_received"]:
        raise RuntimeError("Conteo recibido no coincide con originales Bronze")
    return {"batch_id": report["batch_id"], "verified_records": total, "status": "VERIFIED"}

"""Crea un lote Bronze controlado para validar calidad e incrementalidad."""
from __future__ import annotations

import copy
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from rss_lake.bronze import digest, get_optional, inspect_xml, json_bytes, now_utc, put_new, save_control

SCENARIO = re.compile(r"[a-zA-Z0-9_-]{1,40}\Z")


def _set(item, name, value):
    child = item.find(name)
    if child is None:
        child = ET.SubElement(item, name)
    child.text = value


def _controlled_xml(raw, scenario):
    root = ET.fromstring(raw)
    channel = root.find("channel")
    items = channel.findall("item")
    if not items:
        raise ValueError("El RSS base no contiene noticias")

    # Misma URL: Silver debe reconocer la noticia y aumentar version por cambio de texto.
    description = items[0].find("description")
    current = "" if description is None else (description.text or "")
    _set(items[0], "description", f"{current} [actualizacion controlada {scenario}]")

    # URL nueva y estable: debe producir exactamente una noticia nueva.
    new_item = copy.deepcopy(items[0])
    unique_url = f"https://www.eltiempo.com/pruebas-eia/noticia-controlada-{scenario}"
    _set(new_item, "title", f"Noticia nueva controlada {scenario}")
    _set(new_item, "link", unique_url)
    _set(new_item, "guid", unique_url)
    _set(new_item, "description", "Registro sintetico para validar incrementalidad del Data Lake.")
    _set(new_item, "pubDate", datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000"))
    channel.append(new_item)

    # Registro invalido: sin titulo ni enlace; debe ir a quarantine con motivo.
    invalid = ET.Element("item")
    _set(invalid, "description", f"Registro defectuoso controlado {scenario}")
    _set(invalid, "pubDate", "fecha-invalida")
    channel.append(invalid)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def create_validation_batch(s3, bucket, base_batch_id, scenario):
    if not SCENARIO.fullmatch(scenario):
        raise ValueError("scenario debe contener solo letras, numeros, guion o guion bajo")
    base_manifest = json.loads(s3.get_object(
        Bucket=bucket, Key=f"control/batches/{base_batch_id}/manifest.json")["Body"].read())
    if base_manifest.get("status") != "COMPLETE":
        raise ValueError("El lote base debe estar COMPLETE")

    batch_id = "validation-" + hashlib.sha256(f"{base_batch_id}:{scenario}".encode()).hexdigest()[:20]
    ingestion_date = now_utc()[:10]
    sources = [{key: source[key] for key in ("source_id", "url", "medio", "seccion")}
               for source in base_manifest["sources"]]
    request = {"schema_version": 1, "batch_id": batch_id,
               "original_run_id": f"controlled:{scenario}", "created_at_utc": now_utc(),
               "ingestion_date": ingestion_date, "sources": sources,
               "base_batch_id": base_batch_id, "scenario": scenario}
    request_key = f"control/batches/{batch_id}/request.json"
    if not put_new(s3, bucket, request_key, json_bytes(request), "application/json"):
        existing = json.loads(get_optional(s3, bucket, request_key)["Body"].read())
        if existing["base_batch_id"] != base_batch_id or existing["scenario"] != scenario:
            raise RuntimeError("El lote controlado existente no coincide")

    prepared = []
    prefix = f"s3://{bucket}/"
    for index, (source, base_source) in enumerate(zip(sources, base_manifest["sources"])):
        base_key = base_source["bronze_s3_uri"][len(prefix):]
        raw = s3.get_object(Bucket=bucket, Key=base_key)["Body"].read()
        if index == 0:
            raw = _controlled_xml(raw, scenario)
        inspected = inspect_xml(raw)
        key = (f"bronze/source={source['source_id']}/ingestion_date={ingestion_date}/"
               f"run_id={batch_id}/feed.xml")
        meta = dict(source, **inspected, batch_id=batch_id, original_run_id=request["original_run_id"],
                    ingested_at_utc=now_utc(), http_status=200, final_url=source["url"],
                    content_sha256=digest(raw), byte_count=len(raw),
                    bronze_s3_uri=f"s3://{bucket}/{key}", controlled_validation=True,
                    base_batch_id=base_batch_id)
        created = put_new(s3, bucket, key, raw, "application/rss+xml",
                          {"ingestion": json.dumps(meta, ensure_ascii=True, separators=(",", ":"))})
        if not created and s3.get_object(Bucket=bucket, Key=key)["Body"].read() != raw:
            raise RuntimeError(f"El original controlado ya existe con otro contenido: {key}")
        sidecar = key.rsplit("/", 1)[0] + "/metadata.json"
        put_new(s3, bucket, sidecar, json_bytes(meta), "application/json")
        prepared.append(meta)

    preview = {"schema_version": 1, "stage": "BRONZE", "status": "PREPARED",
               "batch_id": batch_id, "base_batch_id": base_batch_id, "scenario": scenario,
               "records_received": sum(item["record_count"] for item in prepared),
               "expected_effects": {"new": 1, "updated": 1, "quarantined": 1},
               "sources": prepared}
    save_control(s3, bucket, f"control/validation/{batch_id}.json", preview)
    return {"batch_id": batch_id, "base_batch_id": base_batch_id,
            "records_received": preview["records_received"],
            "expected_effects": preview["expected_effects"],
            "control_key": f"control/validation/{batch_id}.json"}


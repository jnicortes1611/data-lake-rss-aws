"""Registro explicito de releases en Glue y validacion con Athena."""
from __future__ import annotations

import json
import time


PARQUET_INPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"


def _read_json(s3, bucket, key):
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def _storage(location, columns):
    return {
        "Columns": [{"Name": name, "Type": kind} for name, kind in columns],
        "Location": location,
        "InputFormat": PARQUET_INPUT,
        "OutputFormat": PARQUET_OUTPUT,
        "Compressed": True,
        "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE, "Parameters": {"serialization.format": "1"}},
    }


def _upsert_table(glue, database, name, location, columns, partition_keys=()):
    table = {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
        "StorageDescriptor": _storage(location, columns),
        "PartitionKeys": [{"Name": key, "Type": "string"} for key in partition_keys],
    }
    try:
        glue.get_table(DatabaseName=database, Name=name)
        glue.update_table(DatabaseName=database, TableInput=table)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table)


def _register_partitions(glue, database, table, bucket, columns, partitions):
    values = []
    for part in partitions:
        location = f"s3://{bucket}/{part['key'].rsplit('/', 1)[0]}/"
        values.append({
            "Values": [part["publication_date"]],
            "StorageDescriptor": _storage(location, columns),
            "Parameters": {"classification": "parquet"},
        })
    for start in range(0, len(values), 100):
        response = glue.batch_create_partition(
            DatabaseName=database, TableName=table,
            PartitionInputList=values[start:start + 100],
        )
        errors = [item for item in response.get("Errors", [])
                  if item.get("ErrorDetail", {}).get("ErrorCode") != "AlreadyExistsException"]
        if errors:
            raise RuntimeError(f"No se pudieron registrar particiones de {table}: {errors}")


def ensure_workgroup(athena, bucket):
    configuration = {
        "ResultConfiguration": {"OutputLocation": f"s3://{bucket}/athena-results/"},
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": True,
        "BytesScannedCutoffPerQuery": 10 * 1024 * 1024,
    }
    try:
        athena.get_work_group(WorkGroup="rss_lab")
        athena.update_work_group(WorkGroup="rss_lab", ConfigurationUpdates={
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": 10 * 1024 * 1024,
            "ResultConfigurationUpdates": {"OutputLocation": f"s3://{bucket}/athena-results/"},
        })
    except athena.exceptions.InvalidRequestException:
        athena.create_work_group(Name="rss_lab", Configuration=configuration,
                                 Description="Consultas controladas del laboratorio RSS")


def register_release(s3, glue, athena, bucket):
    current = _read_json(s3, bucket, "control/current_release.json")
    silver, nlp, gold = current["silver"], current["nlp"], current["gold"]
    database = "rss_lake"
    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": database,
                                            "Description": "Data Lake RSS del taller EIA"})
    ensure_workgroup(athena, bucket)

    suffix = gold["release_id"][:12]
    names = {
        "news": f"news_{suffix}",
        "news_sources": f"news_sources_{suffix}",
        "news_entities": f"news_entities_{suffix}",
        "news_key_phrases": f"news_key_phrases_{suffix}",
        "news_daily": f"news_daily_{suffix}",
        "entity_daily": f"entity_daily_{suffix}",
        "key_phrase_section_daily": f"key_phrase_section_daily_{suffix}",
    }
    news_columns = [(name, "bigint" if name == "version" else "string") for name in [
        "news_id", "canonical_url", "title", "description", "pub_date_original", "pub_date_utc",
        "content_hash", "version", "first_seen_at_utc", "last_seen_at_utc", "last_batch_id",
        "last_source_id", "medio", "section_primary",
    ]]
    source_columns = [(name, "string") for name in [
        "news_id", "source_id", "medio", "seccion", "first_seen_batch_id", "last_seen_batch_id",
        "first_seen_at_utc", "last_seen_at_utc",
    ]]
    entity_columns = [(name, "double" if name == "confidence" else "string") for name in [
        "news_id", "entity_type", "entity_text", "entity_normalized", "confidence",
        "language", "text_hash", "analyzed_at_utc",
    ]]
    phrase_columns = [(name, "double" if name == "confidence" else "string") for name in [
        "news_id", "phrase_text", "phrase_normalized", "confidence", "language",
        "text_hash", "analyzed_at_utc",
    ]]
    silver_specs = {
        "news": news_columns,
        "news_sources": source_columns,
        "news_entities": entity_columns,
        "news_key_phrases": phrase_columns,
    }
    for logical, columns in silver_specs.items():
        table = nlp["tables"][logical]
        location = f"s3://{bucket}/{table['base_prefix']}"
        _upsert_table(glue, database, names[logical], location, columns, ["publication_date"])
        _register_partitions(glue, database, names[logical], bucket, columns, table["partitions"])

    gold_specs = {
        "news_daily": [("medio", "string"), ("seccion", "string"), ("news_count", "bigint")],
        "entity_daily": [("medio", "string"), ("entity_type", "string"),
                         ("entity_normalized", "string"), ("news_count", "bigint")],
        "key_phrase_section_daily": [("medio", "string"), ("seccion", "string"),
                                     ("phrase_normalized", "string"), ("news_count", "bigint")],
    }
    for logical, columns in gold_specs.items():
        table = gold["tables"][logical]
        location = f"s3://{bucket}/{table['base_prefix']}"
        _upsert_table(glue, database, names[logical], location, columns, ["publication_date"])
        _register_partitions(glue, database, names[logical], bucket, columns, table["partitions"])

    manifest = {"database": database, "workgroup": "rss_lab", "release_id": gold["release_id"],
                "tables": names, "athena_output": f"s3://{bucket}/athena-results/"}
    s3.put_object(Bucket=bucket, Key="control/catalog/current.json",
                  Body=json.dumps(manifest, sort_keys=True).encode(), ContentType="application/json")
    return manifest


def _query(athena, database, sql):
    started = athena.start_query_execution(QueryString=sql,
        QueryExecutionContext={"Database": database}, WorkGroup="rss_lab")
    query_id = started["QueryExecutionId"]
    for _ in range(90):
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            rows = athena.get_query_results(QueryExecutionId=query_id)["ResultSet"]["Rows"]
            value = int(rows[1]["Data"][0]["VarCharValue"])
            return {"query_id": query_id, "value": value,
                    "bytes_scanned": execution["Statistics"].get("DataScannedInBytes", 0)}
        if state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(execution["Status"].get("StateChangeReason", state))
        time.sleep(2)
    athena.stop_query_execution(QueryExecutionId=query_id)
    raise TimeoutError(f"Athena no termino la consulta {query_id}")


def validate_release(s3, athena, bucket, manifest):
    results = {}
    for logical in ("news", "news_entities", "news_key_phrases", "news_daily",
                    "entity_daily", "key_phrase_section_daily"):
        table = manifest["tables"][logical]
        results[logical] = _query(athena, manifest["database"], f'SELECT count(*) FROM "{table}"')
    if results["news_daily"]["value"] < 1:
        raise RuntimeError("Gold news_daily esta vacio")
    news, entities, phrases = (manifest["tables"][name]
                               for name in ("news", "news_entities", "news_key_phrases"))
    checks = {
        "duplicate_news_ids": f'''SELECT count(*) FROM (
            SELECT news_id FROM "{news}" GROUP BY news_id HAVING count(*) > 1)''',
        "invalid_required_fields": f'''SELECT count(*) FROM "{news}"
            WHERE news_id IS NULL OR trim(news_id) = '' OR title IS NULL OR trim(title) = ''
               OR canonical_url IS NULL OR NOT regexp_like(canonical_url, '^https?://')
               OR (pub_date_utc IS NOT NULL AND try(from_iso8601_timestamp(pub_date_utc)) IS NULL)''',
        "orphan_entities": f'''SELECT count(*) FROM "{entities}" e
            LEFT JOIN "{news}" n ON e.news_id = n.news_id WHERE n.news_id IS NULL''',
        "orphan_key_phrases": f'''SELECT count(*) FROM "{phrases}" p
            LEFT JOIN "{news}" n ON p.news_id = n.news_id WHERE n.news_id IS NULL''',
    }
    quality = {name: _query(athena, manifest["database"], sql) for name, sql in checks.items()}
    failures = {name: result["value"] for name, result in quality.items() if result["value"] != 0}
    if failures:
        raise RuntimeError(f"Validaciones de calidad fallidas: {failures}")
    results["quality"] = quality
    evidence = {"release_id": manifest["release_id"], "status": "VERIFIED", "queries": results}
    s3.put_object(Bucket=bucket, Key=f"control/catalog/validations/{manifest['release_id']}.json",
                  Body=json.dumps(evidence, sort_keys=True).encode(), ContentType="application/json")
    return evidence


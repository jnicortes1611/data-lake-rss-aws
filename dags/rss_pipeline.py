"""Pipeline integral RSS: Bronze -> Silver/NLP -> Gold -> Glue -> Athena."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="rss_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "mode": Param("ingest", enum=["ingest", "reprocess"]),
        "batch_id": Param("", type="string", description="Obligatorio en modo reprocess"),
        "max_records": Param(1000, type="integer", minimum=1, maximum=5000),
    },
    tags=["eia", "rss", "pipeline", "scheduled"],
)
def rss_pipeline():
    @task
    def validar_configuracion():
        from rss_lake.bronze import validate_environment
        context = get_current_context()
        if context["params"]["mode"] == "reprocess" and not context["params"]["batch_id"].strip():
            raise ValueError("batch_id es obligatorio para reprocesar desde Bronze")
        return validate_environment()

    @task(execution_timeout=timedelta(minutes=10))
    def ingerir_o_reprocesar(config):
        from rss_lake.bronze import ingest_batch, load_sources, make_client, verify_batch
        context = get_current_context()
        params = context["params"]
        original = params["batch_id"].strip() if params["mode"] == "reprocess" else ""
        result = ingest_batch(make_client(), config["bucket"], context["run_id"], load_sources(), original)
        if result["status"] != "COMPLETE":
            raise RuntimeError(f"Bronze {result['status']}: {result['manifest_s3_uri']}")
        verify_batch(make_client(), config["bucket"], result["manifest_key"])
        print(result)
        return result

    @task(execution_timeout=timedelta(minutes=15))
    def publicar_silver(config, batch):
        from rss_lake.bronze import make_client
        from rss_lake.silver import process_silver, verify_silver
        result = process_silver(make_client(), config["bucket"], batch["batch_id"])
        verify_silver(make_client(), config["bucket"], result["release_id"])
        print(result)
        return result

    @task(execution_timeout=timedelta(minutes=30))
    def enriquecer_nlp(config, silver_result):
        import boto3
        from rss_lake.nlp import process_nlp
        context = get_current_context()
        result = process_nlp(boto3.client("s3", region_name="us-east-2"),
            boto3.client("comprehend", region_name="us-east-2"), config["bucket"],
            context["params"]["max_records"])
        print(result)
        return result

    @task(execution_timeout=timedelta(minutes=15))
    def construir_gold(config, nlp_result):
        import boto3
        from rss_lake.gold import process_gold, verify_gold
        s3 = boto3.client("s3", region_name="us-east-2")
        result = process_gold(s3, config["bucket"])
        verify_gold(s3, config["bucket"], result["release_id"])
        print(result)
        return result

    @task(execution_timeout=timedelta(minutes=10))
    def registrar_catalogo(config, gold_result):
        import boto3
        from rss_lake.catalog import register_release
        session = boto3.session.Session(region_name="us-east-2")
        result = register_release(session.client("s3"), session.client("glue"),
                                  session.client("athena"), config["bucket"])
        print(result)
        return result

    @task(execution_timeout=timedelta(minutes=10))
    def validar_athena(config, manifest):
        import boto3
        from rss_lake.catalog import validate_release
        session = boto3.session.Session(region_name="us-east-2")
        result = validate_release(session.client("s3"), session.client("athena"),
                                  config["bucket"], manifest)
        print(result)
        return result

    @task
    def auditar_ejecucion(validation):
        import hashlib, os
        from rss_lake.bronze import make_client, now_utc, save_control
        context = get_current_context()
        batch = context["ti"].xcom_pull(task_ids="ingerir_o_reprocesar") or {}
        report = {
            "schema_version": 1, "stage": "PIPELINE", "airflow_run_id": context["run_id"],
            "batch_id": batch.get("batch_id") or context["params"].get("batch_id") or None,
            "mode": context["params"]["mode"], "completed_at_utc": now_utc(),
            "status": "COMPLETE", "catalog_release_id": validation["release_id"],
        }
        key_id = hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
        bucket = os.environ["DATA_LAKE_BUCKET"]
        save_control(make_client(), bucket, f"control/pipeline/executions/{key_id}.json", report)
        print(report)

    @task(trigger_rule="one_failed")
    def auditar_fallo():
        import hashlib, os
        from rss_lake.bronze import make_client, now_utc, save_control
        context = get_current_context()
        report = {
            "schema_version": 1, "stage": "PIPELINE", "airflow_run_id": context["run_id"],
            "batch_id": context["params"].get("batch_id") or None,
            "mode": context["params"]["mode"], "completed_at_utc": now_utc(),
            "status": "FAILED",
            "detail": "Consultar la ejecucion y la primera tarea roja en Airflow",
        }
        key_id = hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
        save_control(make_client(), os.environ["DATA_LAKE_BUCKET"],
                     f"control/pipeline/executions/{key_id}.json", report)
        print(report)

    config = validar_configuracion()
    batch = ingerir_o_reprocesar(config)
    silver = publicar_silver(config, batch)
    nlp = enriquecer_nlp(config, silver)
    gold = construir_gold(config, nlp)
    catalog = registrar_catalogo(config, gold)
    validation = validar_athena(config, catalog)
    success_audit = auditar_ejecucion(validation)
    failure_audit = auditar_fallo()
    for upstream in (config, batch, silver, nlp, gold, catalog, validation):
        upstream >> failure_audit


rss_pipeline()

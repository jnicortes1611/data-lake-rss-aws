"""Primer tramo del taller: configuracion -> ingesta Bronze -> verificacion.

Se crea pausado. Activarlo inicia programacion horaria durante el laboratorio.
"""
from datetime import datetime, timedelta, timezone
from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="rss_bronze",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "mode": Param("ingest", enum=["ingest", "reprocess"], description="Ingesta nueva o lectura de un lote Bronze existente"),
        "batch_id": Param("", type="string", description="Obligatorio para reprocess; copiarlo del manifiesto del lote original"),
    },
    tags=["eia", "rss", "bronze"],
)
def rss_bronze():
    @task
    def validar_configuracion():
        from rss_lake.bronze import validate_environment
        context = get_current_context()
        if context["params"]["mode"] == "reprocess" and not context["params"]["batch_id"].strip():
            raise ValueError("Para reprocess debes indicar batch_id")
        return validate_environment()

    @task(execution_timeout=timedelta(minutes=10))
    def ingerir_bronze(config):
        from rss_lake.bronze import ingest_batch, load_sources, make_client
        context = get_current_context()
        params = context["params"]
        original = params["batch_id"].strip() if params["mode"] == "reprocess" else ""
        result = ingest_batch(make_client(), config["bucket"], context["run_id"], load_sources(), original)
        print(result)
        # Fuente fallida no se presenta como ingesta exitosa; reintentar reutiliza originales existentes.
        if result["status"] != "COMPLETE":
            raise RuntimeError(f"Bronze {result['status']}. Revisar {result['manifest_s3_uri']}")
        return result

    @task
    def verificar_bronze(config, batch):
        from rss_lake.bronze import make_client, verify_batch
        result = verify_batch(make_client(), config["bucket"], batch["manifest_key"])
        print(result)
        return result

    config = validar_configuracion()
    batch = ingerir_bronze(config)
    verificar_bronze(config, batch)


rss_bronze()

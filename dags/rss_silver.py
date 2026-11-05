"""DAG de transformacion reproducible desde un lote Bronze existente."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="rss_silver",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={
        "batch_id": Param("", type="string", minLength=1,
                          description="batch_id de una ingesta Bronze COMPLETE"),
    },
    tags=["eia", "rss", "silver"],
)
def rss_silver():
    @task
    def validar_entrada():
        import os
        from rss_lake.bronze import make_client
        context = get_current_context()
        batch_id = context["params"]["batch_id"].strip()
        if not batch_id:
            raise ValueError("Debes indicar batch_id")
        bucket = os.environ["DATA_LAKE_BUCKET"]
        make_client().head_object(Bucket=bucket, Key=f"control/batches/{batch_id}/manifest.json")
        return {"bucket": bucket, "batch_id": batch_id}

    @task(execution_timeout=timedelta(minutes=15))
    def transformar(config):
        from rss_lake.bronze import make_client
        from rss_lake.silver import process_silver
        result = process_silver(make_client(), config["bucket"], config["batch_id"])
        print(result)
        return result

    @task
    def verificar(config, result):
        from rss_lake.bronze import make_client
        from rss_lake.silver import verify_silver
        verified = verify_silver(make_client(), config["bucket"], result["release_id"])
        print(verified)
        return verified

    config = validar_entrada()
    result = transformar(config)
    verificar(config, result)


rss_silver()

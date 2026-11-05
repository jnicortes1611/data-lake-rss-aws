"""DAG para Glue Data Catalog y validacion de la release mediante Athena."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="rss_catalog",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["eia", "rss", "glue", "athena"],
)
def rss_catalog():
    @task(execution_timeout=timedelta(minutes=10))
    def registrar():
        import os
        import boto3
        from rss_lake.catalog import register_release
        bucket = os.environ["DATA_LAKE_BUCKET"]
        session = boto3.session.Session(region_name="us-east-2")
        result = register_release(session.client("s3"), session.client("glue"),
                                  session.client("athena"), bucket)
        print(result)
        return result

    @task(execution_timeout=timedelta(minutes=10))
    def validar(manifest):
        import os
        import boto3
        from rss_lake.catalog import validate_release
        bucket = os.environ["DATA_LAKE_BUCKET"]
        session = boto3.session.Session(region_name="us-east-2")
        result = validate_release(session.client("s3"), session.client("athena"), bucket, manifest)
        print(result)
        return result

    validar(registrar())


rss_catalog()

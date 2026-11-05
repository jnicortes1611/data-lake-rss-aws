"""DAG para construir y verificar la capa Gold."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task


@dag(
    dag_id="rss_gold",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["eia", "rss", "gold"],
)
def rss_gold():
    @task(execution_timeout=timedelta(minutes=15))
    def construir():
        import os
        import boto3
        from rss_lake.gold import process_gold
        result = process_gold(boto3.client("s3", region_name="us-east-2"),
                              os.environ["DATA_LAKE_BUCKET"])
        print(result)
        return result

    @task
    def verificar(result):
        import os
        import boto3
        from rss_lake.gold import verify_gold
        checked = verify_gold(boto3.client("s3", region_name="us-east-2"),
                              os.environ["DATA_LAKE_BUCKET"], result["release_id"])
        print(checked)
        return checked

    verificar(construir())


rss_gold()

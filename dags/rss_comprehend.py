"""DAG Silver NLP v2 con cache y tablas normalizadas particionadas."""
from datetime import datetime, timedelta, timezone
from airflow.sdk import Param, dag, task


@dag(
    dag_id="rss_comprehend", start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"max_records": Param(1000, type="integer", minimum=1, maximum=5000)},
    tags=["eia", "rss", "comprehend", "silver"],
)
def rss_comprehend():
    @task(execution_timeout=timedelta(minutes=30))
    def enriquecer():
        import os, boto3
        from airflow.sdk import get_current_context
        from rss_lake.nlp import process_nlp
        context = get_current_context()
        result = process_nlp(boto3.client("s3", region_name="us-east-2"),
            boto3.client("comprehend", region_name="us-east-2"), os.environ["DATA_LAKE_BUCKET"],
            context["params"]["max_records"])
        print(result)
        return result

    @task
    def verificar(result):
        if result["records"] < 1 or result["entity_rows"] < 1 or result["key_phrase_rows"] < 1:
            raise RuntimeError("Las tablas NLP normalizadas quedaron vacias")
        print({"status": "VERIFIED", **result})

    verificar(enriquecer())


rss_comprehend()

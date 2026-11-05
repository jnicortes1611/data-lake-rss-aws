"""DAG auxiliar para preparar escenarios reproducibles de la seccion 4.5."""
from datetime import datetime, timezone
from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="rss_prepare_validation_batch",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
    params={
        "base_batch_id": Param("", type="string", minLength=1),
        "scenario": Param("validation1", type="string", minLength=1, maxLength=40),
    },
    tags=["eia", "rss", "validation", "bronze"],
)
def rss_prepare_validation_batch():
    @task
    def preparar():
        import os
        from rss_lake.bronze import make_client
        from rss_lake.validation_batch import create_validation_batch
        params = get_current_context()["params"]
        result = create_validation_batch(make_client(), os.environ["DATA_LAKE_BUCKET"],
                                         params["base_batch_id"].strip(), params["scenario"].strip())
        print(result)
        return result

    preparar()


rss_prepare_validation_batch()

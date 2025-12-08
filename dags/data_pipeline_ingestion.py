from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

PIPELINE_ROOT = Path(
    os.environ.get("AIRQUALITY_DATA_PIPELINE_ROOT", "/home/pc/airquality-data-pipeline")
)
RUN_SCRIPT = Path(
    os.environ.get(
        "AIRQUALITY_PRODUCER_RUNNER",
        PIPELINE_ROOT / "scripts" / "run_producer.sh",
    )
)


def build_task(task_id: str, module: str, dag: DAG) -> BashOperator:
    bash_command = f"{RUN_SCRIPT} {module}"
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        dag=dag,
        env={"PYTHONUNBUFFERED": "1"},
    )


default_args = {
    "owner": "airquality",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airquality_data_pipeline",
    description="Refresh station catalogs and ingest measurements for DE/NL/PL",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    de_stations = build_task("refresh_de_stations", "app.de_stations", dag)
    nl_stations = build_task("refresh_nl_stations", "app.nl_stations", dag)
    pl_stations = build_task("refresh_pl_stations", "app.pl_stations", dag)

    de_measurements = build_task("ingest_de_measurements", "app.de_measurements", dag)
    nl_measurements = build_task("ingest_nl_measurements", "app.nl_measurements", dag)
    pl_measurements = build_task("ingest_pl_measurements", "app.pl_measurements", dag)

    de_stations >> de_measurements
    nl_stations >> nl_measurements
    pl_stations >> pl_measurements

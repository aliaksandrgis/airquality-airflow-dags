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

default_args = {
    "owner": "airquality",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="airquality_housekeeping",
    description="Daily cleanup: prune old measurements and rotate logs",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # 03:00 UTC
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Prune measurements older than 7 days using the same pipeline runtime (.env, .venv)
    # as ingestion tasks.
    prune_measurements = BashOperator(
        task_id="prune_measurements",
        bash_command=f"{RUN_SCRIPT} app.housekeeping",
        env={"PYTHONUNBUFFERED": "1"},
    )

    # Delete local logs older than 7 days for spark job and producer.
    cleanup_logs = BashOperator(
        task_id="clean_logs",
        bash_command=r"""
        set -euo pipefail
        if [ -n "${SPARK_LOG_DIR:-}" ]; then
          find "${SPARK_LOG_DIR}" -type f -mmin +1440 -print -delete 2>/dev/null || true
        fi
        if [ -n "${PIPELINE_LOG_DIR:-}" ]; then
          find "${PIPELINE_LOG_DIR}" -type f -mmin +1440 -print -delete 2>/dev/null || true
        fi
        """,
        env={
            "SPARK_LOG_DIR": "{{ var.value.AIRQUALITY_SPARK_LOG_DIR | default('', true) }}",
            "PIPELINE_LOG_DIR": "{{ var.value.AIRQUALITY_PIPELINE_LOG_DIR | default('', true) }}",
        },
    )

    prune_measurements >> cleanup_logs

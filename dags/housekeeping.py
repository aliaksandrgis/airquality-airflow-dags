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
    # Rotate/truncate pipeline logs daily so single log files don't grow forever,
    # then delete rotated files older than 24h.
    cleanup_logs = BashOperator(
        task_id="clean_logs",
        bash_command=r"""
        set -euo pipefail
        ROTATE_SUFFIX="$(date -u +%F_%H%M%S)"

        # Pipeline logs are appended to a single file per module (app_*.log). To keep only
        # ~1 day of lines without rewriting big files, rotate by renaming and recreating.
        if [ -n "${PIPELINE_LOG_DIR:-}" ]; then
          mkdir -p "${PIPELINE_LOG_DIR}"
          shopt -s nullglob
          for f in "${PIPELINE_LOG_DIR}"/*.log; do
            [ -f "$f" ] || continue
            # Rotate only non-empty logs
            if [ -s "$f" ]; then
              mv "$f" "${f}.${ROTATE_SUFFIX}" 2>/dev/null || true
              : > "$f"
            fi
          done
          # Delete rotated logs older than 24h
          find "${PIPELINE_LOG_DIR}" -type f -name "*.log.*" -mmin +1440 -print -delete 2>/dev/null || true
        fi

        if [ -n "${SPARK_LOG_DIR:-}" ]; then
          # Spark streaming job writes continuously; removing the file won't help because
          # the running process will keep the file descriptor open. Truncate the file
          # instead to drop old lines while keeping the path stable.
          if [ -f "${SPARK_LOG_DIR}/live_measurements.log" ]; then
            : > "${SPARK_LOG_DIR}/live_measurements.log"
          fi
          find "${SPARK_LOG_DIR}" -type f -mmin +1440 -print -delete 2>/dev/null || true
        fi
        """,
        env={
            "SPARK_LOG_DIR": "{{ var.value.AIRQUALITY_SPARK_LOG_DIR | default('', true) }}",
            "PIPELINE_LOG_DIR": "{{ var.value.AIRQUALITY_PIPELINE_LOG_DIR | default('', true) }}",
        },
    )

    # Prune measurements older than 7 days using the same pipeline runtime (.env, .venv)
    # as ingestion tasks.
    prune_measurements = BashOperator(
        task_id="prune_measurements",
        bash_command=f"{RUN_SCRIPT} app.housekeeping",
        env={"PYTHONUNBUFFERED": "1"},
    )

    cleanup_logs >> prune_measurements

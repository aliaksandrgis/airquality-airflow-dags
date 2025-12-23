from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

PRODUCER_IMAGE = os.environ.get("AIRQUALITY_PRODUCER_IMAGE", "vps_producer:latest")


def _pipeline_env() -> dict[str, str]:
    keys = [
        # Kafka (Confluent Cloud)
        "KAFKA_BOOTSTRAP",
        "KAFKA_TOPIC",
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_SASL_MECHANISM",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
        # Supabase / Postgres catalog
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_SSLMODE",
    ]
    env: dict[str, str] = {}
    for key in keys:
        value = os.environ.get(key)
        if value is not None and value != "":
            env[key] = value
    return env

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
    cleanup_airflow_task_logs = BashOperator(
        task_id="cleanup_airflow_task_logs",
        bash_command=r"""
        set -euo pipefail
        DAYS="${AIRFLOW_TASK_LOG_RETENTION_DAYS:-14}"
        BASE="${AIRFLOW_TASK_LOG_DIR:-/opt/airflow/logs}"
        if [ -d "$BASE" ]; then
          find "$BASE" -type f -mtime +"$DAYS" -print -delete 2>/dev/null || true
          find "$BASE" -type d -empty -print -delete 2>/dev/null || true
        fi
        """,
    )

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
            "SPARK_LOG_DIR": "{{ var.value.get('AIRQUALITY_SPARK_LOG_DIR', '') }}",
            "PIPELINE_LOG_DIR": "{{ var.value.get('AIRQUALITY_PIPELINE_LOG_DIR', '') }}",
        },
    )

    prune_measurements = DockerOperator(
        task_id="prune_measurements",
        image=PRODUCER_IMAGE,
        command="app.housekeeping",
        environment={"PYTHONUNBUFFERED": "1", **_pipeline_env()},
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        mount_tmp_dir=False,
    )

    cleanup_airflow_task_logs >> cleanup_logs >> prune_measurements

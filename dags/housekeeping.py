from __future__ import annotations

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airquality",
    "retries": 0,
}


with DAG(
    dag_id="airquality_housekeeping",
    description="Daily cleanup: prune old measurements and rotate logs",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    # Remove measurements older than 7 days to keep the table lean.
    # Requires Supabase/Postgres creds in env: SUPABASE_HOST, SUPABASE_USER, SUPABASE_PASSWORD.
    # Adjust DB name/SSL as needed.
    prune_measurements = BashOperator(
        task_id="prune_old_measurements",
        bash_command=r"""
        set -euo pipefail
        : "${SUPABASE_HOST:?Missing SUPABASE_HOST}"
        : "${SUPABASE_USER:?Missing SUPABASE_USER}"
        : "${SUPABASE_PASSWORD:?Missing SUPABASE_PASSWORD}"
        DB_NAME="${SUPABASE_DB:-postgres}"

        export PGPASSWORD="${SUPABASE_PASSWORD}"
        psql "sslmode=require host=${SUPABASE_HOST} user=${SUPABASE_USER} dbname=${DB_NAME}" <<'SQL'
        delete from public.measurements_curated
        where observed_at < now() - interval '7 days';
        SQL
        """,
        env={
            "SUPABASE_HOST": "{{ var.value.get('SUPABASE_HOST', '') }}",
            "SUPABASE_USER": "{{ var.value.get('SUPABASE_USER', '') }}",
            "SUPABASE_PASSWORD": "{{ var.value.get('SUPABASE_PASSWORD', '') }}",
            "SUPABASE_DB": "{{ var.value.get('SUPABASE_DB', 'postgres') }}",
        },
    )

    # Delete local logs older than 7 days for spark job and producer.
    cleanup_logs = BashOperator(
        task_id="cleanup_logs",
        bash_command=r"""
        set -euo pipefail
        if [ -n "${SPARK_LOG_DIR:-}" ]; then
          find "${SPARK_LOG_DIR}" -type f -mtime +7 -delete 2>/dev/null || true
        fi
        if [ -n "${PIPELINE_LOG_DIR:-}" ]; then
          find "${PIPELINE_LOG_DIR}" -type f -mtime +7 -delete 2>/dev/null || true
        fi
        """,
        env={
            "SPARK_LOG_DIR": "{{ var.value.get('AIRQUALITY_SPARK_LOG_DIR', '') }}",
            "PIPELINE_LOG_DIR": "{{ var.value.get('AIRQUALITY_PIPELINE_LOG_DIR', '') }}",
        },
    )

    prune_measurements >> cleanup_logs

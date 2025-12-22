from __future__ import annotations

import os
from datetime import timedelta

from airflow import DAG
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
        # Producer behavior
        "PIPELINE_LIVE_API",
        "PIPELINE_SLEEP_SECONDS",
        # Supabase / Postgres catalog
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_SSLMODE",
        # Feature toggles / configs
        "PIPELINE_DE_BASE_URL",
        "PIPELINE_DE_STATIONS",
        "PIPELINE_NL_BASE_URL",
        "PIPELINE_PL_BASE_URL",
        "PIPELINE_PL_SENSORS",
        "PIPELINE_DISABLE_DE",
        "PIPELINE_DISABLE_NL",
        "PIPELINE_DISABLE_PL",
        "PIPELINE_PL_ONLY_AUTO",
    ]
    env: dict[str, str] = {}
    for key in keys:
        value = os.environ.get(key)
        if value is not None and value != "":
            env[key] = value
    return env


def build_task(task_id: str, module: str, dag: DAG) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        dag=dag,
        image=PRODUCER_IMAGE,
        command=module,
        environment={"PYTHONUNBUFFERED": "1", **_pipeline_env()},
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        mount_tmp_dir=False,
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
    disable_de = os.environ.get("PIPELINE_DISABLE_DE", "false").strip().lower() == "true"
    disable_nl = os.environ.get("PIPELINE_DISABLE_NL", "false").strip().lower() == "true"
    disable_pl = os.environ.get("PIPELINE_DISABLE_PL", "false").strip().lower() == "true"

    if not disable_de:
        de_stations = build_task("refresh_de_stations", "app.de_stations", dag)
        de_measurements = build_task("ingest_de_measurements", "app.de_measurements", dag)
        de_stations >> de_measurements

    if not disable_nl:
        nl_stations = build_task("refresh_nl_stations", "app.nl_stations", dag)
        nl_measurements = build_task("ingest_nl_measurements", "app.nl_measurements", dag)
        nl_stations >> nl_measurements

    if not disable_pl:
        pl_stations = build_task("refresh_pl_stations", "app.pl_stations", dag)
        pl_measurements = build_task("ingest_pl_measurements", "app.pl_measurements", dag)
        pl_stations >> pl_measurements

# AirQuality Airflow DAGs

Lightweight Airflow DAG collection for orchestration (trigger Databricks jobs, monitor ingestion, send alerts). Designed to run on Raspberry Pi 3B in LocalExecutor mode.

## Layout
- `dags/` — DAG definitions.
- `requirements.txt` — provider deps (minimal).

## Quick start (dev)
```bash
docker compose up airflow-webserver airflow-scheduler
```

## CI (template)
- Lint DAGs (ruff/flake8) and import check (`airflow dags list`).
- No secrets in repo; connections/vars set in Airflow UI or env.

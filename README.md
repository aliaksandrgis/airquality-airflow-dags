# AirQuality Airflow DAGs

Lightweight DAG collection for orchestration (station refresh, ingestion, monitoring). Target runtime: Raspberry Pi 3B (LocalExecutor).

## Layout
- `dags/` — DAG definitions (`dags/data_pipeline_ingestion.py`).
- `requirements.txt` — future CI deps.

## Quick start (dev)
```bash
docker compose up airflow-webserver airflow-scheduler
```

## VPS (Airflow + Spark)
See `deploy/vps/README.md`.

### DAG `airquality_data_pipeline`
- Requires the producer wrapper `scripts/run_producer.sh` from the `airquality-data-pipeline` repo. Configure paths via env vars:
  - `AIRQUALITY_DATA_PIPELINE_ROOT` (default `/home/pc/airquality-data-pipeline`);
  - `AIRQUALITY_PRODUCER_RUNNER` (defaults to `<root>/scripts/run_producer.sh`).
- DAG layout: six `BashOperator` tasks:
  1. `app.de_stations` → `app.de_measurements`;
  2. `app.nl_stations` → `app.nl_measurements`;
  3. `app.pl_stations` → `app.pl_measurements`.
  Each measurements task depends only on its station refresh. Default schedule: hourly (`0 * * * *`), `catchup=False`.
- The Airflow user must have permission to run the script, access the repo, `.venv`, and `.env`. Producer logs stay under `logs/app_<module>.log`.

## CI (template)
- Lint DAGs (ruff/flake8) and run `airflow dags list`.
- Keep secrets out of Git; manage via Airflow Connections/Variables.

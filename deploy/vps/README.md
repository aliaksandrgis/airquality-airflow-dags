# VPS deploy (Airflow + Spark)

This folder contains a minimal Docker Compose setup to run:
- Airflow (LocalExecutor + Postgres metadata DB)
- Spark streaming job (`airquality-spark-jobs`)

Assumption: repositories are checked out as siblings, e.g.:
```
/opt/airquality/
  airquality-airflow-dags/
  airquality-spark-jobs/
```

## Prereqs (Ubuntu)
Install Docker + Compose (either `docker compose` v2 or `docker-compose` v1).

## Setup
From `airquality-airflow-dags/deploy/vps`:
```bash
cp .env.airflow.example .env.airflow
cp .env.spark.example .env.spark
mkdir -p ./volumes/airflow/{logs,plugins} ./data/{bronze,tmp}
```

Generate Airflow keys and put them into `.env.airflow`:
```bash
python3 - <<'PY'
import base64,os,secrets
print("AIRFLOW__CORE__FERNET_KEY=" + base64.urlsafe_b64encode(os.urandom(32)).decode())
print("AIRFLOW__WEBSERVER__SECRET_KEY=" + secrets.token_hex(16))
PY
```

Fix volume permissions for Airflow (uid 50000 by default):
```bash
sudo chown -R 50000:0 ./volumes/airflow
sudo chmod -R u+rwX,g+rwX ./volumes/airflow
```

Initialize Airflow DB (one-time, or after upgrades):
```bash
docker-compose up -d airflow-postgres
docker-compose -f docker-compose.yml -f docker-compose.init.yml run --rm airflow-init
```

Start:
```bash
docker-compose up -d --build
docker-compose ps
```

## Airflow UI
Compose binds the webserver to `127.0.0.1:8080` on the host.
Access it via SSH tunnel or Tailscale SSH port forwarding.

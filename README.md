# AirQuality Airflow DAGs

Airflow DAGs for scheduling the AirQuality pipeline using DockerOperator.

## DAGs
- airquality_data_pipeline (hourly): refresh station catalogs and ingest measurements for DE/NL/PL.
- airquality_housekeeping (daily): rotate logs and prune old measurements.

## Runtime
Tasks run the producer Docker image (default vps_producer:latest) and require access to /var/run/docker.sock.

## Deploy
See deploy/vps/README.md.

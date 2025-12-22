FROM apache/airflow:2.8.1

ARG AIRFLOW_VERSION=2.8.1

USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-$(python -c 'import sys; print(f\"{sys.version_info.major}.{sys.version_info.minor}\")').txt" \
    apache-airflow-providers-docker==3.8.0 \
    docker==6.1.3

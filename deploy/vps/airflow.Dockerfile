FROM apache/airflow:2.8.1

ARG AIRFLOW_VERSION=2.8.1

USER airflow
RUN set -e; \
    PY_MAJMIN="$(python -c 'import sys; print("{}.{}".format(sys.version_info.major, sys.version_info.minor))')"; \
    pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_MAJMIN}.txt" \
      apache-airflow-providers-docker \
      docker

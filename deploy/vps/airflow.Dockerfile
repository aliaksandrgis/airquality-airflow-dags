FROM apache/airflow:2.8.1

ARG AIRFLOW_VERSION=2.8.1
# apache/airflow:2.8.1 currently uses Python 3.8
ARG PYTHON_VERSION=3.8
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow
RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    apache-airflow-providers-docker==3.8.0 \
    docker==6.1.3

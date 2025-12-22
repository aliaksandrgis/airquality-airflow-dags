FROM apache/airflow:2.8.1

USER root
RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.8.0 \
    docker==6.1.3

USER airflow


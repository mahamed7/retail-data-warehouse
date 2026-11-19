# Airflow image for the local Dukaan stack (see orchestration/README.md).
# Build context is the repository root:
#   docker compose -f orchestration/docker-compose.airflow.yml build
FROM apache/airflow:2.10.5-python3.12

# The pipeline and dbt go into their own virtualenv instead of Airflow's environment.
# dbt-core 1.10 needs protobuf>=6, while Airflow 2.10.5's constraints pin protobuf 4.25,
# so installing both in one environment would break one of them. The DAG calls the
# venv's executables through DUKAAN_PYTHON and DUKAAN_DBT.
USER root
RUN python -m venv /opt/dukaan-venv \
    && chown -R airflow:0 /opt/dukaan-venv

USER airflow
COPY --chown=airflow:0 requirements.txt /tmp/dukaan-requirements.txt
RUN /opt/dukaan-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dukaan-venv/bin/pip install --no-cache-dir -r /tmp/dukaan-requirements.txt

ENV DUKAAN_PYTHON=/opt/dukaan-venv/bin/python \
    DUKAAN_DBT=/opt/dukaan-venv/bin/dbt

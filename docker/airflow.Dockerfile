# Airflow + Java for local PySpark. The pipeline's own deps live in a separate venv
# built from uv.lock, so tasks run the exact versions the tests ran and Airflow's
# constraint pins never touch them (D-018).
FROM apache/airflow:slim-3.3.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:0.12.5 /uv /usr/local/bin/uv
RUN mkdir -p /opt/lichess && chown airflow:0 /opt/lichess

USER airflow
# The slim image has no Postgres driver; the extra is pinned by Airflow's constraints.
RUN pip install --no-cache-dir "apache-airflow[postgres]==3.3.2" \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt

WORKDIR /opt/lichess
COPY --chown=airflow:0 pyproject.toml uv.lock ./
RUN VIRTUAL_ENV= UV_PROJECT_ENVIRONMENT=/opt/lichess/.venv uv sync --frozen --no-dev --no-install-project \
        --python /usr/python/bin/python3.11 --no-python-downloads \
    && /opt/lichess/.venv/bin/python -c "import pyarrow, pyspark, zstandard"

FROM python:3.11-slim-bookworm

ARG DBT_CORE_VERSION=1.10.22
ARG DBT_CLICKHOUSE_VERSION=1.10.0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DBT_PROFILES_DIR=/usr/app/dbt

WORKDIR /usr/app/dbt

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip \
    && python -m pip install \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-clickhouse==${DBT_CLICKHOUSE_VERSION}" \
    && dbt --version

ENTRYPOINT ["dbt"]

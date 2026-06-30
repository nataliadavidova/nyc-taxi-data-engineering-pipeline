from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


DBT_IMAGE = "nyc_taxi_dbt:latest"
DOCKER_NETWORK = "nyc_taxi_network"

DEFAULT_ARGS = {
    "owner": "natalia",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Required environment variable is not set: {name}")
    return value


DBT_ENV = {
    "CLICKHOUSE_HOST": get_required_env("CLICKHOUSE_HOST"),
    "CLICKHOUSE_PORT": get_required_env("CLICKHOUSE_PORT"),
    "CLICKHOUSE_DATABASE": get_required_env("CLICKHOUSE_DATABASE"),
    "CLICKHOUSE_USER": get_required_env("CLICKHOUSE_USER"),
    "CLICKHOUSE_PASSWORD": get_required_env("CLICKHOUSE_PASSWORD"),
    "DBT_TARGET_DATABASE": get_required_env("DBT_TARGET_DATABASE"),
}


with DAG(
    dag_id="nyc_taxi_dbt_analytics_pipeline",
    description="Run dbt analytics layer for NYC Taxi ClickHouse marts.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc_taxi", "dbt", "analytics"],
) as dag:
    dbt_debug = DockerOperator(
        task_id="dbt_debug",
        image=DBT_IMAGE,
        command="debug",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment=DBT_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    dbt_build_analytics_layer = DockerOperator(
        task_id="dbt_build_analytics_layer",
        image=DBT_IMAGE,
        command="build",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        environment=DBT_ENV,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    dbt_debug >> dbt_build_analytics_layer

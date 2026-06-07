"""
Airflow DAG for period-based NYC Taxi pipeline refresh.

This DAG is a safer extension of nyc_taxi_pipeline.py.

Current supported mode:
    refresh_mode = "replace_period"

replace_period mode:
    For each selected year/month:
    - delete existing rows for this month from ClickHouse gold tables;
    - rebuild bronze/silver/gold data for this month;
    - load rebuilt gold marts to ClickHouse;
    - validate ClickHouse gold data for this month.

Why this DAG exists:
    The original nyc_taxi_pipeline.py performs a controlled full-year rebuild.
    This DAG introduces period-based refresh logic without breaking the
    existing stable pipeline.

Runtime configuration:
    The selected refresh period can be passed through Airflow Trigger DAG config.

Example one-month reload:
    {
        "start_year": "2024",
        "start_month": "05",
        "end_year": "2024",
        "end_month": "05",
        "refresh_mode": "replace_period"
    }

Example interval reload:
    {
        "start_year": "2024",
        "start_month": "01",
        "end_year": "2024",
        "end_month": "02",
        "refresh_mode": "replace_period"
    }

If no runtime config is provided, the DAG uses safe default values from
period_refresh_config.py.
"""

from __future__ import annotations

import shlex
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from config import (
    AIRFLOW_JOBS_DIR,
    AIRFLOW_PROJECT_DIR,
    AIRFLOW_RETRY_DELAY_MINUTES,
    BRONZE_TASK_EXECUTION_TIMEOUT_MINUTES,
    CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT_MINUTES,
    GOLD_LOCATION_TASK_EXECUTION_TIMEOUT_MINUTES,
    GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT_MINUTES,
    GOLD_STANDARD_TASK_EXECUTION_TIMEOUT_MINUTES,
    PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES,
    S3_ENDPOINT,
    SILVER_QUALITY_TASK_EXECUTION_TIMEOUT_MINUTES,
    SILVER_TASK_EXECUTION_TIMEOUT_MINUTES,
)

from period_refresh_config import (
    DEFAULT_END_MONTH,
    DEFAULT_END_YEAR,
    DEFAULT_REFRESH_MODE,
    DEFAULT_START_MONTH,
    DEFAULT_START_YEAR,
)

from airflow_callbacks import airflow_failure_callback

PROJECT_DIR = AIRFLOW_PROJECT_DIR
JOBS_DIR = AIRFLOW_JOBS_DIR

SPARK_SUBMIT_BASE = (
    "spark-submit "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    f"--conf spark.hadoop.fs.s3a.endpoint={S3_ENDPOINT} "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=true "
)

SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE = (
    "--packages "
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "com.clickhouse:clickhouse-jdbc:0.6.0"
)

SPARK_POOL = "spark_pool"

AIRFLOW_RETRY_DELAY = timedelta(minutes=AIRFLOW_RETRY_DELAY_MINUTES)

BRONZE_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=BRONZE_TASK_EXECUTION_TIMEOUT_MINUTES
)
SILVER_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=SILVER_TASK_EXECUTION_TIMEOUT_MINUTES
)
SILVER_QUALITY_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=SILVER_QUALITY_TASK_EXECUTION_TIMEOUT_MINUTES
)
GOLD_STANDARD_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT_MINUTES
)
GOLD_LOCATION_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=GOLD_LOCATION_TASK_EXECUTION_TIMEOUT_MINUTES
)
GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT_MINUTES
)
CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT_MINUTES
)

PYTHON_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES
)

default_args = {
    "owner": "natalia",
    "retries": 1,
    "retry_delay": AIRFLOW_RETRY_DELAY,
    "on_failure_callback": airflow_failure_callback,
}

PeriodParam = Dict[str, str]


def get_period_year(period: PeriodParam) -> str:
    """
    Return year from period dictionary.
    """

    return period["year"]


def get_period_month(period: PeriodParam) -> str:
    """
    Return month from period dictionary.
    """

    return period["month"]


def format_period(period: PeriodParam) -> str:
    """
    Format period for logs.
    """

    return f"{get_period_year(period)}-{get_period_month(period)}"


def run_shell_command(command: str) -> None:
    """
    Run a shell command and stream output to Airflow task logs.
    """

    print("Running command:")
    print(command)

    subprocess.run(
        ["bash", "-lc", command],
        check=True,
    )


def build_python_job_command(
    job_file: str,
    period: PeriodParam,
) -> str:
    """
    Build shell command for a lightweight Python monthly job.
    """

    year = get_period_year(period)
    month = get_period_month(period)

    return (
        f"echo {shlex.quote(f'Processing period: {year}-{month}')} && "
        f"cd {shlex.quote(PROJECT_DIR)} && "
        f"PYTHONPATH={shlex.quote(JOBS_DIR)} "
        f"python {shlex.quote(f'jobs/{job_file}')} "
        f"--year {shlex.quote(year)} "
        f"--month {shlex.quote(month)}"
    )


def build_spark_job_command(
    job_file: str,
    period: PeriodParam,
) -> str:
    """
    Build shell command for a Spark monthly job.
    """

    year = get_period_year(period)
    month = get_period_month(period)

    return (
        f"echo {shlex.quote(f'Processing period: {year}-{month}')} && "
        f"cd {shlex.quote(PROJECT_DIR)} && "
        f"PYTHONPATH={shlex.quote(JOBS_DIR)} "
        f"{SPARK_SUBMIT_BASE} "
        f"{shlex.quote(f'jobs/{job_file}')} "
        f"--year {shlex.quote(year)} "
        f"--month {shlex.quote(month)}"
    )


def build_clickhouse_load_command(
    job_file: str,
    period: PeriodParam,
) -> str:
    """
    Build shell command for a Spark-based ClickHouse load monthly job.
    """

    year = get_period_year(period)
    month = get_period_month(period)

    return (
        f"echo {shlex.quote(f'Processing period: {year}-{month}')} && "
        f"cd {shlex.quote(PROJECT_DIR)} && "
        f"PYTHONPATH={shlex.quote(JOBS_DIR)} "
        f"spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} "
        f"{shlex.quote(f'jobs/{job_file}')} "
        f"--year {shlex.quote(year)} "
        f"--month {shlex.quote(month)}"
    )


@task(task_id="read_period_refresh_config", execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT)
def read_period_refresh_config_task() -> List[PeriodParam]:
    """
    Read Airflow runtime config and return period dictionaries for mapped tasks.

    The task reads Airflow Params / Trigger DAG config from the task context.
    Returned format:
        [
            {"year": "2024", "month": "05"},
            {"year": "2024", "month": "06"},
        ]
    """

    from period_refresh_config import (
        build_period_refresh_periods,
        format_period_params,
    )

    context = get_current_context()
    dag_run = context.get("dag_run")

    runtime_config = dict(context.get("params", {}))

    if dag_run and dag_run.conf:
        runtime_config.update(dag_run.conf)

    periods = build_period_refresh_periods(runtime_config)

    print(f"Period refresh runtime config: {runtime_config}")
    print(f"Period refresh selected periods: {format_period_params(periods)}")

    return periods


@task(task_id="log_period_refresh_periods", execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT)
def log_period_refresh_periods(periods: List[PeriodParam]) -> None:
    """
    Log selected period refresh periods.
    """

    if not periods:
        raise ValueError("Period refresh requires at least one selected period")

    formatted_periods = ", ".join(format_period(period) for period in periods)

    print(f"Period refresh will process: {formatted_periods}")


@task_group(group_id="process_month")
def process_month(period: PeriodParam) -> None:
    """
    Refresh one selected monthly period from raw to ClickHouse.

    This TaskGroup is dynamically mapped:
        process_month[0] refreshes the first selected period;
        process_month[1] refreshes the second selected period;
        and so on.

    Inside each mapped group, the full monthly replacement pipeline is executed:
        delete ClickHouse month
        -> bronze
        -> silver
        -> silver quality
        -> gold marts
        -> gold Object Storage quality
        -> ClickHouse load
        -> ClickHouse month quality
    """

    @task(task_id="delete_clickhouse_gold_month", execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT)
    def delete_clickhouse_gold_month_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_python_job_command(
                job_file="delete_clickhouse_gold_month.py",
                period=selected_period,
            )
        )

    @task(task_id="bronze_yellow_taxi", pool=SPARK_POOL, execution_timeout=BRONZE_TASK_EXECUTION_TIMEOUT)
    def bronze_yellow_taxi_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="bronze_yellow_taxi.py",
                period=selected_period,
            )
        )

    @task(task_id="silver_yellow_taxi", pool=SPARK_POOL, execution_timeout=SILVER_TASK_EXECUTION_TIMEOUT)
    def silver_yellow_taxi_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="silver_yellow_taxi.py",
                period=selected_period,
            )
        )

    @task(task_id="check_yellow_taxi_quality", pool=SPARK_POOL, execution_timeout=SILVER_QUALITY_TASK_EXECUTION_TIMEOUT)
    def check_yellow_taxi_quality_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="check_yellow_taxi_quality.py",
                period=selected_period,
            )
        )

    @task(task_id="gold_hourly_trips", pool=SPARK_POOL, execution_timeout=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT)
    def gold_hourly_trips_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_hourly_trips.py",
                period=selected_period,
            )
        )

    @task(task_id="gold_daily_trips", pool=SPARK_POOL, execution_timeout=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT)
    def gold_daily_trips_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_daily_trips.py",
                period=selected_period,
            )
        )

    @task(task_id="gold_payment_type_stats", pool=SPARK_POOL, execution_timeout=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT)
    def gold_payment_type_stats_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_payment_type_stats.py",
                period=selected_period,
            )
        )

    @task(task_id="gold_location_pair_stats", pool=SPARK_POOL, execution_timeout=GOLD_LOCATION_TASK_EXECUTION_TIMEOUT)
    def gold_location_pair_stats_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_location_pair_stats.py",
                period=selected_period,
            )
        )

    @task(task_id="check_gold_schema", pool=SPARK_POOL, execution_timeout=GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT)
    def check_gold_schema_task(selected_period: PeriodParam) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="check_gold_schema.py",
                period=selected_period,
            )
        )

    @task(task_id="load_gold_hourly_trips_to_clickhouse", pool=SPARK_POOL, execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT)
    def load_gold_hourly_trips_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_hourly_trips_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(task_id="load_gold_daily_trips_to_clickhouse", pool=SPARK_POOL, execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT)
    def load_gold_daily_trips_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_daily_trips_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(task_id="load_gold_payment_type_stats_to_clickhouse", pool=SPARK_POOL, execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT)
    def load_gold_payment_type_stats_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_payment_type_stats_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(task_id="load_gold_location_pair_stats_to_clickhouse", pool=SPARK_POOL, execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT)
    def load_gold_location_pair_stats_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_location_pair_stats_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(task_id="check_clickhouse_gold_month_quality", execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT)
    def check_clickhouse_gold_month_quality_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_python_job_command(
                job_file="check_clickhouse_gold_month_quality.py",
                period=selected_period,
            )
        )

    delete_clickhouse_gold_month = delete_clickhouse_gold_month_task(period)

    bronze = bronze_yellow_taxi_task(period)
    silver = silver_yellow_taxi_task(period)
    check_quality = check_yellow_taxi_quality_task(period)

    gold_hourly = gold_hourly_trips_task(period)
    gold_daily = gold_daily_trips_task(period)
    gold_payment = gold_payment_type_stats_task(period)
    gold_location = gold_location_pair_stats_task(period)

    check_gold_schema = check_gold_schema_task(period)

    load_gold_hourly = load_gold_hourly_trips_to_clickhouse_task(period)
    load_gold_daily = load_gold_daily_trips_to_clickhouse_task(period)
    load_gold_payment = load_gold_payment_type_stats_to_clickhouse_task(period)
    load_gold_location = load_gold_location_pair_stats_to_clickhouse_task(period)

    check_clickhouse_gold_month_quality = (
        check_clickhouse_gold_month_quality_task(period)
    )

    delete_clickhouse_gold_month >> bronze

    bronze >> silver >> check_quality >> [
        gold_hourly,
        gold_daily,
        gold_payment,
        gold_location,
    ] >> check_gold_schema

    check_gold_schema >> [
        load_gold_hourly,
        load_gold_daily,
        load_gold_payment,
        load_gold_location,
    ]

    [
        load_gold_hourly,
        load_gold_daily,
        load_gold_payment,
        load_gold_location,
    ] >> check_clickhouse_gold_month_quality


with DAG(
    dag_id="nyc_taxi_period_refresh_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "start_year": DEFAULT_START_YEAR,
        "start_month": DEFAULT_START_MONTH,
        "end_year": DEFAULT_END_YEAR,
        "end_month": DEFAULT_END_MONTH,
        "refresh_mode": DEFAULT_REFRESH_MODE,
    },
    tags=[
        "nyc_taxi",
        "spark",
        "clickhouse",
        "period_refresh",
        "data_engineering",
    ],
) as dag:

    create_clickhouse_gold_tables = BashOperator(
        task_id="create_clickhouse_gold_tables",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} python jobs/create_clickhouse_gold_tables.py
        """,
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )

    periods = read_period_refresh_config_task()

    log_periods = log_period_refresh_periods(periods)

    process_selected_months = process_month.expand(period=periods)

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    create_clickhouse_gold_tables >> periods

    periods >> log_periods
    periods >> process_selected_months

    [
        log_periods,
        process_selected_months,
    ] >> finish
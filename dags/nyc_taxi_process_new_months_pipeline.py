"""
Airflow DAG for processing newly arrived NYC Taxi raw monthly files.

This DAG discovers raw Yellow Taxi monthly parquet files in Object Storage,
compares them with periods fully loaded into the ClickHouse serving layer,
and processes only newly discovered months.

Current approach:
    raw periods       = months discovered from Object Storage raw parquet keys;
    processed periods = months fully present in all expected ClickHouse gold tables;
    new periods       = raw periods - processed periods.

Why this DAG exists:
    nyc_taxi_pipeline.py performs a controlled full-year rebuild.
    nyc_taxi_period_refresh_pipeline.py performs manual period replacement.
    This DAG adds automatic new-month discovery as a foundation for incremental
    monthly processing.

Safety:
    Before processing each discovered month, the DAG deletes this month from
    ClickHouse gold tables. For a truly new month this deletes zero rows.
    For a partially loaded month this cleans up inconsistent serving-layer data
    before rebuilding and reloading the month.

Local execution note:
    max_active_runs=1 and max_active_tasks=1 keep this DAG sequential.
    Spark-heavy tasks also use the spark_pool Airflow Pool to prevent Spark jobs
    from different DAGs from running at the same time in the local Docker
    environment.
"""

from __future__ import annotations

import shlex
import subprocess
from datetime import datetime
from typing import Dict, List

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from config import AIRFLOW_JOBS_DIR, AIRFLOW_PROJECT_DIR, S3_ENDPOINT


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

default_args = {
    "owner": "natalia",
    "retries": 1,
}

DiscoveredPeriod = Dict[str, str]


def get_period_year(discovered_period: DiscoveredPeriod) -> str:
    """
    Return year from discovered period dictionary.
    """

    return discovered_period["year"]


def get_period_month(discovered_period: DiscoveredPeriod) -> str:
    """
    Return month from discovered period dictionary.
    """

    return discovered_period["month"]


def format_period(discovered_period: DiscoveredPeriod) -> str:
    """
    Format discovered period for logs.
    """

    return (
        f"{get_period_year(discovered_period)}-"
        f"{get_period_month(discovered_period)}"
    )


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
    discovered_period: DiscoveredPeriod,
) -> str:
    """
    Build shell command for a lightweight Python monthly job.
    """

    year = get_period_year(discovered_period)
    month = get_period_month(discovered_period)

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
    discovered_period: DiscoveredPeriod,
) -> str:
    """
    Build shell command for a Spark monthly job.
    """

    year = get_period_year(discovered_period)
    month = get_period_month(discovered_period)

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
    discovered_period: DiscoveredPeriod,
) -> str:
    """
    Build shell command for a Spark-based ClickHouse load monthly job.
    """

    year = get_period_year(discovered_period)
    month = get_period_month(discovered_period)

    return (
        f"echo {shlex.quote(f'Processing period: {year}-{month}')} && "
        f"cd {shlex.quote(PROJECT_DIR)} && "
        f"PYTHONPATH={shlex.quote(JOBS_DIR)} "
        f"spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} "
        f"{shlex.quote(f'jobs/{job_file}')} "
        f"--year {shlex.quote(year)} "
        f"--month {shlex.quote(month)}"
    )


@task(task_id="discover_new_raw_periods")
def discover_new_raw_periods_task() -> List[DiscoveredPeriod]:
    """
    Discover new raw periods and return dictionaries for mapped TaskGroup runs.

    Returned format:
        [
            {"year": "2025", "month": "01"},
            {"year": "2025", "month": "02"},
        ]
    """

    from raw_discovery import discover_new_raw_periods, format_periods

    periods = discover_new_raw_periods()

    print(f"Discovered new raw periods: {format_periods(periods)}")

    return [
        {
            "year": year,
            "month": month,
        }
        for year, month in periods
    ]


@task(task_id="log_discovered_periods")
def log_discovered_periods(discovered_periods: List[DiscoveredPeriod]) -> None:
    """
    Log discovered periods.

    This task succeeds even when no new periods are found. This makes an empty
    discovery result a successful no-op DAG run instead of a failure.
    """

    if not discovered_periods:
        print("No new raw periods found. Nothing to process.")
        return

    formatted_periods = ", ".join(
        format_period(discovered_period)
        for discovered_period in discovered_periods
    )

    print(f"New raw periods to process: {formatted_periods}")


@task_group(group_id="process_month")
def process_month(discovered_period: DiscoveredPeriod) -> None:
    """
    Process one discovered monthly period from raw to ClickHouse.

    This TaskGroup is dynamically mapped:
        process_month[0] processes the first discovered period;
        process_month[1] processes the second discovered period;
        and so on.

    Inside each mapped group, the full monthly pipeline is executed:
        delete ClickHouse month
        -> bronze
        -> silver
        -> silver quality
        -> gold marts
        -> gold Object Storage quality
        -> ClickHouse load
        -> ClickHouse month quality
    """

    @task(task_id="delete_clickhouse_gold_month")
    def delete_clickhouse_gold_month_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_python_job_command(
                job_file="delete_clickhouse_gold_month.py",
                discovered_period=period,
            )
        )

    @task(task_id="bronze_yellow_taxi", pool=SPARK_POOL)
    def bronze_yellow_taxi_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="bronze_yellow_taxi.py",
                discovered_period=period,
            )
        )

    @task(task_id="silver_yellow_taxi", pool=SPARK_POOL)
    def silver_yellow_taxi_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="silver_yellow_taxi.py",
                discovered_period=period,
            )
        )

    @task(task_id="check_yellow_taxi_quality", pool=SPARK_POOL)
    def check_yellow_taxi_quality_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="check_yellow_taxi_quality.py",
                discovered_period=period,
            )
        )

    @task(task_id="gold_hourly_trips", pool=SPARK_POOL)
    def gold_hourly_trips_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_hourly_trips.py",
                discovered_period=period,
            )
        )

    @task(task_id="gold_daily_trips", pool=SPARK_POOL)
    def gold_daily_trips_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_daily_trips.py",
                discovered_period=period,
            )
        )

    @task(task_id="gold_payment_type_stats", pool=SPARK_POOL)
    def gold_payment_type_stats_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_payment_type_stats.py",
                discovered_period=period,
            )
        )

    @task(task_id="gold_location_pair_stats", pool=SPARK_POOL)
    def gold_location_pair_stats_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_location_pair_stats.py",
                discovered_period=period,
            )
        )

    @task(task_id="check_gold_schema", pool=SPARK_POOL)
    def check_gold_schema_task(period: DiscoveredPeriod) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="check_gold_schema.py",
                discovered_period=period,
            )
        )

    @task(task_id="load_gold_hourly_trips_to_clickhouse", pool=SPARK_POOL)
    def load_gold_hourly_trips_to_clickhouse_task(
        period: DiscoveredPeriod,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_hourly_trips_to_clickhouse.py",
                discovered_period=period,
            )
        )

    @task(task_id="load_gold_daily_trips_to_clickhouse", pool=SPARK_POOL)
    def load_gold_daily_trips_to_clickhouse_task(
        period: DiscoveredPeriod,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_daily_trips_to_clickhouse.py",
                discovered_period=period,
            )
        )

    @task(task_id="load_gold_payment_type_stats_to_clickhouse", pool=SPARK_POOL)
    def load_gold_payment_type_stats_to_clickhouse_task(
        period: DiscoveredPeriod,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_payment_type_stats_to_clickhouse.py",
                discovered_period=period,
            )
        )

    @task(task_id="load_gold_location_pair_stats_to_clickhouse", pool=SPARK_POOL)
    def load_gold_location_pair_stats_to_clickhouse_task(
        period: DiscoveredPeriod,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_location_pair_stats_to_clickhouse.py",
                discovered_period=period,
            )
        )

    @task(task_id="check_clickhouse_gold_month_quality")
    def check_clickhouse_gold_month_quality_task(
        period: DiscoveredPeriod,
    ) -> None:
        run_shell_command(
            build_python_job_command(
                job_file="check_clickhouse_gold_month_quality.py",
                discovered_period=period,
            )
        )

    delete_clickhouse_gold_month = delete_clickhouse_gold_month_task(
        discovered_period
    )

    bronze = bronze_yellow_taxi_task(discovered_period)
    silver = silver_yellow_taxi_task(discovered_period)
    check_quality = check_yellow_taxi_quality_task(discovered_period)

    gold_hourly = gold_hourly_trips_task(discovered_period)
    gold_daily = gold_daily_trips_task(discovered_period)
    gold_payment = gold_payment_type_stats_task(discovered_period)
    gold_location = gold_location_pair_stats_task(discovered_period)

    check_gold_schema = check_gold_schema_task(discovered_period)

    load_gold_hourly = load_gold_hourly_trips_to_clickhouse_task(
        discovered_period
    )
    load_gold_daily = load_gold_daily_trips_to_clickhouse_task(
        discovered_period
    )
    load_gold_payment = load_gold_payment_type_stats_to_clickhouse_task(
        discovered_period
    )
    load_gold_location = load_gold_location_pair_stats_to_clickhouse_task(
        discovered_period
    )

    check_clickhouse_gold_month_quality = (
        check_clickhouse_gold_month_quality_task(discovered_period)
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
    dag_id="nyc_taxi_process_new_months_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=[
        "nyc_taxi",
        "spark",
        "clickhouse",
        "raw_discovery",
        "incremental",
        "data_engineering",
    ],
) as dag:

    create_clickhouse_gold_tables = BashOperator(
        task_id="create_clickhouse_gold_tables",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} python jobs/create_clickhouse_gold_tables.py
        """,
    )

    discovered_periods = discover_new_raw_periods_task()

    log_periods = log_discovered_periods(discovered_periods)

    process_discovered_months = process_month.expand(
        discovered_period=discovered_periods
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    create_clickhouse_gold_tables >> discovered_periods

    discovered_periods >> log_periods
    discovered_periods >> process_discovered_months

    [
        log_periods,
        process_discovered_months,
    ] >> finish
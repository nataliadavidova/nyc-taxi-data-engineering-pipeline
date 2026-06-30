"""
Airflow DAG for protected NYC Taxi full rebuild pipeline.

Pipeline:
create ClickHouse tables
→ validate full rebuild runtime config
→ discover and validate raw periods
→ truncate ClickHouse gold tables
→ bronze
→ silver
→ check_quality
→ gold_daily / gold_hourly / gold_payment / gold_location
→ check_gold_schema
→ load_gold_daily / load_gold_hourly / load_gold_payment / load_gold_location
→ month-level ClickHouse quality checks
"""

from datetime import datetime, timedelta

import shlex
import subprocess

from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

from full_rebuild_config import (
    DEFAULT_CONFIRM_CLICKHOUSE_TRUNCATE,
    DEFAULT_CONFIRM_FULL_REBUILD,
    DEFAULT_EXPECTED_END_MONTH,
    DEFAULT_EXPECTED_END_YEAR,
    DEFAULT_EXPECTED_START_MONTH,
    DEFAULT_EXPECTED_START_YEAR,
    DEFAULT_REBUILD_MODE,
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


DBT_ANALYTICS_DAG_ID = "nyc_taxi_dbt_analytics_pipeline"
TRIGGER_DBT_ANALYTICS_TASK_ID = "trigger_dbt_analytics_pipeline"


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
    Build shell command for a Spark-based ClickHouse monthly load job.
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


@task(
    task_id="validate_full_rebuild_config",
    execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
)
def validate_full_rebuild_config_task() -> Dict[str, Any]:
    """
    Read and validate runtime config for the protected full rebuild.

    The task fails before ClickHouse truncate if explicit confirmations or
    expected period boundaries are missing or invalid.
    """

    from full_rebuild_config import validate_full_rebuild_runtime_config

    context = get_current_context()
    dag_run = context.get("dag_run")

    runtime_config = dict(context.get("params", {}))

    if dag_run and dag_run.conf:
        runtime_config.update(dag_run.conf)

    validated_config = validate_full_rebuild_runtime_config(runtime_config)

    print(f"Validated full rebuild config: {validated_config}")

    return validated_config


@task(
    task_id="discover_full_rebuild_raw_periods",
    execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
)
def discover_full_rebuild_raw_periods_task() -> List[PeriodParam]:
    """
    Discover all available raw Yellow Taxi monthly periods.
    """

    from raw_discovery import format_periods, list_raw_yellow_periods

    periods = list_raw_yellow_periods()

    print(f"Full rebuild discovered raw periods: {format_periods(periods)}")

    return [
        {
            "year": year,
            "month": month,
        }
        for year, month in periods
    ]


@task(
    task_id="validate_full_rebuild_raw_periods",
    execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
)
def validate_full_rebuild_raw_periods_task(
    raw_periods: List[PeriodParam],
    validated_config: Dict[str, Any],
) -> List[PeriodParam]:
    """
    Validate that discovered raw periods exactly match the confirmed range.

    The task fails before ClickHouse truncate if raw data is missing or contains
    periods outside the explicitly confirmed full rebuild range.
    """

    from raw_discovery import (
        format_periods,
        validate_full_rebuild_raw_periods,
    )

    raw_period_tuples = [
        (period["year"], period["month"])
        for period in raw_periods
    ]

    validated_period_tuples = validate_full_rebuild_raw_periods(
        raw_periods=raw_period_tuples,
        expected_start_year=validated_config["expected_start_year"],
        expected_start_month=validated_config["expected_start_month"],
        expected_end_year=validated_config["expected_end_year"],
        expected_end_month=validated_config["expected_end_month"],
    )

    print(
        "Full rebuild validated raw periods: "
        f"{format_periods(validated_period_tuples)}"
    )

    return [
        {
            "year": year,
            "month": month,
        }
        for year, month in validated_period_tuples
    ]


@task(
    task_id="log_full_rebuild_plan",
    execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
)
def log_full_rebuild_plan_task(
    periods: List[PeriodParam],
) -> None:
    """
    Log the validated full rebuild plan before ClickHouse truncate.
    """

    if not periods:
        raise ValueError(
            "Full rebuild requires at least one validated raw period. "
            "ClickHouse truncate was not executed."
        )

    formatted_periods = ", ".join(
        format_period(period)
        for period in periods
    )

    print("Protected full rebuild plan validated")
    print(f"Periods count: {len(periods)}")
    print(f"Periods to rebuild: {formatted_periods}")


@task_group(group_id="process_month")
def process_month(period: PeriodParam) -> None:
    """
    Rebuild one validated raw monthly period from raw to ClickHouse.

    This TaskGroup is dynamically mapped over all validated raw periods.

    Inside each mapped group, the monthly full rebuild pipeline is executed:
        bronze
        -> silver
        -> silver quality
        -> gold marts
        -> gold Object Storage quality
        -> ClickHouse load
        -> ClickHouse month quality

    The TaskGroup does not delete an individual ClickHouse month because the
    full rebuild DAG truncates all ClickHouse gold tables once before monthly
    processing starts.
    """

    @task(
        task_id="bronze_yellow_taxi",
        pool=SPARK_POOL,
        execution_timeout=BRONZE_TASK_EXECUTION_TIMEOUT,
    )
    def bronze_yellow_taxi_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="bronze_yellow_taxi.py",
                period=selected_period,
            )
        )

    @task(
        task_id="silver_yellow_taxi",
        pool=SPARK_POOL,
        execution_timeout=SILVER_TASK_EXECUTION_TIMEOUT,
    )
    def silver_yellow_taxi_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="silver_yellow_taxi.py",
                period=selected_period,
            )
        )

    @task(
        task_id="check_yellow_taxi_quality",
        pool=SPARK_POOL,
        execution_timeout=SILVER_QUALITY_TASK_EXECUTION_TIMEOUT,
    )
    def check_yellow_taxi_quality_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="check_yellow_taxi_quality.py",
                period=selected_period,
            )
        )

    @task(
        task_id="gold_hourly_trips",
        pool=SPARK_POOL,
        execution_timeout=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT,
    )
    def gold_hourly_trips_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_hourly_trips.py",
                period=selected_period,
            )
        )

    @task(
        task_id="gold_daily_trips",
        pool=SPARK_POOL,
        execution_timeout=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT,
    )
    def gold_daily_trips_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_daily_trips.py",
                period=selected_period,
            )
        )

    @task(
        task_id="gold_payment_type_stats",
        pool=SPARK_POOL,
        execution_timeout=GOLD_STANDARD_TASK_EXECUTION_TIMEOUT,
    )
    def gold_payment_type_stats_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_payment_type_stats.py",
                period=selected_period,
            )
        )

    @task(
        task_id="gold_location_pair_stats",
        pool=SPARK_POOL,
        execution_timeout=GOLD_LOCATION_TASK_EXECUTION_TIMEOUT,
    )
    def gold_location_pair_stats_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="gold_location_pair_stats.py",
                period=selected_period,
            )
        )

    @task(
        task_id="check_gold_schema",
        pool=SPARK_POOL,
        execution_timeout=GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT,
    )
    def check_gold_schema_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_spark_job_command(
                job_file="check_gold_schema.py",
                period=selected_period,
            )
        )

    @task(
        task_id="load_gold_hourly_trips_to_clickhouse",
        pool=SPARK_POOL,
        execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT,
    )
    def load_gold_hourly_trips_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_hourly_trips_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(
        task_id="load_gold_daily_trips_to_clickhouse",
        pool=SPARK_POOL,
        execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT,
    )
    def load_gold_daily_trips_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_daily_trips_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(
        task_id="load_gold_payment_type_stats_to_clickhouse",
        pool=SPARK_POOL,
        execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT,
    )
    def load_gold_payment_type_stats_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_payment_type_stats_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(
        task_id="load_gold_location_pair_stats_to_clickhouse",
        pool=SPARK_POOL,
        execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT,
    )
    def load_gold_location_pair_stats_to_clickhouse_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_clickhouse_load_command(
                job_file="load_gold_location_pair_stats_to_clickhouse.py",
                period=selected_period,
            )
        )

    @task(
        task_id="check_clickhouse_gold_month_quality",
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )
    def check_clickhouse_gold_month_quality_task(
        selected_period: PeriodParam,
    ) -> None:
        run_shell_command(
            build_python_job_command(
                job_file="check_clickhouse_gold_month_quality.py",
                period=selected_period,
            )
        )

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
    dag_id="nyc_taxi_full_rebuild_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "rebuild_mode": DEFAULT_REBUILD_MODE,
        "confirm_full_rebuild": DEFAULT_CONFIRM_FULL_REBUILD,
        "confirm_clickhouse_truncate": DEFAULT_CONFIRM_CLICKHOUSE_TRUNCATE,
        "expected_start_year": DEFAULT_EXPECTED_START_YEAR,
        "expected_start_month": DEFAULT_EXPECTED_START_MONTH,
        "expected_end_year": DEFAULT_EXPECTED_END_YEAR,
        "expected_end_month": DEFAULT_EXPECTED_END_MONTH,
    },
    tags=[
        "nyc_taxi",
        "spark",
        "clickhouse",
        "full_rebuild",
        "raw_discovery",
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

    validated_config = validate_full_rebuild_config_task()

    raw_periods = discover_full_rebuild_raw_periods_task()

    validated_periods = validate_full_rebuild_raw_periods_task(
        raw_periods=raw_periods,
        validated_config=validated_config,
    )

    log_rebuild_plan = log_full_rebuild_plan_task(validated_periods)

    truncate_clickhouse_gold_tables = BashOperator(
        task_id="truncate_clickhouse_gold_tables",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} python jobs/truncate_clickhouse_gold_tables.py
        """,
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )

    process_full_rebuild_months = process_month.expand(
        period=validated_periods
    )

    finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )


    trigger_dbt_analytics_pipeline = TriggerDagRunOperator(
        task_id=TRIGGER_DBT_ANALYTICS_TASK_ID,
        trigger_dag_id=DBT_ANALYTICS_DAG_ID,
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )

    create_clickhouse_gold_tables >> validated_config
    validated_config >> raw_periods

    log_rebuild_plan >> truncate_clickhouse_gold_tables
    truncate_clickhouse_gold_tables >> process_full_rebuild_months
    process_full_rebuild_months >> finish
    finish >> trigger_dbt_analytics_pipeline
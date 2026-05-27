"""
Airflow DAG for period-based NYC Taxi pipeline refresh.

This DAG is a safer extension of nyc_taxi_pipeline.py.

Current implementation:
    REFRESH_MODE = "replace_period"

replace_period mode:
    For each selected year/month:
    - delete existing rows for this month from ClickHouse gold tables;
    - rebuild bronze/silver/gold data for this month;
    - load rebuilt gold marts to ClickHouse;
    - validate ClickHouse gold data for this month.

Why this DAG exists:
    The original nyc_taxi_pipeline.py performs a full-year 2024 rebuild.
    This DAG introduces period-based refresh logic without breaking the
    existing stable pipeline.

Manual one-month reload:
    Set START_YEAR/START_MONTH and END_YEAR/END_MONTH to the same period.

Example:
    START_YEAR = "2024"
    START_MONTH = "05"
    END_YEAR = "2024"
    END_MONTH = "05"
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from config import AIRFLOW_JOBS_DIR, AIRFLOW_PROJECT_DIR, S3_ENDPOINT
from period_utils import generate_month_periods


PROJECT_DIR = AIRFLOW_PROJECT_DIR
JOBS_DIR = AIRFLOW_JOBS_DIR

START_YEAR = "2024"
START_MONTH = "01"
END_YEAR = "2024"
END_MONTH = "12"

REFRESH_MODE = "replace_period"

SUPPORTED_REFRESH_MODES = {"replace_period"}

if REFRESH_MODE not in SUPPORTED_REFRESH_MODES:
    raise ValueError(
        f"Unsupported REFRESH_MODE={REFRESH_MODE!r}. "
        f"Supported modes: {sorted(SUPPORTED_REFRESH_MODES)}"
    )

PERIODS = generate_month_periods(
    start_year=START_YEAR,
    start_month=START_MONTH,
    end_year=END_YEAR,
    end_month=END_MONTH,
)

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


default_args = {
    "owner": "natalia",
    "retries": 1,
}


def spark_task(
    task_id: str,
    job_file: str,
    year: str,
    month: str,
) -> BashOperator:
    """
    Create a Spark-based monthly BashOperator task.
    """

    return BashOperator(
        task_id=f"{task_id}_{year}_{month}",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        PYTHONPATH={JOBS_DIR} {SPARK_SUBMIT_BASE} jobs/{job_file} --year {year} --month {month}
        """,
    )


def python_task(
    task_id: str,
    job_file: str,
    year: str,
    month: str,
) -> BashOperator:
    """
    Create a Python monthly BashOperator task.

    Used for lightweight ClickHouse utility jobs that do not require Spark.
    """

    return BashOperator(
        task_id=f"{task_id}_{year}_{month}",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        PYTHONPATH={JOBS_DIR} python jobs/{job_file} --year {year} --month {month}
        """,
    )


def clickhouse_load_task(
    task_id: str,
    job_file: str,
    year: str,
    month: str,
) -> BashOperator:
    """
    Create a Spark-based ClickHouse load task.
    """

    return BashOperator(
        task_id=f"{task_id}_{year}_{month}",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} \
        jobs/{job_file} \
        --year {year} \
        --month {month}
        """,
    )


with DAG(
    dag_id="nyc_taxi_period_refresh_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
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
    )

    previous_month_final_task = create_clickhouse_gold_tables

    for year, month in PERIODS:
        delete_clickhouse_gold_month = python_task(
            task_id="delete_clickhouse_gold_month",
            job_file="delete_clickhouse_gold_month.py",
            year=year,
            month=month,
        )

        bronze = spark_task(
            task_id="bronze_yellow_taxi",
            job_file="bronze_yellow_taxi.py",
            year=year,
            month=month,
        )

        silver = spark_task(
            task_id="silver_yellow_taxi",
            job_file="silver_yellow_taxi.py",
            year=year,
            month=month,
        )

        check_quality = spark_task(
            task_id="check_yellow_taxi_quality",
            job_file="check_yellow_taxi_quality.py",
            year=year,
            month=month,
        )

        gold_hourly = spark_task(
            task_id="gold_hourly_trips",
            job_file="gold_hourly_trips.py",
            year=year,
            month=month,
        )

        gold_daily = spark_task(
            task_id="gold_daily_trips",
            job_file="gold_daily_trips.py",
            year=year,
            month=month,
        )

        gold_payment = spark_task(
            task_id="gold_payment_type_stats",
            job_file="gold_payment_type_stats.py",
            year=year,
            month=month,
        )

        gold_location = spark_task(
            task_id="gold_location_pair_stats",
            job_file="gold_location_pair_stats.py",
            year=year,
            month=month,
        )

        check_gold_schema = spark_task(
            task_id="check_gold_schema",
            job_file="check_gold_schema.py",
            year=year,
            month=month,
        )

        load_gold_hourly = clickhouse_load_task(
            task_id="load_gold_hourly_trips_to_clickhouse",
            job_file="load_gold_hourly_trips_to_clickhouse.py",
            year=year,
            month=month,
        )

        load_gold_daily = clickhouse_load_task(
            task_id="load_gold_daily_trips_to_clickhouse",
            job_file="load_gold_daily_trips_to_clickhouse.py",
            year=year,
            month=month,
        )

        load_gold_payment = clickhouse_load_task(
            task_id="load_gold_payment_type_stats_to_clickhouse",
            job_file="load_gold_payment_type_stats_to_clickhouse.py",
            year=year,
            month=month,
        )

        load_gold_location = clickhouse_load_task(
            task_id="load_gold_location_pair_stats_to_clickhouse",
            job_file="load_gold_location_pair_stats_to_clickhouse.py",
            year=year,
            month=month,
        )

        check_clickhouse_gold_month_quality = python_task(
            task_id="check_clickhouse_gold_month_quality",
            job_file="check_clickhouse_gold_month_quality.py",
            year=year,
            month=month,
        )

        previous_month_final_task >> delete_clickhouse_gold_month

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

        previous_month_final_task = check_clickhouse_gold_month_quality

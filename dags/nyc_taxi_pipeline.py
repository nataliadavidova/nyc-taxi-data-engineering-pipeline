"""
Airflow DAG for NYC Taxi pipeline.

Pipeline:
bronze
→ silver
→ check_quality
→ gold_daily / gold_hourly / gold_payment / gold_location
→ check_gold_schema
→ load_gold_daily / load_gold_hourly / load_gold_payment / load_gold_location
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    SPARK_TASK_EXECUTION_TIMEOUT_MINUTES,
)

from airflow_callbacks import airflow_failure_callback


PROJECT_DIR = AIRFLOW_PROJECT_DIR
JOBS_DIR = AIRFLOW_JOBS_DIR

YEAR = "2024"
MONTHS = [f"{month:02d}" for month in range(1, 13)]

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

SPARK_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=SPARK_TASK_EXECUTION_TIMEOUT_MINUTES
)
PYTHON_TASK_EXECUTION_TIMEOUT = timedelta(
    minutes=PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES
)

SPARK_TASK_EXECUTION_TIMEOUTS = {
    "bronze_yellow_taxi": BRONZE_TASK_EXECUTION_TIMEOUT,
    "silver_yellow_taxi": SILVER_TASK_EXECUTION_TIMEOUT,
    "check_yellow_taxi_quality": SILVER_QUALITY_TASK_EXECUTION_TIMEOUT,
    "gold_daily_trips": GOLD_STANDARD_TASK_EXECUTION_TIMEOUT,
    "gold_hourly_trips": GOLD_STANDARD_TASK_EXECUTION_TIMEOUT,
    "gold_payment_type_stats": GOLD_STANDARD_TASK_EXECUTION_TIMEOUT,
    "gold_location_pair_stats": GOLD_LOCATION_TASK_EXECUTION_TIMEOUT,
    "check_gold_schema": GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT,
}

default_args = {
    "owner": "natalia",
    "retries": 1,
    "retry_delay": AIRFLOW_RETRY_DELAY,
    "on_failure_callback": airflow_failure_callback,
}


def get_spark_task_execution_timeout(task_id: str) -> timedelta:
    """
    Return task-family-specific timeout for Spark-heavy tasks.

    Falls back to the broad Spark timeout for future Spark tasks that are not
    yet assigned to a specific task family.
    """

    return SPARK_TASK_EXECUTION_TIMEOUTS.get(
        task_id,
        SPARK_TASK_EXECUTION_TIMEOUT,
    )


def spark_task(task_id: str, job_file: str, month: str) -> BashOperator:
    return BashOperator(
        task_id=f"{task_id}_{YEAR}_{month}",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        PYTHONPATH={JOBS_DIR} {SPARK_SUBMIT_BASE} jobs/{job_file} --year {YEAR} --month {month}
        """,
        pool=SPARK_POOL,
        execution_timeout=get_spark_task_execution_timeout(task_id),
    )


def clickhouse_load_task(task_id: str, job_file: str, month: str) -> BashOperator:
    return BashOperator(
        task_id=f"{task_id}_{YEAR}_{month}",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} \
        jobs/{job_file} \
        --year {YEAR} \
        --month {month}
        """,
        pool=SPARK_POOL,
        execution_timeout=CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT,
    )


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc_taxi", "spark", "data_engineering"],
) as dag:

    create_clickhouse_gold_tables = BashOperator(
        task_id="create_clickhouse_gold_tables",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} python jobs/create_clickhouse_gold_tables.py
        """,
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )

    truncate_clickhouse_gold_tables = BashOperator(
        task_id="truncate_clickhouse_gold_tables",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        PYTHONPATH={JOBS_DIR} python jobs/truncate_clickhouse_gold_tables.py
        """,
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )

    check_clickhouse_gold_quality = BashOperator(
        task_id="check_clickhouse_gold_quality",
        bash_command=f"""
        cd {PROJECT_DIR} &&
        PYTHONPATH={JOBS_DIR} python jobs/check_clickhouse_gold_quality.py
        """,
        execution_timeout=PYTHON_TASK_EXECUTION_TIMEOUT,
    )

    create_clickhouse_gold_tables >> truncate_clickhouse_gold_tables

    previous_month_final_tasks = [truncate_clickhouse_gold_tables]

    for month in MONTHS:
        bronze = spark_task(
            task_id="bronze_yellow_taxi",
            job_file="bronze_yellow_taxi.py",
            month=month,
        )

        silver = spark_task(
            task_id="silver_yellow_taxi",
            job_file="silver_yellow_taxi.py",
            month=month,
        )

        check_quality = spark_task(
            task_id="check_yellow_taxi_quality",
            job_file="check_yellow_taxi_quality.py",
            month=month,
        )

        gold_hourly = spark_task(
            task_id="gold_hourly_trips",
            job_file="gold_hourly_trips.py",
            month=month,
        )

        gold_daily = spark_task(
            task_id="gold_daily_trips",
            job_file="gold_daily_trips.py",
            month=month,
        )

        gold_payment = spark_task(
            task_id="gold_payment_type_stats",
            job_file="gold_payment_type_stats.py",
            month=month,
        )

        gold_location = spark_task(
            task_id="gold_location_pair_stats",
            job_file="gold_location_pair_stats.py",
            month=month,
        )

        check_gold_schema = spark_task(
            task_id="check_gold_schema",
            job_file="check_gold_schema.py",
            month=month,
        )

        load_gold_hourly = clickhouse_load_task(
            task_id="load_gold_hourly_trips_to_clickhouse",
            job_file="load_gold_hourly_trips_to_clickhouse.py",
            month=month,
        )

        load_gold_daily = clickhouse_load_task(
            task_id="load_gold_daily_trips_to_clickhouse",
            job_file="load_gold_daily_trips_to_clickhouse.py",
            month=month,
        )

        load_gold_payment = clickhouse_load_task(
            task_id="load_gold_payment_type_stats_to_clickhouse",
            job_file="load_gold_payment_type_stats_to_clickhouse.py",
            month=month,
        )

        load_gold_location = clickhouse_load_task(
            task_id="load_gold_location_pair_stats_to_clickhouse",
            job_file="load_gold_location_pair_stats_to_clickhouse.py",
            month=month,
        )

        previous_month_final_tasks >> bronze

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

        previous_month_final_tasks = [
            load_gold_hourly,
            load_gold_daily,
            load_gold_payment,
            load_gold_location,
        ]

    previous_month_final_tasks >> check_clickhouse_gold_quality
"""
Airflow DAG for NYC Taxi pipeline.

Pipeline:
bronze -> silver -> quality checks -> gold marts -> ClickHouse
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from config import AIRFLOW_JOBS_DIR, AIRFLOW_PROJECT_DIR, S3_ENDPOINT


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


default_args = {
    "owner": "natalia",
    "retries": 1,
}


def spark_task(task_id: str, job_file: str, month: str) -> BashOperator:
    return BashOperator(
        task_id=f"{task_id}_{YEAR}_{month}",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        PYTHONPATH={JOBS_DIR} {SPARK_SUBMIT_BASE} jobs/{job_file} --year {YEAR} --month {month}
        """,
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
    )


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nyc_taxi", "spark", "data_engineering"],
) as dag:

    truncate_clickhouse_gold_tables = BashOperator(
        task_id="truncate_clickhouse_gold_tables",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        PYTHONPATH={JOBS_DIR} python jobs/truncate_clickhouse_gold_tables.py
        """,
    )

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
        ]

        gold_hourly >> load_gold_hourly
        gold_daily >> load_gold_daily
        gold_payment >> load_gold_payment
        gold_location >> load_gold_location

        previous_month_final_tasks = [
            load_gold_hourly,
            load_gold_daily,
            load_gold_payment,
            load_gold_location,
        ]
"""
Airflow DAG for NYC Taxi pipeline.

Pipeline:
bronze -> silver -> gold marts
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

SPARK_SUBMIT_BASE = (
    "spark-submit "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "--conf spark.hadoop.fs.s3a.endpoint=https://storage.yandexcloud.net "
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


with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nyc_taxi", "spark", "data_engineering"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_yellow_taxi",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/bronze_yellow_taxi.py --year 2024 --month 01
        """,
    )

    silver = BashOperator(
        task_id="silver_yellow_taxi",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/silver_yellow_taxi.py --year 2024 --month 01
        """,
    )

    check_quality = BashOperator(
        task_id="check_yellow_taxi_quality",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/check_yellow_taxi_quality.py --year 2024 --month 01
        """,
    )

    gold_hourly = BashOperator(
        task_id="gold_hourly_trips",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/gold_hourly_trips.py --year 2024 --month 01
        """,
    )

    gold_daily = BashOperator(
        task_id="gold_daily_trips",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/gold_daily_trips.py --year 2024 --month 01
        """,
    )

    gold_payment = BashOperator(
        task_id="gold_payment_type_stats",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/gold_payment_type_stats.py --year 2024 --month 01
        """,
    )

    gold_location = BashOperator(
        task_id="gold_location_pair_stats",
        bash_command=f"""
        cd {PROJECT_DIR} && \
        {SPARK_SUBMIT_BASE} jobs/gold_location_pair_stats.py --year 2024 --month 01
        """,
    )

    load_gold_hourly_trips_to_clickhouse = BashOperator(
        task_id="load_gold_hourly_trips_to_clickhouse",
        bash_command=f"""
            cd {PROJECT_DIR} &&
            PYTHONPATH={PROJECT_DIR} spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} \
            jobs/load_gold_hourly_trips_to_clickhouse.py \
            --year 2024 \
            --month 01
        """,
    )

    load_gold_daily_trips_to_clickhouse = BashOperator(
        task_id="load_gold_daily_trips_to_clickhouse",
        bash_command=f"""
            cd {PROJECT_DIR} &&
            PYTHONPATH={PROJECT_DIR} spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} \
            jobs/load_gold_daily_trips_to_clickhouse.py \
            --year 2024 \
            --month 01
        """,
    )

    load_gold_payment_type_stats_to_clickhouse = BashOperator(
        task_id="load_gold_payment_type_stats_to_clickhouse",
        bash_command=f"""
            cd {PROJECT_DIR} &&
            PYTHONPATH={PROJECT_DIR} spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} \
            jobs/load_gold_payment_type_stats_to_clickhouse.py \
            --year 2024 \
            --month 01
        """,
    )

    load_gold_location_pair_stats_to_clickhouse = BashOperator(
        task_id="load_gold_location_pair_stats_to_clickhouse",
        bash_command=f"""
            cd {PROJECT_DIR} &&
            PYTHONPATH={PROJECT_DIR} spark-submit {SPARK_SUBMIT_OPTIONS_WITH_CLICKHOUSE} \
            jobs/load_gold_location_pair_stats_to_clickhouse.py \
            --year 2024 \
            --month 01
        """,
    )

    bronze >> silver >> check_quality >> [
        gold_hourly,
        gold_daily,
        gold_payment,
        gold_location,
    ]

    gold_hourly >> load_gold_hourly_trips_to_clickhouse
    gold_daily >> load_gold_daily_trips_to_clickhouse
    gold_payment >> load_gold_payment_type_stats_to_clickhouse
    gold_location >> load_gold_location_pair_stats_to_clickhouse
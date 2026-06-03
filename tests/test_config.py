import sys
from pathlib import Path

# Add jobs directory to Python path so tests can import config.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = PROJECT_ROOT / "jobs"
sys.path.insert(0, str(JOBS_DIR))

import config


def test_get_month_boundaries_for_regular_month():
    month_start, next_month_start = config.get_month_boundaries("2024", "01")

    assert month_start == "2024-01-01"
    assert next_month_start == "2024-02-01"


def test_get_month_boundaries_for_december():
    month_start, next_month_start = config.get_month_boundaries("2024", "12")

    assert month_start == "2024-12-01"
    assert next_month_start == "2025-01-01"


def test_raw_yellow_path():
    path = config.raw_yellow_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/raw/yellow/"
        "year=2024/month=01/yellow_tripdata_2024-01.parquet"
    )


def test_bronze_yellow_path():
    path = config.bronze_yellow_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/bronze/yellow/"
        "year=2024/month=01"
    )


def test_silver_yellow_path():
    path = config.silver_yellow_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/silver/yellow/"
        "year=2024/month=01"
    )


def test_bad_records_yellow_path():
    path = config.bad_records_yellow_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/bad_records/yellow/"
        "year=2024/month=01"
    )


def test_quality_yellow_path():
    path = config.quality_yellow_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/quality/yellow/"
        "year=2024/month=01"
    )


def test_gold_daily_trips_path():
    path = config.gold_daily_trips_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/gold/yellow/daily_trips/"
        "year=2024/month=01"
    )


def test_gold_hourly_trips_path():
    path = config.gold_hourly_trips_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/gold/yellow/hourly_trips/"
        "year=2024/month=01"
    )


def test_gold_payment_type_stats_path():
    path = config.gold_payment_type_stats_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/gold/yellow/payment_type_stats/"
        "year=2024/month=01"
    )


def test_gold_location_pair_stats_path():
    path = config.gold_location_pair_stats_path("2024", "01")

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/gold/yellow/location_pair_stats/"
        "year=2024/month=01"
    )


def test_taxi_zone_lookup_path():
    path = config.taxi_zone_lookup_path()

    assert path == (
        f"s3a://{config.BUCKET_NAME}/nyc_taxi/raw/lookup/taxi_zone_lookup.csv"
    )


def test_airflow_runtime_settings_defaults():
    assert config.AIRFLOW_RETRY_DELAY_MINUTES == 5
    assert config.SPARK_TASK_EXECUTION_TIMEOUT_MINUTES == 30
    assert config.PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES == 10
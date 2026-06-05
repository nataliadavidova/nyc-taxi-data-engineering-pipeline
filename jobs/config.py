"""
Project configuration.

Loads environment variables from .env and stores shared settings
for all pipeline jobs.
"""

import os
from typing import Tuple
from datetime import date
from dotenv import load_dotenv


load_dotenv()


# =========================
# Airflow / project paths
# =========================

AIRFLOW_PROJECT_DIR = os.getenv("AIRFLOW_PROJECT_DIR", "/opt/airflow")
AIRFLOW_JOBS_DIR = f"{AIRFLOW_PROJECT_DIR}/jobs"

AIRFLOW_RETRY_DELAY_MINUTES = int(
    os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "5")
)
SPARK_TASK_EXECUTION_TIMEOUT_MINUTES = int(
    os.getenv("SPARK_TASK_EXECUTION_TIMEOUT_MINUTES", "30")
)
PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES = int(
    os.getenv("PYTHON_TASK_EXECUTION_TIMEOUT_MINUTES", "10")
)


TELEGRAM_ALERTS_ENABLED = (
    os.getenv("TELEGRAM_ALERTS_ENABLED", "false").lower() == "true"
)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_API_TIMEOUT_SECONDS = int(
    os.getenv("TELEGRAM_API_TIMEOUT_SECONDS", "10")
)


# =========================
# Object Storage settings
# =========================

BUCKET_NAME = os.getenv("BUCKET_NAME", "nyc-taxi-natalia-2026-final-project")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://storage.yandexcloud.net")
S3_REGION = os.getenv("S3_REGION", "ru-central1")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


# =========================
# ClickHouse settings
# =========================

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "nyc_taxi")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "airflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "airflow")


# =========================
# Data lake logical paths
# =========================

NYC_TAXI_PREFIX = "nyc_taxi"
TAXI_TYPE = "yellow"

RAW_LAYER = "raw"
BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"

BAD_RECORDS_LAYER = "bad_records"
QUALITY_LAYER = "quality"


GOLD_CLICKHOUSE_TABLES = [
    "gold_daily_trips",
    "gold_hourly_trips",
    "gold_location_pair_stats",
    "gold_payment_type_stats",
]


def s3_base_path() -> str:
    return f"s3a://{BUCKET_NAME}/{NYC_TAXI_PREFIX}"


def raw_yellow_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{RAW_LAYER}/{TAXI_TYPE}/"
        f"year={year}/month={month}/yellow_tripdata_{year}-{month}.parquet"
    )


def bronze_yellow_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{BRONZE_LAYER}/{TAXI_TYPE}/"
        f"year={year}/month={month}"
    )


def silver_yellow_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{SILVER_LAYER}/{TAXI_TYPE}/"
        f"year={year}/month={month}"
    )


def bad_records_yellow_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{BAD_RECORDS_LAYER}/{TAXI_TYPE}/"
        f"year={year}/month={month}"
    )


def quality_yellow_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{QUALITY_LAYER}/{TAXI_TYPE}/"
        f"year={year}/month={month}"
    )


def gold_daily_trips_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{GOLD_LAYER}/{TAXI_TYPE}/daily_trips/"
        f"year={year}/month={month}"
    )


def gold_hourly_trips_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{GOLD_LAYER}/{TAXI_TYPE}/hourly_trips/"
        f"year={year}/month={month}"
    )


def gold_location_pair_stats_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{GOLD_LAYER}/{TAXI_TYPE}/location_pair_stats/"
        f"year={year}/month={month}"
    )


def gold_payment_type_stats_path(year: str, month: str) -> str:
    return (
        f"{s3_base_path()}/{GOLD_LAYER}/{TAXI_TYPE}/payment_type_stats/"
        f"year={year}/month={month}"
    )

def taxi_zone_lookup_path() -> str:
    return f"{s3_base_path()}/{RAW_LAYER}/lookup/taxi_zone_lookup.csv"


def get_month_boundaries(year: str, month: str) -> Tuple[str, str]:
    year_int = int(year)
    month_int = int(month)

    month_start = date(year_int, month_int, 1)

    if month_int == 12:
        next_month_start = date(year_int + 1, 1, 1)
    else:
        next_month_start = date(year_int, month_int + 1, 1)

    return month_start.isoformat(), next_month_start.isoformat()


def validate_config() -> None:
    if not AWS_ACCESS_KEY_ID:
        raise ValueError("AWS_ACCESS_KEY_ID is not set in .env")

    if not AWS_SECRET_ACCESS_KEY:
        raise ValueError("AWS_SECRET_ACCESS_KEY is not set in .env")

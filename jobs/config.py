"""
Project configuration.

Loads environment variables from .env and stores shared settings
for all pipeline jobs.
"""

import os
from dotenv import load_dotenv


load_dotenv()


BUCKET_NAME = os.getenv("BUCKET_NAME", "nyc-taxi-natalia-2026-final-project")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://storage.yandexcloud.net")
S3_REGION = os.getenv("S3_REGION", "ru-central1")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "nyc_taxi")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "airflow")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "airflow")


def validate_config() -> None:
    if not AWS_ACCESS_KEY_ID:
        raise ValueError("AWS_ACCESS_KEY_ID is not set in .env")

    if not AWS_SECRET_ACCESS_KEY:
        raise ValueError("AWS_SECRET_ACCESS_KEY is not set in .env")
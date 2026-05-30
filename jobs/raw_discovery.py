"""
Raw monthly file discovery helpers for NYC Taxi pipeline.

This module detects which monthly raw Yellow Taxi files exist in Object Storage
and compares them with periods already loaded into the ClickHouse serving layer.

Current approach:
    raw periods       = months discovered from Object Storage raw parquet keys;
    processed periods = months found in ClickHouse gold serving tables;
    new periods       = raw periods - processed periods.

The first implementation keeps the logic lightweight:
- parsing and set-difference logic is pure Python and unit-testable;
- S3 listing uses boto3 only inside the runtime function;
- ClickHouse processed-period discovery reuses clickhouse_utils.py.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Tuple

from clickhouse_utils import fetch_json_data
from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BUCKET_NAME,
    CLICKHOUSE_DATABASE,
    NYC_TAXI_PREFIX,
    RAW_LAYER,
    S3_ENDPOINT,
    S3_REGION,
    TAXI_TYPE,
    validate_config,
)
from period_utils import validate_month_period_range


Period = Tuple[str, str]

RAW_YELLOW_KEY_PATTERN = re.compile(
    rf"^{re.escape(NYC_TAXI_PREFIX)}/"
    rf"{re.escape(RAW_LAYER)}/"
    rf"{re.escape(TAXI_TYPE)}/"
    r"year=(\d{4})/"
    r"month=(\d{2})/"
    r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$"
)


def normalize_period(year: int | str, month: int | str) -> Period:
    """
    Validate and normalize a year/month pair.

    Returns:
        ("2024", "05")
    """

    normalized_year, normalized_month, _, _ = validate_month_period_range(
        start_year=year,
        start_month=month,
        end_year=year,
        end_month=month,
    )

    return str(normalized_year), f"{normalized_month:02d}"


def period_sort_key(period: Period) -> int:
    """
    Return sortable integer key for a period.

    Example:
        ("2024", "05") -> 202405
    """

    year, month = period
    return int(year) * 100 + int(month)


def parse_raw_yellow_key(key: str) -> Optional[Period]:
    """
    Parse a raw Yellow Taxi monthly parquet object key.

    Expected key format:
        nyc_taxi/raw/yellow/year=2024/month=05/yellow_tripdata_2024-05.parquet

    Returns:
        ("2024", "05") for valid keys.
        None for keys that do not match the expected raw Yellow Taxi pattern.

    The function also checks that year/month in directories match the filename.
    """

    match = RAW_YELLOW_KEY_PATTERN.match(key)

    if not match:
        return None

    path_year, path_month, file_year, file_month = match.groups()

    if path_year != file_year or path_month != file_month:
        return None

    try:
        return normalize_period(path_year, path_month)
    except ValueError:
        return None


def discover_raw_yellow_periods_from_keys(keys: Iterable[str]) -> List[Period]:
    """
    Discover unique raw Yellow Taxi periods from object keys.

    Invalid or unrelated keys are ignored.
    Returned periods are sorted chronologically.
    """

    periods = {
        period
        for key in keys
        if (period := parse_raw_yellow_key(key)) is not None
    }

    return sorted(periods, key=period_sort_key)


def find_new_periods(
    raw_periods: Iterable[Period],
    processed_periods: Iterable[Period],
) -> List[Period]:
    """
    Return raw periods that are not yet processed.

    Both inputs may contain duplicates. Result is unique and sorted.
    """

    raw_set = {normalize_period(year, month) for year, month in raw_periods}
    processed_set = {
        normalize_period(year, month) for year, month in processed_periods
    }

    return sorted(raw_set - processed_set, key=period_sort_key)


def get_raw_yellow_prefix() -> str:
    """
    Return Object Storage prefix for raw Yellow Taxi files.
    """

    return f"{NYC_TAXI_PREFIX}/{RAW_LAYER}/{TAXI_TYPE}/"


def get_s3_client():
    """
    Create boto3 S3 client for S3-compatible Object Storage.

    boto3 is imported inside this function so pure unit tests for parsing and
    period comparison do not require boto3 to be installed locally.
    """

    import boto3

    validate_config()

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def list_raw_yellow_keys() -> List[str]:
    """
    List raw Yellow Taxi object keys from S3-compatible Object Storage.
    """

    client = get_s3_client()
    prefix = get_raw_yellow_prefix()

    keys: List[str] = []
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item.get("Key")

            if key:
                keys.append(key)

    return sorted(keys)


def list_raw_yellow_periods() -> List[Period]:
    """
    List available raw Yellow Taxi periods from Object Storage.
    """

    return discover_raw_yellow_periods_from_keys(list_raw_yellow_keys())


def build_processed_periods_query(
    table_name: str = "gold_daily_trips",
) -> str:
    """
    Build ClickHouse query for periods already loaded into the serving layer.

    The daily gold mart is used as the representative processed-period source
    because it has exactly one grain per pickup_date and contains year/month.
    """

    return f"""
    SELECT DISTINCT
        year,
        month
    FROM {CLICKHOUSE_DATABASE}.{table_name}
    ORDER BY
        year,
        month
    FORMAT JSON
    """


def list_processed_clickhouse_periods(
    table_name: str = "gold_daily_trips",
) -> List[Period]:
    """
    List periods already loaded into ClickHouse serving layer.
    """

    rows = fetch_json_data(build_processed_periods_query(table_name))

    periods = [
        normalize_period(row["year"], row["month"])
        for row in rows
    ]

    return sorted(set(periods), key=period_sort_key)


def discover_new_raw_periods() -> List[Period]:
    """
    Discover raw periods that are not yet present in ClickHouse.
    """

    raw_periods = list_raw_yellow_periods()
    processed_periods = list_processed_clickhouse_periods()

    return find_new_periods(raw_periods, processed_periods)


def format_periods(periods: Sequence[Period]) -> str:
    """
    Format periods for logs.
    """

    if not periods:
        return "none"

    return ", ".join(f"{year}-{month}" for year, month in periods)


def main() -> None:
    """
    CLI entrypoint for manual raw discovery checks.
    """

    raw_periods = list_raw_yellow_periods()
    processed_periods = list_processed_clickhouse_periods()
    new_periods = find_new_periods(raw_periods, processed_periods)

    print(f"Raw periods: {format_periods(raw_periods)}")
    print(f"Processed ClickHouse periods: {format_periods(processed_periods)}")
    print(f"New raw periods: {format_periods(new_periods)}")


if __name__ == "__main__":
    main()
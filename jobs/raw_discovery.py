"""
Raw monthly file discovery helpers for NYC Taxi pipeline.

This module detects which monthly raw Yellow Taxi files exist in Object Storage
and compares them with periods fully loaded into the ClickHouse serving layer.

Current approach:
    raw periods       = months discovered from Object Storage raw parquet keys;
    processed periods = months fully present in all expected ClickHouse gold tables;
    new periods       = raw periods - processed periods.

Why processed periods require all gold tables:
    A month can be partially loaded if a pipeline run fails after loading only
    some ClickHouse marts. In this case the month must not be treated as fully
    processed. The discovery logic therefore uses the intersection of periods
    found in all configured ClickHouse gold tables.

The implementation keeps the logic lightweight:
- parsing and set-difference logic is pure Python and unit-testable;
- S3 listing uses boto3 only inside the runtime function;
- ClickHouse processed-period discovery reuses clickhouse_utils.py.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence, Set, Tuple, Union

from clickhouse_utils import fetch_json_data
from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    BUCKET_NAME,
    CLICKHOUSE_DATABASE,
    GOLD_CLICKHOUSE_TABLES,
    NYC_TAXI_PREFIX,
    RAW_LAYER,
    S3_ENDPOINT,
    S3_REGION,
    TAXI_TYPE,
    validate_config,
)
from period_utils import validate_month_period_range


Period = Tuple[str, str]
YearMonthValue = Union[int, str]

RAW_YELLOW_KEY_PATTERN = re.compile(
    rf"^{re.escape(NYC_TAXI_PREFIX)}/"
    rf"{re.escape(RAW_LAYER)}/"
    rf"{re.escape(TAXI_TYPE)}/"
    r"year=(\d{4})/"
    r"month=(\d{2})/"
    r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$"
)


def normalize_period(year: YearMonthValue, month: YearMonthValue) -> Period:
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
    Return raw periods that are not yet fully processed.

    Both inputs may contain duplicates. Result is unique and sorted.
    """

    raw_set = {normalize_period(year, month) for year, month in raw_periods}
    processed_set = {
        normalize_period(year, month) for year, month in processed_periods
    }

    return sorted(raw_set - processed_set, key=period_sort_key)


def build_expected_periods(
    start_year: YearMonthValue,
    start_month: YearMonthValue,
    end_year: YearMonthValue,
    end_month: YearMonthValue,
) -> List[Period]:
    """
    Build a complete chronological list of expected periods.
    """

    (
        normalized_start_year,
        normalized_start_month,
        normalized_end_year,
        normalized_end_month,
    ) = validate_month_period_range(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    periods: List[Period] = []

    current_year = normalized_start_year
    current_month = normalized_start_month

    while (current_year, current_month) <= (
        normalized_end_year,
        normalized_end_month,
    ):
        periods.append((str(current_year), f"{current_month:02d}"))

        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    return periods


def find_missing_periods(
    discovered_periods: Iterable[Period],
    expected_periods: Iterable[Period],
) -> List[Period]:
    """
    Return expected periods that are missing from discovered raw periods.
    """

    discovered_set = {
        normalize_period(year, month) for year, month in discovered_periods
    }
    expected_set = {
        normalize_period(year, month) for year, month in expected_periods
    }

    return sorted(expected_set - discovered_set, key=period_sort_key)


def find_unexpected_periods(
    discovered_periods: Iterable[Period],
    expected_periods: Iterable[Period],
) -> List[Period]:
    """
    Return discovered raw periods that are outside the expected rebuild range.
    """

    discovered_set = {
        normalize_period(year, month) for year, month in discovered_periods
    }
    expected_set = {
        normalize_period(year, month) for year, month in expected_periods
    }

    return sorted(discovered_set - expected_set, key=period_sort_key)


def validate_full_rebuild_raw_periods(
    raw_periods: Iterable[Period],
    expected_start_year: YearMonthValue,
    expected_start_month: YearMonthValue,
    expected_end_year: YearMonthValue,
    expected_end_month: YearMonthValue,
) -> List[Period]:
    """
    Validate that discovered raw periods exactly match the expected full rebuild range.

    This protects destructive full rebuilds from truncating ClickHouse when the
    raw source is incomplete or contains periods outside the confirmed range.
    """

    discovered_periods = sorted(
        {
            normalize_period(year, month)
            for year, month in raw_periods
        },
        key=period_sort_key,
    )

    if not discovered_periods:
        raise ValueError(
            "Full rebuild raw source validation failed: no raw Yellow Taxi "
            "periods were discovered. ClickHouse truncate was not executed."
        )

    expected_periods = build_expected_periods(
        start_year=expected_start_year,
        start_month=expected_start_month,
        end_year=expected_end_year,
        end_month=expected_end_month,
    )

    missing_periods = find_missing_periods(
        discovered_periods=discovered_periods,
        expected_periods=expected_periods,
    )
    unexpected_periods = find_unexpected_periods(
        discovered_periods=discovered_periods,
        expected_periods=expected_periods,
    )

    if missing_periods or unexpected_periods:
        raise ValueError(
            "Full rebuild raw source validation failed. "
            f"Expected periods: {format_periods(expected_periods)}. "
            f"Discovered periods: {format_periods(discovered_periods)}. "
            f"Missing periods: {format_periods(missing_periods)}. "
            f"Unexpected periods: {format_periods(unexpected_periods)}. "
            "ClickHouse truncate was not executed."
        )

    return expected_periods


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


def build_processed_periods_query(table_name: str) -> str:
    """
    Build ClickHouse query for periods loaded into one gold table.
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


def list_clickhouse_table_periods(table_name: str) -> List[Period]:
    """
    List periods present in one ClickHouse gold table.
    """

    rows = fetch_json_data(build_processed_periods_query(table_name))

    periods = [
        normalize_period(row["year"], row["month"])
        for row in rows
    ]

    return sorted(set(periods), key=period_sort_key)


def get_fully_processed_periods_from_table_periods(
    table_periods: Iterable[Iterable[Period]],
) -> List[Period]:
    """
    Return periods that are present in every provided table-period collection.

    This pure helper is used to detect fully processed ClickHouse periods.
    A period is fully processed only if it exists in all expected gold tables.
    """

    period_sets: List[Set[Period]] = [
        {normalize_period(year, month) for year, month in periods}
        for periods in table_periods
    ]

    if not period_sets:
        return []

    fully_processed_periods = set.intersection(*period_sets)

    return sorted(fully_processed_periods, key=period_sort_key)


def list_fully_processed_clickhouse_periods(
    table_names: Sequence[str] = GOLD_CLICKHOUSE_TABLES,
) -> List[Period]:
    """
    List periods fully present in all expected ClickHouse gold tables.

    A period is considered processed only if it exists in every configured
    ClickHouse gold table. This protects the future new-month DAG from
    skipping partially loaded months.
    """

    table_periods = [
        list_clickhouse_table_periods(table_name)
        for table_name in table_names
    ]

    return get_fully_processed_periods_from_table_periods(table_periods)


def list_processed_clickhouse_periods() -> List[Period]:
    """
    List fully processed periods in ClickHouse.

    Kept as a short public alias for discovery code and CLI usage.
    """

    return list_fully_processed_clickhouse_periods()


def discover_new_raw_periods() -> List[Period]:
    """
    Discover raw periods that are not yet fully present in ClickHouse.
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
    print(f"Fully processed ClickHouse periods: {format_periods(processed_periods)}")
    print(f"New raw periods: {format_periods(new_periods)}")


if __name__ == "__main__":
    main()
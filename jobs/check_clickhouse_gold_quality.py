"""
Check ClickHouse gold tables quality for NYC Taxi pipeline.

This job validates the final analytical serving layer after all gold marts
have been loaded into ClickHouse.

Checks:
- gold tables exist;
- gold tables are not empty;
- pickup_date range covers the expected full year;
- location mart has non-empty pickup/dropoff zone names;
- payment mart has non-empty payment type names.
"""

import base64
import urllib.error
import urllib.parse
import urllib.request

from config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)


EXPECTED_MIN_DATE = "2024-01-01"
EXPECTED_MAX_DATE = "2024-12-31"

GOLD_TABLES = [
    "gold_daily_trips",
    "gold_hourly_trips",
    "gold_payment_type_stats",
    "gold_location_pair_stats",
]


def execute_clickhouse_query(query: str) -> str:
    """
    Execute a ClickHouse query via HTTP and return response text.

    We use a local helper here because quality checks need to parse query results.
    """
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

    encoded_query = query.encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=encoded_query,
        method="POST",
    )

    if CLICKHOUSE_USER:
        credentials = f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")
        encoded_credentials = base64.b64encode(credentials).decode("utf-8")
        request.add_header("Authorization", f"Basic {encoded_credentials}")

    try:
        with urllib.request.urlopen(request) as response:
            return response.read().decode("utf-8").strip()

    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8")

        raise RuntimeError(
            "ClickHouse query failed:\n"
            f"{query}\n\n"
            f"Status code: {error.code}\n"
            f"Response: {error_body}"
        ) from error


def fetch_single_value(query: str) -> str:
    result = execute_clickhouse_query(query)

    if result == "":
        raise ValueError(f"Query returned empty result:\n{query}")

    return result.splitlines()[0].strip()


def assert_table_exists(table_name: str) -> None:
    query = f"""
    SELECT count()
    FROM system.tables
    WHERE database = '{CLICKHOUSE_DATABASE}'
      AND name = '{table_name}'
    FORMAT TSV
    """

    result = int(fetch_single_value(query))

    if result != 1:
        raise AssertionError(
            f"Expected table {CLICKHOUSE_DATABASE}.{table_name} to exist, "
            f"but found {result} matching tables"
        )

    print(f"Table exists: {CLICKHOUSE_DATABASE}.{table_name}")


def assert_table_not_empty(table_name: str) -> None:
    query = f"""
    SELECT count()
    FROM {CLICKHOUSE_DATABASE}.{table_name}
    FORMAT TSV
    """

    rows_count = int(fetch_single_value(query))

    if rows_count <= 0:
        raise AssertionError(
            f"Expected table {CLICKHOUSE_DATABASE}.{table_name} "
            f"to be non-empty, but rows_count={rows_count}"
        )

    print(f"Table is not empty: {table_name}, rows_count={rows_count}")


def assert_pickup_date_range(table_name: str) -> None:
    query = f"""
    SELECT
        toString(min(pickup_date)),
        toString(max(pickup_date))
    FROM {CLICKHOUSE_DATABASE}.{table_name}
    FORMAT TSV
    """

    result = fetch_single_value(query)
    min_date, max_date = result.split("\t")

    if min_date != EXPECTED_MIN_DATE or max_date != EXPECTED_MAX_DATE:
        raise AssertionError(
            f"Unexpected pickup_date range for {table_name}: "
            f"{min_date} → {max_date}. "
            f"Expected: {EXPECTED_MIN_DATE} → {EXPECTED_MAX_DATE}"
        )

    print(f"Date range is valid for {table_name}: {min_date} → {max_date}")


def assert_location_zones_not_empty() -> None:
    table_name = "gold_location_pair_stats"

    query = f"""
    SELECT
        countIf(pickup_zone IS NULL OR pickup_zone = ''),
        countIf(dropoff_zone IS NULL OR dropoff_zone = '')
    FROM {CLICKHOUSE_DATABASE}.{table_name}
    FORMAT TSV
    """

    result = fetch_single_value(query)
    empty_pickup_zone_count, empty_dropoff_zone_count = [
        int(value) for value in result.split("\t")
    ]

    if empty_pickup_zone_count > 0 or empty_dropoff_zone_count > 0:
        raise AssertionError(
            f"Found empty zones in {table_name}: "
            f"empty_pickup_zone_count={empty_pickup_zone_count}, "
            f"empty_dropoff_zone_count={empty_dropoff_zone_count}"
        )

    print(
        "Location zones are valid: "
        f"empty_pickup_zone_count={empty_pickup_zone_count}, "
        f"empty_dropoff_zone_count={empty_dropoff_zone_count}"
    )


def assert_payment_type_names_not_empty() -> None:
    table_name = "gold_payment_type_stats"

    query = f"""
    SELECT countIf(payment_type_name IS NULL OR payment_type_name = '')
    FROM {CLICKHOUSE_DATABASE}.{table_name}
    FORMAT TSV
    """

    empty_payment_type_name_count = int(fetch_single_value(query))

    if empty_payment_type_name_count > 0:
        raise AssertionError(
            f"Found empty payment_type_name values in {table_name}: "
            f"empty_payment_type_name_count={empty_payment_type_name_count}"
        )

    print(
        "Payment type names are valid: "
        f"empty_payment_type_name_count={empty_payment_type_name_count}"
    )


def main() -> None:
    print("Starting ClickHouse gold quality checks")
    print(f"Database: {CLICKHOUSE_DATABASE}")
    print(f"Expected pickup_date range: {EXPECTED_MIN_DATE} → {EXPECTED_MAX_DATE}")

    for table_name in GOLD_TABLES:
        print()
        print("=" * 80)
        print(f"Checking table: {table_name}")
        print("=" * 80)

        assert_table_exists(table_name)
        assert_table_not_empty(table_name)
        assert_pickup_date_range(table_name)

    print()
    print("=" * 80)
    print("Checking location zone enrichment")
    print("=" * 80)
    assert_location_zones_not_empty()

    print()
    print("=" * 80)
    print("Checking payment type names")
    print("=" * 80)
    assert_payment_type_names_not_empty()

    print()
    print("ClickHouse gold quality checks passed successfully")


if __name__ == "__main__":
    main()
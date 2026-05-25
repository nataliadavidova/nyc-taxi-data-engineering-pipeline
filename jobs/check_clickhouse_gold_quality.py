"""
Check ClickHouse gold tables quality for NYC Taxi pipeline.

This job validates the final analytical serving layer after all gold marts
have been loaded into ClickHouse.

Checks:
- all expected gold tables exist;
- all gold tables are not empty;
- pickup_date range covers the expected full year;
- trip_type is populated for hourly, payment, and location marts;
- location mart has non-empty pickup/dropoff zone names;
- payment mart has non-empty payment type names.

Optimization decisions:
1. Table existence is checked with one query against system.tables.
   Why:
   - previously each table existence check ran a separate query;
   - one metadata query is simpler and cheaper.

2. Quality metrics are calculated with one aggregate query per table.
   Why:
   - previously each check ran a separate query or countIf;
   - each query can scan the table again;
   - now rows_count, min/max dates, and table-specific checks are computed
     in one pass per table.

3. The job uses ClickHouse HTTP API directly, not Spark.
   Why:
   - data is already in ClickHouse;
   - Spark is unnecessary for final serving-layer validation;
   - direct SQL checks are faster and simpler.

4. FORMAT JSON is used for query results.
   Why:
   - JSON parsing is safer than manually splitting TSV;
   - query result columns can be accessed by alias names.
"""

import base64
import json
import urllib.error
import urllib.request
from typing import Any, Dict, List

from config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    validate_config,
)


EXPECTED_MIN_DATE = "2024-01-01"
EXPECTED_MAX_DATE = "2024-12-31"

GOLD_TABLES: List[str] = [
    "gold_daily_trips",
    "gold_hourly_trips",
    "gold_payment_type_stats",
    "gold_location_pair_stats",
]

TABLES_WITH_TRIP_TYPE: List[str] = [
    "gold_hourly_trips",
    "gold_payment_type_stats",
    "gold_location_pair_stats",
]


def get_clickhouse_url() -> str:
    """
    Build ClickHouse HTTP URL.

    The database is explicitly referenced in SQL queries, so we do not need
    to pass it as a URL parameter here.
    """

    return f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"


def execute_clickhouse_query(query: str) -> str:
    """
    Execute a ClickHouse query via HTTP and return response text.

    This job uses a local HTTP helper because it needs to parse query results.
    Later, this helper can be moved into jobs/clickhouse_utils.py if we decide
    to centralize ClickHouse utilities.
    """

    validate_config()

    request = urllib.request.Request(
        url=get_clickhouse_url(),
        data=query.encode("utf-8"),
        method="POST",
    )

    if CLICKHOUSE_USER:
        credentials = f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD or ''}".encode(
            "utf-8"
        )
        encoded_credentials = base64.b64encode(credentials).decode("utf-8")
        request.add_header("Authorization", f"Basic {encoded_credentials}")

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.read().decode("utf-8").strip()

    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")

        raise RuntimeError(
            "ClickHouse query failed:\n"
            f"{query}\n\n"
            f"Status code: {error.code}\n"
            f"Response: {error_body}"
        ) from error

    except urllib.error.URLError as error:
        raise RuntimeError(f"Cannot connect to ClickHouse: {error}") from error


def fetch_json_data(query: str) -> List[Dict[str, Any]]:
    """
    Execute a ClickHouse query with FORMAT JSON and return the data rows.

    We expect queries passed here to end with FORMAT JSON.
    """

    response_text = execute_clickhouse_query(query)

    if not response_text:
        raise ValueError(f"Query returned empty result:\n{query}")

    response_json = json.loads(response_text)

    return response_json["data"]


def fetch_single_json_row(query: str) -> Dict[str, Any]:
    """
    Execute a query that must return exactly one row.

    Aggregate quality queries should always return exactly one row.
    """

    rows = fetch_json_data(query)

    if len(rows) != 1:
        raise ValueError(
            f"Expected query to return exactly one row, got {len(rows)}:\n{query}"
        )

    return rows[0]


def assert_gold_tables_exist() -> None:
    """
    Check that all expected gold tables exist in ClickHouse.

    Optimization:
    one metadata query checks all expected tables at once instead of running
    one query per table.
    """

    table_names_sql = ", ".join(f"'{table_name}'" for table_name in GOLD_TABLES)

    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{CLICKHOUSE_DATABASE}'
      AND name IN ({table_names_sql})
    FORMAT JSON
    """

    rows = fetch_json_data(query)
    found_tables = {row["name"] for row in rows}
    expected_tables = set(GOLD_TABLES)

    missing_tables = sorted(expected_tables - found_tables)

    if missing_tables:
        raise AssertionError(
            f"Missing ClickHouse gold tables in database "
            f"{CLICKHOUSE_DATABASE}: {missing_tables}"
        )

    print(
        "All expected ClickHouse gold tables exist: "
        f"{sorted(found_tables)}"
    )


def empty_string_count_expression(column_name: str, alias: str) -> str:
    """
    Build a ClickHouse countIf expression for empty string-like columns.

    We treat values as invalid if they are:
    - NULL;
    - empty string;
    - whitespace-only string.

    trim() catches whitespace-only values.
    """

    return (
        f"countIf({column_name} IS NULL OR trim({column_name}) = '') "
        f"AS {alias}"
    )


def build_quality_query(table_name: str) -> str:
    """
    Build one aggregate quality query for a ClickHouse gold table.

    Main optimization:
    all checks for one table are calculated in one SELECT, so ClickHouse does
    not need to scan the same table repeatedly for count(), min(), max(),
    countIf(), etc.
    """

    full_table_name = f"{CLICKHOUSE_DATABASE}.{table_name}"

    expressions = [
        "count() AS rows_count",
        "toString(min(pickup_date)) AS min_pickup_date",
        "toString(max(pickup_date)) AS max_pickup_date",
    ]

    if table_name in TABLES_WITH_TRIP_TYPE:
        expressions.append(
            empty_string_count_expression(
                column_name="trip_type",
                alias="empty_trip_type_count",
            )
        )

    if table_name == "gold_location_pair_stats":
        expressions.extend(
            [
                empty_string_count_expression(
                    column_name="pickup_zone",
                    alias="empty_pickup_zone_count",
                ),
                empty_string_count_expression(
                    column_name="dropoff_zone",
                    alias="empty_dropoff_zone_count",
                ),
            ]
        )

    if table_name == "gold_payment_type_stats":
        expressions.append(
            empty_string_count_expression(
                column_name="payment_type_name",
                alias="empty_payment_type_name_count",
            )
        )

    expressions_sql = ",\n        ".join(expressions)

    return f"""
    SELECT
        {expressions_sql}
    FROM {full_table_name}
    FORMAT JSON
    """


def validate_common_table_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate common quality metrics for all gold tables.

    These checks are performed on the already collected JSON row, so they do
    not trigger additional ClickHouse queries.
    """

    rows_count = int(metrics["rows_count"])
    min_pickup_date = metrics["min_pickup_date"]
    max_pickup_date = metrics["max_pickup_date"]

    print(f"Rows count: {rows_count}")

    if rows_count <= 0:
        raise AssertionError(
            f"Expected table {CLICKHOUSE_DATABASE}.{table_name} "
            f"to be non-empty, but rows_count={rows_count}"
        )

    print(
        f"Date range for {table_name}: "
        f"{min_pickup_date} → {max_pickup_date}"
    )

    if (
        min_pickup_date != EXPECTED_MIN_DATE
        or max_pickup_date != EXPECTED_MAX_DATE
    ):
        raise AssertionError(
            f"Unexpected pickup_date range for {table_name}: "
            f"{min_pickup_date} → {max_pickup_date}. "
            f"Expected: {EXPECTED_MIN_DATE} → {EXPECTED_MAX_DATE}"
        )

    print(
        f"Date range is valid for {table_name}: "
        f"{EXPECTED_MIN_DATE} → {EXPECTED_MAX_DATE}"
    )


def validate_trip_type_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate trip_type quality for marts that contain trip_type.

    gold_daily_trips does not contain trip_type as a row-level dimension,
    so it is intentionally excluded from this check.
    """

    if table_name not in TABLES_WITH_TRIP_TYPE:
        return

    empty_trip_type_count = int(metrics["empty_trip_type_count"])

    print(f"Empty trip_type count: {empty_trip_type_count}")

    if empty_trip_type_count > 0:
        raise AssertionError(
            f"Found empty trip_type values in {table_name}: "
            f"empty_trip_type_count={empty_trip_type_count}"
        )

    print(
        f"trip_type values are valid for {table_name}: "
        f"empty_trip_type_count={empty_trip_type_count}"
    )


def validate_location_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate pickup/dropoff zone enrichment for location mart.

    This check is only relevant for gold_location_pair_stats.
    """

    if table_name != "gold_location_pair_stats":
        return

    empty_pickup_zone_count = int(metrics["empty_pickup_zone_count"])
    empty_dropoff_zone_count = int(metrics["empty_dropoff_zone_count"])

    print(f"Empty pickup_zone count: {empty_pickup_zone_count}")
    print(f"Empty dropoff_zone count: {empty_dropoff_zone_count}")

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


def validate_payment_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate payment type names for payment mart.

    This check is only relevant for gold_payment_type_stats.
    """

    if table_name != "gold_payment_type_stats":
        return

    empty_payment_type_name_count = int(
        metrics["empty_payment_type_name_count"]
    )

    print(f"Empty payment_type_name count: {empty_payment_type_name_count}")

    if empty_payment_type_name_count > 0:
        raise AssertionError(
            f"Found empty payment_type_name values in {table_name}: "
            f"empty_payment_type_name_count={empty_payment_type_name_count}"
        )

    print(
        "Payment type names are valid: "
        f"empty_payment_type_name_count={empty_payment_type_name_count}"
    )


def check_gold_table_quality(table_name: str) -> None:
    """
    Run all quality checks for one gold table using one aggregate query.
    """

    print()
    print("=" * 80)
    print(f"Checking table: {table_name}")
    print("=" * 80)

    quality_query = build_quality_query(table_name)
    metrics = fetch_single_json_row(quality_query)

    validate_common_table_metrics(table_name, metrics)
    validate_trip_type_metrics(table_name, metrics)
    validate_location_metrics(table_name, metrics)
    validate_payment_metrics(table_name, metrics)

    print(f"ClickHouse gold table quality check passed: {table_name}")


def main() -> None:
    validate_config()

    print("Starting ClickHouse gold quality checks")
    print(f"Database: {CLICKHOUSE_DATABASE}")
    print(f"Expected pickup_date range: {EXPECTED_MIN_DATE} → {EXPECTED_MAX_DATE}")

    assert_gold_tables_exist()

    for table_name in GOLD_TABLES:
        check_gold_table_quality(table_name)

    print()
    print("ClickHouse gold quality checks passed successfully")


if __name__ == "__main__":
    main()
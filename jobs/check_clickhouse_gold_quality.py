"""
Check ClickHouse gold tables quality for NYC Taxi pipeline.

This job validates the final analytical serving layer after all gold marts
have been loaded into ClickHouse.

Checks:
- all expected gold tables exist;
- all gold tables are not empty;
- pickup_date range covers the complete discovered raw source range;
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

from calendar import monthrange
from typing import Any, Dict, List, Tuple

from clickhouse_utils import fetch_json_data, fetch_single_json_row
from config import (
    CLICKHOUSE_DATABASE,
    GOLD_CLICKHOUSE_TABLES,
    validate_config,
)
from raw_discovery import list_raw_yellow_periods


TABLES_WITH_TRIP_TYPE: List[str] = [
    "gold_hourly_trips",
    "gold_payment_type_stats",
    "gold_location_pair_stats",
]


def get_expected_date_range() -> Tuple[str, str]:
    """
    Build the expected ClickHouse date range from discovered raw periods.

    The first expected date is the first calendar day of the earliest raw
    month. The last expected date is the final calendar day of the latest raw
    month.

    This avoids hardcoding a historical range such as 2024 or 2016-2025.
    """

    raw_periods = list_raw_yellow_periods()

    if not raw_periods:
        raise AssertionError(
            "Cannot determine expected ClickHouse date range: "
            "no raw Yellow Taxi periods were discovered"
        )

    first_year_raw, first_month_raw = raw_periods[0]
    last_year_raw, last_month_raw = raw_periods[-1]

    first_year = int(first_year_raw)
    first_month = int(first_month_raw)
    last_year = int(last_year_raw)
    last_month = int(last_month_raw)

    last_day = monthrange(last_year, last_month)[1]

    expected_min_date = (
        f"{first_year:04d}-{first_month:02d}-01"
    )
    expected_max_date = (
        f"{last_year:04d}-{last_month:02d}-{last_day:02d}"
    )

    return expected_min_date, expected_max_date


def assert_gold_tables_exist() -> None:
    """
    Check that all expected gold tables exist in ClickHouse.

    Optimization:
    one metadata query checks all expected tables at once instead of running
    one query per table.
    """

    table_names_sql = ", ".join(f"'{table_name}'" for table_name in GOLD_CLICKHOUSE_TABLES)

    query = f"""
    SELECT name
    FROM system.tables
    WHERE database = '{CLICKHOUSE_DATABASE}'
      AND name IN ({table_names_sql})
    FORMAT JSON
    """

    rows = fetch_json_data(query)
    found_tables = {row["name"] for row in rows}
    expected_tables = set(GOLD_CLICKHOUSE_TABLES)

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
    expected_min_date: str,
    expected_max_date: str,
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
        min_pickup_date != expected_min_date
        or max_pickup_date != expected_max_date
    ):
        raise AssertionError(
            f"Unexpected pickup_date range for {table_name}: "
            f"{min_pickup_date} → {max_pickup_date}. "
            f"Expected: {expected_min_date} → {expected_max_date}"
        )

    print(
        f"Date range is valid for {table_name}: "
        f"{expected_min_date} → {expected_max_date}"
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


def check_gold_table_quality(
    table_name: str,
    expected_min_date: str,
    expected_max_date: str,
) -> None:
    """
    Run all quality checks for one gold table using one aggregate query.
    """

    print()
    print("=" * 80)
    print(f"Checking table: {table_name}")
    print("=" * 80)

    quality_query = build_quality_query(table_name)
    metrics = fetch_single_json_row(quality_query)

    validate_common_table_metrics(
        table_name,
        metrics,
        expected_min_date,
        expected_max_date,
    )
    validate_trip_type_metrics(table_name, metrics)
    validate_location_metrics(table_name, metrics)
    validate_payment_metrics(table_name, metrics)

    print(f"ClickHouse gold table quality check passed: {table_name}")


def main() -> None:
    validate_config()

    expected_min_date, expected_max_date = (
        get_expected_date_range()
    )

    print("Starting ClickHouse gold quality checks")
    print(f"Database: {CLICKHOUSE_DATABASE}")
    print(
        "Expected pickup_date range: "
        f"{expected_min_date} → {expected_max_date}"
    )

    assert_gold_tables_exist()

    for table_name in GOLD_CLICKHOUSE_TABLES:
        check_gold_table_quality(
            table_name,
            expected_min_date,
            expected_max_date,
        )

    print()
    print("ClickHouse gold quality checks passed successfully")


if __name__ == "__main__":
    main()
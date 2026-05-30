"""
Check ClickHouse gold tables quality for one selected NYC Taxi month.

This job is used by nyc_taxi_period_refresh_pipeline.py in replace_period mode.

It validates that after reloading one selected month into ClickHouse:
- all expected gold tables exist;
- each table contains rows for selected year/month;
- pickup_date values belong to the selected calendar month;
- trips_count values are positive;
- trip_type is populated for hourly, payment, and location marts;
- pickup_hour is valid for hourly mart;
- location mart has non-empty pickup/dropoff zone names;
- payment mart has non-empty payment type names.

Example:
    python jobs/check_clickhouse_gold_month_quality.py --year 2024 --month 05

Implementation notes:
1. FORMAT JSON is used for query results.
   Why:
   - JSON parsing is safer than splitting raw text output;
   - query result columns can be accessed by alias names.

2. Quality metrics are calculated with one aggregate query per table.
   Why:
   - rows_count, min/max dates, and table-specific checks are computed
     in one pass per table.

3. This job checks only selected year/month.
   Why:
   - replace_period mode reloads one month or a configured month interval;
   - after each monthly reload we need a month-scoped validation.
"""

from __future__ import annotations

import argparse
from typing import Any, Dict, List

from clickhouse_utils import fetch_json_data, fetch_single_json_row
from config import (
    CLICKHOUSE_DATABASE,
    GOLD_CLICKHOUSE_TABLES,
    get_month_boundaries,
    validate_config,
)

from period_utils import validate_month_period_range


TABLES_WITH_TRIP_TYPE: List[str] = [
    "gold_hourly_trips",
    "gold_payment_type_stats",
    "gold_location_pair_stats",
]


def normalize_year_month(year: int | str, month: int | str) -> tuple[str, str]:
    """
    Validate and normalize year/month values.

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


def build_tables_exist_query() -> str:
    """
    Build metadata query that checks all expected gold tables at once.
    """

    table_names_sql = ", ".join(
        f"'{table_name}'" for table_name in GOLD_CLICKHOUSE_TABLES
    )

    return f"""
    SELECT name
    FROM system.tables
    WHERE database = '{CLICKHOUSE_DATABASE}'
      AND name IN ({table_names_sql})
    FORMAT JSON
    """


def assert_gold_tables_exist() -> None:
    """
    Check that all expected gold tables exist in ClickHouse.
    """

    rows = fetch_json_data(build_tables_exist_query())
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
    """

    return (
        f"countIf({column_name} IS NULL OR trim({column_name}) = '') "
        f"AS {alias}"
    )


def build_quality_query(
    table_name: str,
    year: str,
    month: str,
    month_start: str,
    next_month_start: str,
) -> str:
    """
    Build one aggregate quality query for a selected ClickHouse gold table.

    The query is scoped to one selected year/month.
    """

    full_table_name = f"{CLICKHOUSE_DATABASE}.{table_name}"

    expressions = [
        "count() AS rows_count",
        "toString(min(pickup_date)) AS min_pickup_date",
        "toString(max(pickup_date)) AS max_pickup_date",
        "countIf(trips_count <= 0) AS non_positive_trips_count",
        (
            "countIf("
            f"pickup_date < toDate('{month_start}') "
            f"OR pickup_date >= toDate('{next_month_start}')"
            ") AS out_of_month_date_count"
        ),
    ]

    if table_name in TABLES_WITH_TRIP_TYPE:
        expressions.append(
            empty_string_count_expression(
                column_name="trip_type",
                alias="empty_trip_type_count",
            )
        )

    if table_name == "gold_hourly_trips":
        expressions.append(
            "countIf(pickup_hour < 0 OR pickup_hour > 23) "
            "AS invalid_pickup_hour_count"
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
    WHERE year = '{year}'
      AND month = '{month}'
    FORMAT JSON
    """


def validate_common_table_metrics(
    table_name: str,
    metrics: Dict[str, Any],
    month_start: str,
    next_month_start: str,
) -> None:
    """
    Validate common quality metrics for all gold tables.
    """

    rows_count = int(metrics["rows_count"])
    min_pickup_date = metrics["min_pickup_date"]
    max_pickup_date = metrics["max_pickup_date"]
    non_positive_trips_count = int(metrics["non_positive_trips_count"])
    out_of_month_date_count = int(metrics["out_of_month_date_count"])

    print(f"Rows count: {rows_count}")

    if rows_count <= 0:
        raise AssertionError(
            f"Expected table {CLICKHOUSE_DATABASE}.{table_name} "
            f"to have rows for selected month, but rows_count={rows_count}"
        )

    print(
        f"Date range for {table_name}: "
        f"{min_pickup_date} → {max_pickup_date}"
    )

    if min_pickup_date < month_start or max_pickup_date >= next_month_start:
        raise AssertionError(
            f"Unexpected pickup_date range for {table_name}: "
            f"{min_pickup_date} → {max_pickup_date}. "
            f"Expected dates to be within "
            f"[{month_start}, {next_month_start})"
        )

    if out_of_month_date_count > 0:
        raise AssertionError(
            f"Found out-of-month pickup_date values in {table_name}: "
            f"out_of_month_date_count={out_of_month_date_count}"
        )

    if non_positive_trips_count > 0:
        raise AssertionError(
            f"Found non-positive trips_count values in {table_name}: "
            f"non_positive_trips_count={non_positive_trips_count}"
        )

    print(
        f"Common metrics are valid for {table_name}: "
        f"date range within [{month_start}, {next_month_start}), "
        f"non_positive_trips_count={non_positive_trips_count}, "
        f"out_of_month_date_count={out_of_month_date_count}"
    )


def validate_trip_type_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate trip_type quality for marts that contain trip_type.
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


def validate_hourly_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate pickup_hour values for hourly mart.
    """

    if table_name != "gold_hourly_trips":
        return

    invalid_pickup_hour_count = int(metrics["invalid_pickup_hour_count"])

    print(f"Invalid pickup_hour count: {invalid_pickup_hour_count}")

    if invalid_pickup_hour_count > 0:
        raise AssertionError(
            f"Found invalid pickup_hour values in {table_name}: "
            f"invalid_pickup_hour_count={invalid_pickup_hour_count}"
        )

    print(
        f"pickup_hour values are valid for {table_name}: "
        f"invalid_pickup_hour_count={invalid_pickup_hour_count}"
    )


def validate_location_metrics(
    table_name: str,
    metrics: Dict[str, Any],
) -> None:
    """
    Validate pickup/dropoff zone enrichment for location mart.
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


def check_gold_table_month_quality(
    table_name: str,
    year: str,
    month: str,
) -> None:
    """
    Run all month-scoped quality checks for one gold table.
    """

    month_start, next_month_start = get_month_boundaries(year, month)

    print()
    print("=" * 80)
    print(f"Checking table: {table_name}")
    print(f"Selected period: year={year}, month={month}")
    print(f"Expected pickup_date range: [{month_start}, {next_month_start})")
    print("=" * 80)

    quality_query = build_quality_query(
        table_name=table_name,
        year=year,
        month=month,
        month_start=month_start,
        next_month_start=next_month_start,
    )
    metrics = fetch_single_json_row(quality_query)

    validate_common_table_metrics(
        table_name=table_name,
        metrics=metrics,
        month_start=month_start,
        next_month_start=next_month_start,
    )
    validate_trip_type_metrics(table_name, metrics)
    validate_hourly_metrics(table_name, metrics)
    validate_location_metrics(table_name, metrics)
    validate_payment_metrics(table_name, metrics)

    print(
        "ClickHouse gold table month quality check passed: "
        f"{table_name}, year={year}, month={month}"
    )


def check_clickhouse_gold_month_quality(
    year: int | str,
    month: int | str,
) -> None:
    """
    Run ClickHouse gold month quality checks for all configured gold tables.
    """

    normalized_year, normalized_month = normalize_year_month(
        year=year,
        month=month,
    )

    validate_config()

    month_start, next_month_start = get_month_boundaries(
        normalized_year,
        normalized_month,
    )

    print("Starting ClickHouse gold month quality checks")
    print(f"Database: {CLICKHOUSE_DATABASE}")
    print(f"Selected period: year={normalized_year}, month={normalized_month}")
    print(f"Expected pickup_date range: [{month_start}, {next_month_start})")

    assert_gold_tables_exist()

    for table_name in GOLD_CLICKHOUSE_TABLES:
        check_gold_table_month_quality(
            table_name=table_name,
            year=normalized_year,
            month=normalized_month,
        )

    print()
    print(
        "ClickHouse gold month quality checks passed successfully: "
        f"year={normalized_year}, month={normalized_month}"
    )


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description="Check ClickHouse gold tables quality for selected month."
    )

    parser.add_argument(
        "--year",
        required=True,
        help="Year to check, for example: 2024",
    )
    parser.add_argument(
        "--month",
        required=True,
        help="Month to check, for example: 05",
    )

    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint.
    """

    args = parse_args()

    check_clickhouse_gold_month_quality(
        year=args.year,
        month=args.month,
    )


if __name__ == "__main__":
    main()

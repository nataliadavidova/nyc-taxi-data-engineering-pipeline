"""
Delete one selected year-month period from ClickHouse gold tables.

This job is used by nyc_taxi_period_refresh_pipeline.py in replace_period mode.

Why this job exists:
    Monthly ClickHouse load jobs use append mode.
    Before reloading a selected month, we must delete existing rows for this
    exact year/month from ClickHouse gold tables to avoid duplicates.

Example:
    python jobs/delete_clickhouse_gold_month.py --year 2024 --month 05

Important ClickHouse note:
    ALTER TABLE ... DELETE is a mutation and may be applied asynchronously.
    Therefore, after sending DELETE we poll the table until count() for the
    selected year/month becomes zero.
"""

from __future__ import annotations

import argparse
import time
from typing import Sequence

from config import CLICKHOUSE_DATABASE, GOLD_CLICKHOUSE_TABLES
from period_utils import validate_month_period_range
from clickhouse_utils import execute_clickhouse_query


def normalize_year_month(year: int | str, month: int | str) -> tuple[str, str]:
    """
    Validate and normalize year/month values.

    Returns:
        ("2024", "05")

    Raises:
        ValueError: if year/month values are invalid.
    """

    normalized_year, normalized_month, _, _ = validate_month_period_range(
        start_year=year,
        start_month=month,
        end_year=year,
        end_month=month,
    )

    return str(normalized_year), f"{normalized_month:02d}"


def build_delete_query(table_name: str, year: str, month: str) -> str:
    """
    Build ClickHouse DELETE mutation query for one table and one month.
    """

    return (
        f"ALTER TABLE {CLICKHOUSE_DATABASE}.{table_name} "
        f"DELETE WHERE year = '{year}' AND month = '{month}'"
    )


def build_count_query(table_name: str, year: str, month: str) -> str:
    """
    Build query that checks how many rows still exist for selected year/month.
    """

    return (
        "SELECT count()\n"
        f"FROM {CLICKHOUSE_DATABASE}.{table_name}\n"
        f"WHERE year = '{year}' AND month = '{month}'"
    )


def parse_count_response(response_text: str) -> int:
    """
    Parse ClickHouse count() response.

    ClickHouse HTTP response for SELECT count() is usually a simple string:
        "0"
        "123"

    We parse the first non-empty line as integer.
    """

    response_lines = [
        line.strip()
        for line in response_text.splitlines()
        if line.strip()
    ]

    if not response_lines:
        raise ValueError("ClickHouse count response is empty")

    try:
        return int(response_lines[0])
    except ValueError:
        raise ValueError(
            f"Cannot parse ClickHouse count response: {response_text!r}"
        ) from None


def get_month_row_count(table_name: str, year: str, month: str) -> int:
    """
    Return number of rows for selected year/month in one ClickHouse table.
    """

    query = build_count_query(table_name=table_name, year=year, month=month)
    response_text = execute_clickhouse_query(query)

    return parse_count_response(response_text)


def wait_until_month_deleted(
    table_name: str,
    year: str,
    month: str,
    max_attempts: int = 30,
    sleep_seconds: int = 5,
) -> None:
    """
    Wait until selected year/month disappears from one ClickHouse table.

    Why polling:
        ALTER TABLE ... DELETE in ClickHouse is mutation-based and may not be
        visible immediately after the query is submitted.

    Raises:
        RuntimeError: if rows are still present after all attempts.
    """

    for attempt in range(1, max_attempts + 1):
        rows_count = get_month_row_count(
            table_name=table_name,
            year=year,
            month=month,
        )

        if rows_count == 0:
            print(
                f"{CLICKHOUSE_DATABASE}.{table_name}: "
                f"year={year}, month={month} deleted successfully"
            )
            return

        print(
            f"{CLICKHOUSE_DATABASE}.{table_name}: "
            f"year={year}, month={month} still has {rows_count} rows "
            f"after attempt {attempt}/{max_attempts}"
        )

        if attempt < max_attempts:
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"{CLICKHOUSE_DATABASE}.{table_name}: "
        f"year={year}, month={month} was not deleted after "
        f"{max_attempts} attempts"
    )


def delete_gold_month_from_table(
    table_name: str,
    year: str,
    month: str,
    max_attempts: int = 30,
    sleep_seconds: int = 5,
) -> None:
    """
    Delete selected year/month from one ClickHouse gold table and verify it.
    """

    delete_query = build_delete_query(
        table_name=table_name,
        year=year,
        month=month,
    )

    print(f"Executing: {delete_query}")
    execute_clickhouse_query(delete_query)

    wait_until_month_deleted(
        table_name=table_name,
        year=year,
        month=month,
        max_attempts=max_attempts,
        sleep_seconds=sleep_seconds,
    )


def delete_gold_month(
    year: int | str,
    month: int | str,
    table_names: Sequence[str] | None = None,
    max_attempts: int = 30,
    sleep_seconds: int = 5,
) -> None:
    """
    Delete selected year/month from all configured ClickHouse gold tables.
    """

    normalized_year, normalized_month = normalize_year_month(
        year=year,
        month=month,
    )

    tables_to_delete = (
        GOLD_CLICKHOUSE_TABLES
        if table_names is None
        else table_names
    )

    for table_name in tables_to_delete:
        delete_gold_month_from_table(
            table_name=table_name,
            year=normalized_year,
            month=normalized_month,
            max_attempts=max_attempts,
            sleep_seconds=sleep_seconds,
        )

    print(
        "Selected ClickHouse gold month deleted successfully: "
        f"year={normalized_year}, month={normalized_month}"
    )


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """

    parser = argparse.ArgumentParser(
        description="Delete selected year/month from ClickHouse gold tables."
    )

    parser.add_argument(
        "--year",
        required=True,
        help="Year to delete, for example: 2024",
    )
    parser.add_argument(
        "--month",
        required=True,
        help="Month to delete, for example: 05",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=30,
        help="Maximum number of count() polling attempts.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=int,
        default=5,
        help="Sleep time between polling attempts.",
    )

    return parser.parse_args()


def main() -> None:
    """
    CLI entrypoint.
    """

    args = parse_args()

    delete_gold_month(
        year=args.year,
        month=args.month,
        max_attempts=args.max_attempts,
        sleep_seconds=args.sleep_seconds,
    )


if __name__ == "__main__":
    main()

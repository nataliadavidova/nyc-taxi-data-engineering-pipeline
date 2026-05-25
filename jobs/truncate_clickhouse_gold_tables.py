"""
Truncate ClickHouse gold tables before full refresh load.

This job is used before loading all 2024 monthly gold marts into ClickHouse.

Why this job exists:
    Monthly ClickHouse load jobs use mode("append").
    Before a full-year reload, we truncate target gold tables to avoid duplicate
    rows when the DAG is rerun.

This script is intentionally simple:
    - it does not scan data;
    - it only sends TRUNCATE TABLE commands to ClickHouse;
    - performance optimization is not critical here.

Production-like improvements:
1. validate_config() is called before executing ClickHouse commands.
   Why:
   - fail fast if ClickHouse connection settings are missing.

2. HTTP requests use timeout.
   Why:
   - the job should not hang forever if ClickHouse is unavailable.

3. Empty password is handled safely.
   Why:
   - avoid creating credentials like "user:None".

4. URLError is handled explicitly.
   Why:
   - connection failures should produce a clear error message.

5. execute_clickhouse_query returns response text.
   Why:
   - the helper becomes reusable by other small ClickHouse utility scripts.
"""

import base64
import urllib.error
import urllib.request

from config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    validate_config,
)


GOLD_TABLES = [
    "gold_daily_trips",
    "gold_hourly_trips",
    "gold_location_pair_stats",
    "gold_payment_type_stats",
]


def get_clickhouse_url() -> str:
    """
    Build ClickHouse HTTP URL.
    """

    return f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"


def execute_clickhouse_query(query: str) -> str:
    """
    Execute a ClickHouse query via HTTP and return response text.

    This helper is used by truncate_clickhouse_gold_tables.py and currently
    also imported by create_clickhouse_gold_tables.py.

    Later, if we introduce ClickHouse schema migration/versioning, this helper
    can be moved into a dedicated clickhouse_utils.py module.
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
            response_body = response.read().decode("utf-8").strip()

            if response_body:
                print(response_body)

            return response_body

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


def truncate_gold_table(table_name: str) -> None:
    """
    Truncate one ClickHouse gold table if it exists.

    TRUNCATE TABLE IF EXISTS keeps the job idempotent:
    - if the table exists, data is removed;
    - if the table does not exist yet, the command does not fail.
    """

    query = f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table_name}"

    print(f"Executing: {query}")
    execute_clickhouse_query(query)


def main() -> None:
    """
    Truncate all ClickHouse gold tables before full refresh load.
    """

    validate_config()

    for table_name in GOLD_TABLES:
        truncate_gold_table(table_name)

    print("ClickHouse gold tables truncated successfully")


if __name__ == "__main__":
    main()
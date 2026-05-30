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

ClickHouse HTTP execution is handled by jobs/clickhouse_utils.py.
"""

from config import (
    CLICKHOUSE_DATABASE,
    GOLD_CLICKHOUSE_TABLES,
    validate_config,
)
from clickhouse_utils import execute_clickhouse_query


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
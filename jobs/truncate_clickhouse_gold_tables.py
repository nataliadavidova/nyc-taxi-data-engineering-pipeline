"""
Truncate ClickHouse gold tables before full refresh load.

This job is used before loading all 2024 monthly gold marts into ClickHouse.
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
)


GOLD_TABLES = [
    "gold_daily_trips",
    "gold_hourly_trips",
    "gold_location_pair_stats",
    "gold_payment_type_stats",
]


def execute_clickhouse_query(query: str) -> None:
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"

    credentials = f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")
    encoded_credentials = base64.b64encode(credentials).decode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=query.encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Basic {encoded_credentials}",
        },
    )

    try:
        with urllib.request.urlopen(request) as response:
            response_body = response.read().decode("utf-8")
            print(response_body)
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8")
        raise RuntimeError(
            f"ClickHouse query failed: {query}\n"
            f"Status code: {error.code}\n"
            f"Response: {error_body}"
        ) from error


def main() -> None:
    for table_name in GOLD_TABLES:
        query = f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table_name}"
        print(f"Executing: {query}")
        execute_clickhouse_query(query)

    print("ClickHouse gold tables truncated successfully")


if __name__ == "__main__":
    main()
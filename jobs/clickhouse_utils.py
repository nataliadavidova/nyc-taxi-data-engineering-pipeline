"""
Shared ClickHouse HTTP helpers for NYC Taxi pipeline jobs.

This module centralizes lightweight ClickHouse HTTP API logic used by
ClickHouse utility and quality-check jobs.

Why this module exists:
    Several jobs need to execute ClickHouse SQL queries directly without Spark:
    - table creation;
    - table truncation;
    - month-level deletion;
    - full-year quality checks;
    - month-level quality checks;
    - geospatial lookup loading.

Keeping this logic in one place avoids duplicating:
    - ClickHouse HTTP URL construction;
    - Basic Auth handling;
    - HTTP timeout handling;
    - HTTP/URL error handling;
    - JSON response parsing helpers.

This refactor intentionally does not change pipeline behavior.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from typing import Any, Dict, List

from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    validate_config,
)


def get_clickhouse_url() -> str:
    """
    Build ClickHouse HTTP URL.

    The database is explicitly referenced in SQL queries, so we do not need
    to pass it as a URL parameter here.
    """

    return f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"


def execute_clickhouse_query(
    query: str,
    print_response: bool = True,
) -> str:
    """
    Execute a ClickHouse query via HTTP and return response text.

    The helper:
    - validates required configuration;
    - sends the query through ClickHouse HTTP API;
    - applies Basic Auth if CLICKHOUSE_USER is configured;
    - uses a timeout so jobs do not hang forever;
    - raises clear RuntimeError messages for HTTP and connection failures.

    Args:
        query: ClickHouse SQL query.
        print_response: Whether to print non-empty ClickHouse response text.

    Returns:
        Response body as a stripped string.
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

            if response_body and print_response:
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


def fetch_json_data(query: str) -> List[Dict[str, Any]]:
    """
    Execute a ClickHouse query with FORMAT JSON and return data rows.

    Queries passed to this helper are expected to return ClickHouse JSON output.
    """

    response_text = execute_clickhouse_query(query, print_response=False)

    if not response_text:
        raise ValueError(f"Query returned empty result:\n{query}")

    response_json = json.loads(response_text)

    return response_json["data"]


def fetch_single_json_row(query: str) -> Dict[str, Any]:
    """
    Execute a query that must return exactly one JSON row.
    """

    rows = fetch_json_data(query)

    if len(rows) != 1:
        raise ValueError(
            f"Expected query to return exactly one row, got {len(rows)}:\n{query}"
        )

    return rows[0]

"""
Run a ClickHouse SQL query from a .sql file.

Usage:
python jobs/run_clickhouse_sql_file.py --sql-file sql/analytics/01_top_pickup_zones.sql

By default, the runner appends FORMAT TabSeparatedWithNames to SELECT queries
so query results include column names in the first output row.
"""

import argparse
import re
from pathlib import Path

from clickhouse_utils import execute_clickhouse_query


DEFAULT_OUTPUT_FORMAT = "TabSeparatedWithNames"


def read_sql_file(sql_file_path: str) -> str:
    path = Path(sql_file_path)

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file_path}")

    query = path.read_text(encoding="utf-8").strip()

    if query.endswith(";"):
        query = query[:-1].strip()

    if not query:
        raise ValueError(f"SQL file is empty: {sql_file_path}")

    return query


def has_explicit_format(query: str) -> bool:
    """
    Check whether the query already has an explicit ClickHouse FORMAT clause.

    This helps avoid appending FORMAT twice.
    """
    return re.search(r"\bFORMAT\s+\w+\s*$", query, flags=re.IGNORECASE) is not None


def add_default_output_format(query: str) -> str:
    """
    Add a ClickHouse output format that includes column names.

    FORMAT TabSeparatedWithNames makes query output easier to read in terminal:
    the first output row contains column names.
    """
    if has_explicit_format(query):
        return query

    return f"{query}\nFORMAT {DEFAULT_OUTPUT_FORMAT}"


def main(sql_file_path: str) -> None:
    query = read_sql_file(sql_file_path)
    query = add_default_output_format(query)

    print(f"Running SQL file: {sql_file_path}")
    print("=" * 80)
    print(query)
    print("=" * 80)

    execute_clickhouse_query(query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-file", required=True)

    args = parser.parse_args()

    main(sql_file_path=args.sql_file)
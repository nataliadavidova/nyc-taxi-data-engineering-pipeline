"""
Utilities for working with year-month periods in NYC Taxi pipelines.

This module is intentionally small and independent:
- DAGs can use it to build period-based tasks.
- Tests can validate period logic without Airflow/Spark/ClickHouse.
"""

from __future__ import annotations

from datetime import date
from typing import List, Tuple


YearMonth = Tuple[str, str]


def _parse_year(value: int | str, field_name: str) -> int:
    """
    Convert year value to int and validate it.

    Examples:
        "2024" -> 2024
        2024 -> 2024
    """
    try:
        year = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid year, got {value!r}") from None

    if year < 1:
        raise ValueError(f"{field_name} must be greater than 0, got {value!r}")

    return year


def _parse_month(value: int | str, field_name: str) -> int:
    """
    Convert month value to int and validate it.

    Examples:
        "01" -> 1
        "1" -> 1
        1 -> 1
    """
    try:
        month = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name} must be a valid month, got {value!r}") from None

    if month < 1 or month > 12:
        raise ValueError(f"{field_name} must be between 1 and 12, got {value!r}")

    return month


def validate_month_period_range(
    start_year: int | str,
    start_month: int | str,
    end_year: int | str,
    end_month: int | str,
) -> tuple[int, int, int, int]:
    """
    Validate the start/end year-month range.

    Returns normalized integer values:
        (start_year, start_month, end_year, end_month)

    Raises:
        ValueError: if month is invalid or start period is after end period.
    """
    normalized_start_year = _parse_year(start_year, "start_year")
    normalized_start_month = _parse_month(start_month, "start_month")
    normalized_end_year = _parse_year(end_year, "end_year")
    normalized_end_month = _parse_month(end_month, "end_month")

    start_date = date(normalized_start_year, normalized_start_month, 1)
    end_date = date(normalized_end_year, normalized_end_month, 1)

    if start_date > end_date:
        raise ValueError(
            "start period must be earlier than or equal to end period, "
            f"got {normalized_start_year}-{normalized_start_month:02d} "
            f"> {normalized_end_year}-{normalized_end_month:02d}"
        )

    return (
        normalized_start_year,
        normalized_start_month,
        normalized_end_year,
        normalized_end_month,
    )


def generate_month_periods(
    start_year: int | str,
    start_month: int | str,
    end_year: int | str,
    end_month: int | str,
) -> List[YearMonth]:
    """
    Generate a list of year-month tuples for an inclusive period.

    Example:
        generate_month_periods("2024", "11", "2025", "02")

    Returns:
        [
            ("2024", "11"),
            ("2024", "12"),
            ("2025", "01"),
            ("2025", "02"),
        ]

    Notes:
        - The range is inclusive.
        - Year and month are returned as strings.
        - Month is always zero-padded to two digits.
    """
    (
        normalized_start_year,
        normalized_start_month,
        normalized_end_year,
        normalized_end_month,
    ) = validate_month_period_range(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    periods: List[YearMonth] = []

    current_year = normalized_start_year
    current_month = normalized_start_month

    while (current_year, current_month) <= (normalized_end_year, normalized_end_month):
        periods.append((str(current_year), f"{current_month:02d}"))

        if current_month == 12:
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    return periods

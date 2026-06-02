"""
Period refresh runtime configuration helpers.

This module prepares runtime period-refresh parameters for
nyc_taxi_period_refresh_pipeline.py.

The goal is to support Airflow Trigger DAG config / Params instead of editing
static constants inside the DAG file.

Supported mode:
    replace_period

Example trigger config:
    {
        "start_year": "2024",
        "start_month": "05",
        "end_year": "2024",
        "end_month": "05",
        "refresh_mode": "replace_period"
    }
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from period_utils import generate_month_periods


DEFAULT_START_YEAR = "2024"
DEFAULT_START_MONTH = "01"
DEFAULT_END_YEAR = "2024"
DEFAULT_END_MONTH = "01"
DEFAULT_REFRESH_MODE = "replace_period"

SUPPORTED_REFRESH_MODES = {"replace_period"}

# Safety guard for local execution.
# Full-year reloads are still possible, but very large accidental intervals are blocked.
MAX_MONTHS_PER_REFRESH = 12


PeriodRefreshConfig = Dict[str, Any]
PeriodParam = Dict[str, str]


def get_config_value(
    config: Optional[PeriodRefreshConfig],
    key: str,
    default_value: str,
) -> str:
    """
    Read a config value with a default.

    Empty strings and None are treated as missing values.
    """

    if not config:
        return default_value

    value = config.get(key)

    if value is None:
        return default_value

    value = str(value).strip()

    if not value:
        return default_value

    return value


def validate_refresh_mode(refresh_mode: str) -> str:
    """
    Validate refresh mode.
    """

    normalized_refresh_mode = str(refresh_mode).strip()

    if normalized_refresh_mode not in SUPPORTED_REFRESH_MODES:
        raise ValueError(
            f"Unsupported refresh_mode={normalized_refresh_mode!r}. "
            f"Supported modes: {sorted(SUPPORTED_REFRESH_MODES)}"
        )

    return normalized_refresh_mode


def normalize_period_param(year: str, month: str) -> PeriodParam:
    """
    Normalize one period tuple to a mapped TaskGroup parameter dictionary.
    """

    return {
        "year": str(year),
        "month": f"{int(month):02d}",
    }


def build_period_refresh_periods(
    config: Optional[PeriodRefreshConfig],
) -> List[PeriodParam]:
    """
    Build mapped period parameters from runtime config.

    Returned format:
        [
            {"year": "2024", "month": "05"},
            {"year": "2024", "month": "06"},
        ]
    """

    start_year = get_config_value(
        config=config,
        key="start_year",
        default_value=DEFAULT_START_YEAR,
    )
    start_month = get_config_value(
        config=config,
        key="start_month",
        default_value=DEFAULT_START_MONTH,
    )
    end_year = get_config_value(
        config=config,
        key="end_year",
        default_value=DEFAULT_END_YEAR,
    )
    end_month = get_config_value(
        config=config,
        key="end_month",
        default_value=DEFAULT_END_MONTH,
    )
    refresh_mode = get_config_value(
        config=config,
        key="refresh_mode",
        default_value=DEFAULT_REFRESH_MODE,
    )

    validate_refresh_mode(refresh_mode)

    periods = generate_month_periods(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    )

    if len(periods) > MAX_MONTHS_PER_REFRESH:
        raise ValueError(
            f"Period refresh interval is too large: {len(periods)} months. "
            f"Maximum allowed: {MAX_MONTHS_PER_REFRESH} months."
        )

    return [
        normalize_period_param(year, month)
        for year, month in periods
    ]


def format_period_params(periods: List[PeriodParam]) -> str:
    """
    Format period parameters for logs.
    """

    if not periods:
        return "none"

    return ", ".join(
        f"{period['year']}-{period['month']}"
        for period in periods
    )
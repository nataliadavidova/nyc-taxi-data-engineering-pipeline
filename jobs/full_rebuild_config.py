"""
Runtime configuration validation for protected full raw rebuild DAG.

The full rebuild DAG is a destructive operation because it truncates ClickHouse
gold tables before rebuilding them from raw Object Storage data.

This module validates explicit runtime confirmations and the expected full
rebuild period range before the DAG is allowed to reach the truncate step.
"""

from __future__ import annotations

from typing import Any, Dict

from period_utils import validate_month_period_range


FULL_REBUILD_MODE = "full_raw_rebuild"
TRUNCATE_SAFETY_MESSAGE = "ClickHouse truncate was not executed."

DEFAULT_REBUILD_MODE = ""
DEFAULT_CONFIRM_FULL_REBUILD = False
DEFAULT_CONFIRM_CLICKHOUSE_TRUNCATE = False
DEFAULT_EXPECTED_START_YEAR = ""
DEFAULT_EXPECTED_START_MONTH = ""
DEFAULT_EXPECTED_END_YEAR = ""
DEFAULT_EXPECTED_END_MONTH = ""


FullRebuildConfig = Dict[str, Any]


def is_explicit_true(value: Any) -> bool:
    """
    Return True only for explicit true values accepted by runtime config.

    Airflow Trigger DAG config uses JSON booleans, but string "true" is also
    accepted for manual UI usage.
    """

    if value is True:
        return True

    if isinstance(value, str) and value.strip().lower() == "true":
        return True

    return False


def get_required_config_value(
    config: FullRebuildConfig,
    key: str,
) -> Any:
    """
    Read a required config value and reject missing or empty values.
    """

    value = config.get(key)

    if value is None:
        raise ValueError(
            f"Missing required full rebuild config value: {key}. "
            f"{TRUNCATE_SAFETY_MESSAGE}"
        )

    if isinstance(value, str) and not value.strip():
        raise ValueError(
            f"Missing required full rebuild config value: {key}. "
            f"{TRUNCATE_SAFETY_MESSAGE}"
        )

    return value


def validate_full_rebuild_runtime_config(
    runtime_config: FullRebuildConfig,
) -> FullRebuildConfig:
    """
    Validate runtime config required for protected full raw rebuild.

    Required values:
        rebuild_mode = full_raw_rebuild
        confirm_full_rebuild = true
        confirm_clickhouse_truncate = true
        expected_start_year/month
        expected_end_year/month

    Returns normalized config values for downstream Airflow tasks.
    """

    rebuild_mode = runtime_config.get("rebuild_mode")

    if rebuild_mode != FULL_REBUILD_MODE:
        raise ValueError(
            "Full rebuild is not confirmed: rebuild_mode must be "
            f"{FULL_REBUILD_MODE!r}. ClickHouse truncate was not executed."
        )

    if not is_explicit_true(runtime_config.get("confirm_full_rebuild")):
        raise ValueError(
            "Full rebuild is not confirmed: confirm_full_rebuild must be true. "
            "ClickHouse truncate was not executed."
        )

    if not is_explicit_true(runtime_config.get("confirm_clickhouse_truncate")):
        raise ValueError(
            "ClickHouse truncate is not confirmed: "
            "confirm_clickhouse_truncate must be true. "
            "ClickHouse truncate was not executed."
        )

    expected_start_year = get_required_config_value(
        runtime_config,
        "expected_start_year",
    )
    expected_start_month = get_required_config_value(
        runtime_config,
        "expected_start_month",
    )
    expected_end_year = get_required_config_value(
        runtime_config,
        "expected_end_year",
    )
    expected_end_month = get_required_config_value(
        runtime_config,
        "expected_end_month",
    )

    try:
        (
            normalized_start_year,
            normalized_start_month,
            normalized_end_year,
            normalized_end_month,
        ) = validate_month_period_range(
            start_year=expected_start_year,
            start_month=expected_start_month,
            end_year=expected_end_year,
            end_month=expected_end_month,
        )
    except ValueError as error:
        raise ValueError(
            f"{error}. {TRUNCATE_SAFETY_MESSAGE}"
        ) from error

    return {
        "rebuild_mode": FULL_REBUILD_MODE,
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": str(normalized_start_year),
        "expected_start_month": f"{normalized_start_month:02d}",
        "expected_end_year": str(normalized_end_year),
        "expected_end_month": f"{normalized_end_month:02d}",
    }
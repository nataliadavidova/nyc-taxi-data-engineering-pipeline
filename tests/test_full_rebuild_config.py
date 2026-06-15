import pytest

import full_rebuild_config as config


def test_is_explicit_true_accepts_boolean_true():
    assert config.is_explicit_true(True) is True


def test_is_explicit_true_accepts_string_true():
    assert config.is_explicit_true("true") is True
    assert config.is_explicit_true("TRUE") is True
    assert config.is_explicit_true(" true ") is True


def test_is_explicit_true_rejects_non_true_values():
    assert config.is_explicit_true(False) is False
    assert config.is_explicit_true("false") is False
    assert config.is_explicit_true("yes") is False
    assert config.is_explicit_true(1) is False
    assert config.is_explicit_true(None) is False


def test_get_required_config_value_returns_present_value():
    runtime_config = {
        "expected_start_year": "2024",
    }

    assert config.get_required_config_value(
        runtime_config,
        "expected_start_year",
    ) == "2024"


def test_get_required_config_value_rejects_missing_value():
    with pytest.raises(
        ValueError,
        match="Missing required full rebuild config value: expected_start_year",
    ):
        config.get_required_config_value({}, "expected_start_year")


def test_get_required_config_value_rejects_empty_string():
    runtime_config = {
        "expected_start_year": " ",
    }

    with pytest.raises(
        ValueError,
        match="Missing required full rebuild config value: expected_start_year",
    ):
        config.get_required_config_value(
            runtime_config,
            "expected_start_year",
        )


def test_validate_full_rebuild_runtime_config_accepts_valid_json_booleans():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "1",
        "expected_end_year": "2024",
        "expected_end_month": "3",
    }

    assert config.validate_full_rebuild_runtime_config(runtime_config) == {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "03",
    }


def test_validate_full_rebuild_runtime_config_accepts_string_true_values():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": "true",
        "confirm_clickhouse_truncate": "true",
        "expected_start_year": 2024,
        "expected_start_month": 1,
        "expected_end_year": 2024,
        "expected_end_month": 12,
    }

    assert config.validate_full_rebuild_runtime_config(runtime_config) == {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }


def test_validate_full_rebuild_runtime_config_rejects_missing_rebuild_mode():
    runtime_config = {
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(
        ValueError,
        match="rebuild_mode must be 'full_raw_rebuild'",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_validate_full_rebuild_runtime_config_rejects_missing_full_rebuild_confirmation():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": False,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(
        ValueError,
        match="confirm_full_rebuild must be true",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_validate_full_rebuild_runtime_config_rejects_missing_truncate_confirmation():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": False,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(
        ValueError,
        match="confirm_clickhouse_truncate must be true",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_validate_full_rebuild_runtime_config_rejects_missing_expected_period():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
    }

    with pytest.raises(
        ValueError,
        match="Missing required full rebuild config value: expected_end_month",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_validate_full_rebuild_runtime_config_rejects_invalid_month():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "13",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(ValueError, match="must be between 1 and 12"):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_validate_full_rebuild_runtime_config_rejects_reversed_period_range():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2025",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(
        ValueError,
        match="start period must be earlier than or equal to end period",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_validation_errors_include_truncate_safety_message():
    runtime_config = {
        "rebuild_mode": "wrong_mode",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(ValueError, match="ClickHouse truncate was not executed"):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_missing_required_period_error_includes_truncate_safety_message():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2024",
        "expected_start_month": "01",
        "expected_end_year": "2024",
    }

    with pytest.raises(
        ValueError,
        match="ClickHouse truncate was not executed",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)


def test_invalid_period_range_error_includes_truncate_safety_message():
    runtime_config = {
        "rebuild_mode": "full_raw_rebuild",
        "confirm_full_rebuild": True,
        "confirm_clickhouse_truncate": True,
        "expected_start_year": "2025",
        "expected_start_month": "01",
        "expected_end_year": "2024",
        "expected_end_month": "12",
    }

    with pytest.raises(
        ValueError,
        match="ClickHouse truncate was not executed",
    ):
        config.validate_full_rebuild_runtime_config(runtime_config)
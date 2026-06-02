import pytest

import period_refresh_config as config


def test_get_config_value_returns_default_for_none_config():
    assert config.get_config_value(
        config=None,
        key="start_year",
        default_value="2024",
    ) == "2024"


def test_get_config_value_returns_default_for_missing_key():
    assert config.get_config_value(
        config={},
        key="start_year",
        default_value="2024",
    ) == "2024"


def test_get_config_value_returns_default_for_none_value():
    assert config.get_config_value(
        config={"start_year": None},
        key="start_year",
        default_value="2024",
    ) == "2024"


def test_get_config_value_returns_default_for_empty_string():
    assert config.get_config_value(
        config={"start_year": "   "},
        key="start_year",
        default_value="2024",
    ) == "2024"


def test_get_config_value_returns_stripped_string_value():
    assert config.get_config_value(
        config={"start_year": " 2025 "},
        key="start_year",
        default_value="2024",
    ) == "2025"


def test_get_config_value_converts_integer_to_string():
    assert config.get_config_value(
        config={"start_year": 2025},
        key="start_year",
        default_value="2024",
    ) == "2025"


def test_validate_refresh_mode_accepts_replace_period():
    assert config.validate_refresh_mode("replace_period") == "replace_period"


def test_validate_refresh_mode_strips_spaces():
    assert config.validate_refresh_mode(" replace_period ") == "replace_period"


def test_validate_refresh_mode_rejects_unsupported_mode():
    with pytest.raises(ValueError, match="Unsupported refresh_mode"):
        config.validate_refresh_mode("full_rebuild")


def test_normalize_period_param_returns_year_month_dict():
    assert config.normalize_period_param("2024", "5") == {
        "year": "2024",
        "month": "05",
    }


def test_build_period_refresh_periods_uses_safe_default_one_month():
    assert config.build_period_refresh_periods(None) == [
        {"year": "2024", "month": "01"},
    ]


def test_build_period_refresh_periods_for_one_month_config():
    runtime_config = {
        "start_year": "2024",
        "start_month": "05",
        "end_year": "2024",
        "end_month": "05",
        "refresh_mode": "replace_period",
    }

    assert config.build_period_refresh_periods(runtime_config) == [
        {"year": "2024", "month": "05"},
    ]


def test_build_period_refresh_periods_for_same_year_interval():
    runtime_config = {
        "start_year": "2024",
        "start_month": "01",
        "end_year": "2024",
        "end_month": "03",
        "refresh_mode": "replace_period",
    }

    assert config.build_period_refresh_periods(runtime_config) == [
        {"year": "2024", "month": "01"},
        {"year": "2024", "month": "02"},
        {"year": "2024", "month": "03"},
    ]


def test_build_period_refresh_periods_for_cross_year_interval():
    runtime_config = {
        "start_year": "2024",
        "start_month": "11",
        "end_year": "2025",
        "end_month": "02",
        "refresh_mode": "replace_period",
    }

    assert config.build_period_refresh_periods(runtime_config) == [
        {"year": "2024", "month": "11"},
        {"year": "2024", "month": "12"},
        {"year": "2025", "month": "01"},
        {"year": "2025", "month": "02"},
    ]


def test_build_period_refresh_periods_rejects_invalid_month():
    runtime_config = {
        "start_year": "2024",
        "start_month": "13",
        "end_year": "2024",
        "end_month": "13",
        "refresh_mode": "replace_period",
    }

    with pytest.raises(ValueError, match="must be between 1 and 12"):
        config.build_period_refresh_periods(runtime_config)


def test_build_period_refresh_periods_rejects_start_after_end():
    runtime_config = {
        "start_year": "2024",
        "start_month": "06",
        "end_year": "2024",
        "end_month": "05",
        "refresh_mode": "replace_period",
    }

    with pytest.raises(ValueError, match="start period must be earlier"):
        config.build_period_refresh_periods(runtime_config)


def test_build_period_refresh_periods_rejects_unsupported_refresh_mode():
    runtime_config = {
        "start_year": "2024",
        "start_month": "05",
        "end_year": "2024",
        "end_month": "05",
        "refresh_mode": "full_rebuild",
    }

    with pytest.raises(ValueError, match="Unsupported refresh_mode"):
        config.build_period_refresh_periods(runtime_config)


def test_build_period_refresh_periods_allows_twelve_month_interval():
    runtime_config = {
        "start_year": "2024",
        "start_month": "01",
        "end_year": "2024",
        "end_month": "12",
        "refresh_mode": "replace_period",
    }

    periods = config.build_period_refresh_periods(runtime_config)

    assert len(periods) == 12
    assert periods[0] == {"year": "2024", "month": "01"}
    assert periods[-1] == {"year": "2024", "month": "12"}


def test_build_period_refresh_periods_rejects_too_large_interval():
    runtime_config = {
        "start_year": "2024",
        "start_month": "01",
        "end_year": "2025",
        "end_month": "01",
        "refresh_mode": "replace_period",
    }

    with pytest.raises(ValueError, match="Period refresh interval is too large"):
        config.build_period_refresh_periods(runtime_config)


def test_format_period_params_formats_period_list():
    periods = [
        {"year": "2024", "month": "01"},
        {"year": "2024", "month": "02"},
    ]

    assert config.format_period_params(periods) == "2024-01, 2024-02"


def test_format_period_params_handles_empty_list():
    assert config.format_period_params([]) == "none"

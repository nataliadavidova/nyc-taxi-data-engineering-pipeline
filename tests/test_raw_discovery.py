import pytest

import raw_discovery as discovery


def test_normalize_period_accepts_string_values():
    assert discovery.normalize_period("2024", "05") == ("2024", "05")


def test_normalize_period_accepts_integer_values():
    assert discovery.normalize_period(2024, 5) == ("2024", "05")


def test_normalize_period_rejects_invalid_month():
    with pytest.raises(ValueError, match="must be between 1 and 12"):
        discovery.normalize_period("2024", "13")


def test_parse_raw_yellow_key_parses_valid_key():
    key = (
        "nyc_taxi/raw/yellow/year=2024/month=05/"
        "yellow_tripdata_2024-05.parquet"
    )

    assert discovery.parse_raw_yellow_key(key) == ("2024", "05")


def test_parse_raw_yellow_key_rejects_wrong_taxi_type():
    key = (
        "nyc_taxi/raw/green/year=2024/month=05/"
        "green_tripdata_2024-05.parquet"
    )

    assert discovery.parse_raw_yellow_key(key) is None


def test_parse_raw_yellow_key_rejects_wrong_layer():
    key = (
        "nyc_taxi/bronze/yellow/year=2024/month=05/"
        "yellow_tripdata_2024-05.parquet"
    )

    assert discovery.parse_raw_yellow_key(key) is None


def test_parse_raw_yellow_key_rejects_mismatched_filename_period():
    key = (
        "nyc_taxi/raw/yellow/year=2024/month=05/"
        "yellow_tripdata_2024-06.parquet"
    )

    assert discovery.parse_raw_yellow_key(key) is None


def test_parse_raw_yellow_key_rejects_invalid_month():
    key = (
        "nyc_taxi/raw/yellow/year=2024/month=13/"
        "yellow_tripdata_2024-13.parquet"
    )

    assert discovery.parse_raw_yellow_key(key) is None


def test_parse_raw_yellow_key_rejects_unrelated_key():
    key = "nyc_taxi/raw/lookup/taxi_zone_lookup.csv"

    assert discovery.parse_raw_yellow_key(key) is None


def test_discover_raw_yellow_periods_from_keys_filters_deduplicates_and_sorts():
    keys = [
        "nyc_taxi/raw/yellow/year=2024/month=02/yellow_tripdata_2024-02.parquet",
        "nyc_taxi/raw/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet",
        "nyc_taxi/raw/yellow/year=2024/month=02/yellow_tripdata_2024-02.parquet",
        "nyc_taxi/raw/lookup/taxi_zone_lookup.csv",
        "nyc_taxi/raw/green/year=2024/month=01/green_tripdata_2024-01.parquet",
    ]

    assert discovery.discover_raw_yellow_periods_from_keys(keys) == [
        ("2024", "01"),
        ("2024", "02"),
    ]


def test_find_new_periods_returns_raw_minus_processed():
    raw_periods = [
        ("2024", "01"),
        ("2024", "02"),
        ("2025", "01"),
    ]
    processed_periods = [
        ("2024", "01"),
        ("2024", "02"),
    ]

    assert discovery.find_new_periods(raw_periods, processed_periods) == [
        ("2025", "01")
    ]


def test_find_new_periods_sorts_result_chronologically():
    raw_periods = [
        ("2025", "02"),
        ("2024", "12"),
        ("2025", "01"),
    ]
    processed_periods = []

    assert discovery.find_new_periods(raw_periods, processed_periods) == [
        ("2024", "12"),
        ("2025", "01"),
        ("2025", "02"),
    ]


def test_find_new_periods_handles_all_periods_processed():
    raw_periods = [
        ("2024", "01"),
        ("2024", "02"),
    ]
    processed_periods = [
        ("2024", "01"),
        ("2024", "02"),
    ]

    assert discovery.find_new_periods(raw_periods, processed_periods) == []


def test_find_new_periods_handles_empty_raw_periods():
    assert discovery.find_new_periods([], [("2024", "01")]) == []


def test_get_raw_yellow_prefix():
    assert discovery.get_raw_yellow_prefix() == "nyc_taxi/raw/yellow/"


def test_build_processed_periods_query():
    query = discovery.build_processed_periods_query("gold_daily_trips")

    assert "SELECT DISTINCT" in query
    assert "year" in query
    assert "month" in query
    assert "FROM nyc_taxi.gold_daily_trips" in query
    assert "FORMAT JSON" in query


def test_list_clickhouse_table_periods(monkeypatch):
    monkeypatch.setattr(
        discovery,
        "fetch_json_data",
        lambda query: [
            {"year": "2024", "month": "02"},
            {"year": "2024", "month": "01"},
            {"year": "2024", "month": "01"},
        ],
    )

    assert discovery.list_clickhouse_table_periods("gold_daily_trips") == [
        ("2024", "01"),
        ("2024", "02"),
    ]


def test_get_fully_processed_periods_from_table_periods_returns_intersection():
    table_periods = [
        [("2024", "01"), ("2024", "02"), ("2025", "01")],
        [("2024", "01"), ("2024", "02")],
        [("2024", "01"), ("2024", "02")],
        [("2024", "01"), ("2024", "02")],
    ]

    assert discovery.get_fully_processed_periods_from_table_periods(
        table_periods
    ) == [
        ("2024", "01"),
        ("2024", "02"),
    ]


def test_get_fully_processed_periods_excludes_partially_loaded_period():
    table_periods = [
        [("2025", "01")],
        [],
        [],
        [],
    ]

    assert discovery.get_fully_processed_periods_from_table_periods(
        table_periods
    ) == []


def test_get_fully_processed_periods_handles_empty_table_list():
    assert discovery.get_fully_processed_periods_from_table_periods([]) == []


def test_list_fully_processed_clickhouse_periods_uses_all_tables(monkeypatch):
    table_periods_by_name = {
        "gold_daily_trips": [("2024", "01"), ("2024", "02"), ("2025", "01")],
        "gold_hourly_trips": [("2024", "01"), ("2024", "02")],
        "gold_payment_type_stats": [("2024", "01"), ("2024", "02")],
        "gold_location_pair_stats": [("2024", "01"), ("2024", "02")],
    }

    def fake_list_clickhouse_table_periods(table_name):
        return table_periods_by_name[table_name]

    monkeypatch.setattr(
        discovery,
        "list_clickhouse_table_periods",
        fake_list_clickhouse_table_periods,
    )

    assert discovery.list_fully_processed_clickhouse_periods() == [
        ("2024", "01"),
        ("2024", "02"),
    ]


def test_list_processed_clickhouse_periods_returns_fully_processed_periods(
    monkeypatch,
):
    monkeypatch.setattr(
        discovery,
        "list_fully_processed_clickhouse_periods",
        lambda: [("2024", "01")],
    )

    assert discovery.list_processed_clickhouse_periods() == [("2024", "01")]


def test_discover_new_raw_periods_uses_fully_processed_periods(monkeypatch):
    monkeypatch.setattr(
        discovery,
        "list_raw_yellow_periods",
        lambda: [
            ("2024", "01"),
            ("2024", "02"),
            ("2025", "01"),
        ],
    )
    monkeypatch.setattr(
        discovery,
        "list_processed_clickhouse_periods",
        lambda: [
            ("2024", "01"),
            ("2024", "02"),
        ],
    )

    assert discovery.discover_new_raw_periods() == [("2025", "01")]


def test_format_periods():
    assert discovery.format_periods([("2024", "01"), ("2024", "02")]) == (
        "2024-01, 2024-02"
    )


def test_format_periods_handles_empty_list():
    assert discovery.format_periods([]) == "none"


def test_build_expected_periods_for_same_year_range():
    assert discovery.build_expected_periods(
        start_year="2024",
        start_month="01",
        end_year="2024",
        end_month="03",
    ) == [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]


def test_build_expected_periods_for_cross_year_range():
    assert discovery.build_expected_periods(
        start_year="2023",
        start_month="11",
        end_year="2024",
        end_month="02",
    ) == [
        ("2023", "11"),
        ("2023", "12"),
        ("2024", "01"),
        ("2024", "02"),
    ]


def test_find_missing_periods_returns_expected_missing_months():
    discovered_periods = [
        ("2024", "01"),
        ("2024", "03"),
    ]
    expected_periods = [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]

    assert discovery.find_missing_periods(
        discovered_periods=discovered_periods,
        expected_periods=expected_periods,
    ) == [("2024", "02")]


def test_find_unexpected_periods_returns_periods_outside_expected_range():
    discovered_periods = [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]
    expected_periods = [
        ("2024", "01"),
        ("2024", "02"),
    ]

    assert discovery.find_unexpected_periods(
        discovered_periods=discovered_periods,
        expected_periods=expected_periods,
    ) == [("2024", "03")]


def test_validate_full_rebuild_raw_periods_returns_expected_periods_when_complete():
    raw_periods = [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]

    assert discovery.validate_full_rebuild_raw_periods(
        raw_periods=raw_periods,
        expected_start_year="2024",
        expected_start_month="01",
        expected_end_year="2024",
        expected_end_month="03",
    ) == [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]


def test_validate_full_rebuild_raw_periods_sorts_and_deduplicates_raw_periods():
    raw_periods = [
        ("2024", "03"),
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "02"),
    ]

    assert discovery.validate_full_rebuild_raw_periods(
        raw_periods=raw_periods,
        expected_start_year="2024",
        expected_start_month="01",
        expected_end_year="2024",
        expected_end_month="03",
    ) == [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]


def test_validate_full_rebuild_raw_periods_rejects_empty_raw_source():
    with pytest.raises(ValueError, match="no raw Yellow Taxi periods"):
        discovery.validate_full_rebuild_raw_periods(
            raw_periods=[],
            expected_start_year="2024",
            expected_start_month="01",
            expected_end_year="2024",
            expected_end_month="03",
        )


def test_validate_full_rebuild_raw_periods_rejects_missing_periods():
    raw_periods = [
        ("2024", "01"),
        ("2024", "03"),
    ]

    with pytest.raises(ValueError, match="Missing periods: 2024-02"):
        discovery.validate_full_rebuild_raw_periods(
            raw_periods=raw_periods,
            expected_start_year="2024",
            expected_start_month="01",
            expected_end_year="2024",
            expected_end_month="03",
        )


def test_validate_full_rebuild_raw_periods_rejects_unexpected_periods():
    raw_periods = [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]

    with pytest.raises(ValueError, match="Unexpected periods: 2024-03"):
        discovery.validate_full_rebuild_raw_periods(
            raw_periods=raw_periods,
            expected_start_year="2024",
            expected_start_month="01",
            expected_end_year="2024",
            expected_end_month="02",
        )


def test_validate_full_rebuild_raw_periods_rejects_incomplete_source_before_truncate():
    raw_periods = [
        ("2024", "01"),
        ("2024", "02"),
        ("2024", "03"),
    ]

    with pytest.raises(ValueError, match="ClickHouse truncate was not executed"):
        discovery.validate_full_rebuild_raw_periods(
            raw_periods=raw_periods,
            expected_start_year="2023",
            expected_start_month="12",
            expected_end_year="2024",
            expected_end_month="03",
        )
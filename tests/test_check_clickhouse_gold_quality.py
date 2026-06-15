
import pytest

import check_clickhouse_gold_quality as ch_quality
from check_clickhouse_gold_quality import (
    EXPECTED_MAX_DATE,
    EXPECTED_MIN_DATE,
    GOLD_CLICKHOUSE_TABLES,
    build_quality_query,
    empty_string_count_expression,
    validate_common_table_metrics,
    validate_location_metrics,
    validate_payment_metrics,
    validate_trip_type_metrics,
)


def test_empty_string_count_expression_builds_clickhouse_count_if():
    expression = empty_string_count_expression(
        column_name="trip_type",
        alias="empty_trip_type_count",
    )

    assert expression == (
        "countIf(trip_type IS NULL OR trim(trip_type) = '') "
        "AS empty_trip_type_count"
    )


def test_build_quality_query_for_daily_table_contains_common_checks_only():
    query = build_quality_query("gold_daily_trips")

    assert "count() AS rows_count" in query
    assert "toString(min(pickup_date)) AS min_pickup_date" in query
    assert "toString(max(pickup_date)) AS max_pickup_date" in query
    assert "FROM" in query
    assert "gold_daily_trips" in query
    assert "FORMAT JSON" in query

    assert "empty_trip_type_count" not in query
    assert "empty_pickup_zone_count" not in query
    assert "empty_dropoff_zone_count" not in query
    assert "empty_payment_type_name_count" not in query


def test_build_quality_query_for_hourly_table_contains_trip_type_check():
    query = build_quality_query("gold_hourly_trips")

    assert "gold_hourly_trips" in query
    assert "empty_trip_type_count" in query
    assert "trip_type IS NULL OR trim(trip_type) = ''" in query
    assert "FORMAT JSON" in query

    assert "empty_pickup_zone_count" not in query
    assert "empty_dropoff_zone_count" not in query
    assert "empty_payment_type_name_count" not in query


def test_build_quality_query_for_location_table_contains_location_checks():
    query = build_quality_query("gold_location_pair_stats")

    assert "gold_location_pair_stats" in query
    assert "empty_trip_type_count" in query
    assert "empty_pickup_zone_count" in query
    assert "empty_dropoff_zone_count" in query
    assert "pickup_zone IS NULL OR trim(pickup_zone) = ''" in query
    assert "dropoff_zone IS NULL OR trim(dropoff_zone) = ''" in query
    assert "FORMAT JSON" in query

    assert "empty_payment_type_name_count" not in query


def test_build_quality_query_for_payment_table_contains_payment_checks():
    query = build_quality_query("gold_payment_type_stats")

    assert "gold_payment_type_stats" in query
    assert "empty_trip_type_count" in query
    assert "empty_payment_type_name_count" in query
    assert "payment_type_name IS NULL OR trim(payment_type_name) = ''" in query
    assert "FORMAT JSON" in query

    assert "empty_pickup_zone_count" not in query
    assert "empty_dropoff_zone_count" not in query


def test_assert_gold_tables_exist_passes_when_all_expected_tables_exist(monkeypatch):
    monkeypatch.setattr(
        ch_quality,
        "fetch_json_data",
        lambda query: [{"name": table_name} for table_name in GOLD_CLICKHOUSE_TABLES],
    )

    ch_quality.assert_gold_tables_exist()


def test_assert_gold_tables_exist_fails_when_table_is_missing(monkeypatch):
    existing_tables = [
        table_name
        for table_name in GOLD_CLICKHOUSE_TABLES
        if table_name != "gold_daily_trips"
    ]

    monkeypatch.setattr(
        ch_quality,
        "fetch_json_data",
        lambda query: [{"name": table_name} for table_name in existing_tables],
    )

    with pytest.raises(AssertionError, match="Missing ClickHouse gold tables"):
        ch_quality.assert_gold_tables_exist()


def test_validate_common_table_metrics_passes_for_valid_metrics():
    metrics = {
        "rows_count": 100,
        "min_pickup_date": EXPECTED_MIN_DATE,
        "max_pickup_date": EXPECTED_MAX_DATE,
    }

    validate_common_table_metrics("gold_daily_trips", metrics)


def test_validate_common_table_metrics_fails_for_empty_table():
    metrics = {
        "rows_count": 0,
        "min_pickup_date": EXPECTED_MIN_DATE,
        "max_pickup_date": EXPECTED_MAX_DATE,
    }

    with pytest.raises(AssertionError, match="to be non-empty"):
        validate_common_table_metrics("gold_daily_trips", metrics)


def test_validate_common_table_metrics_fails_for_wrong_date_range():
    metrics = {
        "rows_count": 100,
        "min_pickup_date": "2024-01-01",
        "max_pickup_date": "2024-01-31",
    }

    with pytest.raises(AssertionError, match="Unexpected pickup_date range"):
        validate_common_table_metrics("gold_daily_trips", metrics)


def test_validate_trip_type_metrics_skips_daily_table():
    metrics = {}

    validate_trip_type_metrics("gold_daily_trips", metrics)


def test_validate_trip_type_metrics_passes_when_trip_type_is_populated():
    metrics = {
        "empty_trip_type_count": 0,
    }

    validate_trip_type_metrics("gold_hourly_trips", metrics)


def test_validate_trip_type_metrics_fails_for_empty_trip_type():
    metrics = {
        "empty_trip_type_count": 1,
    }

    with pytest.raises(AssertionError, match="empty trip_type"):
        validate_trip_type_metrics("gold_hourly_trips", metrics)


def test_validate_location_metrics_skips_non_location_table():
    metrics = {}

    validate_location_metrics("gold_daily_trips", metrics)


def test_validate_location_metrics_passes_when_zones_are_populated():
    metrics = {
        "empty_pickup_zone_count": 0,
        "empty_dropoff_zone_count": 0,
    }

    validate_location_metrics("gold_location_pair_stats", metrics)


def test_validate_location_metrics_fails_for_empty_pickup_zone():
    metrics = {
        "empty_pickup_zone_count": 1,
        "empty_dropoff_zone_count": 0,
    }

    with pytest.raises(AssertionError, match="Found empty zones"):
        validate_location_metrics("gold_location_pair_stats", metrics)


def test_validate_location_metrics_fails_for_empty_dropoff_zone():
    metrics = {
        "empty_pickup_zone_count": 0,
        "empty_dropoff_zone_count": 1,
    }

    with pytest.raises(AssertionError, match="Found empty zones"):
        validate_location_metrics("gold_location_pair_stats", metrics)


def test_validate_payment_metrics_skips_non_payment_table():
    metrics = {}

    validate_payment_metrics("gold_daily_trips", metrics)


def test_validate_payment_metrics_passes_when_payment_names_are_populated():
    metrics = {
        "empty_payment_type_name_count": 0,
    }

    validate_payment_metrics("gold_payment_type_stats", metrics)


def test_validate_payment_metrics_fails_for_empty_payment_type_name():
    metrics = {
        "empty_payment_type_name_count": 1,
    }

    with pytest.raises(AssertionError, match="empty payment_type_name"):
        validate_payment_metrics("gold_payment_type_stats", metrics)

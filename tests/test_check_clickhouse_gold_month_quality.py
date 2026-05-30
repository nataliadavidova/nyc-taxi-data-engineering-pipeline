
import pytest

import check_clickhouse_gold_month_quality as job


def base_metrics(**overrides):
    metrics = {
        "rows_count": 31,
        "min_pickup_date": "2024-05-01",
        "max_pickup_date": "2024-05-31",
        "non_positive_trips_count": 0,
        "out_of_month_date_count": 0,
    }
    metrics.update(overrides)
    return metrics


def test_normalize_year_month_accepts_zero_padded_month():
    assert job.normalize_year_month("2024", "05") == ("2024", "05")


def test_normalize_year_month_accepts_int_values():
    assert job.normalize_year_month(2024, 5) == ("2024", "05")


def test_normalize_year_month_rejects_invalid_month():
    with pytest.raises(ValueError, match="must be between 1 and 12"):
        job.normalize_year_month("2024", "13")


def test_build_tables_exist_query_contains_expected_tables():
    query = job.build_tables_exist_query()

    assert "FROM system.tables" in query
    assert "database = 'nyc_taxi'" in query

    for table_name in job.GOLD_CLICKHOUSE_TABLES:
        assert f"'{table_name}'" in query


def test_empty_string_count_expression():
    assert job.empty_string_count_expression(
        column_name="trip_type",
        alias="empty_trip_type_count",
    ) == (
        "countIf(trip_type IS NULL OR trim(trip_type) = '') "
        "AS empty_trip_type_count"
    )


def test_build_quality_query_for_daily_table():
    query = job.build_quality_query(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
        month_start="2024-05-01",
        next_month_start="2024-06-01",
    )

    assert "FROM nyc_taxi.gold_daily_trips" in query
    assert "WHERE year = '2024'" in query
    assert "AND month = '05'" in query
    assert "count() AS rows_count" in query
    assert "countIf(trips_count <= 0) AS non_positive_trips_count" in query
    assert "out_of_month_date_count" in query
    assert "empty_trip_type_count" not in query


def test_build_quality_query_for_hourly_table():
    query = job.build_quality_query(
        table_name="gold_hourly_trips",
        year="2024",
        month="05",
        month_start="2024-05-01",
        next_month_start="2024-06-01",
    )

    assert "FROM nyc_taxi.gold_hourly_trips" in query
    assert "empty_trip_type_count" in query
    assert "invalid_pickup_hour_count" in query


def test_build_quality_query_for_location_table():
    query = job.build_quality_query(
        table_name="gold_location_pair_stats",
        year="2024",
        month="05",
        month_start="2024-05-01",
        next_month_start="2024-06-01",
    )

    assert "empty_trip_type_count" in query
    assert "empty_pickup_zone_count" in query
    assert "empty_dropoff_zone_count" in query


def test_build_quality_query_for_payment_table():
    query = job.build_quality_query(
        table_name="gold_payment_type_stats",
        year="2024",
        month="05",
        month_start="2024-05-01",
        next_month_start="2024-06-01",
    )

    assert "empty_trip_type_count" in query
    assert "empty_payment_type_name_count" in query


def test_assert_gold_tables_exist_success(monkeypatch):
    monkeypatch.setattr(
        job,
        "fetch_json_data",
        lambda query: [
            {"name": "gold_daily_trips"},
            {"name": "gold_hourly_trips"},
            {"name": "gold_location_pair_stats"},
            {"name": "gold_payment_type_stats"},
        ],
    )

    job.assert_gold_tables_exist()


def test_assert_gold_tables_exist_raises_for_missing_table(monkeypatch):
    monkeypatch.setattr(
        job,
        "fetch_json_data",
        lambda query: [
            {"name": "gold_daily_trips"},
            {"name": "gold_hourly_trips"},
            {"name": "gold_location_pair_stats"},
        ],
    )

    with pytest.raises(AssertionError, match="Missing ClickHouse gold tables"):
        job.assert_gold_tables_exist()


def test_validate_common_table_metrics_success():
    job.validate_common_table_metrics(
        table_name="gold_daily_trips",
        metrics=base_metrics(),
        month_start="2024-05-01",
        next_month_start="2024-06-01",
    )


def test_validate_common_table_metrics_rejects_empty_month():
    with pytest.raises(AssertionError, match="to have rows"):
        job.validate_common_table_metrics(
            table_name="gold_daily_trips",
            metrics=base_metrics(rows_count=0),
            month_start="2024-05-01",
            next_month_start="2024-06-01",
        )


def test_validate_common_table_metrics_rejects_min_date_before_month():
    with pytest.raises(AssertionError, match="Unexpected pickup_date range"):
        job.validate_common_table_metrics(
            table_name="gold_daily_trips",
            metrics=base_metrics(min_pickup_date="2024-04-30"),
            month_start="2024-05-01",
            next_month_start="2024-06-01",
        )


def test_validate_common_table_metrics_rejects_max_date_after_month():
    with pytest.raises(AssertionError, match="Unexpected pickup_date range"):
        job.validate_common_table_metrics(
            table_name="gold_daily_trips",
            metrics=base_metrics(max_pickup_date="2024-06-01"),
            month_start="2024-05-01",
            next_month_start="2024-06-01",
        )


def test_validate_common_table_metrics_rejects_out_of_month_count():
    with pytest.raises(AssertionError, match="out-of-month pickup_date"):
        job.validate_common_table_metrics(
            table_name="gold_daily_trips",
            metrics=base_metrics(out_of_month_date_count=1),
            month_start="2024-05-01",
            next_month_start="2024-06-01",
        )


def test_validate_common_table_metrics_rejects_non_positive_trips_count():
    with pytest.raises(AssertionError, match="non-positive trips_count"):
        job.validate_common_table_metrics(
            table_name="gold_daily_trips",
            metrics=base_metrics(non_positive_trips_count=1),
            month_start="2024-05-01",
            next_month_start="2024-06-01",
        )


def test_validate_trip_type_metrics_skips_daily_table():
    job.validate_trip_type_metrics(
        table_name="gold_daily_trips",
        metrics={},
    )


def test_validate_trip_type_metrics_success():
    job.validate_trip_type_metrics(
        table_name="gold_hourly_trips",
        metrics={"empty_trip_type_count": 0},
    )


def test_validate_trip_type_metrics_rejects_empty_values():
    with pytest.raises(AssertionError, match="empty trip_type"):
        job.validate_trip_type_metrics(
            table_name="gold_hourly_trips",
            metrics={"empty_trip_type_count": 1},
        )


def test_validate_hourly_metrics_skips_non_hourly_table():
    job.validate_hourly_metrics(
        table_name="gold_daily_trips",
        metrics={},
    )


def test_validate_hourly_metrics_success():
    job.validate_hourly_metrics(
        table_name="gold_hourly_trips",
        metrics={"invalid_pickup_hour_count": 0},
    )


def test_validate_hourly_metrics_rejects_invalid_hours():
    with pytest.raises(AssertionError, match="invalid pickup_hour"):
        job.validate_hourly_metrics(
            table_name="gold_hourly_trips",
            metrics={"invalid_pickup_hour_count": 1},
        )


def test_validate_location_metrics_skips_non_location_table():
    job.validate_location_metrics(
        table_name="gold_daily_trips",
        metrics={},
    )


def test_validate_location_metrics_success():
    job.validate_location_metrics(
        table_name="gold_location_pair_stats",
        metrics={
            "empty_pickup_zone_count": 0,
            "empty_dropoff_zone_count": 0,
        },
    )


def test_validate_location_metrics_rejects_empty_zones():
    with pytest.raises(AssertionError, match="Found empty zones"):
        job.validate_location_metrics(
            table_name="gold_location_pair_stats",
            metrics={
                "empty_pickup_zone_count": 1,
                "empty_dropoff_zone_count": 0,
            },
        )


def test_validate_payment_metrics_skips_non_payment_table():
    job.validate_payment_metrics(
        table_name="gold_daily_trips",
        metrics={},
    )


def test_validate_payment_metrics_success():
    job.validate_payment_metrics(
        table_name="gold_payment_type_stats",
        metrics={"empty_payment_type_name_count": 0},
    )


def test_validate_payment_metrics_rejects_empty_payment_type_names():
    with pytest.raises(AssertionError, match="empty payment_type_name"):
        job.validate_payment_metrics(
            table_name="gold_payment_type_stats",
            metrics={"empty_payment_type_name_count": 1},
        )


def test_check_gold_table_month_quality_runs_all_checks(monkeypatch):
    called_queries = []

    def fake_fetch_single_json_row(query):
        called_queries.append(query)
        return {
            "rows_count": 31,
            "min_pickup_date": "2024-05-01",
            "max_pickup_date": "2024-05-31",
            "non_positive_trips_count": 0,
            "out_of_month_date_count": 0,
        }

    monkeypatch.setattr(
        job,
        "fetch_single_json_row",
        fake_fetch_single_json_row,
    )

    job.check_gold_table_month_quality(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
    )

    assert len(called_queries) == 1
    assert "FROM nyc_taxi.gold_daily_trips" in called_queries[0]
    assert "WHERE year = '2024'" in called_queries[0]
    assert "AND month = '05'" in called_queries[0]


def test_check_clickhouse_gold_month_quality_runs_for_all_tables(monkeypatch):
    checked_tables = []

    monkeypatch.setattr(job, "validate_config", lambda: None)
    monkeypatch.setattr(job, "assert_gold_tables_exist", lambda: None)

    def fake_check_gold_table_month_quality(table_name, year, month):
        checked_tables.append((table_name, year, month))

    monkeypatch.setattr(
        job,
        "check_gold_table_month_quality",
        fake_check_gold_table_month_quality,
    )

    job.check_clickhouse_gold_month_quality(year=2024, month=5)

    assert checked_tables == [
        ("gold_daily_trips", "2024", "05"),
        ("gold_hourly_trips", "2024", "05"),
        ("gold_location_pair_stats", "2024", "05"),
        ("gold_payment_type_stats", "2024", "05"),
    ]

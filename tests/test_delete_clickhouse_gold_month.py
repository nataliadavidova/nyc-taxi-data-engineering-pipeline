import pytest

import delete_clickhouse_gold_month as job


def test_normalize_year_month_accepts_zero_padded_month():
    assert job.normalize_year_month("2024", "05") == ("2024", "05")


def test_normalize_year_month_accepts_int_values():
    assert job.normalize_year_month(2024, 5) == ("2024", "05")


def test_normalize_year_month_rejects_invalid_month():
    with pytest.raises(ValueError, match="must be between 1 and 12"):
        job.normalize_year_month("2024", "13")


def test_build_delete_query():
    assert job.build_delete_query(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
    ) == (
        "ALTER TABLE nyc_taxi.gold_daily_trips "
        "DELETE WHERE year = '2024' AND month = '05'"
    )


def test_build_count_query():
    assert job.build_count_query(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
    ) == (
        "SELECT count()\n"
        "FROM nyc_taxi.gold_daily_trips\n"
        "WHERE year = '2024' AND month = '05'"
    )


@pytest.mark.parametrize(
    "response_text,expected_count",
    [
        ("0", 0),
        ("123", 123),
        ("123\n", 123),
        ("\n123\n", 123),
    ],
)
def test_parse_count_response(response_text, expected_count):
    assert job.parse_count_response(response_text) == expected_count


def test_parse_count_response_rejects_empty_response():
    with pytest.raises(ValueError, match="count response is empty"):
        job.parse_count_response("")


def test_parse_count_response_rejects_non_numeric_response():
    with pytest.raises(ValueError, match="Cannot parse ClickHouse count response"):
        job.parse_count_response("not_a_number")


def test_get_month_row_count_uses_count_query(monkeypatch):
    executed_queries = []

    def fake_execute_clickhouse_query(query):
        executed_queries.append(query)
        return "42"

    monkeypatch.setattr(
        job,
        "execute_clickhouse_query",
        fake_execute_clickhouse_query,
    )

    assert job.get_month_row_count(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
    ) == 42

    assert executed_queries == [
        (
            "SELECT count()\n"
            "FROM nyc_taxi.gold_daily_trips\n"
            "WHERE year = '2024' AND month = '05'"
        )
    ]


def test_wait_until_month_deleted_returns_when_count_is_zero(monkeypatch):
    calls = []

    def fake_get_month_row_count(table_name, year, month):
        calls.append((table_name, year, month))
        return 0

    monkeypatch.setattr(
        job,
        "get_month_row_count",
        fake_get_month_row_count,
    )

    job.wait_until_month_deleted(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
        max_attempts=3,
        sleep_seconds=0,
    )

    assert calls == [("gold_daily_trips", "2024", "05")]


def test_wait_until_month_deleted_retries_until_count_is_zero(monkeypatch):
    responses = [10, 5, 0]

    def fake_get_month_row_count(table_name, year, month):
        return responses.pop(0)

    monkeypatch.setattr(
        job,
        "get_month_row_count",
        fake_get_month_row_count,
    )
    monkeypatch.setattr(job.time, "sleep", lambda seconds: None)

    job.wait_until_month_deleted(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
        max_attempts=3,
        sleep_seconds=0,
    )

    assert responses == []


def test_wait_until_month_deleted_raises_if_rows_still_exist(monkeypatch):
    def fake_get_month_row_count(table_name, year, month):
        return 10

    monkeypatch.setattr(
        job,
        "get_month_row_count",
        fake_get_month_row_count,
    )
    monkeypatch.setattr(job.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="was not deleted"):
        job.wait_until_month_deleted(
            table_name="gold_daily_trips",
            year="2024",
            month="05",
            max_attempts=2,
            sleep_seconds=0,
        )


def test_delete_gold_month_from_table_executes_delete_and_waits(monkeypatch):
    executed_queries = []
    waited_months = []

    def fake_execute_clickhouse_query(query):
        executed_queries.append(query)
        return ""

    def fake_wait_until_month_deleted(
        table_name,
        year,
        month,
        max_attempts,
        sleep_seconds,
    ):
        waited_months.append(
            (table_name, year, month, max_attempts, sleep_seconds)
        )

    monkeypatch.setattr(
        job,
        "execute_clickhouse_query",
        fake_execute_clickhouse_query,
    )
    monkeypatch.setattr(
        job,
        "wait_until_month_deleted",
        fake_wait_until_month_deleted,
    )

    job.delete_gold_month_from_table(
        table_name="gold_daily_trips",
        year="2024",
        month="05",
        max_attempts=7,
        sleep_seconds=1,
    )

    assert executed_queries == [
        (
            "ALTER TABLE nyc_taxi.gold_daily_trips "
            "DELETE WHERE year = '2024' AND month = '05'"
        )
    ]
    assert waited_months == [
        ("gold_daily_trips", "2024", "05", 7, 1)
    ]


def test_delete_gold_month_processes_given_tables(monkeypatch):
    deleted_tables = []

    def fake_delete_gold_month_from_table(
        table_name,
        year,
        month,
        max_attempts,
        sleep_seconds,
    ):
        deleted_tables.append(
            (table_name, year, month, max_attempts, sleep_seconds)
        )

    monkeypatch.setattr(
        job,
        "delete_gold_month_from_table",
        fake_delete_gold_month_from_table,
    )

    job.delete_gold_month(
        year=2024,
        month=5,
        table_names=["gold_daily_trips", "gold_hourly_trips"],
        max_attempts=2,
        sleep_seconds=0,
    )

    assert deleted_tables == [
        ("gold_daily_trips", "2024", "05", 2, 0),
        ("gold_hourly_trips", "2024", "05", 2, 0),
    ]

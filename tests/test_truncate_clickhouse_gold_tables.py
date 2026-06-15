import truncate_clickhouse_gold_tables as truncate_job


def test_truncate_gold_table_executes_expected_query(monkeypatch):
    executed_queries = []

    monkeypatch.setattr(
        truncate_job,
        "execute_clickhouse_query",
        executed_queries.append,
    )

    truncate_job.truncate_gold_table("gold_daily_trips")

    assert executed_queries == [
        (
            f"TRUNCATE TABLE IF EXISTS "
            f"{truncate_job.CLICKHOUSE_DATABASE}.gold_daily_trips"
        )
    ]


def test_main_validates_config_and_truncates_all_configured_tables(
    monkeypatch,
):
    events = []

    monkeypatch.setattr(
        truncate_job,
        "validate_config",
        lambda: events.append("validate_config"),
    )
    monkeypatch.setattr(
        truncate_job,
        "truncate_gold_table",
        lambda table_name: events.append(table_name),
    )

    truncate_job.main()

    assert events == [
        "validate_config",
        *truncate_job.GOLD_CLICKHOUSE_TABLES,
    ]
from types import SimpleNamespace

from airflow_callbacks import (
    airflow_failure_callback,
    build_failure_alert_message,
    get_context_value,
    get_object_attribute,
)


def test_get_context_value_returns_string_value():
    context = {
        "dag_id": "nyc_taxi_pipeline",
    }

    assert get_context_value(context, "dag_id") == "nyc_taxi_pipeline"


def test_get_context_value_returns_default_for_missing_value():
    context = {}

    assert get_context_value(context, "missing_key") == "unknown"


def test_get_context_value_returns_default_for_none_value():
    context = {
        "dag_id": None,
    }

    assert get_context_value(context, "dag_id") == "unknown"


def test_get_object_attribute_returns_string_value():
    obj = SimpleNamespace(task_id="silver_yellow_taxi_2024_01")

    assert get_object_attribute(obj, "task_id") == "silver_yellow_taxi_2024_01"


def test_get_object_attribute_returns_default_for_missing_attribute():
    obj = SimpleNamespace()

    assert get_object_attribute(obj, "task_id") == "unknown"


def test_get_object_attribute_returns_default_for_none_object():
    assert get_object_attribute(None, "task_id") == "unknown"


def test_build_failure_alert_message_from_airflow_context():
    context = {
        "dag": SimpleNamespace(dag_id="nyc_taxi_pipeline"),
        "task_instance": SimpleNamespace(
            task_id="silver_yellow_taxi_2024_01",
            run_id="manual__2026-06-03T10:00:00+00:00",
            try_number=2,
            log_url="http://localhost:8080/log",
        ),
        "logical_date": "2026-06-03T10:00:00+00:00",
        "exception": ValueError("Silver quality check failed"),
    }

    message = build_failure_alert_message(context)

    assert "NYC Taxi Airflow task failed" in message
    assert "DAG: nyc_taxi_pipeline" in message
    assert "Task: silver_yellow_taxi_2024_01" in message
    assert "Run ID: manual__2026-06-03T10:00:00+00:00" in message
    assert "Try number: 2" in message
    assert "Logical date: 2026-06-03T10:00:00+00:00" in message
    assert "Log URL: http://localhost:8080/log" in message
    assert "Exception: Silver quality check failed" in message


def test_build_failure_alert_message_uses_task_when_task_instance_is_missing():
    context = {
        "dag": SimpleNamespace(dag_id="nyc_taxi_period_refresh_pipeline"),
        "task": SimpleNamespace(task_id="process_month.silver_yellow_taxi"),
        "dag_run": SimpleNamespace(run_id="manual__test_run"),
        "exception": RuntimeError("Spark job failed"),
    }

    message = build_failure_alert_message(context)

    assert "DAG: nyc_taxi_period_refresh_pipeline" in message
    assert "Task: process_month.silver_yellow_taxi" in message
    assert "Run ID: manual__test_run" in message
    assert "Exception: Spark job failed" in message


def test_airflow_failure_callback_prints_alert_message(capsys):
    context = {
        "dag": SimpleNamespace(dag_id="nyc_taxi_pipeline"),
        "task_instance": SimpleNamespace(
            task_id="bronze_yellow_taxi_2024_01",
            run_id="manual__test_run",
            try_number=1,
            log_url="http://localhost:8080/log",
        ),
        "exception": RuntimeError("Bronze job failed"),
    }

    airflow_failure_callback(context)

    captured = capsys.readouterr()

    assert "NYC Taxi Airflow task failed" in captured.out
    assert "DAG: nyc_taxi_pipeline" in captured.out
    assert "Task: bronze_yellow_taxi_2024_01" in captured.out
    assert "Exception: Bronze job failed" in captured.out
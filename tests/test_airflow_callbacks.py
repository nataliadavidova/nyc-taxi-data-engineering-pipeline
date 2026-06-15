from types import SimpleNamespace

import airflow_callbacks

from airflow_callbacks import (
    airflow_failure_callback,
    build_failure_alert_message,
    build_telegram_api_url,
    get_context_value,
    get_object_attribute,
    is_telegram_alerting_configured,
    send_telegram_message,
)


def test_get_context_value_returns_string_value():
    context = {
        "dag_id": "nyc_taxi_full_rebuild_pipeline",
    }

    assert get_context_value(context, "dag_id") == "nyc_taxi_full_rebuild_pipeline"


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
        "dag": SimpleNamespace(dag_id="nyc_taxi_full_rebuild_pipeline"),
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
    assert "DAG: nyc_taxi_full_rebuild_pipeline" in message
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
        "dag": SimpleNamespace(dag_id="nyc_taxi_full_rebuild_pipeline"),
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
    assert "DAG: nyc_taxi_full_rebuild_pipeline" in captured.out
    assert "Task: bronze_yellow_taxi_2024_01" in captured.out
    assert "Exception: Bronze job failed" in captured.out


def test_is_telegram_alerting_configured_returns_false_when_disabled(
    monkeypatch,
):
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_ALERTS_ENABLED", False)
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_BOT_TOKEN", None)
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_CHAT_ID", None)

    assert is_telegram_alerting_configured() is False


def test_is_telegram_alerting_configured_returns_true_when_enabled(
    monkeypatch,
):
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_ALERTS_ENABLED", True)
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_CHAT_ID", "test-chat-id")

    assert is_telegram_alerting_configured() is True


def test_build_telegram_api_url():
    url = build_telegram_api_url("123456:test-token")

    assert url == "https://api.telegram.org/bot123456%3Atest-token/sendMessage"


def test_send_telegram_message_posts_expected_payload(monkeypatch):
    captured_request = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return None

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(request, timeout):
        captured_request["url"] = request.full_url
        captured_request["data"] = request.data
        captured_request["timeout"] = timeout

        return FakeResponse()

    monkeypatch.setattr(
        airflow_callbacks.urllib.request,
        "urlopen",
        fake_urlopen,
    )

    send_telegram_message(
        message="NYC Taxi Airflow task failed",
        bot_token="123456:test-token",
        chat_id="123456789",
        timeout_seconds=10,
    )

    assert captured_request["url"] == (
        "https://api.telegram.org/bot123456%3Atest-token/sendMessage"
    )
    assert captured_request["timeout"] == 10
    assert b'"chat_id": "123456789"' in captured_request["data"]
    assert b'"text": "NYC Taxi Airflow task failed"' in captured_request["data"]
    assert b'"disable_web_page_preview": true' in captured_request["data"]


def test_airflow_failure_callback_sends_telegram_when_enabled(
    monkeypatch,
    capsys,
):
    sent_messages = []

    def fake_send_telegram_message(
        message,
        bot_token,
        chat_id,
        timeout_seconds,
    ):
        sent_messages.append(
            {
                "message": message,
                "bot_token": bot_token,
                "chat_id": chat_id,
                "timeout_seconds": timeout_seconds,
            }
        )

    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_ALERTS_ENABLED", True)
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_CHAT_ID", "test-chat-id")
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_API_TIMEOUT_SECONDS", 10)
    monkeypatch.setattr(
        airflow_callbacks,
        "send_telegram_message",
        fake_send_telegram_message,
    )

    context = {
        "dag": SimpleNamespace(dag_id="nyc_taxi_full_rebuild_pipeline"),
        "task_instance": SimpleNamespace(
            task_id="silver_yellow_taxi_2024_01",
            run_id="manual__test_run",
            try_number=2,
            log_url="http://localhost:8080/log",
        ),
        "exception": RuntimeError("Silver job failed"),
    }

    airflow_failure_callback(context)

    captured = capsys.readouterr()

    assert "NYC Taxi Airflow task failed" in captured.out
    assert "Telegram alert sent successfully." in captured.out

    assert len(sent_messages) == 1
    assert sent_messages[0]["bot_token"] == "test-token"
    assert sent_messages[0]["chat_id"] == "test-chat-id"
    assert sent_messages[0]["timeout_seconds"] == 10
    assert "Task: silver_yellow_taxi_2024_01" in sent_messages[0]["message"]
    assert "Exception: Silver job failed" in sent_messages[0]["message"]


def test_airflow_failure_callback_logs_telegram_error(
    monkeypatch,
    capsys,
):
    def fake_send_telegram_message(
        message,
        bot_token,
        chat_id,
        timeout_seconds,
    ):
        raise RuntimeError("Telegram API unavailable")

    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_ALERTS_ENABLED", True)
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(airflow_callbacks, "TELEGRAM_CHAT_ID", "test-chat-id")
    monkeypatch.setattr(
        airflow_callbacks,
        "send_telegram_message",
        fake_send_telegram_message,
    )

    context = {
        "dag": SimpleNamespace(dag_id="nyc_taxi_full_rebuild_pipeline"),
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
    assert "Failed to send Telegram alert: Telegram API unavailable" in captured.out
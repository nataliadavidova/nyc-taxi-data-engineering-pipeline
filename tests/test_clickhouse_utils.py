import io
import json
import urllib.error

import pytest

import clickhouse_utils as utils


def test_get_clickhouse_url_uses_config_defaults():
    assert utils.get_clickhouse_url() == "http://clickhouse:8123/"


def test_execute_clickhouse_query_sends_post_request(monkeypatch):
    captured_request = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def read(self):
            return b"ok\n"

    def fake_validate_config():
        captured_request["validated"] = True

    def fake_urlopen(request, timeout):
        captured_request["url"] = request.full_url
        captured_request["method"] = request.get_method()
        captured_request["data"] = request.data
        captured_request["timeout"] = timeout
        captured_request["auth"] = request.headers.get("Authorization")
        return FakeResponse()

    monkeypatch.setattr(utils, "validate_config", fake_validate_config)
    monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)

    response = utils.execute_clickhouse_query("SELECT 1")

    assert response == "ok"
    assert captured_request["validated"] is True
    assert captured_request["url"] == "http://clickhouse:8123/"
    assert captured_request["method"] == "POST"
    assert captured_request["data"] == b"SELECT 1"
    assert captured_request["timeout"] == 120
    assert captured_request["auth"] is not None


def test_execute_clickhouse_query_can_disable_response_printing(monkeypatch, capsys):
    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def read(self):
            return b"hidden response\n"

    monkeypatch.setattr(utils, "validate_config", lambda: None)
    monkeypatch.setattr(
        utils.urllib.request,
        "urlopen",
        lambda request, timeout: FakeResponse(),
    )

    response = utils.execute_clickhouse_query(
        "SELECT 1",
        print_response=False,
    )

    captured = capsys.readouterr()

    assert response == "hidden response"
    assert captured.out == ""


def test_execute_clickhouse_query_handles_empty_password(monkeypatch):
    captured_request = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def read(self):
            return b""

    def fake_urlopen(request, timeout):
        captured_request["auth"] = request.headers.get("Authorization")
        return FakeResponse()

    monkeypatch.setattr(utils, "validate_config", lambda: None)
    monkeypatch.setattr(utils, "CLICKHOUSE_PASSWORD", None)
    monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)

    assert utils.execute_clickhouse_query("SELECT 1") == ""
    assert captured_request["auth"] is not None


def test_execute_clickhouse_query_raises_runtime_error_on_http_error(monkeypatch):
    def fake_urlopen(request, timeout):
        raise urllib.error.HTTPError(
            url=request.full_url,
            code=500,
            msg="Internal Server Error",
            hdrs=None,
            fp=io.BytesIO(b"broken query"),
        )

    monkeypatch.setattr(utils, "validate_config", lambda: None)
    monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="ClickHouse query failed"):
        utils.execute_clickhouse_query("SELECT broken")


def test_execute_clickhouse_query_raises_runtime_error_on_url_error(monkeypatch):
    def fake_urlopen(request, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(utils, "validate_config", lambda: None)
    monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(RuntimeError, match="Cannot connect to ClickHouse"):
        utils.execute_clickhouse_query("SELECT 1")


def test_fetch_json_data(monkeypatch):
    def fake_execute_clickhouse_query(query, print_response=True):
        assert print_response is False
        return json.dumps(
            {
                "meta": [{"name": "name", "type": "String"}],
                "data": [{"name": "gold_daily_trips"}],
                "rows": 1,
            }
        )

    monkeypatch.setattr(
        utils,
        "execute_clickhouse_query",
        fake_execute_clickhouse_query,
    )

    assert utils.fetch_json_data("SELECT 1 FORMAT JSON") == [
        {"name": "gold_daily_trips"}
    ]


def test_fetch_json_data_rejects_empty_response(monkeypatch):
    monkeypatch.setattr(
        utils,
        "execute_clickhouse_query",
        lambda query, print_response=True: "",
    )

    with pytest.raises(ValueError, match="Query returned empty result"):
        utils.fetch_json_data("SELECT 1 FORMAT JSON")


def test_fetch_single_json_row(monkeypatch):
    monkeypatch.setattr(
        utils,
        "fetch_json_data",
        lambda query: [{"rows_count": 1}],
    )

    assert utils.fetch_single_json_row("SELECT 1 FORMAT JSON") == {
        "rows_count": 1
    }


def test_fetch_single_json_row_rejects_zero_rows(monkeypatch):
    monkeypatch.setattr(utils, "fetch_json_data", lambda query: [])

    with pytest.raises(ValueError, match="exactly one row"):
        utils.fetch_single_json_row("SELECT 1 FORMAT JSON")


def test_fetch_single_json_row_rejects_multiple_rows(monkeypatch):
    monkeypatch.setattr(
        utils,
        "fetch_json_data",
        lambda query: [{"x": 1}, {"x": 2}],
    )

    with pytest.raises(ValueError, match="exactly one row"):
        utils.fetch_single_json_row("SELECT 1 FORMAT JSON")

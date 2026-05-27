import sys
from pathlib import Path

import pytest


airflow = pytest.importorskip("airflow")
from airflow.models import DagBag


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"
JOBS_DIR = PROJECT_ROOT / "jobs"

sys.path.insert(0, str(JOBS_DIR))


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def period_refresh_dag(dagbag):
    dag = dagbag.dags.get("nyc_taxi_period_refresh_pipeline")

    assert dag is not None

    return dag


def test_period_refresh_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_period_refresh_dag_exists(period_refresh_dag):
    assert period_refresh_dag.dag_id == "nyc_taxi_period_refresh_pipeline"


def test_period_refresh_dag_contains_create_tables_task(period_refresh_dag):
    task_ids = set(period_refresh_dag.task_ids)

    assert "create_clickhouse_gold_tables" in task_ids


def test_period_refresh_dag_does_not_contain_full_rebuild_tasks(period_refresh_dag):
    task_ids = set(period_refresh_dag.task_ids)

    assert "truncate_clickhouse_gold_tables" not in task_ids
    assert "check_clickhouse_gold_quality" not in task_ids


def test_period_refresh_dag_contains_january_and_december_tasks(period_refresh_dag):
    task_ids = set(period_refresh_dag.task_ids)

    expected_tasks = {
        "delete_clickhouse_gold_month_2024_01",
        "bronze_yellow_taxi_2024_01",
        "silver_yellow_taxi_2024_01",
        "check_yellow_taxi_quality_2024_01",
        "gold_daily_trips_2024_01",
        "gold_hourly_trips_2024_01",
        "gold_payment_type_stats_2024_01",
        "gold_location_pair_stats_2024_01",
        "check_gold_schema_2024_01",
        "load_gold_daily_trips_to_clickhouse_2024_01",
        "load_gold_hourly_trips_to_clickhouse_2024_01",
        "load_gold_payment_type_stats_to_clickhouse_2024_01",
        "load_gold_location_pair_stats_to_clickhouse_2024_01",
        "check_clickhouse_gold_month_quality_2024_01",
        "delete_clickhouse_gold_month_2024_12",
        "bronze_yellow_taxi_2024_12",
        "silver_yellow_taxi_2024_12",
        "check_yellow_taxi_quality_2024_12",
        "gold_daily_trips_2024_12",
        "gold_hourly_trips_2024_12",
        "gold_payment_type_stats_2024_12",
        "gold_location_pair_stats_2024_12",
        "check_gold_schema_2024_12",
        "load_gold_daily_trips_to_clickhouse_2024_12",
        "load_gold_hourly_trips_to_clickhouse_2024_12",
        "load_gold_payment_type_stats_to_clickhouse_2024_12",
        "load_gold_location_pair_stats_to_clickhouse_2024_12",
        "check_clickhouse_gold_month_quality_2024_12",
    }

    assert expected_tasks.issubset(task_ids)


def test_period_refresh_dag_has_expected_number_of_tasks(period_refresh_dag):
    # 1 ClickHouse service task:
    # - create_clickhouse_gold_tables
    #
    # For each of 12 months:
    # - delete_clickhouse_gold_month
    # - bronze
    # - silver
    # - silver quality check
    # - 4 gold marts
    # - gold Object Storage schema check
    # - 4 ClickHouse load tasks
    # - ClickHouse month quality check
    #
    # Total = 1 + 12 * 14 = 169
    assert len(period_refresh_dag.task_ids) == 169


def test_create_tables_runs_before_first_month_delete(period_refresh_dag):
    january_delete = period_refresh_dag.get_task(
        "delete_clickhouse_gold_month_2024_01"
    )

    assert january_delete.upstream_task_ids == {"create_clickhouse_gold_tables"}


def test_delete_runs_before_first_bronze_task(period_refresh_dag):
    january_bronze = period_refresh_dag.get_task("bronze_yellow_taxi_2024_01")

    assert january_bronze.upstream_task_ids == {
        "delete_clickhouse_gold_month_2024_01"
    }


def test_january_month_check_runs_before_february_delete(period_refresh_dag):
    february_delete = period_refresh_dag.get_task(
        "delete_clickhouse_gold_month_2024_02"
    )

    assert february_delete.upstream_task_ids == {
        "check_clickhouse_gold_month_quality_2024_01"
    }


def test_monthly_pipeline_dependencies_for_january(period_refresh_dag):
    silver = period_refresh_dag.get_task("silver_yellow_taxi_2024_01")
    quality = period_refresh_dag.get_task("check_yellow_taxi_quality_2024_01")

    gold_daily = period_refresh_dag.get_task("gold_daily_trips_2024_01")
    gold_hourly = period_refresh_dag.get_task("gold_hourly_trips_2024_01")
    gold_payment = period_refresh_dag.get_task("gold_payment_type_stats_2024_01")
    gold_location = period_refresh_dag.get_task("gold_location_pair_stats_2024_01")

    assert silver.upstream_task_ids == {"bronze_yellow_taxi_2024_01"}
    assert quality.upstream_task_ids == {"silver_yellow_taxi_2024_01"}

    assert gold_daily.upstream_task_ids == {"check_yellow_taxi_quality_2024_01"}
    assert gold_hourly.upstream_task_ids == {"check_yellow_taxi_quality_2024_01"}
    assert gold_payment.upstream_task_ids == {"check_yellow_taxi_quality_2024_01"}
    assert gold_location.upstream_task_ids == {"check_yellow_taxi_quality_2024_01"}


def test_gold_tasks_run_before_gold_schema_check_for_january(period_refresh_dag):
    check_gold_schema = period_refresh_dag.get_task("check_gold_schema_2024_01")

    expected_upstream_tasks = {
        "gold_daily_trips_2024_01",
        "gold_hourly_trips_2024_01",
        "gold_payment_type_stats_2024_01",
        "gold_location_pair_stats_2024_01",
    }

    assert check_gold_schema.upstream_task_ids == expected_upstream_tasks


def test_gold_schema_check_runs_before_clickhouse_loads_for_january(
    period_refresh_dag,
):
    load_daily = period_refresh_dag.get_task(
        "load_gold_daily_trips_to_clickhouse_2024_01"
    )
    load_hourly = period_refresh_dag.get_task(
        "load_gold_hourly_trips_to_clickhouse_2024_01"
    )
    load_payment = period_refresh_dag.get_task(
        "load_gold_payment_type_stats_to_clickhouse_2024_01"
    )
    load_location = period_refresh_dag.get_task(
        "load_gold_location_pair_stats_to_clickhouse_2024_01"
    )

    assert load_daily.upstream_task_ids == {"check_gold_schema_2024_01"}
    assert load_hourly.upstream_task_ids == {"check_gold_schema_2024_01"}
    assert load_payment.upstream_task_ids == {"check_gold_schema_2024_01"}
    assert load_location.upstream_task_ids == {"check_gold_schema_2024_01"}


def test_clickhouse_loads_run_before_month_quality_check_for_january(
    period_refresh_dag,
):
    month_quality_check = period_refresh_dag.get_task(
        "check_clickhouse_gold_month_quality_2024_01"
    )

    expected_upstream_tasks = {
        "load_gold_daily_trips_to_clickhouse_2024_01",
        "load_gold_hourly_trips_to_clickhouse_2024_01",
        "load_gold_payment_type_stats_to_clickhouse_2024_01",
        "load_gold_location_pair_stats_to_clickhouse_2024_01",
    }

    assert month_quality_check.upstream_task_ids == expected_upstream_tasks


def test_december_month_quality_check_has_no_downstream_tasks(period_refresh_dag):
    december_month_quality_check = period_refresh_dag.get_task(
        "check_clickhouse_gold_month_quality_2024_12"
    )

    assert december_month_quality_check.downstream_task_ids == set()

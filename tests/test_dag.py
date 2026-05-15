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
def nyc_taxi_dag(dagbag):
    dag = dagbag.get_dag("nyc_taxi_pipeline")

    assert dag is not None

    return dag


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_nyc_taxi_dag_exists(nyc_taxi_dag):
    assert nyc_taxi_dag.dag_id == "nyc_taxi_pipeline"


def test_dag_contains_clickhouse_preparation_tasks(nyc_taxi_dag):
    task_ids = set(nyc_taxi_dag.task_ids)

    assert "create_clickhouse_gold_tables" in task_ids
    assert "truncate_clickhouse_gold_tables" in task_ids


def test_dag_contains_january_and_december_tasks(nyc_taxi_dag):
    task_ids = set(nyc_taxi_dag.task_ids)

    expected_tasks = {
        "bronze_yellow_taxi_2024_01",
        "silver_yellow_taxi_2024_01",
        "check_yellow_taxi_quality_2024_01",
        "gold_daily_trips_2024_01",
        "gold_hourly_trips_2024_01",
        "gold_payment_type_stats_2024_01",
        "gold_location_pair_stats_2024_01",
        "load_gold_daily_trips_to_clickhouse_2024_01",
        "load_gold_hourly_trips_to_clickhouse_2024_01",
        "load_gold_payment_type_stats_to_clickhouse_2024_01",
        "load_gold_location_pair_stats_to_clickhouse_2024_01",
        "bronze_yellow_taxi_2024_12",
        "silver_yellow_taxi_2024_12",
        "check_yellow_taxi_quality_2024_12",
        "load_gold_daily_trips_to_clickhouse_2024_12",
        "load_gold_hourly_trips_to_clickhouse_2024_12",
        "load_gold_payment_type_stats_to_clickhouse_2024_12",
        "load_gold_location_pair_stats_to_clickhouse_2024_12",
    }

    assert expected_tasks.issubset(task_ids)


def test_dag_has_expected_number_of_tasks(nyc_taxi_dag):
    # 2 ClickHouse preparation tasks:
    # - create_clickhouse_gold_tables
    # - truncate_clickhouse_gold_tables
    #
    # For each of 12 months:
    # - bronze
    # - silver
    # - quality check
    # - 4 gold marts
    # - 4 ClickHouse load tasks
    #
    # Total = 2 + 12 * 11 = 134
    assert len(nyc_taxi_dag.task_ids) == 134


def test_create_tables_runs_before_truncate(nyc_taxi_dag):
    truncate_task = nyc_taxi_dag.get_task("truncate_clickhouse_gold_tables")

    assert "create_clickhouse_gold_tables" in truncate_task.upstream_task_ids


def test_truncate_runs_before_first_bronze_task(nyc_taxi_dag):
    january_bronze = nyc_taxi_dag.get_task("bronze_yellow_taxi_2024_01")

    assert "truncate_clickhouse_gold_tables" in january_bronze.upstream_task_ids


def test_january_loads_run_before_february_bronze(nyc_taxi_dag):
    february_bronze = nyc_taxi_dag.get_task("bronze_yellow_taxi_2024_02")

    expected_upstream_tasks = {
        "load_gold_daily_trips_to_clickhouse_2024_01",
        "load_gold_hourly_trips_to_clickhouse_2024_01",
        "load_gold_payment_type_stats_to_clickhouse_2024_01",
        "load_gold_location_pair_stats_to_clickhouse_2024_01",
    }

    assert expected_upstream_tasks == february_bronze.upstream_task_ids


def test_monthly_pipeline_dependencies_for_january(nyc_taxi_dag):
    silver = nyc_taxi_dag.get_task("silver_yellow_taxi_2024_01")
    quality = nyc_taxi_dag.get_task("check_yellow_taxi_quality_2024_01")

    gold_daily = nyc_taxi_dag.get_task("gold_daily_trips_2024_01")
    gold_hourly = nyc_taxi_dag.get_task("gold_hourly_trips_2024_01")
    gold_payment = nyc_taxi_dag.get_task("gold_payment_type_stats_2024_01")
    gold_location = nyc_taxi_dag.get_task("gold_location_pair_stats_2024_01")

    assert "bronze_yellow_taxi_2024_01" in silver.upstream_task_ids
    assert "silver_yellow_taxi_2024_01" in quality.upstream_task_ids

    assert "check_yellow_taxi_quality_2024_01" in gold_daily.upstream_task_ids
    assert "check_yellow_taxi_quality_2024_01" in gold_hourly.upstream_task_ids
    assert "check_yellow_taxi_quality_2024_01" in gold_payment.upstream_task_ids
    assert "check_yellow_taxi_quality_2024_01" in gold_location.upstream_task_ids


def test_gold_tasks_run_before_clickhouse_loads_for_january(nyc_taxi_dag):
    load_daily = nyc_taxi_dag.get_task("load_gold_daily_trips_to_clickhouse_2024_01")
    load_hourly = nyc_taxi_dag.get_task("load_gold_hourly_trips_to_clickhouse_2024_01")
    load_payment = nyc_taxi_dag.get_task("load_gold_payment_type_stats_to_clickhouse_2024_01")
    load_location = nyc_taxi_dag.get_task("load_gold_location_pair_stats_to_clickhouse_2024_01")

    assert "gold_daily_trips_2024_01" in load_daily.upstream_task_ids
    assert "gold_hourly_trips_2024_01" in load_hourly.upstream_task_ids
    assert "gold_payment_type_stats_2024_01" in load_payment.upstream_task_ids
    assert "gold_location_pair_stats_2024_01" in load_location.upstream_task_ids
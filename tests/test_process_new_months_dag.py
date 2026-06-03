import sys
from pathlib import Path

import pytest


airflow = pytest.importorskip("airflow")
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"
JOBS_DIR = PROJECT_ROOT / "jobs"

sys.path.insert(0, str(JOBS_DIR))


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def process_new_months_dag(dagbag):
    dag = dagbag.dags.get("nyc_taxi_process_new_months_pipeline")

    assert dag is not None

    return dag

DISCOVERY_TASK_ID = "discover_new_raw_periods"
SPARK_POOL = "spark_pool"

def mapped_task_upstreams(*business_upstream_task_ids):
    """
    Return expected upstreams for tasks inside a mapped TaskGroup.

    Airflow adds discover_new_raw_periods as an upstream dependency because
    mapped tasks receive discovered_period from the discovery task output.
    """

    return set(business_upstream_task_ids) | {DISCOVERY_TASK_ID}


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_process_new_months_dag_exists(process_new_months_dag):
    assert process_new_months_dag.dag_id == (
        "nyc_taxi_process_new_months_pipeline"
    )


def test_dag_has_local_safe_concurrency_settings(process_new_months_dag):
    assert process_new_months_dag.max_active_runs == 1
    assert process_new_months_dag.max_active_tasks == 1


def test_dag_contains_top_level_tasks(process_new_months_dag):
    task_ids = set(process_new_months_dag.task_ids)

    expected_tasks = {
        "create_clickhouse_gold_tables",
        "discover_new_raw_periods",
        "log_discovered_periods",
        "finish",
    }

    assert expected_tasks.issubset(task_ids)


def test_dag_contains_process_month_task_group_tasks(process_new_months_dag):
    task_ids = set(process_new_months_dag.task_ids)

    expected_tasks = {
        "process_month.delete_clickhouse_gold_month",
        "process_month.bronze_yellow_taxi",
        "process_month.silver_yellow_taxi",
        "process_month.check_yellow_taxi_quality",
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
        "process_month.check_gold_schema",
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
        "process_month.check_clickhouse_gold_month_quality",
    }

    assert expected_tasks.issubset(task_ids)


def test_full_rebuild_truncate_task_is_not_used(process_new_months_dag):
    assert "truncate_clickhouse_gold_tables" not in process_new_months_dag.task_ids


def test_create_tables_runs_before_discovery(process_new_months_dag):
    discovery = process_new_months_dag.get_task("discover_new_raw_periods")

    assert "create_clickhouse_gold_tables" in discovery.upstream_task_ids


def test_discovery_runs_before_logging_and_processing(process_new_months_dag):
    log_periods = process_new_months_dag.get_task("log_discovered_periods")
    delete_month = process_new_months_dag.get_task(
        "process_month.delete_clickhouse_gold_month"
    )

    assert "discover_new_raw_periods" in log_periods.upstream_task_ids
    assert "discover_new_raw_periods" in delete_month.upstream_task_ids


def test_monthly_pipeline_dependencies_inside_task_group(process_new_months_dag):
    delete_month = process_new_months_dag.get_task(
        "process_month.delete_clickhouse_gold_month"
    )
    bronze = process_new_months_dag.get_task("process_month.bronze_yellow_taxi")
    silver = process_new_months_dag.get_task("process_month.silver_yellow_taxi")
    check_quality = process_new_months_dag.get_task(
        "process_month.check_yellow_taxi_quality"
    )

    assert delete_month.upstream_task_ids == {DISCOVERY_TASK_ID}
    assert bronze.upstream_task_ids == mapped_task_upstreams(delete_month.task_id)
    assert silver.upstream_task_ids == mapped_task_upstreams(bronze.task_id)
    assert check_quality.upstream_task_ids == mapped_task_upstreams(silver.task_id)


def test_gold_tasks_run_after_quality_check(process_new_months_dag):
    expected_upstream = mapped_task_upstreams(
        "process_month.check_yellow_taxi_quality"
    )

    for task_id in [
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
    ]:
        task = process_new_months_dag.get_task(task_id)
        assert task.upstream_task_ids == expected_upstream


def test_gold_schema_check_runs_after_all_gold_tasks(process_new_months_dag):
    check_gold_schema = process_new_months_dag.get_task(
        "process_month.check_gold_schema"
    )

    expected_upstream = mapped_task_upstreams(
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
    )

    assert check_gold_schema.upstream_task_ids == expected_upstream


def test_clickhouse_loads_run_after_gold_schema_check(process_new_months_dag):
    expected_upstream = mapped_task_upstreams("process_month.check_gold_schema")

    for task_id in [
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    ]:
        task = process_new_months_dag.get_task(task_id)
        assert task.upstream_task_ids == expected_upstream


def test_month_quality_check_runs_after_all_clickhouse_loads(
    process_new_months_dag,
):
    month_quality = process_new_months_dag.get_task(
        "process_month.check_clickhouse_gold_month_quality"
    )

    expected_upstream = mapped_task_upstreams(
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    )

    assert month_quality.upstream_task_ids == expected_upstream


def test_finish_runs_after_logging_and_month_processing(process_new_months_dag):
    finish = process_new_months_dag.get_task("finish")

    assert finish.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    expected_upstream = {
        "log_discovered_periods",
        "process_month.check_clickhouse_gold_month_quality",
    }

    assert finish.upstream_task_ids == expected_upstream


def test_process_new_months_spark_tasks_use_spark_pool(
    process_new_months_dag,
):
    spark_task_ids = [
        "process_month.bronze_yellow_taxi",
        "process_month.silver_yellow_taxi",
        "process_month.check_yellow_taxi_quality",
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
        "process_month.check_gold_schema",
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    ]

    for task_id in spark_task_ids:
        task = process_new_months_dag.get_task(task_id)

        assert task.pool == SPARK_POOL


def test_process_new_months_non_spark_tasks_do_not_use_spark_pool(
    process_new_months_dag,
):
    non_spark_task_ids = [
        "create_clickhouse_gold_tables",
        "discover_new_raw_periods",
        "log_discovered_periods",
        "process_month.delete_clickhouse_gold_month",
        "process_month.check_clickhouse_gold_month_quality",
        "finish",
    ]

    for task_id in non_spark_task_ids:
        task = process_new_months_dag.get_task(task_id)

        assert task.pool != SPARK_POOL
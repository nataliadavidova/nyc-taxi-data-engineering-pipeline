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

CONFIG_TASK_ID = "read_period_refresh_config"
SPARK_POOL = "spark_pool"


def mapped_task_upstreams(*business_upstream_task_ids):
    """
    Return expected upstreams for tasks inside a mapped TaskGroup.

    Airflow adds read_period_refresh_config as an upstream dependency because
    mapped tasks receive period values from the config task output.
    """

    return set(business_upstream_task_ids) | {CONFIG_TASK_ID}


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


def test_period_refresh_dag_has_local_safe_concurrency_settings(
    period_refresh_dag,
):
    assert period_refresh_dag.max_active_runs == 1
    assert period_refresh_dag.max_active_tasks == 1


def test_period_refresh_dag_has_runtime_params(period_refresh_dag):
    assert period_refresh_dag.params["start_year"] == "2024"
    assert period_refresh_dag.params["start_month"] == "01"
    assert period_refresh_dag.params["end_year"] == "2024"
    assert period_refresh_dag.params["end_month"] == "01"
    assert period_refresh_dag.params["refresh_mode"] == "replace_period"


def test_period_refresh_dag_contains_top_level_tasks(period_refresh_dag):
    task_ids = set(period_refresh_dag.task_ids)

    expected_tasks = {
        "create_clickhouse_gold_tables",
        "read_period_refresh_config",
        "log_period_refresh_periods",
        "finish",
    }

    assert expected_tasks.issubset(task_ids)


def test_period_refresh_dag_contains_process_month_task_group_tasks(
    period_refresh_dag,
):
    task_ids = set(period_refresh_dag.task_ids)

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


def test_period_refresh_dag_does_not_contain_full_rebuild_tasks(
    period_refresh_dag,
):
    task_ids = set(period_refresh_dag.task_ids)

    assert "truncate_clickhouse_gold_tables" not in task_ids
    assert "check_clickhouse_gold_quality" not in task_ids


def test_period_refresh_dag_does_not_use_static_month_task_ids(
    period_refresh_dag,
):
    task_ids = set(period_refresh_dag.task_ids)

    assert "delete_clickhouse_gold_month_2024_01" not in task_ids
    assert "bronze_yellow_taxi_2024_01" not in task_ids
    assert "check_clickhouse_gold_month_quality_2024_01" not in task_ids


def test_period_refresh_dag_has_expected_number_of_tasks(period_refresh_dag):
    # 4 top-level tasks:
    # - create_clickhouse_gold_tables
    # - read_period_refresh_config
    # - log_period_refresh_periods
    # - finish
    #
    # 14 tasks inside mapped process_month TaskGroup:
    # - delete_clickhouse_gold_month
    # - bronze
    # - silver
    # - silver quality check
    # - 4 gold marts
    # - gold Object Storage schema check
    # - 4 ClickHouse load tasks
    # - ClickHouse month quality check
    #
    # Total = 4 + 14 = 18
    assert len(period_refresh_dag.task_ids) == 18


def test_create_tables_runs_before_config_read(period_refresh_dag):
    config_task = period_refresh_dag.get_task("read_period_refresh_config")

    assert config_task.upstream_task_ids == {"create_clickhouse_gold_tables"}


def test_config_read_runs_before_logging_and_processing(period_refresh_dag):
    log_periods = period_refresh_dag.get_task("log_period_refresh_periods")
    delete_month = period_refresh_dag.get_task(
        "process_month.delete_clickhouse_gold_month"
    )

    assert log_periods.upstream_task_ids == {CONFIG_TASK_ID}
    assert delete_month.upstream_task_ids == {CONFIG_TASK_ID}


def test_monthly_pipeline_dependencies_inside_task_group(period_refresh_dag):
    delete_month = period_refresh_dag.get_task(
        "process_month.delete_clickhouse_gold_month"
    )
    bronze = period_refresh_dag.get_task("process_month.bronze_yellow_taxi")
    silver = period_refresh_dag.get_task("process_month.silver_yellow_taxi")
    check_quality = period_refresh_dag.get_task(
        "process_month.check_yellow_taxi_quality"
    )

    assert delete_month.upstream_task_ids == {CONFIG_TASK_ID}
    assert bronze.upstream_task_ids == mapped_task_upstreams(delete_month.task_id)
    assert silver.upstream_task_ids == mapped_task_upstreams(bronze.task_id)
    assert check_quality.upstream_task_ids == mapped_task_upstreams(silver.task_id)


def test_gold_tasks_run_after_quality_check(period_refresh_dag):
    expected_upstream = mapped_task_upstreams(
        "process_month.check_yellow_taxi_quality"
    )

    for task_id in [
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
    ]:
        task = period_refresh_dag.get_task(task_id)
        assert task.upstream_task_ids == expected_upstream


def test_gold_schema_check_runs_after_all_gold_tasks(period_refresh_dag):
    check_gold_schema = period_refresh_dag.get_task(
        "process_month.check_gold_schema"
    )

    expected_upstream = mapped_task_upstreams(
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
    )

    assert check_gold_schema.upstream_task_ids == expected_upstream


def test_clickhouse_loads_run_after_gold_schema_check(period_refresh_dag):
    expected_upstream = mapped_task_upstreams("process_month.check_gold_schema")

    for task_id in [
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    ]:
        task = period_refresh_dag.get_task(task_id)
        assert task.upstream_task_ids == expected_upstream


def test_month_quality_check_runs_after_all_clickhouse_loads(
    period_refresh_dag,
):
    month_quality = period_refresh_dag.get_task(
        "process_month.check_clickhouse_gold_month_quality"
    )

    expected_upstream = mapped_task_upstreams(
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    )

    assert month_quality.upstream_task_ids == expected_upstream


def test_finish_runs_after_logging_and_month_processing(period_refresh_dag):
    finish = period_refresh_dag.get_task("finish")

    assert finish.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    expected_upstream = {
        "log_period_refresh_periods",
        "process_month.check_clickhouse_gold_month_quality",
    }

    assert finish.upstream_task_ids == expected_upstream


def test_period_refresh_spark_tasks_use_spark_pool(period_refresh_dag):
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
        task = period_refresh_dag.get_task(task_id)

        assert task.pool == SPARK_POOL


def test_period_refresh_non_spark_tasks_do_not_use_spark_pool(
    period_refresh_dag,
):
    non_spark_task_ids = [
        "create_clickhouse_gold_tables",
        "read_period_refresh_config",
        "log_period_refresh_periods",
        "process_month.delete_clickhouse_gold_month",
        "process_month.check_clickhouse_gold_month_quality",
        "finish",
    ]

    for task_id in non_spark_task_ids:
        task = period_refresh_dag.get_task(task_id)

        assert task.pool != SPARK_POOL
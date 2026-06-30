import sys
from datetime import timedelta
from pathlib import Path

import pytest


airflow = pytest.importorskip("airflow")
from airflow.models import DagBag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"
JOBS_DIR = PROJECT_ROOT / "jobs"

SPARK_POOL = "spark_pool"

AIRFLOW_RETRY_DELAY = timedelta(minutes=5)

BRONZE_TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)
SILVER_TASK_EXECUTION_TIMEOUT = timedelta(minutes=20)
SILVER_QUALITY_TASK_EXECUTION_TIMEOUT = timedelta(minutes=5)
GOLD_STANDARD_TASK_EXECUTION_TIMEOUT = timedelta(minutes=8)
GOLD_LOCATION_TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)
GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT = timedelta(minutes=5)
CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT = timedelta(minutes=5)

PYTHON_TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)

MAPPING_SOURCE_TASK_ID = "validate_full_rebuild_raw_periods"


def mapped_task_upstreams(*business_upstream_task_ids):
    """
    Return expected upstreams for tasks inside the mapped TaskGroup.

    Airflow adds validate_full_rebuild_raw_periods as an upstream because
    mapped tasks receive period values from that task output.
    """

    return set(business_upstream_task_ids) | {MAPPING_SOURCE_TASK_ID}

sys.path.insert(0, str(JOBS_DIR))

from airflow_callbacks import airflow_failure_callback


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


@pytest.fixture(scope="module")
def full_rebuild_dag(dagbag):
    dag = dagbag.dags.get("nyc_taxi_full_rebuild_pipeline")

    assert dag is not None

    return dag


def test_dag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}


def test_nyc_taxi_full_rebuild_dag_exists(full_rebuild_dag):
    assert full_rebuild_dag.dag_id == "nyc_taxi_full_rebuild_pipeline"


def test_full_rebuild_dag_has_safe_runtime_params(full_rebuild_dag):
    assert full_rebuild_dag.params["rebuild_mode"] == ""
    assert full_rebuild_dag.params["confirm_full_rebuild"] is False
    assert full_rebuild_dag.params["confirm_clickhouse_truncate"] is False
    assert full_rebuild_dag.params["expected_start_year"] == ""
    assert full_rebuild_dag.params["expected_start_month"] == ""
    assert full_rebuild_dag.params["expected_end_year"] == ""
    assert full_rebuild_dag.params["expected_end_month"] == ""


def test_dag_contains_top_level_tasks(full_rebuild_dag):
    task_ids = set(full_rebuild_dag.task_ids)

    expected_tasks = {
        "create_clickhouse_gold_tables",
        "validate_full_rebuild_config",
        "discover_full_rebuild_raw_periods",
        "validate_full_rebuild_raw_periods",
        "log_full_rebuild_plan",
        "truncate_clickhouse_gold_tables",
        "finish",
        "trigger_dbt_analytics_pipeline",
    }

    assert expected_tasks.issubset(task_ids)


def test_dag_contains_process_month_task_group_tasks(full_rebuild_dag):
    task_ids = set(full_rebuild_dag.task_ids)

    expected_tasks = {
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


def test_dag_has_expected_number_of_tasks(full_rebuild_dag):
    # 8 top-level service, safety and orchestration tasks:
    # - create_clickhouse_gold_tables
    # - validate_full_rebuild_config
    # - discover_full_rebuild_raw_periods
    # - validate_full_rebuild_raw_periods
    # - log_full_rebuild_plan
    # - truncate_clickhouse_gold_tables
    # - finish
    # - trigger_dbt_analytics_pipeline
    #
    # 13 tasks inside the dynamically mapped process_month TaskGroup.
    #
    # Total = 8 + 13 = 21
    assert len(full_rebuild_dag.task_ids) == 21


def test_create_tables_runs_before_full_rebuild_validation(full_rebuild_dag):
    validation = full_rebuild_dag.get_task(
        "validate_full_rebuild_config"
    )

    assert validation.upstream_task_ids == {
        "create_clickhouse_gold_tables"
    }


def test_full_rebuild_validation_runs_before_raw_discovery(
    full_rebuild_dag,
):
    discovery = full_rebuild_dag.get_task(
        "discover_full_rebuild_raw_periods"
    )

    assert discovery.upstream_task_ids == {
        "validate_full_rebuild_config"
    }


def test_raw_period_validation_depends_on_config_and_discovery(
    full_rebuild_dag,
):
    raw_validation = full_rebuild_dag.get_task(
        "validate_full_rebuild_raw_periods"
    )

    assert raw_validation.upstream_task_ids == {
        "validate_full_rebuild_config",
        "discover_full_rebuild_raw_periods",
    }


def test_raw_period_validation_runs_before_rebuild_plan_logging(
    full_rebuild_dag,
):
    log_rebuild_plan = full_rebuild_dag.get_task(
        "log_full_rebuild_plan"
    )

    assert log_rebuild_plan.upstream_task_ids == {
        "validate_full_rebuild_raw_periods"
    }


def test_rebuild_plan_logging_runs_before_truncate(
    full_rebuild_dag,
):
    truncate = full_rebuild_dag.get_task(
        "truncate_clickhouse_gold_tables"
    )

    assert truncate.upstream_task_ids == {
        "log_full_rebuild_plan"
    }


def test_truncate_runs_before_mapped_bronze_task(full_rebuild_dag):
    bronze = full_rebuild_dag.get_task(
        "process_month.bronze_yellow_taxi"
    )

    assert bronze.upstream_task_ids == mapped_task_upstreams(
        "truncate_clickhouse_gold_tables"
    )


def test_monthly_pipeline_dependencies_inside_task_group(full_rebuild_dag):
    bronze = full_rebuild_dag.get_task(
        "process_month.bronze_yellow_taxi"
    )
    silver = full_rebuild_dag.get_task(
        "process_month.silver_yellow_taxi"
    )
    check_quality = full_rebuild_dag.get_task(
        "process_month.check_yellow_taxi_quality"
    )

    assert silver.upstream_task_ids == mapped_task_upstreams(
        bronze.task_id
    )
    assert check_quality.upstream_task_ids == mapped_task_upstreams(
        silver.task_id
    )


def test_gold_tasks_run_after_quality_check(full_rebuild_dag):
    expected_upstream = mapped_task_upstreams(
        "process_month.check_yellow_taxi_quality"
    )

    for task_id in [
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
    ]:
        task = full_rebuild_dag.get_task(task_id)

        assert task.upstream_task_ids == expected_upstream


def test_gold_schema_check_runs_after_all_gold_tasks(full_rebuild_dag):
    check_gold_schema = full_rebuild_dag.get_task(
        "process_month.check_gold_schema"
    )

    expected_upstream = mapped_task_upstreams(
        "process_month.gold_daily_trips",
        "process_month.gold_hourly_trips",
        "process_month.gold_payment_type_stats",
        "process_month.gold_location_pair_stats",
    )

    assert check_gold_schema.upstream_task_ids == expected_upstream


def test_clickhouse_loads_run_after_gold_schema_check(full_rebuild_dag):
    expected_upstream = mapped_task_upstreams(
        "process_month.check_gold_schema"
    )

    for task_id in [
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    ]:
        task = full_rebuild_dag.get_task(task_id)

        assert task.upstream_task_ids == expected_upstream


def test_month_quality_check_runs_after_all_clickhouse_loads(
    full_rebuild_dag,
):
    month_quality = full_rebuild_dag.get_task(
        "process_month.check_clickhouse_gold_month_quality"
    )

    expected_upstream = mapped_task_upstreams(
        "process_month.load_gold_daily_trips_to_clickhouse",
        "process_month.load_gold_hourly_trips_to_clickhouse",
        "process_month.load_gold_payment_type_stats_to_clickhouse",
        "process_month.load_gold_location_pair_stats_to_clickhouse",
    )

    assert month_quality.upstream_task_ids == expected_upstream


def test_finish_runs_after_month_processing(full_rebuild_dag):
    finish = full_rebuild_dag.get_task("finish")

    assert finish.upstream_task_ids == {
        "process_month.check_clickhouse_gold_month_quality"
    }


def test_full_rebuild_triggers_dbt_after_finish(full_rebuild_dag):
    trigger_dbt = full_rebuild_dag.get_task("trigger_dbt_analytics_pipeline")

    assert isinstance(trigger_dbt, TriggerDagRunOperator)
    assert trigger_dbt.upstream_task_ids == {"finish"}
    assert trigger_dbt.trigger_dag_id == "nyc_taxi_dbt_analytics_pipeline"
    assert trigger_dbt.wait_for_completion is True
    assert trigger_dbt.poke_interval == 30
    assert trigger_dbt.reset_dag_run is True
    assert trigger_dbt.execution_timeout == PYTHON_TASK_EXECUTION_TIMEOUT


def test_spark_tasks_use_spark_pool(full_rebuild_dag):
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
        task = full_rebuild_dag.get_task(task_id)

        assert task.pool == SPARK_POOL


def test_non_spark_tasks_do_not_use_spark_pool(full_rebuild_dag):
    non_spark_task_ids = [
        "create_clickhouse_gold_tables",
        "validate_full_rebuild_config",
        "discover_full_rebuild_raw_periods",
        "validate_full_rebuild_raw_periods",
        "log_full_rebuild_plan",
        "truncate_clickhouse_gold_tables",
        "finish",
        "process_month.check_clickhouse_gold_month_quality",
        "trigger_dbt_analytics_pipeline",
    ]

    for task_id in non_spark_task_ids:
        task = full_rebuild_dag.get_task(task_id)

        assert task.pool != SPARK_POOL


def test_dag_tasks_use_failure_callback_and_retry_delay(full_rebuild_dag):
    for task in full_rebuild_dag.tasks:
        assert task.retries == 1
        assert task.retry_delay == AIRFLOW_RETRY_DELAY
        assert task.on_failure_callback == airflow_failure_callback


def test_spark_tasks_use_task_family_execution_timeouts(
    full_rebuild_dag,
):
    task_timeout_mapping = {
        "process_month.bronze_yellow_taxi": (
            BRONZE_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.silver_yellow_taxi": (
            SILVER_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.check_yellow_taxi_quality": (
            SILVER_QUALITY_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.gold_daily_trips": (
            GOLD_STANDARD_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.gold_hourly_trips": (
            GOLD_STANDARD_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.gold_payment_type_stats": (
            GOLD_STANDARD_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.gold_location_pair_stats": (
            GOLD_LOCATION_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.check_gold_schema": (
            GOLD_SCHEMA_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.load_gold_daily_trips_to_clickhouse": (
            CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.load_gold_hourly_trips_to_clickhouse": (
            CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.load_gold_payment_type_stats_to_clickhouse": (
            CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT
        ),
        "process_month.load_gold_location_pair_stats_to_clickhouse": (
            CLICKHOUSE_LOAD_TASK_EXECUTION_TIMEOUT
        ),
    }

    for task_id, expected_timeout in task_timeout_mapping.items():
        task = full_rebuild_dag.get_task(task_id)

        assert task.execution_timeout == expected_timeout


def test_python_tasks_use_python_execution_timeout(full_rebuild_dag):
    python_task_ids = [
        "create_clickhouse_gold_tables",
        "validate_full_rebuild_config",
        "discover_full_rebuild_raw_periods",
        "validate_full_rebuild_raw_periods",
        "log_full_rebuild_plan",
        "truncate_clickhouse_gold_tables",
        "process_month.check_clickhouse_gold_month_quality",
        "trigger_dbt_analytics_pipeline",
    ]

    for task_id in python_task_ids:
        task = full_rebuild_dag.get_task(task_id)

        assert task.execution_timeout == PYTHON_TASK_EXECUTION_TIMEOUT
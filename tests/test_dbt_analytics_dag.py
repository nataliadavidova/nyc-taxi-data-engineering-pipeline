from pathlib import Path

from airflow.models import DagBag


DAG_ID = "nyc_taxi_dbt_analytics_pipeline"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"


def load_dbt_analytics_dag():
    dag_bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)

    assert dag_bag.import_errors == {}
    assert DAG_ID in dag_bag.dags

    return dag_bag.dags[DAG_ID]


def test_dbt_analytics_dag_imports_without_errors():
    dag = load_dbt_analytics_dag()

    assert dag.dag_id == DAG_ID


def test_dbt_analytics_dag_tasks():
    dag = load_dbt_analytics_dag()

    task_ids = {task.task_id for task in dag.tasks}

    assert task_ids == {
        "dbt_debug",
        "dbt_build_analytics_layer",
    }


def test_dbt_analytics_dag_dependencies():
    dag = load_dbt_analytics_dag()

    dbt_debug = dag.get_task("dbt_debug")
    dbt_build = dag.get_task("dbt_build_analytics_layer")

    assert dbt_build.task_id in dbt_debug.downstream_task_ids


def test_dbt_analytics_dag_docker_operator_config():
    dag = load_dbt_analytics_dag()

    dbt_debug = dag.get_task("dbt_debug")
    dbt_build = dag.get_task("dbt_build_analytics_layer")

    assert dbt_debug.image == "nyc_taxi_dbt:latest"
    assert dbt_build.image == "nyc_taxi_dbt:latest"

    assert dbt_debug.command == "debug"
    assert dbt_build.command == "build"

    assert dbt_debug.network_mode == "nyc_taxi_network"
    assert dbt_build.network_mode == "nyc_taxi_network"

    assert dbt_debug.mount_tmp_dir is False
    assert dbt_build.mount_tmp_dir is False

    assert dbt_debug.auto_remove == "success"
    assert dbt_build.auto_remove == "success"
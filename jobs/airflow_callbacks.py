"""
Airflow callback helpers for NYC Taxi pipeline monitoring.

The callbacks in this module are intentionally transport-agnostic.
At the current stage they write structured failure messages to Airflow logs.

External notification transports such as Telegram or Slack can be added later
on top of the same message builder.
"""

from __future__ import annotations

from typing import Any, Dict


AirflowContext = Dict[str, Any]


def get_context_value(context: AirflowContext, key: str, default: str = "unknown") -> str:
    """
    Safely read a value from an Airflow callback context.
    """

    value = context.get(key)

    if value is None:
        return default

    return str(value)


def get_object_attribute(
    obj: Any,
    attribute_name: str,
    default: str = "unknown",
) -> str:
    """
    Safely read an attribute from an object.
    """

    if obj is None:
        return default

    value = getattr(obj, attribute_name, None)

    if value is None:
        return default

    return str(value)


def build_failure_alert_message(context: AirflowContext) -> str:
    """
    Build a readable Airflow task failure alert message.

    The function accepts a standard Airflow callback context dictionary and
    returns a plain text message that can be written to logs or sent to an
    external notification channel later.
    """

    dag = context.get("dag")
    task = context.get("task")
    task_instance = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")

    dag_id = get_object_attribute(dag, "dag_id")
    task_id = get_object_attribute(task_instance, "task_id")

    if task_id == "unknown":
        task_id = get_object_attribute(task, "task_id")

    run_id = get_object_attribute(task_instance, "run_id")

    if run_id == "unknown":
        run_id = get_object_attribute(dag_run, "run_id")

    try_number = get_object_attribute(task_instance, "try_number")
    log_url = get_object_attribute(task_instance, "log_url")

    logical_date = get_context_value(
        context=context,
        key="logical_date",
        default=get_context_value(context, "execution_date"),
    )

    exception = get_context_value(context, "exception", default="not provided")

    return "\n".join(
        [
            "NYC Taxi Airflow task failed",
            "",
            f"DAG: {dag_id}",
            f"Task: {task_id}",
            f"Run ID: {run_id}",
            f"Try number: {try_number}",
            f"Logical date: {logical_date}",
            f"Log URL: {log_url}",
            f"Exception: {exception}",
        ]
    )


def airflow_failure_callback(context: AirflowContext) -> None:
    """
    Airflow on_failure_callback for NYC Taxi DAGs.

    For now, the callback writes a structured alert message to task logs.
    A later iteration can reuse build_failure_alert_message() to send the same
    message to Telegram, Slack, email, or another notification channel.
    """

    alert_message = build_failure_alert_message(context)

    print(alert_message)
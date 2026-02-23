"""DAG 07: Trigger Rules.

Demonstrates how trigger rules control when a task runs based on
the state of its upstream tasks. Shows ``all_success``,
``one_success``, ``all_done``, ``none_failed``, and ``all_skipped``.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def always_succeed() -> None:
    """Task that always succeeds."""
    print(f"[{timestamp()}] Success!")


def always_fail() -> None:
    """Task that always raises an exception."""
    msg = "Intentional failure for trigger rule demo"
    raise RuntimeError(msg)


with DAG(
    dag_id="007_trigger_rules",
    default_args={**DEFAULT_ARGS, "retries": 0},
    description="Trigger rules: all_success, one_success, all_done, none_failed",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    success_a = PythonOperator(task_id="success_a", python_callable=always_succeed)
    success_b = PythonOperator(task_id="success_b", python_callable=always_succeed)
    fail_c = PythonOperator(task_id="fail_c", python_callable=always_fail)

    # all_success (default): runs only if ALL upstream tasks succeed
    all_success_task = BashOperator(
        task_id="all_success_task",
        bash_command='echo "all_success: will be skipped because fail_c failed"',
        trigger_rule="all_success",
    )

    # one_success: runs if at least one upstream task succeeds
    one_success_task = BashOperator(
        task_id="one_success_task",
        bash_command='echo "one_success: runs because success_a and success_b succeeded"',
        trigger_rule="one_success",
    )

    # all_done: runs after all upstream tasks complete (regardless of state)
    all_done_task = BashOperator(
        task_id="all_done_task",
        bash_command='echo "all_done: always runs after upstream completes"',
        trigger_rule="all_done",
    )

    # none_failed: runs if no upstream task has failed (skipped is ok)
    none_failed_task = BashOperator(
        task_id="none_failed_task",
        bash_command='echo "none_failed: will be skipped because fail_c failed"',
        trigger_rule="none_failed",
    )

    [success_a, success_b, fail_c] >> all_success_task
    [success_a, success_b, fail_c] >> one_success_task
    [success_a, success_b, fail_c] >> all_done_task
    [success_a, success_b, fail_c] >> none_failed_task

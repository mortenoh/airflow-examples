"""DAG 52: Scheduling Features.

Demonstrates scheduling-related DAG parameters: ``depends_on_past``
(each task waits for its own previous run to succeed),
``wait_for_downstream`` (waits for downstream tasks of previous run),
``max_active_runs``, and a real cron schedule expression.

Note: This DAG uses ``schedule=None`` for safe manual testing, but
includes commented examples showing real cron expressions and the
parameters that control execution order across runs.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def strict_sequential_task() -> None:
    """Task with depends_on_past: won't run until previous run's instance succeeded."""
    print(f"[{timestamp()}] Running strict sequential task")
    print(f"[{timestamp()}] This task has depends_on_past=True")
    print(f"[{timestamp()}] It will not run until the same task in the previous")
    print(f"[{timestamp()}] DAG run has completed successfully.")


def downstream_waiter() -> None:
    """Task with wait_for_downstream: waits for own + downstream tasks of previous run."""
    print(f"[{timestamp()}] Running downstream waiter task")
    print(f"[{timestamp()}] This task has wait_for_downstream=True")
    print(f"[{timestamp()}] It waits for BOTH this task AND its downstream tasks")
    print(f"[{timestamp()}] from the previous run to complete before starting.")


# --- Scheduled DAG example --------------------------------------------------
# This DAG demonstrates depends_on_past and wait_for_downstream.
# These parameters are most meaningful with a real schedule but work
# with manual triggers too (first run always executes).
with DAG(
    dag_id="052_scheduling_features",
    default_args=DEFAULT_ARGS,
    description="depends_on_past, wait_for_downstream, max_active_runs, cron schedules",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    # max_active_runs limits how many DAG runs can execute simultaneously.
    # With depends_on_past, this prevents resource contention.
    max_active_runs=1,
    tags=["example", "scheduling"],
) as dag:
    # This comment shows what real scheduling looks like:
    # schedule="0 6 * * *"   -- daily at 06:00 UTC
    # schedule="*/15 * * * *" -- every 15 minutes
    # schedule="@hourly"      -- built-in preset
    # schedule=timedelta(hours=2) -- every 2 hours

    start = BashOperator(
        task_id="start",
        bash_command="""
            echo "=== Scheduling Features Demo ==="
            echo "max_active_runs: 1 (only one DAG run at a time)"
            echo ""
            echo "Common cron expressions:"
            echo "  0 6 * * *     - Daily at 06:00"
            echo "  */15 * * * *  - Every 15 minutes"
            echo "  0 0 1 * *     - First day of month"
            echo "  0 12 * * MON  - Monday at noon"
            echo ""
            echo "Presets: @once @hourly @daily @weekly @monthly @yearly"
        """,
    )

    # depends_on_past=True: this task instance will not execute until
    # the same task in the PREVIOUS DAG run has succeeded.
    extract = PythonOperator(
        task_id="extract",
        python_callable=strict_sequential_task,
        depends_on_past=True,
    )

    # wait_for_downstream=True: this task will not execute in the NEXT
    # DAG run until BOTH itself AND all its downstream tasks from the
    # CURRENT run have completed. Stricter than depends_on_past.
    transform = PythonOperator(
        task_id="transform",
        python_callable=downstream_waiter,
        wait_for_downstream=True,
    )

    load = BashOperator(
        task_id="load",
        bash_command='echo "Loading transformed data"',
    )

    report = BashOperator(
        task_id="report",
        bash_command="""
            echo "=== Run Complete ==="
            echo "depends_on_past: extract won't run until previous run's extract succeeded"
            echo "wait_for_downstream: transform won't run until previous run's"
            echo "  transform + load + report all completed"
        """,
    )

    start >> extract >> transform >> load >> report

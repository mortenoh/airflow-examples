"""DAG 40: LatestOnlyOperator.

Demonstrates ``LatestOnlyOperator`` which skips downstream tasks
when a DAG run is not the most recent. Useful for preventing
backfill runs from executing side-effect tasks like notifications,
deployments, or cache refreshes.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def always_runs() -> None:
    """Task that runs regardless of whether this is the latest run."""
    print(f"[{timestamp()}] This task always runs (before LatestOnlyOperator)")


def only_latest() -> None:
    """Task that only runs on the latest DAG run."""
    print(f"[{timestamp()}] This task only runs on the latest DAG run")
    print(f"[{timestamp()}] Safe to send notifications, update caches, etc.")


with DAG(
    dag_id="040_latest_only",
    default_args=DEFAULT_ARGS,
    description="LatestOnlyOperator to skip tasks during backfills",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    process_data = PythonOperator(
        task_id="process_data",
        python_callable=always_runs,
    )

    # LatestOnlyOperator: downstream tasks only run on the latest DAG run
    latest_check = LatestOnlyOperator(task_id="latest_check")

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=only_latest,
    )

    update_dashboard = BashOperator(
        task_id="update_dashboard",
        bash_command='echo "Updating dashboard (only on latest run)"',
    )

    # Tasks after LatestOnlyOperator are skipped during backfills
    process_data >> latest_check >> [send_notification, update_dashboard]

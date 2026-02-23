"""DAG 18: Short Circuit.

Demonstrates ``ShortCircuitOperator`` which skips all downstream
tasks when its callable returns ``False``. Useful for conditional
pipeline execution based on runtime checks.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def check_condition_true() -> bool:
    """Check that always passes."""
    print(f"[{timestamp()}] Condition check: PASS")
    return True


def check_condition_false() -> bool:
    """Check that always fails (will short-circuit downstream)."""
    print(f"[{timestamp()}] Condition check: FAIL (short-circuiting)")
    return False


def process_data() -> None:
    """Simulated processing step."""
    print(f"[{timestamp()}] Processing data...")


with DAG(
    dag_id="018_short_circuit",
    default_args=DEFAULT_ARGS,
    description="ShortCircuitOperator for conditional pipeline execution",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # Path 1: condition is True, downstream tasks execute normally
    check_pass = ShortCircuitOperator(
        task_id="check_pass",
        python_callable=check_condition_true,
    )

    will_run = BashOperator(
        task_id="will_run",
        bash_command='echo "This task runs because check_pass returned True"',
    )

    # Path 2: condition is False, downstream tasks are skipped
    check_fail = ShortCircuitOperator(
        task_id="check_fail",
        python_callable=check_condition_false,
    )

    will_skip = BashOperator(
        task_id="will_skip",
        bash_command='echo "This task is skipped because check_fail returned False"',
    )

    # Common endpoint
    report = PythonOperator(
        task_id="report",
        python_callable=process_data,
        trigger_rule="all_done",
    )

    check_pass >> will_run >> report
    check_fail >> will_skip >> report

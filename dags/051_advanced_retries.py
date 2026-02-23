"""DAG 51: Advanced Retries, Timeouts, and DAG Callbacks.

Demonstrates ``retry_exponential_backoff``, ``execution_timeout``,
``max_retry_delay``, DAG-level ``on_failure_callback`` /
``on_success_callback``, and ``sla`` for monitoring task duration.
Builds on the basic retry patterns from DAG 12.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp

_attempt_counter: dict[str, int] = {}


# --- DAG-level callbacks ---------------------------------------------------
# These fire when the entire DAG run succeeds or fails (not per-task).
def dag_success_callback(context: dict) -> None:  # type: ignore[type-arg]
    """Called when the entire DAG run completes successfully."""
    dag_id = context["dag_run"].dag_id
    run_id = context["dag_run"].run_id
    print(f"[{timestamp()}] DAG SUCCESS: {dag_id} (run: {run_id})")


def dag_failure_callback(context: dict) -> None:  # type: ignore[type-arg]
    """Called when the DAG run fails (any task fails after all retries)."""
    dag_id = context["dag_run"].dag_id
    print(f"[{timestamp()}] DAG FAILURE: {dag_id}")
    print(f"  Failed task: {context.get('task_instance', {})}")


# --- Task callables --------------------------------------------------------
def flaky_with_backoff() -> None:
    """Task that fails twice then succeeds on third attempt."""
    key = "flaky_backoff"
    _attempt_counter[key] = _attempt_counter.get(key, 0) + 1
    attempt = _attempt_counter[key]
    print(f"[{timestamp()}] Attempt {attempt} (exponential backoff)")
    if attempt < 3:
        msg = f"Transient failure on attempt {attempt}"
        raise RuntimeError(msg)
    print(f"[{timestamp()}] Succeeded on attempt {attempt}")


def slow_task() -> None:
    """Task that demonstrates execution_timeout awareness."""
    import time

    print(f"[{timestamp()}] Starting slow task (will complete within timeout)")
    time.sleep(2)
    print(f"[{timestamp()}] Slow task completed")


with DAG(
    dag_id="051_advanced_retries",
    default_args=DEFAULT_ARGS,
    description="Exponential backoff, execution_timeout, DAG-level callbacks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
    # DAG-level callbacks: fire on DAG run completion
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting advanced retry examples"',
    )

    # --- Exponential backoff retries ------------------------------------
    # retry_exponential_backoff=True doubles the delay each retry:
    # attempt 1: retry_delay, attempt 2: retry_delay*2, etc.
    # max_retry_delay caps the maximum delay.
    exponential_task = PythonOperator(
        task_id="exponential_backoff",
        python_callable=flaky_with_backoff,
        retries=5,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=30),
    )

    # --- execution_timeout ----------------------------------------------
    # If the task takes longer than this, it's killed with SIGTERM.
    # Use for tasks that might hang or take unpredictably long.
    timeout_task = PythonOperator(
        task_id="with_execution_timeout",
        python_callable=slow_task,
        execution_timeout=timedelta(seconds=30),
    )

    # --- Bash with timeout ----------------------------------------------
    bash_timeout = BashOperator(
        task_id="bash_with_timeout",
        bash_command="""
            echo "Running with 30s execution timeout"
            sleep 1
            echo "Task completed within timeout"
        """,
        execution_timeout=timedelta(seconds=30),
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "All advanced retry examples complete"',
    )

    start >> exponential_task >> timeout_task >> bash_timeout >> done

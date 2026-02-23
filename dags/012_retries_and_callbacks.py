"""DAG 12: Retries and Callbacks.

Demonstrates retry configuration and callback functions.
Shows ``retries``, ``retry_delay``, ``on_failure_callback``,
``on_success_callback``, and ``on_retry_callback``.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp

_attempt_counter: dict[str, int] = {}


def on_success(context: dict) -> None:  # type: ignore[type-arg]
    """Callback fired when a task succeeds."""
    print(f"[{timestamp()}] SUCCESS callback: {context['task_instance'].task_id}")


def on_failure(context: dict) -> None:  # type: ignore[type-arg]
    """Callback fired when a task fails after all retries."""
    print(f"[{timestamp()}] FAILURE callback: {context['task_instance'].task_id}")
    print(f"  Exception: {context.get('exception')}")


def on_retry(context: dict) -> None:  # type: ignore[type-arg]
    """Callback fired when a task is retried."""
    print(f"[{timestamp()}] RETRY callback: {context['task_instance'].task_id}")


def succeed_always() -> None:
    """Task that always succeeds."""
    print(f"[{timestamp()}] Task succeeded on first attempt")


def fail_then_succeed() -> None:
    """Task that fails on first attempt then succeeds.

    Uses a module-level counter to track attempts.
    """
    key = "fail_then_succeed"
    _attempt_counter[key] = _attempt_counter.get(key, 0) + 1
    attempt = _attempt_counter[key]
    print(f"[{timestamp()}] Attempt {attempt}")
    if attempt < 2:
        msg = f"Intentional failure on attempt {attempt}"
        raise RuntimeError(msg)
    print(f"[{timestamp()}] Succeeded on attempt {attempt}")


def fail_always() -> None:
    """Task that always fails."""
    msg = "This task always fails"
    raise RuntimeError(msg)


with DAG(
    dag_id="012_retries_and_callbacks",
    default_args=DEFAULT_ARGS,
    description="Retry configuration and success/failure/retry callbacks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    reliable_task = PythonOperator(
        task_id="reliable_task",
        python_callable=succeed_always,
        on_success_callback=on_success,
        on_failure_callback=on_failure,
    )

    flaky_task = PythonOperator(
        task_id="flaky_task",
        python_callable=fail_then_succeed,
        retries=3,
        retry_delay=timedelta(seconds=1),
        on_success_callback=on_success,
        on_failure_callback=on_failure,
        on_retry_callback=on_retry,
    )

    doomed_task = PythonOperator(
        task_id="doomed_task",
        python_callable=fail_always,
        retries=1,
        retry_delay=timedelta(seconds=1),
        on_failure_callback=on_failure,
        on_retry_callback=on_retry,
    )

    reliable_task >> flaky_task >> doomed_task

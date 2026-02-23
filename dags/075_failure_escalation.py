"""DAG 75: Progressive Failure Escalation.

Demonstrates the retry -> warn -> alert -> escalate pattern using
Airflow callbacks. Shows retry behavior with ``try_number``,
on_retry_callback, on_failure_callback, and on_success_callback
for progressive escalation logic.
"""

import time
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

# Track retry attempts (simulated -- in production use task instance try_number)
_ATTEMPT_FILE = str(OUTPUT_BASE / "escalation_attempts")


def on_retry_callback(context: dict[str, Any]) -> None:
    """Log warning on each retry attempt."""
    ti = context.get("task_instance")
    try_number = getattr(ti, "try_number", "?") if ti else "?"
    print(f"[RETRY WARNING] Task {context.get('task_id')} retry attempt {try_number}")
    print("  Escalation: retry level -- monitoring team notified")


def on_failure_callback(context: dict[str, Any]) -> None:
    """Log critical alert on final failure."""
    print(f"[CRITICAL ALERT] Task {context.get('task_id')} FAILED after all retries")
    print("  Escalation: failure level -- on-call engineer paged")
    print("  Would send email to: oncall@example.com")


def on_success_callback(context: dict[str, Any]) -> None:
    """Log recovery notice if task succeeded after retries."""
    ti = context.get("task_instance")
    try_number = getattr(ti, "try_number", 1) if ti else 1
    if isinstance(try_number, int) and try_number > 1:
        print(f"[RECOVERY] Task {context.get('task_id')} recovered after {try_number} attempts")
        print("  Previous retry warnings can be cleared")
    else:
        print(f"[SUCCESS] Task {context.get('task_id')} completed on first attempt")


@task(
    retries=2,
    retry_delay=timedelta(seconds=2),
    on_retry_callback=on_retry_callback,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
)
def unreliable_task() -> dict[str, object]:
    """Fail on first attempts, succeed on final retry."""
    import os

    # Use file to track attempts across retries
    attempt = 1
    if os.path.exists(_ATTEMPT_FILE):
        with open(_ATTEMPT_FILE) as f:
            attempt = int(f.read().strip()) + 1

    with open(_ATTEMPT_FILE, "w") as f:
        f.write(str(attempt))

    print(f"[{timestamp()}] Unreliable task attempt #{attempt}")

    if attempt < 3:
        print(f"[{timestamp()}] Simulating failure (attempt {attempt} of 3)")
        msg = f"Transient error on attempt {attempt}"
        raise RuntimeError(msg)

    print(f"[{timestamp()}] Success on attempt #{attempt}")
    return {"attempts": attempt, "status": "recovered"}


@task(on_success_callback=on_success_callback)
def always_succeeds() -> dict[str, object]:
    """Contrast task that always succeeds on first attempt."""
    print(f"[{timestamp()}] Always-succeeds task running...")
    time.sleep(0.1)
    print(f"[{timestamp()}] Completed successfully (no retries needed)")
    return {"attempts": 1, "status": "clean_success"}


@task
def escalation_summary(
    unreliable: dict[str, object], reliable: dict[str, object]
) -> None:
    """Print retry history and escalation actions taken."""
    import os

    print(f"[{timestamp()}] === Failure Escalation Summary ===")
    print("\n  Unreliable task:")
    print(f"    Attempts: {unreliable['attempts']}")
    print(f"    Status: {unreliable['status']}")
    print("    Escalation chain: retry_warning x2 -> success_recovery")
    print("\n  Reliable task:")
    print(f"    Attempts: {reliable['attempts']}")
    print(f"    Status: {reliable['status']}")
    print("    Escalation chain: none (clean success)")
    print("\n  Escalation levels:")
    print("    1. Retry: automatic, logged as warning")
    print("    2. Failure: critical alert, on-call paged")
    print("    3. Recovery: clear previous warnings")

    # Clean up attempt tracker
    if os.path.exists(_ATTEMPT_FILE):
        os.remove(_ATTEMPT_FILE)


with DAG(
    dag_id="075_failure_escalation",
    default_args=DEFAULT_ARGS,
    description="Progressive failure escalation: retry -> warn -> alert -> recover",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "alerting"],
) as dag:
    u = unreliable_task()
    a = always_succeeds()
    s = escalation_summary(unreliable=u, reliable=a)

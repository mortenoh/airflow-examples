"""DAG 73: SLA Miss Detection.

Demonstrates task-level execution timeouts, DAG-level callbacks for
SLA monitoring, and comparing expected vs actual task durations.
Shows how to track and report on pipeline performance.
"""

import time
from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

# Expected durations for SLA tracking (seconds)
SLA_THRESHOLDS: dict[str, float] = {
    "fast_task": 2.0,
    "slow_task": 10.0,
    "critical_task": 5.0,
}


def on_dag_success(context: dict[str, object]) -> None:
    """DAG-level success callback for SLA reporting."""
    print(f"[DAG SUCCESS] DAG {context.get('dag_id')} completed successfully")


def on_dag_failure(context: dict[str, object]) -> None:
    """DAG-level failure callback for SLA alerting."""
    print(f"[DAG FAILURE] DAG {context.get('dag_id')} failed - SLA breach likely")


@task
def fast_task() -> dict[str, object]:
    """Complete quickly, well within SLA."""
    start = time.time()
    print(f"[{timestamp()}] Fast task executing...")
    time.sleep(0.1)
    elapsed = time.time() - start
    print(f"[{timestamp()}] Fast task done in {elapsed:.2f}s (SLA: {SLA_THRESHOLDS['fast_task']}s)")
    return {"task": "fast_task", "duration": elapsed}


@task(execution_timeout=timedelta(seconds=30))
def slow_task() -> dict[str, object]:
    """Simulate slow work with execution_timeout protection."""
    start = time.time()
    print(f"[{timestamp()}] Slow task executing (timeout=30s)...")
    time.sleep(5)  # Simulate slow but within timeout
    elapsed = time.time() - start
    print(f"[{timestamp()}] Slow task done in {elapsed:.2f}s (SLA: {SLA_THRESHOLDS['slow_task']}s)")
    return {"task": "slow_task", "duration": elapsed}


@task
def critical_task() -> dict[str, object]:
    """Simulate critical-path work with timing info."""
    start = time.time()
    print(f"[{timestamp()}] Critical task executing...")
    # Simulate variable workload
    time.sleep(2)
    elapsed = time.time() - start
    print(f"[{timestamp()}] Critical task done in {elapsed:.2f}s (SLA: {SLA_THRESHOLDS['critical_task']}s)")
    return {"task": "critical_task", "duration": elapsed}


@task
def sla_report(
    fast: dict[str, object], slow: dict[str, object], critical: dict[str, object]
) -> None:
    """Compare expected vs actual durations and flag SLA breaches."""
    results = [fast, slow, critical]
    print(f"[{timestamp()}] === SLA Monitoring Report ===")
    print(f"  {'Task':<15s} {'Actual':>8s} {'SLA':>8s} {'Status':>10s}")
    print(f"  {'-' * 45}")

    breaches = 0
    for r in results:
        task_name = str(r["task"])
        duration = float(r["duration"])  # type: ignore[arg-type]
        threshold = SLA_THRESHOLDS.get(task_name, 999.0)
        status = "OK" if duration <= threshold else "BREACH"
        if status == "BREACH":
            breaches += 1
        print(f"  {task_name:<15s} {duration:>7.2f}s {threshold:>7.1f}s {status:>10s}")

    print(f"\n  Total tasks: {len(results)}")
    print(f"  SLA breaches: {breaches}")
    print(f"  Status: {'ALL CLEAR' if breaches == 0 else 'SLA BREACH DETECTED'}")


with DAG(
    dag_id="073_sla_monitoring",
    default_args=DEFAULT_ARGS,
    description="SLA monitoring with execution_timeout and duration tracking",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "alerting"],
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:
    f = fast_task()
    s = slow_task()
    c = critical_task()
    r = sla_report(fast=f, slow=s, critical=c)

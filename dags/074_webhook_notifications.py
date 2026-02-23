"""DAG 74: Webhook Alert Notifications.

Demonstrates POSTing JSON alert payloads to httpbin (simulating
Slack/Teams webhooks) on pipeline events. Shows notification on
start, success, and conditional failure handling with trigger_rule.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

WEBHOOK_URL = "http://httpbin:8080/post"


@task
def send_start_notification() -> dict[str, object]:
    """POST pipeline-started notification to webhook."""
    import httpx

    payload = {
        "event": "pipeline_started",
        "dag_id": "74_webhook_notifications",
        "timestamp": datetime.now().isoformat(),
        "message": "Data pipeline started processing",
    }
    resp = httpx.post(WEBHOOK_URL, json=payload, timeout=10)
    print(f"[{timestamp()}] Start notification sent: HTTP {resp.status_code}")
    print(f"[{timestamp()}] Payload: {payload}")
    return {"status": resp.status_code, "event": "started"}


@task
def process_data() -> dict[str, object]:
    """Simulate data processing and return stats."""
    import random
    import time

    rng = random.Random(42)
    time.sleep(1)  # Simulate work
    stats = {
        "rows_processed": rng.randint(100, 1000),
        "errors": 0,
        "duration_seconds": 1.0,
    }
    print(f"[{timestamp()}] Processing complete: {stats}")
    return stats


@task
def send_success_notification(start: dict[str, object], stats: dict[str, object]) -> None:
    """POST success payload with processing stats."""
    import httpx

    payload = {
        "event": "pipeline_success",
        "dag_id": "74_webhook_notifications",
        "timestamp": datetime.now().isoformat(),
        "message": "Data pipeline completed successfully",
        "stats": stats,
    }
    resp = httpx.post(WEBHOOK_URL, json=payload, timeout=10)
    print(f"[{timestamp()}] Success notification sent: HTTP {resp.status_code}")
    print(f"[{timestamp()}] Stats: rows={stats['rows_processed']}, errors={stats['errors']}")


@task(trigger_rule="one_failed")
def handle_failure() -> None:
    """POST failure notification (only runs if process_data fails)."""
    import httpx

    payload = {
        "event": "pipeline_failure",
        "dag_id": "74_webhook_notifications",
        "timestamp": datetime.now().isoformat(),
        "message": "ALERT: Data pipeline failed",
        "severity": "critical",
    }
    resp = httpx.post(WEBHOOK_URL, json=payload, timeout=10)
    print(f"[{timestamp()}] Failure notification sent: HTTP {resp.status_code}")


@task(trigger_rule="all_done")
def final_status() -> None:
    """Print final pipeline status regardless of upstream outcome."""
    print(f"[{timestamp()}] === Pipeline Status ===")
    print("  DAG: 74_webhook_notifications")
    print(f"  Completed at: {datetime.now().isoformat()}")
    print("  All notifications dispatched")


with DAG(
    dag_id="074_webhook_notifications",
    default_args=DEFAULT_ARGS,
    description="Webhook notifications on pipeline start, success, and failure",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "alerting"],
) as dag:
    start = send_start_notification()
    data = process_data()
    success = send_success_notification(start=start, stats=data)
    failure = handle_failure()
    final = final_status()

    start >> data >> [success, failure] >> final  # type: ignore[list-item]

"""DAG 108: Backfill Awareness Demo.

Demonstrates that Airflow tasks always receive their *logical date* (the
scheduled slot), not the wall-clock time. When a server is down for hours
and catches up, each backfill run gets the correct ``logical_date`` for its
slot. Tasks can also detect whether they are running a backfill via
``dag_run.run_type``.
"""

from datetime import UTC, datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

with DAG(
    dag_id="108_backfill_awareness",
    default_args=DEFAULT_ARGS,
    description="Backfill awareness: logical_date vs wall-clock, run type detection",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=True,
    max_active_runs=1,
    tags=["example", "backfill"],
) as dag:
    show_dates = BashOperator(
        task_id="show_dates",
        bash_command="""
            echo "=== Logical Date vs Wall-Clock Time ==="
            echo "Logical date (scheduled slot): {{ logical_date }}"
            echo "Wall-clock time (actual now):  $(date -u '+%Y-%m-%dT%H:%M:%S+00:00')"
            echo ""
            echo "During backfill these timestamps differ significantly."
            echo "During normal runs they are close together."
        """,
    )

    show_data_interval = BashOperator(
        task_id="show_data_interval",
        bash_command="""
            echo "=== Data Interval ==="
            echo "Interval start: {{ data_interval_start }}"
            echo "Interval end:   {{ data_interval_end }}"
            echo ""
            echo "This run covers the 1-hour window above."
            echo "Tasks should process data within this interval only."
        """,
    )

    detect_backfill = BashOperator(
        task_id="detect_backfill",
        bash_command="""
            echo "=== Run Type Detection ==="
            echo "Run type: {{ dag_run.run_type }}"
            echo ""
            {% if dag_run.run_type == "backfill" %}
            echo "*** BACKFILL RUN ***"
            echo "This run was created by a backfill command."
            echo "The logical_date is far behind the current time."
            {% elif dag_run.run_type == "scheduled" %}
            echo "--- Scheduled run ---"
            echo "This run was created by the scheduler."
            echo "It may be a catchup run if the DAG was paused or the server was down."
            {% elif dag_run.run_type == "manual" %}
            echo ">>> Manual trigger <<<"
            echo "This run was manually triggered by a user."
            {% else %}
            echo "Unknown run type: {{ dag_run.run_type }}"
            {% endif %}
        """,
    )

    @task
    def staleness_report(**context: object) -> None:
        """Compute how far behind the logical date is from now.

        Prints whether the run is "current" (within 2 hours of wall-clock)
        or "catching up" (logical date is further behind).
        """
        logical_date = context["logical_date"]  # type: ignore[index]
        now = datetime.now(UTC)
        delta = now - logical_date  # type: ignore[operator]

        hours_behind = delta.total_seconds() / 3600  # type: ignore[union-attr]

        print(f"[{timestamp()}] === Staleness Report ===")
        print(f"  Logical date:  {logical_date}")
        print(f"  Current time:  {now.isoformat()}")
        print(f"  Hours behind:  {hours_behind:.1f}")

        if hours_behind <= 2:  # noqa: PLR2004
            print("  Status: CURRENT -- this run is up to date")
        else:
            print(f"  Status: CATCHING UP -- {hours_behind:.0f} hours behind schedule")

    show_dates >> show_data_interval >> detect_backfill >> staleness_report()

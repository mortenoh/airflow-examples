"""DAG 47: TaskFlow Decorator Variants.

Demonstrates TaskFlow API decorator variants beyond the basic ``@task``:
``@task.bash`` for inline bash commands, ``@task.short_circuit`` for
conditional skipping, and ``@task.branch`` with multiple return values.
These decorators let you write tasks as decorated Python functions
instead of instantiating operator classes.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


# --- @task.bash: run bash commands from a Python function -------------------
# The function returns a bash command string that Airflow executes.
@task.bash
def get_system_info() -> str:
    """Return a bash command that prints system information."""
    return """
        echo "Hostname: $(hostname)"
        echo "Date:     $(date '+%Y-%m-%d %H:%M:%S')"
        echo "User:     $(whoami)"
        echo "Uptime:   $(uptime)"
    """


@task.bash
def create_temp_file() -> str:
    """Return a bash command that creates a temp file and prints its path."""
    return """
        TMPFILE=$(mktemp)
        echo "Created: $TMPFILE"
        echo "test data from airflow" > "$TMPFILE"
        cat "$TMPFILE"
        rm "$TMPFILE"
        echo "/tmp/test_created"
    """


# --- @task.short_circuit: skip downstream if False -------------------------
# Returns True/False; if False, all downstream tasks are skipped.
@task.short_circuit
def check_data_available() -> bool:
    """Check if data is available (simulated: always True)."""
    available = True
    print(f"[{timestamp()}] Data available: {available}")
    return available


@task.short_circuit
def check_is_weekday() -> bool:
    """Check if today is a weekday (simulated: returns False to demo skip)."""
    is_weekday = False
    print(f"[{timestamp()}] Is weekday: {is_weekday} -- downstream will be SKIPPED")
    return is_weekday


# --- Regular @task functions for downstream work ----------------------------
@task
def process_data() -> str:
    """Process data (runs after short-circuit passes)."""
    print(f"[{timestamp()}] Processing data...")
    return "processed"


@task
def send_report(status: str) -> None:
    """Send report (runs after processing)."""
    print(f"[{timestamp()}] Sending report with status: {status}")


@task
def weekend_report() -> None:
    """Weekend-specific report (skipped by check_is_weekday)."""
    print(f"[{timestamp()}] Generating weekend report...")


with DAG(
    dag_id="047_taskflow_decorators",
    default_args=DEFAULT_ARGS,
    description="TaskFlow variants: @task.bash, @task.short_circuit",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "taskflow"],
) as dag:
    # @task.bash chain
    info = get_system_info()
    temp = create_temp_file()
    info >> temp

    # @task.short_circuit: True path (downstream runs)
    check_ok = check_data_available()
    processed = process_data()
    report = send_report(processed)
    check_ok >> processed

    # @task.short_circuit: False path (downstream skipped)
    check_weekend = check_is_weekday()
    weekend = weekend_report()
    check_weekend >> weekend

    # Join all paths
    join = BashOperator(
        task_id="join",
        bash_command='echo "All TaskFlow decorator examples complete"',
        trigger_rule="all_done",
    )

    [temp, report, weekend] >> join

"""DAG 49: EmptyOperator and BranchPythonOperator.

Demonstrates ``EmptyOperator`` (the Airflow 3.x replacement for
``DummyOperator``) as a no-op join/fork point, and the classic
``BranchPythonOperator`` (operator-style branching, as opposed to
the ``@task.branch`` decorator shown in DAG 06).
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def decide_processing_path(**context: object) -> str:
    """Choose a branch based on the execution day.

    Returns the task_id of the branch to execute.
    Classic BranchPythonOperator uses a callable that returns a task_id string.
    """
    day = 15  # Simulated day-of-month
    print(f"[{timestamp()}] Day of month: {day}")
    if day <= 10:
        return "early_month_processing"
    elif day <= 20:
        return "mid_month_processing"
    else:
        return "late_month_processing"


def decide_data_quality(**context: object) -> list[str]:
    """Branch that selects MULTIPLE downstream tasks.

    BranchPythonOperator can return a list of task_ids to execute
    multiple branches simultaneously.
    """
    print(f"[{timestamp()}] Checking data quality...")
    return ["validate_schema", "validate_values"]


def do_processing(stage: str) -> None:
    """Simulate a processing step."""
    print(f"[{timestamp()}] Processing: {stage}")


with DAG(
    dag_id="049_empty_and_branch_operators",
    default_args=DEFAULT_ARGS,
    description="EmptyOperator (join/fork) and BranchPythonOperator (classic branching)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # --- EmptyOperator as start/end/join points -------------------------
    # EmptyOperator does nothing -- it's a structural node for organizing
    # DAG topology (forks, joins, explicit start/end markers).
    start = EmptyOperator(task_id="start")

    # --- BranchPythonOperator: single-branch selection ------------------
    branch_by_date = BranchPythonOperator(
        task_id="branch_by_date",
        python_callable=decide_processing_path,
    )

    early = BashOperator(
        task_id="early_month_processing",
        bash_command='echo "Early month: generating preliminary reports"',
    )

    mid = BashOperator(
        task_id="mid_month_processing",
        bash_command='echo "Mid month: running reconciliation"',
    )

    late = BashOperator(
        task_id="late_month_processing",
        bash_command='echo "Late month: closing period"',
    )

    # EmptyOperator as a join point after branching.
    # trigger_rule="none_failed_min_one_success" ensures it runs
    # when at least one branch succeeded and none failed.
    join_date = EmptyOperator(
        task_id="join_date_branches",
        trigger_rule="none_failed_min_one_success",
    )

    # --- BranchPythonOperator: multi-branch selection -------------------
    branch_quality = BranchPythonOperator(
        task_id="branch_quality_checks",
        python_callable=decide_data_quality,
    )

    validate_schema = PythonOperator(
        task_id="validate_schema",
        python_callable=do_processing,
        op_kwargs={"stage": "schema validation"},
    )

    validate_values = PythonOperator(
        task_id="validate_values",
        python_callable=do_processing,
        op_kwargs={"stage": "value range validation"},
    )

    validate_completeness = PythonOperator(
        task_id="validate_completeness",
        python_callable=do_processing,
        op_kwargs={"stage": "completeness check"},
    )

    join_quality = EmptyOperator(
        task_id="join_quality_checks",
        trigger_rule="none_failed_min_one_success",
    )

    # --- Final join with EmptyOperator ----------------------------------
    end = EmptyOperator(task_id="end")

    # Wiring
    start >> branch_by_date >> [early, mid, late] >> join_date
    join_date >> branch_quality >> [validate_schema, validate_values, validate_completeness]
    [validate_schema, validate_values, validate_completeness] >> join_quality >> end

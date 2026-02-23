"""DAG 06: Branching.

Demonstrates conditional execution paths using ``@task.branch``.
The branch function returns the task_id of the next task to execute,
skipping all other downstream branches.
"""

import random
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


@task.branch
def choose_branch() -> str:
    """Randomly choose a processing path."""
    choice = random.choice(["fast_path", "slow_path", "skip_path"])
    print(f"[{timestamp()}] Chose branch: {choice}")
    return choice


with DAG(
    dag_id="006_branching",
    default_args=DEFAULT_ARGS,
    description="Conditional branching with @task.branch",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    branch = choose_branch()

    fast_path = BashOperator(
        task_id="fast_path",
        bash_command='echo "Taking the fast path"',
    )

    slow_path = BashOperator(
        task_id="slow_path",
        bash_command='echo "Taking the slow path (more processing)"',
    )

    skip_path = BashOperator(
        task_id="skip_path",
        bash_command='echo "Skipping most processing"',
    )

    join = BashOperator(
        task_id="join",
        bash_command='echo "All branches converge here"',
        trigger_rule="none_failed_min_one_success",
    )

    branch >> [fast_path, slow_path, skip_path] >> join

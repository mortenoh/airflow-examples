"""DAG 16: Pools and Priority.

Demonstrates resource management with pools and priority weights.
Pools limit concurrent task execution (e.g., database connection limits),
while ``priority_weight`` controls execution order within a pool.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="016_pools_and_priority",
    default_args=DEFAULT_ARGS,
    description="Pools for concurrency control and priority_weight for ordering",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # Tasks using the default pool with different priorities
    # Higher priority_weight = executed first when resources are constrained
    high_priority = BashOperator(
        task_id="high_priority",
        bash_command='echo "High priority task (weight=10)"',
        priority_weight=10,
        pool="default_pool",
    )

    medium_priority = BashOperator(
        task_id="medium_priority",
        bash_command='echo "Medium priority task (weight=5)"',
        priority_weight=5,
        pool="default_pool",
    )

    low_priority = BashOperator(
        task_id="low_priority",
        bash_command='echo "Low priority task (weight=1)"',
        priority_weight=1,
        pool="default_pool",
    )

    # Downstream task waits for all
    summary = BashOperator(
        task_id="summary",
        bash_command='echo "All prioritized tasks completed"',
    )

    [high_priority, medium_priority, low_priority] >> summary

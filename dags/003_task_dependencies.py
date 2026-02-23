"""DAG 03: Task Dependencies.

Demonstrates the various ways to set task dependencies in Airflow:
``>>`` and ``<<`` operators, ``chain()``, ``cross_downstream()``,
and fan-in / fan-out patterns.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain, cross_downstream

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="003_task_dependencies",
    default_args=DEFAULT_ARGS,
    description="Task dependency patterns: >>, chain, cross_downstream",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # Linear chain with >> operator
    start = BashOperator(task_id="start", bash_command='echo "Starting pipeline"')
    middle = BashOperator(task_id="middle", bash_command='echo "Middle step"')
    end = BashOperator(task_id="end", bash_command='echo "End step"')

    start >> middle >> end

    # Fan-out: one task triggers multiple downstream tasks
    fan_out_source = BashOperator(task_id="fan_out_source", bash_command='echo "Fan-out source"')
    fan_out_a = BashOperator(task_id="fan_out_a", bash_command='echo "Branch A"')
    fan_out_b = BashOperator(task_id="fan_out_b", bash_command='echo "Branch B"')
    fan_out_c = BashOperator(task_id="fan_out_c", bash_command='echo "Branch C"')

    fan_out_source >> [fan_out_a, fan_out_b, fan_out_c]

    # Fan-in: multiple tasks converge into one
    fan_in_sink = BashOperator(task_id="fan_in_sink", bash_command='echo "All branches complete"')
    [fan_out_a, fan_out_b, fan_out_c] >> fan_in_sink

    # chain() for linear sequences
    chain_a = BashOperator(task_id="chain_a", bash_command='echo "Chain A"')
    chain_b = BashOperator(task_id="chain_b", bash_command='echo "Chain B"')
    chain_c = BashOperator(task_id="chain_c", bash_command='echo "Chain C"')

    chain(chain_a, chain_b, chain_c)

    # cross_downstream: every task in first list depends on every task in second
    cross_a = BashOperator(task_id="cross_a", bash_command='echo "Cross A"')
    cross_b = BashOperator(task_id="cross_b", bash_command='echo "Cross B"')
    cross_x = BashOperator(task_id="cross_x", bash_command='echo "Cross X"')
    cross_y = BashOperator(task_id="cross_y", bash_command='echo "Cross Y"')

    cross_downstream([cross_a, cross_b], [cross_x, cross_y])

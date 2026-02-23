"""DAG 01: Hello World.

The simplest possible DAG. Demonstrates the minimal structure needed
to define a DAG in Airflow 3.x: imports, DAG context manager,
and a single BashOperator task with ``schedule=None`` (manual trigger).
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="001_hello_world",
    default_args=DEFAULT_ARGS,
    description="Minimal hello world DAG",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow!"',
    )

    date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    hello >> date

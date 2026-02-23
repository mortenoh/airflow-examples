"""DAG 02: PythonOperator.

Shows how to run Python callables as Airflow tasks using
PythonOperator. Demonstrates ``op_args``, ``op_kwargs``,
and return values (which are automatically pushed to XCom).
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def greet(name: str, greeting: str = "Hello") -> str:
    """Print a greeting and return it."""
    msg = f"[{timestamp()}] {greeting}, {name}!"
    print(msg)
    return msg


def compute_sum(a: int, b: int) -> int:
    """Compute and print the sum of two numbers."""
    result = a + b
    print(f"[{timestamp()}] {a} + {b} = {result}")
    return result


with DAG(
    dag_id="002_python_operator",
    default_args=DEFAULT_ARGS,
    description="PythonOperator with args and kwargs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    greet_task = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_args=["Airflow"],
        op_kwargs={"greeting": "Welcome"},
    )

    sum_task = PythonOperator(
        task_id="compute_sum",
        python_callable=compute_sum,
        op_args=[10, 32],
    )

    greet_task >> sum_task

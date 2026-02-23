"""DAG 05: XComs.

Demonstrates manual XCom push/pull for sharing data between tasks.
Uses ``ti.xcom_push()`` and ``ti.xcom_pull()`` for explicit control
over cross-task communication.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def push_values(**context: object) -> str:
    """Push multiple values to XCom."""
    ti = context["ti"]  # type: ignore[index]
    ti.xcom_push(key="greeting", value="Hello from XCom!")
    ti.xcom_push(key="numbers", value=[10, 20, 30])
    msg = "Pushed via return value"
    print(f"[{timestamp()}] Pushed greeting and numbers to XCom")
    return msg


def pull_values(**context: object) -> None:
    """Pull values from XCom and display them."""
    ti = context["ti"]  # type: ignore[index]
    greeting = ti.xcom_pull(task_ids="push_task", key="greeting")
    numbers = ti.xcom_pull(task_ids="push_task", key="numbers")
    return_value = ti.xcom_pull(task_ids="push_task")

    print(f"[{timestamp()}] greeting     = {greeting}")
    print(f"[{timestamp()}] numbers      = {numbers}")
    print(f"[{timestamp()}] return_value = {return_value}")


with DAG(
    dag_id="005_xcoms",
    default_args=DEFAULT_ARGS,
    description="Manual XCom push and pull between tasks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_values,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_values,
    )

    push_task >> pull_task

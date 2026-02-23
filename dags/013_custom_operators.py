"""DAG 13: Custom Operators.

Demonstrates building custom operators by subclassing ``BaseOperator``
from ``airflow.sdk``. Uses ``PrintOperator`` and ``SquareOperator``
defined in ``airflow_examples.operators``.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.operators import PrintOperator, SquareOperator


def show_result(**context: object) -> None:
    """Pull and display the squared result from XCom."""
    ti = context["ti"]  # type: ignore[index]
    result = ti.xcom_pull(task_ids="square_5")
    print(f"[{timestamp()}] Square result from XCom: {result}")


with DAG(
    dag_id="013_custom_operators",
    default_args=DEFAULT_ARGS,
    description="Custom operators: PrintOperator and SquareOperator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    greet = PrintOperator(
        task_id="greet",
        message="Hello from a custom operator!",
    )

    square_5 = SquareOperator(
        task_id="square_5",
        number=5,
    )

    square_12 = SquareOperator(
        task_id="square_12",
        number=12,
    )

    show = PythonOperator(
        task_id="show_result",
        python_callable=show_result,
    )

    greet >> [square_5, square_12] >> show

"""Exercise 01: Your First DAG.

Goal: Create a DAG with 3 PythonOperator tasks and set their dependencies.

Instructions:
1. Create a DAG with dag_id="ex01_first_dag"
2. Create three PythonOperator tasks: extract, transform, load
3. Set dependencies so they run in order: extract >> transform >> load

Each function is provided -- you just need to wire them into a DAG.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def extract() -> None:
    """Simulate data extraction."""
    print("Extracting data from source...")


def transform() -> None:
    """Simulate data transformation."""
    print("Transforming data...")


def load() -> None:
    """Simulate data loading."""
    print("Loading data into destination...")


# TODO: Create a DAG using the `with DAG(...)` context manager.
# - dag_id: "ex01_first_dag"
# - start_date: datetime(2024, 1, 1)
# - schedule: None
# - catchup: False
# - tags: ["exercise"]

# TODO: Inside the DAG context, create three PythonOperator tasks:
# - task_id="extract", python_callable=extract
# - task_id="transform", python_callable=transform
# - task_id="load", python_callable=load

# TODO: Set dependencies: extract >> transform >> load


# SOLUTION (uncomment to check your work):
# -----------------------------------------------------------------------
# with DAG(
#     dag_id="ex01_first_dag",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise"],
# ) as dag:
#     t_extract = PythonOperator(task_id="extract", python_callable=extract)
#     t_transform = PythonOperator(task_id="transform", python_callable=transform)
#     t_load = PythonOperator(task_id="load", python_callable=load)
#     t_extract >> t_transform >> t_load

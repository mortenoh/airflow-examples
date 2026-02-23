"""Exercise 02: Convert to TaskFlow API.

Goal: Rewrite a PythonOperator DAG using the @task decorator (TaskFlow API).

Instructions:
1. Convert each PythonOperator function into a @task-decorated function
2. Tasks should pass data via return values (not XCom push/pull)
3. Wire the tasks by calling them as functions inside the DAG context

The PythonOperator version is shown below. Your job is to rewrite it
using @task decorators so data flows naturally via return values.
"""

from datetime import datetime

from airflow.sdk import DAG, task

# Original PythonOperator version (for reference):
# -------------------------------------------------
# def extract_data(**kwargs):
#     data = {"values": [1, 2, 3, 4, 5]}
#     kwargs["ti"].xcom_push(key="raw_data", value=data)
#
# def transform_data(**kwargs):
#     raw = kwargs["ti"].xcom_pull(key="raw_data", task_ids="extract")
#     transformed = {"values": [v * 2 for v in raw["values"]]}
#     kwargs["ti"].xcom_push(key="transformed_data", value=transformed)
#
# def load_data(**kwargs):
#     data = kwargs["ti"].xcom_pull(key="transformed_data", task_ids="transform")
#     print(f"Loading {len(data['values'])} records")


# TODO: Create a @task function called `extract` that returns a dict
# with key "values" containing [1, 2, 3, 4, 5]

# TODO: Create a @task function called `transform` that takes a dict parameter,
# doubles each value, and returns the result

# TODO: Create a @task function called `load` that takes a dict parameter
# and prints how many records it received

# TODO: Create a DAG with dag_id="ex02_taskflow" and wire the tasks
# by calling them as functions: load(transform(extract()))


# SOLUTION (uncomment to check your work):
# -----------------------------------------------------------------------
# @task
# def extract() -> dict[str, list[int]]:
#     """Extract raw data."""
#     data = {"values": [1, 2, 3, 4, 5]}
#     print(f"Extracted {len(data['values'])} values")
#     return data
#
#
# @task
# def transform(raw: dict[str, list[int]]) -> dict[str, list[int]]:
#     """Double each value."""
#     transformed = {"values": [v * 2 for v in raw["values"]]}
#     print(f"Transformed: {transformed['values']}")
#     return transformed
#
#
# @task
# def load(data: dict[str, list[int]]) -> None:
#     """Load the final data."""
#     print(f"Loading {len(data['values'])} records: {data['values']}")
#
#
# with DAG(
#     dag_id="ex02_taskflow",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise"],
# ) as dag:
#     raw = extract()
#     transformed = transform(raw)
#     load(transformed)

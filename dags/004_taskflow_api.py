"""DAG 04: TaskFlow API.

Demonstrates the ``@task`` decorator (TaskFlow API) which lets you
write DAG tasks as regular Python functions. Return values are
automatically passed between tasks via XCom.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


@task
def extract() -> dict[str, list[int]]:
    """Simulate extracting data from a source."""
    data = {"values": [1, 2, 3, 4, 5]}
    print(f"[{timestamp()}] Extracted: {data}")
    return data


@task
def transform(data: dict[str, list[int]]) -> dict[str, list[int]]:
    """Double each value in the dataset."""
    transformed = {"values": [v * 2 for v in data["values"]]}
    print(f"[{timestamp()}] Transformed: {transformed}")
    return transformed


@task
def load(data: dict[str, list[int]]) -> None:
    """Simulate loading data to a destination."""
    print(f"[{timestamp()}] Loaded {len(data['values'])} values: {data['values']}")


with DAG(
    dag_id="004_taskflow_api",
    default_args=DEFAULT_ARGS,
    description="TaskFlow API with @task decorator and automatic XCom",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    raw = extract()
    processed = transform(raw)
    load(processed)

"""DAG 48: Virtual Environment Tasks.

Demonstrates ``@task.virtualenv`` for running tasks in isolated Python
virtual environments, and ``PythonVirtualenvOperator`` (the classic
operator form). Each task gets its own virtualenv with specified
packages, ensuring dependency isolation between tasks.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS


# --- @task.virtualenv: decorated form --------------------------------------
# The function runs in a fresh virtualenv with the specified packages.
# Note: the function cannot access imports from the DAG file -- it must
# be self-contained.
@task.virtualenv(requirements=["httpx>=0.28"])
def fetch_url() -> str:
    """Fetch a URL using httpx (installed in a temporary virtualenv)."""
    import httpx  # noqa: I001

    resp = httpx.get("https://httpbin.org/get", timeout=10)
    print(f"Status: {resp.status_code}")
    print(f"Server: {resp.headers.get('Server', 'unknown')}")
    return f"status={resp.status_code}"


@task.virtualenv(requirements=["pyyaml>=6.0"])
def parse_yaml() -> dict:  # type: ignore[type-arg]
    """Parse YAML data using pyyaml (installed in a temporary virtualenv)."""
    import yaml

    data = yaml.safe_load("""
        pipeline:
          name: weather_etl
          stages:
            - extract
            - transform
            - load
          config:
            retries: 3
            timeout: 300
    """)
    print(f"Pipeline: {data['pipeline']['name']}")
    print(f"Stages: {data['pipeline']['stages']}")
    return data


# --- PythonVirtualenvOperator: classic operator form ------------------------
def compute_statistics() -> str:
    """Compute statistics using numpy in an isolated virtualenv.

    This function is self-contained -- no external imports from the DAG.
    """
    import numpy as np

    data = np.array([12.5, 13.1, 9.8, 3.2, 11.0, 15.7, 8.3, 14.2])
    print(f"Data: {data}")
    print(f"Mean:   {np.mean(data):.2f}")
    print(f"Std:    {np.std(data):.2f}")
    print(f"Min:    {np.min(data):.2f}")
    print(f"Max:    {np.max(data):.2f}")
    print(f"Median: {np.median(data):.2f}")
    return f"mean={np.mean(data):.2f}"


@task
def report(fetch_result: str, yaml_result: dict, stats_result: str) -> None:  # type: ignore[type-arg]
    """Combine results from all virtualenv tasks."""
    print("=== Virtual Environment Task Results ===")
    print(f"  Fetch:  {fetch_result}")
    print(f"  YAML:   {yaml_result.get('pipeline', {}).get('name', 'N/A')}")
    print(f"  Stats:  {stats_result}")


with DAG(
    dag_id="048_virtualenv_tasks",
    default_args=DEFAULT_ARGS,
    description="@task.virtualenv and PythonVirtualenvOperator for isolated envs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "taskflow"],
) as dag:
    fetch = fetch_url()
    yaml_data = parse_yaml()

    stats = PythonVirtualenvOperator(
        task_id="compute_statistics",
        python_callable=compute_statistics,
        requirements=["numpy>=1.26"],
    )

    report(fetch_result=fetch, yaml_result=yaml_data, stats_result=stats.output)

"""DAG 78: Pipeline with Mockable External Calls.

Demonstrates a pipeline that fetches from an API via a helper function
that tests can mock with ``unittest.mock.patch``. Shows how to
structure API dependencies for testability.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "mock_api")
API_URL = "http://httpbin:8080/get"


@task
def fetch_from_api() -> dict[str, object]:
    """Fetch data from API using mockable helper function."""
    from airflow_examples.etl_logic import fetch_api_data

    response = fetch_api_data(API_URL, timeout=10)
    print(f"[{timestamp()}] API response keys: {list(response.keys())}")
    return response


@task
def parse_response(response: dict[str, object]) -> dict[str, object]:
    """Extract useful fields from API response."""
    parsed = {
        "url": response.get("url", ""),
        "origin": response.get("origin", "unknown"),
        "headers": response.get("headers", {}),
        "timestamp": datetime.now().isoformat(),
    }
    print(f"[{timestamp()}] Parsed response:")
    print(f"  URL: {parsed['url']}")
    print(f"  Origin: {parsed['origin']}")
    return parsed


@task
def write_output(parsed: dict[str, object]) -> str:
    """Write parsed data to CSV."""
    import csv

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = f"{OUTPUT_DIR}/api_response.csv"

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["field", "value"])
        for key, value in parsed.items():
            writer.writerow([key, str(value)])

    print(f"[{timestamp()}] Wrote API data to {output_path}")
    return output_path


@task
def report(output_path: str) -> None:
    """Print summary."""
    print(f"[{timestamp()}] === Mock API Pipeline Complete ===")
    print(f"  Output: {output_path}")
    print("\n  Testing pattern:")
    print("    mock.patch('airflow_examples.etl_logic.requests.get')")
    print("    -> replaces real HTTP call with controlled response")
    print("    -> no network dependency in tests")


with DAG(
    dag_id="078_mock_api_pipeline",
    default_args=DEFAULT_ARGS,
    description="API pipeline with mockable fetch_api_data() for testing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "testing"],
) as dag:
    response = fetch_from_api()
    parsed = parse_response(response=response)
    output = write_output(parsed=parsed)
    r = report(output_path=output)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

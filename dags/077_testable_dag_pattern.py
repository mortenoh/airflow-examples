"""DAG 77: Structuring DAGs for Testability.

Demonstrates how to separate business logic from DAG wiring by
keeping all logic in ``etl_logic.py`` and keeping the DAG file thin.
Each task simply calls a pure function, making the logic independently
testable without Airflow.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "testable_dag")


@task
def extract() -> list[dict[str, object]]:
    """Extract weather data using pure business logic."""
    from airflow_examples.etl_logic import extract_weather_data

    records = extract_weather_data(n_records=50, seed=42)
    print(f"[{timestamp()}] Extracted {len(records)} records")
    print(f"[{timestamp()}] Fields: {list(records[0].keys())}")
    return records


@task
def validate(records: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    """Validate records using pure business logic."""
    from airflow_examples.etl_logic import validate_records

    required = ["station", "date", "temperature_c", "humidity_pct"]
    valid, invalid = validate_records(records, required)
    print(f"[{timestamp()}] Validation: {len(valid)} valid, {len(invalid)} invalid")
    return {"valid": valid, "invalid": invalid}


@task
def transform(validated: dict[str, list[dict[str, object]]]) -> list[dict[str, object]]:
    """Transform records using pure business logic."""
    from airflow_examples.etl_logic import transform_records

    transformed = transform_records(validated["valid"])
    print(f"[{timestamp()}] Transformed {len(transformed)} records")
    if transformed:
        print(f"[{timestamp()}] New fields: temperature_f, wind_category")
    return transformed


@task
def load(records: list[dict[str, object]]) -> dict[str, object]:
    """Load records to CSV using pure business logic."""
    import os

    from airflow_examples.etl_logic import load_records

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    result = load_records(records, f"{OUTPUT_DIR}/weather_output.csv")
    print(f"[{timestamp()}] Loaded: {result}")
    return result


@task
def report(load_result: dict[str, object]) -> None:
    """Print pipeline summary."""
    print(f"[{timestamp()}] === Testable DAG Pattern Complete ===")
    print(f"  Rows written: {load_result['rows']}")
    print(f"  Columns: {load_result['columns']}")
    print(f"  Output: {load_result['path']}")
    print("\n  Key pattern: all logic lives in etl_logic.py")
    print("  DAG file is thin wiring only -- testable without Airflow")


with DAG(
    dag_id="077_testable_dag_pattern",
    default_args=DEFAULT_ARGS,
    description="Thin DAG wiring with testable pure logic in etl_logic.py",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "testing"],
) as dag:
    raw = extract()
    validated = validate(records=raw)
    transformed = transform(validated=validated)
    loaded = load(records=transformed)
    r = report(load_result=loaded)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

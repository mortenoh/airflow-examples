"""DAG 10: Dynamic Task Mapping.

Demonstrates dynamic task creation at runtime using ``.expand()``
and ``.partial()``. Instead of defining a fixed number of tasks,
Airflow maps over a list to create tasks dynamically.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


@task
def generate_files() -> list[str]:
    """Generate a list of files to process."""
    files = ["data_2024_01.csv", "data_2024_02.csv", "data_2024_03.csv", "data_2024_04.csv"]
    print(f"[{timestamp()}] Generated {len(files)} files to process")
    return files


@task
def process_file(filename: str) -> dict[str, object]:
    """Process a single file (simulated)."""
    rows = len(filename) * 100  # Fake row count
    print(f"[{timestamp()}] Processing {filename}: {rows} rows")
    return {"file": filename, "rows": rows}


@task
def summarize(results: list[dict[str, object]]) -> None:
    """Summarize all processed files."""
    total = sum(r["rows"] for r in results)  # type: ignore[union-attr]
    print(f"[{timestamp()}] Processed {len(results)} files, {total} total rows")
    for r in results:
        print(f"  {r['file']}: {r['rows']} rows")


with DAG(
    dag_id="010_dynamic_tasks",
    default_args=DEFAULT_ARGS,
    description="Dynamic task mapping with .expand() and .partial()",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    files = generate_files()
    processed = process_file.expand(filename=files)
    summarize(processed)

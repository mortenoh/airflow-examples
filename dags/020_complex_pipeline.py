"""DAG 20: Complex Pipeline.

A multi-stage ETL pipeline combining many Airflow concepts:
TaskGroups for organization, dynamic task mapping, callbacks,
branching, and dependency management. Simulates a realistic
extract -> transform -> load -> notify workflow.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

SOURCES = ["users_api", "orders_db", "products_csv"]


@task
def extract_source(source: str) -> dict[str, object]:
    """Extract data from a single source."""
    rows = hash(source) % 1000 + 500
    print(f"[{timestamp()}] Extracted {rows} rows from {source}")
    return {"source": source, "rows": abs(rows)}


@task
def validate(data: dict[str, object]) -> dict[str, object]:
    """Validate extracted data."""
    print(f"[{timestamp()}] Validating {data['source']}: {data['rows']} rows")
    data["valid"] = True
    return data


@task
def transform(data: dict[str, object]) -> dict[str, object]:
    """Apply transformations to validated data."""
    print(f"[{timestamp()}] Transforming {data['source']}")
    data["transformed"] = True
    return data


@task
def load(results: list[dict[str, object]]) -> int:
    """Load all transformed data into the warehouse."""
    total = sum(r["rows"] for r in results)  # type: ignore[union-attr]
    print(f"[{timestamp()}] Loading {len(results)} sources ({total} total rows) to warehouse")
    for r in results:
        print(f"  {r['source']}: {r['rows']} rows")
    return total  # type: ignore[return-value]


def notify_success(**context: object) -> None:
    """Send success notification."""
    ti = context["ti"]  # type: ignore[index]
    total = ti.xcom_pull(task_ids="load_group.load")
    print(f"[{timestamp()}] Pipeline complete! Loaded {total} total rows")


def generate_report() -> None:
    """Generate pipeline execution report."""
    print(f"[{timestamp()}] Generating execution report...")
    print(f"[{timestamp()}] Report saved to /tmp/pipeline_report.txt")


with DAG(
    dag_id="020_complex_pipeline",
    default_args=DEFAULT_ARGS,
    description="Multi-stage ETL: extract -> validate -> transform -> load -> notify",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "pipeline"],
) as dag:
    start = BashOperator(task_id="start", bash_command='echo "Pipeline starting"')

    # Extract stage: dynamic task mapping over sources
    with TaskGroup("extract_group") as extract_group:
        extracted = extract_source.expand(source=SOURCES)

    # Transform stage: validate then transform each dataset
    with TaskGroup("transform_group") as transform_group:
        validated = validate.expand(data=extracted)
        transformed = transform.expand(data=validated)

    # Load stage: aggregate and load
    with TaskGroup("load_group") as load_group:
        loaded = load(transformed)

    # Notify stage
    with TaskGroup("notify_group") as notify_group:
        notify = PythonOperator(
            task_id="notify",
            python_callable=notify_success,
        )
        report = PythonOperator(
            task_id="report",
            python_callable=generate_report,
        )

    end = BashOperator(task_id="end", bash_command='echo "Pipeline complete"')

    start >> extract_group >> transform_group >> load_group >> notify_group >> end

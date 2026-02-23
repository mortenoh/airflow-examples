"""DAG 19: Setup and Teardown.

Demonstrates ``@setup`` and ``@teardown`` decorators from Airflow 3.x
for resource lifecycle management. Setup tasks provision resources
before work tasks, and teardown tasks clean up afterwards -- even if
work tasks fail. The ``>>`` operator with ``.as_teardown()`` links
the lifecycle together.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


@task
def create_database() -> str:
    """Provision a temporary database (setup task)."""
    db_name = "tmp_analytics_db"
    print(f"[{timestamp()}] SETUP: Created database '{db_name}'")
    return db_name


@task
def load_data(db_name: str) -> int:
    """Load data into the provisioned database."""
    rows = 500
    print(f"[{timestamp()}] WORK: Loading {rows} rows into '{db_name}'")
    return rows


@task
def run_analysis(db_name: str, row_count: int) -> str:
    """Run analysis on loaded data."""
    print(f"[{timestamp()}] WORK: Analyzing {row_count} rows in '{db_name}'")
    result = f"mean=12.5,std=3.2,count={row_count}"
    print(f"[{timestamp()}] WORK: Result: {result}")
    return result


@task
def drop_database(db_name: str) -> None:
    """Drop the temporary database (teardown task).

    This runs even if work tasks fail, ensuring cleanup always happens.
    """
    print(f"[{timestamp()}] TEARDOWN: Dropping database '{db_name}'")
    print(f"[{timestamp()}] TEARDOWN: Cleanup complete")


with DAG(
    dag_id="019_setup_teardown",
    default_args=DEFAULT_ARGS,
    description="@setup / @teardown for resource lifecycle management",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # Setup: provision the database
    db = create_database()

    # Work: load and analyze
    rows = load_data(db_name=db)
    analysis = run_analysis(db_name=db, row_count=rows)

    # Teardown: drop the database -- linked via as_teardown()
    # as_teardown(setups=db) ensures drop runs even if work tasks fail,
    # and tells Airflow about the setup-teardown relationship.
    cleanup = drop_database(db_name=db)

    db >> rows >> analysis >> cleanup.as_teardown(setups=db)

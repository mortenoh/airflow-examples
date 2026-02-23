"""DAG 102: dbt Transform Orchestration.

Runs dbt models and tests against the raw data loaded by DAG 101.
Demonstrates Airflow + dbt orchestration using BashOperator.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

DBT_DIR = "/opt/airflow/dbt_project"


@task
def check_raw_tables() -> dict[str, int]:
    """Verify raw tables exist before running dbt."""
    from sqlalchemy import create_engine, text

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    with engine.connect() as conn:
        countries = conn.execute(text("SELECT count(*) FROM raw_countries")).scalar() or 0
        indicators = conn.execute(text("SELECT count(*) FROM raw_indicators")).scalar() or 0

    print(f"[{timestamp()}] Raw tables: {countries} countries, {indicators} indicators")
    if countries == 0:
        raise ValueError("raw_countries is empty -- run DAG 101 first")
    return {"countries": countries, "indicators": indicators}


dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
)


@task
def report() -> None:
    """Print dbt transformation summary."""
    from sqlalchemy import create_engine, text

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    with engine.connect() as conn:
        tables = ["dim_country", "fct_indicators", "agg_nordic_summary"]
        print(f"\n[{timestamp()}] === dbt Transform Complete ===")
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()  # noqa: S608
                print(f"  {table}: {count} rows")
            except Exception:
                print(f"  {table}: not found")


with DAG(
    dag_id="102_dbt_transform",
    default_args=DEFAULT_ARGS,
    description="Run dbt models and tests on Airflow-loaded data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dbt"],
) as dag:
    raw = check_raw_tables()
    raw >> dbt_run >> dbt_test >> report()

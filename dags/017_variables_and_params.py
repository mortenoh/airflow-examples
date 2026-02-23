"""DAG 17: Variables and Params.

Demonstrates Airflow Variables for global key-value storage and
DAG-level Params for per-run configuration with type validation.
Variables persist across runs; Params are set at trigger time.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def use_params(**context: object) -> None:
    """Read and display DAG params."""
    params = context.get("params", {})  # type: ignore[union-attr]
    print(f"[{timestamp()}] DAG Params:")
    print(f"  environment = {params.get('environment', 'N/A')}")
    print(f"  batch_size  = {params.get('batch_size', 'N/A')}")
    print(f"  dry_run     = {params.get('dry_run', 'N/A')}")


def use_variables() -> None:
    """Demonstrate Variable get/set (with fallback for test mode)."""
    print(f"[{timestamp()}] Variable demonstration:")
    try:
        from airflow.models import Variable

        Variable.set("demo_key", "demo_value")
        value = Variable.get("demo_key", default_var="fallback")
        print(f"  demo_key = {value}")

        # Variables can store JSON
        Variable.set("demo_config", '{"retries": 3, "timeout": 60}')
        config = Variable.get("demo_config", deserialize_json=True)
        print(f"  demo_config = {config}")
    except Exception as e:
        print(f"  Variable access unavailable in test mode: {e}")


with DAG(
    dag_id="017_variables_and_params",
    default_args=DEFAULT_ARGS,
    description="Variables for global config, Params for per-run input",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
    params={
        "environment": "staging",
        "batch_size": 500,
        "dry_run": True,
    },
) as dag:
    params_task = PythonOperator(
        task_id="use_params",
        python_callable=use_params,
    )

    vars_task = PythonOperator(
        task_id="use_variables",
        python_callable=use_variables,
    )

    params_task >> vars_task

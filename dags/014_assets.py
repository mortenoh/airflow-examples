"""DAG 14: Assets (formerly Datasets).

Demonstrates data-aware scheduling with ``Asset`` (renamed from
``Dataset`` in Airflow 3.x). A producer DAG creates/updates an asset,
and a consumer DAG is automatically triggered when that asset updates.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from airflow_examples.config import DEFAULT_ARGS, timestamp

# Define a shared asset (data dependency)
weather_data = Asset("s3://example-bucket/weather_data.parquet")


def produce_data() -> None:
    """Simulate producing weather data."""
    print(f"[{timestamp()}] Producing weather data...")
    print(f"[{timestamp()}] Wrote 1000 records to weather_data.parquet")


def consume_data() -> None:
    """Simulate consuming weather data."""
    print(f"[{timestamp()}] Consuming weather data...")
    print(f"[{timestamp()}] Read 1000 records from weather_data.parquet")
    print(f"[{timestamp()}] Processing complete")


# Producer DAG: creates the asset
with DAG(
    dag_id="014_assets_producer",
    default_args=DEFAULT_ARGS,
    description="Asset producer: creates weather data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "assets"],
) as producer_dag:
    produce = PythonOperator(
        task_id="produce",
        python_callable=produce_data,
        outlets=[weather_data],
    )


# Consumer DAG: triggered when the asset is updated
with DAG(
    dag_id="014_assets_consumer",
    default_args=DEFAULT_ARGS,
    description="Asset consumer: triggered by weather data updates",
    start_date=datetime(2024, 1, 1),
    schedule=[weather_data],
    catchup=False,
    tags=["example", "assets"],
) as consumer_dag:
    consume = PythonOperator(
        task_id="consume",
        python_callable=consume_data,
    )

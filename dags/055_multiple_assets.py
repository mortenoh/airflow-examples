"""DAG 55: Multiple Asset Dependencies.

Demonstrates advanced ``Asset`` (formerly Dataset) patterns: a consumer
DAG that depends on multiple assets (AND logic -- all must update), a
producer that updates multiple assets, and asset URI patterns. Builds
on the single-asset pattern from DAG 14.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

from airflow_examples.config import DEFAULT_ARGS, timestamp

# Define multiple assets (data dependencies)
temperature_data = Asset("s3://weather-bucket/temperature.parquet")
humidity_data = Asset("s3://weather-bucket/humidity.parquet")
pressure_data = Asset("s3://weather-bucket/pressure.parquet")


# --- Producer 1: updates temperature data -----------------------------------
def produce_temperature() -> None:
    """Simulate producing temperature data."""
    print(f"[{timestamp()}] Writing temperature data...")
    print(f"[{timestamp()}] Wrote 500 temperature records")


def produce_humidity() -> None:
    """Simulate producing humidity data."""
    print(f"[{timestamp()}] Writing humidity data...")
    print(f"[{timestamp()}] Wrote 500 humidity records")


def produce_all_weather() -> None:
    """Simulate producing all weather variables at once."""
    print(f"[{timestamp()}] Writing temperature + humidity + pressure data...")
    print(f"[{timestamp()}] Wrote 1500 records (3 variables x 500 rows)")


def consume_temp_and_humidity() -> None:
    """Consume data when BOTH temperature and humidity are updated."""
    print(f"[{timestamp()}] Both temperature AND humidity data are ready")
    print(f"[{timestamp()}] Running correlation analysis...")
    print(f"[{timestamp()}] Correlation: r=0.72")


def consume_all_weather() -> None:
    """Consume data when ALL three weather variables are updated."""
    print(f"[{timestamp()}] All weather data ready (temp + humidity + pressure)")
    print(f"[{timestamp()}] Running comprehensive analysis...")
    print(f"[{timestamp()}] Generating forecast model input")


# --- Producer DAG A: individual variable producers --------------------------
with DAG(
    dag_id="055_assets_temp_producer",
    default_args=DEFAULT_ARGS,
    description="Asset producer: temperature data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "assets"],
) as temp_dag:
    PythonOperator(
        task_id="produce_temperature",
        python_callable=produce_temperature,
        outlets=[temperature_data],
    )

with DAG(
    dag_id="055_assets_humidity_producer",
    default_args=DEFAULT_ARGS,
    description="Asset producer: humidity data",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "assets"],
) as humidity_dag:
    PythonOperator(
        task_id="produce_humidity",
        python_callable=produce_humidity,
        outlets=[humidity_data],
    )


# --- Producer DAG B: multi-outlet producer ----------------------------------
# A single task can update multiple assets via the outlets parameter.
with DAG(
    dag_id="055_assets_bulk_producer",
    default_args=DEFAULT_ARGS,
    description="Asset producer: all weather variables (multi-outlet)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "assets"],
) as bulk_dag:
    PythonOperator(
        task_id="produce_all",
        python_callable=produce_all_weather,
        outlets=[temperature_data, humidity_data, pressure_data],
    )


# --- Consumer DAG A: AND logic (both must update) ---------------------------
# schedule=[asset1, asset2] means the DAG runs when ALL assets are updated.
with DAG(
    dag_id="055_assets_dual_consumer",
    default_args=DEFAULT_ARGS,
    description="Asset consumer: triggered when temp AND humidity update",
    start_date=datetime(2024, 1, 1),
    schedule=[temperature_data, humidity_data],
    catchup=False,
    tags=["example", "assets"],
) as dual_consumer_dag:
    PythonOperator(
        task_id="analyze_correlation",
        python_callable=consume_temp_and_humidity,
    )


# --- Consumer DAG B: all three assets --------------------------------------
with DAG(
    dag_id="055_assets_full_consumer",
    default_args=DEFAULT_ARGS,
    description="Asset consumer: triggered when ALL weather variables update",
    start_date=datetime(2024, 1, 1),
    schedule=[temperature_data, humidity_data, pressure_data],
    catchup=False,
    tags=["example", "assets"],
) as full_consumer_dag:
    PythonOperator(
        task_id="comprehensive_analysis",
        python_callable=consume_all_weather,
    )

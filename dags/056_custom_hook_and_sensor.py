"""DAG 56: Custom Hook, Custom Sensor, and Incremental Processing.

Demonstrates building custom components: a ``BaseHook`` subclass for
reusable connection logic, a ``BaseSensorOperator`` subclass that uses
the hook, and an incremental processing pattern using ``{{ ds }}`` to
process only data for the current execution date.
"""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

# Note: The custom hook and sensor are defined inline here for clarity.
# In production, they would live in src/airflow_examples/hooks.py and
# src/airflow_examples/sensors.py (or a plugin).

# --- Custom Hook -----------------------------------------------------------
# Hooks encapsulate connection logic. They are reusable across operators
# and sensors. This simulated hook "connects" to a weather API.
#
# from airflow.hooks.base import BaseHook
#
# class WeatherApiHook(BaseHook):
#     conn_name_attr = "weather_conn_id"
#     default_conn_name = "weather_default"
#     conn_type = "http"
#     hook_name = "Weather API"
#
#     def __init__(self, weather_conn_id="weather_default"):
#         super().__init__()
#         self.weather_conn_id = weather_conn_id
#
#     def get_conn(self):
#         conn = self.get_connection(self.weather_conn_id)
#         return {"host": conn.host, "port": conn.port, "token": conn.password}
#
#     def check_data_available(self, station_id, date):
#         # In production: call the actual API
#         return True
#
#     def fetch_data(self, station_id, date):
#         # In production: call the actual API
#         return [{"temp": 12.5, "humidity": 68.0}]


# --- Custom Sensor ---------------------------------------------------------
# Sensors wait for a condition. This simulated sensor checks if weather
# data is available for a given station and date.
#
# from airflow.sensors.base import BaseSensorOperator
#
# class WeatherDataSensor(BaseSensorOperator):
#     template_fields = ("station_id", "check_date")
#
#     def __init__(self, station_id, check_date, weather_conn_id="weather_default",
#                  **kwargs):
#         super().__init__(**kwargs)
#         self.station_id = station_id
#         self.check_date = check_date
#         self.weather_conn_id = weather_conn_id
#
#     def poke(self, context):
#         hook = WeatherApiHook(self.weather_conn_id)
#         available = hook.check_data_available(self.station_id, self.check_date)
#         self.log.info("Data for %s on %s: %s", self.station_id, self.check_date,
#                       "available" if available else "not yet")
#         return available


# --- Incremental processing pattern ----------------------------------------
# Use {{ ds }} (execution date) to process only new/changed data per run.
# This makes the pipeline idempotent: re-running for a date re-processes
# only that date's data.


def simulate_check_data(station_id: str, date: str) -> bool:
    """Simulate checking if data is available (custom sensor logic)."""
    print(f"[{timestamp()}] Checking data for station={station_id}, date={date}")
    print(f"[{timestamp()}] Data is available (simulated)")
    return True


def extract_incremental(station_id: str, date: str) -> dict[str, object]:
    """Extract only data for the given date (incremental pattern).

    In production, this would query: WHERE date = '{date}' AND station = '{station_id}'
    instead of extracting all historical data.
    """
    print(f"[{timestamp()}] EXTRACT: station={station_id}, date={date}")
    print(f"[{timestamp()}] SQL: SELECT * FROM raw_weather")
    print(f"[{timestamp()}]      WHERE date = '{date}' AND station = '{station_id}'")
    records = [
        {"station": station_id, "date": date, "temp": 12.5, "humidity": 68.0},
        {"station": station_id, "date": date, "temp": 13.1, "humidity": 65.0},
    ]
    print(f"[{timestamp()}] Extracted {len(records)} records for {date}")
    return {"records": records, "count": len(records)}


def transform_incremental(data: dict[str, object]) -> dict[str, object]:
    """Transform the incremental extract."""
    count = data["count"]
    print(f"[{timestamp()}] TRANSFORM: Processing {count} records")
    return {"transformed_count": count, "status": "ok"}


def load_incremental(data: dict[str, object], date: str) -> None:
    """Load transformed data with upsert (idempotent).

    In production, use INSERT ... ON CONFLICT (date, station) DO UPDATE
    to make the load idempotent (safe to re-run).
    """
    count = data["transformed_count"]
    print(f"[{timestamp()}] LOAD: Upserting {count} records for {date}")
    print(f"[{timestamp()}] SQL: INSERT INTO prod_weather (...) VALUES (...)")
    print(f"[{timestamp()}]      ON CONFLICT (date, station) DO UPDATE SET ...")
    print(f"[{timestamp()}] Load complete (idempotent upsert)")


with DAG(
    dag_id="056_custom_hook_and_sensor",
    default_args=DEFAULT_ARGS,
    description="Custom hook/sensor patterns and incremental processing with {{ ds }}",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "custom"],
    params={"station_id": "oslo_01"},
) as dag:
    # Step 1: Check if data is available (simulated custom sensor)
    check = PythonOperator(
        task_id="check_data_available",
        python_callable=simulate_check_data,
        op_kwargs={
            "station_id": "{{ params.station_id }}",
            "date": "{{ ds }}",
        },
    )

    # Step 2: Extract only this date's data (incremental)
    @task
    def extract(**context: object) -> dict[str, object]:
        """Extract data for the execution date only."""
        params = context["params"]  # type: ignore[index]
        ds = context["ds"]  # type: ignore[index]
        return extract_incremental(params["station_id"], str(ds))  # type: ignore[index]

    # Step 3: Transform
    @task
    def transform(data: dict[str, object]) -> dict[str, object]:
        """Transform extracted data."""
        return transform_incremental(data)

    # Step 4: Load (idempotent upsert)
    @task
    def load(data: dict[str, object], **context: object) -> None:
        """Load with idempotent upsert."""
        ds = context["ds"]  # type: ignore[index]
        load_incremental(data, str(ds))

    extracted = extract()
    transformed = transform(extracted)

    check >> extracted >> transformed >> load(transformed)

# Advanced Concepts

## Advanced Retries and Timeouts

### Exponential Backoff

Instead of fixed delays between retries, exponential backoff doubles the wait each time:

```python
from datetime import timedelta

task = PythonOperator(
    task_id="api_call",
    python_callable=call_api,
    retries=5,
    retry_delay=timedelta(seconds=2),
    retry_exponential_backoff=True,    # 2s, 4s, 8s, 16s, 30s
    max_retry_delay=timedelta(seconds=30),
)
```

### execution_timeout

Kills a task with SIGTERM if it exceeds the timeout. Use for tasks that might hang:

```python
task = PythonOperator(
    task_id="etl",
    python_callable=run_etl,
    execution_timeout=timedelta(minutes=30),
)
```

### DAG-Level Callbacks

Fire when the entire DAG run completes (not per-task):

```python
def dag_success(context):
    print(f"DAG {context['dag_run'].dag_id} succeeded!")

def dag_failure(context):
    print(f"DAG {context['dag_run'].dag_id} failed!")

with DAG(
    dag_id="my_dag",
    on_success_callback=dag_success,
    on_failure_callback=dag_failure,
):
    ...
```

See: `dags/51_advanced_retries.py`

---

## Scheduling Features

### depends_on_past

When `True`, a task instance will not run until the same task in the **previous** DAG run
has succeeded:

```python
extract = PythonOperator(
    task_id="extract",
    python_callable=extract_data,
    depends_on_past=True,  # Won't run until previous run's extract succeeded
)
```

### wait_for_downstream

Stricter than `depends_on_past`: waits for both the task itself AND all its downstream tasks
from the previous run to complete:

```python
transform = PythonOperator(
    task_id="transform",
    python_callable=transform_data,
    wait_for_downstream=True,
)
```

### max_active_runs

Limits how many DAG runs can execute simultaneously:

```python
with DAG(
    dag_id="sequential_pipeline",
    max_active_runs=1,  # Only one run at a time
    schedule="0 6 * * *",
):
    ...
```

### Cron Expressions

| Expression | Description |
|------------|-------------|
| `0 6 * * *` | Daily at 06:00 UTC |
| `*/15 * * * *` | Every 15 minutes |
| `0 0 1 * *` | First day of each month |
| `0 12 * * MON` | Monday at noon |
| `0 6 * * 1-5` | Weekdays at 06:00 |
| `@hourly` | Every hour (preset) |
| `@daily` | Every day at midnight (preset) |
| `timedelta(hours=2)` | Every 2 hours (timedelta) |

See: `dags/52_scheduling_features.py`

---

## Custom Timetables

For scheduling logic that can't be expressed with cron or timedelta, create a custom
`Timetable` subclass:

```python
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime

class WeekdayTimetable(Timetable):
    def next_dagrun_info(self, *, last_automated_dagrun, restriction):
        # Skip weekends, advance to next Monday
        ...
        return DagRunInfo.interval(start=start, end=end)

    def infer_manual_data_interval(self, *, run_after):
        # Handle manually triggered runs
        ...
```

Register via a plugin:

```python
class TimetablePlugin(AirflowPlugin):
    name = "custom_timetables"
    timetables = [WeekdayTimetable]
```

Use cases: business days only, market trading hours, skip holidays, fiscal calendar periods.

See: `dags/53_custom_timetable.py`

---

## Advanced Dynamic Task Mapping

### expand_kwargs (Zip-Style)

`expand_kwargs()` maps a list of dicts. Each dict provides keyword arguments for one mapped
task instance. This is the "zip" pattern -- parameters are paired together:

```python
@task
def generate_configs() -> list[dict[str, object]]:
    return [
        {"station_id": "oslo_01", "lat": 59.91, "lon": 10.75},
        {"station_id": "bergen_01", "lat": 60.39, "lon": 5.32},
    ]

@task
def process(station_id: str, lat: float, lon: float) -> dict:
    return {"station": station_id, "lat": lat, "lon": lon}

configs = generate_configs()
results = process.expand_kwargs(configs)
```

### Combined Expand Patterns

Use `.partial()` for static arguments and `.expand()` for dynamic ones:

```python
# Same variable for all instances, different dates
extract.partial(variable="temperature").expand(date=dates)

# Same date for all instances, different variables
extract.partial(date="2024-01-01").expand(variable=variables)
```

See: `dags/54_advanced_dynamic_mapping.py`

---

## Multiple Asset Dependencies

### Multi-Outlet Producers

A single task can update multiple assets:

```python
from airflow.sdk import Asset

temp_data = Asset("s3://bucket/temperature.parquet")
humidity_data = Asset("s3://bucket/humidity.parquet")

produce_all = PythonOperator(
    task_id="produce_all",
    python_callable=produce_weather,
    outlets=[temp_data, humidity_data],  # Updates both assets
)
```

### AND-Logic Consumer

A consumer DAG that waits for ALL specified assets to update:

```python
with DAG(
    dag_id="combined_analysis",
    schedule=[temp_data, humidity_data],  # Runs when BOTH update
):
    ...
```

See: `dags/55_multiple_assets.py`

---

## Custom Hooks and Sensors

### Custom Hook (BaseHook)

Hooks encapsulate reusable connection logic shared across operators and sensors:

```python
from airflow.hooks.base import BaseHook

class WeatherApiHook(BaseHook):
    conn_name_attr = "weather_conn_id"
    default_conn_name = "weather_default"
    conn_type = "http"
    hook_name = "Weather API"

    def __init__(self, weather_conn_id="weather_default"):
        super().__init__()
        self.weather_conn_id = weather_conn_id

    def get_conn(self):
        conn = self.get_connection(self.weather_conn_id)
        return {"host": conn.host, "token": conn.password}

    def check_data_available(self, station_id, date):
        # Call external API
        return True
```

### Custom Sensor (BaseSensorOperator)

Sensors wait for an external condition using a hook:

```python
from airflow.sensors.base import BaseSensorOperator

class WeatherDataSensor(BaseSensorOperator):
    template_fields = ("station_id", "check_date")

    def __init__(self, station_id, check_date, **kwargs):
        super().__init__(**kwargs)
        self.station_id = station_id
        self.check_date = check_date

    def poke(self, context):
        hook = WeatherApiHook()
        return hook.check_data_available(self.station_id, self.check_date)
```

See: `dags/56_custom_hook_and_sensor.py`, `src/airflow_examples/hooks.py`, `src/airflow_examples/sensors.py`

---

## Incremental Processing Pattern

Process only data for the current execution date, making the pipeline idempotent:

```python
@task
def extract(**context):
    date = context["ds"]  # "2024-01-15"
    # Only extract this date's data
    return db.query(f"SELECT * FROM raw WHERE date = '{date}'")

@task
def load(data, **context):
    date = context["ds"]
    # Idempotent upsert: safe to re-run
    db.execute("""
        INSERT INTO prod (...) VALUES (...)
        ON CONFLICT (date, station) DO UPDATE SET ...
    """)
```

Key principles:

- **Filter by `{{ ds }}`**: Each run processes only its own date partition
- **Idempotent loads**: Use `INSERT ... ON CONFLICT DO UPDATE` (upsert) or `TRUNCATE` + `INSERT`
- **Re-runnable**: Any failed run can be safely re-executed without duplicates
- **Backfill-friendly**: Works with `airflow dags backfill --start-date --end-date`

See: `dags/56_custom_hook_and_sensor.py`

# DAG Basics

## Your First DAG

A DAG file is just a Python file that Airflow reads. It needs three things:

1. **Imports** -- bring in Airflow's tools
2. **A DAG definition** -- give it a name and configure when it runs
3. **At least one task** -- the actual work

Here is the simplest possible DAG. It prints "Hello, Airflow!" to the logs:

```python
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="my_first_dag",       # Unique name, shows up in the web UI
    start_date=datetime(2024, 1, 1),
    schedule=None,               # Only runs when you click "Trigger" (no automatic schedule)
    catchup=False,               # Do not try to run for past dates
    tags=["example"],            # Labels for filtering in the UI
) as dag:
    task = BashOperator(
        task_id="hello",                          # Unique name within this DAG
        bash_command='echo "Hello, Airflow!"',    # The shell command to run
    )
```

Most examples in this project use the `@task` decorator instead of `BashOperator` -- it
lets you write plain Python functions and skip the operator boilerplate entirely. See
[DAG 04](dag-reference.md#04-taskflow-api) for the first example of that pattern.

## DAG Parameters Explained

| Parameter | Description | Common Values |
|-----------|-------------|---------------|
| `dag_id` | Unique identifier for the DAG | `"my_pipeline"` |
| `start_date` | Earliest date the DAG can be scheduled | `datetime(2024, 1, 1)` |
| `schedule` | How often the DAG runs | `None`, `"@daily"`, `"@hourly"`, `"0 6 * * *"` |
| `catchup` | Whether to backfill missed runs | `True` / `False` |
| `default_args` | Default arguments applied to all tasks | `{"retries": 1, "retry_delay": timedelta(minutes=5)}` |
| `tags` | Labels for filtering in the UI | `["etl", "production"]` |
| `params` | Per-run parameters with defaults | `{"env": "staging", "batch_size": 100}` |
| `max_active_runs` | Max concurrent DAG runs | `1`, `3`, `16` |
| `description` | Human-readable description shown in UI | `"Daily ETL pipeline"` |

## Task Dependencies (Who Runs Before Whom)

The `>>` operator tells Airflow the order. Think of it as "then":

```python
# "Run A, then B, then C"
a >> b >> c

# "Run A, then run B, C, and D in parallel"
a >> [b, c, d]

# "Wait for A, B, and C to all finish, then run D"
[a, b, c] >> d

# Same thing using chain() if you find it more readable
from airflow.sdk.bases.chain import chain
chain(a, b, c, d)
```

If you do not set any dependencies, all tasks run at the same time (in parallel).

---

## When Does a DAG Run?

Every DAG in this project uses `schedule=None` -- meaning they only run when you click
"Trigger" in the web UI (or when `make run` triggers them for you). In production, you
would set a schedule so Airflow runs your workflow automatically.

### Manual Trigger (schedule=None)

All 107 examples use this mode. The DAG sits idle until you trigger it:

```python
with DAG(
    dag_id="manual_dag",
    schedule=None,          # Never auto-scheduled
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    ...
```

**When to use:**

- Development and testing
- Event-driven workflows (triggered by external systems via API)
- One-off data migrations
- Ad-hoc analysis pipelines

**How to trigger:**

- Web UI: Click "Trigger DAG" button
- CLI: `airflow dags trigger manual_dag`
- API: `POST /api/v1/dags/manual_dag/dagRuns`
- Another DAG: `TriggerDagRunOperator`

### Scheduled Runs

Airflow supports cron expressions, presets, and timedelta schedules:

```python
# Cron expression: every day at 6 AM UTC
with DAG(dag_id="daily_6am", schedule="0 6 * * *", ...):
    ...

# Preset: once per day at midnight
with DAG(dag_id="daily_midnight", schedule="@daily", ...):
    ...

# Preset: once per hour
with DAG(dag_id="hourly", schedule="@hourly", ...):
    ...

# Timedelta: every 30 minutes
from datetime import timedelta
with DAG(dag_id="every_30min", schedule=timedelta(minutes=30), ...):
    ...
```

**Available presets:**

| Preset | Cron Equivalent | Description |
|--------|----------------|-------------|
| `@once` | -- | Run exactly once |
| `@hourly` | `0 * * * *` | Every hour |
| `@daily` | `0 0 * * *` | Every day at midnight |
| `@weekly` | `0 0 * * 0` | Every Sunday at midnight |
| `@monthly` | `0 0 1 * *` | First of every month |
| `@yearly` | `0 0 1 1 *` | January 1st |

### Catchup and Backfill

When `catchup=True` (default), Airflow creates DAG runs for every missed interval between `start_date` and now:

```python
# This would create ~365 DAG runs if deployed today with catchup=True
with DAG(
    dag_id="backfill_demo",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=True,           # Will backfill all missed dates
) as dag:
    ...
```

**Best practices:**

- Set `catchup=False` for most DAGs (especially during development)
- Use `catchup=True` intentionally when you need historical data processing
- Use `max_active_runs=1` to prevent overwhelming your infrastructure during backfills
- Use `airflow dags backfill` CLI for controlled historical reruns

### Data-Aware Scheduling with Assets

Airflow 3.x introduced `Asset` (formerly `Dataset`) for event-driven scheduling based on data dependencies:

```python
from airflow.sdk import Asset

weather_data = Asset("s3://bucket/weather.parquet")

# Producer: declares it creates the asset
with DAG(dag_id="producer", schedule="@daily", ...):
    produce = PythonOperator(
        task_id="produce",
        python_callable=create_weather_data,
        outlets=[weather_data],       # This DAG produces this asset
    )

# Consumer: triggered automatically when asset is updated
with DAG(dag_id="consumer", schedule=[weather_data], ...):
    consume = PythonOperator(
        task_id="consume",
        python_callable=process_weather_data,
    )
```

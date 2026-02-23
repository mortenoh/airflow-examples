"""DAG 107: Variable-driven scheduling -- change when a DAG runs without editing code.

Demonstrates reading the DAG schedule from an Airflow Variable at parse time.
Because the scheduler re-parses DAG files periodically, updating the Variable
(via the REST API or the Airflow UI) changes the effective schedule on the next
parse cycle -- no code change or deployment needed.

Key patterns:
    - ``Variable.get()`` at module level with ``default_var`` fallback
    - ``try/except`` guard so the file still imports when no Airflow DB is
      available (e.g. during ``pytest`` collection or ``ruff`` linting)
    - ``schedule=`` accepting a Variable-sourced string
    - ``Variable.set()`` inside a task to demonstrate programmatic updates
    - Printing REST API instructions so users know how to change the schedule
      without touching code

Builds on DAG 17 (Variables & Params) by showing Variables can control *when*
a DAG runs, not just *how* it runs.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.models import Variable
from airflow.sdk import DAG, task

from airflow_examples.apis import NORDIC_CITIES, OPEN_METEO_FORECAST, fetch_open_meteo
from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

# ---------------------------------------------------------------------------
# Read schedule from Variable (safe for test / lint environments)
# ---------------------------------------------------------------------------
SCHEDULE_VAR_KEY = "dag_107_schedule"

try:
    _schedule: str | None = Variable.get(SCHEDULE_VAR_KEY, default_var="@daily")
except Exception:
    _schedule = None  # no DB at parse time (pytest, ruff, etc.)

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------
OUTPUT_DIR = OUTPUT_BASE / "variable_schedule"

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task
def show_schedule_info() -> str:
    """Print the current schedule value and how to change it."""
    current = Variable.get(SCHEDULE_VAR_KEY, default_var="@daily")
    print(f"[{timestamp()}] === Variable-Driven Scheduling ===")
    print(f"  Variable key : {SCHEDULE_VAR_KEY}")
    print(f"  Current value: {current}")
    print()
    print("  To change the schedule without editing code:")
    print()
    print("  Option 1 -- Airflow UI:")
    print("    Admin -> Variables -> dag_107_schedule -> edit value")
    print()
    print("  Option 2 -- REST API (Airflow 3.x):")
    print(f'    PATCH /api/v2/variables/{SCHEDULE_VAR_KEY}')
    print('    Body: {"value": "@hourly"}')
    print()
    print("  Common schedule values:")
    print("    @once       -- run exactly once")
    print("    @hourly     -- every hour")
    print("    @daily      -- every day at midnight")
    print("    @weekly     -- every Sunday at midnight")
    print("    0 6 * * 1-5 -- weekdays at 06:00 (cron)")
    print()
    print("  The new schedule takes effect on the next DAG file parse cycle.")
    return current


@task
def set_new_schedule() -> str:
    """Update the schedule Variable to demonstrate programmatic changes.

    In a real pipeline you would not change the schedule every run -- this
    task exists purely to show that ``Variable.set()`` works and that the
    new value will be picked up on the next parse.
    """
    new_value = "@daily"
    Variable.set(SCHEDULE_VAR_KEY, new_value)
    print(f"[{timestamp()}] Variable.set('{SCHEDULE_VAR_KEY}', '{new_value}')")
    print("  The scheduler will pick up the new value on its next parse cycle.")
    return new_value


@task
def fetch_weather() -> dict[str, Any]:
    """Fetch weather for a Nordic city to show this is a real pipeline."""
    city = NORDIC_CITIES[0]  # Oslo
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "forecast_days": 3,
    }
    data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
    daily = data.get("daily", {})
    dates = daily.get("time", [])
    rows: list[dict[str, object]] = []
    for i, date in enumerate(dates):
        rows.append({
            "city": city["name"],
            "date": date,
            "temp_max": daily.get("temperature_2m_max", [None])[i],
            "temp_min": daily.get("temperature_2m_min", [None])[i],
            "precipitation": daily.get("precipitation_sum", [None])[i],
        })

    print(f"[{timestamp()}] Fetched {len(rows)} forecast rows for {city['name']}")
    for row in rows:
        print(f"  {row['date']}: {row['temp_max']}C / {row['temp_min']}C, precip {row['precipitation']}mm")
    return {"city": city["name"], "rows": rows}


@task
def save_result(schedule_info: str, weather_data: dict[str, Any]) -> None:
    """Write output to the target directory so it persists on the host."""
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "latest_run.json"

    result = {
        "schedule_variable": SCHEDULE_VAR_KEY,
        "schedule_value": schedule_info,
        "city": weather_data["city"],
        "forecast_rows": weather_data["rows"],
    }
    out_file.write_text(json.dumps(result, indent=2))
    print(f"[{timestamp()}] Saved result to {out_file}")
    print(f"  Schedule: {schedule_info}")
    print(f"  City: {weather_data['city']}, rows: {len(weather_data['rows'])}")


# ---------------------------------------------------------------------------
# DAG wiring
# ---------------------------------------------------------------------------

with DAG(
    dag_id="107_variable_driven_scheduling",
    default_args=DEFAULT_ARGS,
    description="Schedule controlled by an Airflow Variable -- changeable via API or UI",
    start_date=datetime(2024, 1, 1),
    schedule=_schedule,
    catchup=False,
    tags=["example", "variables", "scheduling"],
) as dag:
    info = show_schedule_info()
    new_sched = set_new_schedule()
    weather = fetch_weather()
    save = save_result(schedule_info=info, weather_data=weather)

    info >> new_sched >> weather >> save

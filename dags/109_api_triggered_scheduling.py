r"""DAG 109: API-triggered scheduling -- run a DAG on demand via the REST API.

Demonstrates the "run now" pattern: ``schedule=None`` means no automatic runs,
and the DAG is triggered exclusively through the REST API with a config payload.
This is the real alternative for responsive, on-demand scheduling -- not switching
executors.

Contrast with DAG 107:
    - Variable-based = "change when" a DAG runs (~30s delay for re-parse)
    - API-triggered   = "run now" with parameters (~5s)

Trigger via curl::

    curl -X POST http://localhost:8081/api/v2/dags/109_api_triggered_scheduling/dagRuns \\
      -H "Content-Type: application/json" \\
      -d '{"conf": {"city": "Bergen", "forecast_days": 5}}'
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow.sdk import DAG, task

from airflow_examples.apis import NORDIC_CITIES, OPEN_METEO_FORECAST, fetch_open_meteo
from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = OUTPUT_BASE / "api_triggered"

# Default parameters when none are supplied in dag_run.conf
DEFAULT_CITY = "Oslo"
DEFAULT_FORECAST_DAYS = 3


def _resolve_city(city_name: str) -> dict[str, Any]:
    """Look up a city by name from NORDIC_CITIES, falling back to Oslo."""
    for city in NORDIC_CITIES:
        if city["name"].lower() == city_name.lower():
            return city
    print(f"  City '{city_name}' not found in NORDIC_CITIES, falling back to {DEFAULT_CITY}")
    return NORDIC_CITIES[0]


@task
def show_trigger_info(**context: object) -> dict[str, Any]:
    """Print trigger instructions and extract config from dag_run.conf."""
    dag_run = context["dag_run"]  # type: ignore[index]
    conf: dict[str, Any] = dag_run.conf or {}  # type: ignore[union-attr]

    city_name = conf.get("city", DEFAULT_CITY)
    forecast_days = conf.get("forecast_days", DEFAULT_FORECAST_DAYS)

    print(f"[{timestamp()}] === API-Triggered Scheduling ===")
    print(f"  run_id       : {dag_run.run_id}")  # type: ignore[union-attr]
    print(f"  run_type     : {dag_run.run_type}")  # type: ignore[union-attr]
    print(f"  conf         : {conf}")
    print(f"  city         : {city_name}")
    print(f"  forecast_days: {forecast_days}")
    print()
    print("  This DAG has schedule=None -- it never runs automatically.")
    print("  Trigger it via the REST API:")
    print()
    print("  curl -X POST http://localhost:8081/api/v2/dags/109_api_triggered_scheduling/dagRuns \\")
    print('    -H "Content-Type: application/json" \\')
    print(f"    -d '{{\"conf\": {{\"city\": \"{city_name}\", \"forecast_days\": {forecast_days}}}}}'")
    print()
    print("  Contrast with DAG 107 (Variable-based):")
    print("    Variable-based = 'change when' (~30s delay for re-parse)")
    print("    API-triggered  = 'run now' with parameters (~5s)")

    return {"city": city_name, "forecast_days": forecast_days}


@task
def fetch_weather(trigger_conf: dict[str, Any]) -> dict[str, Any]:
    """Fetch weather for the city specified in the trigger config."""
    city_name = trigger_conf["city"]
    forecast_days = trigger_conf["forecast_days"]
    city = _resolve_city(city_name)

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "forecast_days": forecast_days,
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
def save_result(weather_data: dict[str, Any], **context: object) -> None:
    """Write output to the target directory so it persists on the host."""
    dag_run = context["dag_run"]  # type: ignore[index]
    conf: dict[str, Any] = dag_run.conf or {}  # type: ignore[union-attr]

    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "latest_run.json"

    result = {
        "trigger_conf": conf,
        "run_id": dag_run.run_id,  # type: ignore[union-attr]
        "city": weather_data["city"],
        "forecast_rows": weather_data["rows"],
    }
    out_file.write_text(json.dumps(result, indent=2))
    print(f"[{timestamp()}] Saved result to {out_file}")
    print(f"  City: {weather_data['city']}, rows: {len(weather_data['rows'])}")
    print(f"  Trigger conf: {conf}")


with DAG(
    dag_id="109_api_triggered_scheduling",
    default_args=DEFAULT_ARGS,
    description="On-demand DAG triggered via REST API with config payload",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api", "scheduling"],
) as dag:
    conf = show_trigger_info()
    weather = fetch_weather(trigger_conf=conf)
    save = save_result(weather_data=weather)

    conf >> weather >> save

"""DAG 81: Multi-City Weather Forecast.

Fetches 7-day hourly forecasts for 5 Nordic cities from the Open-Meteo
API, compares temperatures across cities, and produces a cross-city
comparison report. Uses dynamic task mapping with ``.expand()`` to
fan out API calls across all cities.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/81_forecast")


@task
def fetch_forecast(city: dict[str, object]) -> dict[str, object]:
    """Fetch 7-day hourly forecast for one city from Open-Meteo."""
    from airflow_examples.apis import OPEN_METEO_FORECAST, fetch_open_meteo

    name = str(city["name"])
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "forecast_days": 7,
    }
    data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
    hourly = data.get("hourly", {})
    print(f"[{timestamp()}] {name}: fetched {len(hourly.get('time', []))} hourly records")
    return {"name": name, "lat": city["lat"], "lon": city["lon"], "hourly": hourly}


@task
def combine_forecasts(forecasts: list[dict[str, object]]) -> dict[str, object]:
    """Merge all city forecasts and compute cross-city stats per hour."""
    import pandas as pd

    frames: list[pd.DataFrame] = []
    for fc in forecasts:
        hourly: dict[str, object] = fc.get("hourly", {})  # type: ignore[assignment]
        df = pd.DataFrame(hourly)
        df["city"] = fc["name"]
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    stats = combined.groupby("time")["temperature_2m"].agg(["min", "max", "mean"]).reset_index()
    print(f"[{timestamp()}] Combined {len(combined)} rows from {len(forecasts)} cities")
    print(f"[{timestamp()}] Cross-city temp range: {stats['min'].min():.1f}C to {stats['max'].max():.1f}C")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    combined.to_csv(f"{OUTPUT_DIR}/combined.csv", index=False)
    stats.to_csv(f"{OUTPUT_DIR}/stats.csv", index=False)
    return {
        "total_rows": len(combined),
        "cities": len(forecasts),
        "temp_min": float(stats["min"].min()),
        "temp_max": float(stats["max"].max()),
    }


@task
def analyze(summary: dict[str, object], forecasts: list[dict[str, object]]) -> dict[str, object]:
    """Find warmest/coldest city per day, max precipitation, wind ranking."""
    import pandas as pd

    frames: list[pd.DataFrame] = []
    for fc in forecasts:
        hourly: dict[str, object] = fc.get("hourly", {})  # type: ignore[assignment]
        df = pd.DataFrame(hourly)
        df["city"] = fc["name"]
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["time"]).dt.date

    daily_temp = combined.groupby(["date", "city"])["temperature_2m"].mean().reset_index()
    warmest_idx = daily_temp.groupby("date")["temperature_2m"].idxmax()
    warmest = daily_temp.loc[warmest_idx][["date", "city", "temperature_2m"]]

    wind_rank = combined.groupby("city")["wind_speed_10m"].mean().sort_values(ascending=False)

    print(f"[{timestamp()}] Analysis complete")
    print("  Warmest city by day:")
    for _, row in warmest.iterrows():
        print(f"    {row['date']}: {row['city']} ({row['temperature_2m']:.1f}C)")
    print("  Wind speed ranking:")
    for city_name, speed in wind_rank.items():
        print(f"    {city_name}: {speed:.1f} km/h")

    return {
        "warmest_days": len(warmest),
        "windiest_city": str(wind_rank.index[0]),
    }


@task
def report(summary: dict[str, object], analysis: dict[str, object]) -> None:
    """Print formatted multi-city comparison report."""
    print(f"\n[{timestamp()}] === Multi-City Forecast Report ===")
    print(f"  Cities compared: {summary.get('cities')}")
    print(f"  Total hourly records: {summary.get('total_rows')}")
    print(f"  Temperature range: {summary.get('temp_min')}C to {summary.get('temp_max')}C")
    print(f"  Windiest city: {analysis.get('windiest_city')}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="081_multi_city_forecast",
    default_args=DEFAULT_ARGS,
    description="Multi-city 7-day forecast comparison using Open-Meteo API",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    from airflow_examples.apis import NORDIC_CITIES

    fetched = fetch_forecast.expand(city=NORDIC_CITIES)
    combined = combine_forecasts(fetched)
    analyzed = analyze(summary=combined, forecasts=fetched)
    report(summary=combined, analysis=analyzed)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )
    cleanup.set_upstream([combined, analyzed])  # type: ignore[arg-type]

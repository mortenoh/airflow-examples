"""Exercise 04: API Pipeline.

Goal: Build a pipeline that fetches data from the Open-Meteo API,
transforms it, and writes the result to a CSV file.

Instructions:
1. Fetch 7-day forecast for Oslo (lat=59.91, lon=10.75) from Open-Meteo
2. Transform: compute daily temperature range (max - min)
3. Write the result to a CSV at /tmp/exercise_output/forecast.csv

Hint: Use `requests.get()` to call the API.
Hint: The Open-Meteo forecast URL is https://api.open-meteo.com/v1/forecast
Hint: Request params: latitude, longitude, daily=temperature_2m_max,temperature_2m_min, timezone=auto
"""

import os
from datetime import datetime

from airflow.sdk import DAG, task


# TODO: Create a @task function called `fetch_forecast` that:
# - Uses requests.get() to call Open-Meteo forecast API for Oslo
# - Returns the "daily" dict from the response JSON

# TODO: Create a @task function called `compute_ranges` that:
# - Takes the daily forecast dict
# - Computes temperature range (max - min) for each day
# - Returns a list of dicts with keys: date, temp_max, temp_min, temp_range

# TODO: Create a @task function called `write_csv` that:
# - Takes the list of range dicts
# - Writes them to /tmp/exercise_output/forecast.csv
# - Prints how many rows were written

# TODO: Create a DAG with dag_id="ex04_api_pipeline" and wire:
# fetch_forecast() >> compute_ranges() >> write_csv()


# SOLUTION (uncomment to check your work):
# -----------------------------------------------------------------------
# @task
# def fetch_forecast() -> dict[str, object]:
#     """Fetch 7-day forecast for Oslo from Open-Meteo."""
#     import requests
#
#     resp = requests.get(
#         "https://api.open-meteo.com/v1/forecast",
#         params={
#             "latitude": 59.91,
#             "longitude": 10.75,
#             "daily": "temperature_2m_max,temperature_2m_min",
#             "timezone": "auto",
#         },
#         timeout=30,
#     )
#     resp.raise_for_status()
#     data = resp.json()
#     daily = data["daily"]
#     print(f"Fetched {len(daily['time'])} days of forecast data")
#     return daily
#
#
# @task
# def compute_ranges(daily: dict[str, object]) -> list[dict[str, object]]:
#     """Compute daily temperature range."""
#     results = []
#     for date, tmax, tmin in zip(daily["time"], daily["temperature_2m_max"], daily["temperature_2m_min"]):
#         results.append({
#             "date": date,
#             "temp_max": tmax,
#             "temp_min": tmin,
#             "temp_range": round(tmax - tmin, 1),
#         })
#         print(f"  {date}: {tmin}C - {tmax}C (range: {round(tmax - tmin, 1)}C)")
#     return results
#
#
# @task
# def write_csv(rows: list[dict[str, object]]) -> None:
#     """Write forecast ranges to CSV."""
#     import csv
#
#     output_dir = "/tmp/exercise_output"
#     os.makedirs(output_dir, exist_ok=True)
#     path = f"{output_dir}/forecast.csv"
#     with open(path, "w", newline="") as f:
#         writer = csv.DictWriter(f, fieldnames=["date", "temp_max", "temp_min", "temp_range"])
#         writer.writeheader()
#         writer.writerows(rows)
#     print(f"Wrote {len(rows)} rows to {path}")
#
#
# with DAG(
#     dag_id="ex04_api_pipeline",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise"],
# ) as dag:
#     daily = fetch_forecast()
#     ranges = compute_ranges(daily=daily)
#     write_csv(rows=ranges)

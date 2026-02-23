"""Exercise 05: Quality Check with Branching.

Goal: Build a pipeline that fetches data, validates it with quality checks,
and branches based on whether the checks pass or fail.

Instructions:
1. Fetch temperature data for Oslo (reuse Open-Meteo)
2. Run quality checks: no nulls, values in physical range (-60 to 60 C)
3. If all checks pass, route to "publish_report"
4. If any check fails, route to "quarantine_data"

This combines API fetching, data quality, and branching patterns.
"""

from datetime import datetime

from airflow.sdk import DAG, task


# TODO: Create a @task function called `fetch_data` that fetches
# 7-day temperature forecast from Open-Meteo and returns the daily dict

# TODO: Create a @task.branch function called `quality_gate` that:
# - Takes the daily forecast dict
# - Checks for null values in temperature_2m_max
# - Checks all values are between -60 and 60
# - Returns "publish_report" if all checks pass
# - Returns "quarantine_data" if any check fails

# TODO: Create a @task function called `publish_report` that prints
# "All quality checks passed. Publishing report..."

# TODO: Create a @task function called `quarantine_data` that prints
# "Quality checks failed. Data quarantined for review."

# TODO: Create a DAG with dag_id="ex05_quality_check" and wire:
# fetch >> quality_gate >> [publish_report, quarantine_data]


# SOLUTION (uncomment to check your work):
# -----------------------------------------------------------------------
# @task
# def fetch_data() -> dict[str, object]:
#     """Fetch temperature forecast for Oslo."""
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
#     daily = resp.json()["daily"]
#     print(f"Fetched {len(daily['time'])} days")
#     return daily
#
#
# @task.branch
# def quality_gate(daily: dict[str, object]) -> str:
#     """Validate data quality and route accordingly."""
#     temps = daily.get("temperature_2m_max", [])
#
#     # Check 1: No nulls
#     null_count = sum(1 for t in temps if t is None)
#     if null_count > 0:
#         print(f"FAIL: {null_count} null values found")
#         return "quarantine_data"
#
#     # Check 2: Physical range
#     out_of_range = [t for t in temps if t < -60 or t > 60]
#     if out_of_range:
#         print(f"FAIL: {len(out_of_range)} values out of range: {out_of_range}")
#         return "quarantine_data"
#
#     print("All quality checks passed")
#     return "publish_report"
#
#
# @task
# def publish_report() -> None:
#     """Publish the validated data."""
#     print("All quality checks passed. Publishing report...")
#
#
# @task
# def quarantine_data() -> None:
#     """Quarantine failed data for review."""
#     print("Quality checks failed. Data quarantined for review.")
#
#
# with DAG(
#     dag_id="ex05_quality_check",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise"],
# ) as dag:
#     data = fetch_data()
#     gate = quality_gate(daily=data)
#     gate >> [publish_report(), quarantine_data()]

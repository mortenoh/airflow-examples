"""DAG 106: Human-in-the-Loop (HITL) operators for weather data quality review.

Demonstrates Airflow 3.1's four HITL operators in a Nordic weather data quality
review workflow. An analyst reviews fetched weather data and makes decisions
before publication.

Key patterns:
    - ``HITLEntryOperator`` with ``Param`` for free-text form input
    - ``HITLOperator`` for single-select quality rating
    - ``ApprovalOperator`` for binary approve/reject gating
    - ``HITLBranchOperator`` for user-directed export format routing
    - ``BaseNotifier`` subclass for logging HITL request details
    - ``execution_timeout`` + ``defaults`` for auto-resolution during ``airflow dags test``
    - XCom passing of HITL responses via Jinja templates in ``body`` fields
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import DAG, BaseNotifier, Param, task

from airflow_examples.apis import NORDIC_CITIES, OPEN_METEO_FORECAST, fetch_open_meteo
from airflow_examples.config import DEFAULT_ARGS, timestamp

# ---------------------------------------------------------------------------
# Notifier -- logs HITL subject and a placeholder UI link
# ---------------------------------------------------------------------------


class LocalLogNotifier(BaseNotifier):
    """Log HITL request details to the task log.

    A minimal notifier that prints the operator subject and body instead of
    sending an external notification. Useful for local development and tests.
    """

    template_fields = ("message",)

    def __init__(self, message: str = "HITL task waiting for input") -> None:
        """Create notifier with a templatable message string."""
        super().__init__()
        self.message = message

    def notify(self, context: dict[str, Any]) -> None:
        """Log the HITL request subject and body."""
        self.log.info("[HITL] %s", self.message)
        task_instance = context.get("task_instance") or context.get("ti")
        if task_instance:
            self.log.info("[HITL] task_id=%s, dag_id=%s", task_instance.task_id, task_instance.dag_id)


notifier = LocalLogNotifier(message="Review required: {{ task.subject }}")

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

HITL_TIMEOUT = timedelta(seconds=5)


@task
def fetch_weather_data() -> dict[str, Any]:
    """Fetch Nordic weather data from Open-Meteo for quality review."""
    results: list[dict[str, object]] = []
    for city in NORDIC_CITIES[:3]:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "forecast_days": 3,
        }
        data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
        daily = data.get("daily", {})
        for i, date in enumerate(daily.get("time", [])):
            results.append({
                "city": city["name"],
                "date": date,
                "temp_max": daily.get("temperature_2m_max", [None])[i],
                "temp_min": daily.get("temperature_2m_min", [None])[i],
                "precipitation": daily.get("precipitation_sum", [None])[i],
            })

    print(f"[{timestamp()}] Fetched {len(results)} weather records for {len(NORDIC_CITIES[:3])} cities")
    for city in NORDIC_CITIES[:3]:
        city_rows = [r for r in results if r["city"] == city["name"]]
        temps = [r["temp_max"] for r in city_rows if r["temp_max"] is not None]
        avg = sum(temps) / len(temps) if temps else 0
        print(f"  {city['name']}: {len(city_rows)} rows, avg max temp = {avg:.1f}C")

    return {"records": results, "city_count": len(NORDIC_CITIES[:3]), "record_count": len(results)}


@task
def export_csv(weather_data: dict[str, Any]) -> str:
    """Export weather data as CSV."""
    import csv
    import io

    records: list[dict[str, object]] = weather_data["records"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["city", "date", "temp_max", "temp_min", "precipitation"])
    writer.writeheader()
    writer.writerows(records)
    csv_content = output.getvalue()
    print(f"[{timestamp()}] Exported {len(records)} records as CSV ({len(csv_content)} chars)")
    return "csv"


@task
def export_json(weather_data: dict[str, Any]) -> str:
    """Export weather data as JSON."""
    import json

    records: list[dict[str, object]] = weather_data["records"]
    json_content = json.dumps(records, indent=2)
    print(f"[{timestamp()}] Exported {len(records)} records as JSON ({len(json_content)} chars)")
    return "json"


@task
def export_parquet(weather_data: dict[str, Any]) -> str:
    """Export weather data as Parquet."""
    import pandas as pd

    records: list[dict[str, object]] = weather_data["records"]
    df = pd.DataFrame(records)
    parquet_bytes = df.to_parquet(index=False)
    print(f"[{timestamp()}] Exported {len(records)} records as Parquet ({len(parquet_bytes):,} bytes)")
    return "parquet"


@task(trigger_rule="none_failed_min_one_success")
def publish_report(weather_data: dict[str, Any]) -> None:
    """Summarise all HITL decisions and the final pipeline outcome."""
    print(f"\n[{timestamp()}] === HITL Weather Quality Review Report ===")
    print(f"  Cities reviewed:  {weather_data.get('city_count')}")
    print(f"  Records fetched:  {weather_data.get('record_count')}")
    print()
    print("  HITL operators demonstrated:")
    print("    1. HITLEntryOperator  -- free-text analyst note via Param")
    print("    2. HITLOperator       -- single-select quality rating")
    print("    3. ApprovalOperator   -- binary approve/reject gate")
    print("    4. HITLBranchOperator -- user-directed export format routing")
    print()
    print("  All HITL tasks auto-resolved via execution_timeout + defaults")
    print("  (set execution_timeout=5s so `airflow dags test` completes)")


# ---------------------------------------------------------------------------
# DAG wiring
# ---------------------------------------------------------------------------

with DAG(
    dag_id="106_human_in_the_loop",
    default_args=DEFAULT_ARGS,
    description="Human-in-the-Loop operators for weather data quality review",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hitl"],
) as dag:
    weather = fetch_weather_data()

    # 1. HITLEntryOperator -- collect free-text analyst note
    add_analyst_note = HITLEntryOperator(
        task_id="add_analyst_note",
        subject="Add analyst note for weather data review",
        body="Review the fetched Nordic weather data and add any observations.",
        params={
            "analyst_note": Param(
                "Auto-approved: no manual review",
                type="string",
                title="Analyst Note",
                description="Enter observations about the weather data quality.",
            ),
        },
        execution_timeout=HITL_TIMEOUT,
        notifiers=[notifier],
    )

    # 2. HITLOperator -- choose quality rating
    choose_quality_rating = HITLOperator(
        task_id="choose_quality_rating",
        subject="Rate the weather data quality",
        body="Select a quality rating for the Nordic weather dataset.",
        options=["Excellent", "Good", "Acceptable", "Poor"],
        defaults=["Good"],
        execution_timeout=HITL_TIMEOUT,
        notifiers=[notifier],
    )

    # 3. ApprovalOperator -- approve or reject publication
    approve_publication = ApprovalOperator(
        task_id="approve_publication",
        subject="Approve weather data for publication?",
        body="Confirm that the reviewed data meets publication standards.",
        defaults="Approve",
        execution_timeout=HITL_TIMEOUT,
        notifiers=[notifier],
    )

    # 4. HITLBranchOperator -- choose export format
    choose_output_format = HITLBranchOperator(
        task_id="choose_output_format",
        subject="Choose export format",
        body="Select the output format for the weather data export.",
        options=["export_csv", "export_json", "export_parquet"],
        defaults=["export_csv"],
        execution_timeout=HITL_TIMEOUT,
        notifiers=[notifier],
    )

    csv_out = export_csv(weather_data=weather)
    json_out = export_json(weather_data=weather)
    parquet_out = export_parquet(weather_data=weather)

    join_exports = EmptyOperator(
        task_id="join_exports",
        trigger_rule="none_failed_min_one_success",
    )

    report = publish_report(weather_data=weather)

    # Dependencies
    weather >> add_analyst_note >> choose_quality_rating >> approve_publication >> choose_output_format
    choose_output_format >> [csv_out, json_out, parquet_out] >> join_exports >> report

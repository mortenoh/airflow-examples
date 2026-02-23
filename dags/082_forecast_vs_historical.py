"""DAG 82: Forecast Accuracy Analysis.

Compares Open-Meteo forecast data with the historical archive API for
the same location to measure forecast accuracy. Computes MAE, RMSE,
and bias metrics, and analyzes how accuracy degrades with lead time.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/82_accuracy")


@task
def fetch_recent_forecast() -> dict[str, object]:
    """Fetch 7-day forecast for Oslo from Open-Meteo."""
    from airflow_examples.apis import OPEN_METEO_FORECAST, fetch_open_meteo

    params = {
        "latitude": 59.91,
        "longitude": 10.75,
        "hourly": "temperature_2m,precipitation",
        "forecast_days": 7,
    }
    data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
    hourly = data.get("hourly", {})
    print(f"[{timestamp()}] Forecast: {len(hourly.get('time', []))} hourly records for Oslo")
    return {"hourly": hourly, "source": "forecast"}


@task
def fetch_historical() -> dict[str, object]:
    """Fetch archive data for Oslo for a past 7-day period (same week last year)."""
    from airflow_examples.apis import OPEN_METEO_ARCHIVE, fetch_open_meteo

    params = {
        "latitude": 59.91,
        "longitude": 10.75,
        "hourly": "temperature_2m,precipitation",
        "start_date": "2024-01-01",
        "end_date": "2024-01-07",
    }
    data = fetch_open_meteo(OPEN_METEO_ARCHIVE, params)
    hourly = data.get("hourly", {})
    print(f"[{timestamp()}] Archive: {len(hourly.get('time', []))} hourly records for Oslo")
    return {"hourly": hourly, "source": "archive"}


@task
def compute_accuracy(forecast: dict[str, object], historical: dict[str, object]) -> dict[str, object]:
    """Compute MAE, RMSE, and bias between forecast and archive patterns."""
    import numpy as np
    import pandas as pd

    fc_hourly: dict[str, object] = forecast.get("hourly", {})  # type: ignore[assignment]
    hist_hourly: dict[str, object] = historical.get("hourly", {})  # type: ignore[assignment]

    df_fc = pd.DataFrame(fc_hourly)
    df_hist = pd.DataFrame(hist_hourly)

    n = min(len(df_fc), len(df_hist))
    fc_temp = df_fc["temperature_2m"].iloc[:n].values.astype(float)
    hist_temp = df_hist["temperature_2m"].iloc[:n].values.astype(float)

    errors = fc_temp - hist_temp
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors**2)))
    bias = float(np.mean(errors))

    print(f"[{timestamp()}] Accuracy (n={n}): MAE={mae:.2f}C, RMSE={rmse:.2f}C, Bias={bias:.2f}C")
    return {"mae": mae, "rmse": rmse, "bias": bias, "n_hours": n}


@task
def analyze_by_lead_time(forecast: dict[str, object]) -> dict[str, object]:
    """Analyze forecast patterns by lead time (day 1 vs day 7)."""
    import pandas as pd

    fc_hourly: dict[str, object] = forecast.get("hourly", {})  # type: ignore[assignment]
    df = pd.DataFrame(fc_hourly)
    df["time"] = pd.to_datetime(df["time"])
    df["day"] = (df["time"] - df["time"].iloc[0]).dt.days + 1

    daily_stats = df.groupby("day")["temperature_2m"].agg(["mean", "std"]).reset_index()
    print(f"[{timestamp()}] Lead-time analysis:")
    for _, row in daily_stats.iterrows():
        print(f"  Day {int(row['day'])}: mean={row['mean']:.1f}C, std={row['std']:.1f}C")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    daily_stats.to_csv(f"{OUTPUT_DIR}/lead_time.csv", index=False)
    return {
        "days_analyzed": len(daily_stats),
        "day1_std": float(daily_stats.iloc[0]["std"]) if len(daily_stats) > 0 else 0,
        "day7_std": float(daily_stats.iloc[-1]["std"]) if len(daily_stats) > 0 else 0,
    }


@task
def report(accuracy: dict[str, object], lead_time: dict[str, object]) -> None:
    """Print accuracy metrics and lead-time degradation."""
    print(f"\n[{timestamp()}] === Forecast Accuracy Report ===")
    print(f"  MAE:  {accuracy.get('mae')}C")
    print(f"  RMSE: {accuracy.get('rmse')}C")
    print(f"  Bias: {accuracy.get('bias')}C")
    print(f"  Compared hours: {accuracy.get('n_hours')}")
    print(f"  Lead-time days: {lead_time.get('days_analyzed')}")
    print(f"  Day 1 variability (std): {lead_time.get('day1_std')}C")
    print(f"  Day 7 variability (std): {lead_time.get('day7_std')}C")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="082_forecast_vs_historical",
    default_args=DEFAULT_ARGS,
    description="Forecast accuracy analysis comparing forecast vs archive",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    fc = fetch_recent_forecast()
    hist = fetch_historical()
    acc = compute_accuracy(forecast=fc, historical=hist)
    lt = analyze_by_lead_time(forecast=fc)
    report(accuracy=acc, lead_time=lt)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )
    cleanup.set_upstream([acc, lt])  # type: ignore[arg-type]

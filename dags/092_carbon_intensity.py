"""DAG 92: UK Carbon Intensity Analysis.

Fetches UK power grid carbon intensity and generation mix data from the
Carbon Intensity API, analyzes daily intensity profiles, decomposes the
generation mix by fuel type, and compares forecast vs actual accuracy.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/92_carbon")


@task
def fetch_intensity() -> list[dict[str, object]]:
    """Fetch half-hourly carbon intensity for past 7 days."""
    from airflow_examples.apis import CARBON_INTENSITY, fetch_json, flatten_carbon_intensity

    all_records: list[dict[str, object]] = []
    for days_ago in range(7, 0, -1):
        date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        data = fetch_json(f"{CARBON_INTENSITY}/intensity/date/{date}")
        raw: list[dict[str, object]] = data.get("data", [])  # type: ignore[assignment]
        flat = flatten_carbon_intensity(raw)
        all_records.extend(flat)

    print(f"[{timestamp()}] Fetched {len(all_records)} half-hourly intensity records")
    return all_records


@task
def fetch_generation_mix() -> list[dict[str, object]]:
    """Fetch generation mix breakdown for past 48 hours."""
    from airflow_examples.apis import CARBON_INTENSITY, fetch_json, flatten_generation_mix

    end = datetime.now()
    start = end - timedelta(hours=48)
    url = f"{CARBON_INTENSITY}/generation/{start.strftime('%Y-%m-%dT%H:%MZ')}/{end.strftime('%Y-%m-%dT%H:%MZ')}"
    data = fetch_json(url)
    raw: list[dict[str, object]] = data.get("data", [])  # type: ignore[assignment]
    flat = flatten_generation_mix(raw)

    print(f"[{timestamp()}] Fetched {len(flat)} generation mix records")
    return flat


@task
def analyze_intensity_patterns(intensity: list[dict[str, object]]) -> dict[str, object]:
    """Compute daily profiles: which hours are cleanest/dirtiest."""
    import pandas as pd

    df = pd.DataFrame(intensity)
    if len(df) == 0:
        return {"cleanest_hour": 0, "dirtiest_hour": 0}

    df["from_dt"] = pd.to_datetime(df["from"])
    df["hour"] = df["from_dt"].dt.hour
    df["weekday"] = df["from_dt"].dt.dayofweek < 5

    hourly_avg = df.groupby("hour")["actual"].mean()
    cleanest = int(hourly_avg.idxmin()) if not hourly_avg.isna().all() else 0
    dirtiest = int(hourly_avg.idxmax()) if not hourly_avg.isna().all() else 0

    weekday_avg = df[df["weekday"]]["actual"].mean()
    weekend_avg = df[~df["weekday"]]["actual"].mean()

    print(f"[{timestamp()}] Intensity patterns:")
    print(f"  Cleanest hour: {cleanest}:00 ({hourly_avg.get(cleanest, 0):.0f} gCO2/kWh)")
    print(f"  Dirtiest hour: {dirtiest}:00 ({hourly_avg.get(dirtiest, 0):.0f} gCO2/kWh)")
    print(f"  Weekday avg: {weekday_avg:.0f}, Weekend avg: {weekend_avg:.0f} gCO2/kWh")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return {
        "cleanest_hour": cleanest,
        "dirtiest_hour": dirtiest,
        "weekday_avg": float(weekday_avg) if not pd.isna(weekday_avg) else 0,
        "weekend_avg": float(weekend_avg) if not pd.isna(weekend_avg) else 0,
    }


@task
def decompose_generation(gen_mix: list[dict[str, object]]) -> dict[str, object]:
    """Stack fuel types, compute % contribution over time."""
    import pandas as pd

    df = pd.DataFrame(gen_mix)
    if len(df) == 0:
        return {"fuel_summary": {}}

    fuel_summary = df.groupby("fuel")["perc"].agg(["mean", "max"]).round(1).to_dict(orient="index")

    print(f"[{timestamp()}] Generation mix decomposition:")
    for fuel, stats in sorted(fuel_summary.items(), key=lambda x: x[1]["mean"], reverse=True):
        print(f"  {fuel}: avg={stats['mean']:.1f}%, max={stats['max']:.1f}%")

    return {"fuel_summary": fuel_summary}


@task
def forecast_vs_actual(intensity: list[dict[str, object]]) -> dict[str, object]:
    """Compare forecast vs actual intensity, compute forecast accuracy."""
    import numpy as np
    import pandas as pd

    df = pd.DataFrame(intensity)
    valid = df.dropna(subset=["forecast", "actual"])

    if len(valid) == 0:
        return {"mae": 0, "rmse": 0, "n": 0}

    fc = valid["forecast"].astype(float).values
    act = valid["actual"].astype(float).values
    errors = fc - act
    mae = float(np.mean(np.abs(errors)))
    rmse = float(np.sqrt(np.mean(errors**2)))
    bias = float(np.mean(errors))

    print(f"[{timestamp()}] Forecast accuracy (n={len(valid)}): MAE={mae:.1f}, RMSE={rmse:.1f}, Bias={bias:.1f}")
    return {"mae": mae, "rmse": rmse, "bias": bias, "n": len(valid)}


@task
def report(
    patterns: dict[str, object],
    generation: dict[str, object],
    accuracy: dict[str, object],
) -> None:
    """Print intensity profile, generation mix, and forecast accuracy."""
    print(f"\n[{timestamp()}] === Carbon Intensity Report ===")
    print(f"  Cleanest hour: {patterns.get('cleanest_hour')}:00")
    print(f"  Dirtiest hour: {patterns.get('dirtiest_hour')}:00")
    print(f"  Weekday avg: {patterns.get('weekday_avg'):.0f} gCO2/kWh")
    print(f"  Weekend avg: {patterns.get('weekend_avg'):.0f} gCO2/kWh")

    fuel: dict[str, dict[str, float]] = generation.get("fuel_summary", {})  # type: ignore[assignment]
    print("\n  Generation Mix:")
    for name, stats in sorted(fuel.items(), key=lambda x: x[1].get("mean", 0), reverse=True):
        print(f"    {name}: avg={stats.get('mean', 0):.1f}%")

    print(f"\n  Forecast Accuracy (n={accuracy.get('n')}):")
    print(f"    MAE: {accuracy.get('mae'):.1f} gCO2/kWh")
    print(f"    RMSE: {accuracy.get('rmse'):.1f} gCO2/kWh")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="092_carbon_intensity",
    default_args=DEFAULT_ARGS,
    description="UK carbon intensity analysis with generation mix decomposition",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    intensity = fetch_intensity()
    gen_mix = fetch_generation_mix()
    patterns = analyze_intensity_patterns(intensity=intensity)
    generation = decompose_generation(gen_mix=gen_mix)
    accuracy = forecast_vs_actual(intensity=intensity)
    report(patterns=patterns, generation=generation, accuracy=accuracy)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

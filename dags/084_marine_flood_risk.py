"""DAG 84: Marine + Flood Composite Risk Index.

Combines marine forecast data (wave height, swell period) from
Open-Meteo Marine API with river flood discharge data from the
Open-Meteo Flood API to produce a weighted composite risk index.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/84_marine_flood")

COASTAL_POINTS = [
    {"name": "Bergen Coast", "lat": 60.39, "lon": 4.50},
    {"name": "Stavanger Coast", "lat": 58.97, "lon": 5.00},
    {"name": "Tromso Coast", "lat": 69.65, "lon": 18.50},
]

RIVER_POINTS = [
    {"name": "Glomma", "lat": 59.28, "lon": 11.11},
    {"name": "Drammenselva", "lat": 59.74, "lon": 10.25},
    {"name": "Namsen", "lat": 64.44, "lon": 11.50},
]


@task
def fetch_marine() -> list[dict[str, object]]:
    """Fetch marine forecast data for Norwegian coastal points."""
    from airflow_examples.apis import OPEN_METEO_MARINE, fetch_open_meteo

    results: list[dict[str, object]] = []
    for pt in COASTAL_POINTS:
        params = {
            "latitude": pt["lat"],
            "longitude": pt["lon"],
            "hourly": "wave_height,swell_wave_height,wave_period",
            "forecast_days": 3,
        }
        data = fetch_open_meteo(OPEN_METEO_MARINE, params)
        hourly = data.get("hourly", {})
        print(f"[{timestamp()}] {pt['name']}: {len(hourly.get('time', []))} marine records")
        results.append({"name": pt["name"], "hourly": hourly})
    return results


@task
def fetch_flood() -> list[dict[str, object]]:
    """Fetch flood discharge data for Norwegian rivers."""
    from airflow_examples.apis import OPEN_METEO_FLOOD, fetch_open_meteo

    results: list[dict[str, object]] = []
    for pt in RIVER_POINTS:
        params = {
            "latitude": pt["lat"],
            "longitude": pt["lon"],
            "daily": "river_discharge,river_discharge_mean",
            "forecast_days": 7,
        }
        data = fetch_open_meteo(OPEN_METEO_FLOOD, params)
        daily = data.get("daily", {})
        print(f"[{timestamp()}] {pt['name']}: {len(daily.get('time', []))} flood records")
        results.append({"name": pt["name"], "daily": daily})
    return results


@task
def compute_marine_risk(marine_data: list[dict[str, object]]) -> list[dict[str, object]]:
    """Score 0-100 based on wave height thresholds."""
    from airflow_examples.transforms import score_wave_height

    results: list[dict[str, object]] = []
    for loc in marine_data:
        name = loc["name"]
        hourly: dict[str, object] = loc.get("hourly", {})  # type: ignore[assignment]
        wave_heights: list[object] = hourly.get("wave_height", [])  # type: ignore[assignment]

        scores: list[float] = []
        for wh in wave_heights:
            if wh is None:
                scores.append(0)
                continue
            scores.append(score_wave_height(float(str(wh))))

        avg_score = sum(scores) / len(scores) if scores else 0
        max_score = max(scores) if scores else 0
        print(f"[{timestamp()}] {name}: marine risk avg={avg_score:.0f}, max={max_score:.0f}")
        results.append({"name": name, "avg_risk": avg_score, "max_risk": max_score})
    return results


@task
def compute_flood_risk(flood_data: list[dict[str, object]]) -> list[dict[str, object]]:
    """Score 0-100 based on discharge vs mean ratio."""
    from airflow_examples.transforms import score_flood_ratio

    results: list[dict[str, object]] = []
    for loc in flood_data:
        name = loc["name"]
        daily: dict[str, object] = loc.get("daily", {})  # type: ignore[assignment]
        discharge: list[object] = daily.get("river_discharge", [])  # type: ignore[assignment]
        mean_discharge: list[object] = daily.get("river_discharge_mean", [])  # type: ignore[assignment]

        scores: list[float] = []
        for d, m in zip(discharge, mean_discharge):
            if d is None or m is None or float(str(m)) == 0:
                scores.append(0)
                continue
            scores.append(score_flood_ratio(float(str(d)), float(str(m))))

        avg_score = sum(scores) / len(scores) if scores else 0
        print(f"[{timestamp()}] {name}: flood risk avg={avg_score:.0f}")
        results.append({"name": name, "avg_risk": avg_score})
    return results


@task
def composite_risk(
    marine_risks: list[dict[str, object]],
    flood_risks: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Weighted average: marine 60%, flood 40%. Classify risk category."""
    from airflow_examples.transforms import classify_risk

    results: list[dict[str, object]] = []
    all_locations = marine_risks + flood_risks
    for loc in all_locations:
        name = str(loc["name"])
        marine_score = float(loc.get("avg_risk", 0))  # type: ignore[arg-type]
        flood_match = [f for f in flood_risks if f["name"] == name]
        flood_score = float(flood_match[0].get("avg_risk", 0)) if flood_match else 0  # type: ignore[arg-type]

        composite = marine_score * 0.6 + flood_score * 0.4
        category = classify_risk(composite)

        print(f"[{timestamp()}] {name}: composite={composite:.0f} ({category})")
        results.append({"name": name, "composite_score": composite, "category": category})

    return results


@task
def report(
    marine_risks: list[dict[str, object]],
    flood_risks: list[dict[str, object]],
    composite: list[dict[str, object]],
) -> None:
    """Print risk dashboard with per-location scores."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n[{timestamp()}] === Marine + Flood Risk Dashboard ===")
    print("\n  Marine Risk Scores:")
    for r in marine_risks:
        print(f"    {r['name']}: avg={r.get('avg_risk', 0):.0f}, max={r.get('max_risk', 0):.0f}")
    print("\n  Flood Risk Scores:")
    for r in flood_risks:
        print(f"    {r['name']}: avg={r.get('avg_risk', 0):.0f}")
    print("\n  Composite Risk Index:")
    for r in composite:
        print(f"    {r['name']}: {r.get('composite_score', 0):.0f} ({r.get('category')})")
    print(f"\n  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="084_marine_flood_risk",
    default_args=DEFAULT_ARGS,
    description="Marine + flood composite risk index from Open-Meteo APIs",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    marine = fetch_marine()
    flood = fetch_flood()
    m_risk = compute_marine_risk(marine_data=marine)
    f_risk = compute_flood_risk(flood_data=flood)
    comp = composite_risk(marine_risks=m_risk, flood_risks=f_risk)
    report(marine_risks=m_risk, flood_risks=f_risk, composite=comp)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

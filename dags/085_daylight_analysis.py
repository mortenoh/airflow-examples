"""DAG 85: Sunrise-Sunset Daylight Analysis.

Analyzes daylight hours across Nordic latitudes using the Sunrise-Sunset
API. Fetches sunrise/sunset times for the 1st of each month, computes
day length, and correlates daylight variation with latitude.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/85_daylight")


@task
def fetch_daylight() -> list[dict[str, object]]:
    """Fetch sunrise/sunset for each city for 12 monthly dates."""
    from airflow_examples.apis import NORDIC_CITIES, SUNRISE_SUNSET, fetch_json

    results: list[dict[str, object]] = []
    for city in NORDIC_CITIES:
        city_data: list[dict[str, object]] = []
        for month in range(1, 13):
            date_str = f"2024-{month:02d}-01"
            params = {
                "lat": city["lat"],
                "lng": city["lon"],
                "date": date_str,
                "formatted": 0,
            }
            data = fetch_json(SUNRISE_SUNSET, params=params)
            api_results: dict[str, object] = data.get("results", {})  # type: ignore[assignment]
            city_data.append({
                "date": date_str,
                "month": month,
                "sunrise": api_results.get("sunrise"),
                "sunset": api_results.get("sunset"),
                "day_length": api_results.get("day_length"),
            })
        print(f"[{timestamp()}] {city['name']}: fetched 12 monthly daylight records")
        results.append({"name": city["name"], "lat": city["lat"], "data": city_data})
    return results


@task
def compute_daylight_hours(daylight_data: list[dict[str, object]]) -> dict[str, object]:
    """Parse times and compute day length in hours for each city x month."""
    matrix: dict[str, list[float]] = {}
    for city in daylight_data:
        name = str(city["name"])
        data: list[dict[str, object]] = city.get("data", [])  # type: ignore[assignment]
        hours: list[float] = []
        for record in data:
            dl = record.get("day_length")
            if dl is not None:
                hours.append(float(str(dl)) / 3600.0)
            else:
                hours.append(0.0)
        matrix[name] = hours
        print(f"[{timestamp()}] {name}: daylight hours = {[f'{h:.1f}' for h in hours]}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return {"matrix": matrix}


@task
def analyze_latitude_effect(
    daylight_data: list[dict[str, object]],
    hours_data: dict[str, object],
) -> dict[str, object]:
    """Correlate latitude with daylight variation amplitude."""
    matrix: dict[str, list[float]] = hours_data.get("matrix", {})  # type: ignore[assignment]

    analysis: list[dict[str, object]] = []
    for city in daylight_data:
        name = str(city["name"])
        lat = float(str(city["lat"]))
        hours = matrix.get(name, [])
        if hours:
            amplitude = max(hours) - min(hours)
            analysis.append({"name": name, "lat": lat, "amplitude": amplitude})
            print(f"[{timestamp()}] {name} (lat={lat:.1f}): amplitude={amplitude:.1f}h")

    analysis.sort(key=lambda x: float(str(x["lat"])))
    return {"latitude_analysis": analysis}


@task
def find_extremes(
    daylight_data: list[dict[str, object]],
    hours_data: dict[str, object],
) -> dict[str, object]:
    """Identify shortest/longest days per city, compute summer/winter ratio."""
    matrix: dict[str, list[float]] = hours_data.get("matrix", {})  # type: ignore[assignment]
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    extremes: list[dict[str, object]] = []
    for city in daylight_data:
        name = str(city["name"])
        hours = matrix.get(name, [])
        if not hours:
            continue

        min_h = min(hours)
        max_h = max(hours)
        min_month = months[hours.index(min_h)]
        max_month = months[hours.index(max_h)]
        ratio = max_h / min_h if min_h > 0 else None

        ratio_str = f"{ratio:.1f}x" if ratio is not None else "N/A"
        print(f"[{timestamp()}] {name}: shortest={min_h:.1f}h ({min_month}), "
              f"longest={max_h:.1f}h ({max_month}), ratio={ratio_str}")
        extremes.append({
            "name": name,
            "shortest_hours": min_h,
            "shortest_month": min_month,
            "longest_hours": max_h,
            "longest_month": max_month,
            "ratio": ratio,
        })
    return {"extremes": extremes}


@task
def report(
    hours_data: dict[str, object],
    latitude: dict[str, object],
    extremes_data: dict[str, object],
) -> None:
    """Print daylight hours table, latitude correlation, extreme day stats."""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    matrix: dict[str, list[float]] = hours_data.get("matrix", {})  # type: ignore[assignment]

    print(f"\n[{timestamp()}] === Daylight Analysis Report ===")
    print("\n  City x Month Daylight Hours:")
    header = "  {:12s}".format("City") + "".join(f" {m:>5s}" for m in months)
    print(header)
    for name, hours in matrix.items():
        row = "  {:12s}".format(name) + "".join(f" {h:5.1f}" for h in hours)
        print(row)

    lat_analysis: list[dict[str, object]] = latitude.get("latitude_analysis", [])  # type: ignore[assignment]
    print("\n  Latitude vs Amplitude:")
    for entry in lat_analysis:
        print(f"    {entry['name']} (lat={entry['lat']}): {entry['amplitude']}h variation")

    ext: list[dict[str, object]] = extremes_data.get("extremes", [])  # type: ignore[assignment]
    print("\n  Extreme Days:")
    for e in ext:
        print(f"    {e['name']}: {e['shortest_hours']}h ({e['shortest_month']}) to "
              f"{e['longest_hours']}h ({e['longest_month']}), ratio={e['ratio']}x")


with DAG(
    dag_id="085_daylight_analysis",
    default_args=DEFAULT_ARGS,
    description="Daylight hours analysis across Nordic latitudes",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    dl_data = fetch_daylight()
    hours = compute_daylight_hours(daylight_data=dl_data)
    lat_effect = analyze_latitude_effect(daylight_data=dl_data, hours_data=hours)
    ext = find_extremes(daylight_data=dl_data, hours_data=hours)
    report(hours_data=hours, latitude=lat_effect, extremes_data=ext)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

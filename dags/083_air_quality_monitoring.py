"""DAG 83: Air Quality Index Pipeline.

Fetches air quality data from the Open-Meteo Air Quality API for
5 Nordic cities, classifies readings using WHO thresholds, identifies
exceedances, and generates per-city health advisories.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/83_air_quality")


@task
def fetch_air_quality() -> list[dict[str, object]]:
    """Fetch air quality data for 5 Nordic cities from Open-Meteo."""
    from airflow_examples.apis import NORDIC_CITIES, OPEN_METEO_AIR_QUALITY, fetch_open_meteo

    results: list[dict[str, object]] = []
    for city in NORDIC_CITIES:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "hourly": "pm2_5,pm10,nitrogen_dioxide,ozone,european_aqi",
            "forecast_days": 3,
        }
        data = fetch_open_meteo(OPEN_METEO_AIR_QUALITY, params)
        hourly = data.get("hourly", {})
        print(f"[{timestamp()}] {city['name']}: {len(hourly.get('time', []))} AQI records")
        results.append({"name": city["name"], "hourly": hourly})
    return results


@task
def classify_aqi(air_data: list[dict[str, object]]) -> list[dict[str, object]]:
    """Classify European AQI values into WHO categories per city."""
    from airflow_examples.transforms import AQI_CATEGORIES, classify_european_aqi

    results: list[dict[str, object]] = []
    for city_data in air_data:
        name = city_data["name"]
        hourly: dict[str, object] = city_data.get("hourly", {})  # type: ignore[assignment]
        aqi_values: list[object] = hourly.get("european_aqi", [])  # type: ignore[assignment]

        counts: dict[str, int] = {cat: 0 for _, cat in AQI_CATEGORIES}
        for val in aqi_values:
            if val is None:
                continue
            counts[classify_european_aqi(float(str(val)))] += 1

        print(f"[{timestamp()}] {name} AQI distribution: {counts}")
        results.append({"name": name, "aqi_distribution": counts})
    return results


@task
def identify_exceedances(air_data: list[dict[str, object]]) -> list[dict[str, object]]:
    """Flag hours exceeding WHO 2021 guideline thresholds."""
    from airflow_examples.transforms import WHO_THRESHOLDS, check_who_exceedance

    results: list[dict[str, object]] = []
    for city_data in air_data:
        name = city_data["name"]
        hourly: dict[str, object] = city_data.get("hourly", {})  # type: ignore[assignment]
        exceedances: dict[str, int] = {}
        total_hours = len(hourly.get("time", []))  # type: ignore[arg-type]

        for pollutant in WHO_THRESHOLDS:
            values: list[object] = hourly.get(pollutant, [])  # type: ignore[assignment]
            count = sum(1 for v in values if v is not None and check_who_exceedance(pollutant, float(str(v))))
            exceedances[pollutant] = count

        print(f"[{timestamp()}] {name} exceedances (of {total_hours}h): {exceedances}")
        results.append({"name": name, "exceedances": exceedances, "total_hours": total_hours})
    return results


@task
def generate_advisory(classifications: list[dict[str, object]]) -> list[dict[str, object]]:
    """Generate health advisory text per city based on worst AQI category."""
    advisories_map = {
        "Good": "Air quality is satisfactory. No precautions needed.",
        "Fair": "Air quality is acceptable. Unusually sensitive individuals should limit outdoor exertion.",
        "Moderate": "Sensitive groups may experience health effects. Reduce prolonged outdoor exertion.",
        "Poor": "Everyone may experience health effects. Avoid prolonged outdoor exertion.",
        "Very Poor": "Health alert: everyone may experience serious health effects. Stay indoors.",
        "Extremely Poor": "Emergency conditions. Avoid all outdoor activity.",
    }

    severity_order = ["Extremely Poor", "Very Poor", "Poor", "Moderate", "Fair", "Good"]

    results: list[dict[str, object]] = []
    for city_data in classifications:
        name = city_data["name"]
        dist: dict[str, int] = city_data.get("aqi_distribution", {})  # type: ignore[assignment]

        worst = "Good"
        for sev in severity_order:
            if dist.get(sev, 0) > 0:
                worst = sev
                break

        advisory = advisories_map.get(worst, "No data available.")
        print(f"[{timestamp()}] {name}: {worst} -> {advisory}")
        results.append({"name": name, "worst_category": worst, "advisory": advisory})
    return results


@task
def report(
    classifications: list[dict[str, object]],
    exceedances: list[dict[str, object]],
    advisories: list[dict[str, object]],
) -> None:
    """Print AQI summary, exceedances, and health advisories."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n[{timestamp()}] === Air Quality Report ===")
    for cls, exc, adv in zip(classifications, exceedances, advisories):
        print(f"\n  {cls['name']}:")
        print(f"    AQI Distribution: {cls.get('aqi_distribution')}")
        print(f"    Exceedances: {exc.get('exceedances')}")
        print(f"    Advisory ({adv.get('worst_category')}): {adv.get('advisory')}")
    print(f"\n  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="083_air_quality_monitoring",
    default_args=DEFAULT_ARGS,
    description="Air quality monitoring with WHO threshold classification",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    aq_data = fetch_air_quality()
    classified = classify_aqi(air_data=aq_data)
    exceeded = identify_exceedances(air_data=aq_data)
    advisories = generate_advisory(classifications=classified)
    report(classifications=classified, exceedances=exceeded, advisories=advisories)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

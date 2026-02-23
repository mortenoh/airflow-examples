"""DAG 90: Cross-API Country + Weather Enrichment.

Joins REST Countries data with Open-Meteo weather and air quality APIs
to create enriched country profiles for Nordic capitals, with composite
rankings across multiple dimensions.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/90_enrichment")


@task
def fetch_countries() -> list[dict[str, object]]:
    """Get Nordic countries from REST Countries with capital and coordinates."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/alpha?codes=NOR,SWE,DNK,FIN,ISL"
    params = {"fields": "name,cca3,capital,capitalInfo,population,area,currencies"}
    data = fetch_json(url, params=params)
    countries: list[dict[str, object]] = data if isinstance(data, list) else []

    results: list[dict[str, object]] = []
    for c in countries:
        name_obj: dict[str, object] = c.get("name", {})  # type: ignore[assignment]
        cap_info: dict[str, object] = c.get("capitalInfo", {})  # type: ignore[assignment]
        latlng: list[float] = cap_info.get("latlng", [0, 0])  # type: ignore[assignment]
        capitals: list[str] = c.get("capital", [])  # type: ignore[assignment]
        results.append({
            "code": c.get("cca3"),
            "name": name_obj.get("common"),
            "capital": capitals[0] if capitals else "",
            "capital_lat": latlng[0] if len(latlng) > 0 else 0,
            "capital_lon": latlng[1] if len(latlng) > 1 else 0,
            "population": c.get("population"),
            "area": c.get("area"),
        })

    print(f"[{timestamp()}] Fetched {len(results)} Nordic countries with capitals")
    return results


@task
def fetch_capital_weather(countries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Fetch current weather for each capital from Open-Meteo."""
    from airflow_examples.apis import OPEN_METEO_FORECAST, fetch_open_meteo

    results: list[dict[str, object]] = []
    for c in countries:
        params = {
            "latitude": c["capital_lat"],
            "longitude": c["capital_lon"],
            "current": "temperature_2m,wind_speed_10m,relative_humidity_2m,weather_code",
        }
        data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
        current = data.get("current", {})
        print(f"[{timestamp()}] {c['capital']}: temp={current.get('temperature_2m')}C")
        results.append({**c, "weather": current})
    return results


@task
def fetch_capital_air_quality(countries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Fetch AQI for each capital from Open-Meteo Air Quality API."""
    from airflow_examples.apis import OPEN_METEO_AIR_QUALITY, fetch_open_meteo

    results: list[dict[str, object]] = []
    for c in countries:
        params = {
            "latitude": c["capital_lat"],
            "longitude": c["capital_lon"],
            "current": "european_aqi,pm2_5,pm10",
        }
        data = fetch_open_meteo(OPEN_METEO_AIR_QUALITY, params)
        current = data.get("current", {})
        print(f"[{timestamp()}] {c['capital']}: AQI={current.get('european_aqi')}")
        results.append({"code": c.get("code"), "aqi": current})
    return results


@task
def enrich_profiles(
    weather_data: list[dict[str, object]],
    aqi_data: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Merge country metadata + weather + AQI into unified profiles."""
    aqi_map: dict[str, dict[str, object]] = {}
    for a in aqi_data:
        aqi_map[str(a.get("code", ""))] = a.get("aqi", {})  # type: ignore[assignment]

    profiles: list[dict[str, object]] = []
    for w in weather_data:
        code = str(w.get("code", ""))
        weather: dict[str, object] = w.get("weather", {})  # type: ignore[assignment]
        aqi: dict[str, object] = aqi_map.get(code, {})
        profiles.append({
            "code": code,
            "name": w.get("name"),
            "capital": w.get("capital"),
            "population": w.get("population"),
            "area": w.get("area"),
            "temperature_c": weather.get("temperature_2m"),
            "wind_kmh": weather.get("wind_speed_10m"),
            "humidity_pct": weather.get("relative_humidity_2m"),
            "european_aqi": aqi.get("european_aqi"),
            "pm2_5": aqi.get("pm2_5"),
        })

    print(f"[{timestamp()}] Enriched {len(profiles)} country profiles")
    return profiles


@task
def compute_rankings(profiles: list[dict[str, object]]) -> dict[str, object]:
    """Rank countries by temperature, density, AQI, and area."""
    import pandas as pd

    df = pd.DataFrame(profiles)
    df["density"] = df["population"].astype(float) / df["area"].astype(float)

    rankings: dict[str, list[str]] = {}
    for col, ascending in [("temperature_c", False), ("density", True), ("european_aqi", True), ("area", False)]:
        ranked = df.sort_values(col, ascending=ascending)["name"].tolist()
        rankings[col] = ranked
        print(f"[{timestamp()}] Ranking by {col}: {ranked}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(f"{OUTPUT_DIR}/profiles.csv", index=False)
    return {"rankings": rankings}


@task
def report(profiles: list[dict[str, object]], rankings: dict[str, object]) -> None:
    """Print enriched country profiles with weather and AQI overlay."""
    print(f"\n[{timestamp()}] === Enriched Country Profiles ===")
    print(f"  {'Name':<12} {'Capital':<14} {'Pop':>10} {'Temp(C)':>8} {'Wind':>6} {'AQI':>5} {'PM2.5':>6}")
    print(f"  {'-' * 70}")
    for p in profiles:
        print(f"  {str(p.get('name', '')):<12} {str(p.get('capital', '')):<14} "
              f"{p.get('population', ''):>10} {p.get('temperature_c', ''):>8} "
              f"{p.get('wind_kmh', ''):>6} {p.get('european_aqi', ''):>5} {p.get('pm2_5', ''):>6}")

    rank_data: dict[str, list[str]] = rankings.get("rankings", {})  # type: ignore[assignment]
    print("\n  Rankings:")
    for metric, order in rank_data.items():
        print(f"    {metric}: {' > '.join(str(n) for n in order)}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="090_country_weather_enrichment",
    default_args=DEFAULT_ARGS,
    description="Cross-API country + weather + AQI enrichment pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    countries = fetch_countries()
    weather = fetch_capital_weather(countries=countries)
    aqi = fetch_capital_air_quality(countries=countries)
    profiles = enrich_profiles(weather_data=weather, aqi_data=aqi)
    ranked = compute_rankings(profiles=profiles)
    report(profiles=profiles, rankings=ranked)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

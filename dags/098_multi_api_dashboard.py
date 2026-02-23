"""DAG 98: 6-API Country Dashboard.

Orchestrates 6 different public APIs (REST Countries, Open-Meteo Forecast,
Open-Meteo Air Quality, Frankfurter, World Bank) to build a comprehensive
Nordic country dashboard with staging layers and a composite livability score.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/98_dashboard")
STAGING_DIR = f"{OUTPUT_DIR}/staging"


@task
def fetch_country_base() -> list[dict[str, object]]:
    """Fetch Nordic countries from REST Countries."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/alpha?codes=NOR,SWE,DNK,FIN,ISL"
    params = {"fields": "name,cca3,capital,capitalInfo,population,area,currencies"}
    data = fetch_json(url, params=params)
    countries: list[dict[str, object]] = data if isinstance(data, list) else []
    print(f"[{timestamp()}] REST Countries: {len(countries)} Nordic countries")
    return countries


@task
def fetch_weather(countries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Fetch Open-Meteo forecast for each capital."""
    from airflow_examples.apis import OPEN_METEO_FORECAST, fetch_open_meteo

    results: list[dict[str, object]] = []
    for c in countries:
        cap_info: dict[str, object] = c.get("capitalInfo", {})  # type: ignore[assignment]
        latlng: list[float] = cap_info.get("latlng", [0, 0])  # type: ignore[assignment]
        params = {
            "latitude": latlng[0] if len(latlng) > 0 else 0,
            "longitude": latlng[1] if len(latlng) > 1 else 0,
            "current": "temperature_2m,wind_speed_10m,weather_code",
        }
        data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
        results.append({"code": c.get("cca3"), "weather": data.get("current", {})})
    print(f"[{timestamp()}] Weather: {len(results)} capital forecasts")
    return results


@task
def fetch_air_quality(countries: list[dict[str, object]]) -> list[dict[str, object]]:
    """Fetch Open-Meteo AQI for each capital."""
    from airflow_examples.apis import OPEN_METEO_AIR_QUALITY, fetch_open_meteo

    results: list[dict[str, object]] = []
    for c in countries:
        cap_info: dict[str, object] = c.get("capitalInfo", {})  # type: ignore[assignment]
        latlng: list[float] = cap_info.get("latlng", [0, 0])  # type: ignore[assignment]
        params = {
            "latitude": latlng[0] if len(latlng) > 0 else 0,
            "longitude": latlng[1] if len(latlng) > 1 else 0,
            "current": "european_aqi,pm2_5",
        }
        data = fetch_open_meteo(OPEN_METEO_AIR_QUALITY, params)
        results.append({"code": c.get("cca3"), "aqi": data.get("current", {})})
    print(f"[{timestamp()}] AQI: {len(results)} capital air quality readings")
    return results


@task
def fetch_economics() -> dict[str, object]:
    """Fetch EUR exchange rates for Nordic currencies from Frankfurter."""
    from airflow_examples.apis import FRANKFURTER, fetch_json

    data = fetch_json(f"{FRANKFURTER}/latest", params={"from": "EUR", "to": "NOK,SEK,DKK"})
    rates: dict[str, float] = data.get("rates", {})  # type: ignore[assignment]
    print(f"[{timestamp()}] Frankfurter: EUR rates -> {rates}")
    return {"rates": rates, "date": data.get("date")}


@task
def fetch_development() -> list[dict[str, object]]:
    """Fetch GDP per capita from World Bank for Nordic countries."""
    from airflow_examples.apis import fetch_world_bank_paginated

    records = fetch_world_bank_paginated("NOR;SWE;DNK;FIN;ISL", "NY.GDP.PCAP.CD", "2020:2023")
    print(f"[{timestamp()}] World Bank GDP: {len(records)} records")
    return records


@task
def stage_raw(
    countries: list[dict[str, object]],
    weather: list[dict[str, object]],
    aqi: list[dict[str, object]],
    economics: dict[str, object],
    development: list[dict[str, object]],
) -> dict[str, object]:
    """Write each raw response to staging Parquet files."""
    import pandas as pd

    os.makedirs(STAGING_DIR, exist_ok=True)

    pd.DataFrame(countries).to_parquet(f"{STAGING_DIR}/countries.parquet")
    pd.DataFrame(weather).to_parquet(f"{STAGING_DIR}/weather.parquet")
    pd.DataFrame(aqi).to_parquet(f"{STAGING_DIR}/aqi.parquet")
    pd.DataFrame([economics]).to_parquet(f"{STAGING_DIR}/economics.parquet")
    pd.DataFrame(development).to_parquet(f"{STAGING_DIR}/development.parquet")

    files = os.listdir(STAGING_DIR)
    print(f"[{timestamp()}] Staged {len(files)} Parquet files to {STAGING_DIR}")
    return {"staged_files": len(files)}


@task
def integrate(
    countries: list[dict[str, object]],
    weather: list[dict[str, object]],
    aqi: list[dict[str, object]],
    economics: dict[str, object],
    development: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Join all sources on country code into unified DataFrame."""
    weather_map = {str(w.get("code", "")): w.get("weather", {}) for w in weather}
    aqi_map = {str(a.get("code", "")): a.get("aqi", {}) for a in aqi}
    rates: dict[str, float] = economics.get("rates", {})  # type: ignore[assignment]

    gdp_map: dict[str, float] = {}
    for r in development:
        country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
        val = r.get("value")
        if val is not None:
            c_code = str(country_info.get("id", ""))
            gdp_map[c_code] = float(str(val))

    currency_map = {"NOR": "NOK", "SWE": "SEK", "DNK": "DKK", "FIN": "EUR", "ISL": "ISK"}

    integrated: list[dict[str, object]] = []
    for c in countries:
        name_obj: dict[str, object] = c.get("name", {})  # type: ignore[assignment]
        code = str(c.get("cca3", ""))
        w: dict[str, object] = weather_map.get(code, {})  # type: ignore[assignment]
        aq: dict[str, object] = aqi_map.get(code, {})  # type: ignore[assignment]
        curr = currency_map.get(code, "EUR")

        integrated.append({
            "code": code,
            "name": name_obj.get("common"),
            "population": c.get("population"),
            "area": c.get("area"),
            "temperature_c": w.get("temperature_2m"),
            "wind_kmh": w.get("wind_speed_10m"),
            "european_aqi": aq.get("european_aqi"),
            "currency": curr,
            "eur_rate": rates.get(curr, 1.0),
            "gdp_per_capita": gdp_map.get(code),
        })

    print(f"[{timestamp()}] Integrated {len(integrated)} country profiles from 6 APIs")
    return integrated


@task
def compute_dashboard_metrics(integrated: list[dict[str, object]]) -> list[dict[str, object]]:
    """Derive composite livability score from weather + AQI + GDP + currency stability."""
    import pandas as pd

    df = pd.DataFrame(integrated)
    if len(df) == 0:
        return []

    for col in ["temperature_c", "european_aqi", "gdp_per_capita"]:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            col_min = numeric.min()
            col_max = numeric.max()
            if col_max > col_min:
                df[f"{col}_norm"] = (numeric - col_min) / (col_max - col_min) * 100
            else:
                df[f"{col}_norm"] = 50

    components: list[str] = [c for c in df.columns if c.endswith("_norm")]
    if components:
        df["livability_score"] = df[components].mean(axis=1).round(1)
    else:
        df["livability_score"] = 50

    df = df.sort_values("livability_score", ascending=False)
    print(f"[{timestamp()}] Livability scores:")
    for _, row in df.iterrows():
        print(f"  {row['name']}: {row['livability_score']}")

    df.to_csv(f"{OUTPUT_DIR}/dashboard.csv", index=False)
    return df.to_dict(orient="records")


@task
def report(dashboard: list[dict[str, object]], staging: dict[str, object]) -> None:
    """Print full dashboard output with per-country profiles."""
    print(f"\n[{timestamp()}] === 6-API Country Dashboard ===")
    print(f"  Staging files: {staging.get('staged_files')}")
    print(f"\n  {'Name':<12} {'Pop':>10} {'Temp':>6} {'AQI':>5} {'GDP/cap':>10} {'Livability':>10}")
    print(f"  {'-' * 60}")
    def fmt(val: object, width: int) -> str:
        """Format a value for display, replacing None with '-'."""
        return f"{val if val is not None else '-':>{width}}"

    for d in dashboard:
        print(f"  {str(d.get('name') or ''):<12} {fmt(d.get('population'), 10)} "
              f"{fmt(d.get('temperature_c'), 6)} {fmt(d.get('european_aqi'), 5)} "
              f"{fmt(d.get('gdp_per_capita'), 10)} {fmt(d.get('livability_score'), 10)}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="098_multi_api_dashboard",
    default_args=DEFAULT_ARGS,
    description="6-API country dashboard with staging layers and livability score",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    base = fetch_country_base()
    weather = fetch_weather(countries=base)
    aqi = fetch_air_quality(countries=base)
    econ = fetch_economics()
    dev = fetch_development()
    staged = stage_raw(countries=base, weather=weather, aqi=aqi, economics=econ, development=dev)
    joined = integrate(countries=base, weather=weather, aqi=aqi, economics=econ, development=dev)
    dashboard = compute_dashboard_metrics(integrated=joined)
    report(dashboard=dashboard, staging=staged)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

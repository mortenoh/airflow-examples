"""DAG 86: Geocoding-Driven Weather Pipeline.

Uses the Open-Meteo Geocoding API to resolve city names to coordinates,
handles disambiguation for ambiguous names like "Springfield", fetches
weather for resolved coordinates, and enriches with elevation data.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/86_geocoding")

SEARCH_CITIES = ["Oslo", "Bergen", "Hammerfest", "Longyearbyen", "Springfield"]


@task
def geocode_cities() -> list[dict[str, object]]:
    """Resolve city names to coordinates via Open-Meteo Geocoding API."""
    from airflow_examples.apis import OPEN_METEO_GEOCODING, fetch_json

    results: list[dict[str, object]] = []
    for name in SEARCH_CITIES:
        data = fetch_json(OPEN_METEO_GEOCODING, params={"name": name, "count": 5})
        matches: list[dict[str, object]] = data.get("results", [])  # type: ignore[assignment]
        print(f"[{timestamp()}] '{name}': {len(matches)} geocoding results")
        for m in matches[:3]:
            print(f"    {m.get('name')}, {m.get('country')} ({m.get('latitude')}, {m.get('longitude')})")
        results.append({"search": name, "matches": matches})
    return results


@task
def handle_disambiguation(geocoded: list[dict[str, object]]) -> list[dict[str, object]]:
    """Select best match per city, preferring Norway for ambiguous results."""
    resolved: list[dict[str, object]] = []
    for entry in geocoded:
        search = str(entry["search"])
        matches: list[dict[str, object]] = entry.get("matches", [])  # type: ignore[assignment]

        if not matches:
            print(f"[{timestamp()}] '{search}': no results found, skipping")
            continue

        norway_matches = [m for m in matches if m.get("country_code") == "NO"]
        if norway_matches:
            best = norway_matches[0]
            reason = "country filter (Norway)"
        else:
            best = matches[0]
            reason = "first result"

        print(f"[{timestamp()}] '{search}' -> {best.get('name')}, {best.get('country')} ({reason})")
        resolved.append({
            "name": best.get("name"),
            "search_term": search,
            "country": best.get("country"),
            "lat": best.get("latitude"),
            "lon": best.get("longitude"),
            "disambiguation": reason,
        })
    return resolved


@task
def fetch_weather(resolved: list[dict[str, object]]) -> list[dict[str, object]]:
    """Fetch current weather for each resolved location."""
    from airflow_examples.apis import OPEN_METEO_FORECAST, fetch_open_meteo

    results: list[dict[str, object]] = []
    for loc in resolved:
        params = {
            "latitude": loc["lat"],
            "longitude": loc["lon"],
            "current": "temperature_2m,wind_speed_10m,weather_code",
        }
        data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
        current = data.get("current", {})
        print(f"[{timestamp()}] {loc['name']}: temp={current.get('temperature_2m')}C, "
              f"wind={current.get('wind_speed_10m')}km/h")
        results.append({**loc, "weather": current})
    return results


@task
def enrich_with_elevation(weather_data: list[dict[str, object]]) -> list[dict[str, object]]:
    """Add elevation data from Open-Meteo Elevation API."""
    from airflow_examples.apis import OPEN_METEO_ELEVATION, fetch_json

    lats = [str(loc["lat"]) for loc in weather_data]
    lons = [str(loc["lon"]) for loc in weather_data]

    data = fetch_json(OPEN_METEO_ELEVATION, params={
        "latitude": ",".join(lats),
        "longitude": ",".join(lons),
    })
    elevations: list[object] = data.get("elevation", [])  # type: ignore[assignment]

    enriched: list[dict[str, object]] = []
    for i, loc in enumerate(weather_data):
        elev = elevations[i] if i < len(elevations) else None
        enriched.append({**loc, "elevation_m": elev})
        print(f"[{timestamp()}] {loc['name']}: elevation={elev}m")
    return enriched


@task
def report(enriched: list[dict[str, object]]) -> None:
    """Print resolved locations with coordinates, elevation, and weather."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n[{timestamp()}] === Geocoding Weather Report ===")
    print(f"  {'Name':<16} {'Country':<10} {'Lat':>7} {'Lon':>7} {'Elev(m)':>8} "
          f"{'Temp(C)':>8} {'Wind':>6} {'Via'}")
    print(f"  {'-' * 90}")
    for loc in enriched:
        weather: dict[str, object] = loc.get("weather", {})  # type: ignore[assignment]
        print(f"  {str(loc.get('name', '')):<16} {str(loc.get('country', '')):<10} "
              f"{loc.get('lat', ''):>7} {loc.get('lon', ''):>7} {loc.get('elevation_m', ''):>8} "
              f"{weather.get('temperature_2m', ''):>8} {weather.get('wind_speed_10m', ''):>6} "
              f"{loc.get('disambiguation', '')}")


with DAG(
    dag_id="086_geocoding_weather",
    default_args=DEFAULT_ARGS,
    description="Geocoding-driven weather pipeline with disambiguation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    geo = geocode_cities()
    resolved = handle_disambiguation(geocoded=geo)
    weather = fetch_weather(resolved=resolved)
    enriched = enrich_with_elevation(weather_data=weather)
    report(enriched=enriched)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

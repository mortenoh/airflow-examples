"""Public JSON API helpers for pipeline examples.

Provides base URLs for free, no-auth public APIs and utility functions
for fetching and flattening API responses. All fetch functions use
``httpx.get`` internally, making them easy to mock in tests.
"""

from typing import Any

import httpx

from airflow_examples.config import OUTPUT_BASE

# -- Open-Meteo ecosystem -----------------------------------------------------
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_AIR_QUALITY = "https://air-quality-api.open-meteo.com/v1/air-quality"
OPEN_METEO_MARINE = "https://marine-api.open-meteo.com/v1/marine"
OPEN_METEO_FLOOD = "https://flood-api.open-meteo.com/v1/flood"
OPEN_METEO_GEOCODING = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_ELEVATION = "https://api.open-meteo.com/v1/elevation"

# -- Other public APIs ---------------------------------------------------------
SUNRISE_SUNSET = "https://api.sunrise-sunset.org/json"
REST_COUNTRIES = "https://restcountries.com/v3.1"
WORLD_BANK = "https://api.worldbank.org/v2"
FRANKFURTER = "https://api.frankfurter.app"
USGS_EARTHQUAKE = "https://earthquake.usgs.gov/fdsnws/event/1/query"
CARBON_INTENSITY = "https://api.carbonintensity.org.uk"
WHO_GHO = "https://ghoapi.azureedge.net/api"

# -- Shared constants ----------------------------------------------------------
OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines")

NORDIC_CITIES: list[dict[str, Any]] = [
    {"name": "Oslo", "lat": 59.91, "lon": 10.75},
    {"name": "Bergen", "lat": 60.39, "lon": 5.32},
    {"name": "Tromso", "lat": 69.65, "lon": 18.96},
    {"name": "Stavanger", "lat": 58.97, "lon": 5.73},
    {"name": "Trondheim", "lat": 63.43, "lon": 10.40},
]


def fetch_json(url: str, params: dict[str, Any] | None = None, timeout: int = 30) -> dict[str, Any]:
    """Fetch JSON from a URL with optional query parameters.

    Args:
        url: The API endpoint URL.
        params: Optional query parameters.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dict.
    """
    resp = httpx.get(url, params=params, timeout=timeout)
    resp.raise_for_status()
    result: dict[str, Any] = resp.json()
    return result


def fetch_open_meteo(base_url: str, params: dict[str, Any]) -> dict[str, Any]:
    """Fetch data from any Open-Meteo API endpoint.

    Automatically adds ``timezone=auto`` to the request parameters.

    Args:
        base_url: The Open-Meteo API base URL.
        params: Query parameters (lat, lon, hourly, etc.).

    Returns:
        Parsed JSON response.
    """
    merged = {**params, "timezone": "auto"}
    return fetch_json(base_url, params=merged)


def fetch_world_bank_paginated(
    country_codes: str,
    indicator: str,
    date_range: str = "2000:2023",
    per_page: int = 500,
) -> list[dict[str, Any]]:
    """Fetch a World Bank indicator with pagination.

    The World Bank API returns ``[metadata, records]`` per page. This function
    iterates all pages and concatenates the record arrays.

    Args:
        country_codes: Semicolon-separated ISO3 country codes (e.g. "NOR;SWE").
        indicator: World Bank indicator code (e.g. "NY.GDP.PCAP.CD").
        date_range: Year range in ``start:end`` format.
        per_page: Records per page.

    Returns:
        Flat list of indicator records across all pages.
    """
    all_records: list[dict[str, Any]] = []
    page = 1
    while True:
        url = f"{WORLD_BANK}/country/{country_codes}/indicator/{indicator}"
        params: dict[str, Any] = {
            "format": "json",
            "date": date_range,
            "per_page": per_page,
            "page": page,
        }
        resp = httpx.get(url, params=params, timeout=60)
        resp.raise_for_status()
        data: Any = resp.json()

        if not isinstance(data, list) or len(data) < 2 or data[1] is None:
            break

        records: list[dict[str, Any]] = data[1]
        all_records.extend(records)

        metadata: dict[str, Any] = data[0]
        total_pages = int(metadata.get("pages", 1))
        if page >= total_pages:
            break
        page += 1

    return all_records


def fetch_who_indicator(
    indicator_code: str,
    country_code: str | None = None,
    top: int = 500,
) -> list[dict[str, Any]]:
    """Fetch a WHO Global Health Observatory indicator via OData.

    Args:
        indicator_code: GHO indicator code (e.g. "WHOSIS_000001").
        country_code: Optional ISO3 country code filter.
        top: Maximum number of records.

    Returns:
        List of indicator value records.
    """
    url = f"{WHO_GHO}/{indicator_code}"
    filters: list[str] = []
    if country_code:
        filters.append(f"SpatialDim eq '{country_code}'")
    params: dict[str, Any] = {"$top": top}
    if filters:
        params["$filter"] = " and ".join(filters)

    resp = httpx.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    result: list[dict[str, Any]] = data.get("value", [])
    return result


def fetch_usgs_earthquakes(
    start: str,
    end: str,
    min_magnitude: float = 4.0,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Fetch earthquakes from USGS and flatten GeoJSON features.

    Args:
        start: Start date (YYYY-MM-DD).
        end: End date (YYYY-MM-DD).
        min_magnitude: Minimum magnitude threshold.
        limit: Maximum number of events.

    Returns:
        List of flat dicts with lat, lon, depth, magnitude, place, time.
    """
    params: dict[str, Any] = {
        "format": "geojson",
        "starttime": start,
        "endtime": end,
        "minmagnitude": min_magnitude,
        "limit": limit,
    }
    data = fetch_json(USGS_EARTHQUAKE, params=params)
    features: list[dict[str, Any]] = data.get("features", [])

    flat: list[dict[str, Any]] = []
    for f in features:
        props: dict[str, Any] = f.get("properties", {})
        coords: list[float] = f.get("geometry", {}).get("coordinates", [0, 0, 0])
        flat.append({
            "lon": coords[0],
            "lat": coords[1],
            "depth": coords[2] if len(coords) > 2 else 0,
            "magnitude": props.get("mag"),
            "place": props.get("place"),
            "time": props.get("time"),
        })
    return flat


def flatten_carbon_intensity(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten UK Carbon Intensity API nested response.

    The API returns nested records with ``intensity`` containing forecast, actual, and index fields.

    Args:
        data: List of raw carbon intensity records.

    Returns:
        Flat list of dicts with from, to, forecast, actual, index fields.
    """
    flat: list[dict[str, Any]] = []
    for record in data:
        intensity: dict[str, Any] = record.get("intensity", {})
        flat.append({
            "from": record.get("from"),
            "to": record.get("to"),
            "forecast": intensity.get("forecast"),
            "actual": intensity.get("actual"),
            "index": intensity.get("index"),
        })
    return flat


def flatten_generation_mix(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flatten UK Carbon Intensity generation mix response.

    Each time slot contains a ``generationmix`` array of ``{"fuel": ..., "perc": ...}`` entries.

    Args:
        data: List of raw generation mix time-slot records.

    Returns:
        One row per time-slot per fuel type.
    """
    flat: list[dict[str, Any]] = []
    for record in data:
        time_from = record.get("from")
        time_to = record.get("to")
        mix: list[dict[str, Any]] = record.get("generationmix", [])
        for entry in mix:
            flat.append({
                "from": time_from,
                "to": time_to,
                "fuel": entry.get("fuel"),
                "perc": entry.get("perc"),
            })
    return flat

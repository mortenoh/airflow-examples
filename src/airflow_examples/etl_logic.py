"""Pure business logic for ETL pipelines, testable without Airflow."""

import csv
import random
from typing import Any

import httpx


def extract_weather_data(n_records: int, seed: int = 42) -> list[dict[str, Any]]:
    """Generate sample weather records for ETL demonstration.

    Args:
        n_records: Number of records to generate.
        seed: Random seed for reproducibility.

    Returns:
        List of weather observation dicts.
    """
    rng = random.Random(seed)
    stations = ["oslo_01", "bergen_01", "tromso_01", "stavanger_01", "trondheim_01"]

    records: list[dict[str, Any]] = []
    for i in range(n_records):
        records.append({
            "station": rng.choice(stations),
            "date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "temperature_c": round(rng.uniform(-15, 35), 1),
            "humidity_pct": round(rng.uniform(20, 100), 1),
            "pressure_hpa": round(rng.uniform(970, 1050), 1),
            "wind_speed_ms": round(rng.uniform(0, 25), 1),
        })

    return records


def validate_records(
    records: list[dict[str, Any]],
    required_fields: list[str],
    temp_range: tuple[float, float] = (-50.0, 60.0),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split records into valid and invalid based on field and range checks.

    Args:
        records: List of record dicts to validate.
        required_fields: Fields that must be present and non-None.
        temp_range: Allowed (min, max) temperature range.

    Returns:
        Tuple of (valid_records, invalid_records).
    """
    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []

    for record in records:
        is_valid = True

        # Check required fields
        for field in required_fields:
            if field not in record or record[field] is None:
                is_valid = False
                break

        # Check temperature range
        if is_valid and "temperature_c" in record:
            temp = record["temperature_c"]
            if not isinstance(temp, (int, float)) or temp < temp_range[0] or temp > temp_range[1]:
                is_valid = False

        if is_valid:
            valid.append(record)
        else:
            invalid.append(record)

    return valid, invalid


def transform_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply unit conversions and derive new fields.

    Adds temperature_f (Fahrenheit) and wind_category derived fields.

    Args:
        records: List of validated record dicts.

    Returns:
        List of transformed records with additional fields.
    """
    transformed: list[dict[str, Any]] = []
    for record in records:
        row = dict(record)

        # Celsius to Fahrenheit
        if "temperature_c" in row:
            row["temperature_f"] = round(row["temperature_c"] * 9 / 5 + 32, 1)

        # Wind category
        if "wind_speed_ms" in row:
            speed = row["wind_speed_ms"]
            if speed < 1:
                row["wind_category"] = "calm"
            elif speed < 5:
                row["wind_category"] = "light"
            elif speed < 10:
                row["wind_category"] = "moderate"
            elif speed < 20:
                row["wind_category"] = "strong"
            else:
                row["wind_category"] = "storm"

        transformed.append(row)

    return transformed


def load_records(records: list[dict[str, Any]], output_path: str) -> dict[str, Any]:
    """Write records to CSV and return summary statistics.

    Args:
        records: List of record dicts to write.
        output_path: CSV file path.

    Returns:
        Summary dict with row count, column count, and output path.
    """
    if not records:
        return {"rows": 0, "columns": 0, "path": output_path}

    fieldnames = list(records[0].keys())
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    return {"rows": len(records), "columns": len(fieldnames), "path": output_path}


def fetch_api_data(url: str, timeout: int = 30) -> dict[str, Any]:
    """Fetch data from an API endpoint (mockable in tests).

    Args:
        url: API URL to fetch.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dict.
    """
    resp = httpx.get(url, timeout=timeout)
    resp.raise_for_status()
    result: dict[str, Any] = resp.json()
    return result

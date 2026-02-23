"""File-based ETL utilities for landing zone, archive, and quarantine patterns."""

import csv
import json
import os
import random
import shutil
from datetime import datetime, timedelta
from typing import Any

from airflow_examples.config import OUTPUT_BASE

LANDING_DIR = str(OUTPUT_BASE / "landing")
PROCESSED_DIR = str(OUTPUT_BASE / "processed")
ARCHIVE_DIR = str(OUTPUT_BASE / "archive")
QUARANTINE_DIR = str(OUTPUT_BASE / "quarantine")


def setup_dirs(*dirs: str) -> None:
    """Create multiple directories, ignoring if they already exist.

    Args:
        *dirs: Directory paths to create.
    """
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def generate_climate_csv(path: str, n_rows: int, *, include_bad_rows: bool = False) -> str:
    """Generate a realistic climate CSV with optional bad rows for testing.

    Args:
        path: Output CSV file path.
        n_rows: Number of data rows to generate.
        include_bad_rows: If True, inject ~10% malformed rows.

    Returns:
        The output file path.
    """
    stations = ["oslo_01", "bergen_01", "tromso_01", "stavanger_01", "trondheim_01"]
    headers = ["station", "date", "temperature_f", "humidity_pct", "pressure_hpa"]

    rng = random.Random(hash(path) % 2**32)
    base_date = datetime(2024, 1, 1)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(n_rows):
            if include_bad_rows and rng.random() < 0.1:
                # Bad row: missing fields or non-numeric values
                writer.writerow([rng.choice(stations), "bad-date", "not_a_number", "", ""])
            else:
                date = (base_date + timedelta(days=i % 365)).strftime("%Y-%m-%d")
                temp_f = round(rng.uniform(14, 95), 1)
                humidity = round(rng.uniform(30, 100), 1)
                pressure = round(rng.uniform(980, 1040), 1)
                writer.writerow([rng.choice(stations), date, temp_f, humidity, pressure])

    return path


def generate_event_json(path: str, n_events: int) -> str:
    """Generate a JSON weather observation event file with nested station/location.

    Args:
        path: Output JSON file path.
        n_events: Number of observation events.

    Returns:
        The output file path.
    """
    rng = random.Random(hash(path) % 2**32)
    stations = [
        {"id": "oslo_01", "name": "Oslo Central", "location": {"lat": 59.91, "lon": 10.75}},
        {"id": "bergen_01", "name": "Bergen Harbor", "location": {"lat": 60.39, "lon": 5.32}},
        {"id": "tromso_01", "name": "Tromso Arctic", "location": {"lat": 69.65, "lon": 18.96}},
    ]
    base_date = datetime(2024, 6, 1)

    events: list[dict[str, Any]] = []
    for i in range(n_events):
        station = rng.choice(stations)
        events.append({
            "event_id": f"evt_{i:04d}",
            "timestamp": (base_date + timedelta(hours=i * 6)).isoformat(),
            "station": station,
            "observations": {
                "temperature_c": round(rng.uniform(-5, 30), 1),
                "humidity_pct": round(rng.uniform(30, 100), 1),
                "pressure_hpa": round(rng.uniform(980, 1040), 1),
                "wind_speed_ms": round(rng.uniform(0, 20), 1),
            },
        })

    with open(path, "w") as f:
        json.dump(events, f, indent=2)

    return path


def archive_file(src: str, archive_dir: str) -> str:
    """Move a file to the archive directory with a timestamp prefix.

    Args:
        src: Source file path.
        archive_dir: Target archive directory.

    Returns:
        The new archive file path.
    """
    os.makedirs(archive_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    basename = os.path.basename(src)
    dest = os.path.join(archive_dir, f"{ts}_{basename}")
    shutil.move(src, dest)
    return dest


def quarantine_file(src: str, quarantine_dir: str, reason: str) -> str:
    """Move a bad file to quarantine and write a companion reason file.

    Args:
        src: Source file path.
        quarantine_dir: Target quarantine directory.
        reason: Human-readable reason for quarantine.

    Returns:
        The quarantine file path.
    """
    os.makedirs(quarantine_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    basename = os.path.basename(src)
    dest = os.path.join(quarantine_dir, f"{ts}_{basename}")
    shutil.move(src, dest)

    reason_path = dest + ".reason"
    with open(reason_path, "w") as f:
        f.write(f"file: {basename}\ntimestamp: {ts}\nreason: {reason}\n")

    return dest

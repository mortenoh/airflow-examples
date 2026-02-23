"""DAG 63: CSV Landing Zone Pipeline.

Demonstrates a file-based ETL pattern: watch a landing directory for CSV
files, validate structure, parse and transform data, write output, and
archive originals with timestamp prefixes. Shows the classic
landing -> processed -> archive directory workflow.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.file_utils import (
    ARCHIVE_DIR,
    LANDING_DIR,
    PROCESSED_DIR,
    archive_file,
    generate_climate_csv,
    setup_dirs,
)


@task
def setup() -> None:
    """Create landing/processed/archive dirs and generate sample CSVs."""
    setup_dirs(LANDING_DIR, PROCESSED_DIR, ARCHIVE_DIR)
    for i in range(3):
        path = f"{LANDING_DIR}/weather_batch_{i}.csv"
        generate_climate_csv(path, n_rows=50)
        print(f"[{timestamp()}] Generated {path}")
    print(f"[{timestamp()}] Setup complete: 3 CSVs in {LANDING_DIR}")


@task
def detect_files() -> list[str]:
    """Glob the landing directory for CSV files."""
    import glob

    files = sorted(glob.glob(f"{LANDING_DIR}/*.csv"))
    print(f"[{timestamp()}] Detected {len(files)} CSV files")
    for f in files:
        print(f"  {f}")
    return files


@task
def process_file(files: list[str]) -> dict[str, object]:
    """Read, validate, and transform each CSV file."""
    import pandas as pd

    total_rows = 0
    output_paths: list[str] = []

    for file_path in files:
        df = pd.read_csv(file_path)
        expected = {"station", "date", "temperature_f", "humidity_pct", "pressure_hpa"}
        actual = set(df.columns)
        if not expected.issubset(actual):
            print(f"[{timestamp()}] WARN: {file_path} missing columns: {expected - actual}")
            continue

        # Convert temperature F -> C
        df["temperature_c"] = ((df["temperature_f"] - 32) * 5 / 9).round(1)
        df = df.drop(columns=["temperature_f"])

        # Handle nulls
        df = df.fillna({"humidity_pct": 50.0, "pressure_hpa": 1013.0})

        import os

        basename = os.path.basename(file_path)
        output_path = f"{PROCESSED_DIR}/{basename}"
        df.to_csv(output_path, index=False)

        total_rows += len(df)
        output_paths.append(output_path)
        print(f"[{timestamp()}] Processed {basename}: {len(df)} rows -> {output_path}")

    return {"total_rows": total_rows, "output_paths": output_paths}


@task
def archive_originals(files: list[str]) -> list[str]:
    """Move processed CSVs from landing to archive with timestamp prefix."""
    archived: list[str] = []
    for f in files:
        dest = archive_file(f, ARCHIVE_DIR)
        archived.append(dest)
        print(f"[{timestamp()}] Archived: {dest}")
    return archived


@task
def report(stats: dict[str, object], archived: list[str]) -> None:
    """Print pipeline summary."""
    print(f"[{timestamp()}] === CSV Landing Zone Pipeline Complete ===")
    print(f"  Files processed: {len(archived)}")
    print(f"  Total rows: {stats['total_rows']}")
    print(f"  Output dir: {PROCESSED_DIR}")
    print(f"  Archive dir: {ARCHIVE_DIR}")
    for a in archived:
        print(f"    {a}")


with DAG(
    dag_id="063_csv_landing_zone",
    default_args=DEFAULT_ARGS,
    description="CSV landing zone -> validate -> transform -> archive pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "etl"],
) as dag:
    s = setup()
    files = detect_files()
    stats = process_file(files=files)
    archived = archive_originals(files=files)
    r = report(stats=stats, archived=archived)

    s >> files >> [stats, archived] >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {LANDING_DIR} {PROCESSED_DIR} {ARCHIVE_DIR} && echo 'Cleaned up ETL dirs'",
    )
    r >> cleanup

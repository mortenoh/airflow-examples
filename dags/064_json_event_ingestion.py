"""DAG 64: JSON Event Stream to Parquet.

Demonstrates parsing JSON event files with nested structures,
normalizing to a flat DataFrame with ``pd.json_normalize()``, and
writing the result as Parquet. Shows event-to-columnar conversion
for analytics-ready output.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.file_utils import (
    ARCHIVE_DIR,
    LANDING_DIR,
    PROCESSED_DIR,
    archive_file,
    generate_event_json,
    setup_dirs,
)


@task
def setup() -> None:
    """Create directories and generate JSON event files."""
    setup_dirs(LANDING_DIR, PROCESSED_DIR, ARCHIVE_DIR)
    for i in range(5):
        path = f"{LANDING_DIR}/events_batch_{i}.json"
        generate_event_json(path, n_events=20)
        print(f"[{timestamp()}] Generated {path} (20 events)")
    print(f"[{timestamp()}] Setup complete: 5 JSON files")


@task
def detect_events() -> list[str]:
    """Glob landing directory for JSON files."""
    import glob

    files = sorted(glob.glob(f"{LANDING_DIR}/*.json"))
    print(f"[{timestamp()}] Detected {len(files)} JSON event files")
    return files


@task
def parse_and_normalize(files: list[str]) -> str:
    """Read JSON files, flatten nested structures, combine into single DataFrame."""
    import json

    import pandas as pd

    all_frames: list[pd.DataFrame] = []

    for file_path in files:
        with open(file_path) as f:
            events = json.load(f)

        # Flatten nested station/location and observations
        df = pd.json_normalize(
            events,
            sep="_",
        )
        all_frames.append(df)
        print(f"[{timestamp()}] Parsed {os.path.basename(file_path)}: {len(df)} events, {len(df.columns)} columns")

    combined = pd.concat(all_frames, ignore_index=True)
    print(f"[{timestamp()}] Combined: {len(combined)} total events")
    print(f"[{timestamp()}] Columns: {list(combined.columns)}")

    output_path = f"{PROCESSED_DIR}/events_combined.parquet"
    combined.to_parquet(output_path, index=False)
    print(f"[{timestamp()}] Wrote Parquet: {output_path}")
    return output_path


@task
def archive_events(files: list[str]) -> list[str]:
    """Move JSON files to archive."""
    archived: list[str] = []
    for f in files:
        dest = archive_file(f, ARCHIVE_DIR)
        archived.append(dest)
    print(f"[{timestamp()}] Archived {len(archived)} JSON files")
    return archived


@task
def report(parquet_path: str, archived: list[str]) -> None:
    """Print summary of ingested events."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    print(f"[{timestamp()}] === JSON Event Ingestion Complete ===")
    print(f"  Total events: {len(df)}")
    print(f"  Columns: {len(df.columns)}")
    print("  Schema:")
    for col in df.columns:
        print(f"    {col}: {df[col].dtype}")
    print(f"  Files archived: {len(archived)}")
    file_size = os.path.getsize(parquet_path)
    print(f"  Parquet size: {file_size:,} bytes")


with DAG(
    dag_id="064_json_event_ingestion",
    default_args=DEFAULT_ARGS,
    description="JSON event files -> json_normalize -> Parquet pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "etl"],
) as dag:
    s = setup()
    files = detect_events()
    parquet = parse_and_normalize(files=files)
    archived = archive_events(files=files)
    r = report(parquet_path=parquet, archived=archived)

    s >> files >> [parquet, archived] >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {LANDING_DIR} {PROCESSED_DIR} {ARCHIVE_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

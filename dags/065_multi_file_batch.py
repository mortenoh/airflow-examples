"""DAG 65: Multi-File Batch Processing.

Demonstrates processing a mixed batch of CSV and JSON files,
harmonizing column names across formats, merging into a unified
dataset, and deduplicating. Shows mixed-format ingestion with
column name standardization and batch merge.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.file_utils import (
    LANDING_DIR,
    PROCESSED_DIR,
    generate_climate_csv,
    generate_event_json,
    setup_dirs,
)


@task
def setup() -> None:
    """Create dirs and generate mixed CSV + JSON files with overlapping data."""
    setup_dirs(LANDING_DIR, PROCESSED_DIR)
    for i in range(3):
        generate_climate_csv(f"{LANDING_DIR}/climate_{i}.csv", n_rows=30)
    for i in range(2):
        generate_event_json(f"{LANDING_DIR}/observations_{i}.json", n_events=25)
    print(f"[{timestamp()}] Generated 3 CSVs + 2 JSONs in {LANDING_DIR}")


@task
def detect_and_classify() -> dict[str, list[str]]:
    """Glob landing dir and classify files by extension."""
    import glob

    csv_files = sorted(glob.glob(f"{LANDING_DIR}/*.csv"))
    json_files = sorted(glob.glob(f"{LANDING_DIR}/*.json"))
    print(f"[{timestamp()}] Found {len(csv_files)} CSV, {len(json_files)} JSON files")
    return {"csv": csv_files, "json": json_files}


@task
def process_csv_batch(file_map: dict[str, list[str]]) -> list[dict[str, object]]:
    """Read all CSVs, standardize column names, return as list of dicts."""
    import pandas as pd

    frames: list[pd.DataFrame] = []
    for path in file_map["csv"]:
        df = pd.read_csv(path)
        # Standardize: rename temperature_f -> temperature, add source
        rename_map: dict[str, str] = {}
        if "temperature_f" in df.columns:
            rename_map["temperature_f"] = "temperature"
        if "humidity_pct" in df.columns:
            rename_map["humidity_pct"] = "humidity"
        if "pressure_hpa" in df.columns:
            rename_map["pressure_hpa"] = "pressure"
        df = df.rename(columns=rename_map)
        df["source"] = "csv"
        df["source_file"] = os.path.basename(path)
        frames.append(df)
        print(f"[{timestamp()}] CSV: {os.path.basename(path)} -> {len(df)} rows")

    combined = pd.concat(frames, ignore_index=True)
    result: list[dict[str, object]] = combined.to_dict(orient="records")
    return result


@task
def process_json_batch(file_map: dict[str, list[str]]) -> list[dict[str, object]]:
    """Read all JSONs, normalize, standardize column names, return as list of dicts."""
    import json

    import pandas as pd

    frames: list[pd.DataFrame] = []
    for path in file_map["json"]:
        with open(path) as f:
            events = json.load(f)
        df = pd.json_normalize(events, sep="_")
        # Standardize nested columns
        rename_map: dict[str, str] = {
            "station_id": "station",
            "observations_temperature_c": "temperature",
            "observations_humidity_pct": "humidity",
            "observations_pressure_hpa": "pressure",
            "observations_wind_speed_ms": "wind_speed",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
        if "timestamp" in df.columns:
            df["date"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%d")
        df["source"] = "json"
        df["source_file"] = os.path.basename(path)
        frames.append(df)
        print(f"[{timestamp()}] JSON: {os.path.basename(path)} -> {len(df)} rows")

    combined = pd.concat(frames, ignore_index=True)
    result: list[dict[str, object]] = combined.to_dict(orient="records")
    return result


@task
def merge_datasets(
    csv_data: list[dict[str, object]], json_data: list[dict[str, object]]
) -> str:
    """Concat CSV + JSON DataFrames, deduplicate, sort by date."""
    import pandas as pd

    csv_df = pd.DataFrame(csv_data)
    json_df = pd.DataFrame(json_data)

    # Keep only common columns for merge
    common_cols = ["station", "date", "temperature", "humidity", "pressure", "source", "source_file"]
    csv_cols = [c for c in common_cols if c in csv_df.columns]
    json_cols = [c for c in common_cols if c in json_df.columns]

    merged = pd.concat([csv_df[csv_cols], json_df[json_cols]], ignore_index=True)
    before_dedup = len(merged)
    merged = merged.drop_duplicates(subset=["station", "date"], keep="first")
    merged = merged.sort_values("date").reset_index(drop=True)

    print(f"[{timestamp()}] Merged: {before_dedup} rows -> {len(merged)} after dedup")

    output_path = f"{PROCESSED_DIR}/unified_dataset.parquet"
    merged.to_parquet(output_path, index=False)
    return output_path


@task
def write_summary(parquet_path: str) -> str:
    """Write a summary CSV alongside the Parquet output."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    summary = df.groupby("source").agg(
        rows=("station", "count"),
        stations=("station", "nunique"),
    )
    summary_path = f"{PROCESSED_DIR}/merge_summary.csv"
    summary.to_csv(summary_path)
    print(f"[{timestamp()}] Summary:\n{summary.to_string()}")
    return summary_path


@task
def report(parquet_path: str, summary_path: str) -> None:
    """Print final pipeline report."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    print(f"[{timestamp()}] === Multi-File Batch Complete ===")
    print(f"  Total rows: {len(df)}")
    print(f"  Unique stations: {df['station'].nunique()}")
    print(f"  Sources: {dict(df['source'].value_counts())}")
    print(f"  Output: {parquet_path}")
    print(f"  Summary: {summary_path}")


with DAG(
    dag_id="065_multi_file_batch",
    default_args=DEFAULT_ARGS,
    description="Mixed CSV+JSON -> harmonize -> merge -> deduplicate pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "etl"],
) as dag:
    s = setup()
    classified = detect_and_classify()
    csv_data = process_csv_batch(file_map=classified)
    json_data = process_json_batch(file_map=classified)
    merged = merge_datasets(csv_data=csv_data, json_data=json_data)
    summary = write_summary(parquet_path=merged)
    r = report(parquet_path=merged, summary_path=summary)

    s >> classified >> [csv_data, json_data] >> merged >> summary >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {LANDING_DIR} {PROCESSED_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

"""DAG 67: Incremental File Processing with Manifest.

Demonstrates tracking processed files with a JSON manifest so that
only new arrivals are processed on each run. Shows manifest-based
state tracking, append-mode output, and idempotent reprocessing.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.file_utils import (
    LANDING_DIR,
    PROCESSED_DIR,
    generate_climate_csv,
    setup_dirs,
)

MANIFEST_PATH = f"{PROCESSED_DIR}/manifest.json"


@task
def setup() -> None:
    """Create dirs and generate a batch of CSVs."""
    setup_dirs(LANDING_DIR, PROCESSED_DIR)
    for i in range(5):
        path = f"{LANDING_DIR}/batch_{i:03d}.csv"
        generate_climate_csv(path, n_rows=20)
    print(f"[{timestamp()}] Generated 5 CSVs in {LANDING_DIR}")


@task
def detect_new_files() -> list[str]:
    """Glob landing dir, diff against manifest, return only unseen paths."""
    import glob
    import json
    import os

    all_files = sorted(glob.glob(f"{LANDING_DIR}/*.csv"))

    # Load existing manifest
    manifest: dict[str, object] = {}
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)

    processed_names: set[str] = set()
    entries = manifest.get("files", [])
    if isinstance(entries, list):
        for entry in entries:
            if isinstance(entry, dict) and "filename" in entry:
                processed_names.add(entry["filename"])

    new_files = [f for f in all_files if os.path.basename(f) not in processed_names]
    print(f"[{timestamp()}] All: {len(all_files)}, Processed: {len(processed_names)}, New: {len(new_files)}")
    return new_files


@task
def process_new_files(new_files: list[str]) -> dict[str, object]:
    """Parse and transform only new files, append to cumulative Parquet."""
    import os

    import pandas as pd

    if not new_files:
        print(f"[{timestamp()}] No new files to process")
        return {"new_rows": 0, "files_processed": []}

    frames: list[pd.DataFrame] = []
    for file_path in new_files:
        df = pd.read_csv(file_path)
        df["source_file"] = os.path.basename(file_path)
        frames.append(df)
        print(f"[{timestamp()}] Processed {os.path.basename(file_path)}: {len(df)} rows")

    new_df = pd.concat(frames, ignore_index=True)

    # Append to cumulative output
    output_path = f"{PROCESSED_DIR}/cumulative_output.parquet"
    if os.path.exists(output_path):
        existing = pd.read_parquet(output_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df

    combined.to_parquet(output_path, index=False)
    print(f"[{timestamp()}] Cumulative output: {len(combined)} total rows")

    return {
        "new_rows": len(new_df),
        "files_processed": [os.path.basename(f) for f in new_files],
        "cumulative_rows": len(combined),
    }


@task
def update_manifest(new_files: list[str], stats: dict[str, object]) -> None:
    """Add newly processed filenames + timestamps to manifest."""
    import json
    import os

    manifest: dict[str, object] = {"files": []}
    if os.path.exists(MANIFEST_PATH):
        with open(MANIFEST_PATH) as f:
            manifest = json.load(f)

    files_list: list[dict[str, object]] = []
    raw = manifest.get("files", [])
    if isinstance(raw, list):
        files_list = raw  # type: ignore[assignment]

    now = datetime.now().isoformat()
    for file_path in new_files:
        files_list.append({
            "filename": os.path.basename(file_path),
            "processed_at": now,
            "rows": 20,  # Each file has 20 rows
        })

    manifest["files"] = files_list
    manifest["last_updated"] = now
    manifest["total_files"] = len(files_list)

    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"[{timestamp()}] Manifest updated: {len(files_list)} total entries")


@task
def report(stats: dict[str, object]) -> None:
    """Print incremental processing summary."""
    print(f"[{timestamp()}] === Incremental Processing Complete ===")
    print(f"  New files this run: {len(stats.get('files_processed', []))}")  # type: ignore[arg-type]
    print(f"  New rows: {stats['new_rows']}")
    print(f"  Cumulative rows: {stats.get('cumulative_rows', 'N/A')}")
    files = stats.get("files_processed", [])
    if isinstance(files, list):
        for f in files:
            print(f"    {f}")


with DAG(
    dag_id="067_incremental_file_processing",
    default_args=DEFAULT_ARGS,
    description="Manifest-based incremental file processing pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "etl"],
) as dag:
    s = setup()
    new = detect_new_files()
    stats = process_new_files(new_files=new)
    manifest = update_manifest(new_files=new, stats=stats)
    r = report(stats=stats)

    s >> new >> stats >> manifest >> r

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {LANDING_DIR} {PROCESSED_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

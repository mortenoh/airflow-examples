"""DAG 66: ETL with Quarantine Pattern.

Demonstrates processing files with intentional errors using the
quarantine pattern: bad rows are isolated, unparseable files are
moved to a dead letter directory, and clean data flows through
to output. Shows graceful error handling in data pipelines.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.file_utils import (
    LANDING_DIR,
    PROCESSED_DIR,
    QUARANTINE_DIR,
    generate_climate_csv,
    quarantine_file,
    setup_dirs,
)


@task
def setup() -> None:
    """Create dirs and generate CSVs: one clean, one with bad rows, one corrupt."""
    import os

    setup_dirs(LANDING_DIR, PROCESSED_DIR, QUARANTINE_DIR)

    # Clean file
    generate_climate_csv(f"{LANDING_DIR}/clean_data.csv", n_rows=50)

    # File with ~10% bad rows
    generate_climate_csv(f"{LANDING_DIR}/messy_data.csv", n_rows=50, include_bad_rows=True)

    # Completely corrupt file (not valid CSV)
    corrupt_path = f"{LANDING_DIR}/corrupt_data.csv"
    with open(corrupt_path, "w") as f:
        f.write("{{{{not,csv,at,all\x00\x01binary\ngarbage\n")

    print(f"[{timestamp()}] Setup: 1 clean, 1 messy, 1 corrupt CSV in {LANDING_DIR}")
    for name in os.listdir(LANDING_DIR):
        print(f"  {name}")


@task
def detect_files() -> list[str]:
    """Glob landing directory for CSV files."""
    import glob

    files = sorted(glob.glob(f"{LANDING_DIR}/*.csv"))
    print(f"[{timestamp()}] Detected {len(files)} files")
    return files


@task
def process_with_quarantine(files: list[str]) -> dict[str, object]:
    """Parse each file; quarantine corrupt files, validate rows in parseable ones."""
    import os

    import pandas as pd

    good_frames: list[pd.DataFrame] = []
    bad_rows: list[dict[str, object]] = []
    quarantined_files: list[str] = []

    for file_path in files:
        basename = os.path.basename(file_path)
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            dest = quarantine_file(file_path, QUARANTINE_DIR, f"Unparseable: {e}")
            quarantined_files.append(dest)
            print(f"[{timestamp()}] QUARANTINED (corrupt): {basename}")
            continue

        # Row-level validation
        valid_mask = pd.Series([True] * len(df), index=df.index)

        # Type check: temperature should be numeric
        if "temperature_f" in df.columns:
            numeric_temp = pd.to_numeric(df["temperature_f"], errors="coerce")
            temp_invalid = numeric_temp.isna() & df["temperature_f"].notna()
            valid_mask &= ~temp_invalid

        # Range check: humidity 0-100
        if "humidity_pct" in df.columns:
            numeric_hum = pd.to_numeric(df["humidity_pct"], errors="coerce")
            hum_invalid = (numeric_hum < 0) | (numeric_hum > 100)
            valid_mask &= ~hum_invalid

        good_df = df[valid_mask].copy()
        bad_df = df[~valid_mask].copy()

        if len(bad_df) > 0:
            for _, row in bad_df.iterrows():
                bad_rows.append({
                    "source_file": basename,
                    "row_data": str(row.to_dict()),
                    "reason": "Failed type or range validation",
                })

        good_frames.append(good_df)
        print(
            f"[{timestamp()}] {basename}: {len(good_df)} good, {len(bad_df)} bad rows"
        )

    return {
        "good_frames_count": len(good_frames),
        "good_row_count": sum(len(f) for f in good_frames),
        "bad_row_count": len(bad_rows),
        "quarantined_count": len(quarantined_files),
        "bad_rows": bad_rows,
    }


@task
def write_clean_output(result: dict[str, object]) -> str:
    """Write validated clean rows to Parquet."""
    # Re-process to get actual DataFrames (simplified for demo)
    import glob

    import pandas as pd

    frames: list[pd.DataFrame] = []
    for file_path in sorted(glob.glob(f"{LANDING_DIR}/*.csv")):
        try:
            df = pd.read_csv(file_path)
            # Keep only rows with valid numeric temperature
            if "temperature_f" in df.columns:
                df["temperature_f"] = pd.to_numeric(df["temperature_f"], errors="coerce")
                df = df.dropna(subset=["temperature_f"])
            frames.append(df)
        except Exception:
            continue

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        output = f"{PROCESSED_DIR}/clean_output.parquet"
        combined.to_parquet(output, index=False)
        print(f"[{timestamp()}] Wrote {len(combined)} clean rows to {output}")
        return output

    print(f"[{timestamp()}] No clean data to write")
    return ""


@task
def write_quarantine_log(result: dict[str, object]) -> str:
    """Write quarantined rows to CSV with reason column."""
    import pandas as pd

    bad_rows_raw: object = result.get("bad_rows", [])
    bad_rows: list[dict[str, object]] = bad_rows_raw if isinstance(bad_rows_raw, list) else []
    if bad_rows:
        df = pd.DataFrame(bad_rows)
        log_path = f"{QUARANTINE_DIR}/quarantine_log.csv"
        df.to_csv(log_path, index=False)
        print(f"[{timestamp()}] Wrote {len(df)} bad rows to {log_path}")
        return log_path
    print(f"[{timestamp()}] No bad rows to log")
    return ""


@task
def quarantine_report(result: dict[str, object]) -> None:
    """Print summary of good/bad/corrupt files and rows."""
    print(f"[{timestamp()}] === Quarantine ETL Report ===")
    print(f"  Clean rows: {result['good_row_count']}")
    print(f"  Bad rows: {result['bad_row_count']}")
    print(f"  Quarantined files: {result['quarantined_count']}")

    bad_rows_raw: object = result.get("bad_rows", [])
    bad_rows: list[dict[str, object]] = bad_rows_raw if isinstance(bad_rows_raw, list) else []
    if bad_rows:
        print("\n  Sample bad rows:")
        for row in bad_rows[:3]:
            print(f"    File: {row['source_file']}")
            print(f"    Reason: {row['reason']}")
            print(f"    Data: {row['row_data'][:80]}...")


with DAG(
    dag_id="066_error_handling_etl",
    default_args=DEFAULT_ARGS,
    description="ETL with quarantine pattern for bad rows and corrupt files",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "etl"],
) as dag:
    s = setup()
    files = detect_files()
    result = process_with_quarantine(files=files)
    clean = write_clean_output(result=result)
    qlog = write_quarantine_log(result=result)
    r = quarantine_report(result=result)

    s >> files >> result >> [clean, qlog, r]  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {LANDING_DIR} {PROCESSED_DIR} {QUARANTINE_DIR} && echo 'Cleaned up'",
    )
    [clean, qlog, r] >> cleanup  # type: ignore[list-item]

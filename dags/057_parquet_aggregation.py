"""DAG 57: Parquet Aggregation Pipeline.

Demonstrates a data pipeline that generates sample weather data as
Parquet, reads it with pandas, performs aggregations (mean, min, max,
count by station), and writes the results as CSV to disk. Shows a
realistic multi-step data processing pattern using ``@task`` and
``PythonOperator``.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

DATA_DIR = str(OUTPUT_BASE / "parquet_demo")


@task
def setup_directory() -> str:
    """Create working directory for the pipeline."""
    import os

    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"[{timestamp()}] Created directory: {DATA_DIR}")
    return DATA_DIR


@task
def generate_parquet(data_dir: str) -> str:
    """Generate sample weather data and write as Parquet.

    Creates a realistic dataset with multiple stations, dates, and
    weather variables.
    """
    import numpy as np
    import pandas as pd

    np.random.seed(42)
    n_records = 200

    stations = ["oslo_01", "oslo_02", "bergen_01", "tromso_01", "stavanger_01"]
    dates = pd.date_range("2024-01-01", periods=40, freq="D")

    df = pd.DataFrame({
        "station": np.random.choice(stations, n_records),
        "date": np.random.choice(dates, n_records),
        "temperature_c": np.round(np.random.normal(10, 8, n_records), 1),
        "humidity_pct": np.round(np.random.uniform(40, 95, n_records), 1),
        "pressure_hpa": np.round(np.random.normal(1013, 15, n_records), 1),
        "wind_speed_ms": np.round(np.random.exponential(5, n_records), 1),
    })

    parquet_path = f"{data_dir}/weather_raw.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} records to {parquet_path}")
    print(f"[{timestamp()}] Columns: {list(df.columns)}")
    print(f"[{timestamp()}] Stations: {sorted(df['station'].unique())}")
    print(f"[{timestamp()}] Date range: {df['date'].min()} to {df['date'].max()}")
    return parquet_path


@task
def validate_parquet(parquet_path: str) -> dict[str, object]:
    """Read and validate the Parquet file."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    print(f"[{timestamp()}] Read {len(df)} records from {parquet_path}")

    # Basic quality checks
    null_counts = df.isnull().sum()
    print(f"[{timestamp()}] Null counts per column:")
    for col, count in null_counts.items():
        print(f"  {col}: {count}")

    print(f"[{timestamp()}] Data types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")

    stats: dict[str, object] = {
        "rows": len(df),
        "columns": len(df.columns),
        "nulls": int(null_counts.sum()),
        "path": parquet_path,
    }
    print(f"[{timestamp()}] Validation passed: {stats}")
    return stats


@task
def aggregate_by_station(parquet_path: str) -> str:
    """Aggregate weather data by station and write as CSV.

    Computes mean, min, max, count for each station.
    """
    import pandas as pd

    df = pd.read_parquet(parquet_path)

    agg = df.groupby("station").agg(
        record_count=("temperature_c", "count"),
        temp_mean=("temperature_c", "mean"),
        temp_min=("temperature_c", "min"),
        temp_max=("temperature_c", "max"),
        humidity_mean=("humidity_pct", "mean"),
        pressure_mean=("pressure_hpa", "mean"),
        wind_mean=("wind_speed_ms", "mean"),
    ).round(2)

    csv_path = f"{parquet_path.rsplit('/', 1)[0]}/station_summary.csv"
    agg.to_csv(csv_path)
    print(f"[{timestamp()}] === Station Summary ===")
    print(agg.to_string())
    print(f"\n[{timestamp()}] Wrote station summary to {csv_path}")
    return csv_path


@task
def aggregate_by_date(parquet_path: str) -> str:
    """Aggregate weather data by date and write as CSV.

    Computes daily averages across all stations.
    """
    import pandas as pd

    df = pd.read_parquet(parquet_path)

    daily = df.groupby("date").agg(
        station_count=("station", "nunique"),
        temp_mean=("temperature_c", "mean"),
        temp_std=("temperature_c", "std"),
        humidity_mean=("humidity_pct", "mean"),
        pressure_mean=("pressure_hpa", "mean"),
    ).round(2)

    csv_path = f"{parquet_path.rsplit('/', 1)[0]}/daily_summary.csv"
    daily.to_csv(csv_path)
    print(f"[{timestamp()}] === Daily Summary (first 10 days) ===")
    print(daily.head(10).to_string())
    print(f"\n[{timestamp()}] Wrote daily summary ({len(daily)} days) to {csv_path}")
    return csv_path


@task
def aggregate_cross_tab(parquet_path: str) -> str:
    """Create a cross-tabulation of stations vs temperature categories."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)

    # Categorize temperatures
    df["temp_category"] = pd.cut(
        df["temperature_c"],
        bins=[-50, 0, 10, 20, 50],
        labels=["freezing", "cold", "mild", "warm"],
    )

    cross = pd.crosstab(df["station"], df["temp_category"], margins=True)

    csv_path = f"{parquet_path.rsplit('/', 1)[0]}/temperature_crosstab.csv"
    cross.to_csv(csv_path)
    print(f"[{timestamp()}] === Temperature Cross-Tab ===")
    print(cross.to_string())
    print(f"\n[{timestamp()}] Wrote cross-tab to {csv_path}")
    return csv_path


@task
def final_report(
    station_csv: str, daily_csv: str, crosstab_csv: str, validation: dict[str, object]
) -> None:
    """Print final report summarizing all outputs."""
    print(f"[{timestamp()}] === Pipeline Complete ===")
    print(f"  Input:  {validation['path']} ({validation['rows']} rows)")
    print(f"  Output: {station_csv}")
    print(f"          {daily_csv}")
    print(f"          {crosstab_csv}")
    print(f"  Nulls:  {validation['nulls']}")


with DAG(
    dag_id="057_parquet_aggregation",
    default_args=DEFAULT_ARGS,
    description="Parquet -> pandas aggregations -> CSV pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "pipeline", "data"],
) as dag:
    directory = setup_directory()
    parquet_file = generate_parquet(data_dir=directory)
    validation = validate_parquet(parquet_path=parquet_file)

    # Three parallel aggregations from the same parquet source
    station_csv = aggregate_by_station(parquet_path=parquet_file)
    daily_csv = aggregate_by_date(parquet_path=parquet_file)
    crosstab_csv = aggregate_cross_tab(parquet_path=parquet_file)

    report = final_report(
        station_csv=station_csv,
        daily_csv=daily_csv,
        crosstab_csv=crosstab_csv,
        validation=validation,
    )

    # Cleanup temp files
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {DATA_DIR} && echo 'Cleaned up {DATA_DIR}'",
    )

    report >> cleanup

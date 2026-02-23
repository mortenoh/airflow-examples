"""DAG 105: Airflow ObjectStoragePath with RustFS.

Demonstrates Airflow's native ObjectStoragePath abstraction for S3-compatible
storage. Contrasts the raw boto3 approach from DAGs 103-104 with a cleaner,
pathlib-like API that leverages Airflow connections instead of hardcoded
credentials.

Key patterns:
    - ``ObjectStoragePath("s3://conn_id@bucket/key")`` for connection-aware paths
    - ``base / "subdir" / "file.parquet"`` for pathlib-like concatenation
    - ``path.open("wb")`` / ``path.open("rb")`` for file-like read/write
    - ``path.iterdir()`` for listing objects
    - ``path.stat().st_size`` for file metadata
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

RUSTFS_ENDPOINT = "http://rustfs:9000"
RUSTFS_ACCESS_KEY = "airflow"
RUSTFS_SECRET_KEY = "airflow123"
BUCKET = "airflow-data"
CONN_ID = "aws_default"


@task
def ensure_bucket() -> None:
    """Create the RustFS bucket if it does not exist.

    ObjectStoragePath cannot create buckets, so we use boto3 for this step.
    """
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"[{timestamp()}] Created bucket: {BUCKET}")


@task
def write_parquet() -> str:
    """Fetch Nordic weather data and write as Parquet via ObjectStoragePath."""
    import pandas as pd
    from airflow.sdk import ObjectStoragePath

    from airflow_examples.apis import NORDIC_CITIES, OPEN_METEO_FORECAST, fetch_open_meteo

    results: list[dict[str, object]] = []
    for city in NORDIC_CITIES[:3]:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
            "forecast_days": 3,
        }
        data = fetch_open_meteo(OPEN_METEO_FORECAST, params)
        daily = data.get("daily", {})
        for i, date in enumerate(daily.get("time", [])):
            results.append({
                "city": city["name"],
                "date": date,
                "temp_max": daily.get("temperature_2m_max", [None])[i],
                "temp_min": daily.get("temperature_2m_min", [None])[i],
                "precipitation": daily.get("precipitation_sum", [None])[i],
            })

    df = pd.DataFrame(results)
    base = ObjectStoragePath(f"s3://{CONN_ID}@{BUCKET}")
    path = base / "objstore" / "weather_forecast.parquet"

    with path.open("wb") as f:
        df.to_parquet(f, index=False)

    size = path.stat().st_size
    print(f"[{timestamp()}] Wrote {len(df)} rows to {path} ({size:,} bytes)")
    return str(path)


@task
def list_files() -> list[str]:
    """List objects in the bucket using ObjectStoragePath.iterdir()."""
    from airflow.sdk import ObjectStoragePath

    base = ObjectStoragePath(f"s3://{CONN_ID}@{BUCKET}/objstore")
    files = [str(p) for p in base.iterdir()]

    print(f"[{timestamp()}] Objects in {base}:")
    for f in files:
        print(f"  {f}")
    return files


@task
def read_and_transform(parquet_path: str) -> dict[str, object]:
    """Read Parquet back via ObjectStoragePath and compute summary stats."""
    import pandas as pd
    from airflow.sdk import ObjectStoragePath

    path = ObjectStoragePath(parquet_path)
    with path.open("rb") as f:
        df = pd.read_parquet(f)

    print(f"[{timestamp()}] Read {len(df)} rows from {path}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Cities: {df['city'].unique().tolist()}")

    stats = df.groupby("city").agg(
        avg_temp_max=("temp_max", "mean"),
        avg_temp_min=("temp_min", "mean"),
        total_precip=("precipitation", "sum"),
    ).round(1)

    print("\n  Summary stats:")
    for city, row in stats.iterrows():
        print(f"    {city}: max={row['avg_temp_max']}C, min={row['avg_temp_min']}C, "
              f"precip={row['total_precip']}mm")

    return {
        "rows": len(df),
        "cities": df["city"].unique().tolist(),
        "stats": stats.to_dict(),
    }


@task
def copy_to_gold(parquet_path: str) -> str:
    """Demonstrate path operations: read, aggregate, and write to gold layer."""
    import pandas as pd
    from airflow.sdk import ObjectStoragePath

    src = ObjectStoragePath(parquet_path)
    with src.open("rb") as f:
        df = pd.read_parquet(f)

    agg = df.groupby("city").agg(
        days=("date", "count"),
        temp_max_avg=("temp_max", "mean"),
        temp_min_avg=("temp_min", "mean"),
        precip_total=("precipitation", "sum"),
    ).reset_index().round(1)

    base = ObjectStoragePath(f"s3://{CONN_ID}@{BUCKET}")
    gold_path = base / "objstore_gold" / "city_summary.parquet"

    with gold_path.open("wb") as f:
        agg.to_parquet(f, index=False)

    size = gold_path.stat().st_size
    print(f"[{timestamp()}] Gold: wrote {len(agg)} city summaries to {gold_path} ({size:,} bytes)")
    return str(gold_path)


@task
def report(parquet_path: str, gold_path: str, transform_result: dict[str, object]) -> None:
    """Print ObjectStoragePath pipeline summary."""
    print(f"\n[{timestamp()}] === ObjectStoragePath Pipeline Report ===")
    print(f"  Source:  {parquet_path}")
    print(f"  Gold:    {gold_path}")
    print(f"  Rows:    {transform_result.get('rows')}")
    print(f"  Cities:  {transform_result.get('cities')}")
    print()
    print("  Key difference from DAGs 103-104 (raw boto3):")
    print("    - No hardcoded credentials in task code")
    print("    - Pathlib-like API: base / 'subdir' / 'file.parquet'")
    print("    - File-like I/O: path.open('wb'), path.open('rb')")
    print("    - Uses Airflow connections (aws_default) automatically")
    print("    - path.stat(), path.iterdir() for metadata and listing")


with DAG(
    dag_id="105_object_storage",
    default_args=DEFAULT_ARGS,
    description="Airflow ObjectStoragePath with RustFS (S3-compatible)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "s3", "objectstorage"],
) as dag:
    bucket_ready = ensure_bucket()
    parquet = write_parquet()
    files = list_files()
    transformed = read_and_transform(parquet_path=parquet)
    gold = copy_to_gold(parquet_path=parquet)
    summary = report(parquet_path=parquet, gold_path=gold, transform_result=transformed)

    bucket_ready >> parquet >> [files, transformed, gold] >> summary

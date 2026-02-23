"""DAG 103: RustFS Object Storage Write/Read.

Demonstrates writing DataFrames to RustFS (S3-compatible) as Parquet
using boto3, then reading them back to verify round-trip integrity.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

RUSTFS_ENDPOINT = "http://rustfs:9000"
RUSTFS_ACCESS_KEY = "airflow"
RUSTFS_SECRET_KEY = "airflow123"
BUCKET = "airflow-data"


@task
def ensure_bucket() -> None:
    """Create the RustFS bucket if it does not exist."""
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
def fetch_sample_data() -> list[dict[str, object]]:
    """Fetch Nordic weather data for MinIO storage demo."""
    from airflow_examples.apis import NORDIC_CITIES, OPEN_METEO_FORECAST, fetch_open_meteo

    results: list[dict[str, object]] = []
    for city in NORDIC_CITIES[:3]:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": "temperature_2m_max,temperature_2m_min",
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
            })
    print(f"[{timestamp()}] Fetched {len(results)} weather records")
    return results


@task
def write_to_minio(data: list[dict[str, object]]) -> str:
    """Write DataFrame to RustFS as Parquet."""
    import io

    import boto3
    import pandas as pd

    df = pd.DataFrame(data)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )

    key = "demo/weather_forecast.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"[{timestamp()}] Wrote {len(df)} rows to s3://{BUCKET}/{key}")
    return key


@task
def read_from_minio(key: str) -> dict[str, object]:
    """Read Parquet back from RustFS and verify."""
    import io

    import boto3
    import pandas as pd

    s3 = boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )

    obj = s3.get_object(Bucket=BUCKET, Key=key)
    buffer = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buffer)

    print(f"[{timestamp()}] Read back {len(df)} rows from s3://{BUCKET}/{key}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Cities: {df['city'].unique().tolist()}")
    return {"rows": len(df), "columns": list(df.columns)}


@task
def report(write_key: str, read_result: dict[str, object]) -> None:
    """Print RustFS round-trip summary."""
    print(f"\n[{timestamp()}] === RustFS Write/Read Report ===")
    print(f"  Object key: {write_key}")
    print(f"  Rows verified: {read_result.get('rows')}")
    print(f"  Columns: {read_result.get('columns')}")
    print("  RustFS console: http://localhost:9001")


with DAG(
    dag_id="103_minio_write",
    default_args=DEFAULT_ARGS,
    description="Write and read Parquet files to/from RustFS object storage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "s3"],
) as dag:
    bucket_ready = ensure_bucket()
    data = fetch_sample_data()
    key = write_to_minio(data=data)
    verified = read_from_minio(key=key)
    report(write_key=key, read_result=verified)
    bucket_ready >> data

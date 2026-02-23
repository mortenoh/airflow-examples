"""DAG 104: RustFS Data Lake (Bronze/Silver/Gold).

Full data lake pattern using RustFS (S3-compatible): raw JSON in bronze
layer, cleaned Parquet in silver layer, aggregated Parquet in gold layer.
"""

import json
from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp

RUSTFS_ENDPOINT = "http://rustfs:9000"
RUSTFS_ACCESS_KEY = "airflow"
RUSTFS_SECRET_KEY = "airflow123"
BUCKET = "airflow-data"


def get_s3_client():  # type: ignore[no-untyped-def]
    """Create boto3 S3 client for RustFS."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
    )


@task
def ensure_bucket() -> None:
    """Create the RustFS bucket if it does not exist."""
    from botocore.exceptions import ClientError

    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=BUCKET)
        print(f"[{timestamp()}] Created bucket: {BUCKET}")


@task
def bronze_ingest() -> str:
    """Fetch raw API data and store as JSON in bronze layer."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/region/europe"
    params = {"fields": "name,cca3,population,area,region,subregion,capital"}
    data = fetch_json(url, params=params)
    countries = data if isinstance(data, list) else []

    s3 = get_s3_client()
    key = "bronze/countries/european_countries.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(countries, indent=2),
        ContentType="application/json",
    )
    print(f"[{timestamp()}] Bronze: wrote {len(countries)} countries to s3://{BUCKET}/{key}")
    return key


@task
def silver_clean(bronze_key: str) -> str:
    """Read bronze JSON, clean and normalize, write as Parquet to silver layer."""
    import io

    import pandas as pd

    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=bronze_key)
    countries = json.loads(obj["Body"].read().decode())

    rows = []
    for c in countries:
        name = c.get("name", {})
        capital_list = c.get("capital", [])
        rows.append({
            "country_code": c.get("cca3"),
            "country_name": name.get("common") if isinstance(name, dict) else str(name),
            "population": c.get("population"),
            "area_km2": c.get("area"),
            "region": c.get("region"),
            "subregion": c.get("subregion"),
            "capital": capital_list[0] if capital_list else None,
        })

    df = pd.DataFrame(rows).dropna(subset=["country_code"])
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    key = "silver/countries/european_countries.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"[{timestamp()}] Silver: cleaned {len(df)} countries to s3://{BUCKET}/{key}")
    return key


@task
def gold_aggregate(silver_key: str) -> str:
    """Read silver data, compute regional aggregations, write to gold layer."""
    import io

    import pandas as pd

    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=silver_key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    agg = df.groupby("subregion").agg(
        country_count=("country_code", "count"),
        total_population=("population", "sum"),
        total_area=("area_km2", "sum"),
        avg_population=("population", "mean"),
    ).reset_index()

    agg["population_density"] = (agg["total_population"] / agg["total_area"]).round(1)

    buffer = io.BytesIO()
    agg.to_parquet(buffer, index=False)
    buffer.seek(0)

    key = "gold/countries/subregion_summary.parquet"
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    print(f"[{timestamp()}] Gold: {len(agg)} subregion summaries to s3://{BUCKET}/{key}")
    return key


@task
def report(bronze_key: str, silver_key: str, gold_key: str) -> None:
    """Print data lake pipeline summary."""
    import io

    import pandas as pd

    s3 = get_s3_client()
    obj = s3.get_object(Bucket=BUCKET, Key=gold_key)
    agg = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    print(f"\n[{timestamp()}] === Data Lake Pipeline Report ===")
    print(f"  Bronze: {bronze_key}")
    print(f"  Silver: {silver_key}")
    print(f"  Gold:   {gold_key}")
    print("\n  Subregion Summary:")
    for _, row in agg.iterrows():
        print(f"    {row['subregion']}: {row['country_count']} countries, "
              f"pop={row['total_population']:,.0f}, density={row['population_density']}/km2")
    print("\n  RustFS console: http://localhost:9001")


with DAG(
    dag_id="104_minio_data_lake",
    default_args=DEFAULT_ARGS,
    description="Bronze/silver/gold data lake pattern using RustFS",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "s3"],
) as dag:
    bucket_ready = ensure_bucket()
    bronze = bronze_ingest()
    silver = silver_clean(bronze_key=bronze)
    gold = gold_aggregate(silver_key=silver)
    report(bronze_key=bronze, silver_key=silver, gold_key=gold)
    bucket_ready >> bronze

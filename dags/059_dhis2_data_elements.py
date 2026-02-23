"""DAG 59: DHIS2 Data Elements to Parquet.

Demonstrates fetching data elements from DHIS2, categorizing with
pandas, and writing as Parquet using pyarrow. Shows Parquet output,
groupby analysis, and boolean derived columns.
"""

import os
from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import OUTPUT_DIR, fetch_metadata


@task
def fetch() -> list[dict]:
    """Fetch all data elements from DHIS2."""
    records = fetch_metadata("dataElements")
    print(f"[{timestamp()}] Fetched {len(records)} data elements")
    return records


@task
def transform(records: list[dict]) -> str:
    """Flatten, derive columns, and write to Parquet."""
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.DataFrame(records)
    print(f"[{timestamp()}] Raw columns: {list(df.columns)}")

    # Flatten categoryCombo.id
    if "categoryCombo" in df.columns:
        df["category_combo_id"] = df["categoryCombo"].apply(
            lambda c: c["id"] if isinstance(c, dict) else None
        )
    else:
        df["category_combo_id"] = None

    # Boolean: has a code assigned
    df["has_code"] = df["code"].notna() if "code" in df.columns else False

    # Name length
    df["name_length"] = df["name"].str.len()

    # Select and rename columns for output
    keep_cols = [
        "id", "name", "shortName", "domainType", "valueType",
        "aggregationType", "category_combo_id", "has_code", "name_length",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    parquet_path = f"{OUTPUT_DIR}/data_elements.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} records to {parquet_path}")

    # Value type breakdown
    if "valueType" in df.columns:
        print(f"[{timestamp()}] === Value Type Breakdown ===")
        for vt, count in df["valueType"].value_counts().head(10).items():
            print(f"  {vt}: {count}")

    # Aggregation type breakdown
    if "aggregationType" in df.columns:
        print(f"[{timestamp()}] === Aggregation Type Breakdown ===")
        for at, count in df["aggregationType"].value_counts().head(10).items():
            print(f"  {at}: {count}")

    return parquet_path


@task
def report(parquet_path: str) -> None:
    """Read Parquet back and print summary stats."""
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    print(f"[{timestamp()}] === Data Elements Report ===")
    print(f"  Total records: {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    if "domainType" in df.columns:
        print("\n  Domain types:")
        for dt, count in df["domainType"].value_counts().items():
            print(f"    {dt}: {count}")

    if "has_code" in df.columns:
        print(f"\n  Elements with code: {df['has_code'].sum()}")
        print(f"  Elements without code: {(~df['has_code']).sum()}")

    if "name_length" in df.columns:
        print("\n  Name length stats:")
        print(f"    Min: {df['name_length'].min()}")
        print(f"    Max: {df['name_length'].max()}")
        print(f"    Mean: {df['name_length'].mean():.1f}")


with DAG(
    dag_id="059_dhis2_data_elements",
    default_args=DEFAULT_ARGS,
    description="DHIS2 data elements -> categorize -> Parquet",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2"],
) as dag:
    raw = fetch()
    parquet_file = transform(records=raw)
    report(parquet_path=parquet_file)

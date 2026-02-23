"""DAG 111: DHIS2 Analytics Data Values.

Fetches analytics data (not metadata) from DHIS2 using the dhis2_default
connection. Demonstrates calling a different API path (/api/analytics),
transforming the DHIS2 rows/headers format into a pandas DataFrame, and
writing to CSV.
"""

import os
from datetime import datetime

import httpx
from airflow.sdk import DAG, task
from airflow.sdk.bases.hook import BaseHook

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import OUTPUT_DIR


@task
def fetch_data_values() -> dict:
    """Fetch analytics data from DHIS2 using the connection."""
    conn = BaseHook.get_connection("dhis2_default")
    base_url = conn.host.rstrip("/")
    url = f"{base_url}/api/analytics"

    params = {
        "dimension": [
            "dx:fbfJHSPpUQD;cYeuwXTCPkU",  # ANC 1st visit; ANC 2nd visit
            "ou:ImspTQPwCqd",  # Sierra Leone (national)
        ],
        "filter": "pe:LAST_4_QUARTERS",
    }

    resp = httpx.get(
        url,
        auth=(conn.login, conn.password),
        params=params,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    row_count = len(data.get("rows", []))
    print(f"[{timestamp()}] Fetched {row_count} analytics rows")
    return data


@task
def transform(analytics: dict) -> str:
    """Parse DHIS2 analytics response into a CSV file."""
    import pandas as pd

    headers = [h["name"] for h in analytics["headers"]]
    rows = analytics["rows"]

    df = pd.DataFrame(rows, columns=headers)
    print(f"[{timestamp()}] DataFrame shape: {df.shape}")
    print(f"[{timestamp()}] Columns: {list(df.columns)}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = f"{OUTPUT_DIR}/analytics_data_values.csv"
    df.to_csv(csv_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} rows to {csv_path}")
    return csv_path


@task
def report(csv_path: str) -> None:
    """Read the CSV and print a summary."""
    import pandas as pd

    df = pd.read_csv(csv_path)
    print(f"[{timestamp()}] === Analytics Data Values Report ===")
    print(f"  Total rows: {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    if "dx" in df.columns:
        print(f"\n  Data elements: {df['dx'].nunique()}")
        for dx_id, group in df.groupby("dx"):
            print(f"    {dx_id}: {len(group)} rows")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        print(f"\n  Value range: {df['value'].min()} - {df['value'].max()}")
        print(f"  Value sum: {df['value'].sum()}")


with DAG(
    dag_id="111_dhis2_data_values",
    default_args=DEFAULT_ARGS,
    description="Fetch DHIS2 analytics data values via connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2", "connections", "analytics"],
) as dag:
    raw = fetch_data_values()
    csv_file = transform(analytics=raw)
    report(csv_path=csv_file)

"""DAG 58: DHIS2 Organisation Units to CSV.

Demonstrates fetching metadata from the DHIS2 play server, flattening
nested JSON with pandas, and writing the result as CSV. Shows HTTP API
integration with basic auth, JSON flattening, and derived columns.
"""

import os
from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import OUTPUT_DIR, fetch_metadata


@task
def fetch() -> list[dict]:
    """Fetch all organisation units from DHIS2."""
    records = fetch_metadata("organisationUnits")
    print(f"[{timestamp()}] Fetched {len(records)} organisation units")
    return records


@task
def transform(records: list[dict]) -> str:
    """Flatten nested JSON and derive columns, write to CSV."""
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.DataFrame(records)
    print(f"[{timestamp()}] Raw columns: {list(df.columns)}")

    # Flatten nested parent.id
    df["parent_id"] = df["parent"].apply(
        lambda p: p["id"] if isinstance(p, dict) else None
    )

    # Flatten createdBy.username
    if "createdBy" in df.columns:
        df["created_by"] = df["createdBy"].apply(
            lambda c: c.get("username") if isinstance(c, dict) else None
        )
    else:
        df["created_by"] = None

    # Extract hierarchy depth from path (count '/' separators minus leading)
    if "path" in df.columns:
        df["hierarchy_depth"] = df["path"].apply(
            lambda p: p.count("/") - 1 if isinstance(p, str) else 0
        )
    else:
        df["hierarchy_depth"] = 0

    # Count translations
    if "translations" in df.columns:
        df["translation_count"] = df["translations"].apply(
            lambda t: len(t) if isinstance(t, list) else 0
        )
    else:
        df["translation_count"] = 0

    # Select useful columns
    keep_cols = [
        "id", "name", "shortName", "level", "parent_id", "created_by",
        "hierarchy_depth", "translation_count", "openingDate",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    csv_path = f"{OUTPUT_DIR}/org_units.csv"
    df.to_csv(csv_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} records to {csv_path}")

    # Count-by-level summary
    if "level" in df.columns:
        level_counts = df["level"].value_counts().sort_index()
        print(f"[{timestamp()}] === Count by Level ===")
        for level, count in level_counts.items():
            print(f"  Level {level}: {count}")

    return csv_path


@task
def report(csv_path: str) -> None:
    """Read CSV and print formatted summary."""
    import pandas as pd

    df = pd.read_csv(csv_path)
    print(f"[{timestamp()}] === Organisation Units Report ===")
    print(f"  Total records: {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    if "level" in df.columns:
        print("\n  Top 10 by level:")
        for _, row in df.sort_values("level").head(10).iterrows():
            print(f"    Level {row['level']}: {row['name']}")

    if "openingDate" in df.columns:
        dates = pd.to_datetime(df["openingDate"], errors="coerce").dropna()
        if len(dates) > 0:
            print(f"\n  Opening date range: {dates.min()} to {dates.max()}")

    print(f"\n  Hierarchy depths: {df['hierarchy_depth'].min()} - {df['hierarchy_depth'].max()}")
    print(f"  Units with translations: {(df['translation_count'] > 0).sum()}")


with DAG(
    dag_id="058_dhis2_org_units",
    default_args=DEFAULT_ARGS,
    description="DHIS2 organisation units -> flatten JSON -> CSV",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2"],
) as dag:
    raw = fetch()
    csv_file = transform(records=raw)
    report(csv_path=csv_file)

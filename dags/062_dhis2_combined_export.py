"""DAG 62: DHIS2 Combined Parallel Export.

Demonstrates fetching all three DHIS2 metadata types in parallel,
transforming each independently, writing to disk in multiple formats
(CSV and Parquet), and producing a combined summary report. Shows
parallel task execution, fan-in pattern, and multi-format output.
"""

import os
import re
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import OUTPUT_DIR, fetch_metadata

# -- Fetch tasks (run in parallel) ------------------------------------------


@task
def fetch_org_units() -> list[dict]:
    """Fetch all organisation units from DHIS2."""
    records = fetch_metadata("organisationUnits")
    print(f"[{timestamp()}] Fetched {len(records)} org units")
    return records


@task
def fetch_data_elements() -> list[dict]:
    """Fetch all data elements from DHIS2."""
    records = fetch_metadata("dataElements")
    print(f"[{timestamp()}] Fetched {len(records)} data elements")
    return records


@task
def fetch_indicators() -> list[dict]:
    """Fetch all indicators from DHIS2."""
    records = fetch_metadata("indicators")
    print(f"[{timestamp()}] Fetched {len(records)} indicators")
    return records


# -- Transform tasks (run in parallel) --------------------------------------


@task
def transform_org_units(records: list[dict]) -> str:
    """Flatten org units and write CSV."""
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(records)

    df["parent_id"] = df["parent"].apply(
        lambda p: p["id"] if isinstance(p, dict) else None
    )
    if "path" in df.columns:
        df["hierarchy_depth"] = df["path"].apply(
            lambda p: p.count("/") - 1 if isinstance(p, str) else 0
        )

    keep = ["id", "name", "shortName", "level", "parent_id"]
    if "hierarchy_depth" in df.columns:
        keep.append("hierarchy_depth")
    keep = [c for c in keep if c in df.columns]

    csv_path = f"{OUTPUT_DIR}/combined_org_units.csv"
    df[keep].to_csv(csv_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} org units to {csv_path}")
    return csv_path


@task
def transform_data_elements(records: list[dict]) -> str:
    """Flatten data elements and write Parquet."""
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(records)

    if "categoryCombo" in df.columns:
        df["category_combo_id"] = df["categoryCombo"].apply(
            lambda c: c["id"] if isinstance(c, dict) else None
        )
    df["has_code"] = df["code"].notna() if "code" in df.columns else False
    df["name_length"] = df["name"].str.len()

    keep = [
        "id", "name", "shortName", "domainType", "valueType",
        "aggregationType", "category_combo_id", "has_code", "name_length",
    ]
    keep = [c for c in keep if c in df.columns]

    parquet_path = f"{OUTPUT_DIR}/combined_data_elements.parquet"
    df[keep].to_parquet(parquet_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} data elements to {parquet_path}")
    return parquet_path


@task
def transform_indicators(records: list[dict]) -> str:
    """Flatten indicators, parse expressions, write CSV."""
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df = pd.DataFrame(records)

    if "indicatorType" in df.columns:
        df["indicator_type_name"] = df["indicatorType"].apply(
            lambda t: t.get("name") if isinstance(t, dict) else None
        )

    def count_operands(expr: object) -> int:
        if not isinstance(expr, str):
            return 0
        return len(re.findall(r"#\{[^}]+\}", expr))

    df["numerator_operands"] = df.get("numerator", pd.Series(dtype="object")).apply(
        count_operands
    )
    df["denominator_operands"] = df.get("denominator", pd.Series(dtype="object")).apply(
        count_operands
    )
    df["expression_complexity"] = df["numerator_operands"] + df["denominator_operands"]

    keep = [
        "id", "name", "shortName", "indicator_type_name",
        "numerator_operands", "denominator_operands", "expression_complexity",
    ]
    keep = [c for c in keep if c in df.columns]

    csv_path = f"{OUTPUT_DIR}/combined_indicators.csv"
    df[keep].to_csv(csv_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} indicators to {csv_path}")
    return csv_path


# -- Combined report (fan-in) -----------------------------------------------


@task
def combined_report(
    org_units_path: str, data_elements_path: str, indicators_path: str
) -> None:
    """Read all outputs and produce a combined summary."""
    import pandas as pd

    org_df = pd.read_csv(org_units_path)
    elem_df = pd.read_parquet(data_elements_path)
    ind_df = pd.read_csv(indicators_path)

    print(f"[{timestamp()}] === Combined DHIS2 Export Summary ===")
    print(f"  Organisation units: {len(org_df)} records (CSV)")
    print(f"  Data elements:      {len(elem_df)} records (Parquet)")
    print(f"  Indicators:         {len(ind_df)} records (CSV)")
    print(f"  Total records:      {len(org_df) + len(elem_df) + len(ind_df)}")

    # Org unit level distribution
    if "level" in org_df.columns:
        print("\n  Org unit levels:")
        for level, count in org_df["level"].value_counts().sort_index().items():
            print(f"    Level {level}: {count}")

    # Data element domain types
    if "domainType" in elem_df.columns:
        print("\n  Data element domains:")
        for dt, count in elem_df["domainType"].value_counts().items():
            print(f"    {dt}: {count}")

    # Indicator complexity summary
    if "expression_complexity" in ind_df.columns:
        print("\n  Indicator complexity:")
        print(f"    Mean: {ind_df['expression_complexity'].mean():.1f}")
        print(f"    Max:  {ind_df['expression_complexity'].max()}")
        print(f"    Trivial (0): {(ind_df['expression_complexity'] == 0).sum()}")

    print("\n  Output files:")
    print(f"    {org_units_path}")
    print(f"    {data_elements_path}")
    print(f"    {indicators_path}")


with DAG(
    dag_id="062_dhis2_combined_export",
    default_args=DEFAULT_ARGS,
    description="DHIS2 parallel fetch -> transform -> multi-format export",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2"],
) as dag:
    # Parallel fetches
    org_raw = fetch_org_units()
    elem_raw = fetch_data_elements()
    ind_raw = fetch_indicators()

    # Parallel transforms
    org_csv = transform_org_units(records=org_raw)
    elem_parquet = transform_data_elements(records=elem_raw)
    ind_csv = transform_indicators(records=ind_raw)

    # Fan-in report
    report = combined_report(
        org_units_path=org_csv,
        data_elements_path=elem_parquet,
        indicators_path=ind_csv,
    )

    # Cleanup
    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

    report >> cleanup

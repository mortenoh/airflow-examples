"""DAG 60: DHIS2 Indicators with Expression Parsing.

Demonstrates fetching indicators from DHIS2, parsing numerator and
denominator expressions with regex to count operands, and computing
an expression complexity score. Shows regex parsing, expression
analysis, and complexity scoring patterns.
"""

import os
import re
from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import OUTPUT_DIR, fetch_metadata


@task
def fetch() -> list[dict]:
    """Fetch all indicators from DHIS2."""
    records = fetch_metadata("indicators")
    print(f"[{timestamp()}] Fetched {len(records)} indicators")
    return records


@task
def transform(records: list[dict]) -> str:
    """Flatten, parse expressions, compute complexity, write CSV."""
    import pandas as pd

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.DataFrame(records)
    print(f"[{timestamp()}] Raw columns: {list(df.columns)}")

    # Flatten indicatorType.id and indicatorType.name
    if "indicatorType" in df.columns:
        df["indicator_type_id"] = df["indicatorType"].apply(
            lambda t: t["id"] if isinstance(t, dict) else None
        )
        df["indicator_type_name"] = df["indicatorType"].apply(
            lambda t: t.get("name") if isinstance(t, dict) else None
        )
    else:
        df["indicator_type_id"] = None
        df["indicator_type_name"] = None

    # Count #{...} operands in numerator
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

    # Expression complexity: total operands + 1 per operator-like character
    def complexity_score(row: pd.Series) -> int:
        score = int(row["numerator_operands"]) + int(row["denominator_operands"])
        for field in ["numerator", "denominator"]:
            expr = row.get(field)
            if isinstance(expr, str):
                score += expr.count("+") + expr.count("-") + expr.count("*")
        return score

    df["expression_complexity"] = df.apply(complexity_score, axis=1)

    # Select columns for output
    keep_cols = [
        "id", "name", "shortName", "indicator_type_id", "indicator_type_name",
        "numerator_operands", "denominator_operands", "expression_complexity",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]
    df = df[keep_cols]

    csv_path = f"{OUTPUT_DIR}/indicators.csv"
    df.to_csv(csv_path, index=False)
    print(f"[{timestamp()}] Wrote {len(df)} records to {csv_path}")

    # Complexity distribution
    print(f"[{timestamp()}] === Complexity Distribution ===")
    bins = [0, 2, 5, 10, 50, 1000]
    labels = ["trivial (0-1)", "simple (2-4)", "moderate (5-9)", "complex (10-49)", "very complex (50+)"]
    df["complexity_bin"] = pd.cut(df["expression_complexity"], bins=bins, labels=labels, right=False)
    for label, count in df["complexity_bin"].value_counts().sort_index().items():
        print(f"  {label}: {count}")

    return csv_path


@task
def report(csv_path: str) -> None:
    """Print formatted summary of indicators."""
    import pandas as pd

    df = pd.read_csv(csv_path)
    print(f"[{timestamp()}] === Indicators Report ===")
    print(f"  Total indicators: {len(df)}")

    # Most complex indicators
    top = df.nlargest(5, "expression_complexity")
    print("\n  Most complex indicators:")
    for _, row in top.iterrows():
        print(f"    [{row['expression_complexity']}] {row['name']}")

    # Least complex (non-zero)
    nonzero = df[df["expression_complexity"] > 0].nsmallest(5, "expression_complexity")
    if len(nonzero) > 0:
        print("\n  Simplest indicators (non-trivial):")
        for _, row in nonzero.iterrows():
            print(f"    [{row['expression_complexity']}] {row['name']}")

    # Indicator type breakdown
    if "indicator_type_name" in df.columns:
        print("\n  Indicator types:")
        for it, count in df["indicator_type_name"].value_counts().items():
            print(f"    {it}: {count}")


with DAG(
    dag_id="060_dhis2_indicators",
    default_args=DEFAULT_ARGS,
    description="DHIS2 indicators -> expression parsing -> CSV",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2"],
) as dag:
    raw = fetch()
    csv_file = transform(records=raw)
    report(csv_path=csv_file)

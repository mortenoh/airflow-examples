"""DAG 68: Schema Validation Pipeline.

Demonstrates validating data against an expected schema definition
including column names, value types, and nullability constraints.
Shows schema-as-code validation and detailed violation reporting.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

DATA_DIR = str(OUTPUT_BASE / "schema_validation")
EXPECTED_COLUMNS = ["station", "date", "temperature_c", "humidity_pct", "pressure_hpa"]


@task
def generate_data() -> dict[str, str]:
    """Create two CSVs: one matching schema, one with violations."""
    import os

    import pandas as pd

    os.makedirs(DATA_DIR, exist_ok=True)

    # Good file: matches expected schema
    good = pd.DataFrame({
        "station": ["oslo_01", "bergen_01", "tromso_01"] * 10,
        "date": [f"2024-01-{(i % 28) + 1:02d}" for i in range(30)],
        "temperature_c": [round(5.0 + i * 0.3, 1) for i in range(30)],
        "humidity_pct": [round(60.0 + i * 0.5, 1) for i in range(30)],
        "pressure_hpa": [round(1013.0 + i * 0.1, 1) for i in range(30)],
    })
    good_path = f"{DATA_DIR}/good_schema.csv"
    good.to_csv(good_path, index=False)

    # Bad file: wrong names, extra columns, missing columns
    bad = pd.DataFrame({
        "station_name": ["oslo_01", "bergen_01", "tromso_01"] * 10,
        "obs_date": [f"2024-01-{(i % 28) + 1:02d}" for i in range(30)],
        "temp": [round(5.0 + i * 0.3, 1) for i in range(30)],
        "humidity_pct": [round(60.0 + i * 0.5, 1) for i in range(30)],
        "extra_col": ["x"] * 30,
    })
    bad_path = f"{DATA_DIR}/bad_schema.csv"
    bad.to_csv(bad_path, index=False)

    print(f"[{timestamp()}] Generated good ({good_path}) and bad ({bad_path}) schema files")
    return {"good": good_path, "bad": bad_path}


@task
def validate_columns(paths: dict[str, str]) -> dict[str, object]:
    """Check column names exist and no unexpected extras."""
    import pandas as pd

    from airflow_examples.quality import check_schema

    results: dict[str, object] = {}
    for label, path in paths.items():
        df = pd.read_csv(path)
        result = check_schema(df, EXPECTED_COLUMNS)
        results[label] = {"passed": result.passed, "details": result.details}
        status = "PASS" if result.passed else "FAIL"
        print(f"[{timestamp()}] Column check [{label}]: {status} - {result.details}")

    return results


@task
def validate_types(paths: dict[str, str]) -> dict[str, object]:
    """Check value types per column."""
    import pandas as pd

    results: dict[str, object] = {}
    type_checks = {
        "temperature_c": "numeric",
        "humidity_pct": "numeric",
        "pressure_hpa": "numeric",
        "date": "date",
    }

    for label, path in paths.items():
        df = pd.read_csv(path)
        violations: list[str] = []

        for col, expected_type in type_checks.items():
            if col not in df.columns:
                violations.append(f"{col}: column missing")
                continue

            if expected_type == "numeric":
                non_numeric = pd.to_numeric(df[col], errors="coerce").isna() & df[col].notna()
                if non_numeric.any():
                    violations.append(f"{col}: {non_numeric.sum()} non-numeric values")

            elif expected_type == "date":
                non_dates = pd.to_datetime(df[col], errors="coerce").isna() & df[col].notna()
                if non_dates.any():
                    violations.append(f"{col}: {non_dates.sum()} unparseable dates")

        passed = len(violations) == 0
        status = "PASS" if passed else "FAIL"
        results[label] = {"passed": passed, "violations": violations}
        print(f"[{timestamp()}] Type check [{label}]: {status} - {violations if violations else 'all types valid'}")

    return results


@task
def validate_nullability(paths: dict[str, str]) -> dict[str, object]:
    """Check required columns have no nulls."""
    import pandas as pd

    from airflow_examples.quality import check_nulls

    required_cols = ["station", "date", "temperature_c"]
    results: dict[str, object] = {}

    for label, path in paths.items():
        df = pd.read_csv(path)
        result = check_nulls(df, required_cols, max_null_pct=0.0)
        results[label] = {"passed": result.passed, "details": result.details}
        status = "PASS" if result.passed else "FAIL"
        print(f"[{timestamp()}] Null check [{label}]: {status} - {result.details}")

    return results


@task
def schema_report(
    col_results: dict[str, object],
    type_results: dict[str, object],
    null_results: dict[str, object],
) -> None:
    """Print consolidated schema validation report."""
    print(f"[{timestamp()}] === Schema Validation Report ===")
    for label in ["good", "bad"]:
        print(f"\n  File: {label}")
        for check_name, results in [("Columns", col_results), ("Types", type_results), ("Nulls", null_results)]:
            r = results.get(label, {})
            if isinstance(r, dict):
                status = "PASS" if r.get("passed") else "FAIL"
                detail = r.get("details", r.get("violations", ""))
                print(f"    {check_name}: [{status}] {detail}")


with DAG(
    dag_id="068_schema_validation",
    default_args=DEFAULT_ARGS,
    description="Schema validation: columns, types, nullability checks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "quality"],
) as dag:
    data = generate_data()
    cols = validate_columns(paths=data)
    types = validate_types(paths=data)
    nulls = validate_nullability(paths=data)
    r = schema_report(col_results=cols, type_results=types, null_results=nulls)

    data >> [cols, types, nulls] >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {DATA_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

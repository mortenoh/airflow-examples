"""DAG 80: Parameterized Pipeline.

Demonstrates a config-driven pipeline using DAG ``params`` with
default values and type validation. Parameters control extraction
size, transformation behavior, validation thresholds, and output
format. Tests can override via ``dag.test(run_conf={...})``.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "parameterized")

DAG_PARAMS = {
    "n_records": 50,
    "seed": 42,
    "temp_unit": "celsius",
    "include_nulls": False,
    "min_rows": 10,
    "max_null_pct": 5.0,
    "output_dir": OUTPUT_DIR,
    "output_format": "csv",
}


@task
def extract(**context: object) -> list[dict[str, object]]:
    """Extract records using params for count and seed."""
    from airflow_examples.etl_logic import extract_weather_data

    params = context.get("params", DAG_PARAMS)
    if not isinstance(params, dict):
        params = DAG_PARAMS
    n = params.get("n_records", 50)
    seed = params.get("seed", 42)

    records = extract_weather_data(n_records=int(n), seed=int(seed))  # type: ignore[arg-type]
    print(f"[{timestamp()}] Extracted {len(records)} records (n={n}, seed={seed})")
    return records


@task
def transform(records: list[dict[str, object]], **context: object) -> list[dict[str, object]]:
    """Transform records based on params (temp_unit, include_nulls)."""
    params = context.get("params", DAG_PARAMS)
    if not isinstance(params, dict):
        params = DAG_PARAMS
    temp_unit = params.get("temp_unit", "celsius")
    include_nulls = params.get("include_nulls", False)

    transformed: list[dict[str, object]] = []
    for record in records:
        row = dict(record)

        # Apply unit conversion based on param
        if temp_unit == "fahrenheit" and "temperature_c" in row:
            temp_c = row["temperature_c"]
            if isinstance(temp_c, (int, float)):
                row["temperature"] = round(temp_c * 9 / 5 + 32, 1)
                row["temp_unit"] = "F"
        else:
            row["temperature"] = row.get("temperature_c")
            row["temp_unit"] = "C"

        # Optionally include nulls for testing quality checks
        if not include_nulls:
            row = {k: v for k, v in row.items() if v is not None}

        transformed.append(row)

    print(f"[{timestamp()}] Transformed {len(transformed)} records (unit={temp_unit}, nulls={include_nulls})")
    return transformed


@task
def validate(records: list[dict[str, object]], **context: object) -> dict[str, object]:
    """Validate records using param thresholds."""
    params = context.get("params", DAG_PARAMS)
    if not isinstance(params, dict):
        params = DAG_PARAMS
    min_rows = int(params.get("min_rows", 10))  # type: ignore[arg-type]
    max_null_pct = float(params.get("max_null_pct", 5.0))  # type: ignore[arg-type]

    n = len(records)
    row_check = n >= min_rows

    # Count null-like values
    null_count = 0
    total_fields = 0
    for record in records:
        for v in record.values():
            total_fields += 1
            if v is None:
                null_count += 1

    null_pct = (null_count / total_fields * 100) if total_fields > 0 else 0
    null_check = null_pct <= max_null_pct

    result = {
        "row_count": n,
        "min_rows": min_rows,
        "row_check_passed": row_check,
        "null_pct": round(null_pct, 2),
        "max_null_pct": max_null_pct,
        "null_check_passed": null_check,
        "all_passed": row_check and null_check,
    }
    print(f"[{timestamp()}] Validation: rows={n} (min={min_rows}), nulls={null_pct:.1f}% (max={max_null_pct}%)")
    print(f"[{timestamp()}] Result: {'PASS' if result['all_passed'] else 'FAIL'}")
    return result


@task
def load(records: list[dict[str, object]], validation: dict[str, object], **context: object) -> str:
    """Write output based on params (output_dir, output_format)."""
    params = context.get("params", DAG_PARAMS)
    if not isinstance(params, dict):
        params = DAG_PARAMS
    out_dir = str(params.get("output_dir", OUTPUT_DIR))
    out_format = str(params.get("output_format", "csv"))

    os.makedirs(out_dir, exist_ok=True)

    if out_format == "parquet":
        import pandas as pd

        df = pd.DataFrame(records)
        output_path = f"{out_dir}/output.parquet"
        df.to_parquet(output_path, index=False)
    else:
        import csv

        output_path = f"{out_dir}/output.csv"
        if records:
            fieldnames = list(records[0].keys())
            with open(output_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(records)

    print(f"[{timestamp()}] Wrote {len(records)} records to {output_path} (format={out_format})")
    print(f"[{timestamp()}] Validation: {'PASSED' if validation['all_passed'] else 'FAILED'}")
    return output_path


with DAG(
    dag_id="080_parameterized_pipeline",
    default_args=DEFAULT_ARGS,
    description="Config-driven pipeline with DAG params and defaults",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "testing"],
    params=DAG_PARAMS,
) as dag:
    raw = extract()
    transformed = transform(records=raw)
    validated = validate(records=transformed)
    output = load(records=transformed, validation=validated)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up'",
    )
    output >> cleanup

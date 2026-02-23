"""DAG 72: Consolidated Quality Dashboard.

Demonstrates orchestrating multiple quality check suites and producing
a comprehensive report with overall quality scoring. Shows check
aggregation, pass/fail/warn breakdown, and actionable reporting.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

DATA_DIR = str(OUTPUT_BASE / "quality_dashboard")


@task
def generate_data() -> dict[str, str]:
    """Create a realistic multi-file dataset for comprehensive quality checks."""
    import numpy as np
    import pandas as pd

    os.makedirs(DATA_DIR, exist_ok=True)
    rng = np.random.default_rng(42)

    # Stations master
    stations = pd.DataFrame({
        "station_id": ["oslo_01", "bergen_01", "tromso_01", "stavanger_01", "trondheim_01"],
        "name": ["Oslo Central", "Bergen Harbor", "Tromso Arctic", "Stavanger Coast", "Trondheim City"],
        "lat": [59.91, 60.39, 69.65, 58.97, 63.43],
        "lon": [10.75, 5.32, 18.96, 5.73, 10.40],
    })
    stations_path = f"{DATA_DIR}/stations.csv"
    stations.to_csv(stations_path, index=False)

    # Observations with some quality issues
    temps = rng.normal(10, 8, 100).round(1)
    temps[5] = 65.0  # Out of range
    temps[20] = -55.0  # Out of range

    humidity = rng.uniform(30, 95, 100).round(1)
    humidity_list: list[float | None] = [float(h) for h in humidity]
    for i in [10, 11, 12, 13, 14]:
        humidity_list[i] = None

    obs = pd.DataFrame({
        "station_id": rng.choice(["oslo_01", "bergen_01", "tromso_01", "stavanger_01", "trondheim_01"], 100),
        "date": [f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(100)],
        "temperature_c": temps,
        "humidity_pct": humidity_list,
        "pressure_hpa": rng.normal(1013, 10, 100).round(1),
    })
    obs_path = f"{DATA_DIR}/observations.csv"
    obs.to_csv(obs_path, index=False)

    print(f"[{timestamp()}] Generated {len(stations)} stations, {len(obs)} observations")
    return {"stations": stations_path, "observations": obs_path}


@task
def run_schema_checks(paths: dict[str, str]) -> list[dict[str, object]]:
    """Run schema validation suite."""
    import pandas as pd

    from airflow_examples.quality import QualityResult, check_nulls, check_schema

    df = pd.read_csv(paths["observations"])
    results: list[QualityResult] = []

    results.append(check_schema(df, ["station_id", "date", "temperature_c", "humidity_pct", "pressure_hpa"]))
    results.append(check_nulls(df, ["station_id", "date"], max_null_pct=0.0))
    results.append(check_nulls(df, ["temperature_c", "humidity_pct", "pressure_hpa"], max_null_pct=10.0))

    print(f"[{timestamp()}] Schema checks: {sum(1 for r in results if r.passed)}/{len(results)} passed")
    return [{"check": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity} for r in results]


@task
def run_statistical_checks(paths: dict[str, str]) -> list[dict[str, object]]:
    """Run bounds and z-score checks."""
    import numpy as np
    import pandas as pd

    from airflow_examples.quality import QualityResult, check_bounds

    df = pd.read_csv(paths["observations"])
    results: list[QualityResult] = []

    results.append(check_bounds(df, "temperature_c", -50.0, 60.0))
    results.append(check_bounds(df, "humidity_pct", 0.0, 100.0))
    results.append(check_bounds(df, "pressure_hpa", 870.0, 1084.0))

    # Z-score check
    for col in ["temperature_c", "pressure_hpa"]:
        values = pd.to_numeric(df[col], errors="coerce").dropna()
        mean = float(values.mean())
        std = float(values.std())
        if std > 0:
            z_scores = np.abs((values - mean) / std)
            outliers = int((z_scores > 3).sum())
            results.append(QualityResult(
                check_name=f"zscore_{col}",
                passed=outliers == 0,
                details=f"{outliers} outliers (|z| > 3)",
                severity="warning" if outliers > 0 else "info",
            ))

    print(f"[{timestamp()}] Statistical checks: {sum(1 for r in results if r.passed)}/{len(results)} passed")
    return [{"check": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity} for r in results]


@task
def run_freshness_checks(paths: dict[str, str]) -> list[dict[str, object]]:
    """Run freshness and completeness checks."""
    import pandas as pd

    from airflow_examples.quality import QualityResult, check_freshness, check_row_count

    results: list[QualityResult] = []

    # Freshness
    for label, path in paths.items():
        results.append(check_freshness(path, max_age_seconds=3600.0))

    # Row counts
    obs = pd.read_csv(paths["observations"])
    results.append(check_row_count(obs, min_rows=50, max_rows=1000))

    stations = pd.read_csv(paths["stations"])
    results.append(check_row_count(stations, min_rows=3, max_rows=100))

    print(f"[{timestamp()}] Freshness checks: {sum(1 for r in results if r.passed)}/{len(results)} passed")
    return [{"check": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity} for r in results]


@task
def compile_report(
    schema: list[dict[str, object]],
    statistical: list[dict[str, object]],
    freshness: list[dict[str, object]],
) -> None:
    """Aggregate all results and print comprehensive quality report."""
    all_results = schema + statistical + freshness
    total = len(all_results)
    passed = sum(1 for r in all_results if r["passed"])
    score = (passed / total * 100) if total > 0 else 0

    print(f"[{timestamp()}] === Consolidated Quality Report ===")
    print(f"\n  Overall Score: {score:.0f}% ({passed}/{total} checks passed)")
    print(f"  Status: {'HEALTHY' if score >= 90 else 'DEGRADED' if score >= 70 else 'CRITICAL'}")

    # Group by category
    categories = [("Schema", schema), ("Statistical", statistical), ("Freshness", freshness)]
    for name, results in categories:
        cat_passed = sum(1 for r in results if r["passed"])
        print(f"\n  {name}: {cat_passed}/{len(results)} passed")
        for r in results:
            status = "PASS" if r["passed"] else "FAIL"
            print(f"    [{status}] [{str(r['severity']).upper()}] {r['check']}: {r['details']}")

    # Recommendations
    failures = [r for r in all_results if not r["passed"]]
    if failures:
        print("\n  Recommended Actions:")
        for r in failures:
            print(f"    - Fix {r['check']}: {r['details']}")
    else:
        print("\n  All checks passed - no action required")


with DAG(
    dag_id="072_quality_report",
    default_args=DEFAULT_ARGS,
    description="Consolidated quality dashboard with scoring and recommendations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "quality"],
) as dag:
    data = generate_data()
    schema = run_schema_checks(paths=data)
    stats = run_statistical_checks(paths=data)
    fresh = run_freshness_checks(paths=data)
    r = compile_report(schema=schema, statistical=stats, freshness=fresh)

    data >> [schema, stats, fresh] >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {DATA_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

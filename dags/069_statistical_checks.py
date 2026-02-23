"""DAG 69: Statistical Anomaly Detection.

Demonstrates detecting data anomalies using statistical methods:
physical bounds checking, z-score outlier detection, and distribution
shift detection via mean comparison. Shows severity-ranked anomaly
reporting.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

DATA_DIR = str(OUTPUT_BASE / "stats_checks")


@task
def generate_data() -> str:
    """Create Parquet with normal data, injected outliers, and a mean shift."""
    import os

    import numpy as np
    import pandas as pd

    os.makedirs(DATA_DIR, exist_ok=True)
    rng = np.random.default_rng(42)
    n = 200

    # Normal data for first 90%
    n_normal = int(n * 0.9)
    n_shifted = n - n_normal

    temperature = np.concatenate([
        rng.normal(15, 5, n_normal),
        rng.normal(35, 5, n_shifted),  # Mean shift in last 10%
    ])
    # Inject a few extreme outliers
    temperature[10] = 65.0  # Beyond physical range
    temperature[50] = -55.0  # Beyond physical range

    humidity = rng.normal(60, 15, n)
    humidity[20] = 110.0  # Beyond physical range
    humidity[80] = -5.0  # Beyond physical range

    pressure = rng.normal(1013, 10, n)

    df = pd.DataFrame({
        "record_id": range(n),
        "temperature_c": np.round(temperature, 1),
        "humidity_pct": np.round(humidity, 1),
        "pressure_hpa": np.round(pressure, 1),
    })

    path = f"{DATA_DIR}/observations.parquet"
    df.to_parquet(path, index=False)
    print(f"[{timestamp()}] Generated {len(df)} records with outliers and mean shift")
    return path


@task
def check_bounds(path: str) -> list[dict[str, object]]:
    """Verify values within physical ranges."""
    import pandas as pd

    from airflow_examples.quality import check_bounds as qcheck_bounds

    df = pd.read_parquet(path)
    results: list[dict[str, object]] = []

    checks = [
        ("temperature_c", -50.0, 60.0),
        ("humidity_pct", 0.0, 100.0),
        ("pressure_hpa", 870.0, 1084.0),
    ]

    for col, lo, hi in checks:
        r = qcheck_bounds(df, col, lo, hi)
        results.append({"check": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity})
        status = "PASS" if r.passed else "FAIL"
        print(f"[{timestamp()}] Bounds {col} [{lo},{hi}]: {status} - {r.details}")

    return results


@task
def check_zscore(path: str) -> list[dict[str, object]]:
    """Flag individual values > 3 standard deviations from column mean."""
    import numpy as np
    import pandas as pd

    df = pd.read_parquet(path)
    results: list[dict[str, object]] = []

    for col in ["temperature_c", "humidity_pct", "pressure_hpa"]:
        values = df[col].astype(float)
        mean = values.mean()
        std = values.std()
        if std == 0:
            continue

        z_scores = np.abs((values - mean) / std)
        outliers = (z_scores > 3).sum()

        passed = int(outliers) == 0
        details = f"{outliers} outliers (|z| > 3), mean={mean:.1f}, std={std:.1f}"
        results.append({
            "check": f"zscore_{col}",
            "passed": passed,
            "details": details,
            "severity": "warning" if not passed else "info",
        })
        print(f"[{timestamp()}] Z-score {col}: {'PASS' if passed else 'FAIL'} - {details}")

    return results


@task
def check_distribution(path: str) -> list[dict[str, object]]:
    """Compare first-half vs second-half statistics for mean shift detection."""
    import pandas as pd

    df = pd.read_parquet(path)
    mid = len(df) // 2
    results: list[dict[str, object]] = []

    for col in ["temperature_c", "humidity_pct"]:
        first_half = df[col].iloc[:mid]
        second_half = df[col].iloc[mid:]

        mean_diff = abs(float(second_half.mean() - first_half.mean()))
        first_std = float(first_half.std())
        threshold = first_std * 1.5  # Flag if shift > 1.5 std

        passed = mean_diff <= threshold
        details = (
            f"First half mean={first_half.mean():.1f}, second half mean={second_half.mean():.1f}, "
            f"diff={mean_diff:.1f}, threshold={threshold:.1f}"
        )
        results.append({
            "check": f"distribution_shift_{col}",
            "passed": passed,
            "details": details,
            "severity": "critical" if not passed else "info",
        })
        print(f"[{timestamp()}] Distribution {col}: {'PASS' if passed else 'FAIL'} - {details}")

    return results


@task
def anomaly_report(
    bounds: list[dict[str, object]],
    zscore: list[dict[str, object]],
    distribution: list[dict[str, object]],
) -> None:
    """Print severity-ranked anomaly report."""
    all_results = bounds + zscore + distribution

    # Sort by severity: critical first, then warning, then info
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    all_results.sort(key=lambda r: severity_order.get(str(r.get("severity", "info")), 3))

    print(f"[{timestamp()}] === Statistical Anomaly Report ===")
    print(f"  Total checks: {len(all_results)}")
    print(f"  Passed: {sum(1 for r in all_results if r['passed'])}")
    print(f"  Failed: {sum(1 for r in all_results if not r['passed'])}")
    print()

    for r in all_results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] [{str(r['severity']).upper()}] {r['check']}")
        print(f"           {r['details']}")

    failed = [r for r in all_results if not r["passed"]]
    if failed:
        print("\n  Recommended actions:")
        for r in failed:
            if "bounds" in str(r["check"]):
                print(f"    - Review out-of-range values in {r['check']}")
            elif "zscore" in str(r["check"]):
                print(f"    - Investigate outlier values in {r['check']}")
            elif "distribution" in str(r["check"]):
                print(f"    - Check for data source changes affecting {r['check']}")


with DAG(
    dag_id="069_statistical_checks",
    default_args=DEFAULT_ARGS,
    description="Statistical anomaly detection: bounds, z-score, distribution shifts",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "quality"],
) as dag:
    data = generate_data()
    b = check_bounds(path=data)
    z = check_zscore(path=data)
    d = check_distribution(path=data)
    r = anomaly_report(bounds=b, zscore=z, distribution=d)

    data >> [b, z, d] >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {DATA_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

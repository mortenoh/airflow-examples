"""DAG 70: Freshness and Completeness Checks.

Demonstrates verifying that data files are recent, have enough rows,
cover expected date ranges, and stay within null rate thresholds.
Shows traffic-light reporting (green/yellow/red) for data quality.
"""

import os
import time
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

DATA_DIR = str(OUTPUT_BASE / "freshness_checks")


@task
def generate_data() -> dict[str, str]:
    """Create multiple CSVs: fresh, stale, with date gaps, and excessive nulls."""
    import numpy as np
    import pandas as pd

    os.makedirs(DATA_DIR, exist_ok=True)

    # Fresh file (created now)
    fresh = pd.DataFrame({
        "station": ["oslo_01"] * 30,
        "date": pd.date_range("2024-01-01", periods=30, freq="D").strftime("%Y-%m-%d"),
        "temperature_c": np.round(np.random.default_rng(42).normal(10, 5, 30), 1),
    })
    fresh_path = f"{DATA_DIR}/fresh_data.csv"
    fresh.to_csv(fresh_path, index=False)

    # Stale file (touch with old mtime)
    stale_path = f"{DATA_DIR}/stale_data.csv"
    fresh.to_csv(stale_path, index=False)
    old_time = time.time() - 7200  # 2 hours ago
    os.utime(stale_path, (old_time, old_time))

    # File with date gaps (missing days 10-14)
    dates = [f"2024-01-{d:02d}" for d in range(1, 31) if d not in range(10, 15)]
    gappy = pd.DataFrame({
        "station": ["oslo_01"] * len(dates),
        "date": dates,
        "temperature_c": np.round(np.random.default_rng(43).normal(10, 5, len(dates)), 1),
    })
    gappy_path = f"{DATA_DIR}/gappy_data.csv"
    gappy.to_csv(gappy_path, index=False)

    # File with excessive nulls
    rng = np.random.default_rng(44)
    temps: list[float | None] = [round(float(x), 1) for x in rng.normal(10, 5, 30)]
    for i in range(0, 30, 3):
        temps[i] = None
    nullish = pd.DataFrame({
        "station": ["oslo_01"] * 30,
        "date": pd.date_range("2024-01-01", periods=30, freq="D").strftime("%Y-%m-%d"),
        "temperature_c": temps,
    })
    nullish_path = f"{DATA_DIR}/nullish_data.csv"
    nullish.to_csv(nullish_path, index=False)

    print(f"[{timestamp()}] Generated 4 test files (fresh, stale, gappy, nullish)")
    return {"fresh": fresh_path, "stale": stale_path, "gappy": gappy_path, "nullish": nullish_path}


@task
def check_freshness(paths: dict[str, str]) -> list[dict[str, object]]:
    """Verify file modification time within threshold."""
    from airflow_examples.quality import check_freshness as qcheck

    results: list[dict[str, object]] = []
    max_age = 3600.0  # 1 hour

    for label, path in paths.items():
        r = qcheck(path, max_age)
        results.append({"file": label, "passed": r.passed, "details": r.details, "severity": r.severity})
        print(f"[{timestamp()}] Freshness [{label}]: {'PASS' if r.passed else 'FAIL'} - {r.details}")

    return results


@task
def check_row_counts(paths: dict[str, str]) -> list[dict[str, object]]:
    """Verify min/max row count constraints per file."""
    import pandas as pd

    from airflow_examples.quality import check_row_count

    results: list[dict[str, object]] = []
    for label, path in paths.items():
        df = pd.read_csv(path)
        r = check_row_count(df, min_rows=25, max_rows=100)
        results.append({"file": label, "passed": r.passed, "details": r.details, "severity": r.severity})
        print(f"[{timestamp()}] Row count [{label}]: {'PASS' if r.passed else 'FAIL'} - {r.details}")

    return results


@task
def check_date_coverage(paths: dict[str, str]) -> list[dict[str, object]]:
    """Find missing dates in time series (expected daily coverage)."""
    import pandas as pd

    results: list[dict[str, object]] = []

    for label, path in paths.items():
        df = pd.read_csv(path)
        if "date" not in df.columns:
            results.append({"file": label, "passed": False, "details": "No date column", "severity": "critical"})
            continue

        dates = pd.to_datetime(df["date"])
        full_range = pd.date_range(dates.min(), dates.max(), freq="D")
        missing = full_range.difference(dates)

        passed = len(missing) == 0
        details = f"{len(missing)} missing dates out of {len(full_range)} expected"
        if len(missing) > 0:
            missing_strs = [d.strftime("%Y-%m-%d") for d in missing[:5]]
            details += f" (first: {missing_strs})"

        severity = "warning" if not passed else "info"
        results.append({"file": label, "passed": passed, "details": details, "severity": severity})
        print(f"[{timestamp()}] Date coverage [{label}]: {'PASS' if passed else 'FAIL'} - {details}")

    return results


@task
def check_null_rates(paths: dict[str, str]) -> list[dict[str, object]]:
    """Verify null percentages per column below thresholds."""
    import pandas as pd

    from airflow_examples.quality import check_nulls

    results: list[dict[str, object]] = []
    for label, path in paths.items():
        df = pd.read_csv(path)
        r = check_nulls(df, list(df.columns), max_null_pct=10.0)
        results.append({"file": label, "passed": r.passed, "details": r.details, "severity": r.severity})
        print(f"[{timestamp()}] Null rate [{label}]: {'PASS' if r.passed else 'FAIL'} - {r.details}")

    return results


@task
def quality_summary(
    freshness: list[dict[str, object]],
    row_counts: list[dict[str, object]],
    date_coverage: list[dict[str, object]],
    null_rates: list[dict[str, object]],
) -> None:
    """Print traffic-light summary (green/yellow/red per check category)."""
    categories = {
        "Freshness": freshness,
        "Row Counts": row_counts,
        "Date Coverage": date_coverage,
        "Null Rates": null_rates,
    }

    print(f"[{timestamp()}] === Quality Summary (Traffic Light) ===")
    for category, results in categories.items():
        passed = sum(1 for r in results if r["passed"])
        total = len(results)
        if passed == total:
            light = "GREEN"
        elif passed >= total // 2:
            light = "YELLOW"
        else:
            light = "RED"
        print(f"  [{light:6s}] {category}: {passed}/{total} passed")

    all_results = freshness + row_counts + date_coverage + null_rates
    total_passed = sum(1 for r in all_results if r["passed"])
    total_checks = len(all_results)
    print(f"\n  Overall: {total_passed}/{total_checks} checks passed ({total_passed / total_checks * 100:.0f}%)")


with DAG(
    dag_id="070_freshness_completeness",
    default_args=DEFAULT_ARGS,
    description="Freshness, row count, date coverage, and null rate checks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "quality"],
) as dag:
    data = generate_data()
    f = check_freshness(paths=data)
    rc = check_row_counts(paths=data)
    dc = check_date_coverage(paths=data)
    nr = check_null_rates(paths=data)
    s = quality_summary(freshness=f, row_counts=rc, date_coverage=dc, null_rates=nr)

    data >> [f, rc, dc, nr] >> s  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {DATA_DIR} && echo 'Cleaned up'",
    )
    s >> cleanup

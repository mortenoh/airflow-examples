"""DAG 93: Earthquake + Weather Null Hypothesis.

Fetches earthquakes and weather for Iceland (a seismically active region),
aligns the datasets on date, computes Pearson correlation, and tests
the null hypothesis. This is a data science lesson in debunking
spurious correlations.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/93_null_hypothesis")


@task
def fetch_earthquakes() -> list[dict[str, object]]:
    """Fetch earthquakes for Iceland region over past 365 days."""
    from airflow_examples.apis import fetch_usgs_earthquakes

    quakes = fetch_usgs_earthquakes(
        start="2024-01-01",
        end="2024-12-31",
        min_magnitude=2.0,
        limit=500,
    )
    iceland = [q for q in quakes
               if 63.0 <= float(str(q.get("lat", 0))) <= 67.0
               and -25.0 <= float(str(q.get("lon", 0))) <= -13.0]
    print(f"[{timestamp()}] Fetched {len(iceland)} Iceland earthquakes (of {len(quakes)} total)")
    return iceland


@task
def fetch_weather() -> dict[str, object]:
    """Fetch daily weather archive for Reykjavik, 2024."""
    from airflow_examples.apis import OPEN_METEO_ARCHIVE, fetch_open_meteo

    params = {
        "latitude": 64.15,
        "longitude": -21.95,
        "daily": "temperature_2m_mean,pressure_msl_mean",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    }
    data = fetch_open_meteo(OPEN_METEO_ARCHIVE, params)
    daily = data.get("daily", {})
    print(f"[{timestamp()}] Weather: {len(daily.get('time', []))} daily records for Reykjavik")
    return {"daily": daily}


@task
def align_datasets(
    quakes: list[dict[str, object]],
    weather: dict[str, object],
) -> dict[str, object]:
    """Merge earthquake counts per day with daily weather."""
    import pandas as pd

    df_weather = pd.DataFrame(weather.get("daily", {}))
    df_weather["time"] = pd.to_datetime(df_weather["time"]).dt.date.astype(str)

    quake_dates: list[str] = []
    for q in quakes:
        t = q.get("time")
        if t is not None:
            quake_dates.append(str(pd.Timestamp(int(str(t)), unit="ms").date()))

    quake_counts = pd.Series(quake_dates).value_counts().reset_index()
    quake_counts.columns = ["time", "quake_count"]

    merged = df_weather.merge(quake_counts, on="time", how="left")
    merged["quake_count"] = merged["quake_count"].fillna(0)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    merged.to_csv(f"{OUTPUT_DIR}/aligned.csv", index=False)
    print(f"[{timestamp()}] Aligned: {len(merged)} days, {int(merged['quake_count'].sum())} total quakes")
    return {"aligned": merged.to_dict(orient="list"), "days": len(merged)}


@task
def compute_correlation(aligned: dict[str, object]) -> dict[str, object]:
    """Compute Pearson correlation between seismic count and weather variables."""
    import math

    import pandas as pd

    from airflow_examples.transforms import safe_pearson

    data: dict[str, list[object]] = aligned.get("aligned", {})  # type: ignore[assignment]
    df = pd.DataFrame(data)

    results: dict[str, dict[str, float | None]] = {}
    for weather_var in ["temperature_2m_mean", "pressure_msl_mean"]:
        if weather_var not in df.columns:
            continue
        valid = df[["quake_count", weather_var]].dropna()
        if len(valid) < 3:
            continue

        x_vals = valid[weather_var].astype(float).tolist()
        y_vals = valid["quake_count"].astype(float).tolist()
        stats = safe_pearson(x_vals, y_vals)

        results[weather_var] = stats
        r = stats.get("r") or 0
        p = stats.get("p_approx") or 1
        r_str = f"{r:.4f}" if isinstance(r, float) and math.isfinite(r) else "N/A"
        print(f"[{timestamp()}] {weather_var} vs quake_count: r={r_str}, p~{p:.4f}")

    return {"correlations": results}


@task
def null_hypothesis_test(correlations: dict[str, object]) -> dict[str, object]:
    """Interpret results and explain why correlation ~ 0 is expected."""
    corr: dict[str, dict[str, float]] = correlations.get("correlations", {})  # type: ignore[assignment]

    interpretations: list[dict[str, object]] = []
    for var, stats in corr.items():
        r = stats.get("r") or 0
        significant = abs(r) > 0.1

        interp = {
            "variable": var,
            "r": r,
            "significant": significant,
            "interpretation": (
                f"Weak correlation (r={r:.4f}) between {var} and earthquake frequency. "
                "This is expected: earthquakes are driven by tectonic forces, not surface weather. "
                "Any apparent correlation is likely spurious and would not replicate across different "
                "time periods or regions."
            ),
        }
        interpretations.append(interp)

    lesson = (
        "NULL HYPOTHESIS TESTING LESSON:\n"
        "H0: There is no linear relationship between weather variables and earthquake frequency.\n"
        "H1: There is a linear relationship.\n\n"
        "With small correlation coefficients and high p-values, we FAIL TO REJECT H0.\n"
        "This is the expected result -- weather does not cause earthquakes.\n\n"
        "Key takeaways:\n"
        "1. Correlation does not imply causation\n"
        "2. Testing obvious null hypotheses validates your statistical pipeline\n"
        "3. Spurious correlations are common in large datasets (data dredging)\n"
        "4. Always consider the physical mechanism before claiming a relationship"
    )

    print(f"[{timestamp()}] {lesson}")
    return {"interpretations": interpretations, "lesson": lesson}


@task
def report(
    aligned: dict[str, object],
    correlations: dict[str, object],
    hypothesis: dict[str, object],
) -> None:
    """Print correlation results, p-values, and data science lesson."""
    print(f"\n[{timestamp()}] === Earthquake-Weather Correlation Report ===")
    print(f"  Days analyzed: {aligned.get('days')}")

    corr: dict[str, dict[str, float]] = correlations.get("correlations", {})  # type: ignore[assignment]
    for var, stats in corr.items():
        r = stats.get("r") or 0
        p = stats.get("p_approx") or 1
        print(f"  {var}: r={r:.4f}, p~{p:.4f}")

    interps: list[dict[str, object]] = hypothesis.get("interpretations", [])  # type: ignore[assignment]
    for i in interps:
        print(f"\n  {i.get('interpretation')}")

    print("\n  Conclusion: Weather does not predict earthquakes.")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="093_earthquake_weather_correlation",
    default_args=DEFAULT_ARGS,
    description="Earthquake + weather null hypothesis test (data science lesson)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    quakes = fetch_earthquakes()
    weather = fetch_weather()
    aligned = align_datasets(quakes=quakes, weather=weather)
    corr = compute_correlation(aligned=aligned)
    hypothesis = null_hypothesis_test(correlations=corr)
    report(aligned=aligned, correlations=corr, hypothesis=hypothesis)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

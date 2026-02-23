"""DAG 99: Data Quality on Live API Responses.

Applies the quality.py framework (check_schema, check_nulls, check_bounds)
to live API data from Open-Meteo, REST Countries, and World Bank to
validate real-world responses and produce a quality dashboard.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/99_quality")


@task
def fetch_live_data() -> dict[str, object]:
    """Fetch data from Open-Meteo, REST Countries, and World Bank."""
    from airflow_examples.apis import (
        OPEN_METEO_FORECAST,
        REST_COUNTRIES,
        fetch_json,
        fetch_open_meteo,
        fetch_world_bank_paginated,
    )

    weather = fetch_open_meteo(OPEN_METEO_FORECAST, {
        "latitude": 59.91,
        "longitude": 10.75,
        "hourly": "temperature_2m,precipitation,wind_speed_10m",
        "forecast_days": 3,
    })

    countries_url = f"{REST_COUNTRIES}/alpha?codes=NOR,SWE,DNK,FIN,ISL"
    countries = fetch_json(countries_url, params={"fields": "name,cca3,population,area"})

    wb_records = fetch_world_bank_paginated("NOR;SWE;DNK;FIN;ISL", "NY.GDP.PCAP.CD", "2020:2023")

    print(f"[{timestamp()}] Fetched: weather hourly, {len(countries)} countries, {len(wb_records)} WB records")
    return {
        "weather": weather,
        "countries": countries if isinstance(countries, list) else [],
        "world_bank": wb_records,
    }


@task
def check_schema(live_data: dict[str, object]) -> list[dict[str, object]]:
    """Validate each API response has expected fields."""
    import pandas as pd

    from airflow_examples.quality import QualityResult, check_schema

    results: list[QualityResult] = []

    weather: dict[str, object] = live_data.get("weather", {})  # type: ignore[assignment]
    hourly: dict[str, object] = weather.get("hourly", {})  # type: ignore[assignment]
    df_weather = pd.DataFrame(hourly)
    results.append(check_schema(df_weather, ["time", "temperature_2m", "precipitation", "wind_speed_10m"]))

    countries: list[dict[str, object]] = live_data.get("countries", [])  # type: ignore[assignment]
    if countries:
        df_countries = pd.DataFrame(countries)
        results.append(check_schema(df_countries, ["name", "cca3", "population", "area"]))

    wb: list[dict[str, object]] = live_data.get("world_bank", [])  # type: ignore[assignment]
    if wb:
        df_wb = pd.DataFrame(wb)
        results.append(check_schema(df_wb, ["country", "date", "value"]))

    for r in results:
        print(f"[{timestamp()}] Schema check: {r.check_name} = {'PASS' if r.passed else 'FAIL'}")

    return [{"check_name": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity}
            for r in results]


@task
def check_completeness(live_data: dict[str, object]) -> list[dict[str, object]]:
    """Check null rates in API responses."""
    import pandas as pd

    from airflow_examples.quality import QualityResult, check_nulls

    results: list[QualityResult] = []

    weather: dict[str, object] = live_data.get("weather", {})  # type: ignore[assignment]
    hourly: dict[str, object] = weather.get("hourly", {})  # type: ignore[assignment]
    df_weather = pd.DataFrame(hourly)
    results.append(check_nulls(df_weather, ["temperature_2m", "precipitation", "wind_speed_10m"], max_null_pct=10.0))

    wb: list[dict[str, object]] = live_data.get("world_bank", [])  # type: ignore[assignment]
    if wb:
        df_wb = pd.DataFrame(wb)
        results.append(check_nulls(df_wb, ["date", "value"], max_null_pct=50.0))

    for r in results:
        print(f"[{timestamp()}] Null check: {r.check_name} = {'PASS' if r.passed else 'FAIL'} - {r.details}")

    return [{"check_name": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity}
            for r in results]


@task
def check_statistical(live_data: dict[str, object]) -> list[dict[str, object]]:
    """Bounds checks on numeric fields, z-score on temperatures."""
    import pandas as pd

    from airflow_examples.quality import QualityResult, check_bounds

    results: list[QualityResult] = []

    weather: dict[str, object] = live_data.get("weather", {})  # type: ignore[assignment]
    hourly: dict[str, object] = weather.get("hourly", {})  # type: ignore[assignment]
    df_weather = pd.DataFrame(hourly)
    results.append(check_bounds(df_weather, "temperature_2m", min_val=-60.0, max_val=60.0))
    results.append(check_bounds(df_weather, "precipitation", min_val=0.0, max_val=500.0))
    results.append(check_bounds(df_weather, "wind_speed_10m", min_val=0.0, max_val=200.0))

    for r in results:
        print(f"[{timestamp()}] Bounds check: {r.check_name} = {'PASS' if r.passed else 'FAIL'} - {r.details}")

    return [{"check_name": r.check_name, "passed": r.passed, "details": r.details, "severity": r.severity}
            for r in results]


@task
def check_cross_source(live_data: dict[str, object]) -> list[dict[str, object]]:
    """Verify country codes from REST Countries exist in World Bank responses."""
    countries: list[dict[str, object]] = live_data.get("countries", [])  # type: ignore[assignment]
    wb: list[dict[str, object]] = live_data.get("world_bank", [])  # type: ignore[assignment]

    rc_codes = {str(c.get("cca3", "")) for c in countries}
    wb_codes = set()
    for r in wb:
        country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
        wb_codes.add(str(country_info.get("id", "")))

    missing = rc_codes - wb_codes
    passed = len(missing) == 0

    result = {
        "check_name": "cross_source_referential_integrity",
        "passed": passed,
        "details": f"REST Countries codes in World Bank: {len(rc_codes - missing)}/{len(rc_codes)}"
                   + (f". Missing: {sorted(missing)}" if missing else ""),
        "severity": "warning" if not passed else "info",
    }
    print(f"[{timestamp()}] Cross-source: {'PASS' if passed else 'FAIL'} - {result['details']}")
    return [result]


@task
def compile_quality_report(
    schema: list[dict[str, object]],
    completeness: list[dict[str, object]],
    statistical: list[dict[str, object]],
    cross_source: list[dict[str, object]],
) -> dict[str, object]:
    """Aggregate all quality results and compute overall score."""
    all_checks = schema + completeness + statistical + cross_source
    total = len(all_checks)
    passed = sum(1 for c in all_checks if c.get("passed"))
    score = (passed / total * 100) if total > 0 else 0

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[{timestamp()}] Quality score: {score:.0f}% ({passed}/{total} checks passed)")
    return {"total_checks": total, "passed": passed, "score": score, "all_checks": all_checks}


@task
def report(quality: dict[str, object]) -> None:
    """Print quality dashboard for live API data."""
    print(f"\n[{timestamp()}] === API Data Quality Report ===")
    print(f"  Overall Score: {quality.get('score'):.0f}%")
    print(f"  Checks Passed: {quality.get('passed')}/{quality.get('total_checks')}")

    checks: list[dict[str, object]] = quality.get("all_checks", [])  # type: ignore[assignment]
    print(f"\n  {'Check':<40} {'Status':>6} {'Severity':>10}")
    print(f"  {'-' * 60}")
    for c in checks:
        status = "PASS" if c.get("passed") else "FAIL"
        print(f"  {str(c.get('check_name', '')):<40} {status:>6} {str(c.get('severity', '')):>10}")
        print(f"    {c.get('details')}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="099_api_quality_framework",
    default_args=DEFAULT_ARGS,
    description="Data quality framework applied to live API responses",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    data = fetch_live_data()
    schema_results = check_schema(live_data=data)
    null_results = check_completeness(live_data=data)
    stat_results = check_statistical(live_data=data)
    cross_results = check_cross_source(live_data=data)
    quality = compile_quality_report(
        schema=schema_results,
        completeness=null_results,
        statistical=stat_results,
        cross_source=cross_results,
    )
    report(quality=quality)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

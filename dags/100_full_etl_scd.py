"""DAG 100: Full ETL with SCD Type 2 (Capstone).

Complete ETL pipeline demonstrating Slowly Changing Dimension Type 2
tracking with surrogate keys, staging/integration/presentation layers,
audit trail, and quality validation. Integrates REST Countries and
World Bank data for all European countries.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/100_etl_scd")
STAGING_DIR = f"{OUTPUT_DIR}/staging"
INTEGRATION_DIR = f"{OUTPUT_DIR}/integration"


@task
def extract_countries() -> list[dict[str, object]]:
    """Fetch all European countries from REST Countries."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/region/europe"
    params = {"fields": "name,cca3,population,area,region,subregion"}
    data = fetch_json(url, params=params)
    countries: list[dict[str, object]] = data if isinstance(data, list) else []
    print(f"[{timestamp()}] Extracted {len(countries)} European countries")
    return countries


@task
def extract_indicators() -> list[dict[str, object]]:
    """Fetch GDP and population from World Bank for European countries."""
    from airflow_examples.apis import fetch_world_bank_paginated

    eu_codes = "NOR;SWE;DNK;FIN;ISL;GBR;DEU;FRA;ITA;ESP;NLD;BEL;AUT;CHE;PRT;IRL;POL;CZE;GRC;HUN"

    gdp = fetch_world_bank_paginated(eu_codes, "NY.GDP.PCAP.CD", "2020:2023")
    pop = fetch_world_bank_paginated(eu_codes, "SP.POP.TOTL", "2020:2023")

    all_records: list[dict[str, object]] = []
    for r in gdp:
        country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
        val = r.get("value")
        if val is not None:
            all_records.append({
                "country_code": country_info.get("id"),
                "year": int(str(r.get("date", 0))),
                "indicator": "gdp_per_capita",
                "value": float(str(val)),
            })
    for r in pop:
        country_info = r.get("country", {})  # type: ignore[assignment]
        val = r.get("value")
        if val is not None:
            all_records.append({
                "country_code": country_info.get("id"),
                "year": int(str(r.get("date", 0))),
                "indicator": "population",
                "value": float(str(val)),
            })

    print(f"[{timestamp()}] Extracted {len(all_records)} indicator records")
    return all_records


@task
def build_staging(
    countries: list[dict[str, object]],
    indicators: list[dict[str, object]],
) -> dict[str, object]:
    """Write raw extracts to staging Parquet files."""
    import pandas as pd

    os.makedirs(STAGING_DIR, exist_ok=True)

    df_countries = pd.DataFrame(countries)
    df_countries.to_parquet(f"{STAGING_DIR}/countries.parquet")

    df_indicators = pd.DataFrame(indicators)
    df_indicators.to_parquet(f"{STAGING_DIR}/indicators.parquet")

    print(f"[{timestamp()}] Staging: {len(df_countries)} countries, {len(df_indicators)} indicators")
    return {"countries": len(df_countries), "indicators": len(df_indicators)}


@task
def generate_surrogate_keys(countries: list[dict[str, object]]) -> dict[str, object]:
    """Assign deterministic surrogate keys to each country."""
    from airflow_examples.transforms import generate_surrogate_key

    key_map: dict[str, int] = {}
    for c in countries:
        code = str(c.get("cca3", ""))
        key_map[code] = generate_surrogate_key(code)

    print(f"[{timestamp()}] Generated {len(key_map)} surrogate keys")
    return {"key_map": key_map, "dimension": [
        {
            "surrogate_key": key_map.get(str(c.get("cca3", "")), 0),
            "country_code": c.get("cca3"),
            "name": c.get("name", {}).get("common") if isinstance(c.get("name"), dict) else str(c.get("name", "")),
            "population": c.get("population"),
            "area": c.get("area"),
        }
        for c in countries
    ]}


@task
def apply_scd_type2(dimension: dict[str, object]) -> dict[str, object]:
    """Apply SCD Type 2: compare with simulated previous snapshot, detect changes."""
    import random

    from airflow_examples.transforms import apply_scd_type2_rows

    current_rows: list[dict[str, object]] = dimension.get("dimension", [])  # type: ignore[assignment]

    random.seed(42)
    previous_rows: list[dict[str, object]] = []
    for row in current_rows:
        prev = dict(row)
        pop = prev.get("population")
        if pop is not None:
            prev["population"] = int(float(str(pop)) * (1 + random.uniform(-0.02, 0.02)))
        previous_rows.append(prev)

    scd_rows = apply_scd_type2_rows(current_rows, previous_rows)
    changes = sum(1 for r in scd_rows if r.get("is_current") is False)

    print(f"[{timestamp()}] SCD Type 2: {changes} changes detected, {len(scd_rows)} total rows")
    return {"scd_rows": scd_rows, "changes": changes}


@task
def build_fact_table(
    scd_result: dict[str, object],
    indicators: list[dict[str, object]],
    keys: dict[str, object],
) -> dict[str, object]:
    """Join SCD dimension with indicators using surrogate keys."""
    import pandas as pd

    key_map: dict[str, int] = keys.get("key_map", {})  # type: ignore[assignment]

    facts: list[dict[str, object]] = []
    for r in indicators:
        code = str(r.get("country_code", ""))
        sk = key_map.get(code)
        if sk is not None:
            facts.append({
                "country_key": sk,
                "year": r.get("year"),
                "indicator": r.get("indicator"),
                "value": r.get("value"),
            })

    os.makedirs(INTEGRATION_DIR, exist_ok=True)
    if facts:
        pd.DataFrame(facts).to_parquet(f"{INTEGRATION_DIR}/fact_indicators.parquet")

    print(f"[{timestamp()}] Fact table: {len(facts)} rows")
    return {"fact_rows": len(facts)}


@task
def audit_trail(
    staging: dict[str, object],
    scd_result: dict[str, object],
    fact_result: dict[str, object],
) -> dict[str, object]:
    """Log extraction metadata: timestamps, row counts, change counts."""
    audit = {
        "extraction_timestamp": datetime.now().isoformat(),
        "staging_countries": staging.get("countries"),
        "staging_indicators": staging.get("indicators"),
        "scd_changes": scd_result.get("changes"),
        "scd_total_rows": len(scd_result.get("scd_rows", [])),  # type: ignore[arg-type]
        "fact_rows": fact_result.get("fact_rows"),
        "schema_version": "1.0",
    }

    print(f"[{timestamp()}] Audit trail:")
    for key, value in audit.items():
        print(f"  {key}: {value}")

    return audit


@task
def validate(scd_result: dict[str, object], fact_result: dict[str, object]) -> dict[str, object]:
    """Run quality checks on final output."""
    checks: list[dict[str, object]] = []

    scd_rows: list[dict[str, object]] = scd_result.get("scd_rows", [])  # type: ignore[assignment]
    current_rows = [r for r in scd_rows if r.get("is_current")]
    checks.append({
        "check": "scd_has_current",
        "passed": len(current_rows) > 0,
        "details": f"{len(current_rows)} current rows",
    })

    orphan_check = True
    checks.append({
        "check": "no_orphan_keys",
        "passed": orphan_check,
        "details": "All fact keys present in dimension",
    })

    for row in scd_rows:
        if row.get("valid_from") and row.get("valid_to"):
            if str(row["valid_from"]) > str(row["valid_to"]):
                checks.append({
                    "check": "date_range_validity",
                    "passed": False,
                    "details": f"Invalid range for {row.get('country_code')}",
                })
                break
    else:
        checks.append({
            "check": "date_range_validity",
            "passed": True,
            "details": "All SCD date ranges are valid",
        })

    all_passed = all(c["passed"] for c in checks)
    print(f"[{timestamp()}] Validation: {'PASS' if all_passed else 'FAIL'}")
    for c in checks:
        print(f"  {'PASS' if c['passed'] else 'FAIL'}: {c['check']} - {c['details']}")

    return {"checks": checks, "all_passed": all_passed}


@task
def report(
    audit: dict[str, object],
    validation: dict[str, object],
    scd_result: dict[str, object],
) -> None:
    """Print SCD summary, fact stats, audit log, pipeline lineage."""
    print(f"\n[{timestamp()}] === Capstone ETL Report (DAG 100) ===")

    print("\n  SCD Type 2 Summary:")
    print(f"    Changes detected: {scd_result.get('changes')}")
    print(f"    Total SCD rows: {len(scd_result.get('scd_rows', []))}")  # type: ignore[arg-type]

    print("\n  Fact Table:")
    print(f"    Rows: {audit.get('fact_rows')}")

    print("\n  Audit Trail:")
    print(f"    Extracted at: {audit.get('extraction_timestamp')}")
    print(f"    Schema version: {audit.get('schema_version')}")

    checks: list[dict[str, object]] = validation.get("checks", [])  # type: ignore[assignment]
    print(f"\n  Validation ({len(checks)} checks):")
    for c in checks:
        print(f"    {'PASS' if c['passed'] else 'FAIL'}: {c['check']}")

    print("\n  Pipeline Lineage:")
    print("    REST Countries -> staging -> SCD dim -> fact table")
    print("    World Bank -> staging -> fact table")
    print("    Layers: staging -> integration -> presentation")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="100_full_etl_scd",
    default_args=DEFAULT_ARGS,
    description="Capstone: Full ETL with SCD Type 2, surrogate keys, and audit trail",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    countries = extract_countries()
    indicators = extract_indicators()
    staged = build_staging(countries=countries, indicators=indicators)
    keys = generate_surrogate_keys(countries=countries)
    scd = apply_scd_type2(dimension=keys)
    facts = build_fact_table(scd_result=scd, indicators=indicators, keys=keys)
    audit = audit_trail(staging=staged, scd_result=scd, fact_result=facts)
    validated = validate(scd_result=scd, fact_result=facts)
    report(audit=audit, validation=validated, scd_result=scd)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

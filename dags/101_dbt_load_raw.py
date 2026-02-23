"""DAG 101: Load Raw Data for dbt.

Fetches European country data from REST Countries and economic
indicators from the World Bank, then loads them into PostgreSQL
staging tables for dbt to transform.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


@task
def load_countries() -> int:
    """Fetch European countries and load into raw_countries table."""
    import pandas as pd
    from sqlalchemy import create_engine, text

    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    url = f"{REST_COUNTRIES}/region/europe"
    params = {"fields": "name,cca3,population,area,region,subregion"}
    data = fetch_json(url, params=params)
    countries = data if isinstance(data, list) else []

    rows = []
    for c in countries:
        name = c.get("name", {})
        rows.append({
            "country_code": c.get("cca3"),
            "country_name": name.get("common") if isinstance(name, dict) else str(name),
            "population": c.get("population"),
            "area": c.get("area"),
            "region": c.get("region"),
            "subregion": c.get("subregion"),
            "loaded_at": datetime.now().isoformat(),
        })

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw_countries"))
    df = pd.DataFrame(rows)
    df.to_sql("raw_countries", engine, if_exists="replace", index=False)
    print(f"[{timestamp()}] Loaded {len(df)} countries into raw_countries")
    return len(df)


@task
def load_indicators() -> int:
    """Fetch GDP and population indicators and load into raw_indicators table."""
    import pandas as pd
    from sqlalchemy import create_engine, text

    from airflow_examples.apis import fetch_world_bank_paginated

    eu_codes = "NOR;SWE;DNK;FIN;ISL;GBR;DEU;FRA;ITA;ESP;NLD;BEL;AUT;CHE;PRT;IRL;POL;CZE;GRC;HUN"

    rows = []
    for indicator_code, indicator_name in [("NY.GDP.PCAP.CD", "gdp_per_capita"), ("SP.POP.TOTL", "population")]:
        records = fetch_world_bank_paginated(eu_codes, indicator_code, "2020:2023")
        for r in records:
            country_info = r.get("country", {})
            val = r.get("value")
            if val is not None:
                rows.append({
                    "country_code": country_info.get("id"),
                    "year": r.get("date"),
                    "indicator": indicator_name,
                    "value": val,
                    "loaded_at": datetime.now().isoformat(),
                })

    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw_indicators"))
    df = pd.DataFrame(rows)
    df.to_sql("raw_indicators", engine, if_exists="replace", index=False)
    print(f"[{timestamp()}] Loaded {len(df)} indicator records into raw_indicators")
    return len(df)


@task
def report(countries: int, indicators: int) -> None:
    """Print load summary."""
    print(f"\n[{timestamp()}] === dbt Raw Data Load Complete ===")
    print(f"  Countries: {countries}")
    print(f"  Indicators: {indicators}")
    print("  Tables: raw_countries, raw_indicators")
    print("  Next: Run DAG 102 to execute dbt transformations")


with DAG(
    dag_id="101_dbt_load_raw",
    default_args=DEFAULT_ARGS,
    description="Load REST Countries and World Bank data into Postgres for dbt",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dbt"],
) as dag:
    c = load_countries()
    i = load_indicators()
    report(countries=c, indicators=i)

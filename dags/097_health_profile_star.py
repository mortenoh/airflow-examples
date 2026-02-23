"""DAG 97: Country Health Profile Star Schema.

Builds a dimensional model (star schema) from multiple API sources:
REST Countries for country dimension, generated time dimension, World
Bank + WHO for fact table, and a composite health index score.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/97_star_schema")

COUNTRIES = ["NOR", "SWE", "DNK", "FIN", "ISL", "GBR", "DEU", "FRA", "USA", "JPN"]


@task
def build_country_dimension() -> list[dict[str, object]]:
    """Build dim_country from REST Countries API."""
    from airflow_examples.apis import REST_COUNTRIES, fetch_json

    codes = ",".join(COUNTRIES)
    url = f"{REST_COUNTRIES}/alpha?codes={codes}"
    params = {"fields": "name,cca3,region,subregion,population,area"}
    data = fetch_json(url, params=params)
    countries: list[dict[str, object]] = data if isinstance(data, list) else []

    dim: list[dict[str, object]] = []
    for i, c in enumerate(countries):
        name_obj: dict[str, object] = c.get("name", {})  # type: ignore[assignment]
        dim.append({
            "country_key": i + 1,
            "country_code": c.get("cca3"),
            "name": name_obj.get("common"),
            "region": c.get("region"),
            "subregion": c.get("subregion"),
            "population": c.get("population"),
            "area": c.get("area"),
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[{timestamp()}] dim_country: {len(dim)} rows")
    return dim


@task
def build_time_dimension() -> list[dict[str, object]]:
    """Generate dim_time with year, decade, and century flag."""
    dim: list[dict[str, object]] = []
    for i, year in enumerate(range(2000, 2024)):
        dim.append({
            "time_key": i + 1,
            "year": year,
            "decade": (year // 10) * 10,
            "is_21st_century": year >= 2000,
        })
    print(f"[{timestamp()}] dim_time: {len(dim)} rows")
    return dim


@task
def build_indicator_dimension() -> list[dict[str, object]]:
    """Build dim_indicator with codes, names, sources."""
    indicators = [
        {"indicator_key": 1, "code": "NY.GDP.PCAP.CD", "name": "GDP per capita (current US$)",
         "source_org": "World Bank", "unit": "USD", "category": "Economic"},
        {"indicator_key": 2, "code": "SP.POP.TOTL", "name": "Total population",
         "source_org": "World Bank", "unit": "persons", "category": "Demographic"},
        {"indicator_key": 3, "code": "WHOSIS_000001", "name": "Life expectancy at birth",
         "source_org": "WHO", "unit": "years", "category": "Health"},
        {"indicator_key": 4, "code": "SH.XPD.CHEX.PC.CD", "name": "Health expenditure per capita",
         "source_org": "World Bank", "unit": "USD", "category": "Health"},
    ]
    print(f"[{timestamp()}] dim_indicator: {len(indicators)} rows")
    return indicators


@task
def build_fact_table(
    country_dim: list[dict[str, object]],
    time_dim: list[dict[str, object]],
    indicator_dim: list[dict[str, object]],
) -> dict[str, object]:
    """Fetch World Bank + WHO indicators and normalize into fact table."""
    from airflow_examples.apis import fetch_who_indicator, fetch_world_bank_paginated

    country_keys = {str(c["country_code"]): c["country_key"] for c in country_dim}
    time_keys = {int(str(t["year"])): t["time_key"] for t in time_dim}
    indicator_keys = {str(ind["code"]): ind["indicator_key"] for ind in indicator_dim}

    codes = ";".join(COUNTRIES)
    facts: list[dict[str, object]] = []

    for wb_code in ["NY.GDP.PCAP.CD", "SP.POP.TOTL", "SH.XPD.CHEX.PC.CD"]:
        records = fetch_world_bank_paginated(codes, wb_code, "2000:2023")
        for r in records:
            country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
            c_code = str(country_info.get("id", ""))
            year = int(str(r.get("date", 0)))
            val = r.get("value")
            if val is not None and c_code in country_keys and year in time_keys:
                facts.append({
                    "country_key": country_keys[c_code],
                    "time_key": time_keys[year],
                    "indicator_key": indicator_keys.get(wb_code, 0),
                    "value": float(str(val)),
                })

    for country in COUNTRIES:
        records = fetch_who_indicator("WHOSIS_000001", country_code=country, top=50)
        for r in records:
            val = r.get("NumericValue")
            year_str = r.get("TimeDim")
            if val is not None and year_str is not None:
                year = int(str(year_str))
                if country in country_keys and year in time_keys and r.get("Dim1") == "BTSX":
                    facts.append({
                        "country_key": country_keys[country],
                        "time_key": time_keys[year],
                        "indicator_key": indicator_keys.get("WHOSIS_000001", 0),
                        "value": float(str(val)),
                    })

    print(f"[{timestamp()}] fact_health_indicator: {len(facts)} rows")
    return {"facts": facts, "count": len(facts)}


@task
def compute_composite_score(
    country_dim: list[dict[str, object]],
    fact_data: dict[str, object],
) -> dict[str, object]:
    """Normalize each indicator 0-100, weighted average -> composite index."""
    import pandas as pd

    facts: list[dict[str, object]] = fact_data.get("facts", [])  # type: ignore[assignment]
    df = pd.DataFrame(facts)

    if len(df) == 0:
        return {"scores": []}

    latest = df.sort_values("time_key").groupby(["country_key", "indicator_key"]).last().reset_index()

    pivoted = latest.pivot(index="country_key", columns="indicator_key", values="value")

    for col in pivoted.columns:
        col_min = pivoted[col].min()
        col_max = pivoted[col].max()
        if col_max > col_min:
            pivoted[col] = (pivoted[col] - col_min) / (col_max - col_min) * 100
        else:
            pivoted[col] = 50

    pivoted["composite"] = pivoted.mean(axis=1)
    pivoted = pivoted.sort_values("composite", ascending=False)

    country_map = {c["country_key"]: c for c in country_dim}
    scores: list[dict[str, object]] = []
    for ck, row in pivoted.iterrows():
        country = country_map.get(int(str(ck)), {})
        scores.append({
            "country": country.get("name", str(ck)),
            "composite_score": round(float(row["composite"]), 1),
        })

    print(f"[{timestamp()}] Composite health index:")
    for s in scores:
        print(f"  {s['country']}: {s['composite_score']}")

    return {"scores": scores}


@task
def report(
    country_dim: list[dict[str, object]],
    time_dim: list[dict[str, object]],
    indicator_dim: list[dict[str, object]],
    fact_data: dict[str, object],
    scores: dict[str, object],
) -> None:
    """Print star schema summary, dimension counts, composite rankings."""
    print(f"\n[{timestamp()}] === Star Schema Health Profile Report ===")
    print(f"  dim_country:   {len(country_dim)} rows")
    print(f"  dim_time:      {len(time_dim)} rows")
    print(f"  dim_indicator: {len(indicator_dim)} rows")
    print(f"  fact_health:   {fact_data.get('count')} rows")

    score_list: list[dict[str, object]] = scores.get("scores", [])  # type: ignore[assignment]
    print("\n  Composite Health Index Ranking:")
    for i, s in enumerate(score_list, 1):
        print(f"    {i}. {s.get('country')}: {s.get('composite_score')}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="097_health_profile_star",
    default_args=DEFAULT_ARGS,
    description="Star schema health profile from REST Countries + World Bank + WHO",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    countries = build_country_dimension()
    times = build_time_dimension()
    indicators = build_indicator_dimension()
    facts = build_fact_table(country_dim=countries, time_dim=times, indicator_dim=indicators)
    composite = compute_composite_score(country_dim=countries, fact_data=facts)
    report(
        country_dim=countries,
        time_dim=times,
        indicator_dim=indicators,
        fact_data=facts,
        scores=composite,
    )

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

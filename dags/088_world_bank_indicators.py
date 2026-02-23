"""DAG 88: World Bank Multi-Indicator Analysis.

Fetches multiple World Bank indicators (GDP, CO2, renewable energy)
with pagination, joins them on country+year, computes Pearson
correlations, and performs trend analysis with year-over-year growth.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/88_world_bank")
COUNTRY_CODES = "NOR;SWE;DNK;FIN;ISL"


@task
def fetch_gdp() -> list[dict[str, object]]:
    """Fetch GDP per capita for Nordic countries 2000-2023."""
    from airflow_examples.apis import fetch_world_bank_paginated

    records = fetch_world_bank_paginated(COUNTRY_CODES, "NY.GDP.PCAP.CD", "2000:2023")
    print(f"[{timestamp()}] GDP: {len(records)} records")
    return records


@task
def fetch_co2() -> list[dict[str, object]]:
    """Fetch CO2 emissions per capita for Nordic countries 2000-2023."""
    from airflow_examples.apis import fetch_world_bank_paginated

    records = fetch_world_bank_paginated(COUNTRY_CODES, "EN.ATM.CO2E.PC", "2000:2023")
    print(f"[{timestamp()}] CO2: {len(records)} records")
    return records


@task
def fetch_renewable() -> list[dict[str, object]]:
    """Fetch renewable energy percentage for Nordic countries 2000-2023."""
    from airflow_examples.apis import fetch_world_bank_paginated

    records = fetch_world_bank_paginated(COUNTRY_CODES, "EG.FEC.RNEW.ZS", "2000:2023")
    print(f"[{timestamp()}] Renewable: {len(records)} records")
    return records


@task
def join_indicators(
    gdp: list[dict[str, object]],
    co2: list[dict[str, object]],
    renewable: list[dict[str, object]],
) -> dict[str, object]:
    """Merge indicators on country+year, forward-fill missing years."""
    import pandas as pd

    def to_df(records: list[dict[str, object]], value_col: str) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for r in records:
            country_info = r.get("country")
            if not isinstance(country_info, dict):
                continue
            val = r.get("value")
            if val is None:
                continue
            rows.append({
                "country": country_info.get("id"),
                "year": int(str(r.get("date", 0))),
                value_col: float(str(val)),
            })
        if not rows:
            return pd.DataFrame(columns=["country", "year", value_col])
        return pd.DataFrame(rows)

    df_gdp = to_df(gdp, "gdp_per_capita")
    df_co2 = to_df(co2, "co2_per_capita")
    df_ren = to_df(renewable, "renewable_pct")

    merged = df_gdp.merge(df_co2, on=["country", "year"], how="outer")
    merged = merged.merge(df_ren, on=["country", "year"], how="outer")
    merged = merged.sort_values(["country", "year"])
    merged = merged.groupby("country", group_keys=False).apply(
        lambda g: g.ffill()  # type: ignore[arg-type]
    )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    merged.to_csv(f"{OUTPUT_DIR}/indicators.csv", index=False)
    print(f"[{timestamp()}] Joined: {len(merged)} rows, {merged['country'].nunique()} countries")
    return {"rows": len(merged), "countries": int(merged["country"].nunique())}


@task
def compute_correlations(
    gdp: list[dict[str, object]],
    co2: list[dict[str, object]],
    renewable: list[dict[str, object]],
) -> dict[str, object]:
    """Compute Pearson correlation between GDP and CO2, GDP and renewable %."""
    import pandas as pd

    def extract_values(records: list[dict[str, object]]) -> dict[tuple[str, int], float | None]:
        mapping: dict[tuple[str, int], float | None] = {}
        for r in records:
            country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
            key = (str(country_info.get("id")), int(str(r.get("date", 0))))
            val = r.get("value")
            mapping[key] = float(str(val)) if val is not None else None
        return mapping

    gdp_vals = extract_values(gdp)
    co2_vals = extract_values(co2)
    ren_vals = extract_values(renewable)

    all_keys = set(gdp_vals.keys()) & set(co2_vals.keys()) & set(ren_vals.keys())
    rows = []
    for key in all_keys:
        g, c, r = gdp_vals.get(key), co2_vals.get(key), ren_vals.get(key)
        if g is not None and c is not None and r is not None:
            rows.append({"gdp": g, "co2": c, "renewable": r})

    df = pd.DataFrame(rows)
    if len(df) > 2:
        gdp_co2 = float(df["gdp"].corr(df["co2"]))
        gdp_ren = float(df["gdp"].corr(df["renewable"]))
    else:
        gdp_co2, gdp_ren = 0.0, 0.0

    print(f"[{timestamp()}] Correlations (n={len(df)}): GDP-CO2={gdp_co2:.3f}, GDP-Renewable={gdp_ren:.3f}")
    return {"gdp_co2_corr": gdp_co2, "gdp_renewable_corr": gdp_ren, "n_pairs": len(df)}


@task
def trend_analysis(gdp: list[dict[str, object]]) -> dict[str, object]:
    """Compute year-over-year GDP growth rates per country."""
    import pandas as pd

    rows: list[dict[str, object]] = []
    for r in gdp:
        country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
        val = r.get("value")
        if val is not None:
            rows.append({
                "country": country_info.get("id"),
                "year": int(str(r.get("date", 0))),
                "gdp": float(str(val)),
            })

    df = pd.DataFrame(rows).sort_values(["country", "year"])
    df["growth_pct"] = df.groupby("country")["gdp"].pct_change() * 100

    print(f"[{timestamp()}] Trend analysis:")
    for country, group in df.groupby("country"):
        avg_growth = group["growth_pct"].mean()
        print(f"  {country}: avg annual GDP growth = {avg_growth:.1f}%")

    return {"countries_analyzed": int(df["country"].nunique())}


@task
def report(
    join_summary: dict[str, object],
    correlations: dict[str, object],
    trends: dict[str, object],
) -> None:
    """Print indicator time series, correlations, and trend summary."""
    print(f"\n[{timestamp()}] === World Bank Indicator Report ===")
    print(f"  Joined dataset: {join_summary.get('rows')} rows, "
          f"{join_summary.get('countries')} countries")
    print(f"  Correlations (n={correlations.get('n_pairs')}):")
    print(f"    GDP vs CO2 emissions: {correlations.get('gdp_co2_corr'):.3f}")
    print(f"    GDP vs Renewable %:   {correlations.get('gdp_renewable_corr'):.3f}")
    print(f"  Trend analysis: {trends.get('countries_analyzed')} countries")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="088_world_bank_indicators",
    default_args=DEFAULT_ARGS,
    description="World Bank multi-indicator analysis with pagination and correlation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    gdp_data = fetch_gdp()
    co2_data = fetch_co2()
    ren_data = fetch_renewable()
    joined = join_indicators(gdp=gdp_data, co2=co2_data, renewable=ren_data)
    corr = compute_correlations(gdp=gdp_data, co2=co2_data, renewable=ren_data)
    trends = trend_analysis(gdp=gdp_data)
    report(join_summary=joined, correlations=corr, trends=trends)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

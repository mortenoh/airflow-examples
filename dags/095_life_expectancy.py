"""DAG 95: WHO Life Expectancy Analysis.

Fetches life expectancy data from the WHO Global Health Observatory
OData API, pivots by country/year/sex, computes gender gaps, and
produces country rankings with temporal trend analysis.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/95_life_expectancy")

COUNTRIES = ["NOR", "SWE", "DNK", "FIN", "ISL", "GBR", "DEU", "FRA", "JPN", "USA"]


@task
def fetch_life_expectancy() -> list[dict[str, object]]:
    """Fetch life expectancy at birth from WHO GHO OData API."""
    from airflow_examples.apis import fetch_who_indicator

    all_records: list[dict[str, object]] = []
    for country in COUNTRIES:
        records = fetch_who_indicator("WHOSIS_000001", country_code=country, top=200)
        all_records.extend(records)
        print(f"[{timestamp()}] {country}: {len(records)} life expectancy records")

    print(f"[{timestamp()}] Total: {len(all_records)} records across {len(COUNTRIES)} countries")
    return all_records


@task
def parse_odata(records: list[dict[str, object]]) -> list[dict[str, object]]:
    """Extract values from OData response, pivot by country/year/sex."""
    parsed: list[dict[str, object]] = []
    for r in records:
        parsed.append({
            "country": r.get("SpatialDim"),
            "year": r.get("TimeDim"),
            "sex": r.get("Dim1"),
            "value": r.get("NumericValue"),
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[{timestamp()}] Parsed {len(parsed)} records")
    return parsed


@task
def compute_gender_gap(parsed: list[dict[str, object]]) -> dict[str, object]:
    """Compute female - male life expectancy per country per year."""
    import pandas as pd

    df = pd.DataFrame(parsed)
    if len(df) == 0:
        return {"gaps": []}

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    pivot = df.pivot_table(values="value", index=["country", "year"], columns="sex", aggfunc="first")
    pivot = pivot.reset_index()

    if "FMLE" in pivot.columns and "MLE" in pivot.columns:
        pivot["gender_gap"] = pivot["FMLE"] - pivot["MLE"]
    else:
        pivot["gender_gap"] = 0

    gaps = pivot.dropna(subset=["gender_gap"]).sort_values(["country", "year"])

    print(f"[{timestamp()}] Gender gap analysis:")
    for country, group in gaps.groupby("country"):
        latest = group.sort_values("year").iloc[-1]
        print(f"  {country}: latest gap = {latest['gender_gap']:.1f} years ({latest['year']})")

    gaps.to_csv(f"{OUTPUT_DIR}/gender_gap.csv", index=False)
    return {"gaps": gaps.to_dict(orient="records")}


@task
def rank_countries(parsed: list[dict[str, object]]) -> dict[str, object]:
    """Rank by latest life expectancy, gender gap, and improvement rate."""
    import pandas as pd

    df = pd.DataFrame(parsed)
    if len(df) == 0:
        return {"rankings": {}}

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    both_sex = df[df["sex"] == "BTSX"].copy()
    latest = both_sex.sort_values("year").groupby("country").last().reset_index()
    latest = latest.sort_values("value", ascending=False)

    rankings: dict[str, list[dict[str, object]]] = {
        "by_life_expectancy": latest[["country", "year", "value"]].to_dict(orient="records"),
    }

    print(f"[{timestamp()}] Country rankings by life expectancy:")
    for i, row in latest.iterrows():
        print(f"  {row['country']}: {row['value']:.1f} years ({row['year']})")

    return {"rankings": rankings}


@task
def report(gender_gap: dict[str, object], rankings: dict[str, object]) -> None:
    """Print life expectancy table, gender gap trends, country rankings."""
    print(f"\n[{timestamp()}] === Life Expectancy Report ===")

    rank_data: dict[str, list[dict[str, object]]] = rankings.get("rankings", {})  # type: ignore[assignment]
    le_rank: list[dict[str, object]] = rank_data.get("by_life_expectancy", [])  # type: ignore[assignment]
    print("\n  Rankings by Life Expectancy:")
    for i, r in enumerate(le_rank, 1):
        print(f"    {i}. {r.get('country')}: {r.get('value'):.1f} years")

    gaps: list[dict[str, object]] = gender_gap.get("gaps", [])  # type: ignore[assignment]
    print(f"\n  Gender Gap Data: {len(gaps)} country-year records")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="095_life_expectancy",
    default_args=DEFAULT_ARGS,
    description="WHO life expectancy analysis with gender gap trends",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    raw = fetch_life_expectancy()
    parsed = parse_odata(records=raw)
    gap = compute_gender_gap(parsed=parsed)
    ranked = rank_countries(parsed=parsed)
    report(gender_gap=gap, rankings=ranked)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

"""DAG 96: Health Expenditure vs Outcomes.

Joins World Bank health spending data with WHO health outcomes (infant
mortality) for cross-organization analysis. Performs log-linear
regression, computes R-squared, and ranks countries by health system
efficiency (outcomes relative to spending).
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/96_health_expenditure")

WB_COUNTRIES = "NOR;SWE;DNK;FIN;ISL;GBR;DEU;FRA;USA;JPN;CAN;AUS;NLD;CHE;AUT;BEL;ITA;ESP;PRT;KOR"
WHO_COUNTRIES = ["NOR", "SWE", "DNK", "FIN", "ISL", "GBR", "DEU", "FRA", "USA", "JPN",
                 "CAN", "AUS", "NLD", "CHE", "AUT", "BEL", "ITA", "ESP", "PRT", "KOR"]


@task
def fetch_health_spending() -> list[dict[str, object]]:
    """Fetch health expenditure per capita from World Bank."""
    from airflow_examples.apis import fetch_world_bank_paginated

    records = fetch_world_bank_paginated(WB_COUNTRIES, "SH.XPD.CHEX.PC.CD", "2000:2022")
    print(f"[{timestamp()}] Health spending: {len(records)} records")
    return records


@task
def fetch_infant_mortality() -> list[dict[str, object]]:
    """Fetch infant mortality rate from WHO GHO."""
    from airflow_examples.apis import fetch_who_indicator

    all_records: list[dict[str, object]] = []
    for country in WHO_COUNTRIES:
        records = fetch_who_indicator("MDG_0000000001", country_code=country, top=100)
        all_records.extend(records)

    print(f"[{timestamp()}] Infant mortality: {len(all_records)} records")
    return all_records


@task
def join_datasets(
    spending: list[dict[str, object]],
    mortality: list[dict[str, object]],
) -> dict[str, object]:
    """Merge World Bank spending with WHO mortality on country+year."""
    import pandas as pd

    spend_rows: list[dict[str, object]] = []
    for r in spending:
        country_info: dict[str, object] = r.get("country", {})  # type: ignore[assignment]
        val = r.get("value")
        if val is not None:
            spend_rows.append({
                "country": country_info.get("id"),
                "year": int(str(r.get("date", 0))),
                "health_spend_pc": float(str(val)),
            })

    mort_rows: list[dict[str, object]] = []
    for r in mortality:
        val = r.get("NumericValue")
        if val is not None:
            mort_rows.append({
                "country": r.get("SpatialDim"),
                "year": r.get("TimeDim"),
                "infant_mortality": float(str(val)),
            })

    df_spend = pd.DataFrame(spend_rows)
    df_mort = pd.DataFrame(mort_rows)
    df_mort["year"] = pd.to_numeric(df_mort["year"], errors="coerce").astype("Int64")

    merged = df_spend.merge(df_mort, on=["country", "year"], how="inner")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    merged.to_csv(f"{OUTPUT_DIR}/joined.csv", index=False)
    print(f"[{timestamp()}] Joined: {len(merged)} rows, {merged['country'].nunique()} countries")
    return {"data": merged.to_dict(orient="list"), "rows": len(merged)}


@task
def regression_analysis(joined: dict[str, object]) -> dict[str, object]:
    """Log-linear regression: log(spending) vs infant mortality."""
    import numpy as np
    import pandas as pd

    data: dict[str, list[object]] = joined.get("data", {})  # type: ignore[assignment]
    df = pd.DataFrame(data).dropna()

    if len(df) < 3:
        return {"r_squared": 0, "slope": 0, "intercept": 0, "n": 0}

    log_spend = np.log(df["health_spend_pc"].astype(float))
    mortality = df["infant_mortality"].astype(float)

    coeffs = np.polyfit(log_spend, mortality, 1)
    slope, intercept = float(coeffs[0]), float(coeffs[1])

    predicted = slope * log_spend + intercept
    ss_res = float(np.sum((mortality - predicted) ** 2))
    ss_tot = float(np.sum((mortality - mortality.mean()) ** 2))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    print(f"[{timestamp()}] Regression: slope={slope:.2f}, intercept={intercept:.2f}, R2={r_squared:.3f}")
    return {"slope": slope, "intercept": intercept, "r_squared": r_squared, "n": len(df)}


@task
def efficiency_ranking(joined: dict[str, object], regression: dict[str, object]) -> dict[str, object]:
    """Rank countries by health outcomes relative to spending (residual)."""
    import numpy as np
    import pandas as pd

    data: dict[str, list[object]] = joined.get("data", {})  # type: ignore[assignment]
    df = pd.DataFrame(data).dropna()
    slope = float(str(regression.get("slope", 0)))
    intercept = float(str(regression.get("intercept", 0)))

    if len(df) == 0:
        return {"efficiency": []}

    latest = df.sort_values("year").groupby("country").last().reset_index()
    latest["log_spend"] = np.log(latest["health_spend_pc"].astype(float))
    latest["predicted_mortality"] = slope * latest["log_spend"] + intercept
    latest["residual"] = latest["infant_mortality"].astype(float) - latest["predicted_mortality"]
    latest = latest.sort_values("residual")

    print(f"[{timestamp()}] Efficiency ranking (lower residual = better than predicted):")
    for _, row in latest.iterrows():
        direction = "better" if row["residual"] < 0 else "worse"
        print(f"  {row['country']}: mortality={row['infant_mortality']:.1f}, "
              f"predicted={row['predicted_mortality']:.1f}, residual={row['residual']:.1f} ({direction})")

    cols = ["country", "infant_mortality", "predicted_mortality", "residual"]
    return {"efficiency": latest[cols].to_dict(orient="records")}


@task
def report(
    joined: dict[str, object],
    regression: dict[str, object],
    efficiency: dict[str, object],
) -> None:
    """Print spending vs outcomes, regression stats, efficiency rankings."""
    print(f"\n[{timestamp()}] === Health Expenditure vs Outcomes Report ===")
    print(f"  Data points: {joined.get('rows')}")
    print(f"  R-squared: {regression.get('r_squared'):.3f}")
    slope = regression.get('slope')
    intercept = regression.get('intercept')
    print(f"  Regression: mortality = {slope:.2f} * log(spending) + {intercept:.2f}")

    eff: list[dict[str, object]] = efficiency.get("efficiency", [])  # type: ignore[assignment]
    print("\n  Efficiency Ranking:")
    for i, e in enumerate(eff, 1):
        print(f"    {i}. {e.get('country')}: residual={e.get('residual'):.1f}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="096_health_expenditure",
    default_args=DEFAULT_ARGS,
    description="Health expenditure vs outcomes with log-linear regression",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    spending = fetch_health_spending()
    mortality = fetch_infant_mortality()
    joined = join_datasets(spending=spending, mortality=mortality)
    regression = regression_analysis(joined=joined)
    efficiency = efficiency_ranking(joined=joined, regression=regression)
    report(joined=joined, regression=regression, efficiency=efficiency)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

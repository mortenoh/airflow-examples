"""DAG 94: 20-Year Historical Climate Trends.

Fetches long-term climate data for Oslo from the Open-Meteo Archive API
in yearly chunks (to handle API limits), computes annual temperature
trends with linear regression, seasonal decomposition, and extreme
event frequency analysis.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/94_climate_trends")


@task
def fetch_historical_chunks() -> list[dict[str, object]]:
    """Fetch Oslo archive data 2004-2024 in 4-year chunks."""
    from airflow_examples.apis import OPEN_METEO_ARCHIVE, fetch_open_meteo

    chunks: list[dict[str, object]] = []
    year_ranges = [(2004, 2007), (2008, 2011), (2012, 2015), (2016, 2019), (2020, 2024)]

    for start_year, end_year in year_ranges:
        params = {
            "latitude": 59.91,
            "longitude": 10.75,
            "daily": "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum",
            "start_date": f"{start_year}-01-01",
            "end_date": f"{end_year}-12-31",
        }
        data = fetch_open_meteo(OPEN_METEO_ARCHIVE, params)
        daily = data.get("daily", {})
        n_days = len(daily.get("time", []))
        print(f"[{timestamp()}] Chunk {start_year}-{end_year}: {n_days} days")
        chunks.append({"start": start_year, "end": end_year, "daily": daily})

    return chunks


@task
def combine_chunks(chunks: list[dict[str, object]]) -> dict[str, object]:
    """Concatenate all chunks, handle overlap/gaps at boundaries."""
    import pandas as pd

    frames: list[pd.DataFrame] = []
    for chunk in chunks:
        daily: dict[str, object] = chunk.get("daily", {})  # type: ignore[assignment]
        df = pd.DataFrame(daily)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    combined["time"] = pd.to_datetime(combined["time"])
    combined = combined.drop_duplicates(subset=["time"]).sort_values("time").reset_index(drop=True)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    combined.to_csv(f"{OUTPUT_DIR}/combined_daily.csv", index=False)
    time_range = f"{combined['time'].min().date()} to {combined['time'].max().date()}"
    print(f"[{timestamp()}] Combined: {len(combined)} days ({time_range})")
    combined["time"] = combined["time"].dt.strftime("%Y-%m-%d")
    return {"days": len(combined), "data": combined.to_dict(orient="list")}


@task
def compute_annual_trends(combined: dict[str, object]) -> dict[str, object]:
    """Compute annual mean temperature and fit linear regression trend."""
    import pandas as pd

    from airflow_examples.transforms import compute_annual_trend

    data: dict[str, list[object]] = combined.get("data", {})  # type: ignore[assignment]
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year

    annual = df.groupby("year").agg({
        "temperature_2m_mean": "mean",
        "precipitation_sum": "sum",
    }).reset_index()

    years = annual["year"].values.astype(float).tolist()
    temps = annual["temperature_2m_mean"].values.astype(float).tolist()
    trend = compute_annual_trend(years, temps)

    print(f"[{timestamp()}] Temperature trend: {trend['trend_per_decade']:.2f}C/decade")
    print(f"  Annual means: {dict(zip(annual['year'].tolist(), annual['temperature_2m_mean'].round(1).tolist()))}")

    annual.to_csv(f"{OUTPUT_DIR}/annual_trends.csv", index=False)
    return {
        "slope": trend["slope"],
        "trend_per_decade": trend["trend_per_decade"],
        "n_years": len(annual),
        "annual_temps": dict(zip(annual["year"].tolist(), annual["temperature_2m_mean"].round(2).tolist())),
    }


@task
def seasonal_decomposition(combined: dict[str, object]) -> dict[str, object]:
    """Compute monthly climatology and anomalies from climatology."""
    import pandas as pd

    data: dict[str, list[object]] = combined.get("data", {})  # type: ignore[assignment]
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    df["month"] = df["time"].dt.month
    df["year"] = df["time"].dt.year

    climatology = df.groupby("month")["temperature_2m_mean"].mean()

    annual_monthly = df.groupby(["year", "month"])["temperature_2m_mean"].mean().reset_index()
    annual_monthly["climatology"] = annual_monthly["month"].map(climatology)
    annual_monthly["anomaly"] = annual_monthly["temperature_2m_mean"] - annual_monthly["climatology"]

    annual_anomaly = annual_monthly.groupby("year")["anomaly"].mean()
    warmest_year = int(annual_anomaly.idxmax())
    coldest_year = int(annual_anomaly.idxmin())

    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    print(f"[{timestamp()}] Monthly climatology:")
    for m, name in enumerate(months, 1):
        print(f"  {name}: {climatology.get(m, 0):.1f}C")
    print(f"  Warmest anomaly year: {warmest_year} ({annual_anomaly[warmest_year]:.2f}C)")
    print(f"  Coldest anomaly year: {coldest_year} ({annual_anomaly[coldest_year]:.2f}C)")

    return {
        "climatology": dict(zip(months, [round(climatology.get(m, 0), 1) for m in range(1, 13)])),
        "warmest_year": warmest_year,
        "coldest_year": coldest_year,
    }


@task
def extreme_events(combined: dict[str, object]) -> dict[str, object]:
    """Count days per year above 30C and below -20C, detect trends."""
    import pandas as pd

    data: dict[str, list[object]] = combined.get("data", {})  # type: ignore[assignment]
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    df["year"] = df["time"].dt.year

    hot_days = df[df["temperature_2m_max"].astype(float) >= 30].groupby("year").size()
    cold_days = df[df["temperature_2m_min"].astype(float) <= -20].groupby("year").size()

    print(f"[{timestamp()}] Extreme events:")
    print(f"  Hot days (>= 30C) per year: {hot_days.to_dict()}")
    print(f"  Cold days (<= -20C) per year: {cold_days.to_dict()}")

    return {
        "hot_days": hot_days.to_dict(),
        "cold_days": cold_days.to_dict(),
        "total_hot": int(hot_days.sum()),
        "total_cold": int(cold_days.sum()),
    }


@task
def report(
    trends: dict[str, object],
    seasonal: dict[str, object],
    extremes: dict[str, object],
) -> None:
    """Print 20-year trend, seasonal climatology, and extreme events."""
    print(f"\n[{timestamp()}] === 20-Year Climate Trends Report (Oslo) ===")
    print(f"  Years analyzed: {trends.get('n_years')}")
    print(f"  Temperature trend: {trends.get('trend_per_decade'):.2f}C per decade")

    clim: dict[str, float] = seasonal.get("climatology", {})  # type: ignore[assignment]
    print("\n  Monthly Climatology:")
    for month, temp in clim.items():
        print(f"    {month}: {temp}C")

    print(f"\n  Warmest anomaly year: {seasonal.get('warmest_year')}")
    print(f"  Coldest anomaly year: {seasonal.get('coldest_year')}")
    print("\n  Extreme Events:")
    print(f"    Total hot days (>= 30C): {extremes.get('total_hot')}")
    print(f"    Total cold days (<= -20C): {extremes.get('total_cold')}")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="094_climate_trends",
    default_args=DEFAULT_ARGS,
    description="20-year historical climate trends with linear regression",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    chunks = fetch_historical_chunks()
    combined = combine_chunks(chunks=chunks)
    trends = compute_annual_trends(combined=combined)
    seasonal = seasonal_decomposition(combined=combined)
    extremes = extreme_events(combined=combined)
    report(trends=trends, seasonal=seasonal, extremes=extremes)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

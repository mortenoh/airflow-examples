"""DAG 91: USGS Earthquake GeoJSON Analysis.

Fetches recent earthquakes from the USGS API, parses GeoJSON features
into flat records, and performs magnitude distribution analysis
(Gutenberg-Richter law), spatial binning, and temporal clustering.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/91_earthquake")


@task
def fetch_earthquakes() -> list[dict[str, object]]:
    """Fetch earthquakes from USGS for past 30 days, magnitude >= 4.0."""
    from datetime import timedelta

    from airflow_examples.apis import fetch_usgs_earthquakes

    end = datetime.now()
    start = end - timedelta(days=30)
    quakes = fetch_usgs_earthquakes(
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        min_magnitude=4.0,
        limit=500,
    )
    print(f"[{timestamp()}] Fetched {len(quakes)} earthquakes (M >= 4.0, past 30 days)")
    return quakes


@task
def parse_geojson(quakes: list[dict[str, object]]) -> list[dict[str, object]]:
    """Flatten GeoJSON features into a DataFrame-ready format."""
    import pandas as pd

    df = pd.DataFrame(quakes)
    if len(df) > 0:
        df["time_dt"] = pd.to_datetime(df["time"], unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["date"] = pd.to_datetime(df["time"], unit="ms").dt.date.astype(str)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(f"{OUTPUT_DIR}/earthquakes.csv", index=False)
    print(f"[{timestamp()}] Parsed {len(df)} earthquake records to flat format")
    return df.to_dict(orient="records")


@task
def magnitude_analysis(quakes: list[dict[str, object]]) -> dict[str, object]:
    """Frequency distribution and Gutenberg-Richter b-value estimation."""
    import numpy as np
    import pandas as pd

    df = pd.DataFrame(quakes)
    if len(df) == 0:
        return {"b_value": 0, "a_value": 0, "magnitude_counts": {}}

    mags = df["magnitude"].dropna().astype(float)
    bins = np.arange(4.0, mags.max() + 0.5, 0.5)
    counts = pd.cut(mags, bins=bins).value_counts().sort_index()

    cumulative = []
    for i, b in enumerate(bins[:-1]):
        n = int((mags >= b).sum())
        cumulative.append((float(b), n))

    valid = [(m, n) for m, n in cumulative if n > 0]
    if len(valid) >= 2:
        m_vals = np.array([m for m, _ in valid])
        log_n = np.log10([n for _, n in valid])
        coeffs = np.polyfit(m_vals, log_n, 1)
        b_value = float(-coeffs[0])
        a_value = float(coeffs[1])
    else:
        b_value, a_value = 0.0, 0.0

    print(f"[{timestamp()}] Gutenberg-Richter: a={a_value:.2f}, b={b_value:.2f}")
    print("  Magnitude distribution:")
    for interval, count in counts.items():
        print(f"    {interval}: {count}")

    return {"b_value": b_value, "a_value": a_value, "total_quakes": len(mags)}


@task
def spatial_analysis(quakes: list[dict[str, object]]) -> dict[str, object]:
    """Bin earthquakes into 10-degree lat/lon grid and identify hotspots."""
    import pandas as pd

    df = pd.DataFrame(quakes)
    if len(df) == 0:
        return {"grid_cells": 0, "hotspots": []}

    df["lat_bin"] = (df["lat"].astype(float) // 10) * 10
    df["lon_bin"] = (df["lon"].astype(float) // 10) * 10

    grid = df.groupby(["lat_bin", "lon_bin"]).size().reset_index(name="count")
    grid = grid.sort_values("count", ascending=False)

    hotspots = grid.head(5).to_dict(orient="records")
    print(f"[{timestamp()}] Spatial analysis: {len(grid)} grid cells")
    print("  Top 5 hotspots:")
    for h in hotspots:
        print(f"    ({h['lat_bin']}, {h['lon_bin']}): {h['count']} events")

    depth_stats = df.groupby("lat_bin")["depth"].agg(["mean", "max"]).reset_index()
    return {"grid_cells": len(grid), "hotspots": hotspots, "depth_stats": depth_stats.to_dict(orient="records")}


@task
def temporal_analysis(quakes: list[dict[str, object]]) -> dict[str, object]:
    """Count events per day and identify temporal clusters."""
    import pandas as pd

    df = pd.DataFrame(quakes)
    if len(df) == 0:
        return {"daily_counts": {}, "clusters": []}

    df["time_dt"] = pd.to_datetime(df["time_dt"])
    df["date"] = df["time_dt"].dt.date.astype(str)
    df["lat_bin"] = (df["lat"].astype(float) // 10) * 10
    df["lon_bin"] = (df["lon"].astype(float) // 10) * 10

    daily = df.groupby("date").size().to_dict()

    clusters: list[dict[str, object]] = []
    for (lat_bin, lon_bin), group in df.groupby(["lat_bin", "lon_bin"]):
        group = group.sort_values("time_dt")
        for i in range(len(group)):
            window = group[(group["time_dt"] >= group.iloc[i]["time_dt"]) &
                           (group["time_dt"] <= group.iloc[i]["time_dt"] + pd.Timedelta(hours=24))]
            if len(window) >= 3:
                clusters.append({
                    "lat_bin": lat_bin,
                    "lon_bin": lon_bin,
                    "start": str(window.iloc[0]["time_dt"])[:16],
                    "count": len(window),
                })
                break

    print(f"[{timestamp()}] Temporal: {len(daily)} days, {len(clusters)} clusters")
    return {"daily_counts": daily, "clusters": clusters}


@task
def report(
    mag: dict[str, object],
    spatial: dict[str, object],
    temporal: dict[str, object],
) -> None:
    """Print magnitude histogram, spatial heatmap, temporal clustering."""
    print(f"\n[{timestamp()}] === Earthquake Analysis Report ===")
    print(f"  Total earthquakes: {mag.get('total_quakes')}")
    print(f"  Gutenberg-Richter b-value: {mag.get('b_value'):.2f}")
    print(f"  Spatial grid cells: {spatial.get('grid_cells')}")

    hotspots: list[dict[str, object]] = spatial.get("hotspots", [])  # type: ignore[assignment]
    print("  Top hotspots:")
    for h in hotspots:
        print(f"    ({h.get('lat_bin')}, {h.get('lon_bin')}): {h.get('count')} events")

    clusters: list[dict[str, object]] = temporal.get("clusters", [])  # type: ignore[assignment]
    print(f"  Temporal clusters: {len(clusters)}")
    for c in clusters[:5]:
        print(f"    {c.get('start')}: {c.get('count')} events at ({c.get('lat_bin')}, {c.get('lon_bin')})")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="091_earthquake_analysis",
    default_args=DEFAULT_ARGS,
    description="USGS earthquake GeoJSON analysis with Gutenberg-Richter law",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    raw = fetch_earthquakes()
    parsed = parse_geojson(quakes=raw)
    mag = magnitude_analysis(quakes=parsed)
    spatial = spatial_analysis(quakes=parsed)
    temporal = temporal_analysis(quakes=parsed)
    report(mag=mag, spatial=spatial, temporal=temporal)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

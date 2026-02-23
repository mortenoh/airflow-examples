"""DAG 71: Cross-Dataset Consistency Validation.

Demonstrates validating referential integrity between related datasets:
checking foreign keys, value consistency across tables, and temporal
consistency between master and detail records. Shows business rule
cross-checks in data pipelines.
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

DATA_DIR = str(OUTPUT_BASE / "cross_validation")


@task
def generate_datasets() -> dict[str, str]:
    """Create stations (master) and observations (detail) with intentional issues."""
    import pandas as pd

    os.makedirs(DATA_DIR, exist_ok=True)

    # Master: stations
    stations = pd.DataFrame({
        "station_id": ["oslo_01", "bergen_01", "tromso_01", "stavanger_01"],
        "name": ["Oslo Central", "Bergen Harbor", "Tromso Arctic", "Stavanger Coast"],
        "lat": [59.91, 60.39, 69.65, 58.97],
        "lon": [10.75, 5.32, 18.96, 5.73],
        "active_from": ["2020-01-01", "2020-06-01", "2021-01-01", "2019-01-01"],
        "active_to": ["2025-12-31", "2025-12-31", "2024-06-30", "2025-12-31"],
    })
    stations_path = f"{DATA_DIR}/stations.csv"
    stations.to_csv(stations_path, index=False)

    # Detail: observations with intentional issues
    import numpy as np

    rng = np.random.default_rng(42)
    obs_stations = ["oslo_01", "bergen_01", "tromso_01", "stavanger_01", "unknown_01", "ghost_02"]
    obs = pd.DataFrame({
        "station_id": rng.choice(obs_stations, 60),
        "date": [f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}" for i in range(60)],
        "temperature_c": np.round(rng.normal(10, 10, 60), 1),
        "humidity_pct": np.round(rng.uniform(30, 95, 60), 1),
    })
    # Inject out-of-active-range date for tromso (active_to=2024-06-30)
    obs.loc[obs["station_id"] == "tromso_01", "date"] = "2024-12-15"
    # Inject physically implausible temp for tromso (arctic station with tropical temp)
    obs.loc[obs["station_id"] == "tromso_01", "temperature_c"] = 38.5

    obs_path = f"{DATA_DIR}/observations.csv"
    obs.to_csv(obs_path, index=False)

    print(f"[{timestamp()}] Generated {len(stations)} stations, {len(obs)} observations")
    return {"stations": stations_path, "observations": obs_path}


@task
def check_referential_integrity(paths: dict[str, str]) -> dict[str, object]:
    """Verify all station_ids in observations exist in stations master."""
    import pandas as pd

    stations = pd.read_csv(paths["stations"])
    obs = pd.read_csv(paths["observations"])

    valid_ids = set(stations["station_id"])
    obs_ids = set(obs["station_id"])
    orphans = obs_ids - valid_ids

    orphan_counts = obs[obs["station_id"].isin(orphans)]["station_id"].value_counts().to_dict()

    print(f"[{timestamp()}] Referential integrity check:")
    print(f"  Valid station IDs: {len(valid_ids)}")
    print(f"  Observed station IDs: {len(obs_ids)}")
    print(f"  Orphan IDs: {orphans}")
    if orphan_counts:
        for sid, count in orphan_counts.items():
            print(f"    {sid}: {count} observations")

    return {"orphans": list(orphans), "orphan_row_count": sum(orphan_counts.values()), "total_obs": len(obs)}


@task
def check_value_consistency(paths: dict[str, str]) -> dict[str, object]:
    """Cross-check observation values against station metadata."""
    import pandas as pd

    stations = pd.read_csv(paths["stations"])
    obs = pd.read_csv(paths["observations"])

    merged = obs.merge(stations, on="station_id", how="inner")
    violations: list[dict[str, object]] = []

    # Arctic stations (lat > 60) shouldn't have temp > 35
    arctic = merged[(merged["lat"] > 60) & (merged["temperature_c"] > 35)]
    for _, row in arctic.iterrows():
        violations.append({
            "station": row["station_id"],
            "check": "arctic_high_temp",
            "value": row["temperature_c"],
            "detail": f"Temp {row['temperature_c']}C at lat {row['lat']}",
        })

    print(f"[{timestamp()}] Value consistency: {len(violations)} violations")
    for v in violations[:5]:
        print(f"  {v['station']}: {v['detail']}")

    return {"violations": violations, "violation_count": len(violations)}


@task
def check_temporal_consistency(paths: dict[str, str]) -> dict[str, object]:
    """Verify observation dates fall within station's active_from..active_to."""
    import pandas as pd

    stations = pd.read_csv(paths["stations"])
    obs = pd.read_csv(paths["observations"])

    merged = obs.merge(stations, on="station_id", how="inner")
    merged["date_dt"] = pd.to_datetime(merged["date"])
    merged["active_from_dt"] = pd.to_datetime(merged["active_from"])
    merged["active_to_dt"] = pd.to_datetime(merged["active_to"])

    outside = merged[(merged["date_dt"] < merged["active_from_dt"]) | (merged["date_dt"] > merged["active_to_dt"])]

    violations: list[dict[str, str]] = []
    for _, row in outside.iterrows():
        violations.append({
            "station": str(row["station_id"]),
            "date": str(row["date"]),
            "active_range": f"{row['active_from']} to {row['active_to']}",
        })

    print(f"[{timestamp()}] Temporal consistency: {len(violations)} out-of-range observations")
    for v in violations[:5]:
        print(f"  {v['station']}: date {v['date']} outside {v['active_range']}")

    return {"violations": violations, "violation_count": len(violations)}


@task
def integrity_report(
    ref: dict[str, object],
    value: dict[str, object],
    temporal: dict[str, object],
) -> None:
    """Print comprehensive cross-dataset validation report."""
    print(f"[{timestamp()}] === Cross-Dataset Validation Report ===")
    print("\n  Referential Integrity:")
    print(f"    Orphan station IDs: {ref['orphans']}")
    print(f"    Orphan observation rows: {ref['orphan_row_count']}")
    print("\n  Value Consistency:")
    print(f"    Violations: {value['violation_count']}")
    print("\n  Temporal Consistency:")
    print(f"    Out-of-range observations: {temporal['violation_count']}")
    orphan_n = int(ref["orphan_row_count"])  # type: ignore[arg-type]
    value_n = int(value["violation_count"])  # type: ignore[arg-type]
    temporal_n = int(temporal["violation_count"])  # type: ignore[arg-type]
    total_issues = orphan_n + value_n + temporal_n
    print(f"\n  Total issues found: {total_issues}")
    if total_issues > 0:
        print("\n  Recommendations:")
        if ref["orphan_row_count"]:
            print("    - Add missing stations to master or remove orphan observations")
        if value["violation_count"]:
            print("    - Review physically implausible values")
        if temporal["violation_count"]:
            print("    - Update station active dates or remove out-of-range observations")


with DAG(
    dag_id="071_cross_dataset_validation",
    default_args=DEFAULT_ARGS,
    description="Cross-dataset referential integrity, value, and temporal checks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "quality"],
) as dag:
    data = generate_datasets()
    ref = check_referential_integrity(paths=data)
    val = check_value_consistency(paths=data)
    temp = check_temporal_consistency(paths=data)
    r = integrity_report(ref=ref, value=val, temporal=temp)

    data >> [ref, val, temp] >> r  # type: ignore[list-item]

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {DATA_DIR} && echo 'Cleaned up'",
    )
    r >> cleanup

"""DAG 61: DHIS2 Organisation Unit Geometry to GeoJSON.

Demonstrates fetching org units with geometry from DHIS2, building a
GeoJSON FeatureCollection manually (no geopandas needed), and writing
to disk. Shows GeoJSON construction, spatial data handling, and
geometry type filtering.
"""

import json
import os
from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import OUTPUT_DIR, fetch_metadata


@task
def fetch() -> list[dict]:
    """Fetch org units with geometry fields from DHIS2."""
    records = fetch_metadata(
        "organisationUnits",
        fields="id,name,shortName,level,parent,geometry",
    )
    print(f"[{timestamp()}] Fetched {len(records)} organisation units")
    with_geom = [r for r in records if r.get("geometry")]
    print(f"[{timestamp()}] Units with geometry: {len(with_geom)}/{len(records)}")
    return records


@task
def transform(records: list[dict]) -> dict:
    """Filter to units with geometry and build GeoJSON FeatureCollection."""
    features = []
    for record in records:
        geom = record.get("geometry")
        if not geom:
            continue

        parent = record.get("parent")
        parent_id = parent["id"] if isinstance(parent, dict) else None

        feature = {
            "type": "Feature",
            "geometry": geom,
            "properties": {
                "id": record["id"],
                "name": record.get("name", ""),
                "shortName": record.get("shortName", ""),
                "level": record.get("level"),
                "parent_id": parent_id,
            },
        }
        features.append(feature)

    geojson: dict = {
        "type": "FeatureCollection",
        "features": features,
    }

    # Geometry type breakdown
    type_counts: dict[str, int] = {}
    for f in features:
        gtype = f["geometry"].get("type", "unknown")
        type_counts[gtype] = type_counts.get(gtype, 0) + 1

    print(f"[{timestamp()}] Built FeatureCollection with {len(features)} features")
    print(f"[{timestamp()}] === Geometry Type Breakdown ===")
    for gtype, count in sorted(type_counts.items()):
        print(f"  {gtype}: {count}")

    return geojson


@task
def write_geojson(geojson: dict) -> str:
    """Write GeoJSON FeatureCollection to disk."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    geojson_path = f"{OUTPUT_DIR}/org_units.geojson"

    with open(geojson_path, "w") as f:
        json.dump(geojson, f, indent=2)

    size_mb = os.path.getsize(geojson_path) / (1024 * 1024)
    print(f"[{timestamp()}] Wrote {geojson_path} ({size_mb:.2f} MB)")
    print(f"[{timestamp()}] Features: {len(geojson['features'])}")
    return geojson_path


@task
def report(geojson_path: str) -> None:
    """Read GeoJSON and print summary."""
    with open(geojson_path) as f:
        data = json.load(f)

    features = data["features"]
    print(f"[{timestamp()}] === GeoJSON Report ===")
    print(f"  Feature count: {len(features)}")

    # Bounding box from Point geometries
    lats: list[float] = []
    lons: list[float] = []
    for feat in features:
        geom = feat["geometry"]
        if geom["type"] == "Point":
            coords = geom["coordinates"]
            lons.append(coords[0])
            lats.append(coords[1])

    if lats and lons:
        print("\n  Bounding box (from Point geometries):")
        print(f"    Lat: {min(lats):.4f} to {max(lats):.4f}")
        print(f"    Lon: {min(lons):.4f} to {max(lons):.4f}")

    # Level distribution
    level_counts: dict[int, int] = {}
    for feat in features:
        level = feat["properties"].get("level")
        if level is not None:
            level_counts[level] = level_counts.get(level, 0) + 1

    print("\n  Level distribution:")
    for level in sorted(level_counts):
        print(f"    Level {level}: {level_counts[level]}")


with DAG(
    dag_id="061_dhis2_org_unit_geometry",
    default_args=DEFAULT_ARGS,
    description="DHIS2 org units -> GeoJSON FeatureCollection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2"],
) as dag:
    raw = fetch()
    geojson_data = transform(records=raw)
    geojson_file = write_geojson(geojson=geojson_data)
    report(geojson_path=geojson_file)

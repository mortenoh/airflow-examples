"""DAG 54: Advanced Dynamic Task Mapping.

Builds on DAG 10 with advanced mapping patterns: ``expand_kwargs()``
for zip-style mapping (multiple params per mapped instance), mapping
with transformed inputs, and combining static/dynamic parameters.
"""

from datetime import datetime

from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp


# --- expand_kwargs: zip-style mapping --------------------------------------
# expand_kwargs() maps a list of dicts, where each dict provides keyword
# arguments for one mapped task instance. This is the "zip" pattern --
# parameters are paired together, not crossed.
@task
def generate_station_configs() -> list[dict[str, object]]:
    """Generate a list of station configurations for processing."""
    configs = [
        {"station_id": "oslo_01", "lat": 59.91, "lon": 10.75},
        {"station_id": "bergen_01", "lat": 60.39, "lon": 5.32},
        {"station_id": "tromso_01", "lat": 69.65, "lon": 18.96},
        {"station_id": "stavanger_01", "lat": 58.97, "lon": 5.73},
    ]
    print(f"[{timestamp()}] Generated {len(configs)} station configs")
    return configs


@task
def process_station(station_id: str, lat: float, lon: float) -> dict[str, object]:
    """Process a single station with its coordinates."""
    print(f"[{timestamp()}] Processing {station_id} at ({lat}, {lon})")
    temp = round(20 - abs(lat - 60) * 0.5, 1)
    return {"station": station_id, "lat": lat, "lon": lon, "est_temp": temp}


# --- Mapping with transformed inputs ---------------------------------------
@task
def generate_date_ranges() -> list[str]:
    """Generate date ranges to process."""
    dates = ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
    print(f"[{timestamp()}] Generated {len(dates)} date ranges")
    return dates


@task
def generate_variables() -> list[str]:
    """Generate variables to extract."""
    variables = ["temperature", "humidity", "pressure"]
    print(f"[{timestamp()}] Generated {len(variables)} variables")
    return variables


@task
def extract_data(date: str, variable: str) -> dict[str, str]:
    """Extract data for a specific date and variable."""
    print(f"[{timestamp()}] Extracting {variable} for {date}")
    return {"date": date, "variable": variable, "rows": "1000"}


# --- Combining expand results -----------------------------------------------
@task
def summarize_stations(results: list[dict[str, object]]) -> None:
    """Summarize all station processing results."""
    print(f"[{timestamp()}] === Station Summary ===")
    for r in results:
        print(f"  {r['station']}: ({r['lat']}, {r['lon']}) -> {r['est_temp']}C")
    print(f"  Total: {len(results)} stations processed")


@task
def summarize_extractions(results: list[dict[str, str]]) -> None:
    """Summarize all extraction results."""
    print(f"[{timestamp()}] === Extraction Summary ===")
    for r in results:
        print(f"  {r['date']} / {r['variable']}: {r['rows']} rows")
    print(f"  Total: {len(results)} extractions")


with DAG(
    dag_id="054_advanced_dynamic_mapping",
    default_args=DEFAULT_ARGS,
    description="expand_kwargs (zip), combined expand patterns",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dynamic"],
) as dag:
    # --- Pattern 1: expand_kwargs (zip-style) ---------------------------
    # Each dict in the list provides keyword arguments for one mapped instance.
    configs = generate_station_configs()
    stations = process_station.expand_kwargs(configs)
    summarize_stations(stations)

    # --- Pattern 2: expand with static partial --------------------------
    # .partial() provides the same argument to all instances,
    # .expand() varies another argument across instances.
    dates = generate_date_ranges()
    temp_extractions = extract_data.partial(variable="temperature").expand(date=dates)

    # --- Pattern 3: two independent expands -----------------------------
    variables = generate_variables()
    var_extractions = extract_data.partial(date="2024-01-01").expand(variable=variables)

    summarize_extractions(temp_extractions)

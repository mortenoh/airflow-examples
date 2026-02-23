# Data Pipelines

## Data Pipelines: Parquet Aggregation

A realistic data pipeline that reads Parquet, performs aggregations with pandas, and writes CSV:

```python
@task
def generate_parquet(data_dir: str) -> str:
    import pandas as pd
    import numpy as np

    df = pd.DataFrame({
        "station": np.random.choice(["oslo_01", "bergen_01"], 200),
        "temperature_c": np.round(np.random.normal(10, 8, 200), 1),
        "humidity_pct": np.round(np.random.uniform(40, 95, 200), 1),
    })
    path = f"{data_dir}/weather.parquet"
    df.to_parquet(path, index=False)
    return path

@task
def aggregate_by_station(parquet_path: str) -> str:
    import pandas as pd

    df = pd.read_parquet(parquet_path)
    agg = df.groupby("station").agg(
        temp_mean=("temperature_c", "mean"),
        temp_min=("temperature_c", "min"),
        temp_max=("temperature_c", "max"),
    ).round(2)

    csv_path = parquet_path.replace(".parquet", "_summary.csv")
    agg.to_csv(csv_path)
    return csv_path
```

The pipeline runs three parallel aggregations (by station, by date, cross-tabulation)
from the same Parquet source, then produces a final report.

See: `dags/57_parquet_aggregation.py`

---

## DHIS2 Metadata Pipelines

DAGs 58--62 and 110--111 are a domain-specific detour into health data. You can safely skip
them if you are not working with health information systems -- the general patterns (API calls,
JSON flattening, pandas transforms) are covered again in DAGs 81-100 with more general APIs.
DAGs 110--111 also demonstrate using Airflow Connections for credential management, which
applies to any external API.

[DHIS2](https://dhis2.org/) (District Health Information Software 2) is the world's largest
health information management system, used in 100+ countries. These examples fetch data from
a public test server and demonstrate patterns you would use against any REST API: nested JSON
flattening, derived columns, multi-format output, and parallel pipeline orchestration.

### What is DHIS2?

DHIS2 organizes health data around three core metadata types:

| Metadata Type | Description | Play Server Records |
|---------------|-------------|---------------------|
| **Organisation Units** | Health facilities, districts, regions -- arranged in a hierarchy (country > province > district > facility) | ~1575 |
| **Data Elements** | Individual data points collected at facilities (e.g., "Malaria cases", "BCG doses given") | ~1037 |
| **Indicators** | Calculated metrics derived from data elements (e.g., "BCG coverage %" = doses / target population) | ~77 |

Organisation units can also carry **geometry** (Point for facilities, Polygon/MultiPolygon for
administrative boundaries), making them a source of spatial data.

### DHIS2 REST API

The DHIS2 Web API follows a consistent pattern across all metadata endpoints:

```
GET /api/{resource}?paging=false&fields={fieldSpec}
```

**Key API parameters:**

| Parameter | Description | Example |
|-----------|-------------|---------|
| `paging` | `false` disables pagination, returns all records in one response | `paging=false` |
| `fields` | Controls which fields are returned. `:owner` returns all fields owned by the object (excludes computed fields) | `fields=:owner` |
| `filter` | Server-side filtering (not used in these examples -- we filter client-side with pandas) | `filter=level:eq:4` |

**Authentication**: HTTP Basic Auth. The play server uses `admin`/`district`:

```python
import requests

resp = requests.get(
    "https://play.im.dhis2.org/dev/api/organisationUnits",
    auth=("admin", "district"),
    params={"paging": "false", "fields": ":owner"},
    timeout=60,
)
```

**Response structure**: The JSON response wraps the record list under a key matching the endpoint name:

```json
{
  "organisationUnits": [
    {"id": "abc123", "name": "Sierra Leone", "level": 1, ...},
    {"id": "def456", "name": "Bo", "level": 2, "parent": {"id": "abc123"}, ...}
  ]
}
```

This means the extraction step is always: `data = resp.json()` then `records = data["organisationUnits"]`.

### Shared Helper Module

All DHIS2 DAGs share `src/airflow_examples/dhis2.py` to avoid duplicating API logic. Credentials
are read from the `dhis2_default` Airflow connection at runtime instead of being hardcoded:

```python
"""DHIS2 API helpers for metadata pipeline examples."""

from typing import Any

import httpx
from airflow.exceptions import AirflowFailException
from airflow.sdk.bases.hook import BaseHook

from airflow_examples.config import OUTPUT_BASE

OUTPUT_DIR = str(OUTPUT_BASE / "dhis2_exports")


def _get_dhis2_config() -> tuple[str, tuple[str, str]]:
    """Read DHIS2 base URL and credentials from the Airflow connection."""
    try:
        conn = BaseHook.get_connection("dhis2_default")
    except Exception as exc:
        raise AirflowFailException(
            "Connection 'dhis2_default' not found. "
            "Add it via the Airflow UI or CLI before running DHIS2 DAGs."
        ) from exc
    base_url = (conn.host or "").rstrip("/")
    credentials = (conn.login or "", conn.password or "")
    return base_url, credentials


def fetch_metadata(endpoint: str, fields: str = ":owner") -> list[dict[str, Any]]:
    """Fetch all records from a DHIS2 metadata endpoint."""
    base_url, credentials = _get_dhis2_config()
    url = f"{base_url}/api/{endpoint}"
    resp = httpx.get(
        url,
        auth=credentials,
        params={"paging": "false", "fields": fields},
        timeout=60,
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    key = endpoint.split("?")[0]
    result: list[dict[str, Any]] = data[key]
    return result
```

**Design decisions:**

- **Connection-managed credentials**: The base URL, username, and password live in the
  `dhis2_default` Airflow connection rather than as Python constants. This means credentials
  are stored in one place (the Airflow metadata database or environment variables), never
  committed to source control, and can be changed without redeploying DAG code.
- **`AirflowFailException`**: If the connection is missing, the task raises
  `AirflowFailException` instead of a generic error. This tells Airflow to mark the task as
  `failed` immediately without retrying -- there is no point retrying when the connection
  does not exist yet.
- **`paging=false`**: The play server has small datasets (~1000s of records). For production DHIS2
  instances with millions of records, you'd paginate with `page=1&pageSize=1000` and loop.
- **`fields=:owner`**: Returns all fields the object "owns" (not computed/derived fields). This is
  the most common field spec for metadata exports. You can also request specific fields:
  `fields=id,name,level,parent` (used in DAG 61 for geometry).
- **`timeout=60`**: The play server can be slow. Production code should use retries (Airflow's
  built-in `retries` parameter handles this at the task level).
- **Type annotations**: `list[dict[str, Any]]` satisfies mypy strict mode. The DHIS2 API returns
  heterogeneous dicts, so `Any` values are appropriate.

**Why a shared module instead of inline code?** The fetch logic is identical across all DHIS2 DAGs --
only the endpoint name changes. Centralizing it in a helper module means:

1. Single place to update if the API URL or auth changes
2. DAG files focus on transform logic, not HTTP boilerplate
3. The helper is importable from both DAGs and tests

See: `src/airflow_examples/dhis2.py`

### JSON Flattening Patterns

DHIS2 returns deeply nested JSON. Most fields are flat strings, but several contain nested objects
or arrays that need flattening before tabular output:

#### Nested Object References

Many DHIS2 objects reference their parent or related objects as nested dicts:

```json
{
  "id": "abc123",
  "name": "Bo District",
  "parent": {"id": "xyz789"},
  "categoryCombo": {"id": "combo1"},
  "createdBy": {"username": "admin", "id": "user1"},
  "indicatorType": {"id": "type1", "name": "Per cent"}
}
```

The flattening pattern extracts the nested value with a safe accessor:

```python
# Extract parent ID from nested dict (or None if missing)
df["parent_id"] = df["parent"].apply(
    lambda p: p["id"] if isinstance(p, dict) else None
)

# Extract nested field with .get() for optional keys
df["created_by"] = df["createdBy"].apply(
    lambda c: c.get("username") if isinstance(c, dict) else None
)

# Extract multiple fields from the same nested object
df["indicator_type_id"] = df["indicatorType"].apply(
    lambda t: t["id"] if isinstance(t, dict) else None
)
df["indicator_type_name"] = df["indicatorType"].apply(
    lambda t: t.get("name") if isinstance(t, dict) else None
)
```

**Why `isinstance(p, dict)` guards?** Some records may have `None` or missing parent fields.
Without the guard, `None["id"]` raises `TypeError`. The `isinstance` check handles both `None`
and unexpected types gracefully.

#### Path-Based Hierarchy Depth

Organisation units have a `path` field encoding their position in the hierarchy:

```
"/abc123"                     -> depth 0 (country)
"/abc123/def456"              -> depth 1 (province)
"/abc123/def456/ghi789"       -> depth 2 (district)
"/abc123/def456/ghi789/jkl0"  -> depth 3 (facility)
```

Extracting depth is a simple string operation:

```python
df["hierarchy_depth"] = df["path"].apply(
    lambda p: p.count("/") - 1 if isinstance(p, str) else 0
)
```

The `-1` accounts for the leading `/` -- a path like `/abc123` has one slash but represents
depth 0 (the root level).

#### Array Length Columns

Some fields are arrays (translations, data element groups). Counting them creates useful
summary columns:

```python
df["translation_count"] = df["translations"].apply(
    lambda t: len(t) if isinstance(t, list) else 0
)
```

#### Boolean Derived Columns

Create boolean flags from nullable fields for easy filtering and aggregation:

```python
# True if the record has a code assigned, False otherwise
df["has_code"] = df["code"].notna()
```

This is more useful than the raw `code` column for summary statistics: `df["has_code"].sum()`
gives you the count of coded elements instantly.

### Output Formats: CSV vs Parquet vs GeoJSON

These DAGs demonstrate three output formats, each suited to different use cases:

#### CSV (DAGs 58, 60)

```python
df.to_csv(csv_path, index=False)
```

| Pros | Cons |
|------|------|
| Human-readable in any text editor | No type preservation (everything becomes strings) |
| Universal compatibility | Larger file size than binary formats |
| Easy to inspect and debug | Slow to read for large datasets |
| Git-diffable | No nested data support |

**Best for**: Small-to-medium datasets, sharing with non-technical users, inspection/debugging,
data that will be imported into spreadsheets or other tools.

#### Parquet (DAG 59)

```python
df.to_parquet(parquet_path, index=False)
```

| Pros | Cons |
|------|------|
| Columnar format -- fast reads for analytical queries | Binary format, not human-readable |
| Type preservation (int, float, bool, datetime, string) | Requires pyarrow or fastparquet to read |
| Efficient compression (often 2-10x smaller than CSV) | Not git-diffable |
| Predicate pushdown -- read only the columns you need | Overkill for tiny datasets |

**Best for**: Analytical pipelines, large datasets, data that feeds into pandas/Spark/DuckDB,
preserving column types across pipeline stages.

**Reading Parquet back** is straightforward and preserves all types:

```python
df = pd.read_parquet(parquet_path)
# Boolean columns are still bool, ints are still int -- no type coercion needed
```

#### GeoJSON (DAG 61)

```python
import json

geojson = {
    "type": "FeatureCollection",
    "features": features,
}
with open(geojson_path, "w") as f:
    json.dump(geojson, f, indent=2)
```

| Pros | Cons |
|------|------|
| Standard format for geographic data | Verbose -- large files for complex geometries |
| Supported by QGIS, Mapbox, Leaflet, GitHub map preview | Slower to parse than binary formats (Shapefile, GeoParquet) |
| JSON-based -- no special libraries needed to create | No indexing -- full scan for spatial queries |
| Human-readable geometry representation | Not suitable for raster data |

**Best for**: Sharing geographic data, web map visualization, datasets with mixed geometry types,
when you want to avoid heavyweight GIS dependencies.

### GeoJSON Construction Without GeoPandas

DAG 61 builds GeoJSON manually using only the `json` standard library module. This avoids pulling
in `geopandas` (which depends on GDAL/GEOS C libraries) for a simple metadata export.

#### GeoJSON Structure

A GeoJSON file is a `FeatureCollection` containing `Feature` objects. Each feature has a `geometry`
(the spatial shape) and `properties` (attribute data):

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [10.75, 59.91]
      },
      "properties": {
        "id": "abc123",
        "name": "Oslo Health Centre",
        "level": 4
      }
    },
    {
      "type": "Feature",
      "geometry": {
        "type": "Polygon",
        "coordinates": [[[10.0, 59.0], [11.0, 59.0], [11.0, 60.0], [10.0, 60.0], [10.0, 59.0]]]
      },
      "properties": {
        "id": "def456",
        "name": "Oslo District",
        "level": 3
      }
    }
  ]
}
```

**Coordinate order**: GeoJSON uses `[longitude, latitude]` (not `[lat, lon]`). This is the opposite
of what most people expect. DHIS2 stores geometry in GeoJSON format natively, so no coordinate
swapping is needed.

#### Geometry Types in DHIS2

The play server's organisation units use three geometry types:

| Type | Count | Represents |
|------|-------|------------|
| `Point` | ~601 | Individual health facilities (lat/lon location) |
| `Polygon` | ~143 | Administrative boundaries (districts, provinces) |
| `MultiPolygon` | ~22 | Complex boundaries with disconnected parts (islands, exclaves) |

The remaining ~809 org units have no geometry at all (data-only nodes in the hierarchy).

#### Building Features

The DHIS2 API returns geometry as a GeoJSON geometry object already -- no coordinate parsing needed.
The construction pattern filters records and wraps each in a Feature:

```python
features = []
for record in records:
    geom = record.get("geometry")
    if not geom:
        continue  # Skip units without geometry (~809 of ~1575)

    parent = record.get("parent")
    parent_id = parent["id"] if isinstance(parent, dict) else None

    feature = {
        "type": "Feature",
        "geometry": geom,  # Already valid GeoJSON geometry from DHIS2
        "properties": {
            "id": record["id"],
            "name": record.get("name", ""),
            "shortName": record.get("shortName", ""),
            "level": record.get("level"),
            "parent_id": parent_id,
        },
    }
    features.append(feature)
```

#### Computing a Bounding Box

To summarize the spatial extent, extract coordinates from Point geometries:

```python
lats, lons = [], []
for feat in features:
    geom = feat["geometry"]
    if geom["type"] == "Point":
        coords = geom["coordinates"]
        lons.append(coords[0])  # GeoJSON: [lon, lat]
        lats.append(coords[1])

if lats and lons:
    print(f"Lat: {min(lats):.4f} to {max(lats):.4f}")
    print(f"Lon: {min(lons):.4f} to {max(lons):.4f}")
```

For Polygon/MultiPolygon geometries, you'd need to recurse into the nested coordinate arrays --
but Point coordinates are sufficient for a quick bounding box of facility locations.

### Regex Expression Parsing

DHIS2 indicators are calculated from formulas that reference data elements using `#{...}` syntax:

```
Numerator:   #{fbfJHSPpUQD.pq2XI5kz2BY} + #{fbfJHSPpUQD.PT59n8BQbqM}
Denominator: #{h0xKKjijTdI}
```

Each `#{...}` token is an operand referencing a data element (and optionally a category option combo,
separated by `.`). DAG 60 uses regex to extract and count these operands:

```python
import re

def count_operands(expr: object) -> int:
    """Count #{...} operand references in a DHIS2 expression."""
    if not isinstance(expr, str):
        return 0
    return len(re.findall(r"#\{[^}]+\}", expr))
```

**Regex breakdown:**

| Part | Meaning |
|------|---------|
| `#\{` | Literal `#{` (braces escaped because `{` is a regex quantifier) |
| `[^}]+` | One or more characters that are NOT `}` (the operand ID) |
| `\}` | Literal closing `}` |

#### Complexity Scoring

The expression complexity score combines operand counts with operator counts to estimate how
"involved" an indicator formula is:

```python
def complexity_score(row: pd.Series) -> int:
    """Score = total operands + arithmetic operators."""
    score = int(row["numerator_operands"]) + int(row["denominator_operands"])
    for field in ["numerator", "denominator"]:
        expr = row.get(field)
        if isinstance(expr, str):
            score += expr.count("+") + expr.count("-") + expr.count("*")
    return score
```

This produces a rough distribution:

| Complexity | Meaning | Typical Example |
|------------|---------|-----------------|
| 0 | No operands (constant or empty) | Fixed-value indicators |
| 1--4 | Simple ratio (1-2 operands per side) | Coverage = doses / population |
| 5--9 | Multi-component (several data elements combined) | Composite scores |
| 10+ | Complex formula (many operands, arithmetic) | Weighted indices |

### Parallel Fan-Out / Fan-In Pattern

DAG 62 demonstrates a common real-world pattern: fetch multiple independent data sources in parallel,
transform each independently, then combine results in a single summary task.

#### Pipeline Structure

```
                    ┌─ fetch_org_units ──> transform_org_units ──────────┐
                    │                                                    │
start ──> parallel ─┼─ fetch_data_elements ──> transform_data_elements ─┼──> combined_report ──> cleanup
                    │                                                    │
                    └─ fetch_indicators ──> transform_indicators ────────┘
```

In TaskFlow API, the parallelism happens automatically -- Airflow's scheduler detects that the three
fetch tasks have no dependencies on each other and runs them concurrently:

```python
with DAG(dag_id="62_dhis2_combined_export", ...):
    # These three have no upstream dependencies -- they run in parallel
    org_raw = fetch_org_units()
    elem_raw = fetch_data_elements()
    ind_raw = fetch_indicators()

    # These depend on their respective fetches -- each starts as soon as its input is ready
    org_csv = transform_org_units(records=org_raw)
    elem_parquet = transform_data_elements(records=elem_raw)
    ind_csv = transform_indicators(records=ind_raw)

    # Fan-in: waits for ALL three transforms to complete
    report = combined_report(
        org_units_path=org_csv,
        data_elements_path=elem_parquet,
        indicators_path=ind_csv,
    )

    # Sequential: cleanup runs after report
    cleanup_task = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )
    report >> cleanup_task
```

**How Airflow resolves the parallelism:**

1. `fetch_org_units`, `fetch_data_elements`, `fetch_indicators` -- all three are schedulable
   immediately (no `blockedBy`). With `LocalExecutor`, they run as concurrent processes.
2. `transform_org_units` starts as soon as `fetch_org_units` completes (it doesn't wait for the
   other fetches).
3. `combined_report` has three upstream dependencies -- Airflow's default `trigger_rule=all_success`
   means it waits for all three transforms to succeed.
4. `cleanup` runs last, after the report.

**Benefits of this pattern:**

| Benefit | Description |
|---------|-------------|
| **Faster execution** | Three API calls run concurrently instead of sequentially (~3x faster) |
| **Independent failure** | If `fetch_indicators` fails, `transform_org_units` still completes |
| **Clear data lineage** | The DAG graph shows exactly which data feeds into which output |
| **Easy to extend** | Adding a 4th data source is just another fetch/transform pair |

#### Multi-Format Output

The combined export writes each dataset in the format best suited to its downstream use:

```python
# Org units -> CSV (human-readable, simple tabular data)
org_df.to_csv(f"{OUTPUT_DIR}/combined_org_units.csv", index=False)

# Data elements -> Parquet (preserves boolean types, efficient for analysis)
elem_df.to_parquet(f"{OUTPUT_DIR}/combined_data_elements.parquet", index=False)

# Indicators -> CSV (small dataset, human-readable expressions)
ind_df.to_csv(f"{OUTPUT_DIR}/combined_indicators.csv", index=False)
```

### 58 -- Organisation Units to CSV

Fetches all organisation units (~1575), flattens nested parent references, createdBy usernames,
and path-based hierarchy depth, counts translations per unit, and writes a flat CSV.

**Pipeline:**

```
fetch (GET /api/organisationUnits) -> transform (flatten + derive columns) -> report (formatted summary)
```

**Key transforms:**

```python
df["parent_id"] = df["parent"].apply(lambda p: p["id"] if isinstance(p, dict) else None)
df["created_by"] = df["createdBy"].apply(lambda c: c.get("username") if isinstance(c, dict) else None)
df["hierarchy_depth"] = df["path"].apply(lambda p: p.count("/") - 1 if isinstance(p, str) else 0)
df["translation_count"] = df["translations"].apply(lambda t: len(t) if isinstance(t, list) else 0)
```

**Output columns:** `id`, `name`, `shortName`, `level`, `parent_id`, `created_by`,
`hierarchy_depth`, `translation_count`, `openingDate`

**Report output includes:**

- Count by organisational level (country, province, district, facility)
- Top 10 org units sorted by level
- Opening date range across all units
- Hierarchy depth range and translation coverage

See: `dags/58_dhis2_org_units.py`

### 59 -- Data Elements to Parquet

Fetches all data elements (~1037), flattens category combo references, adds boolean and computed
columns, and writes as Parquet to preserve column types.

**Pipeline:**

```
fetch (GET /api/dataElements) -> transform (flatten + categorize) -> report (summary stats)
```

**Key transforms:**

```python
df["category_combo_id"] = df["categoryCombo"].apply(lambda c: c["id"] if isinstance(c, dict) else None)
df["has_code"] = df["code"].notna()        # Boolean: was a code assigned?
df["name_length"] = df["name"].str.len()   # Numeric: name string length
```

**Why Parquet for this dataset?** Data elements have boolean (`has_code`) and integer (`name_length`)
columns. CSV would coerce booleans to strings (`"True"`/`"False"`) and require type inference on
read. Parquet preserves exact types:

```python
# Reading Parquet preserves types
df = pd.read_parquet("data_elements.parquet")
assert df["has_code"].dtype == bool          # Stays bool, not object
assert df["name_length"].dtype == "int64"    # Stays int, not object
```

**Report output includes:**

- Value type breakdown (NUMBER, BOOLEAN, TEXT, etc.)
- Aggregation type breakdown (SUM, AVERAGE, COUNT, etc.)
- Domain type distribution (AGGREGATE vs TRACKER)
- Code assignment coverage
- Name length statistics (min, max, mean)

See: `dags/59_dhis2_data_elements.py`

### 60 -- Indicators with Expression Parsing

Fetches all indicators (~77), flattens indicator type references, parses numerator and denominator
expressions with regex, and computes a complexity score.

**Pipeline:**

```
fetch (GET /api/indicators) -> transform (parse expressions + score complexity) -> report (ranked summary)
```

**Key transforms:**

```python
import re

def count_operands(expr: object) -> int:
    if not isinstance(expr, str):
        return 0
    return len(re.findall(r"#\{[^}]+\}", expr))

df["numerator_operands"] = df["numerator"].apply(count_operands)
df["denominator_operands"] = df["denominator"].apply(count_operands)
df["expression_complexity"] = df.apply(complexity_score, axis=1)
```

**Report output includes:**

- Complexity distribution (trivial / simple / moderate / complex / very complex)
- Top 5 most complex indicators with their scores
- Top 5 simplest non-trivial indicators
- Indicator type breakdown (Per cent, Per 1000, etc.)

See: `dags/60_dhis2_indicators.py`

### 61 -- Organisation Unit Geometry to GeoJSON

Fetches org units with a targeted field list (only geometry-relevant fields), filters to the ~766
units that have spatial data, builds a GeoJSON FeatureCollection, and writes to disk.

**Pipeline:**

```
fetch (GET /api/organisationUnits?fields=id,name,...,geometry) -> transform (filter + build GeoJSON) -> write_geojson -> report
```

**Key difference from DAG 58:** This DAG requests specific fields (`id,name,shortName,level,parent,geometry`)
instead of `:owner`. This is more efficient -- geometry data can be large (polygon coordinate arrays),
so we skip irrelevant fields like `translations`, `openingDate`, etc.

```python
records = fetch_metadata(
    "organisationUnits",
    fields="id,name,shortName,level,parent,geometry",
)
```

**Report output includes:**

- Total feature count
- Geometry type breakdown (Point: ~601, Polygon: ~143, MultiPolygon: ~22)
- Bounding box from Point geometries (lat/lon extent)
- Level distribution of features with geometry

See: `dags/61_dhis2_org_unit_geometry.py`

### 62 -- Combined Parallel Export

Fetches all three metadata types in parallel, transforms each independently, writes multi-format
output, and produces a combined summary report. A `BashOperator` cleanup task removes all temporary
files.

**Pipeline:**

```
fetch_org_units ────────> transform_org_units (CSV) ──────────┐
fetch_data_elements ────> transform_data_elements (Parquet) ──┼──> combined_report ──> cleanup
fetch_indicators ───────> transform_indicators (CSV) ─────────┘
```

**Report output includes:**

- Record counts per metadata type and total
- Org unit level distribution
- Data element domain type breakdown
- Indicator complexity summary (mean, max, trivial count)
- List of all output file paths

See: `dags/62_dhis2_combined_export.py`

### DHIS2 with Airflow Connections

DAGs 110--111 revisit DHIS2 but use Airflow Connections to manage credentials instead of
hardcoding them. If you skipped DAGs 58--62 because DHIS2 is not relevant to your work, you
can still use these two DAGs as a reference for the Airflow Connection pattern -- the concepts
apply to any external API.

**Key difference from DAGs 58--62:** The earlier DAGs stored the DHIS2 base URL and credentials
as Python constants in `dhis2.py`. DAGs 110--111 rely on a `dhis2_default` connection configured
in Airflow (via the UI, CLI, or `compose.yml` environment variables). The shared helper
`_get_dhis2_config()` reads the connection at runtime with `BaseHook.get_connection()`.

**Why connections matter:**

| Approach | DAGs 58--62 | DAGs 110--111 |
|----------|-------------|---------------|
| Credentials stored in | Python source code | Airflow metadata DB / env vars |
| Changing the server URL | Edit code, redeploy DAGs | Update the connection, no code change |
| Secret exposure risk | Credentials in version control | Credentials managed by Airflow |
| Missing credentials | `httpx` error at request time | `AirflowFailException` before the request |

#### 110 -- Connection Basics

Retrieves the `dhis2_default` connection details and fetches a simple org unit count to verify
the connection works end-to-end.

**Pipeline:**

```
show_connection -> fetch_org_unit_count
```

**Tasks:**

| Task | Description |
|------|-------------|
| `show_connection` | Reads `dhis2_default` via `BaseHook.get_connection()` and prints connection fields (password redacted) |
| `fetch_org_unit_count` | Calls `fetch_metadata("organisationUnits", fields="id")` and prints the record count |

```python
@task
def show_connection() -> dict:
    conn = BaseHook.get_connection("dhis2_default")
    info = {
        "conn_id": conn.conn_id,
        "conn_type": conn.conn_type,
        "host": conn.host,
        "login": conn.login,
    }
    print(f"[{timestamp()}] Connection details (password redacted):")
    for key, value in info.items():
        print(f"  {key}: {value}")
    return info
```

See: `dags/110_dhis2_connection_basics.py`

#### 111 -- Analytics Data Values

Fetches analytics data (not metadata) from the DHIS2 `/api/analytics` endpoint, transforms the
DHIS2 rows/headers response format into a pandas DataFrame, and writes to CSV.

**Pipeline:**

```
fetch_data_values -> transform -> report
```

**Tasks:**

| Task | Description |
|------|-------------|
| `fetch_data_values` | Calls `/api/analytics` with dimension parameters for ANC visit data elements at the national level |
| `transform` | Parses the DHIS2 `headers`/`rows` response into a DataFrame and writes CSV |
| `report` | Reads the CSV back and prints summary statistics (row count, data element breakdown, value range) |

**DHIS2 analytics response format:** Unlike metadata endpoints that return a list of objects,
the analytics API returns a tabular structure with separate `headers` and `rows` arrays:

```python
headers = [h["name"] for h in analytics["headers"]]
rows = analytics["rows"]
df = pd.DataFrame(rows, columns=headers)
```

See: `dags/111_dhis2_data_values.py`

---

## File-Based ETL Pipelines

DAGs 63--67 demonstrate production-grade file processing patterns: landing zone architecture,
multi-format ingestion, error handling with quarantine, and incremental processing with manifests.
These patterns apply to any batch file processing pipeline -- CSV uploads, JSON event streams,
mixed-format data lakes, and incremental file watches.

### Landing Zone Architecture

File-based ETL pipelines follow a directory-stage pattern where files move through well-defined zones:

```
Landing -> Processing -> Archive
                    \-> Quarantine (bad files)
```

| Directory | Purpose | Lifecycle |
|-----------|---------|-----------|
| **Landing** | Drop zone for incoming files; untouched raw data | Files arrive here, are read, then moved out |
| **Processing** | Intermediate work area; transformed data written here | Populated during pipeline run, consumed downstream |
| **Archive** | Timestamped copies of successfully processed originals | Retained for audit trail and reprocessing |
| **Quarantine** | Bad files + companion `.reason` files explaining why | Reviewed manually, fixed, or discarded |

The shared helper module `src/airflow_examples/file_utils.py` provides constants and functions
for this pattern:

```python
LANDING_DIR = "/tmp/airflow_landing"
PROCESSED_DIR = "/tmp/airflow_processed"
ARCHIVE_DIR = "/tmp/airflow_archive"
QUARANTINE_DIR = "/tmp/airflow_quarantine"

def setup_dirs(*dirs):       # Create all directories
def archive_file(src, dir):  # Move with timestamp prefix (20250101T120000_data.csv)
def quarantine_file(src, dir, reason):  # Move + write .reason companion file
```

**Why timestamp-prefixed archives?** When the same filename arrives in multiple batches (e.g.,
daily `export.csv`), timestamp prefixes prevent overwrites and create a natural audit trail.

### File Detection Patterns

Three approaches for detecting new files, from simplest to most robust:

| Pattern | Use Case | DAG Example |
|---------|----------|-------------|
| `glob.glob("*.csv")` | Simple batch -- process everything in landing dir | DAGs 63-66 |
| Manifest-based diffing | Incremental -- only process files not in manifest | DAG 67 |
| `FileSensor` | Event-driven -- wait for specific file to appear | DAG 50 |

**Glob** is simplest: scan the landing directory, return all matching files. Good when you process
everything and archive after. The downside is it requires cleanup to avoid reprocessing.

**Manifest-based** tracking (DAG 67) maintains a JSON file listing previously processed filenames
and timestamps. On each run, glob the directory, diff against the manifest, and only process new
arrivals:

```python
# Load manifest, diff against directory listing
manifest = json.load(open("manifest.json"))
processed_names = {entry["filename"] for entry in manifest["files"]}
new_files = [f for f in glob.glob("*.csv") if basename(f) not in processed_names]
```

This enables idempotent, incremental processing without moving or deleting source files.

### CSV and JSON Parsing

**CSV parsing** (DAG 63): Use `pd.read_csv()` with validation:

```python
df = pd.read_csv(file_path)
expected = {"station", "date", "temperature_f", "humidity_pct", "pressure_hpa"}
if not expected.issubset(set(df.columns)):
    # Handle schema mismatch
```

**JSON flattening** (DAG 64): Use `pd.json_normalize()` for nested structures:

```python
# Nested JSON: {"station": {"id": "oslo_01", "location": {"lat": 59.91}}}
df = pd.json_normalize(events, sep="_")
# Result columns: station_id, station_location_lat, ...
```

The `sep="_"` parameter controls how nested key paths join into column names.

**Mixed-format harmonization** (DAG 65): When CSV and JSON use different column names for the
same data, standardize with rename maps:

```python
rename_map = {"temperature_f": "temperature", "humidity_pct": "humidity"}
df = df.rename(columns=rename_map)
```

### Quarantine Pattern

DAG 66 demonstrates isolating errors at two levels:

1. **File-level**: Completely unparseable files (corrupt CSV, invalid encoding) are moved to
   quarantine with a `.reason` companion file
2. **Row-level**: Parseable files with some bad rows split into good DataFrame + bad rows list

```python
try:
    df = pd.read_csv(file_path)
except Exception as e:
    quarantine_file(file_path, QUARANTINE_DIR, f"Unparseable: {e}")
    continue

# Row-level validation
numeric_temp = pd.to_numeric(df["temperature_f"], errors="coerce")
valid_mask = ~(numeric_temp.isna() & df["temperature_f"].notna())
good_df = df[valid_mask]
bad_df = df[~valid_mask]
```

This pattern ensures that one bad file doesn't stop the entire batch, and bad data is preserved
for investigation rather than silently dropped.

### Per-DAG Reference

#### 63 -- CSV Landing Zone

Watch directory for CSVs, validate headers, convert temperature F->C, archive originals.

```
setup -> detect_files -> process_file -> archive_originals -> report -> cleanup
```

See: `dags/63_csv_landing_zone.py`

#### 64 -- JSON Event Ingestion

Parse nested JSON event files, flatten with `json_normalize`, write combined Parquet.

```
setup -> detect_events -> parse_and_normalize -> archive_events -> report -> cleanup
```

See: `dags/64_json_event_ingestion.py`

#### 65 -- Multi-File Batch

Process mixed CSV+JSON, harmonize column names, merge and deduplicate.

```
setup -> detect_and_classify -> process_csv_batch -+
                             -> process_json_batch -+-> merge -> write_summary -> report -> cleanup
```

See: `dags/65_multi_file_batch.py`

#### 66 -- Error Handling ETL

Quarantine pattern: corrupt files dead-lettered, bad rows isolated, clean data flows through.

```
setup -> detect -> process_with_quarantine -> write_clean_output --+
                                           -> write_quarantine_log +-> quarantine_report -> cleanup
```

See: `dags/66_error_handling_etl.py`

#### 67 -- Incremental File Processing

Manifest-based tracking -- only process files not yet recorded in `manifest.json`.

```
setup -> detect_new_files -> process_new_files -> update_manifest -> report -> cleanup
```

See: `dags/67_incremental_file_processing.py`

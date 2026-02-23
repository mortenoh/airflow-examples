# Integrations

## dbt Integration

DAGs 101--102 show how Airflow orchestrates [dbt](https://www.getdbt.com/) transformations.
The pattern is: Airflow loads raw data into PostgreSQL, then triggers `dbt run` and `dbt test`
via `BashOperator`.

### How It Works

1. **DAG 101** fetches data from REST Countries and World Bank APIs, then inserts it into
   PostgreSQL tables (`raw_countries`, `raw_indicators`).
2. **DAG 102** runs `dbt run` to build staging and mart models, then `dbt test` to validate
   referential integrity and data quality.

dbt handles the SQL transformations (staging views, dimensional models, aggregations) while
Airflow handles scheduling, retries, and task dependencies.

### Per-DAG Reference

| DAG | What It Does |
|-----|-------------|
| 101 | Load REST Countries + World Bank data into PostgreSQL for dbt |
| 102 | Orchestrate `dbt run` and `dbt test` via BashOperator |

---

## S3 Object Storage

DAGs 103--105 demonstrate working with S3-compatible object storage (RustFS). The project
includes a [RustFS](https://github.com/rustfs/rustfs) container that provides an S3-compatible
API on `http://localhost:9001`.

### Raw boto3 (DAGs 103--104)

DAGs 103 and 104 use `boto3` directly -- creating clients with explicit endpoint URLs and
credentials, using `put_object()` / `get_object()` for I/O, and managing byte buffers manually.
This works but requires hardcoded connection details in every task.

### ObjectStoragePath (DAG 105)

DAG 105 introduces Airflow's native `ObjectStoragePath` -- a pathlib-like abstraction that uses
Airflow connections instead of hardcoded credentials. The `aws_default` connection (configured
in `compose.yml`) provides the endpoint URL and credentials automatically.

Key patterns:

```python
from airflow.sdk import ObjectStoragePath

# Connection-aware path (uses aws_default connection)
base = ObjectStoragePath("s3://aws_default@airflow-data/")

# Pathlib-like concatenation
path = base / "subdir" / "file.parquet"

# File-like read/write
with path.open("wb") as f:
    df.to_parquet(f)
with path.open("rb") as f:
    df = pd.read_parquet(f)

# Listing and metadata
for child in base.iterdir():
    print(child, child.stat().st_size)
```

Compared to raw boto3:

| Feature | boto3 (DAGs 103--104) | ObjectStoragePath (DAG 105) |
|---------|----------------------|---------------------------|
| Credentials | Hardcoded per task | Airflow connection (`aws_default`) |
| Path building | String concatenation | `base / "subdir" / "file"` |
| Read/write | `get_object()` + BytesIO | `path.open("rb")` / `path.open("wb")` |
| Listing | `list_objects_v2()` | `path.iterdir()` |
| Metadata | `head_object()` | `path.stat()` |

### Data Lake Pattern

DAG 104 demonstrates the bronze/silver/gold data lake pattern:

- **Bronze**: raw API responses as JSON
- **Silver**: cleaned, normalized data as Parquet
- **Gold**: aggregated, analysis-ready data as Parquet

DAG 105 follows the same layering with `ObjectStoragePath`, writing forecast data to an
`objstore/` prefix and aggregated summaries to `objstore_gold/`.

### Per-DAG Reference

| DAG | Approach | Key Concept |
|-----|----------|-------------|
| 103 | boto3 | Write/read Parquet round-trip to RustFS |
| 104 | boto3 | Bronze/silver/gold data lake pattern |
| 105 | ObjectStoragePath | Pathlib-like S3 API with Airflow connections |

---

## Human-in-the-Loop (HITL)

Airflow 3.1 introduced Human-in-the-Loop operators that pause DAG execution and present a
form in the Airflow web UI for human input. DAG 106 demonstrates all four HITL operators
in a Nordic weather data quality review workflow.

### HITL Operators

| Operator | Purpose | Key Parameters |
|----------|---------|----------------|
| `HITLEntryOperator` | Collect free-text input via `Param` form fields | `params`, `subject`, `body` |
| `HITLOperator` | Single-select (or multi-select) from a list of options | `options`, `defaults`, `multiple` |
| `ApprovalOperator` | Binary approve/reject gate; skips downstream on reject | `defaults`, `fail_on_reject` |
| `HITLBranchOperator` | Route to different downstream tasks based on user choice | `options`, `options_mapping`, `defaults` |

All four operators inherit from `HITLOperator` and share common parameters:

- **`subject`** -- headline shown in the Airflow UI
- **`body`** -- descriptive text (supports Jinja2 templating and Markdown)
- **`execution_timeout`** -- auto-selects `defaults` when timeout expires (essential for `airflow dags test`)
- **`defaults`** -- pre-selected options used when timeout is reached or as initial selection in the UI
- **`notifiers`** -- `BaseNotifier` subclasses called when the task starts waiting

### Auto-Resolution for Testing

Every HITL operator in DAG 106 sets `execution_timeout=timedelta(seconds=5)` with sensible
defaults so the pipeline completes without human input during `airflow dags test`:

| Operator | Default |
|----------|---------|
| `HITLEntryOperator` | Param default `"Auto-approved: no manual review"` |
| `HITLOperator` | `["Good"]` |
| `ApprovalOperator` | `"Approve"` |
| `HITLBranchOperator` | `["export_csv"]` |

### DAG 106 Task Flow

```
fetch_weather_data          (@task -- fetch Nordic weather from Open-Meteo)
       |
  add_analyst_note          (HITLEntryOperator -- free-text input via Param)
       |
  choose_quality_rating     (HITLOperator -- single-select: Excellent/Good/Acceptable/Poor)
       |
  approve_publication       (ApprovalOperator -- Approve/Reject gate)
       |
  choose_output_format      (HITLBranchOperator -- routes to one export task)
     / | \
export_csv  export_json  export_parquet
     \ | /
  join_exports              (EmptyOperator, trigger_rule="none_failed_min_one_success")
       |
  publish_report            (@task -- summary of all HITL decisions)
```

### Per-DAG Reference

| DAG | Key Concept |
|-----|-------------|
| 106 | All four HITL operators with auto-resolution defaults |

---

## Variable-Driven Scheduling

Airflow's REST API does not support changing a DAG's schedule directly. The
recommended workaround is to read the schedule from an Airflow Variable, which
*can* be updated via the API or the UI. DAG 107 demonstrates this pattern,
building on DAG 17 (Variables & Params) by showing how Variables can control
*when* a DAG runs -- not just *how* it runs.

### How It Works

1. At **parse time** the DAG file calls `Variable.get("dag_107_schedule",
   default_var="@daily")` and passes the result to `schedule=`.
2. The scheduler re-parses DAG files periodically, so updating the Variable
   changes the effective schedule on the next parse cycle.
3. No code change or deployment is needed -- just update the Variable.

### Changing the Schedule

**Via the Airflow UI:** Admin -> Variables -> `dag_107_schedule` -> edit value.

**Via the REST API (Airflow 3.x):**

```bash
curl -X PATCH http://localhost:8081/api/v2/variables/dag_107_schedule \
  -H "Content-Type: application/json" \
  -d '{"value": "@hourly"}'
```

### DAG 107 Task Flow

```
show_schedule_info      (@task -- print current schedule and change instructions)
  |
set_new_schedule        (@task -- Variable.set() to demonstrate programmatic updates)
  |
fetch_weather           (@task -- fetch Nordic weather via Open-Meteo)
  |
save_result             (@task -- write JSON output to target/variable_schedule/)
```

### Quick Reference

| DAG | Key Concept |
|-----|-------------|
| 107 | Variable-sourced schedule, changeable via REST API or UI without code edits |

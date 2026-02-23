# Quality & Testing

## Data Quality & Validation

DAGs 68--72 demonstrate systematic data quality checking: schema validation, statistical anomaly
detection, freshness and completeness monitoring, cross-dataset consistency, and consolidated
quality reporting. These patterns catch data issues early, before they propagate downstream.

### Quality Dimensions

| Dimension | What It Checks | DAG |
|-----------|---------------|-----|
| **Schema** | Column names, data types, nullable constraints | 68 |
| **Accuracy** | Physical bounds, statistical outliers, distribution shifts | 69 |
| **Freshness** | File modification time within expected window | 70 |
| **Completeness** | Row counts, date coverage, null rates | 70 |
| **Consistency** | Referential integrity, cross-table rules, temporal validity | 71 |

### Shared Quality Module

All quality DAGs share `src/airflow_examples/quality.py` which provides a `QualityResult`
dataclass and reusable check functions:

```python
@dataclass
class QualityResult:
    check_name: str       # "schema_columns", "bounds_temperature_c", etc.
    passed: bool          # True if check passed
    details: str          # Human-readable description
    severity: str         # "info", "warning", "critical"
```

Check functions return `QualityResult` objects that can be aggregated:

```python
check_schema(df, expected_columns) -> QualityResult
check_bounds(df, column, min_val, max_val) -> QualityResult
check_nulls(df, columns, max_null_pct) -> QualityResult
check_freshness(file_path, max_age_seconds) -> QualityResult
check_row_count(df, min_rows, max_rows) -> QualityResult
```

**Why a dataclass?** Uniform structure makes it easy to aggregate results from different checks,
compute overall scores, and format reports. Every check returns the same shape regardless of
what it validates.

### Schema-as-Code

DAG 68 validates data against an expected schema definition. The schema is defined in code
(not a separate file), making it version-controlled and testable:

```python
EXPECTED_COLUMNS = ["station", "date", "temperature_c", "humidity_pct", "pressure_hpa"]

# Column presence check
result = check_schema(df, EXPECTED_COLUMNS)

# Type validation per column
numeric_temp = pd.to_numeric(df["temperature_c"], errors="coerce")
non_numeric = numeric_temp.isna() & df["temperature_c"].notna()
```

### Statistical Methods

DAG 69 uses three statistical techniques:

**Bounds checking**: Verify values fall within physically possible ranges. Temperature can't
be 100C, humidity can't be negative:

```python
check_bounds(df, "temperature_c", min_val=-50.0, max_val=60.0)
```

**Z-score outlier detection**: Flag individual values more than 3 standard deviations from
the column mean:

```
z = |x - mean| / std
outlier if z > 3
```

**Distribution shift detection**: Compare first-half vs second-half statistics. A sudden
change in the mean indicates a data source problem:

```python
mean_diff = abs(second_half.mean() - first_half.mean())
shifted = mean_diff > first_half.std() * 1.5
```

### Traffic-Light Reporting

DAG 70 uses a green/yellow/red reporting pattern for at-a-glance status:

```python
if passed == total:
    light = "GREEN"    # All checks passed
elif passed >= total // 2:
    light = "YELLOW"   # Some failures
else:
    light = "RED"      # Majority failing
```

### Cross-Dataset Validation

DAG 71 validates relationships between master (stations) and detail (observations) tables:

- **Referential integrity**: All `station_id` values in observations exist in stations master
- **Value consistency**: Arctic stations (lat > 60) shouldn't report tropical temperatures
- **Temporal consistency**: Observation dates fall within station's `active_from..active_to`

### Quality Scoring

DAG 72 aggregates all check types and computes an overall quality score:

```python
score = (passed / total * 100)
status = "HEALTHY" if score >= 90 else "DEGRADED" if score >= 70 else "CRITICAL"
```

### Per-DAG Reference

#### 68 -- Schema Validation

Column names, value types, and nullability checks against expected schema.

```
generate_data -> validate_columns ─┐
              -> validate_types ───┼-> schema_report -> cleanup
              -> validate_nullability ─┘
```

See: `dags/68_schema_validation.py`

#### 69 -- Statistical Checks

Bounds checking, z-score outlier detection, and distribution shift detection.

```
generate_data -> check_bounds ─────────┐
              -> check_zscore ─────────┼-> anomaly_report -> cleanup
              -> check_distribution ───┘
```

See: `dags/69_statistical_checks.py`

#### 70 -- Freshness & Completeness

File freshness, row counts, date gap detection, and null rate thresholds.

```
generate_data -> check_freshness ────┐
              -> check_row_counts ───┼-> quality_summary -> cleanup
              -> check_date_coverage ┤
              -> check_null_rates ───┘
```

See: `dags/70_freshness_completeness.py`

#### 71 -- Cross-Dataset Validation

Referential integrity, value consistency, and temporal consistency between datasets.

```
generate_datasets -> check_referential_integrity ─┐
                  -> check_value_consistency ──────┼-> integrity_report -> cleanup
                  -> check_temporal_consistency ───┘
```

See: `dags/71_cross_dataset_validation.py`

#### 72 -- Quality Dashboard

Consolidated report aggregating schema, statistical, and freshness checks with overall scoring.

```
generate_data -> run_schema_checks ─────┐
              -> run_statistical_checks ─┼-> compile_report -> cleanup
              -> run_freshness_checks ───┘
```

See: `dags/72_quality_report.py`

---

## Alerting & SLA Monitoring

DAGs 73--76 demonstrate production alerting patterns: SLA tracking with execution timeouts,
webhook notifications to external services, progressive failure escalation with callbacks, and
pipeline health monitoring as a meta-DAG.

### Airflow Callback Types

Airflow provides several callback hooks at the task and DAG level:

| Callback | Trigger | Use Case |
|----------|---------|----------|
| `on_success_callback` | Task completes successfully | Log success, clear alerts, send notification |
| `on_failure_callback` | Task fails after all retries | Page on-call, send critical alert |
| `on_retry_callback` | Task fails but will retry | Log warning, increment retry counter |
| DAG `on_success_callback` | All tasks in DAG complete | Send pipeline-complete notification |
| DAG `on_failure_callback` | Any task in DAG fails permanently | Send pipeline-failed alert |

Callbacks receive a `context` dict with task instance, execution date, and other metadata:

```python
def on_failure_callback(context):
    ti = context["task_instance"]
    print(f"Task {ti.task_id} failed on attempt {ti.try_number}")
```

### Execution Timeout vs SLA

| Mechanism | Scope | Behavior |
|-----------|-------|----------|
| `execution_timeout` | Single task | Kills task if it exceeds duration, raises `AirflowTaskTimeout` |
| Duration tracking | Custom | Compare actual vs expected timing in a report task |
| `on_failure_callback` | Single task | Fires when task fails (including timeout) |

```python
@task(execution_timeout=timedelta(seconds=30))
def slow_task():
    """Will be killed if it runs longer than 30 seconds."""
    time.sleep(5)
```

### Webhook Integration

DAG 74 uses `requests.post()` to send JSON payloads to httpbin (simulating Slack/Teams):

```python
payload = {
    "event": "pipeline_success",
    "dag_id": "74_webhook_notifications",
    "timestamp": datetime.now().isoformat(),
    "stats": {"rows_processed": 500},
}
requests.post("http://httpbin:8080/post", json=payload, timeout=10)
```

**Conditional notifications** use `trigger_rule`:

- `trigger_rule="one_failed"` -- only runs if an upstream task failed (failure handler)
- `trigger_rule="all_done"` -- always runs regardless of upstream success/failure (final status)

### Failure Escalation Strategy

DAG 75 demonstrates progressive escalation:

```
Attempt 1: Fail -> on_retry_callback (log warning)
Attempt 2: Fail -> on_retry_callback (escalate warning)
Attempt 3: Succeed -> on_success_callback (log recovery)
     or
Attempt 3: Fail -> on_failure_callback (critical alert, page on-call)
```

The `try_number` attribute on the task instance tracks which attempt is running:

```python
@task(retries=2, retry_delay=timedelta(seconds=2),
      on_retry_callback=on_retry, on_failure_callback=on_failure)
def unreliable_task():
    ...
```

### Health Check Pattern

DAG 76 is a meta-DAG that monitors other pipelines by checking their output artifacts:

1. **Existence**: Do expected output files exist?
2. **Freshness**: Were they modified within the expected time window?
3. **Size**: Are they non-empty and within expected bounds?

```python
EXPECTED_OUTPUTS = [
    {"path": "/output/pipeline_a.csv", "max_age_s": 3600, "min_size": 100},
    {"path": "/output/pipeline_b.parquet", "max_age_s": 3600, "min_size": 200},
]
```

If any checks fail, the health status is computed (healthy/degraded/critical) and a webhook
alert is sent conditionally.

### Per-DAG Reference

#### 73 -- SLA Monitoring

Task-level `execution_timeout`, duration tracking, and SLA breach reporting.

```
fast_task ─────┐
slow_task ─────┼-> sla_report
critical_task ─┘
```

See: `dags/73_sla_monitoring.py`

#### 74 -- Webhook Notifications

POST JSON alerts to httpbin on start, success, and conditional failure.

```
send_start -> process_data -> send_success ──┐
                           -> handle_failure ─┼-> final_status
```

See: `dags/74_webhook_notifications.py`

#### 75 -- Failure Escalation

Retry -> warn -> alert -> recover with progressive callbacks.

```
unreliable_task (retries=2) ─┐
always_succeeds ─────────────┼-> escalation_summary
```

See: `dags/75_failure_escalation.py`

#### 76 -- Pipeline Health Check

Meta-monitoring: file existence, freshness, size, conditional webhook alerting.

```
setup_test_files -> check_file_existence ─┐
                 -> check_file_freshness ──┼-> health_summary -> alert_if_unhealthy -> cleanup
                 -> check_file_sizes ──────┘
```

See: `dags/76_pipeline_health_check.py`

---

## DAG Testing Patterns

DAGs 77--80 demonstrate how to structure DAGs for testability: separating business logic from
DAG wiring, mocking external services, using `dag.test()` for local development, and building
parameterized pipelines driven by DAG `params`.

### Testable DAG Architecture

The key principle: **keep DAG files thin**. All business logic lives in importable Python modules
that have no Airflow dependencies. DAG files are pure wiring:

```
src/airflow_examples/etl_logic.py    <-- Pure functions (testable)
dags/77_testable_dag_pattern.py      <-- Thin wiring (calls etl_logic)
tests/test_etl_logic.py              <-- Unit tests (no Airflow needed)
```

The helper module `src/airflow_examples/etl_logic.py` provides pure functions:

```python
def extract_weather_data(n_records, seed) -> list[dict]: ...
def validate_records(records, required_fields, temp_range) -> tuple[list, list]: ...
def transform_records(records) -> list[dict]: ...
def load_records(records, output_path) -> dict: ...
def fetch_api_data(url, timeout) -> dict: ...
```

The DAG file simply calls these functions from `@task` decorated wrappers:

```python
@task
def extract():
    from airflow_examples.etl_logic import extract_weather_data
    return extract_weather_data(n_records=50, seed=42)
```

**Why deferred imports?** Importing `etl_logic` inside the `@task` body means the module is
loaded at task execution time, not at DAG parse time. This keeps DAG parsing fast and avoids
import errors if the module has heavy dependencies.

### Mocking External Services

DAG 78 calls `etl_logic.fetch_api_data()` which wraps `requests.get()`. Tests can mock this
without touching Airflow:

```python
from unittest.mock import patch, MagicMock

@patch("airflow_examples.etl_logic.requests.get")
def test_fetch(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": "test"}
    mock_get.return_value = mock_response

    result = fetch_api_data("http://test.com/api")
    assert result["data"] == "test"
    mock_get.assert_called_once()
```

The key is that `fetch_api_data` is a thin wrapper around `requests.get`, making the mock
target predictable and the test simple.

### dag.test() for Local Development

DAG 79 includes an `if __name__ == "__main__"` block:

```python
with DAG(...) as dag:
    data = generate_data()
    processed = process(data=data)
    ...

if __name__ == "__main__":
    dag.test()
```

This lets you run the DAG directly: `python dags/79_dag_test_runner.py`. The `dag.test()` method
executes all tasks sequentially without the scheduler, webserver, or database -- perfect for
quick iteration during development.

### Parameterized Pipelines

DAG 80 uses DAG `params` for configuration with defaults:

```python
DAG_PARAMS = {
    "n_records": 50,
    "seed": 42,
    "temp_unit": "celsius",
    "output_format": "csv",
}

with DAG(..., params=DAG_PARAMS) as dag:
    ...

@task
def extract(**context):
    params = context["params"]
    records = extract_weather_data(n_records=params["n_records"])
```

Tests can override parameters via `run_conf`:

```python
dag.test(run_conf={"n_records": 10, "output_format": "parquet"})
```

This enables config-driven pipelines where the same DAG serves multiple use cases with
different parameters.

### Per-DAG Reference

#### 77 -- Testable DAG Pattern

Thin DAG wiring with all logic in `etl_logic.py`.

```
extract -> validate -> transform -> load -> report -> cleanup
```

See: `dags/77_testable_dag_pattern.py`

#### 78 -- Mock API Pipeline

API pipeline with mockable `fetch_api_data()` for testing.

```
fetch_from_api -> parse_response -> write_output -> report -> cleanup
```

See: `dags/78_mock_api_pipeline.py`

#### 79 -- DAG Test Runner

`dag.test()` and `__main__` pattern for local development.

```
generate_data -> process -> validate_output -> summary
```

See: `dags/79_dag_test_runner.py`

#### 80 -- Parameterized Pipeline

Config-driven pipeline with DAG `params` and overridable defaults.

```
extract -> transform -> validate -> load -> cleanup
```

See: `dags/80_parameterized_pipeline.py`

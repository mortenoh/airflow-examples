# Airflow Examples

You know Python. Now learn [Apache Airflow](https://airflow.apache.org/) -- the tool that turns
your Python scripts into scheduled, monitored, production-ready data pipelines.

These **107 progressive examples** start from a single "hello world" and build up to multi-API
ETL pipelines with dbt integration, S3-compatible object storage, human-in-the-loop workflows,
variable-driven scheduling, and full observability. No prior knowledge of Airflow, Docker, Bash, or SQL is required -- the
[documentation](docs/index.md) explains everything from scratch.

Each example is a real, runnable DAG (workflow) that you can trigger, inspect, and modify.

## Core DAGs (001-020)

| # | DAG | Description |
|---|-----|-------------|
| 001 | `hello_world.py` | Minimal DAG, `BashOperator`, DAG context manager, `schedule=None` |
| 002 | `python_operator.py` | `PythonOperator`, callable tasks, `op_args`/`op_kwargs`, return values |
| 003 | `task_dependencies.py` | `>>`, `<<`, `chain()`, `cross_downstream()`, fan-in/fan-out |
| 004 | `taskflow_api.py` | `@task` decorator, return values as automatic XComs, multiple decorated tasks |
| 005 | `xcoms.py` | Manual `ti.xcom_push()` / `ti.xcom_pull()`, sharing data between tasks |
| 006 | `branching.py` | `@task.branch`, choosing execution paths conditionally, join patterns |
| 007 | `trigger_rules.py` | `all_success`, `one_success`, `all_done`, `none_failed`, `all_skipped` |
| 008 | `templating.py` | Jinja templates in operators, `{{ ds }}`, `{{ params }}`, `{{ macros }}` |
| 009 | `task_groups.py` | `TaskGroup` for organizing complex DAGs, nested groups, prefixed task IDs |
| 010 | `dynamic_tasks.py` | `.expand()`, `.partial()`, dynamic task mapping, map over lists |
| 011 | `sensors.py` | `TimeDeltaSensor`, poke mode vs reschedule mode |
| 012 | `retries_and_callbacks.py` | `retries`, `retry_delay`, `on_failure_callback`, `on_success_callback`, `on_retry_callback` |
| 013 | `custom_operators.py` | `BaseOperator` subclass from `airflow.sdk`, `execute()` method, custom fields |
| 014 | `assets.py` | `Asset` (formerly Dataset), producer DAG, consumer DAG, data-aware scheduling |
| 015 | `dag_dependencies.py` | `TriggerDagRunOperator`, `ExternalTaskSensor`, cross-DAG orchestration |
| 016 | `pools_and_priority.py` | `pool` parameter, `priority_weight`, resource-constrained execution |
| 017 | `variables_and_params.py` | `Variable.get()`/`set()`, DAG-level params with defaults |
| 018 | `short_circuit.py` | `ShortCircuitOperator`, conditional task skipping, downstream impact |
| 019 | `setup_teardown.py` | `as_teardown()` for guaranteed resource cleanup, setup-teardown lifecycle |
| 020 | `complex_pipeline.py` | Multi-stage ETL: extract -> validate -> transform -> load -> notify |

## Docker DAGs (021-032)

| # | DAG | Description |
|---|-----|-------------|
| 021 | `docker_hello.py` | Minimal `DockerOperator`, single container, Alpine image |
| 022 | `docker_python_script.py` | Run Python scripts in containers, install packages inline |
| 023 | `docker_volumes.py` | Bind mounts for sharing data between container steps |
| 024 | `docker_env_and_secrets.py` | Static env vars, Airflow-templated vars, config-driven tasks |
| 025 | `docker_pipeline.py` | Multi-step ETL: extract -> validate -> transform -> report, all in containers |
| 026 | `docker_custom_image.py` | BusyBox, Node.js, Ruby images, working directories, entrypoint args |
| 027 | `docker_network.py` | Bridge mode, host connectivity, DNS resolution inside containers |
| 028 | `docker_resources.py` | CPU/memory limits, resource-constrained container execution |
| 029 | `docker_xcom.py` | Passing data between Docker tasks via XCom (`retrieve_output`) |
| 030 | `docker_mixed_pipeline.py` | Combining DockerOperator + PythonOperator + BashOperator in one DAG |
| 031 | `docker_dynamic_images.py` | Dynamic task mapping across different Docker images with `.expand()` |
| 032 | `docker_compose_task.py` | Running `docker compose` and `docker run` from BashOperator |

## HTTP, SQL, SSH, Email, and Deferrable DAGs (033-042)

| # | DAG | Description |
|---|-----|-------------|
| 033 | `http_requests.py` | `HttpOperator` GET/POST to httpbin, query params, response logging |
| 034 | `http_sensor.py` | `HttpSensor` poke mode vs `deferrable=True` async mode |
| 035 | `postgres_queries.py` | `SQLExecuteQueryOperator` DDL, DML, templated SQL queries |
| 036 | `sql_pipeline.py` | Full SQL ETL: staging -> validate -> transform -> summary -> teardown |
| 037 | `generic_transfer.py` | SQL-based data transfer between tables with transformation |
| 038 | `email_notifications.py` | `EmailOperator` plain and HTML emails via Mailpit |
| 039 | `ssh_commands.py` | `SSHOperator` remote commands, scripts, file operations |
| 040 | `latest_only.py` | `LatestOnlyOperator` to skip tasks during backfills |
| 041 | `deferrable_sensors.py` | `deferrable=True` vs poke mode, triggerer-based async execution |
| 042 | `custom_deferrable.py` | Custom `BaseTrigger` + deferrable operator with `execute_complete` |

## Bash DAGs (043-046)

| # | DAG | Description |
|---|-----|-------------|
| 043 | `bash_basics.py` | Single commands, multi-line scripts, exit codes, `cwd`, output capture via XCom |
| 044 | `bash_environment.py` | `env` parameter, `append_env`, templated env vars, config profiles, XCom in env |
| 045 | `bash_scripting.py` | Functions, loops, conditionals, file I/O, arrays, text processing, data pipelines |
| 046 | `bash_templating.py` | Jinja macros in bash, `{{ ds }}`, `{{ params }}`, dynamic paths, control structures |

## Advanced Feature DAGs (047-057)

| # | DAG | Description |
|---|-----|-------------|
| 047 | `taskflow_decorators.py` | `@task.bash`, `@task.short_circuit`, TaskFlow decorator variants |
| 048 | `virtualenv_tasks.py` | `@task.virtualenv`, `PythonVirtualenvOperator`, isolated Python environments |
| 049 | `empty_and_branch_operators.py` | `EmptyOperator` (join/fork), `BranchPythonOperator` (classic branching) |
| 050 | `file_sensor.py` | `FileSensor` poke vs reschedule, file detection patterns |
| 051 | `advanced_retries.py` | `retry_exponential_backoff`, `execution_timeout`, DAG-level callbacks |
| 052 | `scheduling_features.py` | `depends_on_past`, `wait_for_downstream`, `max_active_runs`, cron expressions |
| 053 | `custom_timetable.py` | Custom `Timetable` patterns, weekday-only scheduling, business hours |
| 054 | `advanced_dynamic_mapping.py` | `expand_kwargs()` (zip-style), combined expand patterns |
| 055 | `multiple_assets.py` | Multi-outlet producers, AND-logic consumers, multiple `Asset` dependencies |
| 056 | `custom_hook_and_sensor.py` | Custom `BaseHook`, custom `BaseSensorOperator`, incremental processing |
| 057 | `parquet_aggregation.py` | Read Parquet, pandas aggregations (station/daily/crosstab), write CSV |

## DHIS2 Metadata Pipeline DAGs (058-062)

| # | DAG | Description |
|---|-----|-------------|
| 058 | `dhis2_org_units.py` | Fetch DHIS2 organisation units, flatten nested JSON, write CSV |
| 059 | `dhis2_data_elements.py` | Fetch DHIS2 data elements, categorize by domain/value type, write Parquet |
| 060 | `dhis2_indicators.py` | Fetch DHIS2 indicators, parse expressions with regex, complexity scoring, write CSV |
| 061 | `dhis2_org_unit_geometry.py` | Fetch org units with geometry, build GeoJSON FeatureCollection, write `.geojson` |
| 062 | `dhis2_combined_export.py` | Parallel fetch all three metadata types, multi-format export, combined summary |

## File-Based ETL Pipeline DAGs (063-067)

| # | DAG | Description |
|---|-----|-------------|
| 063 | `csv_landing_zone.py` | CSV landing zone -> validate -> transform (F->C) -> archive with timestamps |
| 064 | `json_event_ingestion.py` | JSON event files -> `pd.json_normalize()` -> Parquet conversion |
| 065 | `multi_file_batch.py` | Mixed CSV+JSON -> harmonize column names -> merge -> deduplicate |
| 066 | `error_handling_etl.py` | ETL with quarantine pattern: bad rows isolated, corrupt files dead-lettered |
| 067 | `incremental_file_processing.py` | JSON manifest tracks processed files, only new arrivals processed |

## Data Quality & Validation DAGs (068-072)

| # | DAG | Description |
|---|-----|-------------|
| 068 | `schema_validation.py` | Schema-as-code: column names, value types, nullability constraints |
| 069 | `statistical_checks.py` | Z-score outlier detection, physical bounds, distribution shift detection |
| 070 | `freshness_completeness.py` | File freshness, row counts, date gap detection, null rate thresholds |
| 071 | `cross_dataset_validation.py` | Referential integrity, value consistency, temporal consistency cross-checks |
| 072 | `quality_report.py` | Consolidated quality dashboard with scoring and recommendations |

## Alerting & SLA Monitoring DAGs (073-076)

| # | DAG | Description |
|---|-----|-------------|
| 073 | `sla_monitoring.py` | `execution_timeout`, task duration tracking, SLA breach reporting |
| 074 | `webhook_notifications.py` | POST JSON alerts to httpbin on start/success/failure with `trigger_rule` |
| 075 | `failure_escalation.py` | Retry -> warn -> alert -> recover with `on_retry_callback`/`on_failure_callback` |
| 076 | `pipeline_health_check.py` | Meta-monitoring: file existence, freshness, size checks, conditional alerting |

## DAG Testing Pattern DAGs (077-080)

| # | DAG | Description |
|---|-----|-------------|
| 077 | `testable_dag_pattern.py` | Thin DAG wiring with all logic in testable `etl_logic.py` module |
| 078 | `mock_api_pipeline.py` | API pipeline with mockable `fetch_api_data()` for `unittest.mock.patch` |
| 079 | `dag_test_runner.py` | `dag.test()` method and `__main__` pattern for local development |
| 080 | `parameterized_pipeline.py` | DAG `params` with defaults, config-driven pipelines, `run_conf` overrides |

## Weather & Climate API DAGs (081-086)

| # | DAG | Description |
|---|-----|-------------|
| 081 | `multi_city_forecast.py` | Multi-city 7-day forecast with `.expand()` dynamic mapping, cross-city comparison |
| 082 | `forecast_vs_historical.py` | Forecast vs archive accuracy analysis, MAE/RMSE, lead-time degradation |
| 083 | `air_quality_monitoring.py` | Air quality classification with WHO thresholds, health advisories |
| 084 | `marine_flood_risk.py` | Marine + flood composite risk index, weighted scoring, hazard assessment |
| 085 | `daylight_analysis.py` | Sunrise-Sunset API daylight hours, latitude-daylight correlation |
| 086 | `geocoding_weather.py` | Geocoding disambiguation, elevation enrichment, coordinate-driven weather |

## Geographic & Economic API DAGs (087-090)

| # | DAG | Description |
|---|-----|-------------|
| 087 | `country_demographics.py` | REST Countries nested JSON parsing, bridge tables, border graph |
| 088 | `world_bank_indicators.py` | World Bank pagination, multi-indicator join, Pearson correlation |
| 089 | `currency_analysis.py` | Frankfurter time series, log returns, rolling volatility, event detection |
| 090 | `country_weather_enrichment.py` | Cross-API enrichment: REST Countries + Open-Meteo + AQI |

## Geophysical & Environmental API DAGs (091-094)

| # | DAG | Description |
|---|-----|-------------|
| 091 | `earthquake_analysis.py` | USGS GeoJSON parsing, Gutenberg-Richter b-value, spatial binning |
| 092 | `carbon_intensity.py` | UK carbon intensity daily profiles, generation mix decomposition |
| 093 | `earthquake_weather_correlation.py` | Null hypothesis testing, spurious correlation data science lesson |
| 094 | `climate_trends.py` | 20-year archive in chunks, linear regression trends, seasonal decomposition |

## Global Health Indicator DAGs (095-097)

| # | DAG | Description |
|---|-----|-------------|
| 095 | `life_expectancy.py` | WHO GHO OData API, gender gap analysis, country rankings |
| 096 | `health_expenditure.py` | World Bank + WHO cross-org join, log-linear regression, efficiency ranking |
| 097 | `health_profile_star.py` | Star schema dimensional model, composite health index scoring |

## Advanced Multi-API DAGs (098-100)

| # | DAG | Description |
|---|-----|-------------|
| 098 | `multi_api_dashboard.py` | 6-API orchestration, staging layers, composite livability score |
| 099 | `api_quality_framework.py` | Quality framework on live API data, cross-source integrity checks |
| 100 | `full_etl_scd.py` | Capstone: SCD Type 2, surrogate keys, audit trail, full ETL pipeline |

## dbt Integration DAGs (101-102)

| # | DAG | Description |
|---|-----|-------------|
| 101 | `dbt_load_raw.py` | Load REST Countries + World Bank data into PostgreSQL for dbt |
| 102 | `dbt_transform.py` | Orchestrate `dbt run` and `dbt test` via BashOperator |

## S3 Object Storage DAGs (103-105)

| # | DAG | Description |
|---|-----|-------------|
| 103 | `minio_write.py` | Write/read Parquet to RustFS (S3-compatible) with boto3 |
| 104 | `minio_data_lake.py` | Bronze/silver/gold data lake pattern using RustFS |
| 105 | `object_storage.py` | Airflow `ObjectStoragePath` pathlib-like S3 API with connection-aware paths |

## Human-in-the-Loop DAGs (106)

| # | DAG | Description |
|---|-----|-------------|
| 106 | `human_in_the_loop.py` | `HITLEntryOperator`, `HITLOperator`, `ApprovalOperator`, `HITLBranchOperator` for weather data quality review |

## Variable-Driven Scheduling DAGs (107)

| # | DAG | Description |
|---|-----|-------------|
| 107 | `variable_driven_scheduling.py` | `Variable.get()` at parse time for dynamic schedule, REST API / UI schedule changes without code edits |

## Exercises

The `exercises/` directory contains 5 skeleton DAGs with `TODO` markers for hands-on
practice. See [exercises/README.md](exercises/README.md) for details.

## Streamlit Dashboard

Visualize DAG outputs interactively:

```bash
make dashboard    # opens http://localhost:8501
```

See [dashboard/README.md](dashboard/README.md) for details.

## Observability

The project includes a full observability stack:

- **Prometheus** -- scrapes Airflow StatsD metrics at [http://localhost:9090](http://localhost:9090)
- **Grafana** -- pre-built dashboard at [http://localhost:3000](http://localhost:3000) (admin / admin)
- **StatsD Exporter** -- bridges Airflow metrics to Prometheus format

## Prerequisites

- [Docker](https://www.docker.com/) -- runs Airflow and its dependencies in containers so you
  do not need to install anything system-wide. If you have never used Docker, install
  [Docker Desktop](https://www.docker.com/products/docker-desktop/) and make sure it is running.
- [uv](https://docs.astral.sh/uv/) -- a fast Python package manager. Install with
  `curl -LsSf https://astral.sh/uv/install.sh | sh` (or see their docs).

## Quick start

```bash
make sync    # install Python dev dependencies (linting, tests)
make run     # start Airflow + run all 107 examples
```

`make run` does everything: starts all services (PostgreSQL, Airflow, RustFS, Prometheus,
Grafana, and helper services), waits for Airflow to be ready, then runs all 104 DAGs one
by one. Press Ctrl+C to stop -- everything is cleaned up automatically.

## Web UIs

While `make run` is active, you can explore:

- **Airflow Dashboard**: [http://localhost:8081](http://localhost:8081) --
  see your DAGs, their status, logs, and task graphs
- **Mailpit**: [http://localhost:8025](http://localhost:8025) -- catches emails sent by the
  email notification examples (no real emails are sent)
- **RustFS Console**: [http://localhost:9001](http://localhost:9001) (airflow / airflow123) --
  browse S3-compatible object storage buckets
- **Grafana**: [http://localhost:3000](http://localhost:3000) -- Airflow metrics dashboard
- **Prometheus**: [http://localhost:9090](http://localhost:9090) -- metrics query interface

## Tests

Tests verify DAG structure and helper functions. They use an in-memory database (no Docker needed):

```bash
make test
```

## Links

- [Apache Airflow documentation](https://airflow.apache.org/docs/)
- [Airflow on GitHub](https://github.com/apache/airflow)
- [Airflow Docker provider](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/index.html)
- [Airflow HTTP provider](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/index.html)
- [Airflow PostgreSQL provider](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/index.html)
- [Airflow SSH provider](https://airflow.apache.org/docs/apache-airflow-providers-ssh/stable/index.html)

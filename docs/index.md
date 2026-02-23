# Airflow Examples

You know Python. You can write functions, loop through data, call APIs. But at some point you need
to run those scripts on a schedule, retry them when they fail, chain them together in the right
order, and know what happened when something breaks at 3 AM. That is what
[Apache Airflow](https://airflow.apache.org/) is for.

This project teaches Airflow 3.x through **107 progressive examples** -- starting from a single
"hello world" and building up to multi-API data pipelines. Every example is a real, runnable DAG.
You do not need to know Docker, Bash, SQL, or any Airflow concepts to start. The pages below
explain everything from scratch.

---

## Where to Start

### [Getting Started](getting-started.md)

What Bash, Docker, and operators are. Core Airflow concepts: DAGs, tasks, sensors, XComs,
connections, and the architecture behind `make run`.

### [DAG Basics](dag-basics.md)

Your first DAG, DAG parameters, task dependencies, scheduling (cron, presets, timedelta),
catchup/backfill, and data-aware scheduling with Assets.

### [Core Operators](operators.md)

Operator reference (Bash, Python, Docker, Sensors), BashOperator deep dive, TaskFlow decorator
variants (`@task.bash`, `@task.short_circuit`, `@task.virtualenv`), EmptyOperator,
BranchPythonOperator, and FileSensor.

### [Provider Operators](providers.md)

HTTP operators and sensors, SQL operators, email notifications, SSH commands,
LatestOnlyOperator, and deferrable operators with custom triggers.

### [Pipelines](pipelines.md)

Python ETL pipelines with `@task`, multi-source dynamic pipelines, error handling patterns,
and Docker pipelines (custom images, shared volumes, GPU workloads).

### [Advanced Concepts](advanced.md)

Retries with exponential backoff, scheduling features, custom timetables, advanced dynamic
task mapping, multiple asset dependencies, custom hooks/sensors, and incremental processing.

### [DAG Examples Reference](dag-reference.md)

Detailed walkthrough of all 32 core example DAGs (01--32), covering fundamentals, TaskFlow,
branching, sensors, task groups, dynamic mapping, assets, Docker operators, and more.

### [Data Pipelines](data-pipelines.md)

Parquet aggregation, DHIS2 metadata pipelines (organisation units, data elements, indicators,
GeoJSON geometry, parallel exports), and file-based ETL (CSV landing zones, JSON ingestion,
multi-file batch, error handling, incremental processing).

### [Quality & Testing](quality-testing.md)

Data quality dimensions (schema, accuracy, freshness, completeness, consistency), alerting
and SLA monitoring, webhook notifications, failure escalation, and DAG testing patterns
(testable architecture, mocking, `dag.test()`, parameterized pipelines).

### [API Pipelines](external-apis.md)

Weather and climate APIs (Open-Meteo forecast, air quality, marine, flood), geographic and
economic APIs (REST Countries, World Bank, Frankfurter), geophysical APIs (USGS earthquakes,
UK carbon intensity), global health indicators (WHO, World Bank), and advanced multi-API
data engineering (staging layers, quality on live data, SCD Type 2).

### [Integrations](integrations.md)

dbt integration, S3 object storage with RustFS (boto3 and ObjectStoragePath), Human-in-the-Loop
operators, and variable-driven scheduling.

### [Reference](reference.md)

Executors (Local, Celery, Kubernetes), Airflow 3.x migration notes, architecture diagrams,
infrastructure details, and external links.

### [REST API & CLI](cli.md)

Typer CLI wrapping the Airflow REST API for managing DAGs, connections, variables, and pools.

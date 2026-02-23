# Reference

## Executors: How Tasks Actually Run

You do not need to understand executors to use this project -- `LocalExecutor` is
configured automatically. This section is useful if you are deploying Airflow in
production and need to choose how to scale.

### LocalExecutor

- Tasks run as separate processes on the same machine as the scheduler
- Good for development and small-to-medium workloads
- Used in this project's `compose.yml`

### CeleryExecutor

- Tasks dispatched to a pool of Celery workers (can span multiple machines)
- Requires a message broker (Redis/RabbitMQ)
- Good for medium-to-large workloads with horizontal scaling

### KubernetesExecutor

- Each task runs in its own Kubernetes pod
- Maximum isolation: each task gets its own container, resources, and dependencies
- Good for heterogeneous workloads with varying resource requirements
- Tasks can use different Docker images

### Comparison

| Feature | LocalExecutor | CeleryExecutor | KubernetesExecutor |
|---------|--------------|----------------|-------------------|
| Setup complexity | Low | Medium | High |
| Scalability | Single machine | Multi-machine | Cluster-wide |
| Isolation | Process-level | Process-level | Container-level |
| Resource efficiency | High | Medium | Variable |
| Cold start | None | None | Pod spin-up time |
| Best for | Dev/small prod | Medium prod | Large/varied prod |

---

## Airflow 3.x Migration Notes

Airflow 3.0 (April 2025) introduced significant breaking changes from 2.x:

| 2.x | 3.x |
|-----|-----|
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.operators.bash import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.python import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `Dataset` | `Asset` |
| `execution_date` in context | `logical_date` in context |
| `schedule_interval="@daily"` | `schedule="@daily"` |
| SubDAGs | TaskGroups (SubDAGs removed) |

---

## Architecture Diagrams

### Project Infrastructure

```mermaid
graph TB
    subgraph Docker Compose
        PG[(PostgreSQL)]
        WS[Web Server :8081]
        SC[Scheduler]
        TR[Triggerer]
        HB[httpbin :8088]
        MP[Mailpit :8025]
        SSH[SSH Server :2222]
        RFS[RustFS :9000]
        SE[StatsD Exporter]
        PR[Prometheus :9090]
        GR[Grafana :3000]
    end

    SC --> PG
    WS --> PG
    TR --> PG
    SC --> HB
    SC --> MP
    SC --> SSH
    SC --> RFS
    SC --> SE
    SE --> PR
    PR --> GR
```

### Data Flow Pattern

```mermaid
graph LR
    API[Public API] --> F[Fetch Task]
    F --> T[Transform Task]
    T --> V[Validate Task]
    V --> L[Load Task]
    L --> CSV[CSV / Parquet]
    L --> PG[(PostgreSQL)]
    L --> S3[RustFS / S3]
```

### DAG 84: Marine + Flood Risk (Fan-out / Fan-in)

```mermaid
graph TD
    FM[fetch_marine] --> CMR[compute_marine_risk]
    FF[fetch_flood] --> CFR[compute_flood_risk]
    CMR --> CR[composite_risk]
    CFR --> CR
    CMR --> R[report]
    CFR --> R
    CR --> R
```

### DAG 91: Earthquake Analysis (Parallel Analysis Branches)

```mermaid
graph TD
    FE[fetch_earthquakes] --> PG[parse_geojson]
    PG --> MA[magnitude_analysis]
    PG --> SA[spatial_analysis]
    PG --> TA[temporal_analysis]
    MA --> R[report]
    SA --> R
    TA --> R
```

### DAG 98: Multi-API Dashboard (6-Source Fan-In)

```mermaid
graph TD
    RC[REST Countries] --> S[stage_raw]
    OM[Open-Meteo Forecast] --> S
    AQ[Air Quality API] --> S
    FR[Frankfurter API] --> S
    WB[World Bank API] --> S
    S --> I[integrate]
    I --> DM[dashboard_metrics]
    DM --> R[report]
```

### DAG 100: Capstone ETL (Diamond Dependencies)

```mermaid
graph TD
    EC[extract_countries] --> BS[build_staging]
    EI[extract_indicators] --> BS
    EC --> GSK[generate_surrogate_keys]
    GSK --> SCD[apply_scd_type2]
    SCD --> BF[build_fact_table]
    EI --> BF
    GSK --> BF
    BS --> AT[audit_trail]
    SCD --> AT
    BF --> AT
    SCD --> V[validate]
    BF --> V
    AT --> R[report]
    V --> R
    SCD --> R
```

### dbt Integration Flow

```mermaid
graph LR
    subgraph Airflow DAGs
        L[101: Load Raw]
        D[102: dbt Transform]
    end
    subgraph dbt Models
        STG[Staging Models]
        MART[Mart Models]
    end
    subgraph PostgreSQL
        RAW[raw_countries / raw_indicators]
        DIM[dim_country]
        FCT[fct_indicators]
        AGG[agg_nordic_summary]
    end
    L --> RAW
    RAW --> STG
    STG --> MART
    MART --> DIM
    MART --> FCT
    MART --> AGG
    D -.->|orchestrates| STG
    D -.->|orchestrates| MART
```

### DAG 105: ObjectStoragePath Pipeline

```mermaid
graph TD
    EB[ensure_bucket] --> WP[write_parquet]
    WP --> LF[list_files]
    WP --> RT[read_and_transform]
    WP --> CG[copy_to_gold]
    LF --> R[report]
    RT --> R
    CG --> R
```

### Data Lake Layers (RustFS)

```mermaid
graph LR
    API[APIs] --> B[Bronze Layer]
    B --> S[Silver Layer]
    S --> G[Gold Layer]

    subgraph RustFS Buckets
        B -->|Raw JSON| B1[airflow-data/bronze/]
        S -->|Cleaned Parquet| B2[airflow-data/silver/]
        G -->|Aggregated| B3[airflow-data/gold/]
    end
```

### DAG 106: HITL Weather Quality Review

```mermaid
graph TD
    FW[fetch_weather_data] --> AN[add_analyst_note]
    AN --> QR[choose_quality_rating]
    QR --> AP[approve_publication]
    AP --> CO[choose_output_format]
    CO --> EC[export_csv]
    CO --> EJ[export_json]
    CO --> EP[export_parquet]
    EC --> JE[join_exports]
    EJ --> JE
    EP --> JE
    JE --> PR[publish_report]

    style AN fill:#f9e79f
    style QR fill:#f9e79f
    style AP fill:#f9e79f
    style CO fill:#f9e79f
```

---

## Infrastructure

### How `make run` Works

Behind the scenes, `make run` does the following:

1. **Starts Docker containers** -- PostgreSQL (database), Airflow (scheduler + web server +
   triggerer), plus helper services (mock HTTP server, mail catcher, SSH server)
2. **Initializes Airflow** -- creates the database tables, the admin user, and default
   connections
3. **Runs all 107 DAGs** -- executes each one in order using `airflow dags test`
4. **Shows logs** -- tails the scheduler logs so you can watch what is happening
5. **Cleans up on Ctrl+C** -- stops and removes all containers

```bash
make run            # Does everything above
make stop           # Force-stop services if something goes wrong
```

### Running a Single DAG

If you want to run just one DAG (while services are running from `make run`):

```bash
# The pattern is: docker compose exec airflow-scheduler airflow dags test <dag_id> <date>
docker compose exec airflow-scheduler airflow dags test 01_hello_world 2025-01-01
```

### Testing Without Docker

Unit tests do not need Docker -- they use a lightweight in-memory database:

```bash
make test           # pytest with SQLite, no Docker needed
```

---

## Links

- [Apache Airflow documentation](https://airflow.apache.org/docs/)
- [Airflow on GitHub](https://github.com/apache/airflow)
- [Airflow 3.0 release notes](https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html)
- [Airflow providers index](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Docker provider](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/index.html)

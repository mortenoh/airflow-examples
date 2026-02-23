# Pipelines

## Python Pipelines

This is probably the section most relevant to you. The `@task` decorator lets you write
plain Python functions -- no operator classes, no boilerplate. Airflow automatically
tracks dependencies, passes return values between tasks, and handles retries.

### Simple ETL Pipeline

```python
from datetime import datetime
from airflow.sdk import DAG, task

@task
def extract() -> list[dict]:
    """Pull data from source system."""
    return [
        {"id": 1, "name": "Alice", "score": 85},
        {"id": 2, "name": "Bob", "score": 92},
    ]

@task
def transform(records: list[dict]) -> list[dict]:
    """Normalize scores to 0-1 range."""
    return [
        {**r, "score_normalized": r["score"] / 100}
        for r in records
    ]

@task
def load(records: list[dict]) -> None:
    """Write processed records to destination."""
    for r in records:
        print(f"Loading: {r}")

with DAG(
    dag_id="python_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    raw = extract()
    cleaned = transform(raw)
    load(cleaned)
```

### Multi-Source Pipeline with Dynamic Tasks

```python
@task
def get_sources() -> list[str]:
    return ["postgres", "mysql", "mongodb"]

@task
def extract_from(source: str) -> dict:
    print(f"Extracting from {source}")
    return {"source": source, "rows": 1000}

@task
def merge(datasets: list[dict]) -> dict:
    total = sum(d["rows"] for d in datasets)
    return {"total_rows": total, "sources": len(datasets)}

with DAG(dag_id="multi_source", ...):
    sources = get_sources()
    extracted = extract_from.expand(source=sources)  # Dynamic!
    merge(extracted)
```

### Pipeline with Error Handling

```python
@task
def extract_with_retry() -> dict:
    """Extract with built-in retry logic."""
    import requests
    resp = requests.get("https://api.example.com/data", timeout=30)
    resp.raise_for_status()
    return resp.json()

@task(retries=3, retry_delay=timedelta(minutes=1))
def transform_with_validation(data: dict) -> dict:
    """Transform with validation."""
    if not data.get("records"):
        raise ValueError("No records in response")
    return {"processed": len(data["records"])}
```

---

## Docker Pipelines

This is an intermediate topic. If you are new to Airflow, you can skip this section
entirely and come back later.

Docker pipelines run each task in its own container -- a mini-computer with exactly the
packages that task needs. This solves the "works on my machine" problem and avoids
dependency conflicts (e.g., when task A needs pandas 1.x and task B needs pandas 2.x).

### Why Docker Pipelines?

| Benefit | Description |
|---------|-------------|
| **Isolation** | Each task has its own Python version, packages, and system libraries |
| **Reproducibility** | Same container image runs identically everywhere |
| **No dependency conflicts** | Task A can use pandas 1.x while Task B uses pandas 2.x |
| **Language agnostic** | Run R, Julia, Rust, or any language in containers |
| **Production parity** | Same images used in dev, staging, and production |

### Pattern 1: Custom Image per Step

Build specialized images for each pipeline stage:

```dockerfile
# Dockerfile.extract
FROM python:3.12-slim
RUN pip install requests boto3
COPY scripts/extract.py /app/
WORKDIR /app
ENTRYPOINT ["python", "extract.py"]
```

```python
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(dag_id="docker_etl", ...):
    extract = DockerOperator(
        task_id="extract",
        image="my-pipeline/extract:latest",
        command="--source s3://bucket/raw --date {{ ds }}",
        mounts=[Mount(source="/data/shared", target="/data", type="bind")],
        auto_remove="success",
    )

    transform = DockerOperator(
        task_id="transform",
        image="my-pipeline/transform:latest",
        command="--input /data/raw.parquet --output /data/clean.parquet",
        mounts=[Mount(source="/data/shared", target="/data", type="bind")],
        auto_remove="success",
    )

    load = DockerOperator(
        task_id="load",
        image="my-pipeline/load:latest",
        command="--input /data/clean.parquet --table warehouse.clean_data",
        mounts=[Mount(source="/data/shared", target="/data", type="bind")],
        auto_remove="success",
    )

    extract >> transform >> load
```

### Pattern 2: Shared Volume Communication

Tasks pass data through shared mounted volumes:

```python
with DAG(dag_id="docker_shared_volume", ...):
    # All tasks mount the same host directory
    SHARED = [Mount(source="/tmp/pipeline/{{ ds }}", target="/data", type="bind")]

    step1 = DockerOperator(
        task_id="download",
        image="curlimages/curl",
        command="curl -o /data/input.json https://api.example.com/data",
        mounts=SHARED,
        auto_remove="success",
    )

    step2 = DockerOperator(
        task_id="process",
        image="python:3.12",
        command='python -c "import json; data=json.load(open(\'/data/input.json\')); ..."',
        mounts=SHARED,
        auto_remove="success",
    )

    step1 >> step2
```

### Pattern 3: Docker Compose for Multi-Container Tasks

For tasks requiring multiple services (e.g., a web app + database):

```python
# Use BashOperator to run docker compose for complex setups
integration_test = BashOperator(
    task_id="integration_test",
    bash_command=(
        "cd /opt/project && "
        "docker compose -f docker-compose.test.yml up --abort-on-container-exit --exit-code-from test"
    ),
)
```

### Pattern 4: GPU Workloads

```python
train = DockerOperator(
    task_id="train_model",
    image="nvidia/cuda:12.0-runtime",
    command="python train.py --epochs 100 --batch-size 32",
    device_requests=[DeviceRequest(count=-1, capabilities=[["gpu"]])],
    mem_limit="16g",
    auto_remove="success",
)
```

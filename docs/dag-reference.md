# DAG Examples Reference

Each section below describes one example DAG. They are numbered in learning order --
start at 01 and work your way through. DAGs 01-10 cover the fundamentals every Airflow
user needs. The later sections (Docker, SQL, APIs) are independent and can be explored
in any order based on what interests you.

### 01 -- Hello World

The simplest possible DAG. Two `BashOperator` tasks connected with `>>`:

```python
hello >> date
```

See: `dags/01_hello_world.py`

### 02 -- PythonOperator

Run Python callables as tasks with `op_args` and `op_kwargs`:

```python
PythonOperator(
    task_id="greet",
    python_callable=greet,
    op_args=["Airflow"],
    op_kwargs={"greeting": "Welcome"},
)
```

See: `dags/02_python_operator.py`

### 03 -- Task Dependencies

All dependency patterns: `>>`, `<<`, `chain()`, `cross_downstream()`, fan-in, fan-out:

```python
start >> middle >> end
fan_out_source >> [fan_out_a, fan_out_b, fan_out_c]
[fan_out_a, fan_out_b, fan_out_c] >> fan_in_sink
chain(chain_a, chain_b, chain_c)
cross_downstream([cross_a, cross_b], [cross_x, cross_y])
```

See: `dags/03_task_dependencies.py`

### 04 -- TaskFlow API

The `@task` decorator turns Python functions into Airflow tasks with automatic XCom passing:

```python
@task
def extract() -> dict:
    return {"values": [1, 2, 3]}

@task
def transform(data: dict) -> dict:
    return {"values": [v * 2 for v in data["values"]]}

raw = extract()
processed = transform(raw)  # Automatic XCom!
```

See: `dags/04_taskflow_api.py`

### 05 -- XComs

Manual cross-task communication with `ti.xcom_push()` and `ti.xcom_pull()`:

```python
ti.xcom_push(key="greeting", value="Hello!")
value = ti.xcom_pull(task_ids="push_task", key="greeting")
```

See: `dags/05_xcoms.py`

### 06 -- Branching

Conditional execution paths with `@task.branch`:

```python
@task.branch
def choose_branch():
    if condition:
        return "path_a"
    return "path_b"
```

See: `dags/06_branching.py`

### 07 -- Trigger Rules

Control when tasks run based on upstream task states:

| Rule | Runs When |
|------|-----------|
| `all_success` | All upstream tasks succeeded (default) |
| `one_success` | At least one upstream task succeeded |
| `all_done` | All upstream tasks completed (any state) |
| `none_failed` | No upstream task failed (skipped is ok) |
| `all_skipped` | All upstream tasks were skipped |

See: `dags/07_trigger_rules.py`

### 08 -- Jinja Templating

Airflow renders Jinja templates in operator parameters at execution time:

```python
BashOperator(
    task_id="templated",
    bash_command='echo "Date: {{ ds }}, Param: {{ params.env }}"',
)
```

**Commonly used template variables:**

| Variable | Description |
|----------|-------------|
| `{{ ds }}` | Logical date as `YYYY-MM-DD` |
| `{{ logical_date }}` | Full datetime object |
| `{{ params.key }}` | DAG parameter value |
| `{{ macros.ds_add(ds, 7) }}` | Date arithmetic |
| `{{ dag.dag_id }}` | Current DAG ID |
| `{{ task.task_id }}` | Current task ID |
| `{{ run_id }}` | Current DAG run ID |

See: `dags/08_templating.py`

### 09 -- Task Groups

Organize complex DAGs into collapsible groups in the UI:

```python
with TaskGroup("extract") as extract_group:
    extract_users = BashOperator(task_id="users", ...)
    extract_orders = BashOperator(task_id="orders", ...)
    # Task IDs become "extract.users", "extract.orders"
```

Supports nesting: `transform.clean.users`, `transform.enrich.join`.

See: `dags/09_task_groups.py`

### 10 -- Dynamic Task Mapping

Create tasks dynamically at runtime based on data:

```python
@task
def generate_files() -> list[str]:
    return ["file1.csv", "file2.csv", "file3.csv"]

@task
def process_file(filename: str) -> dict:
    return {"file": filename, "rows": 100}

files = generate_files()
results = process_file.expand(filename=files)  # Creates 3 mapped tasks
```

See: `dags/10_dynamic_tasks.py`

### 11 -- Sensors

Wait for external conditions before proceeding:

- **TimeDeltaSensor**: Wait for a time offset from the logical date
- **FileSensor**: Wait for a file to appear on disk
- **poke mode**: Holds worker slot, checks periodically
- **reschedule mode**: Releases worker between checks

See: `dags/11_sensors.py`

### 12 -- Retries and Callbacks

Configure automatic retries and hook into task lifecycle events:

```python
PythonOperator(
    task_id="flaky_task",
    python_callable=my_func,
    retries=3,
    retry_delay=timedelta(minutes=5),
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    on_retry_callback=log_retry,
)
```

See: `dags/12_retries_and_callbacks.py`

### 13 -- Custom Operators

Build reusable operators by subclassing `BaseOperator`:

```python
from airflow.sdk import BaseOperator

class SquareOperator(BaseOperator):
    def __init__(self, number: int, **kwargs):
        super().__init__(**kwargs)
        self.number = number

    def execute(self, context):
        result = self.number ** 2
        return result  # Automatically pushed to XCom
```

See: `dags/13_custom_operators.py`

### 14 -- Assets

Data-aware scheduling with `Asset` (Airflow 3.x):

```python
weather = Asset("s3://bucket/weather.parquet")

# Producer declares outlets
produce = PythonOperator(outlets=[weather], ...)

# Consumer scheduled by asset update
with DAG(schedule=[weather], ...):
    consume = PythonOperator(...)
```

See: `dags/14_assets.py`

### 15 -- DAG Dependencies

Trigger other DAGs with `TriggerDagRunOperator`:

```python
TriggerDagRunOperator(
    task_id="trigger",
    trigger_dag_id="downstream_dag",
    wait_for_completion=True,
)
```

See: `dags/15_dag_dependencies.py`

### 16 -- Pools and Priority

Resource management for concurrent task execution:

```python
BashOperator(
    task_id="db_query",
    pool="database_pool",       # Limits concurrent DB connections
    priority_weight=10,         # Higher = executed first in queue
    ...)
```

See: `dags/16_pools_and_priority.py`

### 17 -- Variables and Params

- **Variables**: Global key-value store persisted in the metadata DB
- **Params**: Per-run configuration with type validation and defaults

```python
# Variables (global, persistent)
from airflow.models import Variable
Variable.set("api_url", "https://api.example.com")
url = Variable.get("api_url")

# Params (per-run, with defaults)
with DAG(params={"env": "staging", "batch_size": 100}, ...):
    ...
```

See: `dags/17_variables_and_params.py`

### 18 -- Short Circuit

Skip downstream tasks conditionally:

```python
check = ShortCircuitOperator(
    task_id="check",
    python_callable=lambda: file_exists("/data/input.csv"),
)
check >> expensive_task  # Skipped if check returns False
```

See: `dags/18_short_circuit.py`

### 19 -- Setup and Teardown

Resource lifecycle management ensuring cleanup always runs:

```python
resource = create_resource()    # Setup
work = use_resource(resource)   # Work
cleanup = cleanup_resource()    # Teardown (runs even if work fails)

resource >> work >> cleanup
```

See: `dags/19_setup_teardown.py`

### 20 -- Complex Pipeline

Multi-stage ETL combining TaskGroups, dynamic mapping, and callbacks:

```
start -> [extract_group] -> [transform_group] -> [load_group] -> [notify_group] -> end
```

See: `dags/20_complex_pipeline.py`

### 21 -- Docker Hello World

The simplest DockerOperator DAG. Runs commands inside Alpine containers:

```python
from airflow.providers.docker.operators.docker import DockerOperator

hello = DockerOperator(
    task_id="hello",
    image="alpine:3.20",
    command='echo "Hello from inside a Docker container!"',
    auto_remove="success",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
)
```

**Key parameters every DockerOperator needs:**

| Parameter | Description |
|-----------|-------------|
| `image` | Docker image to pull and run |
| `command` | Command to execute inside the container |
| `auto_remove` | `"success"` removes container after success, `"force"` always removes |
| `docker_url` | Docker daemon socket path |
| `network_mode` | `"bridge"` (isolated) or `"host"` (shares host network) |

See: `dags/21_docker_hello.py`

### 22 -- Docker Python Scripts

Run Python code inside containers. Demonstrates inline scripts, data processing,
and installing packages at runtime:

```python
# Run inline Python
DockerOperator(
    task_id="inline_script",
    image="python:3.12-slim",
    command='python -c "import sys; print(f\'Python {sys.version}\')"',
    ...
)

# Install packages and use them
DockerOperator(
    task_id="with_package",
    image="python:3.12-slim",
    command='bash -c "pip install humanize && python -c \'import humanize; print(humanize.naturalsize(1e9))\'"',
    ...
)
```

See: `dags/22_docker_python_script.py`

### 23 -- Docker Volumes

Bind mounts let containers share data through the host filesystem. This is the
primary pattern for multi-step Docker pipelines:

```python
from docker.types import Mount

SHARED_MOUNT = Mount(source="/tmp/airflow-demo", target="/data", type="bind")

# Step 1: Write data
create_data = DockerOperator(
    task_id="create_data",
    image="alpine:3.20",
    command='sh -c "echo \'Alice,85\' > /data/scores.csv"',
    mounts=[SHARED_MOUNT],
    ...
)

# Step 2: Process data (reads from same mount)
process_data = DockerOperator(
    task_id="process_data",
    image="python:3.12-slim",
    command='python -c "import csv; rows = list(csv.reader(open(\'/data/scores.csv\')))"',
    mounts=[SHARED_MOUNT],
    ...
)
```

**Mount types:**

| Type | Description | Use Case |
|------|-------------|----------|
| `bind` | Maps a host directory into the container | Sharing data between steps |
| `volume` | Docker-managed named volume | Persistent data across container restarts |
| `tmpfs` | In-memory filesystem | Scratch space, sensitive data |

See: `dags/23_docker_volumes.py`

### 24 -- Docker Environment Variables

Pass configuration to containers via environment variables. Supports both static values
and Airflow template-rendered values:

```python
# Static env vars
DockerOperator(
    task_id="static_env",
    image="alpine:3.20",
    command='sh -c "echo APP_ENV=$APP_ENV"',
    environment={
        "APP_ENV": "staging",
        "LOG_LEVEL": "INFO",
    },
    ...
)

# Templated env vars (Airflow injects values at runtime)
DockerOperator(
    task_id="templated_env",
    image="alpine:3.20",
    command='sh -c "echo EXECUTION_DATE=$EXECUTION_DATE"',
    environment={
        "EXECUTION_DATE": "{{ ds }}",
        "DAG_ID": "{{ dag.dag_id }}",
    },
    ...
)
```

See: `dags/24_docker_env_and_secrets.py`

### 25 -- Docker ETL Pipeline

A complete 5-stage ETL pipeline where every stage runs in its own container.
Demonstrates a realistic pattern: extract raw data, validate, transform, report,
and cleanup -- all connected through a shared volume:

```
extract (python:3.12) -> validate (python:3.12) -> transform (python:3.12) -> report (python:3.12) -> cleanup (alpine)
```

Each stage reads input from and writes output to `/pipeline/` via a bind mount.

See: `dags/25_docker_pipeline.py`

### 26 -- Docker Custom Images

Run tasks in different language runtimes. Demonstrates BusyBox, Node.js, Ruby,
and custom working directory configuration:

```python
# Node.js task
DockerOperator(
    task_id="nodejs_task",
    image="node:22-alpine",
    command='node -e "console.log(\'Hello from Node.js\')"',
    ...
)

# Ruby task
DockerOperator(
    task_id="ruby_task",
    image="ruby:3.3-alpine",
    command='ruby -e "puts \'Hello from Ruby\'"',
    ...
)

# Custom working directory
DockerOperator(
    task_id="workdir_task",
    image="alpine:3.20",
    command='sh -c "pwd && ls -la"',
    working_dir="/opt",
    ...
)
```

See: `dags/26_docker_custom_image.py`

### 27 -- Docker Networking

Explore container networking: bridge mode isolation, host connectivity checks,
and DNS resolution inside containers:

```python
# Bridge mode: container gets its own network namespace
DockerOperator(
    task_id="bridge_mode",
    image="alpine:3.20",
    command='sh -c "hostname -i && cat /etc/resolv.conf"',
    network_mode="bridge",
    ...
)
```

**Network modes:**

| Mode | Description |
|------|-------------|
| `bridge` | Default. Container gets its own IP, isolated from host |
| `host` | Container shares the host's network stack |
| `none` | No networking |
| `container:<id>` | Share another container's network namespace |

See: `dags/27_docker_network.py`

### 28 -- Docker Resource Constraints

Set CPU and memory limits to prevent containers from consuming all host resources:

```python
# Memory-limited task
DockerOperator(
    task_id="memory_limited",
    image="python:3.12-slim",
    command='python -c "data = list(range(100000))"',
    mem_limit="128m",
    ...
)

# CPU-limited task
DockerOperator(
    task_id="cpu_limited",
    image="python:3.12-slim",
    command='python -c "sum(i**2 for i in range(500000))"',
    cpus=0.5,
    ...
)
```

**Resource parameters:**

| Parameter | Description | Example |
|-----------|-------------|---------|
| `mem_limit` | Maximum memory | `"128m"`, `"2g"` |
| `cpus` | CPU core limit (fractional) | `0.5`, `2.0` |
| `mem_reservation` | Soft memory limit | `"64m"` |

See: `dags/28_docker_resources.py`

### 29 -- Docker XCom

Pass data between Docker containers using XCom. The container's stdout is captured
and pushed to XCom when `retrieve_output=True`:

```python
# Docker task pushes last stdout line to XCom
generate = DockerOperator(
    task_id="generate_data",
    image="python:3.12-slim",
    command='python -c "import json; print(json.dumps({\'values\': [10, 20, 30]}))"',
    retrieve_output=True,
    retrieve_output_path="/tmp/xcom_output",
    ...
)

# Python task reads the XCom value
def show_output(**context):
    result = context["ti"].xcom_pull(task_ids="generate_data")
    print(f"Docker output: {result}")
```

See: `dags/29_docker_xcom.py`

### 30 -- Docker Mixed Pipeline

Real-world DAGs often mix execution modes. Some tasks need container isolation
(specific dependencies, language runtimes), while others run fine natively:

```
DockerOperator (extract) -> PythonOperator (validate) -> BashOperator (check) -> DockerOperator (transform) -> PythonOperator (aggregate)
```

**When to use each:**

| Operator | Best For |
|----------|----------|
| `DockerOperator` | Tasks needing specific deps, language isolation, reproducibility |
| `PythonOperator` | Lightweight Python logic, validation, aggregation |
| `BashOperator` | Quick system checks, simple shell commands |

See: `dags/30_docker_mixed_pipeline.py`

### 31 -- Docker Dynamic Images

Dynamic task mapping with DockerOperator. Run the same command across different
container images in parallel:

```python
@task
def list_images() -> list[str]:
    return ["python:3.12-slim", "python:3.11-slim", "node:22-alpine", "ruby:3.3-alpine"]

# .partial() sets shared params, .expand() maps over the variable one
version_check = DockerOperator.partial(
    task_id="version_check",
    command='sh -c "uname -a"',
    auto_remove="success",
    ...
).expand(image=list_images())
```

This creates 4 mapped task instances at runtime, one per image.

See: `dags/31_docker_dynamic_images.py`

### 32 -- Docker Compose from Airflow

Use BashOperator to run `docker compose` and `docker run` commands. Useful for
orchestrating multi-container setups or running integration tests:

```python
# Run docker compose for integration tests
BashOperator(
    task_id="integration_test",
    bash_command="docker compose -f test.yml up --abort-on-container-exit",
)

# Run a one-off container
BashOperator(
    task_id="docker_run",
    bash_command='docker run --rm alpine:3.20 echo "Hello"',
)
```

See: `dags/32_docker_compose_task.py`

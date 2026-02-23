# Core Operators

## Operator Reference

### Core Operators

Operators are the building blocks of every DAG. Each one knows how to do one thing --
run a shell command, call a Python function, send an email. You configure them with
parameters and Airflow handles the rest (scheduling, retries, logging).

All of these come from `airflow.providers.standard`:

#### BashOperator

Runs a command in the terminal (just like typing it in a shell). The command can be
anything: a script, a one-liner, even `python my_script.py`:

```python
from airflow.providers.standard.operators.bash import BashOperator

task = BashOperator(
    task_id="run_script",
    bash_command="python /opt/scripts/etl.py --date {{ ds }}",
    env={"API_KEY": "{{ var.value.api_key }}"},
)
```

#### PythonOperator

Calls a regular Python function. If you know Python, this will feel the most natural.
(The `@task` decorator is the modern shorthand for this -- more on that later.)

```python
from airflow.providers.standard.operators.python import PythonOperator

def my_function(name, **context):
    print(f"Hello {name}, running for {context['ds']}")

task = PythonOperator(
    task_id="python_task",
    python_callable=my_function,
    op_args=["world"],
)
```

#### ShortCircuitOperator

Skips all downstream tasks if the callable returns `False`:

```python
from airflow.providers.standard.operators.python import ShortCircuitOperator

def check_data_exists():
    return os.path.exists("/data/input.csv")

check = ShortCircuitOperator(
    task_id="check",
    python_callable=check_data_exists,
)

check >> process >> load  # process and load are skipped if check returns False
```

### Docker Operators

Remember the Docker explanation from earlier? Docker operators let you run a task
inside a container -- a throwaway mini-computer with exactly the packages you need.
This is useful when your task needs libraries that conflict with Airflow's own
dependencies, or when you want perfect reproducibility.

You do not need Docker operators to use Airflow. They are covered in DAGs 21-32 and
are an intermediate topic. Feel free to skip this section on your first read.

#### DockerOperator

Run a command inside a Docker container:

```python
from airflow.providers.docker.operators.docker import DockerOperator

# Run a Python script inside a container
process_data = DockerOperator(
    task_id="process_data",
    image="python:3.12-slim",
    command="python /scripts/process.py",
    mounts=[
        Mount(source="/host/data", target="/data", type="bind"),
        Mount(source="/host/scripts", target="/scripts", type="bind"),
    ],
    auto_remove="success",
    docker_url="unix://var/run/docker.sock",
)

# Run a custom image with environment variables
train_model = DockerOperator(
    task_id="train_model",
    image="my-ml-pipeline:latest",
    command="python train.py --epochs 50",
    environment={
        "DATA_PATH": "/data/training",
        "MODEL_OUTPUT": "/models/latest",
    },
    mounts=[
        Mount(source="/host/data", target="/data", type="bind"),
        Mount(source="/host/models", target="/models", type="bind"),
    ],
    auto_remove="success",
)
```

**Key parameters:**

| Parameter | Description |
|-----------|-------------|
| `image` | Docker image to use |
| `command` | Command to run inside the container |
| `environment` | Environment variables passed to the container |
| `mounts` | Volume mounts (bind mounts, named volumes) |
| `auto_remove` | Remove container after completion (`"success"`, `"force"`, `"never"`) |
| `docker_url` | Docker daemon URL (`unix://var/run/docker.sock` for local) |
| `network_mode` | Docker network mode (`"bridge"`, `"host"`, custom network name) |
| `cpus` | CPU limit for the container |
| `mem_limit` | Memory limit (e.g., `"512m"`, `"2g"`) |

#### Complete Docker Pipeline Example

```python
from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

with DAG(
    dag_id="docker_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    # Step 1: Download data using a curl container
    download = DockerOperator(
        task_id="download",
        image="curlimages/curl:latest",
        command="curl -o /data/raw.csv https://example.com/data.csv",
        mounts=[Mount(source="/tmp/pipeline", target="/data", type="bind")],
        auto_remove="success",
    )

    # Step 2: Process data using a Python container
    process = DockerOperator(
        task_id="process",
        image="python:3.12-slim",
        command='pip install pandas && python -c "import pandas as pd; df = pd.read_csv(\'/data/raw.csv\')"',
        mounts=[Mount(source="/tmp/pipeline", target="/data", type="bind")],
        auto_remove="success",
    )

    # Step 3: Generate report using a custom image
    report = DockerOperator(
        task_id="report",
        image="my-reporting:latest",
        command="python generate_report.py",
        mounts=[Mount(source="/tmp/pipeline", target="/data", type="bind")],
        environment={"REPORT_FORMAT": "html"},
        auto_remove="success",
    )

    download >> process >> report
```

### Sensors

A sensor is a task that *waits* instead of *doing*. "Wait until this file exists", "wait until
this API is reachable", "wait 10 minutes." Once the condition is met, the next task runs:

```python
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from datetime import timedelta

# Wait for a file to appear
wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/data/input.csv",
    poke_interval=30,       # Check every 30 seconds
    timeout=3600,           # Give up after 1 hour
    mode="reschedule",      # Free worker slot between pokes
)

# Wait for a time offset from the logical date
wait_10min = TimeDeltaSensor(
    task_id="wait_10min",
    delta=timedelta(minutes=10),
    mode="poke",            # Keep worker slot (faster, uses resources)
)
```

**Poke vs Reschedule mode:**

| Mode | Behavior | Best For |
|------|----------|----------|
| `poke` | Holds worker slot, checks periodically | Short waits, frequent checks |
| `reschedule` | Releases worker, re-schedules check | Long waits, resource-conscious |

### Cross-DAG Operators

```python
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# Trigger another DAG
trigger = TriggerDagRunOperator(
    task_id="trigger_downstream",
    trigger_dag_id="downstream_dag",
    wait_for_completion=True,
    reset_dag_run=True,
)
```

---

## BashOperator Deep Dive

`BashOperator` runs a terminal command as an Airflow task. If you have ever typed a command
in a terminal (like `ls`, `python script.py`, or `curl https://api.example.com`), you already
know enough to use it. This section covers the details for when you want to do more than
simple one-liners.

### Import

```python
from airflow.providers.standard.operators.bash import BashOperator
```

### Basic Usage

```python
# Single command
hello = BashOperator(
    task_id="hello",
    bash_command='echo "Hello from Airflow"',
)

# Multi-line script
script = BashOperator(
    task_id="script",
    bash_command="""
        echo "Step 1: check system"
        date '+%Y-%m-%d %H:%M:%S'
        hostname
        echo "Step 2: done"
    """,
)
```

### Key Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `bash_command` | `str` | Shell command or script to execute. Supports Jinja templates |
| `env` | `dict` | Environment variables passed to the subprocess |
| `append_env` | `bool` | If `True`, merge `env` into existing env instead of replacing it |
| `cwd` | `str` | Working directory for the command |
| `skip_on_exit_code` | `int/list` | Mark task as SKIPPED (not FAILED) for these exit codes |
| `do_xcom_push` | `bool` | Push last stdout line to XCom (default: `True`) |
| `output_encoding` | `str` | Encoding for stdout/stderr (default: `utf-8`) |

### Working Directory

```python
# Run command in a specific directory
process_data = BashOperator(
    task_id="process_data",
    bash_command='echo "Working in: $(pwd)" && ls -la',
    cwd="/tmp/data",
)
```

### Exit Codes and Skip Logic

BashOperator fails the task on non-zero exit codes. Use `skip_on_exit_code` for conditional
execution:

```python
# Skip task (don't fail) when data file doesn't exist
check_data = BashOperator(
    task_id="check_data",
    bash_command="""
        if [ ! -f /tmp/data.csv ]; then
            echo "No data file found -- skipping"
            exit 99
        fi
        echo "Data file found"
    """,
    skip_on_exit_code=99,
)
```

See: `dags/43_bash_basics.py`

### Environment Variables

Use the `env` parameter to pass variables. With `append_env=True`, custom variables are merged into
the existing environment (preserving PATH, HOME, etc.):

```python
process = BashOperator(
    task_id="process",
    bash_command="""
        echo "Stage: $PIPELINE_STAGE"
        echo "Date:  $EXEC_DATE"
        echo "PATH still works: $(which python)"
    """,
    env={
        "PIPELINE_STAGE": "transform",
        "EXEC_DATE": "{{ ds }}",         # Jinja template in env value
    },
    append_env=True,
)
```

Without `append_env=True`, the `env` dict **replaces** the entire environment -- PATH, HOME,
USER, etc. are all lost. Always use `append_env=True` unless you need a completely clean
environment.

See: `dags/44_bash_environment.py`

### XCom Output Capture

By default (`do_xcom_push=True`), BashOperator pushes the **last line** of stdout to XCom. Pull it
in downstream tasks:

```python
# Task 1: produce output (last line is captured)
produce = BashOperator(
    task_id="produce",
    bash_command="""
        echo "Processing..."
        echo "42"
    """,
)

# Task 2: consume via Jinja template
consume = BashOperator(
    task_id="consume",
    bash_command='echo "Got: {{ ti.xcom_pull(task_ids=\'produce\') }}"',
)

produce >> consume
```

### Advanced Scripting Patterns

BashOperator supports full bash scripting -- functions, loops, arrays, error handling:

```python
# Strict mode with functions
pipeline = BashOperator(
    task_id="pipeline",
    bash_command="""
        set -euo pipefail

        log() {
            echo "[$(date '+%H:%M:%S')] $*"
        }

        WORKDIR=$(mktemp -d)
        trap 'rm -rf "$WORKDIR"' EXIT

        log "Generating data"
        for i in $(seq 1 10); do
            echo "$i,$((RANDOM % 30))" >> "$WORKDIR/data.csv"
        done

        log "Processing $(wc -l < "$WORKDIR/data.csv") records"
        log "Done"
    """,
)
```

#### Error Handling

| Pattern | Purpose |
|---------|---------|
| `set -e` | Exit on first error |
| `set -u` | Error on undefined variables |
| `set -o pipefail` | Catch failures in pipes |
| `trap 'cleanup' EXIT` | Run cleanup on exit |
| `cmd \|\| true` | Ignore command failure |
| `cmd \|\| exit 99` | Fail with specific exit code |

#### File I/O

```python
file_task = BashOperator(
    task_id="file_task",
    bash_command="""
        WORKDIR=$(mktemp -d)

        # Write CSV data
        cat > "$WORKDIR/data.csv" << 'EOF'
        station,temp_c,humidity
        oslo_01,12.5,68.0
        bergen_01,9.8,82.0
EOF

        # Process
        tail -n +2 "$WORKDIR/data.csv" | while IFS=',' read -r station temp hum; do
            echo "Station: $station, Temp: $temp"
        done

        rm -rf "$WORKDIR"
    """,
)
```

See: `dags/45_bash_scripting.py`

### Jinja Templates in BashOperator

`bash_command` is a Jinja-templated field. You can use all Airflow macros, params, and even
Jinja control structures:

```python
# Date arithmetic
dates = BashOperator(
    task_id="dates",
    bash_command="""
        echo "Today:     {{ ds }}"
        echo "Yesterday: {{ macros.ds_add(ds, -1) }}"
        echo "Year:      {{ logical_date.strftime('%Y') }}"
    """,
)

# Dynamic paths from params
paths = BashOperator(
    task_id="paths",
    bash_command="""
        INPATH="/data/raw/{{ ds_nodash }}/{{ params.station }}"
        OUTPATH="/data/processed/{{ logical_date.strftime('%Y/%m') }}"
        echo "Input:  $INPATH"
        echo "Output: $OUTPATH"
    """,
)

# Jinja for-loop generates bash at render time
generate = BashOperator(
    task_id="generate",
    bash_command="""
        {% for city in ["oslo", "bergen", "tromso"] %}
        echo "Processing: {{ city }}"
        {% endfor %}
    """,
)
```

#### Common Template Variables

| Variable | Example | Description |
|----------|---------|-------------|
| `{{ ds }}` | `2024-01-01` | Execution date (YYYY-MM-DD) |
| `{{ ds_nodash }}` | `20240101` | Execution date without dashes |
| `{{ logical_date }}` | datetime object | Full datetime object |
| `{{ run_id }}` | `manual__2024...` | DAG run identifier |
| `{{ dag.dag_id }}` | `46_bash_templating` | DAG ID |
| `{{ task.task_id }}` | `core_macros` | Task ID |
| `{{ params.key }}` | user-defined | DAG parameter value |
| `{{ macros.ds_add(ds, -1) }}` | `2023-12-31` | Date arithmetic |
| `{{ ti.xcom_pull(...) }}` | any | Pull XCom from another task |

See: `dags/46_bash_templating.py`

---

## TaskFlow Decorator Variants

Beyond the basic `@task` decorator, Airflow 3.x provides specialized decorators that combine the
convenience of TaskFlow with specific operator behavior.

### @task.bash

Runs a bash command returned by a Python function. The function returns a string that Airflow
executes as a shell command:

```python
from airflow.sdk import task

@task.bash
def get_system_info() -> str:
    """Return a bash command string to execute."""
    return 'echo "Hostname: $(hostname)" && date'

@task.bash
def create_report() -> str:
    """Dynamically construct a bash command."""
    output_dir = "/tmp/reports"
    return f'mkdir -p {output_dir} && echo "Report generated" > {output_dir}/status.txt'
```

### @task.short_circuit

Returns `True` or `False`. If `False`, all downstream tasks are skipped (not failed):

```python
@task.short_circuit
def check_data_available() -> bool:
    """Skip downstream if no data is ready."""
    data_exists = True  # Check your data source
    return data_exists

# Downstream tasks only run if check returns True
check = check_data_available()
process = process_data()
check >> process
```

### @task.virtualenv

Runs a task in an isolated Python virtual environment with specified packages. The function
must be self-contained (no imports from the DAG file):

```python
@task.virtualenv(requirements=["requests>=2.31"])
def fetch_api_data() -> str:
    """Fetch data using requests (installed in a temporary virtualenv)."""
    import requests
    resp = requests.get("https://api.example.com/data", timeout=10)
    return f"status={resp.status_code}"
```

The classic operator form:

```python
from airflow.providers.standard.operators.python import PythonVirtualenvOperator

def compute_stats() -> str:
    import numpy as np
    data = np.array([12.5, 13.1, 9.8])
    return f"mean={np.mean(data):.2f}"

stats = PythonVirtualenvOperator(
    task_id="compute_stats",
    python_callable=compute_stats,
    requirements=["numpy>=1.26"],
)
```

See: `dags/47_taskflow_decorators.py`, `dags/48_virtualenv_tasks.py`

---

## EmptyOperator and BranchPythonOperator

### EmptyOperator

`EmptyOperator` (the Airflow 3.x replacement for `DummyOperator`) is a no-op task used as a
structural element: join points after branching, explicit start/end markers, or fork points.

```python
from airflow.providers.standard.operators.empty import EmptyOperator

start = EmptyOperator(task_id="start")
join = EmptyOperator(
    task_id="join",
    trigger_rule="none_failed_min_one_success",
)
end = EmptyOperator(task_id="end")

start >> [branch_a, branch_b] >> join >> end
```

### BranchPythonOperator (Classic)

The operator-style alternative to `@task.branch`. The callable returns one or more task IDs
to execute:

```python
from airflow.providers.standard.operators.python import BranchPythonOperator

def choose_path(**context) -> str:
    day = context["logical_date"].day
    if day <= 15:
        return "first_half"
    return "second_half"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=choose_path,
)

# Return a list to execute multiple branches simultaneously:
def choose_validations(**context) -> list[str]:
    return ["validate_schema", "validate_values"]
```

See: `dags/49_empty_and_branch_operators.py`

---

## FileSensor

`FileSensor` waits for a file to appear at a specified path before proceeding:

```python
from airflow.providers.standard.sensors.filesystem import FileSensor

wait = FileSensor(
    task_id="wait_for_data",
    filepath="/data/incoming/weather.csv",
    poke_interval=10,       # Check every 10 seconds
    timeout=300,            # Give up after 5 minutes
    mode="poke",            # Or "reschedule" to free the worker slot
)
```

| Parameter | Description |
|-----------|-------------|
| `filepath` | Path to the file to detect |
| `poke_interval` | Seconds between checks |
| `timeout` | Maximum seconds to wait before failing |
| `mode` | `"poke"` (holds worker) or `"reschedule"` (frees worker between checks) |

See: `dags/50_file_sensor.py`

# Provider Operators

## HTTP Operators

HTTP operators let Airflow make HTTP requests and poll endpoints. Useful for triggering
external APIs, checking service health, and downloading data.

### HttpOperator

Makes HTTP requests (GET, POST, PUT, DELETE) to any HTTP endpoint:

```python
from airflow.providers.http.operators.http import HttpOperator

# GET request
get = HttpOperator(
    task_id="get_data",
    http_conn_id="http_default",
    method="GET",
    endpoint="/api/data",
    log_response=True,
)

# POST with JSON body
post = HttpOperator(
    task_id="post_data",
    http_conn_id="http_default",
    method="POST",
    endpoint="/api/data",
    data='{"key": "value"}',
    headers={"Content-Type": "application/json"},
    log_response=True,
)
```

**Key parameters:**

| Parameter | Description |
|-----------|-------------|
| `http_conn_id` | Airflow Connection ID with host/port/auth |
| `method` | HTTP method (`GET`, `POST`, `PUT`, `DELETE`) |
| `endpoint` | URL path appended to the connection's host |
| `data` | Request body (string or dict) |
| `headers` | HTTP headers dict |
| `log_response` | Log the response body |
| `response_check` | Callable to validate response |
| `response_filter` | Callable to extract/transform response for XCom |

See: `dags/33_http_requests.py`

### HttpSensor

Polls an HTTP endpoint until a condition is met. Supports both poke and deferrable modes:

```python
from airflow.providers.http.sensors.http import HttpSensor

# Poke mode: holds worker slot
wait_poke = HttpSensor(
    task_id="wait_poke",
    http_conn_id="http_default",
    endpoint="/health",
    response_check=lambda response: response.status_code == 200,
    poke_interval=30,
    timeout=600,
    mode="poke",
)

# Deferrable mode: releases worker slot (recommended for long waits)
wait_defer = HttpSensor(
    task_id="wait_defer",
    http_conn_id="http_default",
    endpoint="/health",
    response_check=lambda response: response.status_code == 200,
    poke_interval=30,
    timeout=600,
    deferrable=True,
)
```

**Poke vs Deferrable:**

| Mode | Worker Slot | Triggerer | Best For |
|------|-------------|-----------|----------|
| `mode="poke"` | Held during wait | Not used | Short waits (<5 min) |
| `mode="reschedule"` | Released between pokes | Not used | Medium waits |
| `deferrable=True` | Released immediately | Handles async wait | Long waits, many sensors |

See: `dags/34_http_sensor.py`

---

## SQL Operators

If your workflow needs to talk to a database (PostgreSQL, MySQL, SQLite), SQL operators let
you run queries as Airflow tasks. You do not need to know SQL to use the rest of this project --
these are covered in DAGs 35-37 and you can skip them if SQL is not your thing.

### SQLExecuteQueryOperator

The universal SQL operator. Works with any database that has an Airflow provider:

```python
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# DDL: Create a table
create = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id="postgres_default",
    sql="""
        CREATE TABLE IF NOT EXISTS measurements (
            id SERIAL PRIMARY KEY,
            station VARCHAR(50),
            temp_c REAL,
            recorded_at TIMESTAMP DEFAULT NOW()
        );
    """,
)

# DML: Insert data
insert = SQLExecuteQueryOperator(
    task_id="insert_data",
    conn_id="postgres_default",
    sql="""
        INSERT INTO measurements (station, temp_c) VALUES
        ('oslo_01', 12.5),
        ('bergen_01', 9.8);
    """,
)

# Query with Jinja templates
query = SQLExecuteQueryOperator(
    task_id="query_data",
    conn_id="postgres_default",
    sql="SELECT * FROM measurements WHERE recorded_at >= '{{ ds }}';",
)
```

**Key parameters:**

| Parameter | Description |
|-----------|-------------|
| `conn_id` | Airflow Connection ID for the database |
| `sql` | SQL query string or path to `.sql` file |
| `parameters` | Query parameters (prevents SQL injection) |
| `autocommit` | Auto-commit after each statement |
| `handler` | Custom result handler function |

**Airflow 3.x migration note:** `PostgresOperator`, `MySqlOperator`, and other provider-specific
SQL operators are deprecated. Use `SQLExecuteQueryOperator` with the appropriate `conn_id` instead.

See: `dags/35_postgres_queries.py`

### SQL ETL Pipeline

A complete SQL-based ETL pipeline with staging, transformation, and summary tables:

```
[setup: create tables] -> [extract: load staging] -> [transform: clean + validate] -> [summarize: aggregates] -> [teardown: drop tables]
```

Each stage uses TaskGroups for organization. The transform step uses SQL CASE expressions
to validate data (reject non-numeric values, out-of-range readings) and cast types:

```sql
INSERT INTO prod_measurements (sensor_id, value_numeric, unit, is_valid, measured_at)
SELECT
    sensor_id,
    CASE WHEN raw_value ~ '^-?[0-9.]+$' THEN raw_value::REAL ELSE NULL END,
    unit,
    CASE
        WHEN raw_value ~ '^-?[0-9.]+$' AND raw_value::REAL BETWEEN -50 AND 60
        THEN TRUE ELSE FALSE
    END,
    ts
FROM stg_sensors;
```

See: `dags/36_sql_pipeline.py`

### Generic SQL Transfer

Transfer data between tables with SQL-based transformation. A common pattern for
database-to-database ETL where the source and destination have different schemas:

```sql
INSERT INTO transfer_dest (city, country, population, size_category)
SELECT
    city, country, population,
    CASE
        WHEN population >= 900000 THEN 'large'
        WHEN population >= 500000 THEN 'medium'
        ELSE 'small'
    END
FROM transfer_source
ORDER BY population DESC;
```

See: `dags/37_generic_transfer.py`

---

## Email Operators

Email operators send notifications from DAG tasks. This project uses Mailpit as a local
SMTP server -- all captured emails are viewable at [http://localhost:8025](http://localhost:8025).

### EmailOperator

Send plain text or HTML emails with Jinja-templated content:

```python
from airflow.providers.smtp.operators.smtp import EmailOperator

# Plain email
send_plain = EmailOperator(
    task_id="send_plain",
    conn_id="smtp_default",
    to="team@example.com",
    subject="Pipeline started ({{ ds }})",
    html_content="<p>The pipeline has started for {{ ds }}.</p>",
    from_email="airflow@example.com",
)

# Rich HTML email with tables
send_html = EmailOperator(
    task_id="send_html",
    conn_id="smtp_default",
    to="team@example.com",
    subject="Pipeline report ({{ ds }})",
    html_content="""
    <h2>Pipeline Report</h2>
    <table border="1">
        <tr><td>DAG</td><td>{{ dag.dag_id }}</td></tr>
        <tr><td>Date</td><td>{{ ds }}</td></tr>
        <tr><td>Run ID</td><td>{{ run_id }}</td></tr>
    </table>
    """,
    from_email="airflow@example.com",
)
```

**Key parameters:**

| Parameter | Description |
|-----------|-------------|
| `conn_id` | SMTP Connection ID |
| `to` | Recipient email (string or list) |
| `subject` | Email subject (supports Jinja) |
| `html_content` | Email body as HTML (supports Jinja) |
| `from_email` | Sender email address |
| `cc` | CC recipients |
| `bcc` | BCC recipients |
| `files` | List of file paths to attach |

**Common patterns:**

- **Pipeline start/end notifications**: Send at DAG start and end
- **Failure alerts**: Use `on_failure_callback` to send on task failure
- **Summary reports**: Build HTML tables with pipeline metrics

See: `dags/38_email_notifications.py`

---

## SSH Operators

SSH operators execute commands on remote hosts. This project uses a local OpenSSH
server container for demonstrations.

### SSHOperator

Execute commands on a remote host via SSH:

```python
from airflow.providers.ssh.operators.ssh import SSHOperator

# Simple command
remote_uname = SSHOperator(
    task_id="remote_uname",
    ssh_conn_id="ssh_default",
    command="uname -a",
    cmd_timeout=10,
)

# Multi-command script
remote_script = SSHOperator(
    task_id="remote_script",
    ssh_conn_id="ssh_default",
    command=(
        "echo 'User: '$(whoami) && "
        "echo 'PWD: '$(pwd) && "
        "echo 'Date: '$(date)"
    ),
    cmd_timeout=10,
)

# File operations on remote host
remote_file = SSHOperator(
    task_id="remote_file",
    ssh_conn_id="ssh_default",
    command=(
        "echo 'Hello from Airflow' > /tmp/test.txt && "
        "cat /tmp/test.txt && "
        "rm /tmp/test.txt"
    ),
    cmd_timeout=10,
)
```

**Key parameters:**

| Parameter | Description |
|-----------|-------------|
| `ssh_conn_id` | Airflow Connection ID with host/port/credentials |
| `command` | Shell command to execute remotely |
| `cmd_timeout` | Timeout in seconds for the command |
| `environment` | Environment variables to set on remote |
| `get_pty` | Request a pseudo-terminal (needed for sudo) |

**Connection setup:**

```bash
airflow connections add ssh_default \
    --conn-type ssh \
    --conn-host remote-host \
    --conn-port 22 \
    --conn-login username \
    --conn-password password
```

See: `dags/39_ssh_commands.py`

---

## LatestOnlyOperator

The `LatestOnlyOperator` skips all downstream tasks when the current DAG run is
not the most recent. This prevents backfill runs from executing side-effect tasks
like notifications, deployments, or cache refreshes.

```python
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

process_data = PythonOperator(task_id="process_data", ...)
latest_check = LatestOnlyOperator(task_id="latest_check")
send_notification = PythonOperator(task_id="send_notification", ...)
update_dashboard = BashOperator(task_id="update_dashboard", ...)

# process_data always runs; notification and dashboard only on latest run
process_data >> latest_check >> [send_notification, update_dashboard]
```

**When to use:**

| Scenario | Use LatestOnlyOperator? |
|----------|----------------------|
| Sending Slack/email notifications | Yes -- don't spam during backfills |
| Updating a dashboard or cache | Yes -- only latest data matters |
| Writing to a data warehouse | No -- backfill data is needed |
| Processing historical data | No -- every run matters |

See: `dags/40_latest_only.py`

---

## Deferrable Operators and Triggers

This is an advanced topic -- feel free to skip on your first read.

Some tasks just wait: "wait until the file appears", "wait until the API responds."
Normally, a waiting task occupies a worker slot the whole time (like a phone call on
hold). Deferrable operators are smarter -- they say "call me back when the condition is
met" and free up the slot for other tasks. Airflow's **triggerer** process handles the
waiting and wakes the task up when it is time.

### How Deferrable Execution Works

```
1. Task.execute() calls self.defer(trigger=..., method_name="execute_complete")
2. Worker slot is RELEASED
3. Trigger runs in the triggerer process (async event loop)
4. When trigger fires, task resumes via execute_complete()
5. Worker slot is RE-ACQUIRED only for execute_complete()
```

**Benefits:**

- Thousands of waiting tasks with minimal worker slots
- Triggerer process handles all async waits efficiently
- No wasted resources during long sensor waits

### Built-in Deferrable Sensors

Many standard sensors support `deferrable=True` in Airflow 3.x:

```python
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

# Traditional: holds worker slot
poke_sensor = TimeDeltaSensor(
    task_id="poke",
    delta=timedelta(minutes=5),
    mode="poke",
)

# Deferrable: releases worker slot
defer_sensor = TimeDeltaSensor(
    task_id="defer",
    delta=timedelta(minutes=5),
    deferrable=True,
)
```

**Sensors with deferrable support:**

| Sensor | Parameter |
|--------|-----------|
| `TimeDeltaSensor` | `deferrable=True` |
| `TimeSensor` | `deferrable=True` |
| `HttpSensor` | `deferrable=True` |
| `FileSensor` | `deferrable=True` |
| `ExternalTaskSensor` | `deferrable=True` |

See: `dags/41_deferrable_sensors.py`

### Writing a Custom Trigger

A trigger runs in the triggerer's async event loop. It must implement `serialize()`
for persistence and `run()` as an async generator:

```python
import asyncio
from airflow.triggers.base import BaseTrigger, TriggerEvent

class CountdownTrigger(BaseTrigger):
    """Waits for N seconds then fires."""

    def __init__(self, seconds: int, message: str) -> None:
        super().__init__()
        self.seconds = seconds
        self.message = message

    def serialize(self) -> tuple[str, dict]:
        """Serialize for storage (classpath + kwargs)."""
        return (
            "airflow_examples.triggers.CountdownTrigger",
            {"seconds": self.seconds, "message": self.message},
        )

    async def run(self):
        """Async generator that yields a TriggerEvent."""
        await asyncio.sleep(self.seconds)
        yield TriggerEvent({"status": "complete", "message": self.message})
```

### Writing a Custom Deferrable Operator

A deferrable operator has two execution phases:

1. **`execute()`**: Starts the task and defers to a trigger
2. **`execute_complete()`**: Resumes when the trigger fires

```python
from airflow.sdk import BaseOperator

class CountdownOperator(BaseOperator):
    def __init__(self, seconds: int = 2, message: str = "default", **kwargs):
        super().__init__(**kwargs)
        self.seconds = seconds
        self.message = message

    def execute(self, context):
        """Start the operator -- defer to triggerer."""
        self.defer(
            trigger=CountdownTrigger(seconds=self.seconds, message=self.message),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        """Resume after trigger fires -- event contains the TriggerEvent payload."""
        print(f"Trigger fired: {event['message']} ({event['elapsed_seconds']}s)")
        return f"Complete: {event['message']}"
```

**Deferrable operator lifecycle:**

```
execute() --[defer]--> Triggerer (async) --[TriggerEvent]--> execute_complete() --[return]--> XCom
    |                       |                                       |
    v                       v                                       v
 Worker released      Runs in event loop                     Worker re-acquired
```

See: `dags/42_custom_deferrable.py` and `src/airflow_examples/triggers.py`

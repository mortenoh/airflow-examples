# REST API & CLI

Airflow has a web UI, and it is great for browsing DAGs, checking logs, and manually
triggering runs. But at some point you will want to do these things from a script -- trigger
a DAG from a CI/CD pipeline, bulk-unpause all your DAGs after a deployment, or poll a run
until it finishes so your next step can proceed. That is what the REST API is for.

This page covers two things:

1. **The Airflow 3 REST API** -- raw HTTP endpoints you can hit with curl or any HTTP client
2. **The `af` CLI** -- a Typer wrapper around that API, installed with this project, that saves
   you from typing auth headers over and over

We will start with curl so you understand what is actually happening over the wire, then
show the `af` shorthand for each operation.

---

## Connecting

### Base URL

With the Docker Compose setup from this project, the API lives at:

```
http://localhost:8081/api/v2/
```

Port 8081 is the web server port set in the `docker-compose.yml`. The `/api/v2/` prefix is
new in Airflow 3 -- Airflow 2 used `/api/v1/`.

Airflow ships a Swagger UI at [http://localhost:8081/api/v2/docs](http://localhost:8081/api/v2/docs)
where you can explore every endpoint interactively.

### Authentication: JWT Tokens

Airflow 3 uses JWT (JSON Web Token) authentication. You POST your credentials to `/auth/token`
and get back an access token. Then you pass that token in the `Authorization` header on every
subsequent request.

```bash
# Step 1: Get a token
TOKEN=$(curl -s -X POST http://localhost:8081/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Step 2: Use the token
curl -s http://localhost:8081/api/v2/dags \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

The token response looks like:

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer"
}
```

This is a change from Airflow 2, which used basic auth (username/password on every request)
or session cookies.

### Unauthenticated Endpoints

Two endpoints do not require a token:

```bash
# Airflow version
curl -s http://localhost:8081/api/v2/version

# Component health (scheduler, triggerer, dag_processor)
curl -s http://localhost:8081/api/v2/monitor/health
```

These are useful for health checks in Docker Compose, Kubernetes, or monitoring scripts.

---

## The `af` CLI

The `af` command is a [Typer](https://typer.tiangolo.com/) CLI that wraps every API call
shown on this page. It handles JWT authentication transparently -- you never have to
manually fetch tokens.

### Installation

It is already installed when you set up this project:

```bash
uv sync
```

The entry point is defined in `pyproject.toml` under `[project.scripts]`, so `uv sync`
makes the `af` command available in your virtualenv.

### Global Options

```bash
af --help
```

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--base-url` | `AIRFLOW_URL` | `http://localhost:8081` | Airflow web server URL |
| `--username` | `AIRFLOW_USER` | `admin` | Username for token auth |
| `--password` | `AIRFLOW_PASS` | `admin` | Password for token auth |

### Command Groups

```
af version              Show Airflow version
af health               Show component health

af dags list            List all DAGs
af dags info <id>       Show DAG details
af dags pause <id>      Pause a DAG
af dags unpause <id>    Unpause a DAG
af dags trigger <id>    Trigger a DAG run
af dags delete <id>     Delete a DAG

af runs list <dag>      List runs for a DAG
af runs get <dag> <run> Show run details
af runs tasks <dag> <run>  List task instances
af runs logs <dag> <run> <task>  Show task logs
af runs wait <dag> <run>   Poll until terminal state

af vars list            List all variables
af vars get <key>       Get variable value
af vars set <key> <val> Create or update variable
af vars delete <key>    Delete variable

af pools list           List all pools
af pools get <name>     Get pool details
af pools set <name> <slots>  Create or update pool
af pools delete <name>  Delete pool

af conns list           List all connections
af conns get <id>       Get connection details

af xcoms list <dag> <task> [run]  List XCom entries
af xcoms list <dag> <task> --latest  List XCom entries (latest run)
af xcoms get <dag> <task> [run]   Get XCom value (default key: return_value)
af xcoms get <dag> <task> --latest  Get XCom value (latest run)
```

---

## Version & Health

These are the simplest endpoints and do not require authentication.

### GET /api/v2/version

```bash
curl -s http://localhost:8081/api/v2/version
```

```json
{
  "version": "3.0.1",
  "git_version": null
}
```

With the CLI:

```bash
af version
# 3.0.1
```

### GET /api/v2/monitor/health

```bash
curl -s http://localhost:8081/api/v2/monitor/health | python3 -m json.tool
```

```json
{
  "metadatabase": { "status": "healthy" },
  "scheduler": {
    "status": "healthy",
    "latest_scheduler_heartbeat": "2025-01-15T10:30:00+00:00"
  },
  "triggerer": {
    "status": "healthy",
    "latest_triggerer_heartbeat": "2025-01-15T10:30:00+00:00"
  },
  "dag_processor": {
    "status": "healthy",
    "latest_dag_processor_heartbeat": "2025-01-15T10:30:00+00:00"
  }
}
```

With the CLI:

```bash
af health
```

---

## DAGs

### List DAGs

**GET /api/v2/dags**

```bash
curl -s http://localhost:8081/api/v2/dags \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

The response includes a `dags` array and a `total_entries` count. You can filter with query
parameters:

| Parameter | Example | Description |
|-----------|---------|-------------|
| `limit` | `?limit=10` | Max results (default 100) |
| `offset` | `?offset=10` | Pagination offset |
| `paused` | `?paused=true` | Only paused / unpaused DAGs |
| `dag_id_pattern` | `?dag_id_pattern=001%` | Filter by DAG ID pattern |

```bash
# Only unpaused DAGs, first 5
curl -s "http://localhost:8081/api/v2/dags?paused=false&limit=5" \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

With the CLI:

```bash
af dags list
```

### DAG Details

**GET /api/v2/dags/{dag_id}**

```bash
curl -s http://localhost:8081/api/v2/dags/001_hello_world \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

```json
{
  "dag_id": "001_hello_world",
  "description": "Hello World -- your first DAG.",
  "is_paused": false,
  "owners": ["airflow"],
  "timetable_summary": null,
  "tags": [{"name": "basics"}],
  "...": "..."
}
```

With the CLI:

```bash
af dags info 001_hello_world
```

### Pause / Unpause

**PATCH /api/v2/dags/{dag_id}**

```bash
# Pause
curl -s -X PATCH http://localhost:8081/api/v2/dags/001_hello_world \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"is_paused": true}'

# Unpause
curl -s -X PATCH http://localhost:8081/api/v2/dags/001_hello_world \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"is_paused": false}'
```

With the CLI:

```bash
af dags pause 001_hello_world
af dags unpause 001_hello_world
```

### Trigger a DAG Run

**POST /api/v2/dags/{dag_id}/dagRuns**

This is the most common API operation. You must provide a `logical_date` -- Airflow 3
requires it and will return 422 if you omit it.

```bash
curl -s -X POST http://localhost:8081/api/v2/dags/001_hello_world/dagRuns \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "logical_date": "2025-01-15T12:00:00Z"
  }'
```

To pass configuration to the DAG:

```bash
curl -s -X POST http://localhost:8081/api/v2/dags/001_hello_world/dagRuns \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "logical_date": "2025-01-15T12:00:00Z",
    "conf": {"key": "value"}
  }'
```

Response:

```json
{
  "dag_run_id": "manual__2025-01-15T12:00:00+00:00",
  "dag_id": "001_hello_world",
  "logical_date": "2025-01-15T12:00:00+00:00",
  "state": "queued",
  "conf": {},
  "...": "..."
}
```

With the CLI (it generates `logical_date` automatically):

```bash
af dags trigger 001_hello_world
af dags trigger 001_hello_world --conf '{"key": "value"}'
```

!!! note "logical_date must be unique"
    Each DAG run needs a unique `logical_date`. If you try to trigger with a date that
    already has a run, you will get a 409 Conflict. The `af` CLI avoids this by using
    the current UTC timestamp.

### Delete a DAG

**DELETE /api/v2/dags/{dag_id}**

```bash
curl -s -X DELETE http://localhost:8081/api/v2/dags/001_hello_world \
  -H "Authorization: Bearer $TOKEN"
```

With the CLI:

```bash
af dags delete 001_hello_world
```

This removes the DAG metadata from the database. The DAG file on disk is not affected --
if the scheduler finds it on the next parse cycle, it will recreate the DAG entry.

---

## DAG Runs

### List Runs

**GET /api/v2/dags/{dag_id}/dagRuns**

```bash
curl -s http://localhost:8081/api/v2/dags/001_hello_world/dagRuns \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Response:

```json
{
  "dag_runs": [
    {
      "dag_run_id": "manual__2025-01-15T12:00:00+00:00",
      "state": "success",
      "logical_date": "2025-01-15T12:00:00+00:00",
      "start_date": "2025-01-15T12:00:01+00:00",
      "end_date": "2025-01-15T12:00:05+00:00"
    }
  ],
  "total_entries": 1
}
```

With the CLI:

```bash
af runs list 001_hello_world
```

### Run Details

**GET /api/v2/dags/{dag_id}/dagRuns/{run_id}**

```bash
curl -s http://localhost:8081/api/v2/dags/001_hello_world/dagRuns/manual__2025-01-15T12:00:00+00:00 \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

With the CLI:

```bash
af runs get 001_hello_world "manual__2025-01-15T12:00:00+00:00"
```

### Task Instances

**GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances**

```bash
curl -s http://localhost:8081/api/v2/dags/001_hello_world/dagRuns/manual__2025-01-15T12:00:00+00:00/taskInstances \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Response:

```json
{
  "task_instances": [
    {
      "task_id": "say_hello",
      "state": "success",
      "try_number": 1,
      "start_date": "2025-01-15T12:00:01+00:00",
      "end_date": "2025-01-15T12:00:02+00:00"
    }
  ],
  "total_entries": 1
}
```

With the CLI:

```bash
af runs tasks 001_hello_world "manual__2025-01-15T12:00:00+00:00"
```

### Task Logs

**GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}**

```bash
curl -s http://localhost:8081/api/v2/dags/001_hello_world/dagRuns/manual__2025-01-15T12:00:00+00:00/taskInstances/say_hello/logs/1 \
  -H "Authorization: Bearer $TOKEN"
```

This returns plain text (the task log output), not JSON.

With the CLI:

```bash
af runs logs 001_hello_world "manual__2025-01-15T12:00:00+00:00" say_hello
af runs logs 001_hello_world "manual__2025-01-15T12:00:00+00:00" say_hello --try 2
```

### Waiting for a Run to Finish

There is no built-in streaming endpoint in the current Airflow 3 API, so the standard
approach is to poll the run status until it reaches a terminal state (`success`, `failed`,
or `upstream_failed`).

The `af` CLI has a `wait` command that does this for you:

```bash
af runs wait 001_hello_world "manual__2025-01-15T12:00:00+00:00" --timeout 300 --interval 5
```

Output:

```
  [  0s] state=running
  [  5s] state=running
  [ 10s] state=success
Terminal state: success
```

If you want to do this with curl in a script:

```bash
TOKEN=$(curl -s -X POST http://localhost:8081/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

RUN_ID="manual__2025-01-15T12:00:00+00:00"

while true; do
  STATE=$(curl -s http://localhost:8081/api/v2/dags/001_hello_world/dagRuns/$RUN_ID \
    -H "Authorization: Bearer $TOKEN" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")

  echo "state=$STATE"

  case $STATE in
    success|failed|upstream_failed) break ;;
  esac

  sleep 5
done
```

---

## Variables

Variables are key-value pairs stored in the Airflow database. DAGs can read them at parse
time or runtime. The API lets you manage them without touching the UI.

### List Variables

**GET /api/v2/variables**

```bash
curl -s http://localhost:8081/api/v2/variables \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

```json
{
  "variables": [
    { "key": "dag_107_schedule", "value": "@daily" }
  ],
  "total_entries": 1
}
```

With the CLI:

```bash
af vars list
```

### Get a Variable

**GET /api/v2/variables/{key}**

```bash
curl -s http://localhost:8081/api/v2/variables/dag_107_schedule \
  -H "Authorization: Bearer $TOKEN"
```

```json
{ "key": "dag_107_schedule", "value": "@daily" }
```

With the CLI:

```bash
af vars get dag_107_schedule
```

### Create a Variable

**POST /api/v2/variables**

```bash
curl -s -X POST http://localhost:8081/api/v2/variables \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"key": "my_api_key", "value": "abc123"}'
```

With the CLI:

```bash
af vars set my_api_key abc123
```

The `af vars set` command is smart -- it tries PATCH first (update), and if the variable
does not exist (404), it falls back to POST (create).

### Update a Variable

**PATCH /api/v2/variables/{key}**

```bash
curl -s -X PATCH http://localhost:8081/api/v2/variables/dag_107_schedule \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"key": "dag_107_schedule", "value": "@hourly"}'
```

With the CLI:

```bash
af vars set dag_107_schedule @hourly
```

!!! note "Quirk: key is required in the PATCH body"
    Even though the key is in the URL path, the Airflow 3 API also requires it in the
    request body. If you omit it, you get a 422 validation error.

### Delete a Variable

**DELETE /api/v2/variables/{key}**

```bash
curl -s -X DELETE http://localhost:8081/api/v2/variables/dag_107_schedule \
  -H "Authorization: Bearer $TOKEN"
```

With the CLI:

```bash
af vars delete dag_107_schedule
```

---

## Pools

Pools limit how many tasks can run concurrently for a given resource. If you have an API
that only allows 3 concurrent requests, you create a pool with 3 slots and assign your
tasks to it.

### List Pools

**GET /api/v2/pools**

```bash
curl -s http://localhost:8081/api/v2/pools \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

```json
{
  "pools": [
    {
      "name": "default_pool",
      "slots": 128,
      "running_slots": 2,
      "queued_slots": 0
    }
  ],
  "total_entries": 1
}
```

With the CLI:

```bash
af pools list
```

### Get a Pool

**GET /api/v2/pools/{name}**

```bash
curl -s http://localhost:8081/api/v2/pools/default_pool \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

With the CLI:

```bash
af pools get default_pool
```

### Create a Pool

**POST /api/v2/pools**

```bash
curl -s -X POST http://localhost:8081/api/v2/pools \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "api_pool", "slots": 3}'
```

With the CLI:

```bash
af pools set api_pool 3
```

Like `af vars set`, the `af pools set` command tries PATCH first, then falls back to POST.

### Update a Pool

**PATCH /api/v2/pools/{name}**

```bash
curl -s -X PATCH http://localhost:8081/api/v2/pools/api_pool \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "api_pool", "slots": 5}'
```

With the CLI:

```bash
af pools set api_pool 5
```

!!! note "Pool name is not modifiable"
    In Airflow 3 you cannot rename a pool via PATCH. The `name` field in the body must
    match the name in the URL. If you need a different name, delete the old pool and
    create a new one.

### Delete a Pool

**DELETE /api/v2/pools/{name}**

```bash
curl -s -X DELETE http://localhost:8081/api/v2/pools/api_pool \
  -H "Authorization: Bearer $TOKEN"
```

With the CLI:

```bash
af pools delete api_pool
```

---

## Connections

Connections store credentials and host information for external systems (databases, APIs,
cloud services). The API exposes read-only access -- you can list and inspect connections
but creating/modifying them is typically done via the UI or environment variables.

### List Connections

**GET /api/v2/connections**

```bash
curl -s http://localhost:8081/api/v2/connections \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

```json
{
  "connections": [
    {
      "connection_id": "postgres_default",
      "conn_type": "postgres",
      "host": "postgres",
      "port": 5432
    }
  ],
  "total_entries": 1
}
```

With the CLI:

```bash
af conns list
```

### Get a Connection

**GET /api/v2/connections/{connection_id}**

```bash
curl -s http://localhost:8081/api/v2/connections/postgres_default \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

With the CLI:

```bash
af conns get postgres_default
```

---

## XComs

XCom ("cross-communication") entries are how tasks pass data to each other. When a
PythonOperator returns a value, Airflow stores it as an XCom entry with the key
`return_value`. The API lets you inspect these values without opening the web UI.

### List XCom Entries

**GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries**

```bash
curl -s http://localhost:8081/api/v2/dags/002_python_operator/dagRuns/manual__2025-01-15T12:00:00+00:00/taskInstances/greet/xcomEntries \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Response:

```json
{
  "xcom_entries": [
    {
      "key": "return_value",
      "value": "Hello, Airflow!",
      "timestamp": "2025-01-15T12:00:05+00:00"
    }
  ],
  "total_entries": 1
}
```

With the CLI:

```bash
# With an explicit run ID
af xcoms list 002_python_operator greet "manual__2025-01-15T12:00:00+00:00"

# Or use --latest to automatically pick the most recent run
af xcoms list 002_python_operator greet --latest
```

### Get an XCom Value

**GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}**

```bash
curl -s http://localhost:8081/api/v2/dags/002_python_operator/dagRuns/manual__2025-01-15T12:00:00+00:00/taskInstances/greet/xcomEntries/return_value \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

```json
{
  "key": "return_value",
  "value": "Hello, Airflow!",
  "timestamp": "2025-01-15T12:00:05+00:00",
  "dag_id": "002_python_operator",
  "run_id": "manual__2025-01-15T12:00:00+00:00",
  "task_id": "greet"
}
```

With the CLI:

```bash
# Default key is return_value
af xcoms get 002_python_operator greet "manual__2025-01-15T12:00:00+00:00"

# Use --latest instead of a run ID
af xcoms get 002_python_operator greet --latest

# Explicit key
af xcoms get 002_python_operator greet --latest --key return_value

# Full JSON response
af xcoms get 002_python_operator greet --latest --json
```

---

## Airflow 3 API Changes

If you are coming from Airflow 2, here are the key differences:

| What Changed | Airflow 2 | Airflow 3 |
|-------------|-----------|-----------|
| Base path | `/api/v1/` | `/api/v2/` |
| Authentication | Basic auth or session cookies | JWT tokens via `/auth/token` |
| Trigger field | `execution_date` | `logical_date` (required) |
| Validation errors | 400 Bad Request | 422 Unprocessable Entity |
| Pool rename | Allowed via PATCH | Not allowed -- name is immutable |
| Variable PATCH body | Only `value` needed | `key` required in body alongside `value` |
| Swagger UI | `/api/v1/ui/` | `/api/v2/docs` |

The `execution_date` to `logical_date` rename is the most common thing that breaks
existing scripts. If you see 422 errors after upgrading, check that your trigger payloads
use `logical_date`.

---

## Recipes

### Trigger + Wait + Check

The most common pattern: trigger a DAG, wait for it to finish, then check the result.

With `af`:

```bash
# Trigger and capture the run ID
af dags trigger 001_hello_world
# Output:
#   Triggered: 001_hello_world
#   run_id: manual__2025-01-15T12:00:00+00:00
#   state:  queued

# Wait for completion
af runs wait 001_hello_world "manual__2025-01-15T12:00:00+00:00"

# Check task results
af runs tasks 001_hello_world "manual__2025-01-15T12:00:00+00:00"
```

With curl in a script:

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8081"

# Authenticate
TOKEN=$(curl -s -X POST "$BASE/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

AUTH="Authorization: Bearer $TOKEN"

# Trigger
RESPONSE=$(curl -s -X POST "$BASE/api/v2/dags/001_hello_world/dagRuns" \
  -H "$AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"logical_date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}")

RUN_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['dag_run_id'])")
echo "Triggered run: $RUN_ID"

# Poll
while true; do
  STATE=$(curl -s "$BASE/api/v2/dags/001_hello_world/dagRuns/$RUN_ID" \
    -H "$AUTH" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['state'])")
  echo "state=$STATE"
  case $STATE in
    success) echo "Done."; exit 0 ;;
    failed|upstream_failed) echo "Failed."; exit 1 ;;
  esac
  sleep 5
done
```

### CI/CD: Trigger from GitHub Actions

```yaml
# .github/workflows/trigger-dag.yml
name: Trigger Airflow DAG

on:
  push:
    branches: [main]

jobs:
  trigger:
    runs-on: ubuntu-latest
    steps:
      - name: Get Airflow token
        id: auth
        run: |
          TOKEN=$(curl -s -X POST ${{ secrets.AIRFLOW_URL }}/auth/token \
            -H "Content-Type: application/json" \
            -d '{"username": "${{ secrets.AIRFLOW_USER }}", "password": "${{ secrets.AIRFLOW_PASS }}"}' \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
          echo "token=$TOKEN" >> "$GITHUB_OUTPUT"

      - name: Trigger DAG
        run: |
          curl -s -X POST ${{ secrets.AIRFLOW_URL }}/api/v2/dags/my_deploy_dag/dagRuns \
            -H "Authorization: Bearer ${{ steps.auth.outputs.token }}" \
            -H "Content-Type: application/json" \
            -d "{\"logical_date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"conf\": {\"commit\": \"${{ github.sha }}\"}}"
```

### Bulk Unpause All DAGs

With `af` and a bit of shell:

```bash
af dags list 2>/dev/null | tail -n +3 | awk '{print $1}' | while read dag_id; do
  af dags unpause "$dag_id"
done
```

With curl:

```bash
TOKEN=$(curl -s -X POST http://localhost:8081/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -s http://localhost:8081/api/v2/dags?paused=true&limit=500 \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys, json
for dag in json.load(sys.stdin)['dags']:
    print(dag['dag_id'])
" | while read dag_id; do
  curl -s -X PATCH "http://localhost:8081/api/v2/dags/$dag_id" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"is_paused": false}'
  echo "Unpaused: $dag_id"
done
```

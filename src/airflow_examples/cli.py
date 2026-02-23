"""Typer CLI for the Airflow REST API.

Wraps the Airflow v2 REST API endpoints behind a convenient ``af`` command,
handling JWT authentication transparently.

Usage::

    af version
    af dags list
    af dags trigger 001_hello_world --conf '{"key": "value"}'
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Annotated, Any, Optional

import httpx
import typer

# ---------------------------------------------------------------------------
# Typer app & sub-commands
# ---------------------------------------------------------------------------

app = typer.Typer(help="Airflow REST API CLI", no_args_is_help=True)
dags_app = typer.Typer(help="Manage DAGs", no_args_is_help=True)
runs_app = typer.Typer(help="Manage DAG runs", no_args_is_help=True)
vars_app = typer.Typer(help="Manage variables", no_args_is_help=True)
pools_app = typer.Typer(help="Manage pools", no_args_is_help=True)
conns_app = typer.Typer(help="Manage connections", no_args_is_help=True)

app.add_typer(dags_app, name="dags")
app.add_typer(runs_app, name="runs")
app.add_typer(vars_app, name="vars")
app.add_typer(pools_app, name="pools")
app.add_typer(conns_app, name="conns")

# ---------------------------------------------------------------------------
# Global state (set via callback)
# ---------------------------------------------------------------------------

_base_url: str = ""
_username: str = ""
_password: str = ""


@app.callback()
def _main(
    base_url: Annotated[
        str, typer.Option("--base-url", envvar="AIRFLOW_URL", help="Airflow base URL")
    ] = "http://localhost:8081",
    username: Annotated[
        str, typer.Option("--username", envvar="AIRFLOW_USER", help="Airflow username")
    ] = "admin",
    password: Annotated[
        str, typer.Option("--password", envvar="AIRFLOW_PASS", help="Airflow password")
    ] = "admin",
) -> None:
    global _base_url, _username, _password  # noqa: PLW0603
    _base_url = base_url.rstrip("/")
    _username = username
    _password = password


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _api_url(path: str) -> str:
    """Build a full API URL from a relative path."""
    return f"{_base_url}/api/v2/{path.lstrip('/')}"


def _get_token() -> str:
    """Authenticate and return a JWT access token."""
    resp = httpx.post(
        f"{_base_url}/auth/token",
        json={"username": _username, "password": _password},
    )
    if resp.status_code >= 400:
        typer.echo(f"Authentication failed: {resp.status_code} {resp.text}", err=True)
        raise typer.Exit(1)
    token: str = resp.json()["access_token"]
    return token


def _client() -> httpx.Client:
    """Return an authenticated httpx client."""
    token = _get_token()
    return httpx.Client(
        base_url=f"{_base_url}/api/v2/",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30.0,
    )


def _check(resp: httpx.Response) -> dict[str, Any]:
    """Check response status and return JSON, or exit on error."""
    if resp.status_code >= 400:
        typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
        raise typer.Exit(1)
    result: dict[str, Any] = resp.json()
    return result


def _print_table(rows: list[dict[str, Any]], columns: list[tuple[str, str, int]]) -> None:
    """Print a simple aligned table.

    Args:
        rows: List of dicts (JSON objects).
        columns: List of (header, key, width) tuples.
    """
    header = "  ".join(h.ljust(w) for h, _, w in columns)
    typer.echo(header)
    typer.echo("-" * len(header))
    for row in rows:
        line = "  ".join(str(row.get(k, "")).ljust(w) for _, k, w in columns)
        typer.echo(line)


# ===========================================================================
# Top-level commands
# ===========================================================================


@app.command()
def version() -> None:
    """Show Airflow version."""
    resp = httpx.get(_api_url("version"))
    data = _check(resp)
    typer.echo(data.get("version", json.dumps(data)))


@app.command()
def health() -> None:
    """Show Airflow component health."""
    resp = httpx.get(_api_url("monitor/health"))
    data = _check(resp)
    typer.echo(json.dumps(data, indent=2))


# ===========================================================================
# DAGs
# ===========================================================================


@dags_app.command("list")
def dags_list() -> None:
    """List all DAGs."""
    with _client() as c:
        resp = c.get("dags", params={"limit": 500})
        data = _check(resp)
    dags = data.get("dags", [])
    typer.echo(f"Total DAGs: {data.get('total_entries', len(dags))}\n")
    _print_table(
        dags,
        [
            ("DAG ID", "dag_id", 45),
            ("PAUSED", "is_paused", 8),
            ("OWNER", "owners", 20),
            ("SCHEDULE", "timetable_summary", 25),
        ],
    )


@dags_app.command("info")
def dags_info(dag_id: str) -> None:
    """Show details for a DAG."""
    with _client() as c:
        resp = c.get(f"dags/{dag_id}")
        data = _check(resp)
    typer.echo(json.dumps(data, indent=2))


@dags_app.command("pause")
def dags_pause(dag_id: str) -> None:
    """Pause a DAG."""
    with _client() as c:
        resp = c.patch(f"dags/{dag_id}", json={"is_paused": True})
        _check(resp)
    typer.echo(f"Paused: {dag_id}")


@dags_app.command("unpause")
def dags_unpause(dag_id: str) -> None:
    """Unpause a DAG."""
    with _client() as c:
        resp = c.patch(f"dags/{dag_id}", json={"is_paused": False})
        _check(resp)
    typer.echo(f"Unpaused: {dag_id}")


@dags_app.command("trigger")
def dags_trigger(
    dag_id: str,
    conf: Annotated[Optional[str], typer.Option(help="JSON config for the run")] = None,
) -> None:
    """Trigger a DAG run."""
    body: dict[str, Any] = {
        "logical_date": datetime.now(tz=timezone.utc).isoformat(),
    }
    if conf:
        body["conf"] = json.loads(conf)
    with _client() as c:
        resp = c.post(f"dags/{dag_id}/dagRuns", json=body)
        data = _check(resp)
    typer.echo(f"Triggered: {dag_id}")
    typer.echo(f"  run_id: {data.get('dag_run_id')}")
    typer.echo(f"  state:  {data.get('state')}")


@dags_app.command("delete")
def dags_delete(dag_id: str) -> None:
    """Delete a DAG."""
    with _client() as c:
        resp = c.delete(f"dags/{dag_id}")
        if resp.status_code >= 400:
            typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
            raise typer.Exit(1)
    typer.echo(f"Deleted: {dag_id}")


# ===========================================================================
# DAG Runs
# ===========================================================================


@runs_app.command("list")
def runs_list(dag_id: str) -> None:
    """List runs for a DAG."""
    with _client() as c:
        resp = c.get(f"dags/{dag_id}/dagRuns")
        data = _check(resp)
    runs = data.get("dag_runs", [])
    typer.echo(f"Runs for {dag_id}: {data.get('total_entries', len(runs))}\n")
    _print_table(
        runs,
        [
            ("RUN ID", "dag_run_id", 45),
            ("STATE", "state", 12),
            ("LOGICAL DATE", "logical_date", 28),
        ],
    )


@runs_app.command("get")
def runs_get(dag_id: str, run_id: str) -> None:
    """Show details for a DAG run."""
    with _client() as c:
        resp = c.get(f"dags/{dag_id}/dagRuns/{run_id}")
        data = _check(resp)
    typer.echo(json.dumps(data, indent=2))


@runs_app.command("tasks")
def runs_tasks(dag_id: str, run_id: str) -> None:
    """List task instances for a DAG run."""
    with _client() as c:
        resp = c.get(f"dags/{dag_id}/dagRuns/{run_id}/taskInstances")
        data = _check(resp)
    tasks = data.get("task_instances", [])
    typer.echo(f"Tasks for {dag_id}/{run_id}: {len(tasks)}\n")
    _print_table(
        tasks,
        [
            ("TASK ID", "task_id", 35),
            ("STATE", "state", 12),
            ("TRY", "try_number", 5),
            ("START", "start_date", 28),
        ],
    )


@runs_app.command("logs")
def runs_logs(
    dag_id: str,
    run_id: str,
    task_id: str,
    try_number: Annotated[int, typer.Option("--try", help="Try number")] = 1,
) -> None:
    """Show logs for a task instance."""
    with _client() as c:
        resp = c.get(
            f"dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}"
        )
        if resp.status_code >= 400:
            typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
            raise typer.Exit(1)
    typer.echo(resp.text)


@runs_app.command("wait")
def runs_wait(
    dag_id: str,
    run_id: str,
    timeout: Annotated[int, typer.Option(help="Timeout in seconds")] = 300,
    interval: Annotated[int, typer.Option(help="Poll interval in seconds")] = 5,
) -> None:
    """Poll a DAG run until it reaches a terminal state."""
    terminal = {"success", "failed", "upstream_failed"}
    elapsed = 0
    with _client() as c:
        while elapsed < timeout:
            resp = c.get(f"dags/{dag_id}/dagRuns/{run_id}")
            data = _check(resp)
            state = data.get("state", "")
            typer.echo(f"  [{elapsed:>3d}s] state={state}")
            if state in terminal:
                typer.echo(f"Terminal state: {state}")
                if state != "success":
                    raise typer.Exit(1)
                return
            time.sleep(interval)
            elapsed += interval
    typer.echo("Timeout waiting for run to complete", err=True)
    raise typer.Exit(1)


# ===========================================================================
# Variables
# ===========================================================================


@vars_app.command("list")
def vars_list() -> None:
    """List all variables."""
    with _client() as c:
        resp = c.get("variables")
        data = _check(resp)
    variables = data.get("variables", [])
    typer.echo(f"Variables: {data.get('total_entries', len(variables))}\n")
    _print_table(
        variables,
        [
            ("KEY", "key", 35),
            ("VALUE", "value", 50),
        ],
    )


@vars_app.command("get")
def vars_get(key: str) -> None:
    """Get a variable by key."""
    with _client() as c:
        resp = c.get(f"variables/{key}")
        data = _check(resp)
    typer.echo(f"{data.get('key')}: {data.get('value')}")


@vars_app.command("set")
def vars_set(key: str, value: str) -> None:
    """Set a variable (create or update)."""
    with _client() as c:
        # Try update first
        resp = c.patch(f"variables/{key}", json={"key": key, "value": value})
        if resp.status_code == 404:
            # Create new
            resp = c.post("variables", json={"key": key, "value": value})
            _check(resp)
            typer.echo(f"Created: {key}")
        elif resp.status_code >= 400:
            typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
            raise typer.Exit(1)
        else:
            typer.echo(f"Updated: {key}")


@vars_app.command("delete")
def vars_delete(key: str) -> None:
    """Delete a variable."""
    with _client() as c:
        resp = c.delete(f"variables/{key}")
        if resp.status_code >= 400:
            typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
            raise typer.Exit(1)
    typer.echo(f"Deleted: {key}")


# ===========================================================================
# Pools
# ===========================================================================


@pools_app.command("list")
def pools_list() -> None:
    """List all pools."""
    with _client() as c:
        resp = c.get("pools")
        data = _check(resp)
    pools = data.get("pools", [])
    typer.echo(f"Pools: {data.get('total_entries', len(pools))}\n")
    _print_table(
        pools,
        [
            ("NAME", "name", 30),
            ("SLOTS", "slots", 8),
            ("RUNNING", "running_slots", 10),
            ("QUEUED", "queued_slots", 10),
        ],
    )


@pools_app.command("get")
def pools_get(name: str) -> None:
    """Get a pool by name."""
    with _client() as c:
        resp = c.get(f"pools/{name}")
        data = _check(resp)
    typer.echo(json.dumps(data, indent=2))


@pools_app.command("set")
def pools_set(name: str, slots: int) -> None:
    """Set a pool (create or update)."""
    with _client() as c:
        resp = c.patch(f"pools/{name}", json={"name": name, "slots": slots})
        if resp.status_code == 404:
            resp = c.post("pools", json={"name": name, "slots": slots})
            _check(resp)
            typer.echo(f"Created: {name} ({slots} slots)")
        elif resp.status_code >= 400:
            typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
            raise typer.Exit(1)
        else:
            typer.echo(f"Updated: {name} ({slots} slots)")


@pools_app.command("delete")
def pools_delete(name: str) -> None:
    """Delete a pool."""
    with _client() as c:
        resp = c.delete(f"pools/{name}")
        if resp.status_code >= 400:
            typer.echo(f"Error {resp.status_code}: {resp.text}", err=True)
            raise typer.Exit(1)
    typer.echo(f"Deleted: {name}")


# ===========================================================================
# Connections
# ===========================================================================


@conns_app.command("list")
def conns_list() -> None:
    """List all connections."""
    with _client() as c:
        resp = c.get("connections")
        data = _check(resp)
    connections = data.get("connections", [])
    typer.echo(f"Connections: {data.get('total_entries', len(connections))}\n")
    _print_table(
        connections,
        [
            ("ID", "connection_id", 30),
            ("TYPE", "conn_type", 20),
            ("HOST", "host", 30),
            ("PORT", "port", 8),
        ],
    )


@conns_app.command("get")
def conns_get(conn_id: str) -> None:
    """Get a connection by ID."""
    with _client() as c:
        resp = c.get(f"connections/{conn_id}")
        data = _check(resp)
    typer.echo(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Entry point (for `python -m airflow_examples.cli`)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app()

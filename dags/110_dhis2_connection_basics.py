"""DAG 110: DHIS2 Connection Basics.

Demonstrates using an Airflow Connection to manage DHIS2 credentials
instead of hardcoding them. The dhis2_default connection is configured
in compose.yml and read at runtime via BaseHook.get_connection().
"""

from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.dhis2 import fetch_metadata


@task
def show_connection() -> dict:
    """Retrieve the dhis2_default connection and display its details."""
    conn = BaseHook.get_connection("dhis2_default")
    info = {
        "conn_id": conn.conn_id,
        "conn_type": conn.conn_type,
        "host": conn.host,
        "login": conn.login,
    }
    print(f"[{timestamp()}] Connection details (password redacted):")
    for key, value in info.items():
        print(f"  {key}: {value}")
    return info


@task
def fetch_org_unit_count() -> int:
    """Fetch organisation units using the connection-backed helper."""
    records = fetch_metadata("organisationUnits", fields="id")
    count = len(records)
    print(f"[{timestamp()}] Organisation unit count: {count}")
    return count


with DAG(
    dag_id="110_dhis2_connection_basics",
    default_args=DEFAULT_ARGS,
    description="Show how DHIS2 credentials come from an Airflow Connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "dhis2", "connections"],
) as dag:
    show_connection() >> fetch_org_unit_count()

"""Shared configuration for Airflow DAG examples.

Provides default DAG arguments, timestamp helper, and context
printing utilities used across all example DAGs.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

OUTPUT_BASE: Path = Path("/opt/airflow/target")

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "airflow_examples",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


def timestamp() -> str:
    """Return current time as HH:MM:SS.fff string."""
    return datetime.now().strftime("%H:%M:%S.%f")[:12]


def print_context(context: dict[str, Any]) -> None:
    """Print key Airflow context variables.

    Args:
        context: The Airflow task instance context dict.
    """
    print(f"  dag_id      = {context.get('dag_id', 'N/A')}")
    print(f"  task_id     = {context.get('task_id', 'N/A')}")
    print(f"  logical_date= {context.get('logical_date', 'N/A')}")
    print(f"  run_id      = {context.get('run_id', 'N/A')}")

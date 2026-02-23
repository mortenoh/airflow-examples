"""Tests for airflow_examples.config module."""

import re
from io import StringIO
from unittest.mock import patch

from airflow_examples.config import DEFAULT_ARGS, print_context, timestamp


def test_default_args_keys() -> None:
    """DEFAULT_ARGS contains required keys."""
    assert "owner" in DEFAULT_ARGS
    assert "retries" in DEFAULT_ARGS
    assert "retry_delay" in DEFAULT_ARGS


def test_default_args_values() -> None:
    """DEFAULT_ARGS has sensible default values."""
    assert DEFAULT_ARGS["owner"] == "airflow_examples"
    assert DEFAULT_ARGS["retries"] == 1


def test_timestamp_format() -> None:
    """timestamp() returns HH:MM:SS.fff format."""
    ts = timestamp()
    assert re.match(r"\d{2}:\d{2}:\d{2}\.\d{3}", ts), f"Unexpected format: {ts}"


def test_print_context() -> None:
    """print_context() prints key context variables."""
    context = {
        "dag_id": "test_dag",
        "task_id": "test_task",
        "logical_date": "2024-01-01T00:00:00",
        "run_id": "test_run_123",
    }
    with patch("sys.stdout", new_callable=StringIO) as mock_out:
        print_context(context)
        output = mock_out.getvalue()

    assert "test_dag" in output
    assert "test_task" in output
    assert "2024-01-01" in output
    assert "test_run_123" in output

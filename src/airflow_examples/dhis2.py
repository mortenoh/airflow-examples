"""DHIS2 API helpers for metadata pipeline examples."""

from typing import Any

import httpx
from airflow.hooks.base import BaseHook

from airflow_examples.config import OUTPUT_BASE

OUTPUT_DIR = str(OUTPUT_BASE / "dhis2_exports")


def _get_dhis2_config() -> tuple[str, tuple[str, str]]:
    """Read DHIS2 base URL and credentials from the Airflow connection."""
    conn = BaseHook.get_connection("dhis2_default")
    base_url = conn.host.rstrip("/")
    credentials = (conn.login, conn.password)
    return base_url, credentials


def fetch_metadata(endpoint: str, fields: str = ":owner") -> list[dict[str, Any]]:
    """Fetch all records from a DHIS2 metadata endpoint.

    Args:
        endpoint: The API endpoint name (e.g. "organisationUnits").
        fields: The fields parameter for the DHIS2 API.

    Returns:
        List of metadata records as dicts.
    """
    base_url, credentials = _get_dhis2_config()
    url = f"{base_url}/api/{endpoint}"
    resp = httpx.get(
        url,
        auth=credentials,
        params={"paging": "false", "fields": fields},
        timeout=60,
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    key = endpoint.split("?")[0]
    result: list[dict[str, Any]] = data[key]
    return result

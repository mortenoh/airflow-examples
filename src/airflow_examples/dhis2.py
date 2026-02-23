"""DHIS2 API helpers for metadata pipeline examples."""

from typing import Any

import httpx

from airflow_examples.config import OUTPUT_BASE

DHIS2_BASE_URL = "https://play.im.dhis2.org/dev"
DHIS2_CREDENTIALS = ("admin", "district")
OUTPUT_DIR = str(OUTPUT_BASE / "dhis2_exports")


def fetch_metadata(endpoint: str, fields: str = ":owner") -> list[dict[str, Any]]:
    """Fetch all records from a DHIS2 metadata endpoint.

    Args:
        endpoint: The API endpoint name (e.g. "organisationUnits").
        fields: The fields parameter for the DHIS2 API.

    Returns:
        List of metadata records as dicts.
    """
    url = f"{DHIS2_BASE_URL}/api/{endpoint}"
    resp = httpx.get(
        url,
        auth=DHIS2_CREDENTIALS,
        params={"paging": "false", "fields": fields},
        timeout=60,
    )
    resp.raise_for_status()
    data: dict[str, Any] = resp.json()
    key = endpoint.split("?")[0]
    result: list[dict[str, Any]] = data[key]
    return result

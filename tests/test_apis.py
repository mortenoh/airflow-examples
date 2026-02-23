"""Tests for the apis.py helper module."""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from airflow_examples.apis import (
    fetch_json,
    fetch_open_meteo,
    fetch_usgs_earthquakes,
    fetch_who_indicator,
    fetch_world_bank_paginated,
    flatten_carbon_intensity,
    flatten_generation_mix,
)


class TestFetchJson:
    @patch("airflow_examples.apis.httpx.get")
    def test_basic_get(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"key": "value"}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_json("https://example.com/api")
        assert result == {"key": "value"}
        mock_get.assert_called_once_with("https://example.com/api", params=None, timeout=30)

    @patch("airflow_examples.apis.httpx.get")
    def test_with_params(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"data": [1, 2, 3]}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_json("https://example.com/api", params={"q": "test"}, timeout=10)
        assert result == {"data": [1, 2, 3]}
        mock_get.assert_called_once_with("https://example.com/api", params={"q": "test"}, timeout=10)

    @patch("airflow_examples.apis.httpx.get")
    def test_http_error(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )
        mock_get.return_value = mock_resp

        with pytest.raises(httpx.HTTPStatusError):
            fetch_json("https://example.com/bad")


class TestFetchOpenMeteo:
    @patch("airflow_examples.apis.fetch_json")
    def test_adds_timezone(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = {"hourly": {"time": [], "temperature_2m": []}}

        result = fetch_open_meteo("https://api.open-meteo.com/v1/forecast", {"latitude": 59.91, "longitude": 10.75})
        assert result == {"hourly": {"time": [], "temperature_2m": []}}

        call_params = mock_fetch.call_args[1]["params"]
        assert call_params["timezone"] == "auto"
        assert call_params["latitude"] == 59.91

    @patch("airflow_examples.apis.fetch_json")
    def test_preserves_existing_params(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = {}
        fetch_open_meteo("https://test.com", {"hourly": "temperature_2m", "forecast_days": 7})

        call_params = mock_fetch.call_args[1]["params"]
        assert call_params["hourly"] == "temperature_2m"
        assert call_params["forecast_days"] == 7
        assert call_params["timezone"] == "auto"


class TestFetchWorldBankPaginated:
    @patch("airflow_examples.apis.httpx.get")
    def test_single_page(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"page": 1, "pages": 1, "total": 2},
            [{"country": {"id": "NOR"}, "date": "2023", "value": 100000}],
        ]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_world_bank_paginated("NOR", "NY.GDP.PCAP.CD", "2023:2023")
        assert len(result) == 1
        assert result[0]["country"]["id"] == "NOR"

    @patch("airflow_examples.apis.httpx.get")
    def test_multi_page(self, mock_get: MagicMock) -> None:
        page1_resp = MagicMock()
        page1_resp.json.return_value = [
            {"page": 1, "pages": 2, "total": 3},
            [{"value": 1}, {"value": 2}],
        ]
        page1_resp.raise_for_status.return_value = None

        page2_resp = MagicMock()
        page2_resp.json.return_value = [
            {"page": 2, "pages": 2, "total": 3},
            [{"value": 3}],
        ]
        page2_resp.raise_for_status.return_value = None

        mock_get.side_effect = [page1_resp, page2_resp]

        result = fetch_world_bank_paginated("NOR", "NY.GDP.PCAP.CD")
        assert len(result) == 3

    @patch("airflow_examples.apis.httpx.get")
    def test_empty_response(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = [{"page": 1, "pages": 1}, None]
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_world_bank_paginated("NOR", "INVALID")
        assert result == []


class TestFetchWhoIndicator:
    @patch("airflow_examples.apis.httpx.get")
    def test_basic_fetch(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": [{"NumericValue": 82.5}]}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = fetch_who_indicator("WHOSIS_000001", country_code="NOR")
        assert len(result) == 1
        assert result[0]["NumericValue"] == 82.5

    @patch("airflow_examples.apis.httpx.get")
    def test_odata_url_construction(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": []}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        fetch_who_indicator("TEST_CODE", country_code="SWE", top=100)
        call_args = mock_get.call_args
        assert "TEST_CODE" in call_args[0][0]
        assert call_args[1]["params"]["$top"] == 100
        assert "SpatialDim eq 'SWE'" in call_args[1]["params"]["$filter"]

    @patch("airflow_examples.apis.httpx.get")
    def test_no_country_filter(self, mock_get: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": []}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        fetch_who_indicator("TEST_CODE")
        call_params = mock_get.call_args[1]["params"]
        assert "$filter" not in call_params


class TestFetchUsgsEarthquakes:
    @patch("airflow_examples.apis.fetch_json")
    def test_flattens_geojson(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = {
            "features": [
                {
                    "properties": {"mag": 5.2, "place": "Pacific Ocean", "time": 1700000000000},
                    "geometry": {"coordinates": [-150.5, 60.1, 10.0]},
                },
                {
                    "properties": {"mag": 4.1, "place": "Iceland", "time": 1700100000000},
                    "geometry": {"coordinates": [-20.0, 64.5, 5.0]},
                },
            ]
        }

        result = fetch_usgs_earthquakes("2024-01-01", "2024-02-01")
        assert len(result) == 2
        assert result[0]["magnitude"] == 5.2
        assert result[0]["lon"] == -150.5
        assert result[0]["lat"] == 60.1
        assert result[0]["depth"] == 10.0
        assert result[0]["place"] == "Pacific Ocean"
        assert result[1]["place"] == "Iceland"

    @patch("airflow_examples.apis.fetch_json")
    def test_empty_features(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = {"features": []}
        result = fetch_usgs_earthquakes("2024-01-01", "2024-02-01")
        assert result == []

    @patch("airflow_examples.apis.fetch_json")
    def test_missing_coordinates(self, mock_fetch: MagicMock) -> None:
        mock_fetch.return_value = {
            "features": [
                {"properties": {"mag": 3.0}, "geometry": {"coordinates": [10.0, 20.0]}},
            ]
        }
        result = fetch_usgs_earthquakes("2024-01-01", "2024-02-01")
        assert result[0]["depth"] == 0


class TestFlattenCarbonIntensity:
    def test_flattens_nested_structure(self) -> None:
        raw = [
            {
                "from": "2024-01-01T00:00Z",
                "to": "2024-01-01T00:30Z",
                "intensity": {"forecast": 200, "actual": 195, "index": "moderate"},
            },
            {
                "from": "2024-01-01T00:30Z",
                "to": "2024-01-01T01:00Z",
                "intensity": {"forecast": 180, "actual": 175, "index": "moderate"},
            },
        ]
        result = flatten_carbon_intensity(raw)
        assert len(result) == 2
        assert result[0]["forecast"] == 200
        assert result[0]["actual"] == 195
        assert result[0]["index"] == "moderate"
        assert result[1]["from"] == "2024-01-01T00:30Z"

    def test_empty_input(self) -> None:
        assert flatten_carbon_intensity([]) == []

    def test_missing_intensity(self) -> None:
        result = flatten_carbon_intensity([{"from": "t1", "to": "t2"}])
        assert result[0]["forecast"] is None
        assert result[0]["actual"] is None


class TestFlattenGenerationMix:
    def test_flattens_fuel_types(self) -> None:
        raw = [
            {
                "from": "2024-01-01T00:00Z",
                "to": "2024-01-01T00:30Z",
                "generationmix": [
                    {"fuel": "gas", "perc": 40.0},
                    {"fuel": "wind", "perc": 30.0},
                    {"fuel": "nuclear", "perc": 20.0},
                ],
            },
        ]
        result = flatten_generation_mix(raw)
        assert len(result) == 3
        assert result[0]["fuel"] == "gas"
        assert result[0]["perc"] == 40.0
        assert result[1]["fuel"] == "wind"
        assert result[2]["from"] == "2024-01-01T00:00Z"

    def test_empty_input(self) -> None:
        assert flatten_generation_mix([]) == []

    def test_empty_mix(self) -> None:
        result = flatten_generation_mix([{"from": "t1", "to": "t2", "generationmix": []}])
        assert result == []

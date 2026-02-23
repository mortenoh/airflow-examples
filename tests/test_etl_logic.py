"""Tests for etl_logic pure business logic functions."""

import os
import tempfile
from unittest.mock import MagicMock, patch

import httpx
import pytest

from airflow_examples.etl_logic import (
    extract_weather_data,
    fetch_api_data,
    load_records,
    transform_records,
    validate_records,
)


class TestExtractWeatherData:
    """Tests for extract_weather_data()."""

    def test_produces_expected_count(self) -> None:
        records = extract_weather_data(n_records=10, seed=42)
        assert len(records) == 10

    def test_records_have_required_fields(self) -> None:
        records = extract_weather_data(n_records=5, seed=42)
        required = {"station", "date", "temperature_c", "humidity_pct", "pressure_hpa", "wind_speed_ms"}
        for record in records:
            assert required.issubset(record.keys())

    def test_seed_reproducibility(self) -> None:
        a = extract_weather_data(n_records=10, seed=99)
        b = extract_weather_data(n_records=10, seed=99)
        assert a == b

    def test_different_seeds_produce_different_data(self) -> None:
        a = extract_weather_data(n_records=10, seed=1)
        b = extract_weather_data(n_records=10, seed=2)
        assert a != b


class TestValidateRecords:
    """Tests for validate_records()."""

    def test_all_valid(self) -> None:
        records = extract_weather_data(n_records=10, seed=42)
        valid, invalid = validate_records(records, ["station", "date", "temperature_c"])
        assert len(valid) == 10
        assert len(invalid) == 0

    def test_missing_field_rejected(self) -> None:
        records = [{"station": "oslo_01", "date": "2024-01-01"}]  # no temperature_c
        valid, invalid = validate_records(records, ["station", "date", "temperature_c"])
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_out_of_range_temp_rejected(self) -> None:
        records = [
            {"station": "test", "date": "2024-01-01", "temperature_c": 100.0},  # too hot
        ]
        valid, invalid = validate_records(records, ["station", "date"], temp_range=(-50, 60))
        assert len(valid) == 0
        assert len(invalid) == 1

    def test_custom_temp_range(self) -> None:
        records = [
            {"station": "test", "date": "2024-01-01", "temperature_c": 25.0},
        ]
        valid, invalid = validate_records(records, ["station"], temp_range=(0, 30))
        assert len(valid) == 1

    def test_none_value_rejected(self) -> None:
        records = [
            {"station": "test", "date": None, "temperature_c": 10.0},
        ]
        valid, invalid = validate_records(records, ["station", "date"])
        assert len(valid) == 0
        assert len(invalid) == 1


class TestTransformRecords:
    """Tests for transform_records()."""

    def test_adds_fahrenheit(self) -> None:
        records = [{"temperature_c": 0.0}]
        transformed = transform_records(records)
        assert transformed[0]["temperature_f"] == 32.0

    def test_fahrenheit_conversion_accuracy(self) -> None:
        records = [{"temperature_c": 100.0}]
        transformed = transform_records(records)
        assert transformed[0]["temperature_f"] == 212.0

    def test_wind_category_calm(self) -> None:
        records = [{"wind_speed_ms": 0.5}]
        transformed = transform_records(records)
        assert transformed[0]["wind_category"] == "calm"

    def test_wind_category_storm(self) -> None:
        records = [{"wind_speed_ms": 22.0}]
        transformed = transform_records(records)
        assert transformed[0]["wind_category"] == "storm"

    def test_does_not_mutate_input(self) -> None:
        records = [{"temperature_c": 10.0, "wind_speed_ms": 3.0}]
        original_keys = set(records[0].keys())
        transform_records(records)
        assert set(records[0].keys()) == original_keys


class TestLoadRecords:
    """Tests for load_records()."""

    def test_writes_csv_with_expected_content(self) -> None:
        records = [
            {"station": "oslo_01", "temp": 10.0},
            {"station": "bergen_01", "temp": 8.0},
        ]
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = load_records(records, path)
            assert result["rows"] == 2
            assert result["columns"] == 2
            assert os.path.exists(path)

            with open(path) as f:
                content = f.read()
            assert "station" in content
            assert "oslo_01" in content
        finally:
            os.unlink(path)

    def test_empty_records(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            path = f.name
        try:
            result = load_records([], path)
            assert result["rows"] == 0
        finally:
            if os.path.exists(path):
                os.unlink(path)


class TestFetchApiData:
    """Tests for fetch_api_data() with mocked httpx."""

    @patch("airflow_examples.etl_logic.httpx.get")
    def test_returns_json_response(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.json.return_value = {"url": "http://test.com", "data": "ok"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_api_data("http://test.com/api", timeout=5)
        assert result["url"] == "http://test.com"
        mock_get.assert_called_once_with("http://test.com/api", timeout=5)

    @patch("airflow_examples.etl_logic.httpx.get")
    def test_raises_on_error(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )
        mock_get.return_value = mock_response

        with pytest.raises(httpx.HTTPStatusError):
            fetch_api_data("http://test.com/bad")

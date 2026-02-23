"""Tests for custom hooks and sensors."""

from unittest.mock import MagicMock

from airflow_examples.hooks import WeatherApiHook
from airflow_examples.sensors import WeatherDataSensor


def test_hook_check_data_available() -> None:
    """WeatherApiHook.check_data_available() returns True."""
    hook = WeatherApiHook()
    assert hook.check_data_available("oslo_01", "2024-01-01") is True


def test_hook_fetch_data_returns_records() -> None:
    """WeatherApiHook.fetch_data() returns a non-empty list."""
    hook = WeatherApiHook()
    data = hook.fetch_data("oslo_01", "2024-01-01")
    assert len(data) > 0
    assert "temp" in data[0]
    assert "humidity" in data[0]


def test_hook_get_conn_fallback() -> None:
    """WeatherApiHook.get_conn() returns fallback when no real connection."""
    hook = WeatherApiHook()
    conn = hook.get_conn()
    assert "host" in conn
    assert "port" in conn
    assert "token" in conn


def test_sensor_template_fields() -> None:
    """WeatherDataSensor has templated fields for station_id and check_date."""
    assert "station_id" in WeatherDataSensor.template_fields
    assert "check_date" in WeatherDataSensor.template_fields


def test_sensor_poke_returns_true() -> None:
    """WeatherDataSensor.poke() returns True (data is available)."""
    sensor = WeatherDataSensor(
        task_id="test_sensor",
        station_id="oslo_01",
        check_date="2024-01-01",
    )
    result = sensor.poke(context=MagicMock())
    assert result is True

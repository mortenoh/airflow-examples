"""Custom hooks for Airflow DAG examples.

Demonstrates how to build reusable hooks by subclassing BaseHook.
Hooks encapsulate connection logic and are shared across operators
and sensors.
"""

from typing import Any

from airflow.hooks.base import BaseHook  # type: ignore[attr-defined]


class WeatherApiHook(BaseHook):
    """Hook for connecting to a simulated weather data API.

    In production, this would manage HTTP sessions, authentication,
    retries, and connection pooling for an external weather API.

    Args:
        weather_conn_id: Airflow connection ID for the weather API.
    """

    conn_name_attr = "weather_conn_id"
    default_conn_name = "weather_default"
    conn_type = "http"
    hook_name = "Weather API"

    def __init__(self, weather_conn_id: str = "weather_default") -> None:
        """Initialize WeatherApiHook.

        Args:
            weather_conn_id: Airflow connection ID for the weather API.
        """
        super().__init__()
        self.weather_conn_id = weather_conn_id

    def get_conn(self) -> dict[str, Any]:
        """Get connection parameters from Airflow's connection store.

        Returns:
            A dict with host, port, and token from the Airflow connection.
        """
        try:
            conn = self.get_connection(self.weather_conn_id)
            return {"host": conn.host, "port": conn.port, "token": conn.password}
        except Exception:
            # Fallback for testing without a real connection
            return {"host": "api.weather.example.com", "port": 443, "token": "test"}

    def check_data_available(self, station_id: str, date: str) -> bool:
        """Check if weather data is available for a station and date.

        Args:
            station_id: The weather station identifier.
            date: The date to check (YYYY-MM-DD format).

        Returns:
            True if data is available, False otherwise.
        """
        self.log.info("Checking data for station=%s, date=%s", station_id, date)
        # Simulated: always available
        return True

    def fetch_data(self, station_id: str, date: str) -> list[dict[str, Any]]:
        """Fetch weather data for a station and date.

        Args:
            station_id: The weather station identifier.
            date: The date to fetch (YYYY-MM-DD format).

        Returns:
            A list of weather observation dicts.
        """
        self.log.info("Fetching data for station=%s, date=%s", station_id, date)
        # Simulated data
        return [
            {"station": station_id, "date": date, "temp": 12.5, "humidity": 68.0},
            {"station": station_id, "date": date, "temp": 13.1, "humidity": 65.0},
        ]

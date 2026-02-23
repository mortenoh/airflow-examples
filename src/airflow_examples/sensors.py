"""Custom sensors for Airflow DAG examples.

Demonstrates how to build custom sensors by subclassing
BaseSensorOperator. Sensors wait for an external condition
before allowing downstream tasks to proceed.
"""

from typing import Any

from airflow.sdk import BaseSensorOperator

from airflow_examples.hooks import WeatherApiHook


class WeatherDataSensor(BaseSensorOperator):
    """Sensor that waits for weather data to become available.

    Uses the WeatherApiHook to check if data for a given station
    and date has been published. Supports Jinja templating on
    station_id and check_date.

    Args:
        station_id: Weather station identifier (supports Jinja templates).
        check_date: Date to check for data availability (supports Jinja templates).
        weather_conn_id: Airflow connection ID for the weather API.
    """

    template_fields = ("station_id", "check_date")

    def __init__(
        self,
        station_id: str,
        check_date: str,
        weather_conn_id: str = "weather_default",
        **kwargs: Any,
    ) -> None:
        """Initialize WeatherDataSensor.

        Args:
            station_id: Weather station identifier.
            check_date: Date to check (YYYY-MM-DD).
            weather_conn_id: Airflow connection ID.
            **kwargs: Additional BaseSensorOperator arguments.
        """
        super().__init__(**kwargs)
        self.station_id = station_id
        self.check_date = check_date
        self.weather_conn_id = weather_conn_id

    def poke(self, context: Any) -> bool:
        """Check if weather data is available.

        Args:
            context: The Airflow task instance context.

        Returns:
            True if data is available, False to keep waiting.
        """
        hook = WeatherApiHook(self.weather_conn_id)
        available = hook.check_data_available(self.station_id, self.check_date)
        self.log.info(
            "Data for %s on %s: %s",
            self.station_id,
            self.check_date,
            "available" if available else "not yet",
        )
        return available

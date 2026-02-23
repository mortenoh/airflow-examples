"""Custom triggers for deferrable operator examples.

Demonstrates how to write a trigger that runs in the Airflow
triggerer process, enabling deferrable (async) operator patterns.
"""

import asyncio
from datetime import datetime
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent


class CountdownTrigger(BaseTrigger):
    """Trigger that counts down for a given number of seconds.

    Args:
        seconds: Number of seconds to wait before firing.
        message: Message to include in the trigger event.
    """

    def __init__(self, seconds: int, message: str) -> None:
        """Initialize CountdownTrigger.

        Args:
            seconds: Number of seconds to wait.
            message: Message to include in the event payload.
        """
        super().__init__()
        self.seconds = seconds
        self.message = message

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for storage.

        Returns:
            A tuple of (classpath, kwargs) for reconstruction.
        """
        return (
            "airflow_examples.triggers.CountdownTrigger",
            {"seconds": self.seconds, "message": self.message},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait for the countdown then fire an event.

        Yields:
            A TriggerEvent with the countdown result.
        """
        start = datetime.now()
        await asyncio.sleep(self.seconds)
        elapsed = (datetime.now() - start).total_seconds()
        yield TriggerEvent(
            {
                "status": "complete",
                "message": self.message,
                "elapsed_seconds": round(elapsed, 2),
            }
        )

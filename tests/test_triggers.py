"""Tests for custom triggers."""

import asyncio

from airflow_examples.triggers import CountdownTrigger


def test_trigger_serialize() -> None:
    """CountdownTrigger.serialize() returns classpath and kwargs."""
    trigger = CountdownTrigger(seconds=5, message="test")
    classpath, kwargs = trigger.serialize()
    assert classpath == "airflow_examples.triggers.CountdownTrigger"
    assert kwargs["seconds"] == 5
    assert kwargs["message"] == "test"


def test_trigger_run_completes() -> None:
    """CountdownTrigger.run() yields a TriggerEvent after countdown."""

    async def run_trigger() -> dict:  # type: ignore[type-arg]
        trigger = CountdownTrigger(seconds=0, message="done")
        async for event in trigger.run():
            return event.payload  # type: ignore[return-value]
        return {}

    result = asyncio.run(run_trigger())
    assert result["status"] == "complete"
    assert result["message"] == "done"


def test_trigger_run_elapsed_time() -> None:
    """CountdownTrigger reports elapsed time."""

    async def run_trigger() -> dict:  # type: ignore[type-arg]
        trigger = CountdownTrigger(seconds=0, message="fast")
        async for event in trigger.run():
            return event.payload  # type: ignore[return-value]
        return {}

    result = asyncio.run(run_trigger())
    assert "elapsed_seconds" in result
    assert result["elapsed_seconds"] >= 0

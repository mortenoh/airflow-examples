"""DAG 42: Custom Deferrable Operator.

Demonstrates writing a custom deferrable operator that defers
execution to the triggerer process. The operator starts, defers
to a custom trigger, and resumes when the trigger fires.
This is Airflow 3.x's pattern for efficient async operations.
"""

from datetime import datetime
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, BaseOperator

from airflow_examples.config import DEFAULT_ARGS, timestamp
from airflow_examples.triggers import CountdownTrigger


class CountdownOperator(BaseOperator):
    """Operator that defers a countdown to the triggerer.

    Demonstrates the deferrable operator pattern: execute() defers,
    and execute_complete() handles the trigger result.

    Args:
        seconds: Number of seconds for the countdown.
        message: Message to pass through the trigger.
    """

    def __init__(self, seconds: int = 2, message: str = "default", **kwargs: Any) -> None:
        """Initialize CountdownOperator.

        Args:
            seconds: Number of seconds to count down.
            message: Message to include in trigger event.
            **kwargs: Additional BaseOperator arguments.
        """
        super().__init__(**kwargs)
        self.seconds = seconds
        self.message = message

    def execute(self, context: Any) -> None:
        """Start the operator and defer to the triggerer.

        Args:
            context: The Airflow task instance context.
        """
        print(f"[{timestamp()}] Deferring {self.seconds}s countdown to triggerer...")
        self.defer(
            trigger=CountdownTrigger(seconds=self.seconds, message=self.message),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict[str, Any]) -> str:
        """Resume after the trigger fires.

        Args:
            context: The Airflow task instance context.
            event: The trigger event payload.

        Returns:
            A summary string pushed to XCom.
        """
        print(f"[{timestamp()}] Trigger fired!")
        print(f"  status: {event['status']}")
        print(f"  message: {event['message']}")
        print(f"  elapsed: {event['elapsed_seconds']}s")
        return f"Countdown complete: {event['message']} ({event['elapsed_seconds']}s)"


def show_result(**context: object) -> None:
    """Display the result from the deferrable operator."""
    ti = context["ti"]  # type: ignore[index]
    result = ti.xcom_pull(task_ids="countdown_fast")
    print(f"[{timestamp()}] Fast countdown result: {result}")
    result = ti.xcom_pull(task_ids="countdown_slow")
    print(f"[{timestamp()}] Slow countdown result: {result}")


with DAG(
    dag_id="042_custom_deferrable",
    default_args=DEFAULT_ARGS,
    description="Custom deferrable operator with CountdownTrigger",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "deferrable"],
) as dag:
    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting custom deferrable operator demo"',
    )

    countdown_fast = CountdownOperator(
        task_id="countdown_fast",
        seconds=1,
        message="fast countdown",
    )

    countdown_slow = CountdownOperator(
        task_id="countdown_slow",
        seconds=3,
        message="slow countdown",
    )

    show = PythonOperator(
        task_id="show_result",
        python_callable=show_result,
    )

    start >> [countdown_fast, countdown_slow] >> show

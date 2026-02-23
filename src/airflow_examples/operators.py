"""Custom operators for Airflow DAG examples.

Demonstrates how to build reusable operators by subclassing
BaseOperator from the Airflow SDK.
"""

from typing import Any

from airflow.sdk import BaseOperator


class PrintOperator(BaseOperator):
    """Simple operator that prints a message.

    Args:
        message: The message to print during execution.
    """

    def __init__(self, message: str, **kwargs: Any) -> None:
        """Initialize PrintOperator.

        Args:
            message: The message to print during execution.
            **kwargs: Additional BaseOperator arguments.
        """
        super().__init__(**kwargs)
        self.message = message

    def execute(self, context: Any) -> str:
        """Print the message and return it.

        Args:
            context: The Airflow task instance context.

        Returns:
            The message that was printed.
        """
        print(f"PrintOperator: {self.message}")
        return self.message


class SquareOperator(BaseOperator):
    """Operator that computes the square of a number.

    Args:
        number: The number to square.
    """

    def __init__(self, number: int, **kwargs: Any) -> None:
        """Initialize SquareOperator.

        Args:
            number: The number to square.
            **kwargs: Additional BaseOperator arguments.
        """
        super().__init__(**kwargs)
        self.number = number

    def execute(self, context: Any) -> int:
        """Compute and return the square of the number.

        Args:
            context: The Airflow task instance context.

        Returns:
            The squared value.
        """
        result = self.number**2
        print(f"SquareOperator: {self.number}^2 = {result}")
        return result

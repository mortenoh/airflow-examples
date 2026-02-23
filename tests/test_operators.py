"""Tests for custom operators.

Unit tests that exercise PrintOperator and SquareOperator
by calling execute() directly with a mock context.
"""

from unittest.mock import MagicMock

from airflow_examples.operators import PrintOperator, SquareOperator


def test_print_operator_returns_message() -> None:
    """PrintOperator.execute() returns the message."""
    op = PrintOperator(task_id="test", message="hello airflow")
    result = op.execute(context=MagicMock())
    assert result == "hello airflow"


def test_print_operator_prints_message(capsys: object) -> None:
    """PrintOperator.execute() prints the message to stdout."""
    import sys
    from io import StringIO

    captured = StringIO()
    old_stdout = sys.stdout
    sys.stdout = captured
    try:
        op = PrintOperator(task_id="test", message="test output")
        op.execute(context=MagicMock())
    finally:
        sys.stdout = old_stdout
    assert "test output" in captured.getvalue()


def test_square_operator_computes_square() -> None:
    """SquareOperator.execute() returns the squared value."""
    op = SquareOperator(task_id="test", number=7)
    result = op.execute(context=MagicMock())
    assert result == 49


def test_square_operator_zero() -> None:
    """SquareOperator handles zero correctly."""
    op = SquareOperator(task_id="test", number=0)
    result = op.execute(context=MagicMock())
    assert result == 0


def test_square_operator_negative() -> None:
    """SquareOperator handles negative numbers."""
    op = SquareOperator(task_id="test", number=-5)
    result = op.execute(context=MagicMock())
    assert result == 25

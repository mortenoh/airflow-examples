"""Data quality validation utilities for pipeline checks."""

import os
import time
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class QualityResult:
    """Result of a single data quality check.

    Attributes:
        check_name: Human-readable name of the check.
        passed: Whether the check passed.
        details: Description of the result or violations found.
        severity: One of "info", "warning", or "critical".
    """

    check_name: str
    passed: bool
    details: str
    severity: str  # "info", "warning", "critical"


def check_schema(df: pd.DataFrame, expected_columns: list[str]) -> QualityResult:
    """Verify that a DataFrame has the expected column names.

    Args:
        df: DataFrame to validate.
        expected_columns: List of required column names.

    Returns:
        QualityResult with details on missing or extra columns.
    """
    actual = set(df.columns)
    expected = set(expected_columns)
    missing = expected - actual
    extra = actual - expected

    if missing:
        return QualityResult(
            check_name="schema_columns",
            passed=False,
            details=f"Missing columns: {sorted(missing)}. Extra columns: {sorted(extra)}",
            severity="critical",
        )
    if extra:
        return QualityResult(
            check_name="schema_columns",
            passed=True,
            details=f"All required columns present. Extra columns: {sorted(extra)}",
            severity="warning",
        )
    return QualityResult(
        check_name="schema_columns",
        passed=True,
        details=f"All {len(expected_columns)} expected columns present",
        severity="info",
    )


def check_bounds(df: pd.DataFrame, column: str, min_val: float, max_val: float) -> QualityResult:
    """Validate that values in a column fall within a given range.

    Args:
        df: DataFrame to validate.
        column: Column name to check.
        min_val: Minimum allowed value (inclusive).
        max_val: Maximum allowed value (inclusive).

    Returns:
        QualityResult with count of out-of-range values.
    """
    if column not in df.columns:
        return QualityResult(
            check_name=f"bounds_{column}",
            passed=False,
            details=f"Column '{column}' not found",
            severity="critical",
        )

    numeric = pd.to_numeric(df[column], errors="coerce")
    violations = ((numeric < min_val) | (numeric > max_val)).sum()

    if violations > 0:
        return QualityResult(
            check_name=f"bounds_{column}",
            passed=False,
            details=f"{violations} values outside [{min_val}, {max_val}]",
            severity="warning",
        )
    return QualityResult(
        check_name=f"bounds_{column}",
        passed=True,
        details=f"All values within [{min_val}, {max_val}]",
        severity="info",
    )


def check_nulls(df: pd.DataFrame, columns: list[str], max_null_pct: float) -> QualityResult:
    """Check null percentage per column against a threshold.

    Args:
        df: DataFrame to validate.
        columns: Column names to check for nulls.
        max_null_pct: Maximum allowed null percentage (0-100).

    Returns:
        QualityResult with per-column null rates.
    """
    violations: list[str] = []
    total_rows = len(df)
    if total_rows == 0:
        return QualityResult(
            check_name="null_check",
            passed=True,
            details="Empty DataFrame, no nulls to check",
            severity="info",
        )

    for col in columns:
        if col not in df.columns:
            violations.append(f"{col}: column missing")
            continue
        null_count = int(df[col].isnull().sum())
        null_pct = (null_count / total_rows) * 100
        if null_pct > max_null_pct:
            violations.append(f"{col}: {null_pct:.1f}% null (max {max_null_pct}%)")

    if violations:
        return QualityResult(
            check_name="null_check",
            passed=False,
            details=f"Null violations: {'; '.join(violations)}",
            severity="warning",
        )
    return QualityResult(
        check_name="null_check",
        passed=True,
        details=f"All {len(columns)} columns within null threshold ({max_null_pct}%)",
        severity="info",
    )


def check_freshness(file_path: str, max_age_seconds: float) -> QualityResult:
    """Check that a file was modified within a given time window.

    Args:
        file_path: Path to the file to check.
        max_age_seconds: Maximum allowed age in seconds.

    Returns:
        QualityResult indicating whether the file is fresh.
    """
    if not os.path.exists(file_path):
        return QualityResult(
            check_name="freshness",
            passed=False,
            details=f"File not found: {file_path}",
            severity="critical",
        )

    mtime = os.path.getmtime(file_path)
    age = time.time() - mtime

    if age > max_age_seconds:
        return QualityResult(
            check_name="freshness",
            passed=False,
            details=f"File is {age:.0f}s old (max {max_age_seconds:.0f}s)",
            severity="warning",
        )
    return QualityResult(
        check_name="freshness",
        passed=True,
        details=f"File is {age:.0f}s old (within {max_age_seconds:.0f}s threshold)",
        severity="info",
    )


def check_row_count(df: pd.DataFrame, min_rows: int, max_rows: int) -> QualityResult:
    """Validate that a DataFrame has a row count within expected bounds.

    Args:
        df: DataFrame to validate.
        min_rows: Minimum expected row count.
        max_rows: Maximum expected row count.

    Returns:
        QualityResult with actual row count.
    """
    n = len(df)
    if n < min_rows:
        return QualityResult(
            check_name="row_count",
            passed=False,
            details=f"Only {n} rows (minimum {min_rows})",
            severity="critical",
        )
    if n > max_rows:
        return QualityResult(
            check_name="row_count",
            passed=False,
            details=f"{n} rows exceeds maximum {max_rows}",
            severity="warning",
        )
    return QualityResult(
        check_name="row_count",
        passed=True,
        details=f"{n} rows (within [{min_rows}, {max_rows}])",
        severity="info",
    )


def format_quality_report(results: list[QualityResult]) -> str:
    """Format a list of QualityResults into a human-readable report.

    Args:
        results: List of quality check results.

    Returns:
        Formatted multi-line report string with pass/fail/warn counts.
    """
    lines: list[str] = ["=== Data Quality Report ===", ""]

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)
    warnings = sum(1 for r in results if r.severity == "warning")
    critical = sum(1 for r in results if r.severity == "critical" and not r.passed)

    lines.append(f"Total checks: {len(results)}")
    lines.append(f"Passed: {passed}  |  Failed: {failed}")
    lines.append(f"Warnings: {warnings}  |  Critical: {critical}")
    lines.append("")

    for r in results:
        status = "PASS" if r.passed else "FAIL"
        lines.append(f"  [{status}] [{r.severity.upper()}] {r.check_name}")
        lines.append(f"         {r.details}")

    overall: Any = "PASS" if failed == 0 else "FAIL"
    lines.append("")
    lines.append(f"Overall: {overall}")
    return "\n".join(lines)

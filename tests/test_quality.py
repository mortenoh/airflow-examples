"""Tests for quality check functions and QualityResult dataclass."""

import os
import tempfile
import time

import pandas as pd

from airflow_examples.quality import (
    QualityResult,
    check_bounds,
    check_freshness,
    check_nulls,
    check_row_count,
    check_schema,
    format_quality_report,
)


class TestQualityResult:
    """Tests for the QualityResult dataclass."""

    def test_create_passing_result(self) -> None:
        r = QualityResult(check_name="test", passed=True, details="ok", severity="info")
        assert r.passed is True
        assert r.severity == "info"

    def test_create_failing_result(self) -> None:
        r = QualityResult(check_name="test", passed=False, details="bad", severity="critical")
        assert r.passed is False
        assert r.severity == "critical"


class TestCheckSchema:
    """Tests for check_schema()."""

    def test_correct_columns_pass(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        r = check_schema(df, ["a", "b", "c"])
        assert r.passed is True

    def test_missing_columns_fail(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2]})
        r = check_schema(df, ["a", "b", "c"])
        assert r.passed is False
        assert "Missing" in r.details

    def test_extra_columns_pass_with_warning(self) -> None:
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3], "d": [4]})
        r = check_schema(df, ["a", "b", "c"])
        assert r.passed is True
        assert r.severity == "warning"
        assert "Extra" in r.details


class TestCheckBounds:
    """Tests for check_bounds()."""

    def test_in_range_passes(self) -> None:
        df = pd.DataFrame({"temp": [10.0, 20.0, 30.0]})
        r = check_bounds(df, "temp", 0.0, 50.0)
        assert r.passed is True

    def test_out_of_range_fails(self) -> None:
        df = pd.DataFrame({"temp": [10.0, 20.0, 100.0]})
        r = check_bounds(df, "temp", 0.0, 50.0)
        assert r.passed is False
        assert "1 values outside" in r.details

    def test_missing_column_fails(self) -> None:
        df = pd.DataFrame({"other": [1]})
        r = check_bounds(df, "temp", 0.0, 50.0)
        assert r.passed is False
        assert r.severity == "critical"


class TestCheckNulls:
    """Tests for check_nulls()."""

    def test_no_nulls_passes(self) -> None:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        r = check_nulls(df, ["a", "b"], max_null_pct=0.0)
        assert r.passed is True

    def test_exceeds_threshold_fails(self) -> None:
        df = pd.DataFrame({"a": [1, None, None], "b": [4, 5, 6]})
        r = check_nulls(df, ["a"], max_null_pct=10.0)
        assert r.passed is False

    def test_within_threshold_passes(self) -> None:
        df = pd.DataFrame({"a": [1, None, 3, 4, 5, 6, 7, 8, 9, 10]})
        r = check_nulls(df, ["a"], max_null_pct=15.0)
        assert r.passed is True

    def test_empty_dataframe(self) -> None:
        df = pd.DataFrame({"a": pd.Series(dtype="float64")})
        r = check_nulls(df, ["a"], max_null_pct=0.0)
        assert r.passed is True


class TestCheckFreshness:
    """Tests for check_freshness()."""

    def test_fresh_file_passes(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            path = f.name
        try:
            r = check_freshness(path, max_age_seconds=60.0)
            assert r.passed is True
        finally:
            os.unlink(path)

    def test_stale_file_fails(self) -> None:
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            path = f.name
        try:
            old_time = time.time() - 7200  # 2 hours ago
            os.utime(path, (old_time, old_time))
            r = check_freshness(path, max_age_seconds=3600.0)
            assert r.passed is False
        finally:
            os.unlink(path)

    def test_missing_file_fails(self) -> None:
        r = check_freshness("/nonexistent/file.csv", max_age_seconds=60.0)
        assert r.passed is False
        assert r.severity == "critical"


class TestCheckRowCount:
    """Tests for check_row_count()."""

    def test_within_bounds_passes(self) -> None:
        df = pd.DataFrame({"a": range(50)})
        r = check_row_count(df, min_rows=10, max_rows=100)
        assert r.passed is True

    def test_too_few_rows_fails(self) -> None:
        df = pd.DataFrame({"a": range(5)})
        r = check_row_count(df, min_rows=10, max_rows=100)
        assert r.passed is False
        assert r.severity == "critical"

    def test_too_many_rows_fails(self) -> None:
        df = pd.DataFrame({"a": range(200)})
        r = check_row_count(df, min_rows=10, max_rows=100)
        assert r.passed is False
        assert r.severity == "warning"


class TestFormatQualityReport:
    """Tests for format_quality_report()."""

    def test_output_contains_counts(self) -> None:
        results = [
            QualityResult("check_a", True, "all good", "info"),
            QualityResult("check_b", False, "bad data", "warning"),
        ]
        report = format_quality_report(results)
        assert "Total checks: 2" in report
        assert "Passed: 1" in report
        assert "Failed: 1" in report

    def test_pass_fail_markers(self) -> None:
        results = [
            QualityResult("ok_check", True, "fine", "info"),
            QualityResult("bad_check", False, "broken", "critical"),
        ]
        report = format_quality_report(results)
        assert "[PASS]" in report
        assert "[FAIL]" in report

    def test_all_passing_shows_pass_overall(self) -> None:
        results = [QualityResult("check", True, "ok", "info")]
        report = format_quality_report(results)
        assert "Overall: PASS" in report

    def test_any_failing_shows_fail_overall(self) -> None:
        results = [
            QualityResult("check_a", True, "ok", "info"),
            QualityResult("check_b", False, "bad", "critical"),
        ]
        report = format_quality_report(results)
        assert "Overall: FAIL" in report

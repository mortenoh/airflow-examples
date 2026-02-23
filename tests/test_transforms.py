"""Tests for the transforms.py pure business logic module."""

import pytest

from airflow_examples.transforms import (
    apply_scd_type2_rows,
    check_who_exceedance,
    classify_european_aqi,
    classify_risk,
    compute_annual_trend,
    compute_b_value,
    generate_surrogate_key,
    safe_pearson,
    score_flood_ratio,
    score_wave_height,
)


class TestClassifyEuropeanAqi:
    def test_good(self) -> None:
        assert classify_european_aqi(10) == "Good"

    def test_fair(self) -> None:
        assert classify_european_aqi(30) == "Fair"

    def test_moderate(self) -> None:
        assert classify_european_aqi(50) == "Moderate"

    def test_poor(self) -> None:
        assert classify_european_aqi(70) == "Poor"

    def test_very_poor(self) -> None:
        assert classify_european_aqi(90) == "Very Poor"

    def test_extremely_poor(self) -> None:
        assert classify_european_aqi(150) == "Extremely Poor"

    def test_boundary_good(self) -> None:
        assert classify_european_aqi(20) == "Good"

    def test_boundary_fair(self) -> None:
        assert classify_european_aqi(40) == "Fair"

    def test_zero(self) -> None:
        assert classify_european_aqi(0) == "Good"


class TestCheckWhoExceedance:
    def test_pm25_exceeds(self) -> None:
        assert check_who_exceedance("pm2_5", 20.0) is True

    def test_pm25_below(self) -> None:
        assert check_who_exceedance("pm2_5", 10.0) is False

    def test_pm10_at_threshold(self) -> None:
        assert check_who_exceedance("pm10", 45.0) is False

    def test_unknown_pollutant(self) -> None:
        assert check_who_exceedance("unknown", 100.0) is False

    def test_nitrogen_dioxide_exceeds(self) -> None:
        assert check_who_exceedance("nitrogen_dioxide", 30.0) is True


class TestScoreWaveHeight:
    def test_calm(self) -> None:
        assert score_wave_height(0.5) == 0

    def test_low(self) -> None:
        assert score_wave_height(1.5) == 25

    def test_moderate(self) -> None:
        assert score_wave_height(3.0) == 50

    def test_high(self) -> None:
        assert score_wave_height(5.0) == 75

    def test_extreme(self) -> None:
        assert score_wave_height(8.0) == 100

    def test_zero(self) -> None:
        assert score_wave_height(0) == 0


class TestScoreFloodRatio:
    def test_normal(self) -> None:
        assert score_flood_ratio(100, 100) == 0

    def test_elevated(self) -> None:
        assert score_flood_ratio(200, 100) == 50

    def test_extreme(self) -> None:
        assert score_flood_ratio(400, 100) == 100

    def test_zero_mean(self) -> None:
        assert score_flood_ratio(100, 0) == 0

    def test_boundary_1_5(self) -> None:
        assert score_flood_ratio(150, 100) == 50

    def test_below_1_5(self) -> None:
        assert score_flood_ratio(140, 100) == 0


class TestClassifyRisk:
    def test_low(self) -> None:
        assert classify_risk(10) == "Low"

    def test_moderate(self) -> None:
        assert classify_risk(30) == "Moderate"

    def test_high(self) -> None:
        assert classify_risk(60) == "High"

    def test_extreme(self) -> None:
        assert classify_risk(80) == "Extreme"

    def test_boundary_low(self) -> None:
        assert classify_risk(25) == "Low"

    def test_zero(self) -> None:
        assert classify_risk(0) == "Low"


class TestComputeBValue:
    def test_typical_distribution(self) -> None:
        magnitudes = [4.0] * 100 + [4.5] * 30 + [5.0] * 10 + [5.5] * 3 + [6.0] * 1
        b = compute_b_value(magnitudes)
        assert 0.5 < b < 2.0

    def test_empty_list(self) -> None:
        assert compute_b_value([]) == 0.0

    def test_single_magnitude(self) -> None:
        assert compute_b_value([5.0]) == 0.0

    def test_two_magnitudes(self) -> None:
        result = compute_b_value([4.0, 5.0])
        assert isinstance(result, float)


class TestSafePearson:
    def test_perfect_positive(self) -> None:
        result = safe_pearson([1.0, 2.0, 3.0, 4.0], [2.0, 4.0, 6.0, 8.0])
        assert result["r"] is not None
        assert abs(result["r"] - 1.0) < 0.001

    def test_no_correlation(self) -> None:
        result = safe_pearson([1.0, 2.0, 3.0, 4.0], [4.0, 1.0, 3.0, 2.0])
        assert result["r"] is not None
        assert result["n"] == 4

    def test_too_short(self) -> None:
        result = safe_pearson([1.0, 2.0], [3.0, 4.0])
        assert result["r"] is None

    def test_mismatched_lengths(self) -> None:
        result = safe_pearson([1.0, 2.0, 3.0], [1.0, 2.0])
        assert result["r"] is None

    def test_with_nan(self) -> None:
        result = safe_pearson([1.0, float("nan"), 3.0, 4.0], [2.0, 4.0, 6.0, 8.0])
        assert result["n"] == 3

    def test_constant_values(self) -> None:
        result = safe_pearson([1.0, 1.0, 1.0, 1.0], [2.0, 4.0, 6.0, 8.0])
        assert result["r"] is None

    def test_empty(self) -> None:
        result = safe_pearson([], [])
        assert result["r"] is None


class TestComputeAnnualTrend:
    def test_positive_trend(self) -> None:
        result = compute_annual_trend([2000, 2005, 2010, 2015, 2020], [5.0, 5.5, 6.0, 6.5, 7.0])
        assert result["slope"] > 0
        assert result["trend_per_decade"] == pytest.approx(1.0, abs=0.01)

    def test_flat_trend(self) -> None:
        result = compute_annual_trend([2000, 2010, 2020], [5.0, 5.0, 5.0])
        assert result["slope"] == pytest.approx(0.0, abs=0.001)

    def test_insufficient_data(self) -> None:
        result = compute_annual_trend([2020], [5.0])
        assert result["slope"] == 0.0
        assert result["trend_per_decade"] == 0.0

    def test_empty(self) -> None:
        result = compute_annual_trend([], [])
        assert result["slope"] == 0.0


class TestGenerateSurrogateKey:
    def test_deterministic(self) -> None:
        key1 = generate_surrogate_key("NOR")
        key2 = generate_surrogate_key("NOR")
        assert key1 == key2

    def test_different_codes(self) -> None:
        key1 = generate_surrogate_key("NOR")
        key2 = generate_surrogate_key("SWE")
        assert key1 != key2

    def test_range(self) -> None:
        key = generate_surrogate_key("NOR")
        assert 0 <= key < 100000

    def test_empty_string(self) -> None:
        key = generate_surrogate_key("")
        assert 0 <= key < 100000


class TestApplyScdType2Rows:
    def test_no_changes(self) -> None:
        current = [{"country_code": "NOR", "population": 5000000}]
        previous = [{"country_code": "NOR", "population": 5000000}]
        result = apply_scd_type2_rows(current, previous)
        assert len(result) == 1
        assert result[0]["is_current"] is True
        assert result[0]["valid_to"] == "9999-12-31"

    def test_with_change(self) -> None:
        current = [{"country_code": "NOR", "population": 5500000}]
        previous = [{"country_code": "NOR", "population": 5000000}]
        result = apply_scd_type2_rows(current, previous)
        assert len(result) == 2
        assert result[0]["is_current"] is False
        assert result[0]["valid_to"] == "2025-01-01"
        assert result[1]["is_current"] is True
        assert result[1]["population"] == 5500000

    def test_empty_lists(self) -> None:
        result = apply_scd_type2_rows([], [])
        assert result == []

    def test_multiple_countries(self) -> None:
        current = [
            {"country_code": "NOR", "population": 5000000},
            {"country_code": "SWE", "population": 10500000},
        ]
        previous = [
            {"country_code": "NOR", "population": 5000000},
            {"country_code": "SWE", "population": 10000000},
        ]
        result = apply_scd_type2_rows(current, previous)
        assert len(result) == 3
        nor_rows = [r for r in result if r["country_code"] == "NOR"]
        swe_rows = [r for r in result if r["country_code"] == "SWE"]
        assert len(nor_rows) == 1
        assert len(swe_rows) == 2

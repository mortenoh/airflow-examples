"""Pure transform functions extracted from DAGs for testability.

These functions contain the business logic used across various DAGs.
They have no Airflow dependencies and can be tested independently.
"""

import hashlib
import math
from typing import Any

# -- Air Quality (DAG 83) -----------------------------------------------------

AQI_CATEGORIES: list[tuple[float, str]] = [
    (20, "Good"),
    (40, "Fair"),
    (60, "Moderate"),
    (80, "Poor"),
    (100, "Very Poor"),
    (float("inf"), "Extremely Poor"),
]

WHO_THRESHOLDS: dict[str, float] = {
    "pm2_5": 15.0,
    "pm10": 45.0,
    "nitrogen_dioxide": 25.0,
}


def classify_european_aqi(value: float) -> str:
    """Map a European AQI value to its WHO category.

    Args:
        value: AQI numeric value.

    Returns:
        Category string: Good, Fair, Moderate, Poor, Very Poor, or Extremely Poor.
    """
    for threshold, category in AQI_CATEGORIES:
        if value <= threshold:
            return category
    return "Extremely Poor"


def check_who_exceedance(pollutant: str, value: float) -> bool:
    """Check if a pollutant value exceeds its WHO 2021 guideline.

    Args:
        pollutant: Pollutant key (pm2_5, pm10, nitrogen_dioxide).
        value: Measured concentration.

    Returns:
        True if the value exceeds the WHO threshold for that pollutant.
    """
    limit = WHO_THRESHOLDS.get(pollutant)
    if limit is None:
        return False
    return value > limit


# -- Marine + Flood Risk (DAG 84) ---------------------------------------------

def score_wave_height(height: float) -> float:
    """Score 0-100 based on wave height thresholds.

    Args:
        height: Wave height in meters.

    Returns:
        Risk score from 0 to 100.
    """
    if height < 1:
        return 0
    elif height < 2:
        return 25
    elif height < 4:
        return 50
    elif height < 6:
        return 75
    else:
        return 100


def score_flood_ratio(discharge: float, mean: float) -> float:
    """Score 0-100 based on discharge/mean ratio.

    Args:
        discharge: Current river discharge.
        mean: Mean river discharge.

    Returns:
        Risk score from 0, 50, or 100.
    """
    if mean == 0:
        return 0
    ratio = discharge / mean
    if ratio < 1.5:
        return 0
    elif ratio < 3.0:
        return 50
    else:
        return 100


RISK_CATEGORIES: list[tuple[float, str]] = [
    (25, "Low"),
    (50, "Moderate"),
    (75, "High"),
    (float("inf"), "Extreme"),
]


def classify_risk(score: float) -> str:
    """Map a composite score to a risk category.

    Args:
        score: Composite risk score (0-100).

    Returns:
        Risk category: Low, Moderate, High, or Extreme.
    """
    for threshold, category in RISK_CATEGORIES:
        if score <= threshold:
            return category
    return "Extreme"


# -- Earthquake Analysis (DAG 91) ---------------------------------------------

def compute_b_value(magnitudes: list[float]) -> float:
    """Compute Gutenberg-Richter b-value from a list of magnitudes.

    Uses log10(N) vs magnitude linear regression to estimate the b-value,
    which describes the frequency-magnitude relationship of earthquakes.

    Args:
        magnitudes: List of earthquake magnitudes.

    Returns:
        Estimated b-value (typically ~1.0 for tectonic earthquakes).
        Returns 0.0 if insufficient data.
    """
    import numpy as np

    if len(magnitudes) < 2:
        return 0.0

    mags = np.array(magnitudes, dtype=float)
    min_mag = float(np.floor(mags.min() * 2) / 2)
    max_mag = float(mags.max())
    bins = np.arange(min_mag, max_mag + 0.5, 0.5)

    if len(bins) < 2:
        return 0.0

    cumulative = [(float(b), int((mags >= b).sum())) for b in bins[:-1]]
    valid = [(m, n) for m, n in cumulative if n > 0]

    if len(valid) < 2:
        return 0.0

    m_vals = np.array([m for m, _ in valid])
    log_n = np.log10([n for _, n in valid])
    coeffs = np.polyfit(m_vals, log_n, 1)
    return float(-coeffs[0])


# -- Correlation (DAG 93) -----------------------------------------------------

def safe_pearson(x: list[float], y: list[float]) -> dict[str, float | None]:
    """Compute Pearson r with NaN/inf safety.

    Args:
        x: First variable values.
        y: Second variable values (same length as x).

    Returns:
        Dict with keys: r, t_stat, p_approx, n.
        Values may be None if computation is not possible.
    """
    import numpy as np

    if len(x) != len(y) or len(x) < 3:
        return {"r": None, "t_stat": None, "p_approx": None, "n": len(x)}

    x_arr = np.array(x, dtype=float)
    y_arr = np.array(y, dtype=float)

    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_clean = x_arr[mask]
    y_clean = y_arr[mask]
    n = len(x_clean)

    if n < 3:
        return {"r": None, "t_stat": None, "p_approx": None, "n": n}

    x_std = float(np.std(x_clean))
    y_std = float(np.std(y_clean))
    if x_std == 0 or y_std == 0:
        return {"r": None, "t_stat": None, "p_approx": None, "n": n}

    r = float(np.corrcoef(x_clean, y_clean)[0, 1])

    if not math.isfinite(r):
        return {"r": None, "t_stat": None, "p_approx": None, "n": n}

    if abs(r) < 1.0 and n > 2:
        t_stat = r * np.sqrt((n - 2) / (1 - r**2))
        p_approx = 2 * (1 - min(0.9999, abs(float(t_stat)) / np.sqrt(n)))
    else:
        t_stat = 0.0
        p_approx = 1.0

    def _safe(val: float) -> float | None:
        return float(val) if math.isfinite(float(val)) else None

    return {
        "r": _safe(r),
        "t_stat": _safe(float(t_stat)),
        "p_approx": _safe(float(p_approx)),
        "n": n,
    }


# -- Climate Trends (DAG 94) --------------------------------------------------

def compute_annual_trend(years: list[float], values: list[float]) -> dict[str, float]:
    """Compute linear regression slope and intercept for a trend line.

    Args:
        years: List of year values (x-axis).
        values: List of measured values (y-axis).

    Returns:
        Dict with slope, intercept, and trend_per_decade.
    """
    import numpy as np

    if len(years) < 2 or len(values) < 2:
        return {"slope": 0.0, "intercept": 0.0, "trend_per_decade": 0.0}

    x = np.array(years, dtype=float)
    y = np.array(values, dtype=float)
    coeffs = np.polyfit(x, y, 1)
    slope = float(coeffs[0])
    intercept = float(coeffs[1])
    return {"slope": slope, "intercept": intercept, "trend_per_decade": slope * 10}


# -- ETL SCD (DAG 100) --------------------------------------------------------

def generate_surrogate_key(code: str) -> int:
    """Generate a deterministic hash-based surrogate key.

    Args:
        code: Business key (e.g. country code like "NOR").

    Returns:
        Integer surrogate key in range 0-99999.
    """
    hash_val = int(hashlib.md5(code.encode()).hexdigest()[:8], 16)  # noqa: S324
    return hash_val % 100000


def apply_scd_type2_rows(
    current: list[dict[str, Any]],
    previous: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply SCD Type 2 change detection and generate versioned rows.

    Compares current and previous snapshots, producing expired + new rows
    for changed records and single current rows for unchanged records.

    Args:
        current: Current dimension snapshot rows.
        previous: Previous dimension snapshot rows (same order).

    Returns:
        List of SCD rows with valid_from, valid_to, is_current fields.
    """
    scd_rows: list[dict[str, Any]] = []

    for curr, prev in zip(current, previous):
        curr_pop = curr.get("population")
        prev_pop = prev.get("population")
        changed = str(curr_pop) != str(prev_pop)

        if changed:
            scd_rows.append({
                **prev,
                "valid_from": "2024-01-01",
                "valid_to": "2025-01-01",
                "is_current": False,
            })
            scd_rows.append({
                **curr,
                "valid_from": "2025-01-01",
                "valid_to": "9999-12-31",
                "is_current": True,
            })
        else:
            scd_rows.append({
                **curr,
                "valid_from": "2024-01-01",
                "valid_to": "9999-12-31",
                "is_current": True,
            })

    return scd_rows

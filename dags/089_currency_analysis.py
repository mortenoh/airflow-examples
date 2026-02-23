"""DAG 89: Currency Time Series Analysis.

Fetches historical exchange rates from the Frankfurter API, computes
daily log returns, rolling volatility, cross-currency correlations,
and identifies extreme market events (>2 std dev moves).
"""

import os
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

from airflow_examples.config import DEFAULT_ARGS, OUTPUT_BASE, timestamp

OUTPUT_DIR = str(OUTPUT_BASE / "api_pipelines/89_currency")


@task
def fetch_time_series() -> dict[str, object]:
    """Fetch 2024 EUR exchange rates for Nordic + major currencies."""
    from airflow_examples.apis import FRANKFURTER, fetch_json

    url = f"{FRANKFURTER}/2024-01-01..2024-12-31"
    data = fetch_json(url, params={"from": "EUR", "to": "NOK,SEK,DKK,GBP,USD,CHF"})
    rates: dict[str, dict[str, float]] = data.get("rates", {})  # type: ignore[assignment]
    print(f"[{timestamp()}] Fetched {len(rates)} trading days of EUR rates")
    return {"rates": rates, "base": data.get("base"), "start": data.get("start_date"), "end": data.get("end_date")}


@task
def compute_returns(time_series: dict[str, object]) -> dict[str, object]:
    """Compute daily log returns for each currency pair."""
    import numpy as np
    import pandas as pd

    rates: dict[str, dict[str, float]] = time_series.get("rates", {})  # type: ignore[assignment]
    df = pd.DataFrame.from_dict(rates, orient="index")
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    log_returns = np.log(df / df.shift(1)).dropna()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_returns.to_csv(f"{OUTPUT_DIR}/log_returns.csv")

    print(f"[{timestamp()}] Computed log returns: {len(log_returns)} days, {len(log_returns.columns)} pairs")
    for col in log_returns.columns:
        print(f"  {col}: mean={log_returns[col].mean():.6f}, std={log_returns[col].std():.6f}")

    return {"returns": log_returns.to_dict(), "days": len(log_returns)}


@task
def compute_volatility(returns_data: dict[str, object]) -> dict[str, object]:
    """Compute 30-day rolling standard deviation of returns (annualized)."""
    import numpy as np
    import pandas as pd

    returns_dict: dict[str, dict[str, float]] = returns_data.get("returns", {})  # type: ignore[assignment]
    df = pd.DataFrame(returns_dict)
    df.index = pd.to_datetime(df.index)

    rolling_vol = df.rolling(window=30).std() * np.sqrt(252)
    latest_vol = rolling_vol.iloc[-1] if len(rolling_vol) > 0 else pd.Series(dtype=float)

    print(f"[{timestamp()}] Annualized 30-day rolling volatility (latest):")
    for col in latest_vol.index:
        print(f"  {col}: {latest_vol[col]:.4f} ({latest_vol[col] * 100:.2f}%)")

    return {"latest_volatility": latest_vol.to_dict()}


@task
def compute_correlation(returns_data: dict[str, object]) -> dict[str, object]:
    """Compute correlation matrix of daily returns across all pairs."""
    import pandas as pd

    returns_dict: dict[str, dict[str, float]] = returns_data.get("returns", {})  # type: ignore[assignment]
    df = pd.DataFrame(returns_dict)
    corr = df.corr()

    print(f"[{timestamp()}] Correlation matrix:")
    print(corr.round(3).to_string())

    corr.to_csv(f"{OUTPUT_DIR}/correlation.csv")
    return {"correlation": corr.to_dict()}


@task
def identify_events(returns_data: dict[str, object]) -> dict[str, object]:
    """Flag dates with >2 std dev moves and identify clustering."""
    import pandas as pd

    returns_dict: dict[str, dict[str, float]] = returns_data.get("returns", {})  # type: ignore[assignment]
    df = pd.DataFrame(returns_dict)
    df.index = pd.to_datetime(df.index)

    events: list[dict[str, object]] = []
    for col in df.columns:
        mean = df[col].mean()
        std = df[col].std()
        extreme = df[df[col].abs() > mean + 2 * std][col]
        for date, value in extreme.items():
            std_devs = float(abs(value) / std)
            events.append({"date": str(date)[:10], "currency": col, "return": float(value), "std_devs": std_devs})

    events.sort(key=lambda x: abs(float(str(x["return"]))), reverse=True)
    print(f"[{timestamp()}] Extreme events (>2 std dev): {len(events)}")
    for e in events[:10]:
        print(f"  {e['date']} {e['currency']}: {float(str(e['return'])) * 100:.3f}% ({e['std_devs']:.1f} sigma)")

    return {"events": events[:20], "total_events": len(events)}


@task
def report(
    ts_info: dict[str, object],
    volatility: dict[str, object],
    correlation: dict[str, object],
    events: dict[str, object],
) -> None:
    """Print summary stats, volatility, correlation, and extreme events."""
    print(f"\n[{timestamp()}] === Currency Analysis Report ===")
    print(f"  Period: {ts_info.get('start')} to {ts_info.get('end')}")
    print(f"  Base: {ts_info.get('base')}")

    vol: dict[str, float] = volatility.get("latest_volatility", {})  # type: ignore[assignment]
    print("\n  Annualized Volatility:")
    for curr, v in vol.items():
        print(f"    {curr}: {float(v) * 100:.2f}%")

    print(f"\n  Extreme Events: {events.get('total_events')} total")
    evt_list: list[dict[str, object]] = events.get("events", [])  # type: ignore[assignment]
    for e in evt_list[:5]:
        print(f"    {e['date']} {e['currency']}: {float(str(e['return'])) * 100:.3f}%")
    print(f"  Output: {OUTPUT_DIR}/")


with DAG(
    dag_id="089_currency_analysis",
    default_args=DEFAULT_ARGS,
    description="Currency time series analysis with volatility and correlation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "api"],
) as dag:
    ts = fetch_time_series()
    returns = compute_returns(time_series=ts)
    vol = compute_volatility(returns_data=returns)
    corr = compute_correlation(returns_data=returns)
    events = identify_events(returns_data=returns)
    report(ts_info=ts, volatility=vol, correlation=corr, events=events)

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command=f"rm -rf {OUTPUT_DIR} && echo 'Cleaned up {OUTPUT_DIR}'",
    )

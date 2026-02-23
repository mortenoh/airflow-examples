"""Streamlit dashboard for visualizing Airflow DAG outputs.

Reads CSV and Parquet files written by DAG pipelines to ./target/api_pipelines/
and provides interactive tables and charts for the most interesting datasets.

Launch with: uv run streamlit run dashboard/app.py
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

OUTPUT_ROOT = Path("/opt/airflow/target/api_pipelines")

st.set_page_config(page_title="Airflow Pipeline Dashboard", layout="wide")
st.title("Airflow Pipeline Dashboard")


def load_csv(path: Path) -> pd.DataFrame | None:
    """Load a CSV file, returning None on error."""
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def load_parquet(path: Path) -> pd.DataFrame | None:
    """Load a Parquet file, returning None on error."""
    try:
        return pd.read_parquet(path)
    except Exception:
        return None


def load_file(path: Path) -> pd.DataFrame | None:
    """Load a CSV or Parquet file based on extension."""
    if path.suffix == ".csv":
        return load_csv(path)
    elif path.suffix == ".parquet":
        return load_parquet(path)
    return None


def discover_outputs() -> dict[str, list[Path]]:
    """Scan the output root for DAG output directories and their files."""
    outputs: dict[str, list[Path]] = {}
    if not OUTPUT_ROOT.exists():
        return outputs
    for subdir in sorted(OUTPUT_ROOT.iterdir()):
        if subdir.is_dir():
            files = sorted(
                p for p in subdir.rglob("*") if p.is_file() and p.suffix in (".csv", ".parquet")
            )
            if files:
                outputs[subdir.name] = files
    return outputs


# -- Sidebar -------------------------------------------------------------------
st.sidebar.header("Navigation")
outputs = discover_outputs()

if not outputs:
    st.warning(
        "No pipeline outputs found in `./target/api_pipelines/`. "
        "Run `make run` to execute DAGs first."
    )
    st.stop()

pages = ["Climate Trends", "Country Dashboard", "World Bank", "Air Quality", "Earthquake", "File Browser"]
page = st.sidebar.radio("Dashboard", pages)


# -- Climate Trends (DAG 94) --------------------------------------------------
if page == "Climate Trends":
    st.header("Climate Trends (DAG 94)")
    dag_key = "94_climate_trends"

    if dag_key not in outputs:
        st.info(f"No output found for {dag_key}. Run DAG 94 first.")
    else:
        for path in outputs[dag_key]:
            df = load_file(path)
            if df is not None:
                st.subheader(path.name)
                if "year" in df.columns and "temperature_2m_mean" in df.columns:
                    st.line_chart(df.set_index("year")["temperature_2m_mean"])
                    st.caption("Annual mean temperature trend for Oslo")
                st.dataframe(df, use_container_width=True)


# -- Country Dashboard (DAG 98) -----------------------------------------------
elif page == "Country Dashboard":
    st.header("Country Dashboard (DAG 98)")
    dag_key = "98_multi_api_dashboard"

    if dag_key not in outputs:
        st.info(f"No output found for {dag_key}. Run DAG 98 first.")
    else:
        for path in outputs[dag_key]:
            df = load_file(path)
            if df is not None:
                st.subheader(path.name)
                if "livability_score" in df.columns and "country" in df.columns:
                    chart_df = df[["country", "livability_score"]].dropna().sort_values(
                        "livability_score", ascending=False
                    )
                    st.bar_chart(chart_df.set_index("country")["livability_score"])
                    st.caption("Composite livability scores by country")
                st.dataframe(df, use_container_width=True)


# -- World Bank (DAG 88) ------------------------------------------------------
elif page == "World Bank":
    st.header("World Bank Indicators (DAG 88)")
    dag_key = "88_world_bank"

    if dag_key not in outputs:
        st.info(f"No output found for {dag_key}. Run DAG 88 first.")
    else:
        for path in outputs[dag_key]:
            df = load_file(path)
            if df is not None:
                st.subheader(path.name)
                if "year" in df.columns and "value" in df.columns:
                    if "country_code" in df.columns:
                        for country in df["country_code"].unique()[:5]:
                            country_df = df[df["country_code"] == country]
                            st.line_chart(
                                country_df.set_index("year")["value"],
                            )
                            st.caption(f"Indicator trend for {country}")
                st.dataframe(df, use_container_width=True)


# -- Air Quality (DAG 83) -----------------------------------------------------
elif page == "Air Quality":
    st.header("Air Quality Monitoring (DAG 83)")
    dag_key = "83_air_quality"

    if dag_key not in outputs:
        st.info(f"No output found for {dag_key}. Run DAG 83 first.")
    else:
        for path in outputs[dag_key]:
            df = load_file(path)
            if df is not None:
                st.subheader(path.name)
                aqi_cols = ["Good", "Fair", "Moderate", "Poor", "Very Poor", "Extremely Poor"]
                present_cols = [c for c in aqi_cols if c in df.columns]
                if present_cols and "name" in df.columns:
                    st.bar_chart(df.set_index("name")[present_cols])
                    st.caption("AQI category distribution by city")
                st.dataframe(df, use_container_width=True)


# -- Earthquake (DAG 91) ------------------------------------------------------
elif page == "Earthquake":
    st.header("Earthquake Analysis (DAG 91)")
    dag_key = "91_earthquake"

    if dag_key not in outputs:
        st.info(f"No output found for {dag_key}. Run DAG 91 first.")
    else:
        for path in outputs[dag_key]:
            df = load_file(path)
            if df is not None:
                st.subheader(path.name)
                if "magnitude" in df.columns:
                    st.bar_chart(df["magnitude"].value_counts().sort_index())
                    st.caption("Earthquake magnitude distribution")
                st.dataframe(df, use_container_width=True)


# -- Generic File Browser -----------------------------------------------------
elif page == "File Browser":
    st.header("File Browser")
    selected_dag = st.selectbox("Select DAG output", list(outputs.keys()))

    if selected_dag:
        for path in outputs[selected_dag]:
            df = load_file(path)
            if df is not None:
                st.subheader(path.name)
                st.dataframe(df, use_container_width=True)
                st.caption(f"{len(df)} rows x {len(df.columns)} columns")

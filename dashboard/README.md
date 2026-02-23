# Streamlit Dashboard

Interactive dashboard for exploring Airflow DAG pipeline outputs.

## Prerequisites

Run the Airflow DAGs first to generate output files:

```bash
make run
```

## Launch

```bash
make dashboard
```

This opens a browser at `http://localhost:8501` with:

- **Climate Trends** -- temperature trend line chart from DAG 94
- **Country Dashboard** -- livability score bar chart from DAG 98
- **World Bank** -- GDP time series from DAG 88
- **Air Quality** -- AQI distribution from DAG 83
- **Earthquake** -- magnitude histogram from DAG 91
- **File Browser** -- generic table viewer for all DAG outputs

## How it works

DAGs write CSV and Parquet files to `/tmp/airflow_api_pipelines/<dag_name>/`.
The dashboard scans this directory and renders the files as interactive tables
and charts using Streamlit.

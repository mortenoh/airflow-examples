"""DAG 36: SQL ETL Pipeline.

A complete SQL-based ETL pipeline using ``SQLExecuteQueryOperator``.
Creates staging and production tables, loads raw data, transforms
with SQL, and produces a summary -- all within PostgreSQL.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG, TaskGroup

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="036_sql_pipeline",
    default_args=DEFAULT_ARGS,
    description="SQL ETL: staging -> transform -> production -> summary",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "sql", "pipeline"],
) as dag:
    # Setup: create schemas and tables
    with TaskGroup("setup") as setup_group:
        create_staging = SQLExecuteQueryOperator(
            task_id="create_staging",
            conn_id="postgres_default",
            sql="""
                CREATE TABLE IF NOT EXISTS stg_sensors (
                    sensor_id VARCHAR(20),
                    raw_value TEXT,
                    unit VARCHAR(10),
                    ts TIMESTAMP
                );
            """,
        )

        create_production = SQLExecuteQueryOperator(
            task_id="create_production",
            conn_id="postgres_default",
            sql="""
                CREATE TABLE IF NOT EXISTS prod_measurements (
                    id SERIAL PRIMARY KEY,
                    sensor_id VARCHAR(20),
                    value_numeric REAL,
                    unit VARCHAR(10),
                    is_valid BOOLEAN,
                    measured_at TIMESTAMP,
                    loaded_at TIMESTAMP DEFAULT NOW()
                );
            """,
        )

        create_summary = SQLExecuteQueryOperator(
            task_id="create_summary",
            conn_id="postgres_default",
            sql="""
                CREATE TABLE IF NOT EXISTS summary_stats (
                    sensor_id VARCHAR(20),
                    avg_value REAL,
                    min_value REAL,
                    max_value REAL,
                    record_count INTEGER,
                    computed_at TIMESTAMP DEFAULT NOW()
                );
            """,
        )

    # Extract: load raw data into staging
    with TaskGroup("extract") as extract_group:
        load_staging = SQLExecuteQueryOperator(
            task_id="load_staging",
            conn_id="postgres_default",
            sql="""
                TRUNCATE stg_sensors;
                INSERT INTO stg_sensors (sensor_id, raw_value, unit, ts) VALUES
                ('temp_01', '22.5', 'C', '2024-01-01 08:00:00'),
                ('temp_01', '23.1', 'C', '2024-01-01 09:00:00'),
                ('temp_01', 'ERR', 'C', '2024-01-01 10:00:00'),
                ('temp_02', '19.8', 'C', '2024-01-01 08:00:00'),
                ('temp_02', '20.3', 'C', '2024-01-01 09:00:00'),
                ('hum_01', '65.0', '%', '2024-01-01 08:00:00'),
                ('hum_01', '68.2', '%', '2024-01-01 09:00:00'),
                ('hum_01', '-5.0', '%', '2024-01-01 10:00:00');
            """,
        )

    # Transform: clean, validate, and load into production
    with TaskGroup("transform") as transform_group:
        load_production = SQLExecuteQueryOperator(
            task_id="load_production",
            conn_id="postgres_default",
            sql="""
                INSERT INTO prod_measurements (sensor_id, value_numeric, unit, is_valid, measured_at)
                SELECT
                    sensor_id,
                    CASE WHEN raw_value ~ '^-?[0-9.]+$' THEN raw_value::REAL ELSE NULL END,
                    unit,
                    CASE
                        WHEN raw_value ~ '^-?[0-9.]+$'
                             AND raw_value::REAL BETWEEN -50 AND 60
                        THEN TRUE
                        ELSE FALSE
                    END,
                    ts
                FROM stg_sensors;
            """,
        )

    # Summarize: compute aggregate statistics
    with TaskGroup("summarize") as summarize_group:
        compute_stats = SQLExecuteQueryOperator(
            task_id="compute_stats",
            conn_id="postgres_default",
            sql="""
                TRUNCATE summary_stats;
                INSERT INTO summary_stats (sensor_id, avg_value, min_value, max_value, record_count)
                SELECT
                    sensor_id,
                    AVG(value_numeric),
                    MIN(value_numeric),
                    MAX(value_numeric),
                    COUNT(*)
                FROM prod_measurements
                WHERE is_valid = TRUE
                GROUP BY sensor_id;
            """,
        )

    # Teardown: clean up all tables
    with TaskGroup("teardown") as teardown_group:
        drop_tables = SQLExecuteQueryOperator(
            task_id="drop_tables",
            conn_id="postgres_default",
            sql="""
                DROP TABLE IF EXISTS summary_stats;
                DROP TABLE IF EXISTS prod_measurements;
                DROP TABLE IF EXISTS stg_sensors;
            """,
        )

    setup_group >> extract_group >> transform_group >> summarize_group >> teardown_group

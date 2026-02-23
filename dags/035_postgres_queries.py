"""DAG 35: PostgreSQL Queries.

Demonstrates ``SQLExecuteQueryOperator`` for executing SQL against a
PostgreSQL database. Shows DDL (CREATE TABLE), DML (INSERT, UPDATE),
and queries with templated parameters.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="035_postgres_queries",
    default_args=DEFAULT_ARGS,
    description="SQLExecuteQueryOperator for DDL, DML, and SQL queries",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "sql"],
) as dag:
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS weather_readings (
                id SERIAL PRIMARY KEY,
                station VARCHAR(50) NOT NULL,
                temp_c REAL NOT NULL,
                humidity REAL NOT NULL,
                recorded_at TIMESTAMP DEFAULT NOW()
            );
        """,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id="postgres_default",
        sql="""
            INSERT INTO weather_readings (station, temp_c, humidity) VALUES
            ('oslo_01', 12.5, 68.0),
            ('oslo_02', 13.1, 65.0),
            ('bergen_01', 9.8, 82.0),
            ('tromso_01', 3.2, 71.0),
            ('stavanger_01', 11.0, 75.0);
        """,
    )

    update_data = SQLExecuteQueryOperator(
        task_id="update_data",
        conn_id="postgres_default",
        sql="""
            UPDATE weather_readings
            SET temp_c = temp_c + 0.5
            WHERE station LIKE 'oslo%';
        """,
    )

    # Templated query using Airflow macros
    query_data = SQLExecuteQueryOperator(
        task_id="query_data",
        conn_id="postgres_default",
        sql="""
            SELECT station, temp_c, humidity, recorded_at
            FROM weather_readings
            ORDER BY temp_c DESC;
        """,
    )

    cleanup_table = SQLExecuteQueryOperator(
        task_id="cleanup_table",
        conn_id="postgres_default",
        sql="DROP TABLE IF EXISTS weather_readings;",
    )

    create_table >> insert_data >> update_data >> query_data >> cleanup_table

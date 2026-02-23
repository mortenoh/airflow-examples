"""DAG 37: Generic SQL Transfer.

Demonstrates transferring data between tables using SQL operators.
Creates a source table, transforms data via SQL, and loads into a
destination table -- a common pattern for database-to-database ETL.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="037_generic_transfer",
    default_args=DEFAULT_ARGS,
    description="SQL-based data transfer between tables",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "sql"],
) as dag:
    # Create source and destination tables
    create_source = SQLExecuteQueryOperator(
        task_id="create_source",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS transfer_source (
                id SERIAL PRIMARY KEY,
                city VARCHAR(50),
                country VARCHAR(50),
                population INTEGER
            );
            TRUNCATE transfer_source;
            INSERT INTO transfer_source (city, country, population) VALUES
            ('Oslo', 'Norway', 709037),
            ('Bergen', 'Norway', 289330),
            ('Stockholm', 'Sweden', 984748),
            ('Copenhagen', 'Denmark', 644431),
            ('Helsinki', 'Finland', 658864);
        """,
    )

    create_dest = SQLExecuteQueryOperator(
        task_id="create_dest",
        conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS transfer_dest (
                city VARCHAR(50),
                country VARCHAR(50),
                population INTEGER,
                size_category VARCHAR(20),
                transferred_at TIMESTAMP DEFAULT NOW()
            );
            TRUNCATE transfer_dest;
        """,
    )

    # Transfer with transformation: add size_category
    transfer_data = SQLExecuteQueryOperator(
        task_id="transfer_data",
        conn_id="postgres_default",
        sql="""
            INSERT INTO transfer_dest (city, country, population, size_category)
            SELECT
                city,
                country,
                population,
                CASE
                    WHEN population >= 900000 THEN 'large'
                    WHEN population >= 500000 THEN 'medium'
                    ELSE 'small'
                END
            FROM transfer_source
            ORDER BY population DESC;
        """,
    )

    # Verify transfer
    verify = SQLExecuteQueryOperator(
        task_id="verify",
        conn_id="postgres_default",
        sql="""
            SELECT city, country, population, size_category
            FROM transfer_dest
            ORDER BY population DESC;
        """,
    )

    # Cleanup
    cleanup = SQLExecuteQueryOperator(
        task_id="cleanup",
        conn_id="postgres_default",
        sql="""
            DROP TABLE IF EXISTS transfer_dest;
            DROP TABLE IF EXISTS transfer_source;
        """,
    )

    [create_source, create_dest] >> transfer_data >> verify >> cleanup

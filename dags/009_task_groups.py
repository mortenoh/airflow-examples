"""DAG 09: Task Groups.

Demonstrates ``TaskGroup`` for organizing complex DAGs into
collapsible sections in the Airflow UI. Shows nested groups
and how task IDs are automatically prefixed with the group name.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TaskGroup

from airflow_examples.config import DEFAULT_ARGS

with DAG(
    dag_id="009_task_groups",
    default_args=DEFAULT_ARGS,
    description="TaskGroup for organizing complex DAGs with nested groups",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    start = BashOperator(task_id="start", bash_command='echo "Pipeline start"')

    # First group: data extraction
    with TaskGroup("extract") as extract_group:
        extract_users = BashOperator(
            task_id="users",
            bash_command='echo "Extracting users"',
        )
        extract_orders = BashOperator(
            task_id="orders",
            bash_command='echo "Extracting orders"',
        )
        extract_products = BashOperator(
            task_id="products",
            bash_command='echo "Extracting products"',
        )

    # Second group: data transformation with nested subgroups
    with TaskGroup("transform") as transform_group:
        with TaskGroup("clean") as clean_group:
            clean_users = BashOperator(
                task_id="users",
                bash_command='echo "Cleaning user data"',
            )
            clean_orders = BashOperator(
                task_id="orders",
                bash_command='echo "Cleaning order data"',
            )

        with TaskGroup("enrich") as enrich_group:
            enrich_users = BashOperator(
                task_id="users",
                bash_command='echo "Enriching user data"',
            )
            join_data = BashOperator(
                task_id="join",
                bash_command='echo "Joining enriched data"',
            )

        clean_group >> enrich_group

    # Third group: loading
    with TaskGroup("load") as load_group:
        load_warehouse = BashOperator(
            task_id="warehouse",
            bash_command='echo "Loading to warehouse"',
        )
        load_cache = BashOperator(
            task_id="cache",
            bash_command='echo "Updating cache"',
        )

    end = BashOperator(task_id="end", bash_command='echo "Pipeline complete"')

    start >> extract_group >> transform_group >> load_group >> end

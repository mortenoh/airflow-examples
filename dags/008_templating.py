"""DAG 08: Jinja Templating.

Demonstrates Airflow's Jinja templating engine in operator parameters.
Shows template variables like ``{{ ds }}``, ``{{ params }}``,
``{{ macros }}``, and custom parameter passing.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from airflow_examples.config import DEFAULT_ARGS, timestamp


def show_template_vars(**context: object) -> None:
    """Print commonly-used template variables from context."""
    print(f"[{timestamp()}] Template variables:")
    print(f"  ds           = {context.get('ds')}")
    print(f"  logical_date = {context.get('logical_date')}")
    print(f"  dag_id       = {context.get('dag').dag_id}")  # type: ignore[union-attr]
    print(f"  task_id      = {context.get('task_id')}")
    print(f"  run_id       = {context.get('run_id')}")
    params = context.get("params", {})
    print(f"  params       = {params}")


with DAG(
    dag_id="008_templating",
    default_args=DEFAULT_ARGS,
    description="Jinja templating in operators: {{ ds }}, {{ params }}, {{ macros }}",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
    params={"env": "staging", "batch_size": 100},
) as dag:
    # BashOperator supports Jinja templates in bash_command
    templated_bash = BashOperator(
        task_id="templated_bash",
        bash_command=(
            'echo "Execution date: {{ ds }}" && '
            'echo "DAG: {{ dag.dag_id }}" && '
            'echo "Task: {{ task.task_id }}" && '
            'echo "Environment: {{ params.env }}" && '
            'echo "Batch size: {{ params.batch_size }}"'
        ),
    )

    # Using macros for date arithmetic
    macro_bash = BashOperator(
        task_id="macro_bash",
        bash_command=(
            'echo "ds: {{ ds }}" && '
            'echo "Yesterday: {{ macros.ds_add(ds, -1) }}" && '
            'echo "Tomorrow: {{ macros.ds_add(ds, 1) }}"'
        ),
    )

    # PythonOperator with context access
    context_task = PythonOperator(
        task_id="show_context",
        python_callable=show_template_vars,
    )

    templated_bash >> macro_bash >> context_task

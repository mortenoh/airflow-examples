# DAG Anatomy

Minimal structure: context manager, start date, schedule, tasks, dependencies

```python {all|5-12|13-17|19-23|25}
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="001_hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger only
    catchup=False,          # don't backfill past runs
    tags=["example"],
) as dag:
    hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow!"',
    )

    date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    hello >> date               # set dependency
```

<span class="text-sm opacity-60">DAG 001 -- hello_world.py</span>

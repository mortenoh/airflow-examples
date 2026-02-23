# BashOperator

Run shell commands as tasks -- with full Jinja templating

```python {all|3-8|10-14}
# Airflow macros available inside bash_command
core_macros = BashOperator(
    task_id="core_macros",
    bash_command="""
        echo "ds (YYYY-MM-DD):  {{ ds }}"
        echo "logical_date:     {{ logical_date }}"
        echo "Yesterday:        {{ macros.ds_add(ds, -1) }}"
    """,
)

dynamic_paths = BashOperator(
    task_id="dynamic_paths",
    bash_command="""
        INGEST="/data/raw/{{ ds_nodash }}/{{ params.station }}"
        mkdir -p "$INGEST"
    """,
)
```

Jinja `{% for %}` and `{% if %}` expand at render time -- generate bash from templates

<span class="text-sm opacity-60">DAG 046 -- bash_templating.py</span>

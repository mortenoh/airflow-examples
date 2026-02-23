# Advanced Patterns

<div class="grid grid-cols-2 gap-4">
<div>

### Assets (data-aware scheduling)

```python
from airflow.sdk import Asset

data = Asset("s3://bucket/weather.parquet")

# Producer declares outlet
produce = PythonOperator(
    task_id="produce",
    python_callable=produce_data,
    outlets=[data],
)

# Consumer triggers on update
with DAG(schedule=[data]):
    consume = ...
```

<span class="text-sm opacity-60">DAG 014</span>

</div>
<div>

### Other patterns

- **Task groups** -- visual grouping in the UI, shared prefixes
- **Pools** -- limit concurrency across DAGs (e.g. max 3 DB connections)
- **Priority weights** -- control execution order within a pool
- **Setup / teardown** -- provision and clean up resources around tasks
- **XCom** -- pass metadata between tasks (keep it small!)

</div>
</div>

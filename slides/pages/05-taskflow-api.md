# TaskFlow API (`@task`)

Modern decorator syntax -- automatic XCom between tasks

```python {all|1-5|7-10|12-14|16-19}
from airflow.sdk import DAG, task

@task
def extract() -> dict[str, list[int]]:
    return {"values": [1, 2, 3, 4, 5]}

@task
def transform(data: dict[str, list[int]]) -> dict[str, list[int]]:
    return {"values": [v * 2 for v in data["values"]]}

@task
def load(data: dict[str, list[int]]) -> None:
    print(f"Loaded {len(data['values'])} values: {data['values']}")

with DAG("004_taskflow_api", ...):
    raw = extract()
    processed = transform(raw)          # XCom passed automatically
    load(processed)                     # no ti.xcom_pull needed
```

No `PythonOperator`, no `op_args`, no manual XCom -- just functions

<span class="text-sm opacity-60">DAG 004 -- taskflow_api.py</span>

# Variable-Driven Scheduling

Change **when** a DAG runs without editing code or redeploying

```python {all|1-4|6-9|11}
SCHEDULE_VAR_KEY = "dag_107_schedule"

try:
    _schedule = Variable.get(SCHEDULE_VAR_KEY, default_var="@daily")
except Exception:
    _schedule = None          # no DB at parse time (pytest, ruff)

with DAG(
    dag_id="107_variable_driven_scheduling",
    schedule=_schedule,       # read from Variable at parse time
    catchup=False,
):
```

<v-clicks>

- `Variable.get()` runs at **parse time** -- scheduler re-parses every ~30s by default (`min_file_process_interval`)
- Update via **UI** (Admin -> Variables) or **REST API** (`PATCH /api/v2/variables/{key}`)
- A custom web app can call the API to adjust schedules on demand -- no Airflow code changes needed
- Guard with `try/except` so the file still imports when no Airflow DB is available

</v-clicks>

<span class="text-sm opacity-60">DAG 107 -- variable_driven_scheduling.py</span>

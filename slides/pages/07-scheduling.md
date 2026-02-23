# Scheduling & Time Model

`logical_date` is the scheduled slot, not when the task actually runs

```python
with DAG(
    dag_id="108_backfill_awareness",
    schedule="@hourly",           # cron presets: @daily, @weekly, @monthly
    catchup=True,                 # replay missed runs on unpause
    max_active_runs=1,
):
    show_dates = BashOperator(
        task_id="show_dates",
        bash_command="""
            echo "Logical date (slot): {{ logical_date }}"
            echo "Wall-clock (now):    $(date -u)"
            echo "Interval start:     {{ data_interval_start }}"
            echo "Interval end:       {{ data_interval_end }}"
        """,
    )
```

<v-clicks>

- **`catchup=True`** -- scheduler auto-creates runs for every missed interval between `start_date` and now when the DAG is unpaused or first deployed
- **`catchup=False`** -- only the most recent interval runs; past slots are skipped entirely
- **`airflow dags backfill`** -- CLI command to manually trigger runs for a specific date range, independent of the catchup setting

</v-clicks>

<span class="text-sm opacity-60">DAG 108 -- backfill_awareness.py</span>

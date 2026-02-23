# Scheduling & Time Model

`logical_date` is the scheduled slot, not when the task actually runs

<div style="transform: scale(0.78); transform-origin: top left; width: 128%;">

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

```
# Cron expressions   ┌─min ┌─hour ┌─day ┌─month ┌─weekday
"0 1 * * *"          # Every night at 01:00
"30 6 * * 1-5"       # Weekdays at 06:30
"0 */4 * * *"        # Every 4 hours
"0 9 1 * *"          # First of each month at 09:00
```

- **`catchup=True`** -- auto-creates runs for every missed interval between `start_date` and now
- **`catchup=False`** -- only the most recent interval runs; past slots are skipped
- **`airflow dags backfill`** -- manually trigger runs for a specific date range

<span class="opacity-60">DAG 108 -- backfill_awareness.py</span>

</div>

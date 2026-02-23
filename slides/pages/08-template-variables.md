# Template Variables <span class="text-sm opacity-60 ml-4">DAG 046</span>

None of these are wall-clock time -- they represent the scheduled slot

<div class="grid grid-cols-2 gap-4 text-sm">
<div>

| Variable | Example |
|---|---|
| `logical_date` | `2024-06-15T00:00:00` |
| `ds` | `2024-06-15` |
| `ds_nodash` | `20240615` |
| `data_interval_start` | `2024-06-15T00:00:00` |
| `data_interval_end` | `2024-06-16T00:00:00` |

</div>
<div>

```python
BashOperator(
    task_id="core_macros",
    bash_command="""
      echo "{{ ds }}"
      echo "{{ logical_date }}"
      echo "{{ macros.ds_add(ds, -1) }}"
    """,
)
```

</div>
</div>

<v-click>

Jinja-rendered before execution -- use `macros.ds_add(ds, -1)` for date math, `.strftime()` on `logical_date`

</v-click>

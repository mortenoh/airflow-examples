# Sensors & Deferrables

Wait for external conditions before proceeding

<v-clicks>

- **Sensors** -- poll for a condition (file exists, HTTP 200, time reached)
- **Poke mode** -- holds a worker slot while waiting (simple, wastes resources)
- **Reschedule mode** -- releases the slot between checks (efficient, standard)
- **Deferrable operators** -- suspend to the triggerer process, zero worker slots used

</v-clicks>

<br>

```python
from airflow.providers.standard.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_data",
    filepath="/data/incoming/report.csv",
    mode="reschedule",           # release worker between checks
    poke_interval=60,            # check every 60 seconds
    timeout=3600,                # fail after 1 hour
)
```

<v-click>

> Deferrables are the future -- sensors that hold zero worker slots while waiting

</v-click>

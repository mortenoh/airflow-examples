# Executor != Scheduler

Executor controls **where** tasks run -- it has zero effect on **when** DAGs are scheduled

<div class="grid grid-cols-2 gap-6">
<div>

### Executor comparison

| Executor | Where tasks run | Scheduling effect |
|---|---|---|
| **LocalExecutor** | Subprocess on scheduler node | None |
| **CeleryExecutor** | Distributed Celery workers | None |
| **KubernetesExecutor** | One pod per task | None |

<v-click>

The DAG processor's refresh interval (`dag_processor.refresh_interval`) and the scheduler's heartbeat are **executor-agnostic**. Switching executor does not make DAGs trigger faster.

</v-click>

</div>
<div>

<v-click>

### Real alternatives for responsive scheduling

- **API-triggered runs** (~5s) -- `POST /api/v2/dags/{id}/dagRuns`
- **Asset-driven scheduling** -- DAG runs when upstream data lands
- **Deferrable operators** -- async wait without blocking a worker slot
- **Celery Beat** (standalone) -- sub-minute cron, but loses the DAG model

</v-click>

<v-click>

```
Variable-based  = "change when"  (~30s delay)
API-triggered   = "run now"      (~5s)
```

</v-click>

</div>
</div>

<span class="text-sm opacity-60">DAG 109 -- api_triggered_scheduling.py</span>

# When to Use Airflow

<div class="grid grid-cols-2 gap-8">
<div>

### Use Airflow when

- Batch orchestration on a schedule
- Heterogeneous systems (SQL + Python + Docker + API)
- You need visibility, retries, alerting
- Dependencies between pipelines matter
- Team needs a shared UI for operations

</div>
<div>

### Consider alternatives

| Need | Tool |
|---|---|
| Streaming | Kafka, Flink |
| Sub-second | Celery Beat, cron |
| Pure SQL transforms | dbt (alone) |
| Simple single-script | cron + logging |
| ML pipelines | Kubeflow, MLflow |

</div>
</div>

<br>

<v-click>

> Airflow is at its best when you need a **reliable, visible, scheduled orchestrator**
> for pipelines that touch multiple systems.

</v-click>

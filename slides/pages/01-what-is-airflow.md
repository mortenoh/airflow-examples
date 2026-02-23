# What is Airflow?

An **orchestrator**, not a processing engine

<v-clicks>

- **DAGs** -- Directed Acyclic Graphs define task dependencies
- **Scheduler** -- triggers tasks when their dependencies are met
- **Executor** -- runs tasks (Local, Celery, Kubernetes)
- **Web UI** -- monitor, trigger, debug, browse logs
- **XCom** -- pass small metadata between tasks

</v-clicks>

<br>

<v-click>

> Airflow tells **other systems** what to do and when.
> It does not process your data itself.

</v-click>

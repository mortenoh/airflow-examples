# What Airflow CAN'T Do

<v-clicks>

- **Not a streaming processor** -- batch-only; use Kafka/Flink for event streams
- **No sub-second scheduling** -- minimum interval is ~1 minute; use Celery Beat or cron
- **XCom is not for large data** -- metadata store, not a data pipeline; pass file paths instead
- **DAG parsing overhead** -- every `.py` in the dags folder is parsed every 30s; 100s of DAGs = slow
- **No dynamic DAGs from external state at parse time** -- DAG structure is static per parse cycle
- **No built-in column-level lineage** -- tracks DAG/task dependencies, not data field provenance
- **Single-point-of-failure scheduler** -- requires HA setup (multiple schedulers) for production
- **Complex local dev story** -- needs scheduler + webserver + database + executor just to test

</v-clicks>
